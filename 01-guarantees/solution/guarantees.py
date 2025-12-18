from anysystem import Context, Message, Process


# AT MOST ONCE ---------------------------------------------------------------------------------------------------------


class AtMostOnceSender(Process):
    def __init__(self, proc_id: str, receiver_id: str):
        self._id = proc_id
        self._receiver = receiver_id
        self._next_seq = 1

    def on_local_message(self, msg: Message, ctx: Context):
        if msg.type != "MESSAGE":
            return
        seq = self._next_seq
        self._next_seq += 1
        ctx.send(Message("DATA", {"seq": seq, "text": msg["text"]}), self._receiver)

    def on_message(self, msg: Message, sender: str, ctx: Context):
        pass

    def on_timer(self, timer_name: str, ctx: Context):
        pass


class AtMostOnceReceiver(Process):
    def __init__(self, proc_id: str):
        self._id = proc_id
        self._next_seq = 1
        self._buffer = {}
        self._max_buffer = 12

    def on_local_message(self, msg: Message, ctx: Context):
        pass

    def _deliver(self, text: str, ctx: Context):
        ctx.send_local(Message("MESSAGE", {"text": text}))

    def _flush(self, ctx: Context):
        while self._next_seq in self._buffer:
            text = self._buffer.pop(self._next_seq)
            self._deliver(text, ctx)
            self._next_seq += 1

    def _skip_ahead(self, ctx: Context):
        if not self._buffer:
            return
        new_next = min(self._buffer.keys())
        if new_next <= self._next_seq:
            return
        self._next_seq = new_next
        self._flush(ctx)

    def on_message(self, msg: Message, sender: str, ctx: Context):
        if msg.type != "DATA":
            return
        seq = msg["seq"]
        text = msg["text"]
        if seq < self._next_seq:
            return
        if seq == self._next_seq:
            self._deliver(text, ctx)
            self._next_seq += 1
            self._flush(ctx)
        else:
            if seq not in self._buffer:
                self._buffer[seq] = text
        if len(self._buffer) > self._max_buffer:
            self._skip_ahead(ctx)

    def on_timer(self, timer_name: str, ctx: Context):
        pass


# AT LEAST ONCE --------------------------------------------------------------------------------------------------------


class AtLeastOnceSender(Process):
    def __init__(self, proc_id: str, receiver_id: str):
        self._id = proc_id
        self._receiver = receiver_id
        self._next_seq = 1
        self._base = 1
        self._unacked = {}
        self._pending = []
        self._pending_idx = 0
        self._window = 10
        self._timeout = 6.5
        self._timer = "rtx"
        self._timer_active = False

    def _has_window_space(self) -> bool:
        return self._next_seq < self._base + self._window

    def _arm_timer(self, ctx: Context):
        ctx.set_timer_once(self._timer, self._timeout)
        self._timer_active = True

    def _disarm_timer(self, ctx: Context):
        if self._timer_active:
            ctx.cancel_timer(self._timer)
            self._timer_active = False

    def _restart_timer(self, ctx: Context):
        self._disarm_timer(ctx)
        self._arm_timer(ctx)

    def _send_data(self, seq: int, text: str, ctx: Context):
        ctx.send(Message("DATA", {"seq": seq, "text": text}), self._receiver)

    def _try_send_pending(self, ctx: Context):
        while self._pending_idx < len(self._pending) and self._has_window_space():
            text = self._pending[self._pending_idx]
            self._pending_idx += 1
            seq = self._next_seq
            self._next_seq += 1
            was_empty = not self._unacked
            self._unacked[seq] = text
            self._send_data(seq, text, ctx)
            if was_empty:
                self._restart_timer(ctx)
        if self._pending_idx > 0 and self._pending_idx * 2 >= len(self._pending):
            self._pending = self._pending[self._pending_idx :]
            self._pending_idx = 0

    def on_local_message(self, msg: Message, ctx: Context):
        if msg.type != "MESSAGE":
            return
        text = msg["text"]
        if self._has_window_space():
            seq = self._next_seq
            self._next_seq += 1
            was_empty = not self._unacked
            self._unacked[seq] = text
            self._send_data(seq, text, ctx)
            if was_empty:
                self._restart_timer(ctx)
        else:
            self._pending.append(text)

    def on_message(self, msg: Message, sender: str, ctx: Context):
        if msg.type != "ACK":
            return
        seq = msg["seq"]
        if seq in self._unacked:
            del self._unacked[seq]
        base_changed = False
        while self._base < self._next_seq and self._base not in self._unacked:
            self._base += 1
            base_changed = True
        if not self._unacked:
            self._disarm_timer(ctx)
        else:
            if base_changed:
                self._restart_timer(ctx)
        self._try_send_pending(ctx)

    def on_timer(self, timer_name: str, ctx: Context):
        if timer_name != self._timer:
            return
        self._timer_active = False
        if not self._unacked:
            return
        seq = self._base
        if seq in self._unacked:
            self._send_data(seq, self._unacked[seq], ctx)
        self._arm_timer(ctx)


class AtLeastOnceReceiver(Process):
    def __init__(self, proc_id: str):
        self._id = proc_id

    def on_local_message(self, msg: Message, ctx: Context):
        pass

    def on_message(self, msg: Message, sender: str, ctx: Context):
        if msg.type != "DATA":
            return
        ctx.send_local(Message("MESSAGE", {"text": msg["text"]}))
        ctx.send(Message("ACK", {"seq": msg["seq"]}), sender)

    def on_timer(self, timer_name: str, ctx: Context):
        pass


# EXACTLY ONCE ---------------------------------------------------------------------------------------------------------


class ExactlyOnceSender(Process):
    def __init__(self, proc_id: str, receiver_id: str):
        self._id = proc_id
        self._receiver = receiver_id
        self._next_seq = 1
        self._base = 1
        self._unacked = {}
        self._pending = []
        self._pending_idx = 0
        self._window = 10
        self._timeout = 6.5
        self._timer = "rtx"
        self._timer_active = False

    def _has_window_space(self) -> bool:
        return self._next_seq < self._base + self._window

    def _arm_timer(self, ctx: Context):
        ctx.set_timer_once(self._timer, self._timeout)
        self._timer_active = True

    def _disarm_timer(self, ctx: Context):
        if self._timer_active:
            ctx.cancel_timer(self._timer)
            self._timer_active = False

    def _restart_timer(self, ctx: Context):
        self._disarm_timer(ctx)
        self._arm_timer(ctx)

    def _send_data(self, seq: int, text: str, ctx: Context):
        ctx.send(Message("DATA", {"seq": seq, "text": text}), self._receiver)

    def _try_send_pending(self, ctx: Context):
        while self._pending_idx < len(self._pending) and self._has_window_space():
            text = self._pending[self._pending_idx]
            self._pending_idx += 1
            seq = self._next_seq
            self._next_seq += 1
            was_empty = not self._unacked
            self._unacked[seq] = text
            self._send_data(seq, text, ctx)
            if was_empty:
                self._restart_timer(ctx)
        if self._pending_idx > 0 and self._pending_idx * 2 >= len(self._pending):
            self._pending = self._pending[self._pending_idx :]
            self._pending_idx = 0

    def on_local_message(self, msg: Message, ctx: Context):
        if msg.type != "MESSAGE":
            return
        text = msg["text"]
        if self._has_window_space():
            seq = self._next_seq
            self._next_seq += 1
            was_empty = not self._unacked
            self._unacked[seq] = text
            self._send_data(seq, text, ctx)
            if was_empty:
                self._restart_timer(ctx)
        else:
            self._pending.append(text)

    def on_message(self, msg: Message, sender: str, ctx: Context):
        if msg.type != "ACK":
            return
        seq = msg["seq"]
        if seq in self._unacked:
            del self._unacked[seq]
        base_changed = False
        while self._base < self._next_seq and self._base not in self._unacked:
            self._base += 1
            base_changed = True
        if not self._unacked:
            self._disarm_timer(ctx)
        else:
            if base_changed:
                self._restart_timer(ctx)
        self._try_send_pending(ctx)

    def on_timer(self, timer_name: str, ctx: Context):
        if timer_name != self._timer:
            return
        self._timer_active = False
        if not self._unacked:
            return
        seq = self._base
        if seq in self._unacked:
            self._send_data(seq, self._unacked[seq], ctx)
        self._arm_timer(ctx)


class ExactlyOnceReceiver(Process):
    def __init__(self, proc_id: str):
        self._id = proc_id
        self._next_seq = 1
        self._win = 32
        self._head = 0
        self._seen = bytearray(self._win)

    def on_local_message(self, msg: Message, ctx: Context):
        pass

    def _advance(self):
        self._head = (self._head + 1) % self._win
        self._next_seq += 1
        tail = (self._head + self._win - 1) % self._win
        self._seen[tail] = 0

    def _ensure_window(self, offset: int):
        if offset < self._win:
            return
        new_win = self._win
        while offset >= new_win and new_win < 1024:
            new_win *= 2
        if offset >= new_win:
            new_win = offset + 1
        new_seen = bytearray(new_win)
        for d in range(min(self._win, new_win)):
            new_seen[d] = self._seen[(self._head + d) % self._win]
        self._win = new_win
        self._seen = new_seen
        self._head = 0

    def on_message(self, msg: Message, sender: str, ctx: Context):
        if msg.type != "DATA":
            return
        seq = msg["seq"]
        text = msg["text"]
        if seq < self._next_seq:
            ctx.send(Message("ACK", {"seq": seq}), sender)
            return
        offset = seq - self._next_seq
        self._ensure_window(offset)
        if offset == 0:
            ctx.send_local(Message("MESSAGE", {"text": text}))
            self._advance()
            while self._seen[self._head] == 1:
                self._seen[self._head] = 0
                self._advance()
        else:
            idx = (self._head + offset) % self._win
            if self._seen[idx] == 0:
                self._seen[idx] = 1
                ctx.send_local(Message("MESSAGE", {"text": text}))
        ctx.send(Message("ACK", {"seq": seq}), sender)

    def on_timer(self, timer_name: str, ctx: Context):
        pass


# EXACTLY ONCE + ORDERED -----------------------------------------------------------------------------------------------


class ExactlyOnceOrderedSender(Process):
    def __init__(self, proc_id: str, receiver_id: str):
        self._id = proc_id
        self._receiver = receiver_id
        self._next_seq = 1
        self._base = 1
        self._unacked = {}
        self._pending = []
        self._pending_idx = 0
        self._window = 4
        self._timeout = 6.5
        self._timer = "rtx"
        self._timer_active = False

    def _has_window_space(self) -> bool:
        return self._next_seq < self._base + self._window

    def _arm_timer(self, ctx: Context):
        ctx.set_timer_once(self._timer, self._timeout)
        self._timer_active = True

    def _disarm_timer(self, ctx: Context):
        if self._timer_active:
            ctx.cancel_timer(self._timer)
            self._timer_active = False

    def _restart_timer(self, ctx: Context):
        self._disarm_timer(ctx)
        self._arm_timer(ctx)

    def _send_data(self, seq: int, text: str, ctx: Context):
        ctx.send(Message("DATA", {"seq": seq, "text": text}), self._receiver)

    def _try_send_pending(self, ctx: Context):
        while self._pending_idx < len(self._pending) and self._has_window_space():
            text = self._pending[self._pending_idx]
            self._pending_idx += 1
            seq = self._next_seq
            self._next_seq += 1
            was_empty = not self._unacked
            self._unacked[seq] = text
            self._send_data(seq, text, ctx)
            if was_empty:
                self._restart_timer(ctx)
        if self._pending_idx > 0 and self._pending_idx * 2 >= len(self._pending):
            self._pending = self._pending[self._pending_idx :]
            self._pending_idx = 0

    def on_local_message(self, msg: Message, ctx: Context):
        if msg.type != "MESSAGE":
            return
        text = msg["text"]
        if self._has_window_space():
            seq = self._next_seq
            self._next_seq += 1
            was_empty = not self._unacked
            self._unacked[seq] = text
            self._send_data(seq, text, ctx)
            if was_empty:
                self._restart_timer(ctx)
        else:
            self._pending.append(text)

    def on_message(self, msg: Message, sender: str, ctx: Context):
        if msg.type != "ACK":
            return
        seq = msg["seq"]
        if seq in self._unacked:
            del self._unacked[seq]
        base_changed = False
        while self._base < self._next_seq and self._base not in self._unacked:
            self._base += 1
            base_changed = True
        if not self._unacked:
            self._disarm_timer(ctx)
        else:
            if base_changed:
                self._restart_timer(ctx)
        self._try_send_pending(ctx)

    def on_timer(self, timer_name: str, ctx: Context):
        if timer_name != self._timer:
            return
        self._timer_active = False
        if not self._unacked:
            return
        seq = self._base
        if seq in self._unacked:
            self._send_data(seq, self._unacked[seq], ctx)
        self._arm_timer(ctx)


class ExactlyOnceOrderedReceiver(Process):
    def __init__(self, proc_id: str):
        self._id = proc_id
        self._next_seq = 1
        self._buffer = {}

    def on_local_message(self, msg: Message, ctx: Context):
        pass

    def _deliver(self, text: str, ctx: Context):
        ctx.send_local(Message("MESSAGE", {"text": text}))

    def _flush(self, ctx: Context):
        while self._next_seq in self._buffer:
            text = self._buffer.pop(self._next_seq)
            self._deliver(text, ctx)
            self._next_seq += 1

    def on_message(self, msg: Message, sender: str, ctx: Context):
        if msg.type != "DATA":
            return
        seq = msg["seq"]
        text = msg["text"]
        if seq < self._next_seq:
            ctx.send(Message("ACK", {"seq": seq}), sender)
            return
        if seq == self._next_seq:
            self._deliver(text, ctx)
            self._next_seq += 1
            self._flush(ctx)
        else:
            if seq not in self._buffer:
                self._buffer[seq] = text
        ctx.send(Message("ACK", {"seq": seq}), sender)

    def on_timer(self, timer_name: str, ctx: Context):
        pass
