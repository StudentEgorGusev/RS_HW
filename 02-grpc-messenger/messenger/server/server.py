import os
import queue
import threading
import time
from concurrent import futures

import grpc
from google.protobuf import empty_pb2
from google.protobuf import timestamp_pb2

from messenger.proto import messenger_pb2
from messenger.proto import messenger_pb2_grpc


class MessengerService(messenger_pb2_grpc.MessengerServerServicer):
    def __init__(self):
        self._lock = threading.Lock()
        self._subs = []
        self._last_ns = 0

    def _next_timestamp_locked(self) -> timestamp_pb2.Timestamp:
        ns = time.time_ns()
        if ns <= self._last_ns:
            ns = self._last_ns + 1
        self._last_ns = ns
        ts = timestamp_pb2.Timestamp()
        ts.seconds = ns // 1_000_000_000
        ts.nanos = ns % 1_000_000_000
        return ts

    def SendMessage(self, request, context):
        with self._lock:
            ts = self._next_timestamp_locked()
            msg = messenger_pb2.ChatMessage(author=request.author, text=request.text, send_time=ts)
            subs = list(self._subs)
            for q in subs:
                m = messenger_pb2.ChatMessage()
                m.CopyFrom(msg)
                q.put(m)
        return messenger_pb2.SendMessageResponse(send_time=ts)

    def ReadMessages(self, request: empty_pb2.Empty, context):
        q = queue.Queue()
        with self._lock:
            self._subs.append(q)
        try:
            while context.is_active():
                try:
                    msg = q.get(timeout=1.0)
                except queue.Empty:
                    continue
                yield msg
        finally:
            with self._lock:
                if q in self._subs:
                    self._subs.remove(q)


def main():
    port = os.environ.get("MESSENGER_SERVER_PORT", "51075")
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=32))
    messenger_pb2_grpc.add_MessengerServerServicer_to_server(MessengerService(), server)
    server.add_insecure_port(f"0.0.0.0:{port}")
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    main()
