import copy
import json
import os
import threading
import time
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import List, Dict

import google.protobuf.empty_pb2  # Empty
import google.protobuf.json_format  # ParseDict, MessageToDict
import grpc

from messenger.proto import messenger_pb2
from messenger.proto import messenger_pb2_grpc


def ts_to_str(ts) -> str:
    dt = datetime.fromtimestamp(ts.seconds, tz=timezone.utc)
    base = dt.strftime("%Y-%m-%dT%H:%M:%S")
    return f"{base}.{ts.nanos:09d}Z"


class PostBox:
    def __init__(self):
        self._messages: List[Dict] = []
        self._lock = threading.Lock()

    def collect_messages(self) -> List[Dict]:
        with self._lock:
            messages = copy.deepcopy(self._messages)
            self._messages = []
        return messages

    def put_message(self, message: Dict):
        with self._lock:
            self._messages.append(message)


class MessageHandler(BaseHTTPRequestHandler):
    _stub = None
    _postbox: PostBox

    def _read_content(self):
        content_length = int(self.headers['Content-Length'])
        bytes_content = self.rfile.read(content_length)
        return bytes_content.decode('ascii')

    # noinspection PyPep8Naming
    def do_POST(self):
        if self.path == '/sendMessage':
            response = self._send_message(self._read_content())
        elif self.path == '/getAndFlushMessages':
            response = self._get_messages()
        else:
            self.send_error(HTTPStatus.NOT_IMPLEMENTED)
            self.end_headers()
            return

        response_bytes = json.dumps(response).encode('ascii')
        self.send_response(HTTPStatus.OK)
        self.send_header('Content-Length', str(len(response_bytes)))
        self.end_headers()
        self.wfile.write(response_bytes)

    def _send_message(self, content: str) -> dict:
        json_request = json.loads(content)

        req = messenger_pb2.SendMessageRequest()
        google.protobuf.json_format.ParseDict(json_request, req)

        resp = self._stub.SendMessage(req)

        return {'sendTime': ts_to_str(resp.send_time)}

    def _get_messages(self) -> List[dict]:
        return self._postbox.collect_messages()


def main():
    grpc_server_address = os.environ.get('MESSENGER_SERVER_ADDR', 'localhost:51075')

    channel = grpc.insecure_channel(grpc_server_address)
    try:
        grpc.channel_ready_future(channel).result(timeout=15)
    except Exception:
        pass
    stub = messenger_pb2_grpc.MessengerServerStub(channel)

    postbox = PostBox()

    ready = threading.Event()

    def consume():
        while True:
            try:
                call = stub.ReadMessages(google.protobuf.empty_pb2.Empty())
                try:
                    call.initial_metadata()
                    ready.set()
                except Exception:
                    pass
                for msg in call:
                    postbox.put_message({'author': msg.author, 'text': msg.text, 'sendTime': ts_to_str(msg.send_time)})
            except grpc.RpcError:
                time.sleep(0.2)

    t = threading.Thread(target=consume, daemon=True)
    t.start()
    ready.wait(timeout=10)

    MessageHandler._stub = stub
    MessageHandler._postbox = postbox

    http_port = os.environ.get('MESSENGER_HTTP_PORT', '8080')
    http_server_address = ('0.0.0.0', int(http_port))

    httpd = HTTPServer(http_server_address, MessageHandler)
    httpd.serve_forever()


if __name__ == '__main__':
    main()
