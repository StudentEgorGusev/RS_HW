import datetime
import gzip
import logging
import mimetypes
import os
import pathlib
import posixpath
import shutil
from dataclasses import dataclass
from socketserver import StreamRequestHandler
import typing as t
import click
import socket
import stat as statmod
import sys

from http_messages import (
    HTTP_VERSION,
    METHODS,
    GET,
    POST,
    PUT,
    DELETE,
    HEADER_HOST,
    HEADER_CONTENT_LENGTH,
    HEADER_CONTENT_TYPE,
    HEADER_CONTENT_ENCODING,
    HEADER_ACCEPT_ENCODING,
    HEADER_CREATE_DIRECTORY,
    HEADER_REMOVE_DIRECTORY,
    HEADER_SERVER,
    GZIP,
    TEXT_PLAIN,
    APPLICATION_OCTET_STREAM,
    OK,
    BAD_REQUEST,
    NOT_FOUND,
    METHOD_NOT_ALLOWED,
    NOT_ACCEPTABLE,
    CONFLICT,
    HTTP_REASON_BY_STATUS,
)

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

SERVER_NAME = "hse-http-file-server"


@dataclass
class HTTPServer:
    server_address: t.Tuple[str, int]
    socket: socket.socket
    server_domain: str
    working_directory: pathlib.Path


def _lower_headers(headers: t.Dict[str, str]) -> t.Dict[str, str]:
    return {k.strip().lower(): v.strip() for k, v in headers.items()}


def _parse_bool(v: str) -> bool:
    return v.strip().lower() == "true"


def _host_only(host_header: str) -> str:
    host_header = host_header.strip()
    if not host_header:
        return ""
    if ":" in host_header:
        return host_header.split(":", 1)[0]
    return host_header


def _wants_gzip(headers_lc: t.Dict[str, str]) -> bool:
    ae = headers_lc.get(HEADER_ACCEPT_ENCODING.lower(), "")
    if not ae:
        return False
    parts = [p.strip().lower() for p in ae.split(",")]
    return GZIP.lower() in parts


def _safe_resolve(root: pathlib.Path, url_path: str) -> pathlib.Path:
    p = url_path.split("?", 1)[0].split("#", 1)[0]
    if not p.startswith("/"):
        raise ValueError("bad path")

    rel = p.lstrip("/")
    rel = posixpath.normpath(rel)
    if rel == ".":
        rel = ""
    if rel == ".." or rel.startswith("../"):
        raise ValueError("bad path")

    root_resolved = root.resolve()
    target = (root_resolved / rel).resolve()
    if not target.is_relative_to(root_resolved):
        raise ValueError("bad path")

    return target


def _guess_content_type(p: pathlib.Path) -> str:
    tpe, _ = mimetypes.guess_type(str(p))
    if not tpe:
        return APPLICATION_OCTET_STREAM
    return tpe


def _format_dir_listing(dir_path: pathlib.Path) -> str:
    lines: list[str] = []
    entries = sorted(dir_path.iterdir(), key=lambda x: x.name)

    for entry in entries:
        st = entry.lstat()
        perm = statmod.filemode(st.st_mode)
        user = str(st.st_uid)
        group = str(st.st_gid)
        size = st.st_size
        sec = st.st_mtime_ns // 1_000_000_000
        dt = datetime.datetime.fromtimestamp(sec).strftime("%Y-%m-%d %H:%M:%S")
        lines.append(f"{perm} {user} {group} {size} {dt} {entry.name}")

    if not lines:
        return ""
    return "\n".join(lines) + "\n"


class HTTPHandler(StreamRequestHandler):
    server: HTTPServer

    # Use self.rfile and self.wfile to interact with the client
    # Access domain and working directory with self.server.{attr}
    def handle(self) -> None:
        first_line = self.rfile.readline()
        logger.info(f"Handle connection from {self.client_address}, first_line {first_line}")

        if not first_line:
            return

        req_line = first_line.decode("latin-1").strip("\r\n")
        parts = req_line.split()
        if len(parts) != 3:
            self._send_bytes(BAD_REQUEST, b"Bad request\n")
            return

        method, target, httpver = parts
        if method not in METHODS:
            headers = self._read_headers()
            headers_lc = _lower_headers(headers)
            cl = int(headers_lc.get(HEADER_CONTENT_LENGTH.lower(), "0") or "0")
            if cl > 0:
                self._discard_body(cl)
            self._send_bytes(METHOD_NOT_ALLOWED, b"Method not allowed\n")
            return

        if not httpver.startswith("HTTP/"):
            headers = self._read_headers()
            headers_lc = _lower_headers(headers)
            cl = int(headers_lc.get(HEADER_CONTENT_LENGTH.lower(), "0") or "0")
            if cl > 0:
                self._discard_body(cl)
            self._send_bytes(BAD_REQUEST, b"Bad request\n")
            return

        ver = httpver.split("/", 1)[1]
        if ver != HTTP_VERSION:
            headers = self._read_headers()
            headers_lc = _lower_headers(headers)
            cl = int(headers_lc.get(HEADER_CONTENT_LENGTH.lower(), "0") or "0")
            if cl > 0:
                self._discard_body(cl)
            self._send_bytes(BAD_REQUEST, b"Bad request\n")
            return

        headers = self._read_headers()
        headers_lc = _lower_headers(headers)
        content_length = int(headers_lc.get(HEADER_CONTENT_LENGTH.lower(), "0") or "0")

        host_header = headers_lc.get(HEADER_HOST.lower(), "")
        if _host_only(host_header) != self.server.server_domain:
            if content_length > 0:
                self._discard_body(content_length)
            self._send_bytes(BAD_REQUEST, b"Bad request\n")
            return

        try:
            fs_path = _safe_resolve(self.server.working_directory, target)
        except Exception:
            if content_length > 0:
                self._discard_body(content_length)
            self._send_bytes(NOT_FOUND, b"Not found\n")
            return

        want_gzip = _wants_gzip(headers_lc)

        if method == GET:
            self._handle_get(fs_path, want_gzip)
            return
        if method == POST:
            self._handle_post(fs_path, headers_lc, content_length)
            return
        if method == PUT:
            self._handle_put(fs_path, content_length)
            return
        if method == DELETE:
            self._handle_delete(fs_path, headers_lc)
            return

        if content_length > 0:
            self._discard_body(content_length)
        self._send_bytes(METHOD_NOT_ALLOWED, b"Method not allowed\n")

    def _read_headers(self) -> t.Dict[str, str]:
        headers: t.Dict[str, str] = {}
        while True:
            line = self.rfile.readline()
            if not line:
                break
            if line in (b"\r\n", b"\n"):
                break
            s = line.decode("latin-1").rstrip("\r\n")
            if ":" not in s:
                continue
            k, v = s.split(":", 1)
            headers[k.strip()] = v.strip()
        return headers

    def _read_exact_to(self, n: int, out) -> None:
        remaining = n
        while remaining > 0:
            chunk = self.rfile.read(min(64 * 1024, remaining))
            if not chunk:
                break
            out.write(chunk)
            remaining -= len(chunk)

    def _discard_body(self, n: int) -> None:
        remaining = n
        while remaining > 0:
            chunk = self.rfile.read(min(64 * 1024, remaining))
            if not chunk:
                break
            remaining -= len(chunk)

    def _send_response_head(self, status: str, headers: t.Dict[str, str]) -> None:
        reason = HTTP_REASON_BY_STATUS.get(status, "")
        status_line = f"HTTP/{HTTP_VERSION} {status} {reason}\r\n"
        self.wfile.write(status_line.encode("latin-1"))

        headers_out = dict(headers)
        headers_out[HEADER_SERVER] = SERVER_NAME
        headers_out["Connection"] = "close"

        for k, v in headers_out.items():
            self.wfile.write(f"{k}: {v}\r\n".encode("latin-1"))
        self.wfile.write(b"\r\n")

    def _send_bytes(self, status: str, body: bytes, content_type: str = TEXT_PLAIN, extra_headers: t.Optional[t.Dict[str, str]] = None) -> None:
        hdrs = {
            HEADER_CONTENT_TYPE: content_type,
            HEADER_CONTENT_LENGTH: str(len(body)),
        }
        if extra_headers:
            hdrs.update(extra_headers)
        self._send_response_head(status, hdrs)
        if body:
            self.wfile.write(body)
        self.wfile.flush()

    def _handle_get(self, fs_path: pathlib.Path, want_gzip: bool) -> None:
        if not fs_path.exists():
            self._send_bytes(NOT_FOUND, b"Not found\n")
            return

        if fs_path.is_dir():
            listing = _format_dir_listing(fs_path).encode("utf-8", "replace")
            if want_gzip and listing:
                self._send_response_head(
                    OK,
                    {
                        HEADER_CONTENT_TYPE: TEXT_PLAIN,
                        HEADER_CONTENT_ENCODING: GZIP,
                    },
                )
                gz = gzip.GzipFile(fileobj=self.wfile, mode="wb")
                try:
                    gz.write(listing)
                finally:
                    gz.close()
                self.wfile.flush()
                return

            self._send_bytes(OK, listing, content_type=TEXT_PLAIN)
            return

        content_type = _guess_content_type(fs_path)

        if want_gzip:
            self._send_response_head(
                OK,
                {
                    HEADER_CONTENT_TYPE: content_type,
                    HEADER_CONTENT_ENCODING: GZIP,
                },
            )
            with open(fs_path, "rb") as f:
                gz = gzip.GzipFile(fileobj=self.wfile, mode="wb")
                try:
                    shutil.copyfileobj(f, gz, length=64 * 1024)
                finally:
                    gz.close()
            self.wfile.flush()
            return

        size = fs_path.stat().st_size
        self._send_response_head(
            OK,
            {
                HEADER_CONTENT_TYPE: content_type,
                HEADER_CONTENT_LENGTH: str(size),
            },
        )
        with open(fs_path, "rb") as f:
            shutil.copyfileobj(f, self.wfile, length=64 * 1024)
        self.wfile.flush()

    def _handle_post(self, fs_path: pathlib.Path, headers_lc: t.Dict[str, str], content_length: int) -> None:
        if fs_path.exists():
            if content_length > 0:
                self._discard_body(content_length)
            self._send_bytes(CONFLICT, b"Conflict\n")
            return

        parent = fs_path.parent
        if not parent.exists() or not parent.is_dir():
            if content_length > 0:
                self._discard_body(content_length)
            self._send_bytes(NOT_FOUND, b"Not found\n")
            return

        create_dir = _parse_bool(headers_lc.get(HEADER_CREATE_DIRECTORY.lower(), "False"))

        if create_dir:
            try:
                fs_path.mkdir(mode=0o777)
            except FileExistsError:
                self._send_bytes(CONFLICT, b"Conflict\n")
                return
            self._send_bytes(OK, b"")
            return

        try:
            with open(fs_path, "xb") as f:
                if content_length > 0:
                    self._read_exact_to(content_length, f)
        except FileExistsError:
            if content_length > 0:
                self._discard_body(content_length)
            self._send_bytes(CONFLICT, b"Conflict\n")
            return

        self._send_bytes(OK, b"")

    def _handle_put(self, fs_path: pathlib.Path, content_length: int) -> None:
        if not fs_path.exists():
            if content_length > 0:
                self._discard_body(content_length)
            self._send_bytes(NOT_FOUND, b"Not found\n")
            return

        if fs_path.is_dir():
            if content_length > 0:
                self._discard_body(content_length)
            self._send_bytes(CONFLICT, b"Conflict\n")
            return

        with open(fs_path, "wb") as f:
            if content_length > 0:
                self._read_exact_to(content_length, f)

        self._send_bytes(OK, b"")

    def _handle_delete(self, fs_path: pathlib.Path, headers_lc: t.Dict[str, str]) -> None:
        if not fs_path.exists():
            self._send_bytes(NOT_FOUND, b"Not found\n")
            return

        if fs_path.is_dir():
            remove_dir = _parse_bool(headers_lc.get(HEADER_REMOVE_DIRECTORY.lower(), "False"))
            if not remove_dir:
                self._send_bytes(NOT_ACCEPTABLE, b"Not acceptable\n")
                return
            shutil.rmtree(fs_path)
            self._send_bytes(OK, b"")
            return

        fs_path.unlink()
        self._send_bytes(OK, b"")


@click.command()
@click.option("--host", type=str)
@click.option("--port", type=int)
@click.option("--server-domain", type=str)
@click.option("--working-directory", type=str)
def main(host, port, server_domain, working_directory):
    if host is None:
        host = os.environ.get("SERVER_HOST", "0.0.0.0")

    if port is None:
        p = os.environ.get("SERVER_PORT", "")
        port = int(p) if p else 8080

    if server_domain is None:
        server_domain = os.environ.get("SERVER_DOMAIN", "localhost")

    if working_directory is None:
        working_directory = os.environ.get("SERVER_WORKING_DIRECTORY", "")

    if not working_directory:
        sys.exit(1)

    working_directory_path = pathlib.Path(working_directory)

    logger.info(
        f"Starting server on {host}:{port}, domain {server_domain}, working directory {working_directory}"
    )

    # Create a server socket
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # Set SO_REUSEADDR option
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    # Bind the socket object to the address and port
    s.bind((host, port))
    # Start listening for incoming connections
    s.listen()

    logger.info(f"Listening at {s.getsockname()}")
    server = HTTPServer((host, port), s, server_domain, working_directory_path)

    while True:
        # Accept any new connection (request, client_address)
        try:
            conn, addr = s.accept()
        except OSError:
            break

        try:
            # Handle the request
            HTTPHandler(conn, addr, server)

            # Close the connection
            conn.shutdown(socket.SHUT_WR)
            conn.close()
        except Exception as e:
            logger.error(e)
            conn.close()


if __name__ == "__main__":
    main()
