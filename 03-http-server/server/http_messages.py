import dataclasses
import typing as t


@dataclasses.dataclass
class HTTPRequest:
    method: str
    path: str
    version: str
    parameters: t.Dict[str, str]
    headers: t.Dict[str, str]

    @staticmethod
    def from_bytes(data: bytes) -> "HTTPRequest":
        if b"\r\n\r\n" in data:
            head, _ = data.split(b"\r\n\r\n", 1)
        elif b"\n\n" in data:
            head, _ = data.split(b"\n\n", 1)
        else:
            head = data

        lines = head.splitlines()
        if not lines:
            raise ValueError("empty request")

        req_line = lines[0].decode("latin-1").strip()
        parts = req_line.split()
        if len(parts) != 3:
            raise ValueError("bad request line")

        method, target, version = parts
        if version.startswith("HTTP/"):
            version = version.split("/", 1)[1]

        target = target.split("#", 1)[0]
        if "?" in target:
            path, query = target.split("?", 1)
        else:
            path, query = target, ""

        if not path:
            path = "/"

        parameters: t.Dict[str, str] = {}
        if query:
            for chunk in query.split("&"):
                if not chunk:
                    continue
                if "=" in chunk:
                    k, v = chunk.split("=", 1)
                else:
                    k, v = chunk, ""
                parameters[k] = v

        headers: t.Dict[str, str] = {}
        for raw in lines[1:]:
            s = raw.decode("latin-1").strip()
            if not s:
                continue
            if ":" not in s:
                continue
            k, v = s.split(":", 1)
            headers[k.strip()] = v.strip()

        return HTTPRequest(
            method=method,
            path=path,
            version=version,
            parameters=parameters,
            headers=headers,
        )

    def to_bytes(self) -> bytes:
        query = ""
        if self.parameters:
            items = []
            for k, v in self.parameters.items():
                items.append(f"{k}={v}")
            query = "?" + "&".join(items)

        start = f"{self.method} {self.path}{query} HTTP/{self.version}".encode("latin-1") + CRLF
        hdrs = b"".join(
            (f"{k}: {v}".encode("latin-1") + CRLF) for k, v in self.headers.items()
        )
        return start + hdrs + CRLF


@dataclasses.dataclass
class HTTPResponse:
    version: str
    status: str
    headers: t.Dict[str, str]

    @staticmethod
    def from_bytes(data: bytes) -> "HTTPResponse":
        lines = data.splitlines()
        if not lines:
            raise ValueError("empty response")

        status_line = lines[0].decode("latin-1").strip()
        parts = status_line.split()
        if len(parts) < 2:
            raise ValueError("bad status line")

        ver = parts[0]
        if ver.startswith("HTTP/"):
            ver = ver.split("/", 1)[1]

        status = parts[1]
        headers: t.Dict[str, str] = {}
        for raw in lines[1:]:
            s = raw.decode("latin-1").strip()
            if not s:
                break
            if ":" not in s:
                continue
            k, v = s.split(":", 1)
            headers[k.strip()] = v.strip()

        return HTTPResponse(version=ver, status=status, headers=headers)

    def to_bytes(self) -> bytes:
        reason = HTTP_REASON_BY_STATUS.get(self.status, "")
        start = f"HTTP/{self.version} {self.status} {reason}".encode("latin-1") + CRLF
        hdrs = b"".join(
            (f"{k}: {v}".encode("latin-1") + CRLF) for k, v in self.headers.items()
        )
        return start + hdrs + CRLF


# Common HTTP strings and constants


CR = b'\r'
LF = b'\n'
CRLF = CR + LF

HTTP_VERSION = "1.1"

OPTIONS = 'OPTIONS'
GET = 'GET'
HEAD = 'HEAD'
POST = 'POST'
PUT = 'PUT'
DELETE = 'DELETE'

METHODS = [
    OPTIONS,
    GET,
    HEAD,
    POST,
    PUT,
    DELETE,
]

HEADER_HOST = "Host"
HEADER_CONTENT_LENGTH = "Content-Length"
HEADER_CONTENT_TYPE = "Content-Type"
HEADER_CONTENT_ENCODING = "Content-Encoding"
HEADER_ACCEPT_ENCODING = "Accept-Encoding"
HEADER_CREATE_DIRECTORY = "Create-Directory"
HEADER_SERVER = "Server"
HEADER_REMOVE_DIRECTORY = "Remove-Directory"

GZIP = "gzip"

TEXT_PLAIN = "text/plain"
APPLICATION_OCTET_STREAM = "application/octet-stream"
APPLICATION_GZIP = "application/gzip"

OK = "200"
BAD_REQUEST = "400"
NOT_FOUND = "404"
METHOD_NOT_ALLOWED = "405"
NOT_ACCEPTABLE = "406"
CONFLICT = "409"

HTTP_REASON_BY_STATUS = {
    "100": "Continue",
    "101": "Switching Protocols",
    "200": "OK",
    "201": "Created",
    "202": "Accepted",
    "203": "Non-Authoritative Information",
    "204": "No Content",
    "205": "Reset Content",
    "206": "Partial Content",
    "300": "Multiple Choices",
    "301": "Moved Permanently",
    "302": "Found",
    "303": "See Other",
    "304": "Not Modified",
    "305": "Use Proxy",
    "307": "Temporary Redirect",
    "400": "Bad Request",
    "401": "Unauthorized",
    "402": "Payment Required",
    "403": "Forbidden",
    "404": "Not Found",
    "405": "Method Not Allowed",
    "406": "Not Acceptable",
    "407": "Proxy Authentication Required",
    "408": "Request Time-out",
    "409": "Conflict",
    "410": "Gone",
    "411": "Length Required",
    "412": "Precondition Failed",
    "413": "Request Entity Too Large",
    "414": "Request-URI Too Large",
    "415": "Unsupported Media Type",
    "416": "Requested range not satisfiable",
    "417": "Expectation Failed",
    "500": "Internal Server Error",
    "501": "Not Implemented",
    "502": "Bad Gateway",
    "503": "Service Unavailable",
    "504": "Gateway Time-out",
    "505": "HTTP Version not supported",
}
