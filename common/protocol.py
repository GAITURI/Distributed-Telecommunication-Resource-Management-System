"""
TeleRM wire protocol.

Every message is a JSON object preceded by a 4-byte big-endian length
prefix (design doc section 6: "4-byte length plus JSON over TCP. Each
exchange is one request and one reply.").
"""
import json
import socket
import struct

_HEADER = struct.Struct(">I")


def send_msg(sock: socket.socket, obj: dict) -> int:
    """Send one length-prefixed JSON message. Returns bytes written
    (header + payload), for message/byte counters."""
    payload = json.dumps(obj).encode("utf-8")
    sock.sendall(_HEADER.pack(len(payload)) + payload)
    return len(payload) + _HEADER.size


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("peer closed connection mid-message")
        buf.extend(chunk)
    return bytes(buf)


def recv_msg(sock: socket.socket):
    """Read one length-prefixed JSON message. Returns (obj, bytes_read)."""
    header = _recv_exact(sock, _HEADER.size)
    (length,) = _HEADER.unpack(header)
    payload = _recv_exact(sock, length)
    return json.loads(payload.decode("utf-8")), length + _HEADER.size


def request(host: str, port: int, obj: dict, timeout: float = 5.0, counters=None):
    """Open a connection, send one message, read the one reply, close.
    This is the request/reply pattern used for every exchange in the
    protocol table (REGISTER, HEARTBEAT, REQUEST, RESIZE, ALLOCATE, ...).
    """
    with socket.create_connection((host, port), timeout=timeout) as sock:
        sock.settimeout(timeout)
        sent = send_msg(sock, obj)
        if counters is not None:
            counters.sent(sent)
        reply, recvd = recv_msg(sock)
        if counters is not None:
            counters.recv(recvd)
        return reply


def serve_forever(host: str, port: int, handler, counters=None, stop_flag=None):
    """Generic request/reply TCP server. `handler(msg) -> reply_dict`.
    Each accepted connection handles exactly one request/reply exchange,
    matching the protocol's request/reply semantics. Returns the bound
    server socket so the caller can close it to stop serving.
    """
    import threading

    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((host, port))
    srv.listen(64)

    def _client(conn):
        try:
            with conn:
                conn.settimeout(30)
                msg, recvd = recv_msg(conn)
                if counters is not None:
                    counters.recv(recvd)
                reply = handler(msg)
                sent = send_msg(conn, reply)
                if counters is not None:
                    counters.sent(sent)
        except (ConnectionError, socket.timeout, OSError):
            pass

    def _accept_loop():
        while True:
            try:
                conn, _addr = srv.accept()
            except OSError:
                break  # socket was closed -> shut down
            threading.Thread(target=_client, args=(conn,), daemon=True).start()

    t = threading.Thread(target=_accept_loop, daemon=True)
    t.start()
    return srv
