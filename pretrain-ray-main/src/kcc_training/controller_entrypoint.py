"""Controller process with Kubernetes-compatible health endpoints."""

from __future__ import annotations

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import os
from threading import Thread
from typing import Callable, Sequence

from .controller import main as controller_main


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path not in {"/healthz", "/readyz"}:
            self.send_response(404)
            self.end_headers()
            return
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(b"ok\n")

    def log_message(self, format: str, *args: object) -> None:
        del format, args


def main(
    argv: Sequence[str] | None = None,
    *,
    controller: Callable[[Sequence[str] | None], int] = controller_main,
) -> int:
    port = int(os.environ.get("HEALTH_PORT", "8080"))
    server = ThreadingHTTPServer(("0.0.0.0", port), _Handler)
    Thread(target=server.serve_forever, name="health-server", daemon=True).start()
    try:
        return controller(argv)
    finally:
        server.shutdown()


if __name__ == "__main__":
    raise SystemExit(main())

