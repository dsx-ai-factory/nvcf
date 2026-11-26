#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""CPU-only session lifecycle fixture; End replies successfully then exits Kit-style."""

import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
import threading
import time


class Sessions:
    def __init__(self, warmup=30):
        self.lock = threading.Lock()
        self.ready_at = time.monotonic() + warmup
        self.session = None
        self.ending = False

    def apply(self, action, session=None, exit_code=0):
        with self.lock:
            if action == "status":
                return {"state": "ending" if self.ending else "running" if self.session else "idle",
                        "session": self.session}
            if action == "resume":
                if self.ending or not isinstance(session, str) or not session or self.session:
                    raise ValueError("resume requires an idle process and a session ID")
                self.session = session
                return {"state": "running", "session": session}
            if action == "end":
                if self.ending or not self.session or session != self.session or exit_code not in (0, 1):
                    raise ValueError("end requires the active session and exit code zero or one")
                self.ending = True
                return {"state": "ended", "session": self.session, "exitCode": exit_code}
            raise ValueError("unsupported session action")


def log(event, **fields):
    print(json.dumps({"time": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                      "event": event, **fields}), flush=True)


def main():
    # Span two real utils readiness probes so cached Ready cannot hide startup.
    sessions = Sessions(float(os.environ.get("FAKE_KIT_WARMUP_SECONDS", "30")))

    class Handler(BaseHTTPRequestHandler):
        def reply(self, status, body):
            data = json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
            self.wfile.flush()

        def do_GET(self):
            ready = time.monotonic() >= sessions.ready_at and not sessions.ending
            self.reply(200 if ready else 503, {"ready": ready})

        def do_POST(self):
            try:
                size = int(self.headers.get("Content-Length", "0"))
                if not 0 < size <= 4096:
                    raise ValueError("invalid request size")
                request = json.loads(self.rfile.read(size))
                response = sessions.apply(request.get("action"), request.get("session"),
                                          request.get("exitCode", 0))
            except (ValueError, TypeError, AttributeError):
                self.reply(400, {"error": "invalid session request"})
                return
            log(request["action"], **response)
            self.reply(200, response)
            if request["action"] == "end":
                # Let the real utils proxy finish forwarding the successful reply.
                def finish():
                    log("exit", session=response["session"], exitCode=response["exitCode"])
                    os._exit(response["exitCode"])
                threading.Timer(1, finish).start()

        def log_message(self, _format, *_args):
            pass

    server = ThreadingHTTPServer(("0.0.0.0", int(os.environ.get("PORT", "8000"))), Handler)
    log("boot", pid=os.getpid(), hostname=os.environ.get("HOSTNAME"), port=server.server_address[1])
    server.serve_forever()


if __name__ == "__main__":
    main()
