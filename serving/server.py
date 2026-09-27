"""EpiScope dashboard server - one command: ``python -m serving.server`` (or ``make serve``).

* Serves the static D3.js dashboard from ``dashboard/`` and the role-scoped JSON bundles from
  ``serving/out/<bundle>/`` (standard library only, no extra dependencies).
* PRV-3 role-based views: every data request needs a bearer token (``Authorization: Bearer <token>`` or
  ``?token=``). Tokens are read from environment variables named in ``config/roles.yaml`` (values live in
  the git-ignored ``.env``; ``make env`` creates random ones). A national token receives the national
  bundle; a district token receives only that district's bundle (own district in detail, others aggregated).
* Every data access is appended to ``data/runs/access_audit.log`` (time, role, file, client address).
* Binds to 127.0.0.1 by default.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hmac
import json
import os
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import yaml

from pipeline.common.config import REPO_ROOT, load_config, path

DASHBOARD = REPO_ROOT / "dashboard"
BUNDLES = Path(path("serving_out"))


def load_roles() -> dict[str, dict]:
    cfg = load_config()
    roles = yaml.safe_load(open(REPO_ROOT / cfg["serving"]["roles_file"]))["roles"]
    out = {}
    for name, r in roles.items():
        tok = os.environ.get(r["token_env"], "")
        if len(tok) >= 16:
            bundle = "national" if r["scope"] == "national" else f"district_{r['district_pcode']}"
            out[tok] = {"role": name, "scope": r["scope"], "bundle": bundle, "district_pcode": r.get("district_pcode")}
    return out


class Handler(SimpleHTTPRequestHandler):
    roles: dict[str, dict] = {}
    audit_file: Path

    def __init__(self, *a, **kw):
        super().__init__(*a, directory=str(DASHBOARD), **kw)

    def log_message(self, fmt, *args):  # quieter console
        pass

    def _role(self) -> dict | None:
        tok = ""
        auth = self.headers.get("Authorization", "")
        if auth.startswith("Bearer "):
            tok = auth[7:].strip()
        else:
            tok = parse_qs(urlparse(self.path).query).get("token", [""])[0]
        for known, role in self.roles.items():
            if hmac.compare_digest(known, tok):
                return role
        return None

    def _json(self, code: int, obj) -> None:
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _audit(self, role: str, what: str, status: int) -> None:
        with open(self.audit_file, "a") as fh:
            fh.write(f"{dt.datetime.now(dt.timezone.utc).isoformat(timespec='seconds')}\t{role}\t{what}\t{status}\t{self.client_address[0]}\n")

    def do_GET(self):
        url = urlparse(self.path)
        if url.path == "/api/session":
            role = self._role()
            self._audit(role["role"] if role else "-", "session", 200 if role else 401)
            return self._json(200, role) if role else self._json(401, {"error": "invalid or missing token"})
        if url.path.startswith("/api/data/"):
            role = self._role()
            name = Path(url.path).name
            if role is None:
                self._audit("-", name, 401)
                return self._json(401, {"error": "invalid or missing token"})
            f = BUNDLES / role["bundle"] / name
            if not name.endswith(".json") or not f.is_file() or ".." in url.path:
                self._audit(role["role"], name, 404)
                return self._json(404, {"error": "not found"})
            self._audit(role["role"], name, 200)
            body = f.read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        return super().do_GET()


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description="Serve the EpiScope dashboard")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=int(os.environ.get("EPISCOPE_PORT", 8765)))
    args = ap.parse_args(argv)
    Handler.roles = load_roles()
    Handler.audit_file = Path(path("runs")) / "access_audit.log"
    Handler.audit_file.parent.mkdir(parents=True, exist_ok=True)
    if not Handler.roles:
        raise SystemExit("No role tokens configured - run `make env` first (creates .env with random tokens).")
    if not (BUNDLES / "national").exists():
        raise SystemExit("No exported data in serving/out - run `make all` (or `python -m serving.export`).")
    print(f"EpiScope dashboard: http://{args.host}:{args.port}/", flush=True)
    for tok, r in Handler.roles.items():
        print(f"  {r['role']:22s} -> http://{args.host}:{args.port}/#token={tok}", flush=True)
    ThreadingHTTPServer((args.host, args.port), Handler).serve_forever()


if __name__ == "__main__":
    main()
