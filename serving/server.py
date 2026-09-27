"""EpiScope dashboard server - one command: ``python -m serving.server`` (or ``make serve``).

* Serves the static D3.js dashboard from ``dashboard/`` and the role-scoped JSON bundles from
  ``serving/out/<bundle>/`` (standard library only, no extra dependencies).
* PRV-3 role-based views: every data request needs a bearer token (``Authorization: Bearer <token>`` or
  ``?token=``). Tokens are read from environment variables named in ``config/roles.yaml`` (values live in
  the git-ignored ``.env``; ``make env`` creates random ones). A national token receives the national
  bundle; a district token receives only that district's bundle (own district in detail, others aggregated).
* Demo mode (default, for marking and the video): the server creates ``.env`` if it is missing, opens the
  browser and the dashboard signs in as the national role automatically. A "View as" switcher (served by
  ``/api/demo-roles``) lets the viewer change to a district role to see the PRV-3 restriction in action.
  The role checks on ``/api/data/`` are unchanged - demo mode only hands the local browser its tokens.
  ``--secure`` turns demo mode off (no auto sign-in, token required), as a real deployment would run.
* Every data access is appended to ``data/runs/access_audit.log`` (time, role, file, client address).
* Binds to 127.0.0.1 by default.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hmac
import json
import os
import threading
import webbrowser
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


def demo_roles(roles: dict[str, dict]) -> list[dict]:
    """Roles offered by the "View as" switcher in demo mode, national first."""
    out = [{"token": tok, "role": r["role"], "scope": r["scope"], "district_pcode": r["district_pcode"]}
           for tok, r in roles.items()]
    return sorted(out, key=lambda r: (r["scope"] != "national", r["role"]))


class Handler(SimpleHTTPRequestHandler):
    roles: dict[str, dict] = {}
    audit_file: Path
    demo: bool = True

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
        if url.path == "/api/demo-roles":
            self._audit("-", "demo-roles", 200 if self.demo else 404)
            return self._json(200, demo_roles(self.roles)) if self.demo else self._json(404, {"error": "demo mode off"})
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
    ap.add_argument("--secure", action="store_true",
                    help="no automatic sign-in: every viewer must paste a role token (deployment mode)")
    ap.add_argument("--no-browser", action="store_true", help="do not open a browser window")
    args = ap.parse_args(argv)
    Handler.roles = load_roles()
    if not Handler.roles:  # first run from a fresh clone: create .env with a random salt and role tokens
        from orchestration import init_env
        from pipeline.common import config
        init_env.main()
        config._load_dotenv()
        Handler.roles = load_roles()
    Handler.demo = not args.secure
    Handler.audit_file = Path(path("runs")) / "access_audit.log"
    Handler.audit_file.parent.mkdir(parents=True, exist_ok=True)
    if not (BUNDLES / "national").exists():
        raise SystemExit("No exported data in serving/out - run `make all` (or `python -m serving.export`).")
    local = args.host if args.host not in ("0.0.0.0", "::") else "localhost"
    url = f"http://{local}:{args.port}/"
    print(f"\nEpiScope dashboard is running:  {url}", flush=True)
    if Handler.demo:
        print("  Open the link above - it signs in as the national role automatically.", flush=True)
        print("  Use 'View as' (top right) to switch to a district officer's restricted view (PRV-3).", flush=True)
    else:
        print("  Secure mode: sign in with one of these role links:", flush=True)
        for tok, r in Handler.roles.items():
            print(f"  {r['role']:22s} -> {url}#token={tok}", flush=True)
    print("  Press Ctrl+C to stop.\n", flush=True)
    server = ThreadingHTTPServer((args.host, args.port), Handler)
    if not args.no_browser and args.host in ("127.0.0.1", "localhost") and not os.environ.get("EPISCOPE_NO_BROWSER"):
        threading.Timer(1.0, lambda: webbrowser.open(url)).start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("stopped.")


if __name__ == "__main__":
    main()
