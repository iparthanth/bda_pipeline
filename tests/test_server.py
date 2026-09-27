"""PRV-3 server: demo mode signs the local browser in, --secure does not, and data always needs a valid token."""
from __future__ import annotations

import json
import threading
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer
from pathlib import Path

import pytest

from serving import server

ROLES = {
    "n" * 20: {"role": "national", "scope": "national", "bundle": "national", "district_pcode": None},
    "d" * 20: {"role": "district_dhaka", "scope": "district", "bundle": "district_BD3026", "district_pcode": "BD3026"},
}


@pytest.fixture
def serve(tmp_path, monkeypatch):
    for bundle in ("national", "district_BD3026"):
        (tmp_path / bundle).mkdir()
        (tmp_path / bundle / "meta.json").write_text(json.dumps({"bundle": bundle}))
    monkeypatch.setattr(server, "BUNDLES", tmp_path)
    running = []

    def start(demo: bool) -> str:
        handler = type("H", (server.Handler,), {"roles": ROLES, "demo": demo, "audit_file": Path(tmp_path / "audit.log")})
        httpd = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        threading.Thread(target=httpd.serve_forever, daemon=True).start()
        running.append(httpd)
        return f"http://127.0.0.1:{httpd.server_address[1]}"

    yield start
    for httpd in running:
        httpd.shutdown()


def get(url: str, token: str | None = None) -> tuple[int, object]:
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"} if token else {})
    try:
        with urllib.request.urlopen(req) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


def test_demo_mode_lists_roles_national_first(serve):
    base = serve(demo=True)
    code, roles = get(f"{base}/api/demo-roles")
    assert code == 200 and [r["role"] for r in roles] == ["national", "district_dhaka"]


def test_secure_mode_has_no_automatic_sign_in(serve):
    base = serve(demo=False)
    assert get(f"{base}/api/demo-roles")[0] == 404


@pytest.mark.parametrize("demo", [True, False])
def test_data_always_requires_a_valid_token_and_is_role_scoped(serve, demo):
    base = serve(demo=demo)
    assert get(f"{base}/api/data/meta.json")[0] == 401
    assert get(f"{base}/api/data/meta.json", "x" * 20)[0] == 401
    assert get(f"{base}/api/data/meta.json", "n" * 20) == (200, {"bundle": "national"})
    assert get(f"{base}/api/data/meta.json", "d" * 20) == (200, {"bundle": "district_BD3026"})
