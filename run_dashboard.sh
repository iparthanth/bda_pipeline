#!/usr/bin/env bash
# One command to see the EpiScope dashboard (macOS / Linux / Git Bash on Windows):  ./run_dashboard.sh
# Creates .venv if needed, installs the one package the server needs (PyYAML), creates .env with random
# tokens on the first run, starts the server and opens the dashboard signed in as the national role.
# The dashboard data (serving/out/) is committed; `make all` or `docker compose up` rebuilds it from scratch.
set -e
cd "$(dirname "$0")"
PY=""
for c in python3.11 python3 py python; do
  if command -v "$c" >/dev/null 2>&1 && "$c" -c "import sys; assert sys.version_info >= (3, 9)" >/dev/null 2>&1; then PY="$c"; break; fi
done
[ -n "$PY" ] || { echo "Python 3.9+ not found - install it from https://www.python.org/downloads/"; exit 1; }
[ -d .venv ] || "$PY" -m venv .venv
if [ -x .venv/bin/python ]; then VPY=.venv/bin/python; else VPY=.venv/Scripts/python; fi
"$VPY" -m pip install -q --disable-pip-version-check pyyaml
exec "$VPY" -u -m serving.server "$@"
