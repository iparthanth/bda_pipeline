@echo off
REM One click to see the EpiScope dashboard on Windows: double-click this file (or run run_dashboard.bat).
REM Creates .venv if needed, installs PyYAML, creates .env with random tokens on the first run, starts the
REM server and opens the dashboard in your browser signed in as the national role. Close this window to stop.
cd /d "%~dp0"
if not exist ".venv\Scripts\python.exe" (
  where py >nul 2>nul && (py -3 -m venv .venv) || (python -m venv .venv)
)
if not exist ".venv\Scripts\python.exe" (
  echo Python 3.9+ was not found. Install it from https://www.python.org/downloads/ and tick "Add python.exe to PATH".
  pause
  exit /b 1
)
".venv\Scripts\python.exe" -m pip install -q --disable-pip-version-check pyyaml
".venv\Scripts\python.exe" -u -m serving.server %*
pause
