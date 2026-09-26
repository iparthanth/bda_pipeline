"""Minimal DAG orchestrator with retries (ENG-1, ENG-4).

    python -m orchestration.dag all                 # full rebuild: public data -> generator -> lake -> analytics -> dashboard
    python -m orchestration.dag drop --source DIR   # ingest an incremental drop (e.g. the hidden 14-day test drop)
    python -m orchestration.dag analytics           # re-run analytics + export on the current lake

Each task runs in its own Python process (a fresh JVM per Spark stage keeps memory bounded on a 16 GB
laptop). Tasks declare upstream dependencies; a failed task is retried with exponential back-off before the
run stops. Every run writes ``data/runs/dag_<timestamp>.json`` with per-task start, end, duration, attempts
and exit code - the scalability experiment (ENG-2) reads these timings.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import platform
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

from pipeline.common.config import REPO_ROOT, load_config, path


@dataclass
class Task:
    name: str
    cmd: list[str]
    deps: list[str] = field(default_factory=list)
    retries: int = 2


def dags(args: argparse.Namespace) -> dict[str, list[Task]]:
    py = [sys.executable, "-m"]
    sf = ["--sf", str(args.sf)] if args.sf is not None else []
    return {
        "all": [
            Task("env", py + ["orchestration.init_env"], retries=0),
            Task("fetch_public", py + ["generator.fetch_public"] + (["--offline"] if args.offline else []), ["env"]),
            Task("generate", py + ["generator.generate"] + sf, ["fetch_public"], retries=1),
            Task("load_history", py + ["pipeline.run_load"], ["generate"]),
            Task("analytics", py + ["analytics.run_analytics"], ["load_history"]),
            Task("export", py + ["serving.export"], ["analytics"]),
        ],
        "drop": [
            Task("env", py + ["orchestration.init_env"], retries=0),
            Task("load_drop", py + ["pipeline.run_load", "--source", str(args.source), "--no-public"], ["env"]),
            Task("analytics", py + ["analytics.run_analytics"], ["load_drop"]),
            Task("export", py + ["serving.export"], ["analytics"]),
        ],
        "analytics": [
            Task("analytics", py + ["analytics.run_analytics"]),
            Task("export", py + ["serving.export"], ["analytics"]),
        ],
    }


def topo(tasks: list[Task]) -> list[Task]:
    done, order, pending = set(), [], list(tasks)
    while pending:
        ready = [t for t in pending if set(t.deps) <= done]
        if not ready:
            raise ValueError(f"cycle or missing dependency among {[t.name for t in pending]}")
        for t in ready:
            order.append(t)
            done.add(t.name)
            pending.remove(t)
    return order


def run_task(t: Task, log_dir: Path) -> dict:
    rec = {"task": t.name, "cmd": " ".join(t.cmd[1:]), "attempts": 0}
    delay = 5.0
    log = log_dir / f"{t.name}.log"
    while True:
        rec["attempts"] += 1
        start = time.time()
        rec["start"] = dt.datetime.now().isoformat(timespec="seconds")
        print(f"[dag] {rec['start']} START {t.name} (attempt {rec['attempts']})", flush=True)
        with open(log, "a") as fh:
            proc = subprocess.run(t.cmd, cwd=REPO_ROOT, stdout=fh, stderr=subprocess.STDOUT, env=os.environ.copy())
        rec.update({"end": dt.datetime.now().isoformat(timespec="seconds"), "seconds": round(time.time() - start, 1),
                    "returncode": proc.returncode})
        status = "OK" if proc.returncode == 0 else "FAILED"
        print(f"[dag] {rec['end']} {status:6s} {t.name} in {rec['seconds']}s (log: {log.relative_to(REPO_ROOT) if log.is_relative_to(REPO_ROOT) else log})", flush=True)
        if proc.returncode == 0 or rec["attempts"] > t.retries:
            return rec
        print(f"[dag] retrying {t.name} in {delay:.0f}s", flush=True)
        time.sleep(delay)
        delay *= 2


def hardware() -> dict:
    mem = None
    try:
        with open("/proc/meminfo") as fh:
            mem = round(int(fh.readline().split()[1]) / 1e6, 1)
    except OSError:
        pass
    return {"platform": platform.platform(), "python": platform.python_version(), "cpus": os.cpu_count(), "mem_gb": mem,
            "spark_master": load_config()["spark"]["master"], "driver_memory": load_config()["spark"]["driver_memory"]}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("dag", choices=["all", "drop", "analytics"])
    ap.add_argument("--source", help="drop directory for the 'drop' DAG")
    ap.add_argument("--sf", type=float, default=None, help="scale factor for the generator (default: config)")
    ap.add_argument("--offline", action="store_true", help="do not download public data (use cache/reference/fallback)")
    args = ap.parse_args(argv)
    if args.dag == "drop" and not args.source:
        ap.error("--source is required for the drop DAG")
    stamp = dt.datetime.now().strftime("%Y%m%dT%H%M%S")
    log_dir = Path(path("runs")) / f"dag_{args.dag}_{stamp}"
    log_dir.mkdir(parents=True, exist_ok=True)
    summary = {"dag": args.dag, "started": stamp, "scale_factor": args.sf or load_config()["generator"]["scale_factor"],
               "hardware": hardware(), "tasks": []}
    t0 = time.time()
    ok = True
    for t in topo(dags(args)[args.dag]):
        rec = run_task(t, log_dir)
        summary["tasks"].append(rec)
        if rec["returncode"] != 0:
            ok = False
            print(f"[dag] stopping: {t.name} failed after {rec['attempts']} attempt(s); see {log_dir / (t.name + '.log')}", flush=True)
            break
    summary["seconds"] = round(time.time() - t0, 1)
    summary["status"] = "success" if ok else "failed"
    out = Path(path("runs")) / f"dag_{args.dag}_{stamp}.json"
    out.write_text(json.dumps(summary, indent=2))
    print(f"[dag] {summary['status']} in {summary['seconds']}s -> {out}", flush=True)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
