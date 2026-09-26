"""Scalability experiment (ENG-2): the full pipeline at SF in {0.25, 0.5, 1}.

    python -m orchestration.scale_experiment [--root /big/disk/episcope_scale] [--sf 0.25 0.5 1]

Each scale factor is built from scratch in its own data root (EPISCOPE_DATA_ROOT) with the normal
``orchestration.dag all`` command, so the timings are exactly what ``make all`` does. Per-stage run times
come from the DAG run record; output sizes are measured on disk; the hardware is recorded. Results go to
``docs/experiments/scaling.json`` and ``docs/experiments/scaling.csv`` (read by the report).
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

from orchestration.dag import hardware
from pipeline.common.config import REPO_ROOT

OUT = REPO_ROOT / "docs" / "experiments"


def du(p: Path) -> int:
    return sum(f.stat().st_size for f in p.rglob("*") if f.is_file()) if p.exists() else 0


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--root", default=str(REPO_ROOT / "data" / "scale"))
    ap.add_argument("--sf", nargs="+", type=float, default=[0.25, 0.5, 1.0])
    ap.add_argument("--keep", action="store_true", help="keep the lakes afterwards (they are large)")
    ap.add_argument("--existing", nargs="*", default=[], metavar="SF=ROOT",
                    help="reuse a finished `orchestration.dag all` run for this SF instead of rebuilding it")
    args = ap.parse_args(argv)
    existing = {float(e.split("=", 1)[0]): Path(e.split("=", 1)[1]) for e in args.existing}
    rows, runs = [], []
    for sf in sorted(set(args.sf) | set(existing)):
        root = existing.get(sf, Path(args.root) / f"sf{sf}")
        if sf in existing:
            print(f"=== SF={sf}: reusing finished run in {root}", flush=True)
        elif root.exists():
            shutil.rmtree(root)
        if sf not in existing:
            (root / "landing" / "public").mkdir(parents=True)
            pub = REPO_ROOT / "data" / "landing" / "public"
            if pub.exists():   # reuse the cached public data (the experiment measures the pipeline, not the internet)
                shutil.copytree(pub, root / "landing" / "public", dirs_exist_ok=True)
                (root / "landing" / "public" / "provenance.json").unlink(missing_ok=True)
            env = os.environ | {"EPISCOPE_DATA_ROOT": str(root), "EPISCOPE__PATHS__SERVING_OUT": str(root / "serving_out")}
            print(f"=== SF={sf} -> {root}", flush=True)
            subprocess.run([sys.executable, "-m", "orchestration.dag", "all", "--sf", str(sf), "--offline"], cwd=REPO_ROOT, env=env, check=True)
        rec = json.loads(sorted((root / "runs").glob("dag_all_*.json"))[-1].read_text())
        gen = json.loads((root / "ground_truth" / "generator_manifest.json").read_text())
        loads = sorted((root / "runs").glob("load_*.json"))
        load = json.loads(loads[0].read_text()) if loads else {}   # the initial (history) load
        sizes = {"landing_mb": du(root / "landing") / 1e6, "bronze_mb": du(root / "lake" / "bronze") / 1e6,
                 "silver_mb": du(root / "lake" / "silver") / 1e6, "gold_mb": du(root / "lake" / "gold") / 1e6,
                 "serving_mb": du(root / "serving_out" / "national") / 1e6}
        stage = {t["task"]: t["seconds"] for t in rec["tasks"]}
        row = {"sf": sf, "encounter_rows": gen["encounter_rows_history"], "total_seconds": rec["seconds"], **stage,
               **{f"load_{k}": v for k, v in load.get("stage_seconds", {}).items()}, **{k: round(v, 1) for k, v in sizes.items()}}
        rows.append(row)
        runs.append({"sf": sf, "dag": rec, "load": load, "generator": gen})
        if not args.keep and sf not in existing and sf != max(args.sf):
            shutil.rmtree(root / "lake", ignore_errors=True)
            shutil.rmtree(root / "landing" / "encounters", ignore_errors=True)
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "scaling.json").write_text(json.dumps({"hardware": hardware(), "rows": rows, "runs": runs}, indent=2))
    keys = sorted({k for r in rows for k in r}, key=lambda k: (k != "sf", k))
    with open(OUT / "scaling.csv", "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=keys)
        w.writeheader()
        w.writerows(rows)
    for r in rows:
        print(f"SF={r['sf']:<5} rows={r['encounter_rows']:>11,}  total={r['total_seconds'] / 60:6.1f} min")


if __name__ == "__main__":
    main()
