"""Rehearse incremental loads and the hidden test drop on a built lake, and record the evidence.

    python -m orchestration.rehearse          # after `make all`; takes about 25 min at SF = 1

1. **One incremental day** (ING-4): the first generated drop day goes through the ``drop`` DAG; then the same
   day is loaded again (a no-op: every file checksum is already in the manifest) and finally force
   re-ingested (bronze accepts the files again, silver MERGEs on ``encounter_id``, quarantine replaces the
   rows of the same file checksum). Counts after each step -> ``docs/experiments/incremental_drop.json``.
2. **Hidden-drop rehearsal** (brief Section 5.2): the folder with all generated drops goes through the same
   DAG, exactly as the instructor's drop would; the day already loaded is skipped. Rows, quarantine, run
   time, the top alerts afterwards and the injected drop outbreaks (read from the ground truth *here*, for
   evaluation only) -> ``docs/experiments/hidden_drop_rehearsal.json``.
3. **Time travel** (ING-7): gold weekly counts as known at the Delta version before step 2 vs after, next to
   the reporting-triangle reconstruction the backtest uses -> ``docs/experiments/time_travel.json``.

The dashboard bundles are rebuilt by the drop DAG, so run ``make all`` (or restore ``serving/out``) afterwards
if the committed dashboard should show the historical state again.
"""
from __future__ import annotations

import json
import subprocess
import sys
import time
from pathlib import Path

import pandas as pd
from pyspark.sql import functions as F

from pipeline.common.config import REPO_ROOT, lake, path
from pipeline.common.spark import get_spark

OUT = REPO_ROOT / "docs" / "experiments"


def _snapshot_here() -> dict:
    from delta.tables import DeltaTable

    spark = get_spark("rehearse-snapshot")
    enc = spark.read.format("delta").load(lake("silver", "encounters"))
    wk = spark.read.format("delta").load(lake("gold", "weekly_counts"))
    s = enc.agg(F.count("*").alias("rows"), F.countDistinct("encounter_id").alias("ids")).first()
    g = wk.agg(F.sum("cases").alias("cases"), F.count("*").alias("rows"), F.max("as_of").alias("as_of")).first()
    return {"silver_rows": s.rows, "distinct_ids": s.ids, "gold_cases": int(g.cases), "gold_rows": g.rows,
            "quarantine_rows": spark.read.format("delta").load(lake("silver", "quarantine")).count(), "as_of": str(g.as_of),
            "silver_versions": DeltaTable.forPath(spark, lake("silver", "encounters")).history().count(),
            "gold_weekly_version": DeltaTable.forPath(spark, lake("gold", "weekly_counts")).history()
            .agg(F.max("version")).first()[0]}


def snapshot() -> dict:
    """Counts of the lake, measured in a separate process (the embedded Derby catalog admits one JVM)."""
    _, out = _py("orchestration.rehearse", "--snapshot")
    return json.loads(out[out.index("{"):])


def _py(*args: str) -> tuple[float, str]:
    t = time.time()
    p = subprocess.run([sys.executable, "-m", *args], cwd=REPO_ROOT, capture_output=True, text=True)
    if p.returncode != 0:
        raise SystemExit(f"{' '.join(args)} failed:\n{p.stdout[-3000:]}\n{p.stderr[-3000:]}")
    return round(time.time() - t, 1), p.stdout


def _latest(pattern: str) -> dict:
    f = max(Path(path("runs")).glob(pattern), key=lambda p: p.stat().st_mtime)
    return json.loads(f.read_text())


def _dag_drop(source: str) -> tuple[dict, dict]:
    _py("orchestration.dag", "drop", "--source", source)
    dag = _latest("dag_drop_*.json")
    return dag, _latest("load_*.json")


def main() -> int:
    if "--snapshot" in sys.argv:
        print(json.dumps(_snapshot_here(), default=str))
        return 0
    drops = sorted(p for p in Path(path("drops")).iterdir() if p.is_dir())
    first = str(drops[0])

    # 1. one incremental day, then a re-run and a forced re-ingest
    before = snapshot()
    dag, load = _dag_drop(first)
    after = snapshot()
    rerun_s, rerun_out = _py("pipeline.run_load", "--source", first, "--no-public")
    status = json.loads(rerun_out[rerun_out.index("{"):])["status"]
    force_s, _ = _py("pipeline.run_load", "--source", first, "--no-public", "--force")
    forced = _latest("load_*.json")
    after_force = snapshot()
    inc = {"description": "one incremental daily drop (first future day) through the drop DAG, then the same "
                          "drop loaded again (no-op) and force re-ingested",
           "drop_dir": Path(first).name, "before_drop": before, "after_drop": after,
           "after_rerun_and_forced_reingest": after_force,
           "drop_dag_seconds": dag["seconds"], "drop_dag_tasks": {t["task"]: t["seconds"] for t in dag["tasks"]},
           "drop_load": {k: load[k] for k in ("seconds", "stage_seconds", "dq", "files")},
           "rerun_status": f"{status}, {rerun_s:.0f} s", "rerun_seconds": rerun_s, "forced_reingest_load":
               {k: forced[k] for k in ("seconds", "stage_seconds", "dq", "files")} | {"wall_seconds": force_s},
           "checks": {"no_duplicates_after_drop": after["silver_rows"] == after["distinct_ids"],
                      "forced_reingest_changes_nothing": all(after_force[k] == after[k] for k in
                                                            ("silver_rows", "distinct_ids", "gold_cases", "quarantine_rows"))}}
    (OUT / "incremental_drop.json").write_text(json.dumps(inc, indent=1, default=str))
    print(json.dumps(inc["checks"]), flush=True)

    # 2. the whole drop folder, as the hidden test drop would arrive
    pre = snapshot()
    dag, load = _dag_drop(str(Path(path("drops"))))
    post = snapshot()
    alerts = json.loads((Path(path("serving_out")) / "national" / "alerts.json").read_text())["table"]
    top = sorted(alerts, key=lambda r: -(r.get("statistic") or 0))[:4]
    gt = pd.read_csv(Path(path("ground_truth")) / "ground_truth_outbreaks.csv")
    window = gt[pd.to_datetime(gt["end_date"]) >= pd.Timestamp(pre["as_of"])]
    injected = window[["outbreak_id", "adm2_pcode", "disease_group", "start_date"]].rename(columns={"start_date": "start"})
    flagged = {(r["adm2_pcode"], r["disease_group"]) for r in alerts if r["status"] in ("alert", "verify_reporting")}
    reh = {"description": "the folder with all generated daily drops through `make drop` (same schema, unannounced "
                          "outbreaks and quality defects); the day already loaded is skipped by checksum",
           "days": load["files"].get("encounters", 0), "files": load["files"], "skipped_files": load["skipped_files"],
           "new_rows": post["silver_rows"] - pre["silver_rows"],
           "quarantined": sum(d["rows_quarantined"] for d in load["dq"]),
           "duplicates_removed": sum(d["duplicates_removed"] for d in load["dq"]),
           "load_seconds": load["seconds"], "dag_seconds": dag["seconds"],
           "dag_tasks": {t["task"]: t["seconds"] for t in dag["tasks"]}, "before": pre, "after": post,
           "as_of_after": post["as_of"],
           "top_alerts_after": [{k: r.get(k) for k in ("adm2_pcode", "disease_group", "status", "statistic", "days_in_alert")} for r in top],
           "new_alert_series": len(flagged),
           "injected_in_drop_window": injected.to_dict("records"),
           "injected_flagged": [o for o in injected.to_dict("records") if (o["adm2_pcode"], o["disease_group"]) in flagged]}
    (OUT / "hidden_drop_rehearsal.json").write_text(json.dumps(reh, indent=1, default=str))
    print(json.dumps({k: reh[k] for k in ("days", "new_rows", "quarantined", "new_alert_series")}), flush=True)

    # 3. time travel: what did the lake know before the rehearsal drop?
    tmp = OUT / "time_travel.json"
    _py("orchestration.inspect_lake", "asof", "--version", str(pre["gold_weekly_version"]), "--json", str(tmp))
    tt = json.loads(tmp.read_text())
    tt["description"] = ("gold.weekly_counts read with Delta versionAsOf before the rehearsal drop vs the current "
                         "version (late reports revise past weeks); 'triangle_then' rebuilds the old counts from "
                         "gold.reporting_triangle, the backtest's method, at weekly resolution (it also counts the "
                         "reports made later in the as-of week, so it can only be equal or slightly higher)")
    rows = pd.DataFrame(tt["rows"])
    tt["revised_weeks"] = int((rows.known_now != rows.known_then).sum())
    tt["triangle_minus_time_travel"] = {"max_abs": int((rows.triangle_then - rows.known_then).abs().max()),
                                        "total": int((rows.triangle_then - rows.known_then).sum()),
                                        "known_then_total": int(rows.known_then.sum())}
    (OUT / "time_travel.json").write_text(json.dumps(tt, indent=1, default=str))
    print(json.dumps({k: tt[k] for k in ("revised_weeks", "triangle_minus_time_travel")}), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
