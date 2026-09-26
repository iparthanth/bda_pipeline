"""Run one load: bronze ingest -> silver (DQ, quarantine, de-identification, dedup) -> gold marts.

    python -m pipeline.run_load                      # landing zone (historical/initial load)
    python -m pipeline.run_load --source data/drops/2026-01-01   # one incremental daily drop
    python -m pipeline.run_load --source /path/to/hidden_drop    # the instructor's 14-day drop

Idempotent: files already in bronze (same SHA-256) are skipped, silver MERGEs on encounter_id and gold
is recomputed with replaceWhere for the affected window, so running the same command twice changes nothing.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import time
from pathlib import Path

from pipeline.bronze.ingest import ingest
from pipeline.common.config import lake, path
from pipeline.common.spark import get_spark, table_exists
from pipeline.gold import build as gold
from pipeline.silver import dedup, transform as silver


def run(source: str, include_public: bool = True, force: bool = False) -> dict:
    t0 = time.time()
    spark = get_spark("load")
    first_load = not table_exists(spark, lake("silver", "encounters"))
    timings: dict[str, float] = {}

    t = time.time()
    res = ingest(spark, Path(source), include_public=include_public, force=force)
    timings["bronze"] = time.time() - t
    summary = {"batch_id": res.batch_id, "source": str(source), "files": {k: len(v) for k, v in res.files.items()},
               "skipped_files": len(res.skipped), "first_load": first_load}
    if not res.sources:
        summary["status"] = "nothing new - all files already ingested (idempotent no-op)"
        print(json.dumps(summary, indent=2))
        return summary

    t = time.time()
    if "public_boundaries" in res.sources or first_load:
        silver.silver_districts(spark, res.batch_id)
    if "public_population" in res.sources:
        silver.silver_population(spark, res.batch_id)
    if "public_weather" in res.sources:
        silver.silver_weather(spark, res.batch_id)
    reports = [silver.silver_facilities(spark, res.batch_id)]
    enc_report = silver.silver_encounters(spark, res.batch_id, res.load_ts) if "encounters" in res.sources else None
    reports += [enc_report, silver.silver_stock(spark, res.batch_id) if "stock" in res.sources else None]
    timings["silver"] = time.time() - t

    t = time.time()
    if enc_report:
        summary["episode_links"] = dedup.update_links(spark, None if first_load else res.batch_id)
    timings["dedup"] = time.time() - t

    t = time.time()
    since = None
    if not first_load and enc_report and enc_report.get("min_onset"):
        since = dt.date.fromisoformat(enc_report["min_onset"])
    as_of = gold.build(spark, since, res.sources)
    gold.dq_tables(spark, [r for r in reports if r], res.load_ts)
    timings["gold"] = time.time() - t

    summary.update({"as_of": str(as_of), "status": "loaded", "seconds": round(time.time() - t0, 1),
                    "stage_seconds": {k: round(v, 1) for k, v in timings.items()},
                    "dq": [{k: r[k] for k in ("source", "rows_in", "rows_quarantined")} | {"duplicates_removed": r.get("duplicates_removed", 0)}
                           for r in reports if r]})
    out = Path(path("runs")) / f"load_{res.batch_id}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(summary, indent=2, default=str))
    print(json.dumps(summary, indent=2, default=str))
    return summary


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--source", default=None, help="directory to ingest (default: landing zone)")
    ap.add_argument("--no-public", action="store_true", help="do not (re)ingest public sources")
    ap.add_argument("--force", action="store_true", help="re-ingest files even if their checksum is known")
    args = ap.parse_args(argv)
    run(args.source or path("landing"), include_public=not args.no_public, force=args.force)


if __name__ == "__main__":
    main()
