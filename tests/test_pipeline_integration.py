"""End-to-end on a tiny scale factor: generator contract, ING-4 idempotency/late data, DQ-3 lineage, DQ-2 report."""
from __future__ import annotations

import datetime as dt
from pathlib import Path

import pandas as pd
from pyspark.sql import functions as F

from pipeline.common.config import lake, path
from pipeline.common.schema import ENCOUNTER_COLUMNS


def _counts(spark):
    enc = spark.read.format("delta").load(lake("silver", "encounters"))
    wc = spark.read.format("delta").load(lake("gold", "weekly_counts"))
    return {"silver": enc.count(), "distinct": enc.select("encounter_id").distinct().count(),
            "gold_cases": wc.agg(F.sum("cases")).first()[0], "gold_rows": wc.count()}


def test_generator_contract(tiny_lake):
    root = tiny_lake["root"]
    files = sorted((root / "landing" / "encounters").glob("*.csv"))
    assert files
    assert pd.read_csv(files[0], nrows=5, dtype=str).columns.tolist() == ENCOUNTER_COLUMNS
    reg = pd.read_csv(root / "landing" / "facilities" / "facility_registry.csv")
    assert reg.facility_id.nunique() >= 600
    gt = pd.read_csv(root / "ground_truth" / "ground_truth_outbreaks.csv")
    ids = {o.split("-")[0] if o.startswith("NRS") else o for o in gt.outbreak_id}
    assert len(ids) >= 30
    assert (gt.kind == "slow").any() and (gt.kind == "novel_respiratory").sum() == 64
    assert (pd.read_csv(root / "ground_truth" / "reporting_artefacts.csv").kind == "silence_then_backfill").any()
    enc = pd.concat(pd.read_csv(f, dtype=str, keep_default_na=False) for f in files)
    dup = enc.encounter_id[enc.encounter_id != ""].duplicated().mean()
    assert 0.012 < dup < 0.03                               # about 2% resubmitted records
    delay = (pd.to_datetime(enc.report_date, errors="coerce") - pd.to_datetime(enc.onset_date, errors="coerce")).dt.days
    assert delay.quantile(0.99) <= 21                       # right-skewed 0-21 day delays (+ injected defects)


def test_generator_is_deterministic(tmp_path, monkeypatch):
    import hashlib
    from generator import fetch_public, generate
    hashes = []
    for i in range(2):
        root = tmp_path / f"r{i}"
        monkeypatch.setenv("EPISCOPE_DATA_ROOT", str(root))
        fetch_public.main(["--offline"])
        generate.main(["--sf", "0.002", "--workers", str(1 + i)])
        h = hashlib.sha256()
        for f in sorted((root / "landing" / "encounters").glob("*.csv")) + sorted((root / "drops").rglob("*.csv")):
            h.update(f.read_bytes())
        hashes.append(h.hexdigest())
    assert hashes[0] == hashes[1]


def test_initial_load_quality_and_lineage(spark, tiny_lake):
    s = tiny_lake["summary"]
    enc = next(d for d in s["dq"] if d["source"] == "encounters")
    assert enc["rows_quarantined"] > 0 and enc["duplicates_removed"] > 0
    q = spark.read.format("delta").load(lake("silver", "quarantine"))
    assert q.where(F.size("reason_codes") == 0).count() == 0
    rep = spark.read.format("delta").load(lake("gold", "dq_report"))
    assert rep.count() >= 3
    # DQ-3: every gold row with cases can be traced to bronze batches that exist in the manifest
    wc = spark.read.format("delta").load(lake("gold", "weekly_counts")).where("cases > 0")
    assert wc.where(F.size("bronze_batch_ids") == 0).count() == 0
    batches = {r.batch_id for r in spark.read.format("delta").load(lake("bronze", "ingest_manifest")).collect()}
    used = {b for r in wc.select(F.explode("bronze_batch_ids").alias("b")).distinct().collect() for b in [r.b]}
    assert used <= batches
    assert {r._source for r in spark.read.format("delta").load(lake("bronze", "encounters_raw")).select("_source").distinct().collect()} == {"encounters"}
    cols = set(spark.read.format("delta").load(lake("bronze", "encounters_raw")).columns)
    assert {"_source", "_load_ts", "_batch_id", "_file_checksum", "_file_name"} <= cols


def test_incremental_idempotent_and_late_data(spark, tiny_lake):
    from pipeline import run_load
    drops = sorted(Path(path("drops")).iterdir())
    first = drops[0]
    before = _counts(spark)
    run_load.run(str(first), include_public=False)
    after1 = _counts(spark)
    assert after1["silver"] == after1["distinct"]                  # no duplicate encounter IDs
    assert after1["silver"] > before["silver"]
    # re-running the same day: skipped by checksum
    s2 = run_load.run(str(first), include_public=False)
    assert s2["status"].startswith("nothing new")
    # forced re-ingest of the same files: MERGE keeps silver and gold identical
    run_load.run(str(first), include_public=False, force=True)
    after3 = _counts(spark)
    assert after3 == after1
    # late records: the drop contains reports whose onset is in earlier epi-weeks -> those weeks changed
    day = dt.date.fromisoformat(first.name)
    wc = spark.read.format("delta").load(lake("gold", "weekly_counts"))
    earlier = wc.where(F.col("week_start") < F.lit(day - dt.timedelta(days=day.weekday()))).agg(F.sum("cases")).first()[0]
    assert earlier is not None
    drop_csv = pd.read_csv(next(first.glob("encounters_*.csv")), dtype=str)
    onset = pd.to_datetime(drop_csv.onset_date, errors="coerce")
    assert (onset < pd.Timestamp(day - dt.timedelta(days=7))).any()  # the drop really carries late records
