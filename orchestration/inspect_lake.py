"""Look inside the lake from the command line - for the video (chapter 3) and for checking a load by hand.

    python -m orchestration.inspect_lake tables        # catalog tables per layer with row counts (ING-3)
    python -m orchestration.inspect_lake idempotency   # silver rows vs distinct encounter_id, files in the manifest (ING-4)
    python -m orchestration.inspect_lake quarantine    # rows per rule and five quarantined examples (DQ-1)
    python -m orchestration.inspect_lake history       # Delta versions of silver/gold (ING-7)
    python -m orchestration.inspect_lake asof          # time travel: national weekly cases as known at an
                                                       # earlier Delta version vs now (ING-7), checked against
                                                       # the reporting-triangle reconstruction used by the backtest
    python -m orchestration.inspect_lake all

``make inspect WHAT=quarantine`` runs the same commands inside the project environment.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipeline.common.config import lake
from pipeline.common.spark import get_spark, table_exists

LAYERS = ("bronze", "silver", "gold")


def _title(t: str) -> None:
    print(f"\n=== {t} " + "=" * max(0, 70 - len(t)))


def tables(spark: SparkSession) -> None:
    _title("Catalog tables (Spark SQL / Hive, Delta)")
    for layer in LAYERS:
        for t in spark.sql(f"SHOW TABLES IN episcope_{layer}").collect():
            n = spark.table(f"episcope_{layer}.{t.tableName}").count()
            print(f"  episcope_{layer}.{t.tableName:28s} {n:>14,d} rows")


def idempotency(spark: SparkSession) -> None:
    _title("Idempotency: every encounter_id exactly once in silver (ING-4)")
    enc = spark.read.format("delta").load(lake("silver", "encounters"))
    r = enc.agg(F.count("*").alias("rows"), F.countDistinct("encounter_id").alias("ids")).first()
    print(f"  silver.encounters rows = {r.rows:,d}   distinct encounter_id = {r.ids:,d}   "
          f"-> {'OK, no duplicates' if r.rows == r.ids else 'DUPLICATES PRESENT'}")
    man = spark.read.format("delta").load(lake("bronze", "ingest_manifest"))
    m = man.agg(F.count("*").alias("files"), F.countDistinct("checksum").alias("sums"),
                F.countDistinct("batch_id").alias("batches")).first()
    print(f"  bronze manifest: {m.files:,d} files, {m.sums:,d} distinct SHA-256 checksums, {m.batches:,d} load batches")
    print("  (re-running a load skips every file whose checksum is already in the manifest; a forced re-load is a")
    print("   MERGE on encounter_id, so the two silver numbers above stay equal)")


def quarantine(spark: SparkSession, n: int = 5) -> None:
    _title("Quarantine: failing rows are kept with their rule IDs (DQ-1)")
    q = spark.read.format("delta").load(lake("silver", "quarantine"))
    rules = q.select("source", F.explode("reason_codes").alias("rule")).groupBy("source", "rule").count()
    for r in rules.orderBy(F.desc("count")).limit(12).collect():
        print(f"  {r.source:11s} {r.rule:18s} {r['count']:>10,d} rows")
    print(f"\n  One example for each of the {n} most frequent encounter rules (patient IDs are already hashed - PRV-1):")
    enc = q.where(F.col("source") == "encounters").withColumn("rule", F.col("reason_codes")[0])
    top = [r.rule for r in enc.groupBy("rule").count().orderBy(F.desc("count")).limit(n).collect()]
    for rule in top:
        r = enc.where(F.col("rule") == rule).first()
        print(f"  [{','.join(r.reason_codes)}] {r.record[:160]}")


def history(spark: SparkSession, table: str = "gold/weekly_counts", n: int = 8) -> list:
    from delta.tables import DeltaTable

    layer, name = table.split("/")
    h = (DeltaTable.forPath(spark, lake(layer, name)).history()
         .select("version", "timestamp", "operation").orderBy(F.desc("version")).limit(n).collect())
    _title(f"Delta history of {layer}.{name} (newest first)")
    for r in h:
        print(f"  version {r.version:>4d}  {r.timestamp}  {r.operation}")
    return h


def _national(df: DataFrame, weeks: list) -> DataFrame:
    return (df.where(F.col("week_start").isin(weeks)).groupBy("disease_group", "week_start")
            .agg(F.sum("cases").alias("cases")))


def asof(spark: SparkSession, version: int | None = None, n_weeks: int = 6) -> dict:
    """What did we know at Delta version ``version`` of gold.weekly_counts (default: the version before the
    latest load)? Compared with today and with the reporting-triangle reconstruction the backtest uses."""
    from delta.tables import DeltaTable

    loc = lake("gold", "weekly_counts")
    versions = [r.version for r in DeltaTable.forPath(spark, loc).history().select("version").collect()]
    if version is None:
        version = sorted(versions)[-2] if len(versions) > 1 else max(versions)
    then = spark.read.format("delta").option("versionAsOf", version).load(loc)
    now = spark.read.format("delta").load(loc)
    known_until = then.agg(F.max("as_of")).first()[0]
    weeks = [r.week_start for r in then.select("week_start").distinct().orderBy(F.desc("week_start")).limit(n_weeks).collect()]
    t = _national(then, weeks).withColumnRenamed("cases", "known_then")
    c = _national(now, weeks).withColumnRenamed("cases", "known_now")
    # backtest reconstruction: cases by onset week that had been reported by the end of the as-of week
    tri = spark.read.format("delta").load(lake("gold", "reporting_triangle"))
    cut = F.date_trunc("week", F.lit(known_until)).cast("date")
    r = (tri.where(F.col("report_week") <= cut).transform(lambda d: _national(d, weeks))
         .withColumnRenamed("cases", "triangle_then"))
    rows = (t.join(c, ["disease_group", "week_start"], "outer").join(r, ["disease_group", "week_start"], "left")
            .fillna(0).orderBy("disease_group", "week_start").collect())
    _title(f"Time travel (ING-7): gold.weekly_counts at version {version} (data to {known_until}) vs now")
    print(f"  {'disease':10s} {'week':10s} {'known then':>11s} {'known now':>10s} {'revised by':>11s} {'triangle':>9s}")
    out = []
    for x in rows:
        print(f"  {x.disease_group:10s} {str(x.week_start):10s} {x.known_then:>11,d} {x.known_now:>10,d} "
              f"{x.known_now - x.known_then:>+11,d} {x.triangle_then:>9,d}")
        out.append({"disease_group": x.disease_group, "week_start": str(x.week_start), "known_then": int(x.known_then),
                    "known_now": int(x.known_now), "triangle_then": int(x.triangle_then)})
    return {"table": "gold.weekly_counts", "version_then": version, "latest_version": max(versions),
            "known_until_then": str(known_until), "rows": out}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("what", nargs="?", default="all",
                    choices=["tables", "idempotency", "quarantine", "history", "asof", "all"])
    ap.add_argument("--version", type=int, default=None, help="Delta version for 'asof' (default: before the last load)")
    ap.add_argument("--json", help="also write the 'asof' comparison to this JSON file")
    args = ap.parse_args(argv)
    spark = get_spark("inspect")
    if not table_exists(spark, lake("silver", "encounters")):
        print("No lake yet - run `make all` (or `docker compose up`) first.")
        return 1
    if args.what in ("tables", "all"):
        tables(spark)
    if args.what in ("idempotency", "all"):
        idempotency(spark)
    if args.what in ("quarantine", "all"):
        quarantine(spark)
    if args.what in ("history", "all"):
        history(spark)
    if args.what in ("asof", "all"):
        res = asof(spark, args.version)
        if args.json:
            Path(args.json).write_text(json.dumps(res, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
