"""Spark optimisation experiments with before/after evidence (ENG-3).

    python -m pipeline.experiments.spark_optimisations [--repeat 3]

Each experiment runs the *same* query twice - without and with the optimisation - on the SF=1 lake,
repeats it ``--repeat`` times (median reported), and records wall time, the physical plan and Spark SQL
metrics. Every query is tagged with a job description so it is easy to find in the Spark UI / History
Server (event logs in ``data/spark-events``; ``docs/experiments/screenshots`` holds the captured pages).

1. **Broadcast join of the facility registry** (silver step): 28M bronze encounters joined to the ~790-row
   registry. Before: broadcast disabled -> SortMergeJoin with a full shuffle of the large side. After:
   BroadcastHashJoin, no shuffle of the encounters.
2. **Partition pruning** on ``silver.encounters`` partitioned by (epi_year, adm1_pcode): a division-year
   epi-curve query. Before: the same data written *unpartitioned* (full scan). After: partition filters
   prune to 1 of 48 directories.
3. **Columnar storage** (bronze as Delta/Parquet instead of the landing CSV): the same per-facility,
   per-disease count over all 28M encounters. Before: scanning and parsing the raw CSV files. After: reading only
   the two needed columns from the Parquet files of bronze (column pruning, compression, no text parsing).
"""
from __future__ import annotations

import argparse
import json
import statistics
import time
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pipeline.common.config import REPO_ROOT, lake, path
from pipeline.common.spark import get_spark

OUT = REPO_ROOT / "docs" / "experiments"


def timed(spark: SparkSession, label: str, fn, repeat: int) -> dict:
    times, plan = [], None
    for i in range(repeat):
        spark.sparkContext.setJobDescription(f"ENG-3 {label} run {i + 1}")
        t = time.time()
        df = fn()
        df.write.format("noop").mode("overwrite").save()   # forces full execution without writing output
        times.append(time.time() - t)
        plan = df._jdf.queryExecution().executedPlan().toString()
    spark.sparkContext.setJobDescription(None)
    return {"label": label, "seconds_median": round(statistics.median(times), 2), "seconds_all": [round(t, 2) for t in times],
            "plan_excerpt": "\n".join(plan.splitlines()[:40])}


def exp_broadcast(spark: SparkSession, repeat: int) -> dict:
    enc = spark.read.format("delta").load(lake("bronze", "encounters_raw")).select("facility_id", "icd10_code")
    fac = spark.read.format("delta").load(lake("silver", "facilities")).select("facility_id", "adm2_pcode")

    def q(hint: bool):
        f = F.broadcast(fac) if hint else fac
        return enc.join(f, "facility_id", "left").groupBy("adm2_pcode").agg(F.count("*").alias("n"))

    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    before = timed(spark, "broadcast: OFF (sort-merge join)", lambda: q(False), repeat)
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(10 * 1024 * 1024))
    after = timed(spark, "broadcast: ON (broadcast hash join)", lambda: q(True), repeat)
    return {"experiment": "broadcast_registry_join", "before": before, "after": after,
            "speedup": round(before["seconds_median"] / max(after["seconds_median"], 1e-6), 2),
            "check_before_smj": "SortMergeJoin" in before["plan_excerpt"], "check_after_bhj": "BroadcastHashJoin" in after["plan_excerpt"]}


def exp_pruning(spark: SparkSession, repeat: int) -> dict:
    src = lake("silver", "encounters")
    flat = str(Path(path("data_root")) / "experiments" / "encounters_unpartitioned")
    if not DeltaTable.isDeltaTable(spark, flat):
        spark.read.format("delta").load(src).write.format("delta").mode("overwrite").save(flat)
    part = spark.read.format("delta").load(src)
    year = part.agg(F.max("epi_year")).first()[0] - 1

    def q(loc):
        d = spark.read.format("delta").load(loc)
        return (d.where((F.col("epi_year") == year) & (F.col("adm1_pcode") == "BD20") & (F.col("disease_group") == "dengue"))
                .groupBy("week_start").agg(F.count("*").alias("cases")))

    before = timed(spark, "pruning: unpartitioned table (full scan)", lambda: q(flat), repeat)
    after = timed(spark, "pruning: partitioned by epi_year, adm1_pcode", lambda: q(src), repeat)
    files_flat = spark.read.format("delta").load(flat).inputFiles()
    files_part = [f for f in spark.read.format("delta").load(src).where((F.col("epi_year") == year) & (F.col("adm1_pcode") == "BD20")).inputFiles()]
    return {"experiment": "partition_pruning", "before": before, "after": after, "files_scanned_before": len(files_flat),
            "files_scanned_after": len(files_part), "speedup": round(before["seconds_median"] / max(after["seconds_median"], 1e-6), 2)}


def exp_columnar(spark: SparkSession, repeat: int) -> dict:
    from pipeline.common.schema import ENCOUNTER_COLUMNS
    from pyspark.sql import types as T

    csv_dir = str(Path(path("landing")) / "encounters")
    schema = T.StructType([T.StructField(c, T.StringType()) for c in ENCOUNTER_COLUMNS])

    def q_csv():
        d = spark.read.option("header", True).schema(schema).csv(csv_dir)
        return d.groupBy("facility_id", F.substring("icd10_code", 1, 3).alias("icd")).agg(F.count("*").alias("n"))

    def q_parquet():
        d = spark.read.format("delta").load(lake("bronze", "encounters_raw"))
        return d.groupBy("facility_id", F.substring("icd10_code", 1, 3).alias("icd")).agg(F.count("*").alias("n"))

    before = timed(spark, "columnar: raw CSV landing files", q_csv, repeat)
    after = timed(spark, "columnar: bronze Delta/Parquet (2 of 17 columns read)", q_parquet, repeat)
    size_csv = sum(f.stat().st_size for f in Path(csv_dir).glob("*.csv"))
    loc = Path(lake("bronze", "encounters_raw"))
    size_pq = sum(f.stat().st_size for f in loc.rglob("*.parquet")) if loc.exists() else None
    return {"experiment": "columnar_storage", "before": before, "after": after, "csv_bytes": size_csv, "parquet_bytes": size_pq,
            "speedup": round(before["seconds_median"] / max(after["seconds_median"], 1e-6), 2)}


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--repeat", type=int, default=3)
    args = ap.parse_args(argv)
    spark = get_spark("eng3-optimisations")
    res = {"app_id": spark.sparkContext.applicationId,
           "results": [exp_broadcast(spark, args.repeat), exp_pruning(spark, args.repeat), exp_columnar(spark, args.repeat)]}
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "spark_optimisations.json").write_text(json.dumps(res, indent=2))
    for r in res["results"]:
        print(f"{r['experiment']:26s} before {r['before']['seconds_median']:7.2f}s  after {r['after']['seconds_median']:7.2f}s  x{r['speedup']}")


if __name__ == "__main__":
    main()
