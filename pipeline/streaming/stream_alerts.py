"""Near-real-time path (ING-6): Spark Structured Streaming over a file source that refreshes alerts.

    python -m pipeline.streaming.stream_alerts [--once] [--trigger-seconds 30]

New encounter files dropped into ``data/stream_inbox/encounters/`` (same schema contract as the batch
feed) are picked up by a file-source stream. Each micro-batch (``foreachBatch``) reuses the *same* code as
the batch pipeline, so there is one set of rules:

1. land the rows in bronze with ingestion metadata (source, load time, batch ID, file checksum);
2. silver MERGE (quality rules, quarantine, de-identification, resubmission handling);
3. episode links for the affected patients, incremental gold rebuild of the affected window;
4. re-run detection and publish ``gold.alerts``, then refresh ``alerts.json`` for the dashboard.

For every file the latency from arrival (file modification time) to refreshed alerts is appended to
``data/runs/stream_latency.jsonl`` - the requirement is <= 5 minutes. The checkpoint directory makes the
stream exactly-once with respect to input files; the silver MERGE makes re-processing harmless anyway.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import time
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from analytics.detection import detect
from pipeline.bronze.ingest import sha256
from pipeline.common.config import lake, path
from pipeline.common.schema import ENCOUNTER_COLUMNS
from pipeline.common.spark import get_spark
from pipeline.gold import build as gold
from pipeline.silver import dedup, transform as silver


def process(batch_df: DataFrame, epoch: int) -> None:
    spark = batch_df.sparkSession
    rows = batch_df.withColumn("_file_name", F.input_file_name()).persist()
    files = [r._file_name for r in rows.select("_file_name").distinct().collect()]
    if not files:
        rows.unpersist()
        return
    t_start = time.time()
    now = dt.datetime.now(dt.timezone.utc)
    batch_id = f"S{now:%Y%m%dT%H%M%S}-e{epoch}"
    local = {f: f.replace("file://", "").replace("file:", "") for f in files}
    sums = spark.createDataFrame([(f, sha256(Path(p))) for f, p in local.items()], "_file_name string, _file_checksum string")
    out = (rows.join(F.broadcast(sums), "_file_name").withColumn("_source", F.lit("encounters"))
           .withColumn("_batch_id", F.lit(batch_id)).withColumn("_load_ts", F.lit(now.isoformat()).cast("timestamp")))
    loc = lake("bronze", "encounters_raw")
    out.write.format("delta").mode("append").partitionBy("_batch_id").save(loc)
    rep = silver.silver_encounters(spark, batch_id, now.isoformat())
    dedup.update_links(spark, batch_id)
    since = dt.date.fromisoformat(rep["min_onset"]) if rep and rep.get("min_onset") else None
    gold.build(spark, since, ["encounters"])
    gold.dq_tables(spark, [rep], now.isoformat())
    detect.run(spark)
    op = _operating_point()
    detect.publish_alerts(spark, op["method"], op["threshold"])
    try:
        from serving.export import export_alerts_only
        export_alerts_only(spark)
    except FileNotFoundError:
        pass  # no full export yet - the dashboard bundle is created by `make all`
    done = time.time()
    log = Path(path("runs")) / "stream_latency.jsonl"
    with open(log, "a") as fh:
        for f, p in local.items():
            arrived = os.path.getmtime(p)
            fh.write(json.dumps({"batch_id": batch_id, "file": Path(p).name, "arrived": arrived, "alerts_refreshed": done,
                                 "latency_seconds": round(done - arrived, 1), "processing_seconds": round(done - t_start, 1),
                                 "rows": rep["rows_in"] if rep else 0}) + "\n")
    print(f"[stream] epoch {epoch}: {len(files)} file(s), {rep['rows_in'] if rep else 0} rows, alerts refreshed in {done - t_start:.0f}s", flush=True)
    rows.unpersist()


def _operating_point() -> dict:
    f = Path(path("results")) / "detection_summary.json"
    if f.exists():
        return json.loads(f.read_text())["operating_point"]
    from pipeline.common.config import load_config
    op = load_config()["detection"]["operating_point"]
    return {"method": op["method"], "threshold": float(op["threshold"])}


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--trigger-seconds", type=int, default=30)
    ap.add_argument("--once", action="store_true", help="process what is in the inbox and stop (availableNow)")
    args = ap.parse_args(argv)
    spark = get_spark("stream")
    inbox = Path(path("stream_inbox")) / "encounters"
    inbox.mkdir(parents=True, exist_ok=True)
    schema = T.StructType([T.StructField(c, T.StringType()) for c in ENCOUNTER_COLUMNS] +
                          [T.StructField("_corrupt_record", T.StringType())])
    stream = (spark.readStream.schema(schema).option("header", True).option("mode", "PERMISSIVE")
              .option("columnNameOfCorruptRecord", "_corrupt_record").option("maxFilesPerTrigger", 50)
              .csv(str(inbox)))
    w = (stream.writeStream.foreachBatch(process)
         .option("checkpointLocation", str(Path(path("data_root")) / "checkpoints" / "stream_alerts")))
    w = w.trigger(availableNow=True) if args.once else w.trigger(processingTime=f"{args.trigger_seconds} seconds")
    q = w.start()
    print(f"[stream] watching {inbox} (trigger {'availableNow' if args.once else str(args.trigger_seconds) + 's'})", flush=True)
    q.awaitTermination()


if __name__ == "__main__":
    main()
