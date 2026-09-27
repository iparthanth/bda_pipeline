# Experiments (ENG-2, ENG-3, ING-5, ING-6)

| File | Produced by | Content |
|---|---|---|
| `scaling.json`, `scaling.csv` | `make scale` (`orchestration/scale_experiment.py`) | Full `make all` at SF 0.25 / 0.5 / 1: rows, runtime per stage, output sizes, hardware |
| `spark_optimisations.json` | `make optimisations` (`pipeline/experiments/spark_optimisations.py`) | Before/after timings, plans and file counts for broadcast join, partition pruning and columnar storage (CSV vs Delta/Parquet) |
| `screenshots/*.png` | Spark History Server (`data/spark-events`) captured with a headless browser | Spark UI evidence for the optimisation experiments and the SF=1 load |
| `s3a_minio_check.json` | `pipeline/experiments/s3a_check.py` | Lake written to and read from an S3-compatible endpoint through S3A (ING-5) |
| `stream_latency.jsonl` | `make stream` + a file dropped in `data/stream_inbox/encounters/` | Arrival -> refreshed-alerts latency per file (ING-6) |
| `incremental_drop.json` | `make incremental` twice | Idempotency evidence: counts after the first and second run of the same day |
| `hidden_drop_rehearsal.json` | `make drop DROP=data/drops` | The 14 generated rehearsal drops: rows, run time, quarantine, which alerts the injected outbreaks raised |
| `time_travel.json` | `make inspect WHAT=asof` after a drop | Delta time travel (ING-7): national weekly cases as known before the drop vs after, checked against the reporting-triangle reconstruction |

Hardware used for the numbers in the report: 4 vCPU, 16 GB RAM, Linux container, Spark 3.5.3 local mode,
6 GB driver heap, Java 17, Python 3.11.

To view the Spark UI of past runs: `SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=file://$PWD/data/spark-events"
.venv/lib/python3.11/site-packages/pyspark/bin/spark-class org.apache.spark.deploy.history.HistoryServer` and open
http://localhost:18080.
