"""Gold layer: epi-week marts and features consumed by analytics and serving.

Incremental maintenance (ING-4): after a daily load, every gold mart is recomputed only for the window
touched by the batch (from the earliest onset/report date in the batch, minus a guard band) and written
with Delta ``replaceWhere``; re-running the same load rewrites the same rows (idempotent), and late
records (up to 21 days and beyond) update the epi-weeks they belong to.

Lineage (DQ-3): every gold row carries ``bronze_batch_ids`` - the set of bronze batches whose records
contributed to it; ``gold.lineage`` maps batch IDs to files and checksums.
"""
from __future__ import annotations

import datetime as dt

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from pipeline.common.config import lake, load_config
from pipeline.common.spark import register_table, table_exists

TARGETS = ["dengue", "diarrhoea", "ili", "malaria"]


def _read(spark: SparkSession, layer: str, table: str) -> DataFrame:
    return spark.read.format("delta").load(lake(layer, table))


def _write(spark: SparkSession, df: DataFrame, table: str, partition_by: list[str] | None, where: str | None) -> None:
    loc = lake("gold", table)
    w = df.write.format("delta").mode("overwrite")
    if where and table_exists(spark, loc):
        w = w.option("replaceWhere", where)
    else:
        w = w.option("overwriteSchema", "true")
    if partition_by:
        w = w.partitionBy(*partition_by)
    w.save(loc)
    register_table(spark, "gold", table, loc)


def _cases(spark: SparkSession, since: dt.date | None) -> DataFrame:
    enc = _read(spark, "silver", "encounters").where(F.col("disease_group").isin(TARGETS))
    if since is not None:
        enc = enc.where((F.col("onset_date") >= F.lit(since)) & (F.col("epi_year") >= since.year - 1))
    links = _read(spark, "silver", "episode_links").select("encounter_id", F.lit(True).alias("is_duplicate"))
    return enc.join(links, "encounter_id", "left").withColumn("is_duplicate", F.coalesce("is_duplicate", F.lit(False)))


def _grid_weeks(spark: SparkSession, lo: dt.date, hi: dt.date) -> DataFrame:
    lo = lo - dt.timedelta(days=lo.weekday())
    return spark.sql(f"SELECT explode(sequence(to_date('{lo}'), to_date('{hi}'), interval 7 days)) AS week_start")


def weekly_counts(spark: SparkSession, since: dt.date | None, as_of: dt.date) -> None:
    enc = _cases(spark, since)
    agg = (enc.groupBy("adm2_pcode", "disease_group", "week_start")
           .agg(F.sum(F.when(~F.col("is_duplicate"), 1).otherwise(0)).alias("cases"),
                F.count("*").alias("encounters"),
                F.sum(F.when(F.col("admitted") & ~F.col("is_duplicate"), 1).otherwise(0)).alias("admitted"),
                F.sum(F.when(F.col("lab_confirmed") & ~F.col("is_duplicate"), 1).otherwise(0)).alias("lab_confirmed"),
                F.sum(F.when(F.col("outcome") == "died", 1).otherwise(0)).alias("deaths"),
                F.array_sort(F.collect_set("_batch_id")).alias("bronze_batch_ids")))
    lo = since or enc.agg(F.min("onset_date")).first()[0]
    dist = _read(spark, "silver", "districts").select("adm2_pcode", "adm1_pcode")
    grid = (_grid_weeks(spark, lo, as_of).crossJoin(dist)
            .crossJoin(spark.createDataFrame([(g,) for g in TARGETS], "disease_group string")))
    out = (grid.join(agg, ["adm2_pcode", "disease_group", "week_start"], "left")
           .fillna(0, ["cases", "encounters", "admitted", "lab_confirmed", "deaths"])
           .withColumn("bronze_batch_ids", F.coalesce("bronze_batch_ids", F.array().cast("array<string>")))
           .withColumn("epi_year", F.year(F.date_add("week_start", 3)))
           .withColumn("epi_week", F.weekofyear("week_start"))
           .withColumn("as_of", F.lit(as_of)))
    where = f"week_start >= '{lo - dt.timedelta(days=lo.weekday())}'" if since else None
    _write(spark, out, "weekly_counts", ["epi_year"], where)


def reporting_triangle(spark: SparkSession, since: dt.date | None) -> None:
    """Cases by onset week x first-report week: lets analytics rebuild 'what we knew on date t' exactly."""
    enc = _cases(spark, since).where(~F.col("is_duplicate"))
    out = (enc.withColumn("report_week", F.date_trunc("week", "first_report_date").cast("date"))
           .groupBy("adm2_pcode", "disease_group", "week_start", "report_week")
           .agg(F.count("*").alias("cases"), F.array_sort(F.collect_set("_batch_id")).alias("bronze_batch_ids"))
           .withColumn("epi_year", F.year(F.date_add("week_start", 3))))
    where = f"week_start >= '{since - dt.timedelta(days=since.weekday())}'" if since else None
    _write(spark, out, "reporting_triangle", ["epi_year"], where)


def daily_signal(spark: SparkSession, since: dt.date | None, as_of: dt.date) -> None:
    """Daily counts by first report date for prospective detection, with reporting-artefact context.

    * ``n_naive``  : every valid record reported that day (what a naive detector sees).
    * ``n_signal`` : excludes duplicate episodes and *back-filled* records - records sent on the first day a
      normally-daily facility reports again after >= backfill_gap_days of silence, with delay >= backfill_min_delay.
    * ``new_facility_share``: share of the day's records coming from facilities that started reporting in the
      last 56 days (reporting-practice change; used to annotate alerts).
    """
    dcfg = load_config()["detection"]
    enc_all = _read(spark, "silver", "encounters")
    guard = since - dt.timedelta(days=90) if since else None
    if guard:
        enc_all = enc_all.where((F.col("first_report_date") >= F.lit(guard)) | (F.col("onset_date") >= F.lit(guard)))
    # facility reporting pattern over all encounter types
    fday = enc_all.groupBy("facility_id", F.col("first_report_date").alias("d")).agg(F.count("*").alias("n"))
    wf = Window.partitionBy("facility_id").orderBy("d")
    fday = fday.withColumn("gap", F.datediff("d", F.lag("d").over(wf)))
    typical = fday.groupBy("facility_id").agg(F.expr("percentile_approx(gap, 0.5)").alias("median_gap"),
                                               F.min("d").alias("first_seen"))
    if table_exists(spark, lake("gold", "facility_first_report")):
        prev = _read(spark, "gold", "facility_first_report")
        typical = (typical.join(prev.select("facility_id", F.col("first_seen").alias("prev_seen")), "facility_id", "left")
                   .withColumn("first_seen", F.least("first_seen", F.coalesce("prev_seen", "first_seen"))).drop("prev_seen"))
    typical = typical.cache()
    ret = (fday.join(typical, "facility_id")
           .where((F.col("gap") >= dcfg["backfill_gap_days"]) & (F.col("median_gap") <= 2))
           .select("facility_id", "d", F.lit(True).alias("return_day")))
    hist_start = _read(spark, "silver", "encounters").agg(F.min("first_report_date")).first()[0]

    enc = _cases(spark, guard)
    enc = (enc.join(F.broadcast(ret), (enc.facility_id == ret.facility_id) & (enc.first_report_date == ret.d), "left")
           .drop(ret.facility_id).drop("d")
           .join(F.broadcast(typical.select("facility_id", "first_seen")), "facility_id", "left"))
    backfill = F.coalesce(F.col("return_day"), F.lit(False)) & (F.datediff("first_report_date", "onset_date") >= dcfg["backfill_min_delay"])
    new_fac = (F.datediff("first_report_date", "first_seen") < 56) & (F.datediff("first_seen", F.lit(hist_start)) > 56)
    agg = (enc.groupBy("adm2_pcode", "disease_group", F.col("first_report_date").alias("date"))
           .agg(F.count("*").alias("n_naive"),
                F.sum(F.when(~backfill & ~F.col("is_duplicate"), 1).otherwise(0)).alias("n_signal"),
                F.sum(F.when(backfill, 1).otherwise(0)).alias("n_backfill"),
                F.sum(F.when(new_fac, 1).otherwise(0)).alias("n_new_facility"),
                F.array_sort(F.collect_set("_batch_id")).alias("bronze_batch_ids")))
    lo = since or hist_start
    dist = _read(spark, "silver", "districts").select("adm2_pcode", "adm1_pcode")
    days = spark.sql(f"SELECT explode(sequence(to_date('{lo}'), to_date('{as_of}'))) AS date")
    grid = days.crossJoin(dist).crossJoin(spark.createDataFrame([(g,) for g in TARGETS], "disease_group string"))
    out = (grid.join(agg, ["adm2_pcode", "disease_group", "date"], "left")
           .fillna(0, ["n_naive", "n_signal", "n_backfill", "n_new_facility"])
           .withColumn("bronze_batch_ids", F.coalesce("bronze_batch_ids", F.array().cast("array<string>")))
           .withColumn("new_facility_share", F.when(F.col("n_naive") > 0, F.col("n_new_facility") / F.col("n_naive")).otherwise(0.0))
           .withColumn("year", F.year("date")))
    _write(spark, out, "daily_signal", ["year"], f"date >= '{lo}'" if since else None)
    _write(spark, typical.select("facility_id", "first_seen", "median_gap"), "facility_first_report", None, None)


def weather_weekly(spark: SparkSession) -> None:
    w = _read(spark, "silver", "weather_daily").withColumn("week_start", F.date_trunc("week", "date").cast("date"))
    out = w.groupBy("adm2_pcode", "week_start").agg(
        F.sum("rain_mm").alias("rain_mm"), F.avg("temp_c").alias("temp_c"), F.avg("humidity").alias("humidity"),
        F.max(F.when(F.col("rain_mm") > 0, F.col("rain_mm"))).alias("max_daily_rain_mm"),
        F.first("source_status").alias("source_status"))
    _write(spark, out, "weather_weekly", None, None)


def population(spark: SparkSession) -> None:
    pop = _read(spark, "silver", "population")
    dist = _read(spark, "silver", "districts")
    out = (pop.groupBy("adm2_pcode").agg(F.sum("population").alias("population"), F.first("source_status").alias("source_status"))
           .join(dist.select("adm2_pcode", "adm2_name", "adm1_pcode", "adm1_name", "center_lat", "center_lon"), "adm2_pcode"))
    _write(spark, out, "district_population", None, None)
    _write(spark, pop, "population_structure", None, None)


def stock_daily(spark: SparkSession, since: dt.date | None) -> None:
    s = _read(spark, "silver", "stock")
    fac = _read(spark, "silver", "facilities").select("facility_id", "beds", "icu_beds")
    if since is not None:
        s = s.where((F.col("snapshot_date") >= F.lit(since)) & (F.col("year") >= since.year))
    out = (s.join(F.broadcast(fac), "facility_id")
           .groupBy("adm2_pcode", F.col("snapshot_date").alias("date"))
           .agg(*[F.sum(c).alias(c) for c in ["iv_saline", "ns1_kits", "ors", "antimalarials", "occupied_beds"]],
                F.sum("beds").alias("beds_reporting"), F.countDistinct("facility_id").alias("facilities_reporting"),
                F.array_sort(F.collect_set("_batch_id")).alias("bronze_batch_ids"))
           .withColumn("year", F.year("date")))
    _write(spark, out, "stock_district_daily", ["year"], f"date >= '{since}'" if since else None)
    beds = fac.join(_read(spark, "silver", "facilities").select("facility_id", "adm2_pcode"), "facility_id") \
        .groupBy("adm2_pcode").agg(F.sum("beds").alias("beds_total"), F.sum("icu_beds").alias("icu_total"), F.count("*").alias("facilities"))
    _write(spark, beds, "district_capacity", None, None)


def dq_tables(spark: SparkSession, reports: list[dict], load_ts: str) -> None:
    """DQ-2: per-load quality report stored as gold tables (and exported to the dashboard)."""
    rows, rule_rows = [], []
    for r in reports:
        if not r:
            continue
        rows.append((r["batch_id"], load_ts, r["source"], r["rows_in"], r["rows_in"] - r["rows_quarantined"],
                     r["rows_quarantined"], int(r.get("duplicates_removed", 0))))
        rule_rows += [(r["batch_id"], load_ts, r["source"], x["rule_id"], x["description"], x["failed_rows"], float(x["pass_rate"]))
                      for x in r["rules"]]
    if not rows:
        return
    rep = spark.createDataFrame(rows, "batch_id string, load_ts string, source string, rows_in long, rows_out long, "
                                      "rows_quarantined long, duplicates_removed long").withColumn("load_ts", F.col("load_ts").cast("timestamp"))
    rr = spark.createDataFrame(rule_rows, "batch_id string, load_ts string, source string, rule_id string, description string, "
                                          "failed_rows long, pass_rate double").withColumn("load_ts", F.col("load_ts").cast("timestamp"))
    for df, name in ((rep, "dq_report"), (rr, "dq_rule_results")):
        loc = lake("gold", name)
        df.write.format("delta").mode("append").save(loc)
        register_table(spark, "gold", name, loc)


def lineage(spark: SparkSession) -> None:
    man = _read(spark, "bronze", "ingest_manifest")
    out = man.groupBy("batch_id", "load_ts").agg(
        F.collect_list(F.struct("source", "file_path", "checksum", "rows")).alias("files"),
        F.sum("rows").alias("rows"))
    _write(spark, out, "lineage_batches", None, None)


def build(spark: SparkSession, since: dt.date | None, sources: list[str]) -> dt.date:
    """(Re)build gold. ``since=None`` means a full rebuild; returns the as-of date of the data."""
    as_of = _read(spark, "silver", "encounters").agg(F.max("first_report_date")).first()[0]
    if since is not None:
        since = since - dt.timedelta(days=since.weekday())  # recompute whole epi-weeks
    if "public_weather" in sources or not table_exists(spark, lake("gold", "weather_weekly")):
        weather_weekly(spark)
    if "public_population" in sources or not table_exists(spark, lake("gold", "district_population")):
        population(spark)
    if "encounters" in sources:
        weekly_counts(spark, since, as_of)
        reporting_triangle(spark, since)
        daily_signal(spark, since, as_of)
    if "stock" in sources or "facilities" in sources:
        stock_daily(spark, since if "stock" in sources else None)
    lineage(spark)
    return as_of
