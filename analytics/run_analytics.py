"""Run every analytics stage on the current gold layer and write results (gold tables + data/results).

    python -m analytics.run_analytics [--stages detect,evaluate,forecast,demand,optimise,calendar,seir,privacy]

Nothing here hard-codes a date: the as-of date is the latest first-report date in silver, the forecast
origin is the last complete epi-week before it, and the calendar covers the 52 weeks that follow.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import time
from pathlib import Path

import numpy as np
import pandas as pd
from pyspark.sql import functions as F

from analytics.calendar import calendar
from analytics.demand import demand
from analytics.detection import detect
from analytics.forecast import forecast
from analytics.optimise import allocate
from analytics.privacy import kanon
from analytics.scenario import seir
from pipeline.common.config import lake, load_config, path
from pipeline.common.spark import get_spark, register_table, table_exists

ALL = ["detect", "evaluate", "forecast", "demand", "optimise", "calendar", "seir", "privacy", "dedup_eval"]


def results_dir() -> Path:
    p = Path(path("results"))
    p.mkdir(parents=True, exist_ok=True)
    return p


def _gold(spark, t):
    return spark.read.format("delta").load(lake("gold", t))


def as_of_date(spark) -> dt.date:
    return spark.read.format("delta").load(lake("silver", "encounters")).agg(F.max("first_report_date")).first()[0]


def operating_point() -> dict:
    f = results_dir() / "detection_summary.json"
    if f.exists():
        return json.loads(f.read_text())["operating_point"]
    op = load_config()["detection"]["operating_point"]
    return {"method": op["method"], "threshold": float(op["threshold"])}


def stage_detect(spark, as_of):
    detect.run(spark)


def stage_evaluate(spark, as_of):
    from analytics.evaluation import detection_eval

    det = _gold(spark, "detection_daily").drop("bronze_batch_ids").toPandas()
    summ = detection_eval.run(det, results_dir())
    op = summ["operating_point"]
    detect.publish_alerts(spark, op["method"], op["threshold"])


def stage_forecast(spark, as_of):
    forecast.run(spark, as_of, with_comparator=True)
    forecast.metrics(spark).to_csv(results_dir() / "forecast_metrics.csv", index=False)


def stage_demand(spark, as_of):
    demand.run(spark)


def _problem_inputs(spark, as_of):
    dem = _gold(spark, "demand").toPandas()
    st = _gold(spark, "stock_district_daily")
    last = st.agg(F.max("date")).first()[0]
    stock = st.where(F.col("date") == F.lit(last)).toPandas()
    pop = _gold(spark, "district_population").toPandas()
    det = _gold(spark, "detection_daily").where(F.col("date") > F.date_sub(F.lit(as_of), 7)).drop("bronze_batch_ids").toPandas()
    fc = _gold(spark, "forecasts").where(F.col("model") == "glm_nb").toPandas()
    wc = _gold(spark, "nowcasts").toPandas()
    cur = wc[wc.horizon == 0].groupby("adm2_pcode")["mean"].sum()
    fut = fc[fc.horizon == 4].groupby("adm2_pcode")["mean"].sum()
    growth = pd.DataFrame({"adm2_pcode": fut.index, "growth": (fut / cur.reindex(fut.index).clip(lower=1) - 1).to_numpy()})
    return dem, stock, pop, det, growth, last


def stage_optimise(spark, as_of):
    dem, stock, pop, det, growth, stock_date = _problem_inputs(spark, as_of)
    op = operating_point()
    cfg = load_config()["detection"]
    active = det[detect_mask(det, op["method"], op["threshold"], cfg)][["adm2_pcode", "disease_group"]].drop_duplicates()
    prob = allocate.build_problem(dem, stock, pop, active, growth)
    out = allocate.write_outputs(prob, results_dir())
    # what-if grid over detection thresholds (the alert set changes weights and team demand)
    thrs = cfg["cusum_thresholds"] if op["method"] == "cusum" else cfg["ears_thresholds"]
    thrs = sorted({t for t in thrs if op["threshold"] / 3 <= t <= op["threshold"] * 2} | {op["threshold"]})
    probs = {}
    for t in thrs:
        act = det[detect_mask(det, op["method"], t, cfg)][["adm2_pcode", "disease_group"]].drop_duplicates()
        probs[t] = allocate.build_problem(dem, stock, pop, act, growth)
    allocate.scenario_grid(probs, results_dir())
    for name, df in (("allocation", out["alloc"]), ("allocation_teams", out["teams"]), ("efficiency_equity", out["curve"])):
        loc = lake("gold", name)
        spark.createDataFrame(df.astype({c: float for c in df.columns if df[c].dtype == "int64"})).write.format("delta") \
            .mode("overwrite").option("overwriteSchema", "true").save(loc)
        register_table(spark, "gold", name, loc)
    s = out["summary"]
    s["stock_snapshot_date"] = str(stock_date)
    s["operating_point"] = op
    (results_dir() / "allocation_summary.json").write_text(json.dumps(s, indent=2, default=float))


def detect_mask(det: pd.DataFrame, method: str, thr: float, cfg: dict) -> np.ndarray:
    from analytics.evaluation.detection_eval import alert_mask

    return alert_mask(det, method, thr, False, int(cfg["min_count"]))


def stage_calendar(spark, as_of):
    calendar.run(spark, as_of).to_csv(results_dir() / "preparedness_calendar.csv", index=False)


def stage_seir(spark, as_of):
    wc = _gold(spark, "weekly_counts").where(F.col("disease_group") == "ili")
    nat = wc.groupBy("week_start").agg(F.sum("cases").alias("cases"), F.sum("admitted").alias("admitted")).toPandas()
    last = nat.week_start.max()
    nat = nat[nat.week_start < last]
    nat_b, window = seir.find_surge(nat)
    pop = float(_gold(spark, "district_population").agg(F.sum("population")).first()[0])
    params = seir.fit(window, pop)
    adm_rate = float(window.admitted.sum() / max(window.cases.sum(), 1))
    # detection week of the surge = first week in the window with any ILI alert at the operating point
    al = _gold(spark, "alerts").where(F.col("disease_group") == "ili").select("date").toPandas()
    wk0 = pd.Timestamp(window.week_start.iloc[0])
    first_alert = pd.to_datetime(al.date)
    first_alert = first_alert[first_alert >= wk0].min() if len(al) else pd.NaT
    detect_week = int((first_alert - wk0).days // 7) if pd.notna(first_alert) else 2
    sc = seir.scenarios(params, pop, adm_rate, load_config()["demand"]["los_days"]["ili"], detect_week)
    sc.to_csv(results_dir() / "seir_scenarios.csv", index=False)
    params.update({"admission_rate": adm_rate, "detect_week": detect_week, "first_alert": str(first_alert.date()) if pd.notna(first_alert) else None})
    (results_dir() / "seir_fit.json").write_text(json.dumps(params, indent=2))


def stage_privacy(spark, as_of):
    rep = kanon.check(spark, disease="malaria") + kanon.check(spark, disease="dengue", write_extract=True)
    (results_dir() / "kanonymity.json").write_text(json.dumps(rep, indent=2, default=str))


def stage_dedup_eval(spark, as_of):
    """DQ-4 precision/recall against the generator's injected duplicate visits (evaluation only)."""
    truth_f = Path(path("ground_truth")) / "patient_pairs.csv"
    if not truth_f.exists():
        return
    truth = spark.read.option("header", True).csv(str(truth_f))
    links = spark.read.format("delta").load(lake("silver", "episode_links")).select("encounter_id", "duplicate_of")
    enc = spark.read.format("delta").load(lake("silver", "encounters")).select("encounter_id")
    t = (truth.join(enc.withColumnRenamed("encounter_id", "encounter_id_a"), "encounter_id_a", "left_semi")
         .join(enc.withColumnRenamed("encounter_id", "encounter_id_b"), "encounter_id_b", "left_semi"))
    dup = t.where(F.col("label") == "duplicate_visit")
    pred_pairs = links.select(F.least("encounter_id", "duplicate_of").alias("a"), F.greatest("encounter_id", "duplicate_of").alias("b"))
    true_pairs = dup.select(F.least("encounter_id_a", "encounter_id_b").alias("a"), F.greatest("encounter_id_a", "encounter_id_b").alias("b"))
    distinct_pairs = t.where(F.col("label") == "distinct_episode").select(
        F.least("encounter_id_a", "encounter_id_b").alias("a"), F.greatest("encounter_id_a", "encounter_id_b").alias("b"))
    tp = pred_pairs.join(true_pairs, ["a", "b"], "inner").count()
    n_pred, n_true = pred_pairs.count(), true_pairs.count()
    # predicted links involving at least one injected encounter (the population we can score precisely)
    inj = true_pairs.select(F.col("b").alias("id")).union(true_pairs.select(F.col("a").alias("id"))).distinct()
    scored = pred_pairs.join(inj.withColumnRenamed("id", "b"), "b", "left_semi")
    merged_distinct = pred_pairs.join(distinct_pairs, ["a", "b"], "inner").count()
    res = {"true_duplicate_pairs": n_true, "predicted_links": n_pred, "true_positives": tp,
           "precision_all_links": tp / max(n_pred, 1), "recall": tp / max(n_true, 1),
           "precision_on_injected": tp / max(scored.count(), 1),
           "distinct_episodes_wrongly_merged": merged_distinct, "distinct_episode_pairs": distinct_pairs.count(),
           "note": "all predicted links are counted as positives; links between records not injected as duplicates "
                   "are real same-patient repeats created by chance or genuine false merges - precision_all_links is the conservative figure"}
    (results_dir() / "dedup_eval.json").write_text(json.dumps(res, indent=2))


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--stages", default=",".join(ALL))
    args = ap.parse_args(argv)
    spark = get_spark("analytics")
    as_of = as_of_date(spark)
    timings = {}
    for st in args.stages.split(","):
        t = time.time()
        globals()[f"stage_{st}"](spark, as_of)
        timings[st] = round(time.time() - t, 1)
        print(f"[analytics] {st:10s} {timings[st]:7.1f}s", flush=True)
    (results_dir() / f"analytics_timings_{dt.datetime.now():%Y%m%dT%H%M%S}.json").write_text(json.dumps(
        {"as_of": str(as_of), "stage_seconds": timings}, indent=2))


if __name__ == "__main__":
    main()
