"""Weekly 1-4 week-ahead forecasts per district and disease (AN-4) with a rolling-origin backtest (AN-5).

Models
------
* ``glm_nb`` (operational, interpretable): one Poisson log-linear regression per district x disease x
  horizon (ridge-penalised) on the features in ``features.py``; negative-binomial dispersion estimated by
  moments gives the predictive distribution. Trained **per district in parallel** with Spark
  ``groupBy(adm2_pcode, disease_group).applyInPandas``.
* ``seasonal_naive`` (baseline): the same week last year; intervals from the empirical distribution of
  past seasonal-naive errors.
* ``hgb_global`` (complex comparator, conflict C5): gradient-boosted trees on log(1 + cases), pooled over all
  districts of a disease (``groupBy(disease_group).applyInPandas``), with NB intervals.

Nowcasting: the most recent 4 weeks are incomplete because of reporting delays. At every origin the
counts known at that time are rebuilt from ``gold.reporting_triangle`` (onset week x report week) and
divided by the national completeness of that delay estimated from earlier weeks only.

Backtest: origins are the last 52 complete weeks; models are refitted every ``refit_every`` origins on
data whose targets were complete at the origin (no look-ahead); targets in the final 3 weeks are skipped
because they are not yet complete. Metrics: MAE, 80% interval coverage and WIS.
"""
from __future__ import annotations

import datetime as dt
import warnings

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from analytics.forecast.features import FEATURES, QUANTILES, design, nb_quantiles, nb_size, wis
from pipeline.common.config import lake, load_config
from pipeline.common.spark import register_table

QCOLS = [f"q{int(q * 100):02d}" for q in QUANTILES]
OUT_SCHEMA = T.StructType(
    [T.StructField("adm2_pcode", T.StringType()), T.StructField("disease_group", T.StringType()),
     T.StructField("model", T.StringType()), T.StructField("kind", T.StringType()),
     T.StructField("origin_week", T.DateType()), T.StructField("target_week", T.DateType()),
     T.StructField("horizon", T.IntegerType()), T.StructField("observed", T.DoubleType()),
     T.StructField("mean", T.DoubleType())]
    + [T.StructField(c, T.DoubleType()) for c in QCOLS])


# --------------------------------------------------------------------------------------------- data
def panel(spark: SparkSession) -> tuple[DataFrame, dict]:
    """Weekly panel (cases, weather, triangle as a list) and national completeness by delay (prospective)."""
    cfg = load_config()["forecast"]
    wc = spark.read.format("delta").load(lake("gold", "weekly_counts")).where(F.col("disease_group").isin(cfg["diseases"]))
    tri = spark.read.format("delta").load(lake("gold", "reporting_triangle"))
    ww = spark.read.format("delta").load(lake("gold", "weather_weekly"))
    tri_list = tri.groupBy("adm2_pcode", "disease_group", "week_start").agg(
        F.collect_list(F.struct("report_week", "cases")).alias("tri"))
    df = (wc.select("adm2_pcode", "disease_group", "week_start", "cases")
          .join(ww.select("adm2_pcode", "week_start", "rain_mm", "temp_c", "max_daily_rain_mm"), ["adm2_pcode", "week_start"], "left")
          .join(tri_list, ["adm2_pcode", "disease_group", "week_start"], "left"))
    # national completeness c_k(origin): share of final cases reported within k weeks of the onset week
    nat = (tri.withColumn("k", (F.datediff("report_week", "week_start") / 7).cast("int"))
           .groupBy("disease_group", "week_start", "k").agg(F.sum("cases").alias("n")).toPandas())
    return df, _completeness(nat)


def _completeness(nat: pd.DataFrame) -> dict:
    """{disease: DataFrame(index=origin week, columns k=0..3)} using onset weeks 8-60 weeks before the origin."""
    out = {}
    for g, d in nat.groupby("disease_group"):
        piv = d.pivot_table(index="week_start", columns="k", values="n", aggfunc="sum").fillna(0).sort_index()
        tot = piv.sum(axis=1)
        cum = piv.cumsum(axis=1).div(tot.replace(0, np.nan), axis=0)
        comp = {}
        weeks = piv.index
        for i, w in enumerate(weeks):
            ref = cum.iloc[max(0, i - 60): max(0, i - 8)]
            comp[pd.Timestamp(w)] = [float(np.nanmean(ref[k])) if (len(ref) and k in ref) else 1.0 for k in range(4)]
        out[g] = comp
    return out


def _known_as_of(tri_rows, origin: pd.Timestamp) -> float:
    if tri_rows is None:
        return 0.0
    return float(sum(r["cases"] for r in tri_rows if pd.Timestamp(r["report_week"]) <= origin))


# --------------------------------------------------------------------------------------------- per-district models
def district_udf(completeness: dict, cfg: dict, as_of: dt.date):
    horizons = cfg["horizons"]
    backtest_weeks = int(cfg["backtest_weeks"])
    refit_every = int(cfg["refit_every"])
    min_train = int(cfg["min_train_weeks"])

    def fn(pdf: pd.DataFrame) -> pd.DataFrame:
        from sklearn.linear_model import PoissonRegressor

        warnings.filterwarnings("ignore")
        pdf = pdf.sort_values("week_start").reset_index(drop=True)
        pdf["week_start"] = pd.to_datetime(pdf["week_start"])
        adm2, dis = pdf.adm2_pcode.iloc[0], pdf.disease_group.iloc[0]
        as_of_ts = pd.Timestamp(as_of)
        last_complete = pdf.index[pdf.week_start + pd.Timedelta(days=6) <= as_of_ts].max()
        y = pdf["cases"].to_numpy(dtype=float)
        weeks = pdf["week_start"]
        rain, temp, mx = (pdf[c].to_numpy(dtype=float) for c in ("rain_mm", "temp_c", "max_daily_rain_mm"))
        comp = completeness.get(dis, {})
        rows = []
        origins = list(range(last_complete - backtest_weeks, last_complete + 1))
        designs = {h: design(y, weeks, rain, temp, mx, h) for h in horizons}
        models: dict[int, tuple] = {}
        for oi, t in enumerate(origins):
            origin = weeks[t]
            final_eval = t == last_complete
            # --- nowcast the last 4 weeks as known at the origin
            y_known = y.copy()
            c = comp.get(origin, [1.0] * 4)
            for back in range(4):
                i = t - back
                known = _known_as_of(pdf.at[i, "tri"], origin)
                y_known[i] = known / max(c[back], 0.2)
            for h in horizons:
                X = designs[h]
                if oi % refit_every == 0 or h not in models or final_eval:
                    train = X.iloc[: max(0, t - 3 - h + 1)].dropna()
                    train = train[train.index >= 0]
                    if len(train) < min_train // 2:
                        models[h] = None
                    else:
                        mu_x, sd_x = train[FEATURES].mean(), train[FEATURES].std().replace(0, 1).fillna(1)
                        m = PoissonRegressor(alpha=0.02, max_iter=500)
                        Xt = ((train[FEATURES] - mu_x) / sd_x).to_numpy()
                        m.fit(Xt, train["target"].to_numpy())
                        k = nb_size(train["target"].to_numpy(), m.predict(Xt))
                        idx = train.index.to_numpy()
                        idx = idx[idx + h - 52 >= 0]
                        resid = y[idx + h] - y[idx + h - 52]  # past seasonal-naive errors (same horizon)
                        models[h] = (m, mu_x, sd_x, k, resid)
                if models.get(h) is None:
                    continue
                m, mu_x, sd_x, k, resid = models[h]
                xrow = X.loc[[t], FEATURES].copy()
                xrow["lag_0"] = np.log1p(max(y_known[t], 0))
                xrow["lag_1"] = np.log1p(max(y_known[t - 1], 0))
                xrow["lag_2_5"] = np.log1p(np.mean(y_known[t - 5: t - 1]))
                if xrow.isna().any(axis=None):
                    xrow = xrow.fillna(mu_x)
                mu = float(m.predict(((xrow - mu_x) / sd_x).to_numpy())[0])
                q = nb_quantiles(np.array([mu]), k)[0]
                target_i = t + h
                target_week = origin + pd.Timedelta(weeks=h)
                observed = float(y[target_i]) if target_i <= last_complete - 3 else np.nan
                kind = "forecast" if final_eval else "backtest"
                if kind == "backtest" and np.isnan(observed):
                    continue
                rows.append((adm2, dis, "glm_nb", kind, origin.date(), target_week.date(), h, observed, mu, *q))
                # seasonal-naive baseline: same week last year (+ empirical error quantiles)
                ly = y[target_i - 52] if target_i - 52 >= 0 else np.nan
                if not np.isnan(ly):
                    qb = np.maximum(ly + np.nanquantile(resid, QUANTILES), 0) if len(resid) > 10 else np.full(len(QUANTILES), ly)
                    rows.append((adm2, dis, "seasonal_naive", kind, origin.date(), target_week.date(), h, observed, float(ly), *qb))
        # nowcast rows for the dashboard (latest origin): observed-so-far and corrected estimate
        origin = weeks[last_complete]
        c = comp.get(origin, [1.0] * 4)
        for back in range(4):
            i = last_complete - back
            known = _known_as_of(pdf.at[i, "tri"], as_of_ts)
            est = known / max(c[back], 0.2)
            rows.append((adm2, dis, "nowcast", "nowcast", origin.date(), weeks[i].date(), -back, known, est,
                         *([np.nan] * len(QUANTILES))))
        return pd.DataFrame(rows, columns=[f.name for f in OUT_SCHEMA.fields])

    return fn


# --------------------------------------------------------------------------------------------- pooled comparator
def global_udf(cfg: dict, as_of: dt.date, pop: dict):
    horizons = cfg["horizons"]
    backtest_weeks = int(cfg["backtest_weeks"])
    refit_every = int(cfg["refit_every"])

    def fn(pdf: pd.DataFrame) -> pd.DataFrame:
        from sklearn.ensemble import HistGradientBoostingRegressor

        warnings.filterwarnings("ignore")
        pdf["week_start"] = pd.to_datetime(pdf["week_start"])
        dis = pdf.disease_group.iloc[0]
        frames = []
        for adm2, g in pdf.groupby("adm2_pcode"):
            g = g.sort_values("week_start").reset_index(drop=True)
            y = g["cases"].to_numpy(dtype=float)
            for h in horizons:
                X = design(y, g["week_start"], g["rain_mm"].to_numpy(float), g["temp_c"].to_numpy(float),
                           g["max_daily_rain_mm"].to_numpy(float), h)
                X["log_pop"] = np.log(pop.get(adm2, 1e5))
                X["adm2_pcode"], X["h"], X["t"], X["week_start"] = adm2, h, np.arange(len(g)), g["week_start"]
                frames.append(X)
        allx = pd.concat(frames, ignore_index=True)
        feats = FEATURES + ["log_pop"]
        weeks = sorted(allx.week_start.unique())
        as_of_ts = pd.Timestamp(as_of)
        last_complete = max(i for i, w in enumerate(weeks) if pd.Timestamp(w) + pd.Timedelta(days=6) <= as_of_ts)
        rows = []
        for h in horizons:
            sub = allx[allx.h == h]
            model = None
            for oi, t in enumerate(range(last_complete - backtest_weeks, last_complete)):
                if oi % refit_every == 0 or model is None:
                    train = sub[(sub.t <= t - 3 - h)].dropna(subset=feats + ["target"])
                    # gradient boosting on log(1 + y): trees cannot extrapolate counts, but on the log scale a
                    # large district is a shift, which keeps the pooled model stable across very different sizes
                    model = HistGradientBoostingRegressor(loss="squared_error", max_iter=300, learning_rate=0.06,
                                                          max_leaf_nodes=31, min_samples_leaf=40, random_state=0)
                    model.fit(train[feats], np.log1p(train["target"]))
                    k = nb_size(train["target"].to_numpy(), np.expm1(model.predict(train[feats])).clip(0))
                test = sub[(sub.t == t)]
                test = test[test.t + h <= last_complete - 3]
                if test.empty:
                    continue
                mu = np.expm1(model.predict(test[feats].fillna(train[feats].mean()))).clip(0)
                q = nb_quantiles(mu, k)
                for (_, r), m_, qq in zip(test.iterrows(), mu, q):
                    rows.append((r.adm2_pcode, dis, "hgb_global", "backtest", pd.Timestamp(weeks[t]).date(),
                                 (pd.Timestamp(weeks[t]) + pd.Timedelta(weeks=h)).date(), h, float(r.target), float(m_), *qq))
        return pd.DataFrame(rows, columns=[f.name for f in OUT_SCHEMA.fields])

    return fn


# --------------------------------------------------------------------------------------------- driver
def run(spark: SparkSession, as_of: dt.date, with_comparator: bool = True) -> None:
    cfg = load_config()["forecast"]
    df, completeness = panel(spark)
    df = df.repartition(64, "adm2_pcode", "disease_group")
    res = df.groupBy("adm2_pcode", "disease_group").applyInPandas(district_udf(completeness, cfg, as_of), OUT_SCHEMA)
    if with_comparator:
        pop = {r.adm2_pcode: r.population for r in spark.read.format("delta").load(lake("gold", "district_population")).collect()}
        glob = (df.drop("tri").groupBy("disease_group").applyInPandas(global_udf(cfg, as_of, pop), OUT_SCHEMA))
        res = res.unionByName(glob)
    loc = lake("gold", "forecast_all")
    res.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy("kind").save(loc)
    register_table(spark, "gold", "forecast_all", loc)
    allf = spark.read.format("delta").load(loc)
    for kind, name in (("forecast", "forecasts"), ("backtest", "forecast_backtest"), ("nowcast", "nowcasts")):
        loc = lake("gold", name)
        allf.where(F.col("kind") == kind).write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(loc)
        register_table(spark, "gold", name, loc)
    metrics(spark)


def metrics(spark: SparkSession) -> pd.DataFrame:
    bt = spark.read.format("delta").load(lake("gold", "forecast_backtest")).toPandas()
    q = bt[QCOLS].to_numpy()
    yv = bt["observed"].to_numpy()
    bt["abs_err"] = np.abs(yv - bt["mean"].to_numpy())
    bt["abs_err_median"] = np.abs(yv - bt["q50"].to_numpy())
    bt["covered80"] = (yv >= bt["q10"].to_numpy()) & (yv <= bt["q90"].to_numpy())
    bt["wis"] = wis(yv, q)
    # models are compared on the SAME (district, disease, origin, horizon) cells
    key = ["adm2_pcode", "disease_group", "origin_week", "horizon"]
    common = bt.groupby(key)["model"].nunique()
    n_models = bt["model"].nunique()
    keep = common[common == n_models].index
    bt = bt.set_index(key).loc[keep].reset_index()
    agg = (bt.groupby(["model", "disease_group", "horizon"])
           .agg(mae=("abs_err", "mean"), mae_median=("abs_err_median", "mean"), coverage80=("covered80", "mean"),
                wis=("wis", "mean"), n=("wis", "size")).reset_index())
    overall = (bt.groupby(["model", "horizon"]).agg(mae=("abs_err", "mean"), mae_median=("abs_err_median", "mean"),
                                                    coverage80=("covered80", "mean"), wis=("wis", "mean"), n=("wis", "size"))
               .reset_index().assign(disease_group="all"))
    agg = pd.concat([agg, overall], ignore_index=True)
    base = agg[agg.model == "seasonal_naive"].set_index(["disease_group", "horizon"])["wis"]
    agg["relative_wis_vs_baseline"] = agg.apply(lambda r: r.wis / base.get((r.disease_group, r.horizon), np.nan), axis=1)
    sdf = spark.createDataFrame(agg)
    loc = lake("gold", "forecast_metrics")
    sdf.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(loc)
    register_table(spark, "gold", "forecast_metrics", loc)
    return agg
