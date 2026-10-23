"""Convert case forecasts into resource demand with uncertainty (AN-6).

For every district the 1-4 week case forecasts (quantiles) of each disease are turned into demand for beds,
IV saline, NS1 kits, ORS and antimalarials over the planning horizon by Monte-Carlo simulation:

1. draw weekly cases by inverse-CDF sampling of the forecast quantiles (linear interpolation, clipped tails);
2. admissions ~ Binomial(cases, admission rate), the rate being the district's observed rate over the last
   52 weeks shrunk towards the national rate (m = 50 pseudo-cases) so small districts are not erratic;
3. apply the conversion assumptions in ``config/episcope.yaml`` (``demand``), e.g. 6 IV-saline bags per
   admitted dengue case, 10 ORS sachets per diarrhoea case, 1.3 NS1 kits per suspected dengue case;
4. bed demand = peak weekly (admissions x length of stay / 7).

The output keeps the mean and the 10/50/80/90% quantiles so that the optimiser plans for an upper quantile
(``planning_quantile``) and the dashboard can show the uncertainty.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from analytics.forecast.features import QUANTILES
from analytics.forecast.forecast import QCOLS
from pipeline.common.config import lake, load_config
from pipeline.common.spark import register_table

RESOURCES = ["beds", "iv_saline", "ns1_kits", "ors", "antimalarial"]
OUT = T.StructType([T.StructField("adm2_pcode", T.StringType()), T.StructField("resource", T.StringType()),
                    T.StructField("mean", T.DoubleType()), T.StructField("q10", T.DoubleType()),
                    T.StructField("q50", T.DoubleType()), T.StructField("q80", T.DoubleType()),
                    T.StructField("q90", T.DoubleType()), T.StructField("horizon_weeks", T.IntegerType())])


def _sample(qvals: np.ndarray, u: np.ndarray) -> np.ndarray:
    """Inverse-CDF sampling from quantiles (piecewise linear; tails extended linearly to 0 and 99.9%)."""
    qs = np.r_[0.0, QUANTILES, 0.999]
    lo = max(0.0, qvals[0] - (qvals[1] - qvals[0]))
    hi = qvals[-1] + (qvals[-1] - qvals[-2]) * 1.5
    return np.interp(u, qs, np.r_[lo, qvals, hi])


def demand_udf(cfg: dict, rates: dict, national: dict, seed: int):
    n = int(cfg["mc_draws"])

    def fn(pdf: pd.DataFrame) -> pd.DataFrame:
        adm2 = pdf.adm2_pcode.iloc[0]
        rng = np.random.default_rng([seed, int(adm2[2:])])
        weeks = sorted(pdf.target_week.unique())
        H = len(weeks)
        cases = {g: np.zeros((n, H)) for g in ["dengue", "diarrhoea", "ili", "malaria"]}
        for _, r in pdf.iterrows():
            h = weeks.index(r.target_week)
            u = rng.random(n)
            cases[r.disease_group][:, h] = np.maximum(_sample(r[QCOLS].to_numpy(dtype=float), u), 0)
        adm = {}
        for g, c in cases.items():
            p = rates.get((adm2, g), national.get(g, 0.1))
            adm[g] = rng.binomial(np.round(c).astype(int), p)
        beds_week = sum(adm[g] * cfg["los_days"][g] / 7.0 for g in adm)
        out = {
            "beds": beds_week.max(axis=1),
            "iv_saline": sum(adm[g].sum(axis=1) * cfg["iv_saline_per_admit"].get(g, 0) for g in adm)
                         + cases["diarrhoea"].sum(axis=1) * cfg["iv_saline_per_case_outpatient"]["diarrhoea"],
            "ns1_kits": cases["dengue"].sum(axis=1) * cfg["ns1_kits_per_case"]["dengue"],
            "ors": cases["diarrhoea"].sum(axis=1) * cfg["ors_per_case"]["diarrhoea"],
            "antimalarial": cases["malaria"].sum(axis=1) * cfg["antimalarial_per_case"]["malaria"],
        }
        rows = [(adm2, k, float(v.mean()), *[float(np.quantile(v, q)) for q in (0.1, 0.5, 0.8, 0.9)], H) for k, v in out.items()]
        return pd.DataFrame(rows, columns=[f.name for f in OUT.fields])

    return fn


def run(spark: SparkSession) -> None:
    cfg = load_config()["demand"]
    fc = (spark.read.format("delta").load(lake("gold", "forecasts")).where(F.col("model") == "glm_nb")
          .select("adm2_pcode", "disease_group", "target_week", *QCOLS))
    wc = spark.read.format("delta").load(lake("gold", "weekly_counts"))
    last = wc.agg(F.max("week_start")).first()[0]
    recent = wc.where(F.col("week_start") > F.date_sub(F.lit(last), 364))
    by_d = recent.groupBy("adm2_pcode", "disease_group").agg(F.sum("admitted").alias("a"), F.sum("cases").alias("c")).toPandas()
    nat = by_d.groupby("disease_group")[["a", "c"]].sum()
    national = (nat.a / nat.c.clip(lower=1)).to_dict()
    m = 50.0
    rates = {(r.adm2_pcode, r.disease_group): (r.a + m * national[r.disease_group]) / (r.c + m) for r in by_d.itertuples()}
    res = fc.groupBy("adm2_pcode").applyInPandas(demand_udf(cfg, rates, national, int(load_config()["generator"]["seed"])), OUT)
    loc = lake("gold", "demand")
    res.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(loc)
    register_table(spark, "gold", "demand", loc)
    rate_df = spark.createDataFrame(pd.DataFrame([(k[0], k[1], v) for k, v in rates.items()],
                                                 columns=["adm2_pcode", "disease_group", "admission_rate"]))
    loc = lake("gold", "admission_rates")
    rate_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(loc)
    register_table(spark, "gold", "admission_rates", loc)
