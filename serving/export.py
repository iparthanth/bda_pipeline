"""Gold -> serving exports: small, aggregated, suppressed JSON bundles per role (VIS-4, PRV-2, PRV-3).

    python -m serving.export

Writes ``serving/out/<bundle>/*.json`` where bundle = ``national`` or ``district_<pcode>`` for every
district role in ``config/roles.yaml``. The browser only ever receives these files (through the
token-checking server), never line-level data. Aggregation levels (justified in the report):

* epi-week x district x disease for counts (the finest level that is still actionable for districts and
  keeps the payload small: 64 x 4 x ~262 weeks);
* division and national aggregates of the same cells (secondary suppression protects the hierarchy);
* forecasts, demand and allocations at district level for the next 4 weeks; detection statistics for the
  last ``alert_days`` days (statistics, not counts).
"""
from __future__ import annotations

import datetime as dt
import json
import shutil
from pathlib import Path

import numpy as np
import pandas as pd
import yaml
from pyspark.sql import functions as F

from analytics.forecast.forecast import QCOLS
from pipeline.common.config import REPO_ROOT, lake, load_config, path
from pipeline.common.spark import get_spark
from serving.suppress import hierarchical, primary, suppress_array

OUT = Path(path("serving_out"))
DISEASES = ["dengue", "diarrhoea", "ili", "malaria"]


def _gold(spark, t: str) -> pd.DataFrame:
    return spark.read.format("delta").load(lake("gold", t)).toPandas()


def _dump(obj, f: Path) -> None:
    f.parent.mkdir(parents=True, exist_ok=True)
    f.write_text(json.dumps(obj, separators=(",", ":"), default=_default))


def _default(o):
    if isinstance(o, (np.integer,)):
        return int(o)
    if isinstance(o, (np.floating,)):
        return None if np.isnan(o) else round(float(o), 4)
    if isinstance(o, (dt.date, pd.Timestamp)):
        return str(o)[:10]
    if isinstance(o, np.bool_):
        return bool(o)
    raise TypeError(type(o))


def _k(v) -> str:
    """Number formatted like JavaScript's String(number) (5.0 -> '5', 0.25 -> '0.25') for scenario keys."""
    return format(float(v), "g")


def _r(x, nd=2):
    return None if x is None or (isinstance(x, float) and np.isnan(x)) else round(float(x), nd)


# ------------------------------------------------------------------------------------------ builders
def build_weekly(wc: pd.DataFrame, dist: pd.DataFrame, n_weeks: int) -> dict:
    weeks = sorted(wc.week_start.unique())[-n_weeks:]
    wc = wc[wc.week_start.isin(weeks)]
    pcodes = dist.adm2_pcode.tolist()
    divs = sorted(dist.adm1_pcode.unique())
    gidx = np.array([divs.index(d) for d in dist.adm1_pcode])
    series: dict[str, list] = {}
    for dis in DISEASES:
        piv = (wc[wc.disease_group == dis].pivot_table(index="adm2_pcode", columns="week_start", values="cases", aggfunc="sum")
               .reindex(index=pcodes, columns=weeks).fillna(0).to_numpy())
        dvals = np.full(piv.shape, None, dtype=object)
        pvals = np.full((len(divs), len(weeks)), None, dtype=object)
        nvals = []
        for w in range(len(weeks)):
            cm, parent, pm, nat = hierarchical(piv[:, w], gidx)
            for i in range(len(pcodes)):
                dvals[i, w] = None if cm[i] else int(piv[i, w])
            for p in range(len(divs)):
                pvals[p, w] = None if pm[p] else int(parent[p])
            nvals.append(None if nat[1] else int(nat[0]))
        for i, pc in enumerate(pcodes):
            series[f"{pc}|{dis}"] = dvals[i].tolist()
        for p, dv in enumerate(divs):
            series[f"{dv}|{dis}"] = pvals[p].tolist()
        series[f"BD|{dis}"] = nvals
    return {"weeks": [str(w)[:10] for w in weeks], "series": series}


def build_forecasts(fc: pd.DataFrame, now: pd.DataFrame, dist: pd.DataFrame) -> dict:
    out: dict[str, dict] = {}
    fc = fc[fc.model == "glm_nb"]
    for (pc, dis), g in fc.groupby(["adm2_pcode", "disease_group"]):
        g = g.sort_values("horizon")
        out[f"{pc}|{dis}"] = {"weeks": [str(w)[:10] for w in g.target_week],
                              "mean": suppress_array(g["mean"]), **{c: suppress_array(g[c]) for c in QCOLS}}
    # division and national aggregates: sum of means, normal approximation for the spread (independence)
    z = {"q05": -1.645, "q10": -1.2816, "q25": -0.6745, "q50": 0.0, "q75": 0.6745, "q90": 1.2816, "q95": 1.645}
    fc = fc.merge(dist[["adm2_pcode", "adm1_pcode"]], on="adm2_pcode")
    fc["var"] = ((fc.q90 - fc.q10) / 2.5631) ** 2
    for level, key in (("adm1_pcode", None), (None, "BD")):
        grp = fc.groupby([level, "disease_group", "target_week"]) if level else fc.groupby(["disease_group", "target_week"])
        agg = grp.agg(mean=("mean", "sum"), var=("var", "sum")).reset_index()
        keys = ["adm1_pcode", "disease_group"] if level else ["disease_group"]
        for k, g in agg.groupby(keys):
            name = f"{k[0]}|{k[1]}" if level else f"BD|{k if isinstance(k, str) else k[0]}"
            g = g.sort_values("target_week")
            sd = np.sqrt(g["var"].to_numpy())
            out[name] = {"weeks": [str(w)[:10] for w in g.target_week], "mean": suppress_array(g["mean"]),
                         **{c: suppress_array(np.maximum(g["mean"].to_numpy() + z[c] * sd, 0)) for c in QCOLS}}
    nc = {}
    for (pc, dis), g in now.groupby(["adm2_pcode", "disease_group"]):
        g = g.sort_values("target_week")
        # "reported so far" is NOT repeated here: it is the weekly series value, which carries the complementary
        # suppression; publishing it again with primary suppression only could reveal a protected cell
        nc[f"{pc}|{dis}"] = {"weeks": [str(w)[:10] for w in g.target_week], "nowcast": suppress_array(g["mean"])}
    # division and national nowcasts = sums of the district values (primary suppression applied after summing)
    nw = now.merge(dist[["adm2_pcode", "adm1_pcode"]], on="adm2_pcode")
    for level in ("adm1_pcode", None):
        keys = [level, "disease_group"] if level else ["disease_group"]
        agg = nw.groupby(keys + ["target_week"]).agg(est=("mean", "sum")).reset_index()
        for k, g in agg.groupby(keys):
            name = f"{k[0]}|{k[1]}" if level else f"BD|{k if isinstance(k, str) else k[0]}"
            g = g.sort_values("target_week")
            nc[name] = {"weeks": [str(w)[:10] for w in g.target_week], "nowcast": suppress_array(g["est"])}
    return {"forecast": out, "nowcast": nc}


def build_alerts(det: pd.DataFrame, fc: pd.DataFrame, now: pd.DataFrame, op: dict, days: int) -> dict:
    cfg = load_config()["detection"]
    method, thr = op["method"], float(op["threshold"])
    det = det.sort_values("date")
    last = det.date.max()
    recent = det[det.date > last - pd.Timedelta(days=days)]
    fc1 = fc[(fc.model == "glm_nb") & (fc.horizon == 1)].set_index(["adm2_pcode", "disease_group"])["mean"]
    fc4 = fc[(fc.model == "glm_nb") & (fc.horizon == 4)].set_index(["adm2_pcode", "disease_group"])["mean"]
    nw = now[now.horizon == 0].set_index(["adm2_pcode", "disease_group"])["mean"]
    rows, stats = [], {}
    for (pc, dis), g in recent.groupby(["adm2_pcode", "disease_group"]):
        g = g.sort_values("date")
        stat = g[method].to_numpy()
        eligible = (g.raw7.to_numpy() >= int(cfg["min_count"]))
        newfac = (g.reporting_flag.fillna("") == "new_facility").to_numpy()
        alert = (stat >= thr) & eligible & ~newfac
        run = 0
        for a in alert[::-1]:
            if not a:
                break
            run += 1
        cur = g.iloc[-1]
        status = ("verify_reporting" if (stat[-1] >= thr and eligible[-1] and newfac[-1]) else
                  "alert" if alert[-1] else "watch" if stat[-1] >= 0.5 * thr and eligible[-1] else "normal")
        f1, f4, n0 = fc1.get((pc, dis), np.nan), fc4.get((pc, dis), np.nan), nw.get((pc, dis), np.nan)
        rows.append({
            "adm2_pcode": pc, "disease_group": dis, "status": status, "statistic": _r(stat[-1]),
            "count7": primary(cur.raw7), "expected7": primary(cur.cusum_expected) if method == "cusum" else None,
            "forecast_next": primary(f1), "trend": _r((f4 + 1) / (n0 + 1) - 1) if not np.isnan(f4) and not np.isnan(n0) else None,
            "days_in_alert": run, "reporting_flag": cur.reporting_flag if isinstance(cur.reporting_flag, str) else None,
            "last_alert": str(g.date[alert].max())[:10] if alert.any() else None,
        })
        stats[f"{pc}|{dis}"] = {"stat": [_r(v) for v in stat], "eligible": eligible.astype(int).tolist(),
                                "new_facility": newfac.astype(int).tolist()}
    rank = {"alert": 0, "verify_reporting": 1, "watch": 2, "normal": 3}
    rows.sort(key=lambda r: (rank[r["status"]], -(r["statistic"] or 0)))
    return {"as_of": str(last)[:10], "method": method, "threshold": thr, "min_count": int(cfg["min_count"]),
            "dates": [str(d)[:10] for d in sorted(recent.date.unique())], "table": rows, "stats": stats}


def build_thresholds(det: pd.DataFrame, weeks: list[str]) -> dict:
    """Weekly alert-threshold line: expected 7-day count + 2 SD at the end of each week (historical limits)."""
    d = det[pd.to_datetime(det.date).dt.dayofweek == 6].copy()
    d["week_start"] = (pd.to_datetime(d.date) - pd.Timedelta(days=6)).dt.strftime("%Y-%m-%d")
    d = d[d.week_start.isin(set(weeks))]
    out = {}
    for (pc, dis), g in d.groupby(["adm2_pcode", "disease_group"]):
        m = dict(zip(g.week_start, g.cusum_expected + 2 * g.cusum_sd))
        out[f"{pc}|{dis}"] = suppress_array([m.get(w) for w in weeks])
    return out


def build_allocation(alloc: pd.DataFrame, dem: pd.DataFrame, teams: pd.DataFrame, grid: pd.DataFrame,
                     curve: pd.DataFrame, summary: dict) -> dict:
    dm = dem.set_index(["adm2_pcode", "resource"])
    rows = []
    for r in alloc.itertuples():
        d = dm.loc[(r.adm2_pcode, r.resource)] if (r.adm2_pcode, r.resource) in dm.index else None
        dvals = {f"demand_{k}": primary(d[k]) if d is not None else None for k in ("mean", "q10", "q50", "q80", "q90")}
        rows.append({"adm2_pcode": r.adm2_pcode, "resource": r.resource, **dvals, "stock": int(round(r.stock)),
                     "gap": int(round(r.gap)) if dvals["demand_q80"] is not None else None,
                     "recommended": int(round(r.recommended)), "baseline": int(round(r.baseline_population)),
                     "weight": _r(r.weight)})
    beds = dem[dem.resource == "beds"]
    bed_rows = [{"adm2_pcode": b.adm2_pcode, "demand_mean": primary(b["mean"]), "demand_q80": primary(b.q80),
                 "demand_q90": primary(b.q90)} for _, b in beds.iterrows()]
    g = grid.copy()
    res_cols = [c for c in ["iv_saline", "ns1_kits", "ors", "antimalarial"] if c in g]
    scen = {"thresholds": sorted(g.threshold.unique().tolist()), "budget_levels": sorted(g.budget_level.unique().tolist()),
            "alphas": sorted(g.alpha.unique().tolist()), "resources": res_cols, "cells": {}}
    for (t, b, a), gg in g.groupby(["threshold", "budget_level", "alpha"]):
        gg = gg.sort_values("adm2_pcode")
        scen["cells"][f"{_k(t)}|{_k(b)}|{_k(a)}"] = {"adm2": gg.adm2_pcode.tolist(),
                                         **{c: [int(round(v)) for v in gg[c]] for c in res_cols},
                                         "teams": [int(v) for v in gg.teams], "coverage": [_r(v, 3) for v in gg.coverage]}
    return {"rows": rows, "beds": bed_rows, "teams": teams.to_dict("records"), "scenarios": scen,
            "curve": curve.replace({np.nan: None}).to_dict("records"), "summary": summary}


def build_dq(rep: pd.DataFrame, rules: pd.DataFrame) -> dict:
    rep = rep.sort_values("load_ts")
    loads = [{"batch_id": r.batch_id, "load_ts": str(r.load_ts)[:19], "source": r.source, "rows_in": primary(r.rows_in),
              "rows_out": primary(r.rows_out), "rows_quarantined": primary(r.rows_quarantined),
              "duplicates_removed": primary(r.duplicates_removed)} for r in rep.itertuples()]
    rr = [{"batch_id": r.batch_id, "source": r.source, "rule_id": r.rule_id, "description": r.description,
           "failed_rows": primary(r.failed_rows), "pass_rate": _r(r.pass_rate, 5)} for r in rules.itertuples()]
    return {"loads": loads, "rules": rr}


# ------------------------------------------------------------------------------------------ role filtering
def _filter_for_district(bundle: dict, pcode: str, dist: pd.DataFrame) -> dict:
    own_div = dist.set_index("adm2_pcode").adm1_pcode[pcode]
    def keep(key: str) -> bool:
        """own district in detail; every division (4-char pcode) and the national total (BD) in aggregate"""
        code = key.split("|")[0]
        return code == pcode or len(code) == 4 or code == "BD"

    b = json.loads(json.dumps(bundle, default=_default))
    b["weekly"]["series"] = {k: v for k, v in b["weekly"]["series"].items() if keep(k)}
    b["forecasts"]["forecast"] = {k: v for k, v in b["forecasts"]["forecast"].items() if keep(k)}
    b["forecasts"]["nowcast"] = {k: v for k, v in b["forecasts"]["nowcast"].items() if keep(k)}
    b["thresholds"] = {k: v for k, v in b["thresholds"].items() if k.startswith(pcode + "|")}
    tab = b["alerts"]["table"]
    div_of = dist.set_index("adm2_pcode").adm1_pcode.to_dict()
    summary: dict[tuple, dict] = {}
    for r in tab:
        s = summary.setdefault((div_of[r["adm2_pcode"]], r["disease_group"]), {"districts_alerting": 0, "districts": 0})
        s["districts"] += 1
        s["districts_alerting"] += int(r["status"] == "alert")
    b["alerts"]["table"] = [r for r in tab if r["adm2_pcode"] == pcode]
    b["alerts"]["division_summary"] = [{"adm1_pcode": k[0], "disease_group": k[1], **v} for k, v in summary.items()]
    b["alerts"]["stats"] = {k: v for k, v in b["alerts"]["stats"].items() if k.startswith(pcode + "|")}
    al = b["allocation"]
    al["rows"] = [r for r in al["rows"] if r["adm2_pcode"] == pcode]
    al["beds"] = [r for r in al["beds"] if r["adm2_pcode"] == pcode]
    al["teams"] = [r for r in al["teams"] if r["adm2_pcode"] == pcode]
    for cell in al["scenarios"]["cells"].values():
        i = cell["adm2"].index(pcode)
        for k in list(cell):
            cell[k] = [cell[k][i]]
    b["meta"]["role"] = {"scope": "district", "district_pcode": pcode, "division_pcode": own_div}
    return b


# ------------------------------------------------------------------------------------------ main
def export() -> dict:
    cfg = load_config()
    spark = get_spark("serving")
    dist = _gold(spark, "district_population").sort_values("adm2_pcode")
    wc = _gold(spark, "weekly_counts")
    fc = _gold(spark, "forecasts")
    now = _gold(spark, "nowcasts")
    det = spark.read.format("delta").load(lake("gold", "detection_daily")).drop("bronze_batch_ids")
    last = det.agg(F.max("date")).first()[0]
    det = det.where(F.col("date") > F.lit(last - dt.timedelta(days=int(cfg["serving"]["history_weeks"]) * 7 + 7))).toPandas()
    det["date"] = pd.to_datetime(det["date"])
    res = Path(path("results"))
    op = json.loads((res / "detection_summary.json").read_text())["operating_point"]
    weekly = build_weekly(wc, dist, int(cfg["serving"]["history_weeks"]))
    bundle = {
        "weekly": weekly,
        "forecasts": build_forecasts(fc, now, dist),
        "alerts": build_alerts(det, fc, now, op, int(cfg["serving"]["alert_days"])),
        "thresholds": build_thresholds(det, weekly["weeks"]),
        "allocation": build_allocation(_gold(spark, "allocation"), _gold(spark, "demand"), _gold(spark, "allocation_teams"),
                                       pd.read_csv(res / "scenario_grid.csv"), pd.read_csv(res / "efficiency_equity.csv"),
                                       json.loads((res / "allocation_summary.json").read_text())),
        "dq": build_dq(_gold(spark, "dq_report"), _gold(spark, "dq_rule_results")),
        "calendar": pd.read_csv(res / "preparedness_calendar.csv").replace({np.nan: None}).to_dict("records"),
        "evaluation": _evaluation(res),
    }
    seir_fit = json.loads((res / "seir_fit.json").read_text())
    sc = pd.read_csv(res / "seir_scenarios.csv")
    bundle["seir"] = {"fit": {k: seir_fit[k] for k in ("beta", "R0", "rho", "r2", "week_start", "detect_week", "first_alert")}
                      | {"fitted": suppress_array(seir_fit["fitted"]), "observed_excess": suppress_array(seir_fit["observed_excess"])},
                      "scenarios": {s: {"reported_cases": suppress_array(g[g.week >= 0].reported_cases),
                                        "summary": {k: _r(v) for k, v in g[g.week < 0].iloc[0][["reported_cases", "bed_days", "peak_week", "peak_cases", "cases_averted"]].to_dict().items()}}
                                    for s, g in sc.groupby("scenario", sort=False)}}
    for s in bundle["seir"]["scenarios"].values():
        s["summary"]["reported_cases"] = primary(s["summary"]["reported_cases"])
    prov = json.loads((Path(path("landing")) / "public" / "provenance.json").read_text())
    stock_date = spark.read.format("delta").load(lake("gold", "stock_district_daily")).agg(F.max("date")).first()[0]
    weather_last = spark.read.format("delta").load(lake("silver", "weather_daily")).agg(F.max("date")).first()[0]
    bundle["meta"] = {
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "freshness": {"cases": bundle["alerts"]["as_of"], "stock": str(stock_date), "weather": str(weather_last),
                      "forecast_origin": str(fc.origin_week.max())[:10], "detection": bundle["alerts"]["as_of"]},
        "provenance": {k: v.get("status") for k, v in prov.items() if isinstance(v, dict)},
        "weather_observed_until": prov.get("S3_weather", {}).get("observed_until"),
        "synthetic_notice": "Encounter, facility and stock data are SYNTHETIC (CSE 4345 teaching case).",
        "suppression": {"min_count": 5, "rule": "counts 1-4 shown as '<5'; complementary suppression protects district/division/national totals"},
        "districts": dist[["adm2_pcode", "adm2_name", "adm1_pcode", "adm1_name", "population", "center_lat", "center_lon"]].to_dict("records"),
        "divisions": dist.groupby(["adm1_pcode", "adm1_name"]).population.sum().reset_index().to_dict("records"),
        "diseases": {k: v["label"] for k, v in cfg["diseases"].items()},
        "resources": {k: v["label"] for k, v in cfg["optimise"]["resources"].items()},
        "operating_point": op, "role": {"scope": "national"},
    }
    if OUT.exists():
        shutil.rmtree(OUT)
    sizes = {}
    roles = yaml.safe_load(open(REPO_ROOT / cfg["serving"]["roles_file"]))["roles"]
    bundles = {"national": bundle}
    for name, r in roles.items():
        if r["scope"] == "district":
            bundles[f"district_{r['district_pcode']}"] = _filter_for_district(bundle, r["district_pcode"], dist)
    for bname, b in bundles.items():
        for part, obj in b.items():
            _dump(obj, OUT / bname / f"{part}.json")
        sizes[bname] = round(sum(f.stat().st_size for f in (OUT / bname).glob("*.json")) / 1e6, 3)
    geo = REPO_ROOT / "dashboard" / "data" / "geo" / "districts.topo.json"
    sizes["geo_shared"] = round(geo.stat().st_size / 1e6, 3)
    (OUT / "sizes.json").write_text(json.dumps(sizes, indent=2))
    print(json.dumps(sizes, indent=2))
    return sizes


def export_alerts_only(spark=None) -> None:
    """Refresh only alerts.json in every existing bundle (used by the streaming path, ING-6)."""
    spark = spark or get_spark("serving")
    if not (OUT / "national" / "meta.json").exists():
        raise FileNotFoundError("run the full export first")
    cfg = load_config()
    det = spark.read.format("delta").load(lake("gold", "detection_daily")).drop("bronze_batch_ids")
    last = det.agg(F.max("date")).first()[0]
    det = det.where(F.col("date") > F.lit(last - dt.timedelta(days=int(cfg["serving"]["alert_days"]) + 7))).toPandas()
    det["date"] = pd.to_datetime(det["date"])
    op = json.loads((Path(path("results")) / "detection_summary.json").read_text())["operating_point"]
    alerts = build_alerts(det, _gold(spark, "forecasts"), _gold(spark, "nowcasts"), op, int(cfg["serving"]["alert_days"]))
    dist = _gold(spark, "district_population").sort_values("adm2_pcode")
    for bdir in OUT.iterdir():
        if not bdir.is_dir():
            continue
        meta = json.loads((bdir / "meta.json").read_text())
        obj = alerts
        if meta["role"]["scope"] == "district":
            full = {"weekly": {"series": {}}, "forecasts": {"forecast": {}, "nowcast": {}}, "thresholds": {}, "alerts": alerts,
                    "allocation": {"rows": [], "beds": [], "teams": [], "scenarios": {"cells": {}}}, "meta": dict(meta)}
            obj = _filter_for_district(full, meta["role"]["district_pcode"], dist)["alerts"]
        _dump(obj, bdir / "alerts.json")
        meta["freshness"]["detection"] = alerts["as_of"]
        meta["freshness"]["cases"] = alerts["as_of"]
        _dump(meta, bdir / "meta.json")


def _evaluation(res: Path) -> dict:
    out = {}
    for f, key in (("detection_summary.json", "detection"), ("allocation_summary.json", "allocation"), ("dedup_eval.json", "dedup"),
                   ("kanonymity.json", "kanonymity")):
        if (res / f).exists():
            out[key] = json.loads((res / f).read_text())
    if (res / "detection_tradeoff.csv").exists():
        out["tradeoff"] = pd.read_csv(res / "detection_tradeoff.csv").replace({np.nan: None}).to_dict("records")
    if (res / "forecast_metrics.csv").exists():
        m = pd.read_csv(res / "forecast_metrics.csv")
        out["forecast_metrics"] = m.replace({np.nan: None}).to_dict("records")
    return out


if __name__ == "__main__":
    export()
