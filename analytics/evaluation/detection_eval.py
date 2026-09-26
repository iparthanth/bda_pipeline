"""Evaluation of outbreak detection against the generator's ground truth (AN-2, AN-3, conflict C1).

This evaluation module (not the detector) is the only analytics code allowed to read
``ground_truth_outbreaks.csv`` and ``reporting_artefacts.csv``.

Definitions (stated in the report):
* An outbreak row (district x disease) is *detected* if at least one alert day falls in
  [start - 3 days, end + 7 days]; days-to-detection = first such alert - start (floored at 0).
* An *alert event* is a run of consecutive alert days (gaps of 1 day are bridged); an event is *false*
  when none of its days lies in any outbreak window of the same district and disease.
* False alerts per district-year = false events summed over the four diseases / (64 districts x years).
* Local outbreaks and the nationwide novel-respiratory surge (NRS) are reported separately so that the
  64 NRS district rows do not dominate sensitivity.
"""
from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

import numpy as np
import pandas as pd

from pipeline.common.config import load_config, path

METHODS = ["ears_c1", "ears_c2", "ears_c3", "cusum"]


def load_truth() -> tuple[pd.DataFrame, pd.DataFrame]:
    gt = pd.read_csv(Path(path("ground_truth")) / "ground_truth_outbreaks.csv", parse_dates=["start_date", "peak_date", "end_date"])
    art = pd.read_csv(Path(path("ground_truth")) / "reporting_artefacts.csv", parse_dates=["start_date", "end_date"])
    return gt, art


def alert_mask(det: pd.DataFrame, method: str, thr: float, naive: bool, min_count: int) -> np.ndarray:
    p = "naive_" if naive else ""
    cnt = det["naive_raw7" if naive else "raw7"].to_numpy()
    m = (det[f"{p}{method}"].to_numpy() >= thr) & (cnt >= min_count)
    if not naive:
        m &= det["reporting_flag"].fillna("").to_numpy() != "new_facility"
    return m


def _events(dates: np.ndarray) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
    if len(dates) == 0:
        return []
    dates = np.sort(dates)
    ev, s, prev = [], dates[0], dates[0]
    for d in dates[1:]:
        if (d - prev) / np.timedelta64(1, "D") > 2:
            ev.append((s, prev))
            s = d
        prev = d
    ev.append((s, prev))
    return ev


def evaluate(det: pd.DataFrame, gt: pd.DataFrame, method: str, thr: float, naive: bool, cfg: dict,
             eval_start: pd.Timestamp, eval_end: pd.Timestamp) -> dict:
    before, after = int(cfg["eval_start_buffer_days"]), int(cfg["eval_window_after_end_days"])
    m = alert_mask(det, method, thr, naive, int(cfg["min_count"])) & (det["date"] >= eval_start).to_numpy()
    al = det.loc[m, ["adm2_pcode", "disease_group", "date"]]
    by_key = {k: g["date"].to_numpy() for k, g in al.groupby(["adm2_pcode", "disease_group"])}
    gt = gt[(gt.start_date >= eval_start) & (gt.start_date <= eval_end - pd.Timedelta(days=14))]
    rows = []
    for _, o in gt.iterrows():
        a = by_key.get((o.adm2_pcode, o.disease_group), np.array([], dtype="datetime64[ns]"))
        lo, hi = o.start_date - pd.Timedelta(days=before), o.end_date + pd.Timedelta(days=after)
        hit = a[(a >= lo) & (a <= hi)]
        delay = max(0, int((hit.min() - o.start_date) / np.timedelta64(1, "D"))) if len(hit) else np.nan
        rows.append({"outbreak_id": o.outbreak_id, "kind": o.kind, "adm2_pcode": o.adm2_pcode,
                     "disease_group": o.disease_group, "detected": len(hit) > 0, "delay_days": delay})
    res = pd.DataFrame(rows)
    windows = {}
    for _, o in gt.iterrows():
        windows.setdefault((o.adm2_pcode, o.disease_group), []).append(
            (o.start_date - pd.Timedelta(days=before), o.end_date + pd.Timedelta(days=after)))
    # outbreaks that started before the evaluation window still suppress "false" labels
    full = load_truth()[0]
    for _, o in full.iterrows():
        windows.setdefault((o.adm2_pcode, o.disease_group), []).append(
            (o.start_date - pd.Timedelta(days=before), o.end_date + pd.Timedelta(days=after)))
    n_false = n_events = 0
    false_by_disease: dict[str, int] = {}
    for key, dates in by_key.items():
        for s, e in _events(dates):
            n_events += 1
            if not any(s <= w1 and e >= w0 for w0, w1 in windows.get(key, [])):
                n_false += 1
                false_by_disease[key[1]] = false_by_disease.get(key[1], 0) + 1
    years = (eval_end - eval_start).days / 365.25
    n_d = det["adm2_pcode"].nunique()
    local = res[res.kind != "novel_respiratory"]
    nrs = res[res.kind == "novel_respiratory"]
    return {
        "method": method, "threshold": thr, "variant": "naive" if naive else "guarded",
        "sensitivity_local": float(local.detected.mean()) if len(local) else np.nan,
        "n_local": int(len(local)),
        "median_delay_local": float(local.delay_days.median()) if local.detected.any() else np.nan,
        "sensitivity_nrs_districts": float(nrs.detected.mean()) if len(nrs) else np.nan,
        "median_delay_nrs": float(nrs.delay_days.median()) if nrs.detected.any() else np.nan,
        "false_alerts_per_district_year": n_false / (n_d * years),
        "false_by_disease": false_by_disease, "alert_events": n_events,
        "ppv_events": (n_events - n_false) / n_events if n_events else np.nan,
    }, res


def artefact_check(det: pd.DataFrame, art: pd.DataFrame, method: str, thr: float, cfg: dict) -> list[dict]:
    """AN-3: alerts in the artefact district during the artefact window, naive vs guarded pipeline."""
    out = []
    for _, a in art.iterrows():
        lo, hi = a.start_date, a.end_date + pd.Timedelta(days=14)
        sel = (det.adm2_pcode == a.adm2_pcode) & (det.date >= lo) & (det.date <= hi)
        sub = det[sel]
        for naive in (True, False):
            m = alert_mask(sub, method, thr, naive, int(cfg["min_count"]))
            out.append({"artefact_id": a.artefact_id, "kind": a.kind, "variant": "naive" if naive else "guarded",
                        "method": method, "threshold": thr, "alert_days": int(m.sum()),
                        "diseases_alerting": sorted(sub.loc[m, "disease_group"].unique().tolist())})
    return out


def choose_operating_point(curve: pd.DataFrame, max_fa: float) -> dict:
    """C1 rule: highest local sensitivity with <= max_fa false alerts per district-year (guarded variant);
    ties broken by fewer false alerts, then shorter median delay."""
    c = curve[(curve.variant == "guarded") & (curve.false_alerts_per_district_year <= max_fa)]
    if c.empty:
        c = curve[curve.variant == "guarded"].nsmallest(1, "false_alerts_per_district_year")
    best = c.sort_values(["sensitivity_local", "false_alerts_per_district_year", "median_delay_local"],
                         ascending=[False, True, True]).iloc[0]
    return {"method": best.method, "threshold": float(best.threshold), "rule": f"max sensitivity s.t. FA/district-year <= {max_fa}"}


def run(det: pd.DataFrame, out_dir: Path) -> dict:
    cfg = load_config()["detection"]
    max_fa = float(cfg["max_false_alerts_per_district_year"])
    gt, art = load_truth()
    det = det.copy()
    det["date"] = pd.to_datetime(det["date"])
    eval_start = det.date.min() + pd.Timedelta(days=365)
    eval_end = det.date.max()
    curve, per_outbreak = [], {}
    for method in METHODS:
        thrs = cfg["cusum_thresholds"] if method == "cusum" else cfg["ears_thresholds"]
        for thr in thrs:
            for naive in (False, True):
                summ, res = evaluate(det, gt, method, thr, naive, cfg, eval_start, eval_end)
                curve.append(summ)
                per_outbreak[(method, thr, naive)] = res
    curve_df = pd.DataFrame(curve)
    op_cfg = cfg.get("operating_point") or {}
    recommended = choose_operating_point(curve_df, max_fa)
    op = {"method": op_cfg.get("method", recommended["method"]), "threshold": float(op_cfg.get("threshold", recommended["threshold"]))}
    op_row = curve_df[(curve_df.method == op["method"]) & (curve_df.threshold == op["threshold"]) & (curve_df.variant == "guarded")].iloc[0].to_dict()
    artefacts = []
    for method in METHODS:
        thr = op["threshold"] if method == op["method"] else float(
            choose_operating_point(curve_df[curve_df.method == method], max_fa)["threshold"])
        artefacts += artefact_check(det, art, method, thr, cfg)
    detail = per_outbreak[(op["method"], op["threshold"], False)]
    out_dir.mkdir(parents=True, exist_ok=True)
    curve_df.drop(columns=["false_by_disease"]).to_csv(out_dir / "detection_tradeoff.csv", index=False)
    detail.to_csv(out_dir / "detection_per_outbreak.csv", index=False)
    summary = {"operating_point": op, "recommended_by_rule": recommended, "operating_point_metrics": op_row,
               "eval_period": [str(eval_start.date()), str(eval_end.date())], "artefacts": artefacts,
               "n_outbreak_rows_evaluated": int(len(detail))}
    (out_dir / "detection_summary.json").write_text(json.dumps(summary, indent=2, default=str))
    return summary
