"""Collect the results of the latest run into docs/ for the report (numbers are never typed by hand).

    python -m orchestration.collect_results

* copies the small result files from ``data/results`` (or ``$EPISCOPE_DATA_ROOT/results``) to ``docs/results``;
* copies the latest DAG and load run records to ``docs/results/runs``;
* writes ``docs/report/generated/numbers.tex`` (LaTeX macros) and table fragments ``*.tex`` used by
  ``docs/report/EpiScope_report.tex``.
"""
from __future__ import annotations

import json
import shutil
from pathlib import Path

import pandas as pd

from pipeline.common.config import REPO_ROOT, load_config, path

DOCS = REPO_ROOT / "docs"
GEN = DOCS / "report" / "generated"


def _fmt(v, nd=1):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "--"
    if isinstance(v, (int,)) or (isinstance(v, float) and float(v).is_integer() and abs(v) >= 1000):
        return f"{int(round(v)):,}".replace(",", "{,}")
    return f"{v:,.{nd}f}".replace(",", "{,}")


def _pct(v, nd=0):
    return "--" if v is None or pd.isna(v) else f"{100 * v:.{nd}f}\\%"


def main() -> None:
    res = Path(path("results"))
    out = DOCS / "results"
    out.mkdir(parents=True, exist_ok=True)
    for f in res.glob("*"):
        if f.suffix in {".json", ".csv"} and f.stat().st_size < 5_000_000:
            shutil.copy(f, out / f.name)
    runs = Path(path("runs"))
    (out / "runs").mkdir(exist_ok=True)
    for pattern in ("dag_all_*.json", "load_*.json", "dag_drop_*.json"):
        for f in sorted(runs.glob(pattern))[-3:]:
            shutil.copy(f, out / "runs" / f.name)
    gen_manifest = Path(path("ground_truth")) / "generator_manifest.json"
    if gen_manifest.exists():
        shutil.copy(gen_manifest, out / "generator_manifest.json")
    prov = Path(path("landing")) / "public" / "provenance.json"
    if prov.exists():
        shutil.copy(prov, out / "provenance.json")

    GEN.mkdir(parents=True, exist_ok=True)
    m: dict[str, str] = {}
    det = json.loads((out / "detection_summary.json").read_text())
    op, om = det["operating_point"], det["operating_point_metrics"]
    m.update(OpMethod=op["method"].upper().replace("_", " "), OpThreshold=_fmt(op["threshold"], 1),
             SensLocal=_pct(om["sensitivity_local"]), MedDelayLocal=_fmt(om["median_delay_local"], 0),
             FAperDY=_fmt(om["false_alerts_per_district_year"], 2), SensNRS=_pct(om["sensitivity_nrs_districts"]),
             MedDelayNRS=_fmt(om["median_delay_nrs"], 0), PPVEvents=_pct(om["ppv_events"]),
             EvalStart=det["eval_period"][0], EvalEnd=det["eval_period"][1], NOutbreakRows=str(det["n_outbreak_rows_evaluated"]))
    arts = pd.DataFrame(det["artefacts"])
    a_op = arts[(arts.method == op["method"])]
    for aid, key in (("ART-BACKFILL", "Backfill"), ("ART-ONBOARD", "Onboard")):
        for var in ("naive", "guarded"):
            r = a_op[(a_op.artefact_id == aid) & (a_op.variant == var)]
            m[f"Art{key}{var.capitalize()}"] = str(int(r.alert_days.iloc[0])) if len(r) else "--"
    curve = pd.read_csv(out / "detection_tradeoff.csv")
    g = curve[curve.variant == "guarded"]
    # comparison at a similar alert burden (<= 3.5 false alerts per district-year) and CUSUM delay at 10-20
    for meth, key in (("ears_c1", "EarsCOne"), ("ears_c2", "EarsCTwo"), ("ears_c3", "EarsCThree"), ("cusum", "Cusum")):
        low = g[(g.method == meth) & (g.false_alerts_per_district_year <= 3.5)]
        m[f"{key}SensLowFA"] = _pct(low.sensitivity_local.max()) if len(low) else "--"
    hi = g[(g.method == "cusum") & g.false_alerts_per_district_year.between(10, 20)]
    m["CusumDelayHighFA"] = (f"{int(hi.median_delay_local.min())}--{int(hi.median_delay_local.max())}"
                             if len(hi) and hi.median_delay_local.min() != hi.median_delay_local.max()
                             else (_fmt(hi.median_delay_local.iloc[0], 0) if len(hi) else "--"))
    lines = []
    for _, r in g.iterrows():
        mark = r"\textbf{*}" if (r.method == op["method"] and r.threshold == op["threshold"]) else ""
        n = curve[(curve.variant == "naive") & (curve.method == r.method) & (curve.threshold == r.threshold)]
        lines.append(f"{r.method.upper().replace('_', ' ')} & {r.threshold:g}{mark} & {_pct(r.sensitivity_local)} & "
                     f"{_fmt(r.median_delay_local, 0)} & {_fmt(r.false_alerts_per_district_year, 2)} & "
                     f"{_fmt(n.false_alerts_per_district_year.iloc[0], 2) if len(n) else '--'} & {_pct(r.sensitivity_nrs_districts)} \\\\")
    (GEN / "tradeoff_table.tex").write_text("\n".join(lines) + "\n")
    for meth, gm in g.groupby("method"):   # one file per method for pgfplots
        gm.sort_values("false_alerts_per_district_year")[["threshold", "sensitivity_local", "false_alerts_per_district_year"]] \
            .to_csv(GEN / f"tradeoff_{meth.replace('_', '')}.csv", index=False)

    fm = pd.read_csv(out / "forecast_metrics.csv")
    rows = []
    for dis in ["dengue", "diarrhoea", "ili", "malaria", "all"]:
        for h in (1, 4):
            sub = fm[(fm.disease_group == dis) & (fm.horizon == h)].set_index("model")
            if sub.empty:
                continue
            def v(model, col, nd=1):
                return _fmt(sub.loc[model, col], nd) if model in sub.index else "--"
            rows.append(f"{dis} & {h} & {v('seasonal_naive', 'mae')} & {v('glm_nb', 'mae')} & {v('hgb_global', 'mae')} & "
                        f"{_pct(sub.loc['seasonal_naive', 'coverage80']) if 'seasonal_naive' in sub.index else '--'} & "
                        f"{_pct(sub.loc['glm_nb', 'coverage80'])} & {v('glm_nb', 'relative_wis_vs_baseline', 2)} & "
                        f"{v('hgb_global', 'relative_wis_vs_baseline', 2)} \\\\")
    (GEN / "forecast_table.tex").write_text("\n".join(rows) + "\n")
    allm = fm[fm.disease_group == "all"].set_index(["model", "horizon"])
    m.update(GlmRelWISOne=_fmt(allm.loc[("glm_nb", 1), "relative_wis_vs_baseline"], 2),
             GlmRelWISFour=_fmt(allm.loc[("glm_nb", 4), "relative_wis_vs_baseline"], 2),
             HgbRelWISOne=_fmt(allm.loc[("hgb_global", 1), "relative_wis_vs_baseline"], 2),
             HgbRelWISFour=_fmt(allm.loc[("hgb_global", 4), "relative_wis_vs_baseline"], 2),
             HgbCovOne=_pct(allm.loc[("hgb_global", 1), "coverage80"]), HgbCovFour=_pct(allm.loc[("hgb_global", 4), "coverage80"]),
             GlmCovOne=_pct(allm.loc[("glm_nb", 1), "coverage80"]), GlmCovFour=_pct(allm.loc[("glm_nb", 4), "coverage80"]),
             BaseCovOne=_pct(allm.loc[("seasonal_naive", 1), "coverage80"]),
             GlmMAEOne=_fmt(allm.loc[("glm_nb", 1), "mae"]), BaseMAEOne=_fmt(allm.loc[("seasonal_naive", 1), "mae"]),
             HgbMAEOne=_fmt(allm.loc[("hgb_global", 1), "mae"]))

    al = json.loads((out / "allocation_summary.json").read_text())
    o, b, rec = al["optimised"], al["population_baseline"], al["recommended_policy"]
    m.update(BudgetBDT=_fmt(al["budget_bdt"] / 1e6, 1), FullGapBDT=_fmt(al["full_gap_cost_bdt"] / 1e6, 1),
             AllocAlpha=_fmt(al["alpha"], 2), OptWUnmet=_fmt(o["weighted_unmet"], 0), BaseWUnmet=_fmt(b["weighted_unmet"], 0),
             OptGapCov=_fmt(o["gap_covered_pct"], 1), BaseGapCov=_fmt(b["gap_covered_pct"], 1),
             OptMinCov=_pct(o["min_district_coverage"]), BaseMinCov=_pct(b["min_district_coverage"]),
             OptBelowHalf=str(o["districts_below_50pct"]), BaseBelowHalf=str(b["districts_below_50pct"]),
             OptTeamsUnmet=_fmt(o["teams_unmet"], 0), BaseTeamsUnmet=_fmt(b["teams_unmet"], 0),
             RecAlpha=_fmt(rec["alpha"], 2), RecEffLoss=_fmt(rec["efficiency_loss_pct"], 1),
             RecMinCovZero=_pct(rec["min_coverage_alpha0"]), RecMinCov=_pct(rec["min_coverage_recommended"]),
             RecNothingZero=str(rec.get("nothing_alpha0", "--")), RecNothing=str(rec.get("nothing_recommended", "--")),
             StockDate=al.get("stock_snapshot_date", "--"))
    ee = pd.read_csv(out / "efficiency_equity.csv")
    lines = []
    for bl, gg in ee.groupby("budget_level"):
        for _, r in gg.sort_values("alpha", na_position="last").iterrows():
            name = "pop. baseline" if r.status == "population_baseline" else f"$\\alpha={r.alpha:g}$"
            lines.append(f"{bl:g} & {name} & {_fmt(r.weighted_unmet, 0)} & {_fmt(r.gap_covered_pct, 1)} & "
                         f"{_pct(r.min_district_coverage)} & {int(r.districts_below_50pct)} \\\\")
    (GEN / "equity_table.tex").write_text("\n".join(lines) + "\n")
    ee.to_csv(GEN / "efficiency_equity.csv", index=False)

    cal = pd.read_csv(out / "preparedness_calendar.csv")
    lines = []
    for _, r in cal[cal.disease_group == "dengue"].iterrows():
        if r.seasonal:
            lines.append(f"{r.division} & {r.campaign_start} & {r.preposition_by} & {r.season_onset} & {r.season_peak} & {r.season_end} \\\\")
    (GEN / "calendar_table.tex").write_text("\n".join(lines) + "\n")

    seir = json.loads((out / "seir_fit.json").read_text())
    sc = pd.read_csv(out / "seir_scenarios.csv")
    summ = sc[sc.week < 0].set_index("scenario")
    m.update(SeirRzero=_fmt(seir["R0"], 2), SeirRho=_pct(seir["rho"], 2), SeirRtwo=_fmt(seir["r2"], 3),
             SeirStart=seir["week_start"][0], SeirWeeks=str(seir["weeks"]))
    lines = []
    for name, r in summ.iterrows():
        lines.append(f"{name.replace('%', chr(92) + '%')} & {_fmt(r.reported_cases, 0)} & {int(r.peak_week) + 1} & {_fmt(r.peak_cases, 0)} & "
                     f"{_pct(r.cases_averted / max(summ.reported_cases.iloc[0], 1))} \\\\")
    (GEN / "seir_table.tex").write_text("\n".join(lines) + "\n")

    dd = json.loads((out / "dedup_eval.json").read_text())
    m.update(DedupPrecision=_pct(dd["precision_all_links"], 2), DedupRecall=_pct(dd["recall"], 1),
             DedupTrue=_fmt(dd["true_duplicate_pairs"], 0), DedupWrongMerge=str(dd["distinct_episodes_wrongly_merged"]))
    ka = json.loads((out / "kanonymity.json").read_text())
    lines = [f"{r['level']} & {_fmt(r['records'], 0)} & {r['classes'] if r['classes'] is not None else '--'} & "
             f"{r['k_min_before_suppression']} & {_pct(r['suppressed_share'], 1)} & {'yes' if r['meets_k_after_suppression'] else 'no'} \\\\"
             for r in ka if "malaria" in str(r.get("path", "malaria")) or True][:6]
    (GEN / "kanon_table.tex").write_text("\n".join(lines) + "\n")

    gm = json.loads((out / "generator_manifest.json").read_text()) if (out / "generator_manifest.json").exists() else {}
    m.update(GenRows=_fmt(gm.get("encounter_rows_history", 0), 0), GenDropRows=_fmt(gm.get("encounter_rows_drops", 0), 0),
             GenFacilities=str(gm.get("facilities", "--")), GenOutbreaks=str(gm.get("outbreaks", "--")),
             GenSF=_fmt(gm.get("scale_factor", 1.0), 2), GenSeconds=_fmt(gm.get("seconds", 0), 0))
    dags = sorted((out / "runs").glob("dag_all_*.json"))
    if dags:
        dg = json.loads(dags[-1].read_text())
        st = {t["task"]: t["seconds"] for t in dg["tasks"]}
        m.update(DagMinutes=_fmt(dg["seconds"] / 60, 1), **{f"Dag{k.replace('_', '').capitalize()}Sec": _fmt(v, 0) for k, v in st.items()},
                 HwCpus=str(dg["hardware"]["cpus"]), HwMem=_fmt(dg["hardware"]["mem_gb"], 0))
    loads = sorted((out / "runs").glob("load_*.json"))
    if loads:
        ld = json.loads(loads[0].read_text())
        m.update(**{f"Load{k.capitalize()}Sec": _fmt(v, 0) for k, v in ld.get("stage_seconds", {}).items()})
        enc = next((d for d in ld.get("dq", []) if d["source"] == "encounters"), None)
        if enc:
            m.update(DqRowsIn=_fmt(enc["rows_in"], 0), DqQuarantined=_fmt(enc["rows_quarantined"], 0),
                     DqQuarPct=_pct(enc["rows_quarantined"] / enc["rows_in"], 2), DqDupRemoved=_fmt(enc["duplicates_removed"], 0))
    for f, prefix in (("scaling.json", "Scale"), ("spark_optimisations.json", "Opt")):
        p = DOCS / "experiments" / f
        if not p.exists():
            continue
        js = json.loads(p.read_text())
        if prefix == "Scale":
            lines = []
            for r in js["rows"]:
                lines.append(f"{r['sf']:g} & {_fmt(r['encounter_rows'] / 1e6, 1)}M & {_fmt(r.get('generate', 0), 0)} & "
                             f"{_fmt(r.get('load_bronze', 0), 0)} & {_fmt(r.get('load_silver', 0), 0)} & {_fmt(r.get('load_gold', 0), 0)} & "
                             f"{_fmt(r.get('analytics', 0), 0)} & {_fmt(r.get('export', 0), 0)} & {_fmt(r['total_seconds'] / 60, 1)} & "
                             f"{_fmt(r['landing_mb'] / 1000, 2)} & {_fmt((r['bronze_mb'] + r['silver_mb'] + r['gold_mb']) / 1000, 2)} \\\\")
            (GEN / "scaling_table.tex").write_text("\n".join(lines) + "\n")
            hw = js["hardware"]
            m.update(ScaleCpus=str(hw["cpus"]), ScaleMem=_fmt(hw["mem_gb"], 0))
            for r in js["rows"]:
                tag = {0.25: "A", 0.5: "B", 1.0: "C"}.get(r["sf"], "X")
                m[f"ScaleMin{tag}"] = _fmt(r["total_seconds"] / 60, 1)
                m[f"ScaleRows{tag}"] = _fmt(r["encounter_rows"], 0)
        else:
            for r in js["results"]:
                tag = {"broadcast_registry_join": "Bcast", "partition_pruning": "Prune", "columnar_storage": "Col"}[r["experiment"]]
                m[f"Opt{tag}Before"] = _fmt(r["before"]["seconds_median"], 2)
                m[f"Opt{tag}After"] = _fmt(r["after"]["seconds_median"], 2)
                m[f"Opt{tag}Speedup"] = _fmt(r["speedup"], 1)
            pr = next(r for r in js["results"] if r["experiment"] == "partition_pruning")
            cp = next(r for r in js["results"] if r["experiment"] == "columnar_storage")
            m.update(OptPruneFilesBefore=str(pr["files_scanned_before"]), OptPruneFilesAfter=str(pr["files_scanned_after"]),
                     OptColCsvGB=_fmt(cp["csv_bytes"] / 1e9, 2), OptColParquetGB=_fmt((cp["parquet_bytes"] or 0) / 1e9, 2))
    inc = DOCS / "experiments" / "incremental_drop.json"
    if inc.exists():
        ij = json.loads(inc.read_text())
        a, b = ij["before_drop"], ij["after_drop"]
        m.update(IncNewRows=_fmt(b["silver_rows"] - a["silver_rows"], 0), IncLoadSec=_fmt(ij["drop_load"]["seconds"], 0),
                 IncDagMin=_fmt(ij["drop_dag_seconds"] / 60, 1), IncSilverRows=_fmt(b["silver_rows"], 0),
                 IncForceSec=_fmt(ij["forced_reingest_load"]["seconds"], 0), IncRerunSec=_fmt(ij.get("rerun_seconds"), 0))
    reh = DOCS / "experiments" / "hidden_drop_rehearsal.json"
    if reh.exists():
        rj = json.loads(reh.read_text())
        m.update(RehDays=str(rj["days"]), RehRows=_fmt(rj["new_rows"], 0), RehMin=_fmt(rj["dag_seconds"] / 60, 1),
                 RehLoadSec=_fmt(rj["load_seconds"], 0), RehQuar=_fmt(rj["quarantined"], 0), RehAlerts=str(rj.get("new_alert_series", "--")))
    tt = DOCS / "experiments" / "time_travel.json"
    if tt.exists():
        tj = json.loads(tt.read_text())
        m.update(TTKnownUntil=pd.Timestamp(tj["known_until_then"]).strftime("%-d %b %Y"), TTRevisedWeeks=str(tj["revised_weeks"]),
                 TTRows=str(len(tj["rows"])), TTTriangleDiff=_fmt(tj["triangle_minus_time_travel"]["total"], 0))
    lat = DOCS / "experiments" / "stream_latency.jsonl"
    if lat.exists():
        L = [json.loads(x) for x in lat.read_text().splitlines() if x.strip()]
        m.update(StreamLatencyMax=_fmt(max(x["latency_seconds"] for x in L), 0),
                 StreamLatencyMed=_fmt(pd.Series([x["latency_seconds"] for x in L]).median(), 0))
    pv = json.loads((out / "provenance.json").read_text()) if (out / "provenance.json").exists() else {}
    m.update(ProvWeather=pv.get("S3_weather", {}).get("status", "--").replace("_", " "),
             ProvPop=pv.get("S2_population", {}).get("status", "--").replace("_", " "),
             ProvBound=pv.get("S1_boundaries", {}).get("status", "--").replace("_", " "))
    s3 = pv.get("S3_weather", {})
    if s3.get("observed_until"):                     # pinned NOAA GSOD snapshot (S3)
        from generator.fetch_public import districts, gsod_to_districts, read_gsod

        start, end = s3["range"]
        gs = sorted((Path(path("reference")) / "gsod").rglob("*.csv.gz"))
        wx = gsod_to_districts(read_gsod([(f.name, f.read_bytes()) for f in gs]), districts(), start, end)
        m.update(WxStations=str(s3["stations"]), WxObservedUntil=pd.Timestamp(s3["observed_until"]).strftime("%-d %b %Y"),
                 WxFillPct=_fmt(100 * (wx["source_status"] == "climatology_fill").mean(), 0))
    hrsl = Path(path("reference")) / "population_hrsl_adm2.csv"
    if hrsl.exists():
        m["PopTotalM"] = _fmt(pd.read_csv(hrsl)["population"].sum() / 1e6, 1)
    m["MinCount"] = str(load_config()["detection"]["min_count"])
    tex = "% generated by orchestration/collect_results.py - do not edit\n" + "\n".join(
        f"\\newcommand{{\\{k}}}{{{v}}}" for k, v in sorted(m.items()))
    (GEN / "numbers.tex").write_text(tex + "\n")
    print(f"collected {len(m)} numbers -> {GEN / 'numbers.tex'}")


if __name__ == "__main__":
    main()
