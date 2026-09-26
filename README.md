# EpiScope - Big Data Surveillance, Forecasting and Response Planning

**Video presentation:** [VIDEO LINK - YouTube (Unlisted) or Drive/OneDrive "anyone with the link"; chapter timestamps below]
**Report (PDF):** [`docs/EpiScope_report.pdf`](docs/EpiScope_report.pdf) · **Proposal (M1):** [`docs/proposal.md`](docs/proposal.md) · **AI usage:** [`AI_USAGE.md`](AI_USAGE.md)

CSE 4345 Big Data Analytics - Complex Engineering Problem, Premier University Chittagong, Spring 2026.
Group [GROUP NUMBER]: [MEMBER 1 NAME, ID] · [MEMBER 2 NAME, ID] · [MEMBER 3 NAME, ID]

> **All encounter, facility and stock data are synthetic** (a teaching case, not real patients). Public data:
> district boundaries from OCHA COD-AB (HDX), OpenDengue V1.3, Open-Meteo weather and HDX/BBS 2022 population.
> When a public source cannot be downloaded, the pipeline uses a clearly labelled synthetic fallback and says so
> in `data/landing/public/provenance.json` and in a banner on the dashboard.

---

## 1. Prerequisites

| Need | Version | Notes |
|---|---|---|
| Linux / macOS / WSL2 | - | 16 GB RAM, 4 cores, ~15 GB free disk for SF = 1 |
| Python | 3.11 | `python3 -m venv` available |
| Java | **17** | Spark 3.5 supports Java 8/11/17 (not 21). The Makefile picks Java 17 automatically if installed |
| make | any | |
| Internet (first run) | - | PyPI, Maven Central (Delta Lake jars), Open-Meteo, HDX, GitHub raw (boundaries, OpenDengue) |
| Docker (optional) | 24+ | alternative to the above: `docker compose up` |

All Python dependencies are pinned in [`requirements.txt`](requirements.txt); D3.js v7.9.0 and
topojson-client 3.1.0 are vendored in `dashboard/vendor/` (no CDN needed).

## 2. One-command run (ENG-1)

```bash
git clone <repo-url> episcope && cd episcope
make all          # venv + .env (random salt/tokens) + public data + generator (SF=1) + lake + analytics + dashboard data
make serve        # dashboard at http://127.0.0.1:8765 - the command prints a login link per role
```

`make all SF=0.25` runs a smaller scale factor. `make all` at SF = 1 (28.2 M encounter rows) took 22.5 min on a
4-vCPU / 15 GB machine (per-stage times for SF 0.25 / 0.5 / 1 in `docs/experiments/scaling.csv`); the limit is 45 min. Docker alternative:
`docker compose up` (pipeline, then dashboard on port 8765).

Other targets: `make test` (automated tests), `make incremental` (ingest the first future daily drop),
`make stream` (near-real-time path), `make scale` (ENG-2 experiment), `make optimisations` (ENG-3),
`make report` (rebuild the report PDF from the latest results).

### Dashboard login (PRV-3)
Tokens live only in the git-ignored `.env` (created by `make env`/`make all`). `make serve` prints links such as
`http://127.0.0.1:8765/#token=...` for the **national** role (all districts in detail) and three **district**
roles (own district in detail, all other areas only as division aggregates). Every data request is logged in
`data/runs/access_audit.log`.

## 3. Running the hidden 14-day test drop

1. Run `make all` once (builds the lake from the historical landing zone).
2. Put the drop anywhere, e.g. `/tmp/hidden_drop/` - any folder structure, any file names, CSV or JSON Lines.
   Files are recognised **by their header** (schema contract in `pipeline/common/schema.py`), so no names,
   dates or row counts are hard-coded.
3. Run
   ```bash
   make drop DROP=/tmp/hidden_drop
   make serve
   ```
   This ingests the drop into bronze (skipping files already loaded, by SHA-256), applies the quality rules
   (quarantine with reason codes), merges into silver (idempotent on `encounter_id`; late records update their
   epi-weeks), rebuilds the affected gold window, and regenerates **alerts, forecasts, allocation plan,
   calendar and dashboard data**. Running the same command twice changes nothing.

The generator writes 14 rehearsal drops to `data/drops/<date>/` (same schema, including two unannounced outbreaks
and quality defects): `make drop DROP=data/drops` loads all of them at once. Rehearsed at SF = 1: 13 new days
(174,804 rows) ingested and every output regenerated in 11.4 min, and the two injected outbreaks became the top two
alerts (`docs/experiments/hidden_drop_rehearsal.json`).

## 4. Architecture summary

```
 S1 boundaries (COD-AB)  S2 population  S3 Open-Meteo  S7 OpenDengue      S4 encounters  S5 registry  S6 stock/beds
          \________________ raw cache (landing/public) ___/                  \___ landing / drops (CSV, JSONL) __/
                                   |  ING-1: header sniffing, SHA-256 manifest, metadata (source, load_ts, batch_id, checksum)
                                   v
   BRONZE (Delta, raw strings, partitioned by batch)  --->  SILVER (Delta: typed, 27 quality rules -> quarantine,
                                                            salted hash + age bands, MERGE on encounter_id,
                                                            partitioned by epi_year x division, episode links)
                                                                    |
                                                                    v
   GOLD (Delta): weekly_counts, reporting_triangle, daily_signal, weather_weekly, stock_district_daily,
                 dq_report, lineage_batches (+ bronze_batch_ids on every row)
                                                                    |
      +-----------------------------+-----------------------------+----------------------------+
      v                             v                             v                            v
  detection (EARS window fns,   forecasting (per-district     demand (Monte-Carlo) ->      calendar (STL per
  CUSUM applyInPandas) +        GLM in applyInPandas,         optimiser (MILP, HiGHS,      division), SEIR
  evaluation vs ground truth    nowcast, backtest vs          equity floor, teams) +       scenarios, k-anonymity
                                seasonal naive, HGB)          efficiency-equity curve
      +-----------------------------+-----------------------------+----------------------------+
                                                                    v
            serving/export.py: aggregated JSON, small-cell + complementary suppression, one bundle per role (<= 10 MB)
                                                                    v
            serving/server.py (token check, audit log)  --->  D3.js v7 dashboard (10 linked views + what-if panel)
 Orchestration: Makefile -> orchestration/dag.py (dependencies, retries, timings) · Streaming: pipeline/streaming
```

Key decisions (details and alternatives in report Section 3): Spark 3.5 local mode + Delta Lake (ACID MERGE,
time travel) instead of plain Parquet; Derby-backed Hive catalog (`episcope_bronze/silver/gold` databases);
daily incremental batch as the main cadence with an optional streaming path (conflict C4); interpretable
per-district GLM as the operational forecaster (C5); CUSUM on historical limits as the operational detector (C1).

## 5. Requirement traceability (ID -> code -> report section)

| ID | Pri. | Where in the repository | Report |
|---|---|---|---|
| ING-1 | M | `generator/` (S4-S6, `generate.py`), `generator/fetch_public.py` (S1-S3, S7), `pipeline/bronze/ingest.py` | §4.1, §4.2 |
| ING-2 | M | all heavy steps in PySpark: `pipeline/*`, `analytics/*` (`applyInPandas`, window functions) | §3 |
| ING-3 | M | `pipeline/silver/transform.py` (partitioned by `epi_year, adm1_pcode`), `pipeline/gold/build.py`, `pipeline/common/spark.py` (`register_table`) | §3.2 |
| ING-4 | M | `pipeline/silver/transform.py` (MERGE), `pipeline/gold/build.py` (`replaceWhere` window), `pipeline/run_load.py`, `tests/test_pipeline_integration.py` | §4.3, §6.5 |
| ING-5 | S | `pipeline/common/spark.py` (S3A when `lake_root` = `s3a://`), `docker-compose.yml` (MinIO profile) | §3.2 |
| ING-6 | S | `pipeline/streaming/stream_alerts.py`, `make stream` | §3.3, §6.5 |
| ING-7 | C | Delta tables everywhere; `gold.reporting_triangle` + Delta versions for "what did we know on day t" | §4.3, §5.2 |
| DQ-1 | M | `pipeline/quality/rules.py` (16 encounter + 6 facility + 5 stock rules), `silver.quarantine` | §4.4 |
| DQ-2 | M | `gold.dq_report`, `gold.dq_rule_results` (`pipeline/gold/build.py::dq_tables`), dashboard "Data quality" view | §4.4, §7 |
| DQ-3 | M | `bronze_batch_ids` on every gold row, `gold.lineage_batches`, `bronze.ingest_manifest` | §4.5 |
| DQ-4 | S | `pipeline/silver/dedup.py`, precision/recall in `analytics/run_analytics.py::stage_dedup_eval` | §4.4, §6 |
| PRV-1 | M | `pipeline/common/privacy.py`, `.env.example`, `orchestration/init_env.py`, `tests/test_deidentification.py` | §4.6 |
| PRV-2 | M | `serving/suppress.py`, `serving/export.py`, `tests/test_suppression.py` | §4.6, §7 |
| PRV-3 | S | `config/roles.yaml`, `serving/export.py::_filter_for_district`, `serving/server.py` | §7 |
| PRV-4 | C | `analytics/privacy/kanon.py`, differential privacy discussion | §4.6, §8 |
| AN-1 | M | `analytics/detection/detect.py` (EARS C1/C2/C3 window functions, CUSUM `applyInPandas`) | §5.1 |
| AN-2 | M | `analytics/evaluation/detection_eval.py`, `docs/results/detection_tradeoff.csv` | §6.1 |
| AN-3 | S | two methods + naive vs guarded variants, artefact check in `detection_eval.py` | §6.1 |
| AN-4 | M | `analytics/forecast/forecast.py`, `analytics/forecast/features.py` | §5.2 |
| AN-5 | M | rolling-origin backtest, MAE / 80% coverage / WIS in `forecast.py::metrics` | §6.2 |
| AN-6 | M | `analytics/demand/demand.py` (assumptions in `config/episcope.yaml: demand`) | §5.3 |
| AN-7 | M | `analytics/optimise/allocate.py` (MILP + population baseline), `tests/test_optimiser.py` | §5.4, §6.3 |
| AN-8 | S | `allocate.py::efficiency_equity`, `docs/results/efficiency_equity.csv` | §6.3 |
| AN-9 | M | `analytics/calendar/calendar.py` (STL) | §5.5 |
| AN-10 | C | `analytics/scenario/seir.py` | §5.6 |
| VIS-1 | M | `dashboard/` (D3 v7 only, vendored) | §7 |
| VIS-2 | M | `dashboard/js/views/{map,timeseries,heatmap,allocation,table}.js` (+ calendar, SEIR, DQ, evaluation) | §7 |
| VIS-3 | M | `dashboard/js/app.js` (shared state + dispatch), click/brush handlers in every view | §7 |
| VIS-4 | M | `serving/export.py` (aggregated bundles, size check), `tests/test_suppression.py` (<= 10 MB) | §7 |
| VIS-5 | S | `dashboard/js/views/whatif.js`, `allocate.py::scenario_grid` | §7 |
| VIS-6 | S | validated colour-blind-safe palette (`dashboard/css/app.css`), bands, hatching, freshness stamps | §7 |
| VIS-7 | C | responsive layout (`app.css`) + animated playback (`map.js`) | §7 |
| ENG-1 | M | `Makefile`, `orchestration/dag.py`, `requirements.txt`, `Dockerfile`, `docker-compose.yml` | §3.4 |
| ENG-2 | M | `orchestration/scale_experiment.py`, `docs/experiments/scaling.*` | §6.4 |
| ENG-3 | M | `pipeline/experiments/spark_optimisations.py`, `docs/experiments/` (+ Spark UI screenshots) | §6.4 |
| ENG-4 | S | `tests/` (pytest), `orchestration/dag.py` (retries) | §3.4 |

## 6. Repository layout (Appendix A of the brief)

```
README.md  AI_USAGE.md  Makefile  requirements.txt  docker-compose.yml  Dockerfile  .env.example  package.json
config/          episcope.yaml (all parameters), roles.yaml
generator/       fetch_public.py (S1-S3, S7), epi_model.py, facilities.py, generate.py (S4-S6 + ground truth)
pipeline/        bronze/ silver/ gold/ quality/ streaming/ common/ experiments/ run_load.py
analytics/       detection/ forecast/ demand/ optimise/ calendar/ scenario/ privacy/ evaluation/ run_analytics.py
serving/         export.py (gold -> suppressed JSON), suppress.py, server.py, out/ (exported bundles)
dashboard/       index.html, css/, js/ (D3 modules), vendor/ (d3, topojson-client), data/geo/ (TopoJSON)
orchestration/   dag.py (DAG + retries), init_env.py, scale_experiment.py, collect_results.py
tests/           pytest suite
docs/            proposal.md, EpiScope_report.pdf, report/ (LaTeX), results/, experiments/, video/, usability/
data/reference/  small real reference files (districts, upazilas, OpenDengue) - everything else in data/ is generated
```

## 7. Video chapters (timestamps to be filled in after recording)

| Time | Chapter | Presenter |
|---|---|---|
| 00:00 | 1. Problem, stakeholders and conflicts C1-C5 | [MEMBER] |
| [mm:ss] | 2. Architecture walkthrough | [MEMBER] |
| [mm:ss] | 3. Recorded pipeline run: clean run, one incremental day, idempotent re-run, quarantine example | Data Engineer |
| [mm:ss] | 4. Analytics results against baselines | Analytics Engineer |
| [mm:ss] | 5. Dashboard walkthrough: linked views and what-if panel | Visualisation Engineer |
| [mm:ss] | 6. Recommendations, ethics and limitations | [MEMBER] |

Recording plan and script: [`docs/video/video_script.md`](docs/video/video_script.md).

## 8. Data sources and licences

| Source | Licence / terms | Use |
|---|---|---|
| OCHA COD-AB Bangladesh ADM2/ADM3 v03 (HDX), via a pinned GitHub mirror | CC BY-IGO | district/upazila boundaries, centroids, facility placement |
| HDX COD-PS Bangladesh (BBS Census 2022) | CC BY-IGO | population denominators (S2) |
| Open-Meteo historical archive | CC BY 4.0 | daily rainfall, temperature, humidity per district (S3) |
| OpenDengue V1.3 | CC BY 4.0 | checking synthetic dengue seasonality (S7) |
| D3.js 7.9.0, topojson-client 3.1.0 | ISC | dashboard (vendored, licences in `dashboard/vendor/`) |
| Synthetic S4-S6 | generated by this repository | encounters, registry, stock |
