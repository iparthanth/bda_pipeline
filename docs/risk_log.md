# EpiScope - risk log (updated register, milestone M2 and handover)

The proposal (`docs/proposal.md`, section 6) listed seven risks. This log records how each developed and the
new risks that appeared during the build. Status: **closed** (mitigated and verified), **watch** (mitigated,
still worth monitoring), **open** (needs the team).

## Risks from the proposal

| # | Risk | Status | What happened / evidence |
|---|---|---|---|
| R1 | SF = 1 does not finish in 45 min on a 16 GB machine | closed | Full DAG at SF = 1 runs well inside 45 min on 4 vCPU / 16 GB (`docs/experiments/scaling.csv`); broadcast join, partition pruning and columnar storage measured in `docs/experiments/spark_optimisations.json`. |
| R2 | Public APIs (Open-Meteo, HDX) unavailable or rate-limited | closed | It happened: the build environment's network policy blocked Open-Meteo, HDX and WorldPop. Mitigation changed from "synthetic fallback" to **pinned real snapshots** in `data/reference` (NOAA GSOD station weather, Meta/CIESIN HRSL population) built by `generator/build_reference.py`; `fetch_public --refresh` still downloads from Open-Meteo/HDX where reachable. |
| R3 | Detector flags reporting artefacts | closed | Back-fill guard and reporting-change flag; naive vs guarded comparison in the report (AN-3). The artefact first leaked through injected duplicate visits and was fixed in the generator. |
| R4 | Small-cell leakage through totals | closed | Primary + complementary suppression with an automated recovery audit (`tests/test_suppression.py`); nowcast "known" counts were removed from the exports because they could reveal suppressed cells. |
| R5 | Hidden test drop breaks ingestion | closed | Header-based file recognition, no hard-coded names/dates/row counts; rehearsed with 14 generated drops (`docs/experiments/hidden_drop_rehearsal.json`); re-running a drop is a no-op (manifest + MERGE). |
| R6 | Team member unavailable / uneven commits | **open** | The history must show commits from all three members across roles (brief, H1 and R9). Each member commits their own parts before tagging `v1.0`. |
| R7 | Java/Spark differences on laptops | watch | Spark 3.5's Arrow path fails on Java 21, so the project pins Java 17 (Makefile, Dockerfile). Windows users run the pipeline in Docker or WSL2; the dashboard alone needs only Python (`run_dashboard.bat`). |

## New risks found during the build

| # | Risk | Status | Mitigation |
|---|---|---|---|
| R8 | Weather mirror ends before the study window (NOAA GSOD on AWS stops on 24 Aug 2025) | watch | Later days use each district's day-of-year climatology from the same stations, labelled `climatology_fill` in `silver.weather_daily` and on the dashboard banner. |
| R9 | Population layers lack some age bands (HRSL publishes 0-4, 15-24, 60+ only) | watch | Remaining ages split with stated fixed proportions (`generator/fetch_public.py`); district totals and sex ratios are real. |
| R10 | Seasonal CUSUM produced ~15 false alerts per district-year | closed | Wider level-ratio clip, daily (not overlapping weekly) increments, quasi-Poisson dispersion; operating point chosen by a stated rule (report, Section 6.1). |
| R11 | Optimiser degenerate (a high-need district could get nothing) | closed | Harm weights in patient-equivalents and a convex "first half of each gap" penalty; constraints covered by `tests/test_optimiser.py`. |
| R12 | Memory pressure with two Spark JVMs on 16 GB | closed | Jobs run one at a time from the DAG; 6 GB driver memory. |
| R13 | Dashboard hard to open for a marker (token step) | closed | One command (`run_dashboard.bat` / `./run_dashboard.sh` / `make serve`) opens the dashboard signed in; role restriction still enforced by the server, `--secure` for deployment. |
| R14 | Video / usability test / reflections not done in time | **open** | Protocol in `docs/usability/usability_protocol.md`, plan in `docs/video/video_script.md`; owners in the contribution matrix. |
