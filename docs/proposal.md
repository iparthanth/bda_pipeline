# EpiScope - Project Proposal (Milestone M1)

**CSE 4345 Big Data Analytics - CEP · Group [GROUP NUMBER]** · [MEMBER 1 NAME, ID] (Data Engineer) · [MEMBER 2 NAME, ID] (Analytics Engineer) · [MEMBER 3 NAME, ID] (Visualisation Engineer)

## 1. Problem restatement
The Ministry of Health needs a prototype national platform that turns routine encounter, laboratory, climate, population and logistics data into (i) early warnings of unusual rises of dengue, acute watery diarrhoea/cholera, influenza-like illness (ILI) and malaria per district, (ii) 1-4-week forecasts of cases and hospital demand, (iii) an allocation of a fixed national stock of supplies and rapid-response teams, and (iv) a seasonal preparedness calendar. Users are non-specialists, so outputs must be explainable, honest about uncertainty, robust to late and incomplete data, and must never expose identifiable patients. The system has to run on a 16 GB laptop and scale to national volumes (25 M+ encounters over 5 years).

## 2. Stakeholders and conflicts (C1-C5)
| Conflict | Stakeholders pulling each way | How we will resolve it (evidence) |
|---|---|---|
| **C1** sensitivity vs specificity | epidemiologists / district officers (early warning) vs district officers (alert fatigue) | Trade-off curves (sensitivity, false alerts per district-year, days to detection) for EARS C1-C3 and a seasonal CUSUM against injected outbreaks; choose an explicit operating point. |
| **C2** granularity vs privacy | vector-control units (fine maps) vs patients, privacy regulator | District x epi-week aggregates, primary suppression of counts 1-4 plus complementary suppression so totals cannot reveal a hidden cell; k-anonymity check (k = 5) for any record-level extract. |
| **C3** efficiency vs equity | MoH (minimise unmet need) vs low-risk districts (minimum share) | LP/MILP with an equity floor alpha; efficiency-equity curve over >= 5 budgets; recommend alpha with a stated rule. |
| **C4** freshness vs cost | district officers (fresh alerts) vs modest infrastructure | Daily incremental batch (decisions are weekly) + optional Structured Streaming path that refreshes alerts within 5 min for emergencies. |
| **C5** accuracy vs interpretability | hospital managers (accuracy) vs officials (trust) | Per-district NB/Poisson GLM with named covariates vs a pooled gradient-boosting comparator, both backtested (MAE, 80% coverage, WIS). |

## 3. Planned architecture and tools (justification)
Medallion lakehouse on **Apache Spark 3.5 (PySpark) + Delta Lake** (ACID MERGE for idempotent loads, time travel for "what did we know on day t"), Parquet/zstd, Hive catalog (Derby). Bronze keeps raw strings + ingestion metadata (source, load time, batch ID, SHA-256); silver applies >= 12 quality rules with quarantine, salted hashing and age bands, MERGE on `encounter_id`; gold holds epi-week marts, a reporting triangle for nowcasting, daily detection signals and lineage (`bronze_batch_ids`). Partitioning: silver by (epi_year, division), gold by epi_year. Orchestration: Makefile + a small Python DAG runner with retries. Optional MinIO (S3A) storage. Serving: a stdlib Python server exporting suppressed, role-scoped JSON (<= 10 MB) to a **D3.js v7** dashboard with >= 5 linked views and a what-if panel. Optimisation: SciPy HiGHS (MILP). Forecasting: scikit-learn GLM/HGB inside Spark `applyInPandas`. Everything runs in Spark local mode or Docker.

## 4. Diseases and methods
Dengue (A90/A91), diarrhoea/cholera (A00/A09), ILI (J10/J11), malaria (B50-B54). Detection: EARS C1/C2/C3 (window functions) and seasonal historical-limits CUSUM (grouped pandas UDF), both with a reporting-artefact guard. Forecasting: direct 1-4 week per-district models with lagged rainfall/temperature (4-8 weeks), nowcast correction of reporting delays, rolling-origin backtest vs seasonal naive. Demand: Monte-Carlo conversion to beds, IV saline, NS1 kits, ORS, antimalarials. Calendar: STL decomposition per division. Bonus: SEIR scenarios, k-anonymity, streaming, Delta time travel.

## 5. Roles
| Role | Lead | Primary requirements |
|---|---|---|
| Data Engineer | [MEMBER 1] | ING-*, DQ-*, PRV-1, ENG-1-3 |
| Analytics Engineer | [MEMBER 2] | AN-1-AN-10, evaluation |
| Visualisation Engineer | [MEMBER 3] | VIS-*, PRV-2, PRV-3, usability test (>= 2 non-CS users) |
Every member commits outside their role (e.g. tests, report sections, reviews).

## 6. Risk register
| # | Risk | Likelihood / impact | Mitigation |
|---|---|---|---|
| R1 | SF=1 (25 M rows) does not finish in 45 min on a 16 GB laptop | M / H | Broadcast joins, partition pruning, caching, 6 GB driver; measure at SF 0.25/0.5/1 early |
| R2 | Public APIs (Open-Meteo, HDX) unavailable or rate-limited | M / M | Cache raw responses in bronze; pinned mirrors; clearly labelled synthetic fallback |
| R3 | Detector flags reporting artefacts (back-fill, new facilities) | H / H | Facility-gap back-fill guard, reporting-change flag, AN-3 evaluation |
| R4 | Small-cell leakage through totals | M / H | Complementary suppression + automated recovery audit test |
| R5 | Hidden test drop breaks ingestion (names, order, defects) | M / H | Header-sniffing ingestion, no hard-coded dates/names, idempotent MERGE, rehearsal with generator drops |
| R6 | Team member unavailable / uneven commits | M / M | Pair on each area, weekly merge, contribution matrix updated each milestone |
| R7 | Java/Spark environment differences on laptops | M / M | Pinned requirements, Java 17, Docker image |
