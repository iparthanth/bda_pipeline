# EpiScope - video presentation plan (H3)

**Constraints from the brief:** 15-20 minutes · at least 1080p · code/terminal/dashboard text readable ·
hosted on YouTube (Unlisted) or Drive/OneDrive with "anyone with the link can view" (test in a private
window, signed out) · **every member presents their lead area for at least 4 minutes, with their own voice and
name on screen** (camera recommended) · required chapters below, timestamps in the video description *and*
in README section 7 · long jobs may be sped up but terminal timestamps must stay visible · no edits that hide
failures · everything shown must match the tagged `v1.0` code.

## Before recording
- `git checkout v1.0`, `make all` finished; keep the `data/runs/dag_all_*.json` of that run.
- Terminal: font size >= 18 pt, dark theme, window 1920x1080; browser zoom 110-125 %.
- Keep `docs/results/*.json` open for numbers; add a name lower-third for each speaker.
- Record a *fresh* clean run for chapter 3 (you may speed it up; `[dag]` lines print timestamps).
- On Windows use Docker Desktop (or WSL2) for the pipeline; every `make` command has a Docker equivalent:

| Step | Linux / macOS / WSL2 | Windows with Docker Desktop |
|---|---|---|
| Clean run (quick) | `make all SF=0.25` | `$env:SF="0.25"; docker compose up` (PowerShell) |
| One incremental day | `make incremental` | `docker compose run --rm pipeline python -m orchestration.dag drop --source data/drops/2026-01-01` |
| Same day again (idempotency) | `make incremental` | the same command again |
| Show no duplicates | `make inspect WHAT=idempotency` | `docker compose run --rm pipeline python -m orchestration.inspect_lake idempotency` |
| Quarantine example | `make inspect WHAT=quarantine` | `docker compose run --rm pipeline python -m orchestration.inspect_lake quarantine` |
| Tables in the catalog | `make inspect WHAT=tables` | `docker compose run --rm pipeline python -m orchestration.inspect_lake tables` |
| Time travel (optional) | `make inspect WHAT=asof` | `docker compose run --rm pipeline python -m orchestration.inspect_lake asof` |
| Dashboard | `make serve` | `docker compose up --no-deps dashboard`, or double-click `run_dashboard.bat` |

## Chapters, owners and target durations (total about 18 min)

| # | Chapter (required) | Presenter | Target | What to show |
|---|---|---|---|---|
| 1 | Problem, stakeholders, conflicts | Member A | 2:00 | Brief in one sentence; stakeholder table; C1-C5 and where each is resolved (curve, suppression, efficiency-equity curve, cadence, GLM vs HGB) |
| 2 | Architecture walkthrough | Data Engineer | 2:00 | README diagram; bronze/silver/gold tables in the Spark catalog (`make inspect WHAT=tables`); why Delta, partitioning, cadence; real public inputs pinned in `data/reference` |
| 3 | Recorded pipeline run | Data Engineer | 3:00 | `make all` (sped up, timestamps visible) -> `make incremental` (one day) -> run it **again** and show "nothing new" -> `make inspect WHAT=idempotency` (rows = distinct `encounter_id`) -> `make inspect WHAT=quarantine` (rows per rule + examples such as age 212) |
| 4 | Analytics vs baselines | Analytics Engineer | 4:30 | Trade-off curve and chosen operating point; artefact naive vs guarded; forecast backtest table (MAE/coverage/WIS vs seasonal naive); optimiser vs population baseline; efficiency-equity curve; calendar; SEIR |
| 5 | Dashboard walkthrough | Visualisation Engineer | 4:30 | Opens as national; click a district on the map -> time series, heatmap, table, allocation update; brush a period; what-if sliders (lower the threshold to show alert markers); suppressed cells "<5"; then **View as -> District officer** to show PRV-3; mobile width + playback |
| 6 | Recommendations, ethics, limitations | Member A/all | 2:00 | Top 3 recommendations of the policy brief; privacy design; equity; synthetic-data and fallback limitations |

Each member's own segment (Data Engineer: 2+3, Analytics: 4, Visualisation: 5) is >= 4 minutes. The member
without a separate engineering chapter presents chapters 1 and 6 **and** at least 4 minutes of their lead area.

## Suggested lines (adapt - do not read verbatim; R8 penalises reading without understanding)

**Chapter 3 (Data Engineer):** "Every file is hashed with SHA-256 before it enters bronze; the manifest makes
the load idempotent. Watch: I run the same day again - the loader reports that all files were already
ingested. Even if I force it, silver is a MERGE on `encounter_id`, so the two counts stay equal. Records that
fail a rule are not dropped: they sit in quarantine with the rule IDs, like this age of 212 (E11)."

**Chapter 4 (Analytics Engineer):** "Lowering the threshold detects outbreaks earlier but costs false alarms;
this curve is the trade-off. We chose CUSUM at threshold X because ... The naive detector fires on the day a
silent hospital back-fills three weeks of reports; our pipeline excludes back-filled records, so it does not."

**Chapter 5 (Visualisation Engineer):** "The browser never sees a patient record: under 2 MB of aggregated JSON,
counts below five hidden, and a second cell hidden whenever a total would give it away."

## After recording
- Export 1080p (H.264), upload, set Unlisted / anyone-with-link, paste chapter timestamps in the description.
- Put the link at the top of `README.md` and on the report cover page; commit; tag `v1.0` only after that commit.
