# EpiScope video: recording script and screenplay

CSE 4345 · H3 video · target length 18-19 min (the brief allows 15-20 min) · 1920×1080

| Chapter | Presenter | Length | Clips |
|---|---|---|---|
| 1. Problem, stakeholders, conflicts C1-C5 | Partha Nath | ~2:30 | C1-01 … C1-08 |
| 2. Architecture walkthrough | Partha Nath | ~2:30 | C2-01 … C2-09 |
| 3. Recorded pipeline run (clean run, one incremental day, idempotent re-run, quarantine) | Partha Nath | ~3:00 | C3-01 … C3-11 |
| 4. Analytics results against baselines | Tahmid Jamal | ~4:50 | C4-01 … C4-13 |
| 5. Dashboard walkthrough (linked views, what-if) | Taofiq Omar Raju | ~4:30 | C5-01 … C5-12 |
| 6. Recommendations, ethics, limitations | Taofiq Omar Raju | ~1:40 | C6-01 … C6-03 |

Partha presents chapters 1-3, Tahmid chapter 4 and Taofiq chapters 5-6. Each member's own lead-area segment is over 4 minutes: Partha in chapters 2 and 3, Tahmid in chapter 4 and Taofiq in chapter 5. Chapter 4 is Tahmid's only segment and runs about 4:50, so **don't trim chapter 4**, or his part could drop under the 4-minute minimum. The spoken lines total about 2,500 words without the clips marked *(optional)*. With pauses, the sped-up pipeline run and the cards, that is about 18.5 minutes at 150 words a minute and about 20 at 140. **Time a full read-through first.** If it runs over 19 minutes, leave out the optional clips, then shorten C1-03, C2-03 and C5-02.

**How to use this script.** Every clip has an ID (for example `C4-07`). Each clip has three parts:
- **Screen:** what to show and do while recording the screen.
- **Say:** the voice line, recorded separately as an audio file with the same ID.
- **Overlay:** what the editor adds on top of the video.

The lines are a draft. **Say them in your own words**: the brief's lowest band for the individual video segment (R8) includes "read from a script without understanding". Every number in a line comes from the repository's result files, so keep the numbers exact even when you rephrase the rest.

---

## Part A: before recording

1. **Record from `D:\bda_claude`.** `main` has `analytics/` and `dashboard/` again, and its code is identical to commit `976b32d`, the one the results in the report come from. The brief says the results in the video must match the code tagged `v1.0`, so finish any code changes first, record, and tag that state `v1.0`. If anyone later changes code shown in the video, re-record that clip. **Note:** a local `v1.0` tag already exists but points to the older commit `1dfb7f8`, whose report still has the old numbers, and GitHub currently has no tags. Move it to the final commit with `git tag -f v1.0 <commit>` and push it with `git push -f origin v1.0`.
   Also fill in the group number before recording. Otherwise README line 7 and the report cover show `[GROUP NUMBER]` on camera; the video link on README line 3 can only be added afterwards.
2. **Recording order**: record chapters 1, 2, 4, 5 and 6 first, and chapter 3 (the pipeline run) **last**. The chapter 3 run rewrites `serving/out/`, after which the dashboard would show the small test run instead of the full-scale results. If you must record chapter 5 after chapter 3, first run `git -C D:\bda_claude restore serving/out` (PowerShell) or `git -C /d/bda_claude restore serving/out` (Git Bash).
3. **Docker (chapter 3 only)**: start Docker Desktop. Build the image once off camera with `docker compose build` in `D:\bda_claude` (needs internet). This PC has 11.8 GB RAM, so Docker gets about 6 GB. Always use the small scale factor and a 3 GB Spark driver: run `$env:SF="0.02"; $env:SPARK_DRIVER_MEMORY="3g"` in **every** new PowerShell terminal before any chapter 3 command, because the setting only lasts for that terminal. Never run plain `docker compose up` in a new window: without `SF` set, it starts the full 28-million-row run.
4. **Rehearse chapter 3 once off camera and time it.** The SF = 0.02 analytics step has not been run before. If it fails, use `SF=0.05` and re-run; don't cut a failure out of the video (the brief forbids edits that hide failures). After the rehearsal, empty the lake in Git Bash:
   ```bash
   cd /d/bda_claude && rm -rf data/lake data/warehouse data/metastore_db data/spark-events data/landing/encounters data/landing/stock data/landing/facilities data/drops data/ground_truth data/results data/exports
   ```
   Then run `git restore serving/out` as well.
5. **Screen setup.**
   - **Resolution:** 1920×1080 with Windows display scaling at 100%.
   - **VS Code:** font size 18 or more (`editor.fontSize`), line numbers on, word wrap on (Alt+Z). Use the terminal inside VS Code (PowerShell), not a separate Git Bash window. Jump to a line with Ctrl+G.
   - **Browser:** Chrome or Edge, full screen (F11), zoom 100-110%, light mode. Open a fresh tab for each dashboard take so it starts signed in as national.
   - **Report PDF:** open `file:///D:/bda_claude/docs/EpiScope_report.pdf#page=N` in the browser.
   - **Never show:** the `.env` file, or the server started with `--secure`, because it prints every login token.
6. **Dashboard start (chapters 4 and 5)**: double-click `D:\bda_claude\run_dashboard.bat`. It opens `http://127.0.0.1:8765` signed in as national. Default state: Disease = Dengue, Area = Bangladesh (national), Resource = ORS sachet, Alert threshold 17, Budget ৳303k, Equity floor α 0.1.
7. **Things the dashboard does that would look like bugs on camera**
   - **No alerts at the default threshold:** on 31 Dec 2025 no disease is alerting at threshold 17. The only way to show an alert marker is Disease = Influenza-like illness with Alert threshold 6, which shows one marker on Kushtia. Clip C5-08 does this.
   - **No threshold line on the national view:** the national time series has no threshold line, so select Dhaka first.
   - **Two numbers not to point at:** the "Days in alert" cell for Kushtia (it does not follow the threshold slider), and the "Response teams sent" tile while moving the threshold (it jumps erratically: 0, 20, then 6).
   - **"View as" resets the page:** choosing an option reloads the page and resets every slider, so do the what-if clip before the "View as" clip.
   - **A heatmap click empties the time series:** clicking a heatmap cell shrinks the time series to a single week. Click "Reset selection" afterwards.
8. **Usability test:** don't claim it in the video until Taofiq has actually run it.

---

## Part B: the script

Clip format: **Screen** = what to do on screen · **Say** = the voice line · **Overlay** = what the editor adds.

**Overlays used throughout** (white text on a dark semi-transparent bar, lower-left):
- `TAHMID` lower third: **Tahmid Jamal** · ID 0222220005101083 · Analytics Engineer
- `PARTHA` lower third: **Partha Nath** · ID 0222220005101104 · Data Engineer
- `TAOFIQ` lower third: **Taofiq Omar Raju** · ID 0222220005101100 · Visualisation Engineer

A lower third appears for 6 s starting at the word "I am", and a small name tag (name only) stays in the top-right corner for that member's whole segment. The brief requires "their name on screen" during their segment.

### Title card (5 s, no voice)
**Screen:** title card made in the edit: "EpiScope: Big Data Surveillance, Forecasting and Response Planning · CSE 4345 · Group [number]".

---

### Chapter 1: Problem, stakeholders and conflicts (Partha, ~2:30)

**C1-01 · Partha · 10 s**
**Screen:** VS Code, `README.md` lines 1-8, cursor on line 7 (the group line).
**Say:** "Hello. I am Partha Nath, ID 0222220005101104, the Data Engineer of our group. Together with Tahmid Jamal, our Analytics Engineer, and Taofiq Omar Raju, our Visualisation Engineer, we present EpiScope."
**Overlay:** `PARTHA` lower third; corner tag "Partha Nath" until the end of chapter 3. Chapter title "1 · Problem, stakeholders and conflicts".

**C1-02 · Partha · 20 s**
**Screen:** report PDF page 2, Section I, the paragraph starting "The client brief asks for a platform that…".
**Say:** "The Ministry of Health asked for one platform that, for each of Bangladesh's 64 districts, detects rises of dengue, diarrhoea, influenza-like illness and malaria early, forecasts cases and hospital demand up to four weeks ahead, shares a limited stock of supplies and teams, and gives a yearly preparedness calendar, for non-expert users, with late data and strict privacy."

**C1-03 · Partha · 22 s**
**Screen:** same page, TABLE I "Stakeholders, needs and how EpiScope serves them". Move the cursor slowly down the seven rows.
**Say:** "Seven stakeholder groups want different things: policymakers need an allocation they can defend, epidemiologists need transparent signals, district officers want early warnings without alert fatigue, hospital managers need demand with uncertainty, vector-control teams need campaign timing, and patients and the privacy regulator need identities protected."

**C1-04 · Partha · 20 s**
**Screen:** VS Code, `docs/proposal.md` lines 8-15 (the C1-C5 table); then report page 2, bottom, paragraph "C1 sensitivity vs. specificity" (it continues on page 3).
**Say:** "These needs conflict in five places, C1 to C5, and we settled each one with a number. C1 is sensitivity against false alarms: catch as many outbreaks as possible with at most three false alerts per district per year. CUSUM at threshold 17 catches 88 percent of local outbreaks, nine days after onset on median."

**C1-05 · Partha · 14 s**
**Screen:** report page 3, left column below TABLE II, paragraph "C2 granularity vs. privacy".
**Say:** "C2 is detail against privacy: small counts can identify a patient, so we publish district-by-week counts, hide every count from one to four, and hide a second cell whenever a total would reveal it."

**C1-06 · Partha · 14 s**
**Screen:** report page 3, left column, paragraph "C3 efficiency vs. equity".
**Say:** "C3 is efficiency against equity: an equity floor, alpha, guarantees every district a minimum share of each shipment. At alpha 0.1 it costs 9.8 percent more unmet need; without it, four districts with a gap would get nothing."

**C1-07 · Partha · 12 s**
**Screen:** report page 3, left column, paragraph "C4 freshness vs. cost".
**Say:** "C4 is freshness against cost. Decisions are weekly and reports arrive days late, so a daily incremental batch is our main cadence. For emergencies, a streaming path refreshes the alerts within five minutes."

**C1-08 · Partha · 18 s**
**Screen:** report page 3, left column, lower half, paragraph "C5 accuracy vs. interpretability" (the top of page 3 is TABLE II).
**Say:** "C5 is accuracy against interpretability. Pooled over all diseases, gradient boosting forecasts better, 0.35 of the baseline's error against 0.41 for our GLM one week ahead, but it can't be explained per district. So the readable per-district GLM decides, and boosting runs beside it as a challenger. Now, how the system is built."

---

### Chapter 2: Architecture walkthrough (Partha, ~2:30)

**C2-01 · Partha · 8 s**
**Screen:** VS Code, `README.md` line 7.
**Say:** "Now my own area as Data Engineer: the data generator, the lakehouse, data quality, privacy and orchestration."
**Overlay:** `PARTHA` lower third again (6 s); the corner tag "Partha Nath" continues. Chapter title "2 · Architecture".

**C2-02 · Partha · 25 s**
**Screen:** `README.md` Markdown preview (Ctrl+Shift+V), section "8. Architecture summary". Move the cursor along the diagram: sources → BRONZE → SILVER → GOLD → the four analytics boxes → serving → dashboard.
**Say:** "This is the whole system. Four sources are real public data, pinned in the repository so every clone gives the same result; encounters, facilities and stock are synthetic. Data flow through three Delta Lake layers: bronze keeps the raw files, silver cleans, checks and de-identifies them, and gold holds weekly tables for analytics. Only small, suppressed summaries reach the dashboard."

**C2-03 · Partha · 15 s**
**Screen:** report PDF Section III, the table "Alternatives considered" (or the "Key decisions" paragraph under section 8 of `README.md`).
**Say:** "We chose Delta Lake over plain Parquet for ACID MERGE and time travel, a small Python DAG runner over Airflow to stay light, and partitions by year and division: 48 folders instead of 384 small district ones."

**C2-04 · Partha · 14 s**
**Screen:** `generator/generate.py` lines 1-14, then lines 56-63, then line 280.
**Say:** "The generator is deterministic for a given scale and seed: at full scale, 28.6 million encounters for 2021 to 2025 plus 14 daily drops, with deliberate defects such as age 212, orphan facility keys and two percent resubmissions."

**C2-05 · Partha · 16 s**
**Screen:** `pipeline/bronze/ingest.py` lines 44-49 (`sha256`), then 92-106 (`classify`), then 194-206.
**Say:** "Bronze recognises every file by its header, not its name, so any drop folder works. Each file is SHA-256 hashed, and a file whose checksum is already in the manifest is skipped. Every row keeps its source, load time, batch ID and checksum."

**C2-06 · Partha · 12 s**
**Screen:** `pipeline/quality/rules.py` lines 43-71, highlight lines 61-62 (`E11_AGE_RANGE`), then 102-105.
**Say:** "Silver applies 27 quality rules: 16 for encounters, 6 for facilities and 5 for stock. A failing row is not deleted. It goes to quarantine with every rule it broke, for example E11, an age outside 0 to 120."

**C2-07 · Partha · 16 s**
**Screen:** `pipeline/silver/transform.py` lines 247-259 (the MERGE), then 213-214.
**Say:** "Silver uses a Delta MERGE on the encounter ID, so a resubmitted or reloaded record updates the existing row and is never inserted twice. The small facility registry is broadcast into the join. That is one of our Spark optimisations, 2.5 times faster at full scale."

**C2-08 · Partha · 12 s**
**Screen:** `pipeline/common/privacy.py` lines 20-42, then `config/episcope.yaml` lines 47-50.
**Say:** "Patient IDs become a salted SHA-256 hash. The salt lives only in a local file that never goes into Git, and exact ages are reduced to six age bands."

**C2-09 · Partha · 18 s**
**Screen:** `pipeline/gold/build.py` line 62, then `orchestration/dag.py` lines 36-58.
**Say:** "Every gold row carries the bronze batch IDs it came from, so any number traces back to its files. One command runs the whole pipeline with retries and timings; at full scale it took 19.6 minutes, against a limit of 45."

---

### Chapter 3: Recorded pipeline run (Partha, ~3:00)

All commands run in the VS Code terminal (PowerShell) in `D:\bda_claude`, after setting `$env:SF="0.02"; $env:SPARK_DRIVER_MEMORY="3g"` in that terminal. The direct commands (`run_load`, `inspect_lake`) first print Spark start-up lines; that is normal, so don't hide them. The DAG commands print only `[dag]` lines, whose timestamps are UTC (the container's clock).

**C3-01 · Partha · 10 s**
**Screen:** run `git log -1 --oneline`, then `ls data`: `reference`, plus `landing` and `runs` left from the rehearsal; no `lake` folder.
**Say:** "Now a recorded run of our code. The data folder holds only the reference files and cached public downloads, and there is no lake folder yet."
**Overlay:** chapter title "3 · Recorded pipeline run".

**C3-02 · Partha · 40 s (screen sped up)**
**Screen:** type and run:
```powershell
$env:SF="0.02"; $env:SPARK_DRIVER_MEMORY="3g"
docker compose run --rm pipeline
```
Let it run to the end. In the edit, speed up the waiting but slow down on each `[dag] … OK` line and on the final `[dag] success in … s`.
**Say:** "I start the full pipeline with one Docker command, at scale factor 0.02 so it fits this laptop. The video is sped up, but the timestamp on each line is real. The six tasks finish in order: environment, public data, generator, load, analytics and export."
**Overlay:** small "▶▶ sped up" label in the corner while sped up.

**C3-03 · Partha · 16 s**
**Screen:** open the newest `data/runs/load_B*.json` in VS Code, scroll to the `dq` section; then `docs/results/runs/load_B20260927T052510-59e9bc77.json` lines 34-36.
**Say:** "The load report lists, for each source, the rows in, the rows quarantined and the duplicates removed. In the full-scale run, 446,927 of 28.6 million encounter rows were quarantined, which is 1.56 percent, and 552,161 resubmissions were removed."

**C3-04 · Partha · 28 s (screen sped up)**
**Screen:** run `ls data/drops`, then `ls data/drops/2026-01-01`, then:
```powershell
docker compose run --rm pipeline python -m orchestration.dag drop --source data/drops/2026-01-01
```
Speed up the run; pause on the `[dag] … OK` lines.
**Say:** "Next, one incremental day: the first daily drop, the first of January 2026. It goes into bronze, is merged into silver, only the affected weeks in gold are rebuilt, and alerts, forecasts and dashboard data are refreshed. Late records in the drop also update earlier weeks."

**C3-05 · Partha · 16 s**
**Screen:** run:
```powershell
docker compose run --rm pipeline python -m pipeline.run_load --source data/drops/2026-01-01 --no-public
```
Zoom in on the line `"status": "nothing new - all files already ingested (idempotent no-op)"`.
**Say:** "Now I run the same day again. Every file's checksum is already in the manifest, so the loader answers: nothing new, all files already ingested. No batch is written."

**C3-06 · Partha · 14 s**
**Screen:** the same command with `--force` at the end; show `"status": "loaded"`.
**Say:** "Even if I force the files in again, past the manifest, silver does a MERGE on the encounter ID, so nothing is duplicated."

**C3-07 · Partha · 16 s**
**Screen:** run:
```powershell
docker compose run --rm pipeline python -m orchestration.inspect_lake idempotency
```
Zoom in on `silver.encounters rows = N   distinct encounter_id = N   -> OK, no duplicates`.
**Say:** "This check proves it: the number of rows in silver equals the number of distinct encounter IDs. In our full-scale test the two numbers were also equal: 27,656,066."

**C3-08 · Partha · 20 s**
**Screen:** run:
```powershell
docker compose run --rm pipeline python -m orchestration.inspect_lake quarantine
```
Show the rows-per-rule list and the example records with `[E11_AGE_RANGE]`. Then flash `pipeline/quality/rules.py` lines 61-62.
**Say:** "And here is the quarantine: the number of rows per rule, and example records with the rule IDs they broke. Age failures, rule E11, are one of the biggest groups, 100,345 rows at full scale, because the generator injects ages such as 212."

**C3-09 · Partha · 12 s**
**Screen:** run:
```powershell
docker compose run --rm pipeline python -m orchestration.inspect_lake tables
```
Then the GOLD rows of the diagram in `README.md` section 8.
**Say:** "All bronze, silver and gold tables are registered in the Spark catalog. The gold weekly counts and daily signals are what Tahmid's analytics read."

**C3-10 · Partha · 20 s** *(optional)*
**Screen:** `docs/experiments/scaling.csv`, then `docs/experiments/screenshots/spark_ui_broadcast_off.png` next to `spark_ui_broadcast_on.png`.
**Say:** "For the scaling experiment, the full pipeline took 11.7 minutes at scale 0.25, 14.1 at 0.5 and 19.6 at full scale. In the Spark UI, without broadcasting, Spark shuffles all 28.9 million encounter rows into a sort-merge join; with broadcasting it only ships the small registry, which is 2.5 times faster. Columnar storage made a full scan 5 times faster than CSV."

**C3-11 · Partha · 14 s** *(optional)*
**Screen:** run:
```powershell
docker compose run --rm pipeline python -m orchestration.inspect_lake asof --version 0
```
**Say:** "Every load is a new Delta version, so we can ask what we knew on a given day. In the full-scale test, late reports had revised 15 of the 24 most recent disease-weeks."

---

### Chapter 4: Analytics results against baselines (Tahmid, ~4:50)

**C4-01 · Tahmid · 10 s**
**Screen:** `README.md` section 8, the analytics row of the diagram.
**Say:** "I am Tahmid Jamal, ID 0222220005101083, the Analytics Engineer. Analytics reads only the gold tables. All results here come from the full-scale build: detection is evaluated over 2022 to 2025, forecasts are back-tested over the last 52 weeks, and the allocation uses the stock on 31 December 2025."
**Overlay:** `TAHMID` lower third; corner tag "Tahmid Jamal" until the end of chapter 4. Chapter title "4 · Analytics results".

**C4-02 · Tahmid · 28 s**
**Screen:** `analytics/detection/detect.py` lines 42-51 (EARS), then 63-106 (CUSUM), then 134-135 (`applyInPandas`).
**Say:** "Detection uses two established methods. EARS C1, C2 and C3 compare today with the previous week, using Spark window functions. The seasonal CUSUM compares the last seven days with the same time of year in up to four previous years and adds up small, persistent excesses; it runs through applyInPandas, 256 district-disease series in parallel."

**C4-03 · Tahmid · 30 s**
**Screen:** `analytics/evaluation/detection_eval.py` lines 123-133, then `config/episcope.yaml` lines 68-72, then `docs/results/detection_summary.json` lines 1-29.
**Say:** "We evaluate against the injected outbreaks, which the detector never reads. The rule: the highest sensitivity with at most three false alerts per district-year. Thresholds 17, 20 and 25 tie at 87.5 percent, so the shorter delay wins: threshold 17, nine days median, 2.97 false alerts. The national surge was flagged in every district, but late, a median of 93 days after its seeding began."

**C4-04 · Tahmid · 18 s**
**Screen:** report PDF page 7, Fig. 2, then TABLE V, pointing at the rows EARS C1 6, EARS C2 8, EARS C3 8 and CUSUM 14; end on the "Interpretation" paragraph under Fig. 2 (TABLE V rounds delays to whole days, the paragraph gives 1.5-5.5).
**Say:** "This curve is conflict C1. At a similar burden of at most four false alerts per district per year, EARS C1 finds 34 percent of outbreaks, C2 38 percent and C3 66 percent; CUSUM finds 94 percent. The price is delay: nine days at our operating point, against 1.5 to 5.5 days at CUSUM thresholds that give 10 to 20 false alerts per district per year."

**C4-05 · Tahmid · 18 s**
**Screen:** `analytics/detection/detect.py` lines 15-16 (what the guard does), then `pipeline/gold/build.py` lines 121-127 (where back-filled records are left out of the signal), then `docs/results/detection_summary.json` lines 159-179 (`ART-BACKFILL`).
**Say:** "The data also contains a trap: a facility goes silent for weeks, then back-fills all its reports at once. A naive detector raises 13 alert days on it. Our guard leaves out back-filled records, and the guarded pipeline raises zero."

**C4-06 · Tahmid · 28 s**
**Screen:** `analytics/forecast/forecast.py` lines 1-20, then 114-133, then 233-237; then `analytics/forecast/features.py` lines 3-11.
**Say:** "For forecasting, one Poisson GLM per district and disease predicts one to four weeks ahead, trained in parallel with applyInPandas, from recent weeks, last year's season, and rainfall and temperature lagged four to eight weeks. Weeks still being reported are nowcast first, and the 52-week backtest never uses future data."

**C4-07 · Tahmid · 30 s**
**Screen:** report PDF page 7, TABLE VI, pointing at the rows "all 1", "all 4", "malaria 1/4" and "ili 4".
**Say:** "Our baseline is seasonal naive, the same week last year. One week ahead, the mean absolute error drops from 64.7 to 36.3 cases, the weighted interval score is 0.41 of the baseline's, and the 80 percent intervals hold the true value 81 percent of the time. Pooled, boosting scores 0.35, but for malaria the GLM wins at every horizon. Four weeks ahead the gain shrinks to 0.94, and for influenza-like illness the GLM is worse than the baseline: a mean error of 221.6 against 151.2."

**C4-08 · Tahmid · 14 s**
**Screen:** `config/episcope.yaml` lines 82-91 (the `demand` section).
**Say:** "Forecasts become resource demand through 1,000 Monte-Carlo draws, for example six IV saline bags per admitted dengue case and ten ORS sachets per diarrhoea case. The optimiser plans for the 80th percentile."

**C4-09 · Tahmid · 32 s**
**Screen:** `analytics/optimise/allocate.py` lines 1-28 (the formulation), then 104-123, then `docs/results/allocation_summary.json` lines 1-41.
**Say:** "The optimiser is a mixed-integer programme solved with SciPy's HiGHS. It minimises weighted unmet need under the budget, the equity floor, truck capacity per division and at most 20 response teams. With 60 percent of the gap as budget it fills 57.4 percent of it; the same money shared by population fills only 22.2 percent and leaves 22 districts below half coverage instead of 9."

**C4-10 · Tahmid · 25 s**
**Screen:** report PDF page 8, TABLE VII, the rows for budget 0.6; then `analytics/optimise/allocate.py` lines 210-219 (the recommendation rule); then `docs/results/allocation_summary.json` lines 5-14 (`nothing_alpha0` 4, `nothing_recommended` 0, `efficiency_loss_pct` 9.78). Don't use the dashboard curve for this clip: at 60% budget its α points sit on top of each other.
**Say:** "Across six budgets and six equity floors we get the efficiency-equity curve. At a 60 percent budget, alpha zero leaves four districts with a gap without any shipment. Alpha 0.1 costs 9.8 percent more unmet need but leaves none without one, and higher floors cost much more, so we recommend alpha 0.1."

**C4-11 · Tahmid · 15 s**
**Screen:** `analytics/calendar/calendar.py` lines 1-11, then `docs/results/preparedness_calendar.csv` line 10 (Dhaka dengue); then the dashboard panel "Preparedness calendar", hovering the Dhaka row.
**Say:** "The preparedness calendar comes from STL seasonal decomposition per division. For Dhaka dengue, in the twelve months after our data end, the larval campaign should start on 13 April, supplies should be in place by 11 May, and the peak is expected in mid-August."

**C4-12 · Tahmid · 18 s**
**Screen:** `analytics/scenario/seir.py` lines 61-87, then `docs/results/seir_fit.json` lines 1-7, then report PDF page 10, TABLE X (briefly: its "2 weeks earlier" rows equal the "at detection" rows because the surge was detected in its first week).
**Say:** "Finally, an SEIR model calibrated on the novel respiratory surge, with R-nought 2.07 and an R-squared of 0.997. Within the 23 simulated weeks, cutting contacts by 40 percent at detection would avert 64 percent of reported cases, partly by delaying them; cutting them by 20 percent averts only 18 percent."

**C4-13 · Tahmid · 10 s**
**Screen:** `allocate.py` lines 179-182, then cut to the top of the dashboard.
**Say:** "Detection also sets the optimiser's weights, so districts with alerts get priority. All of this reaches the dashboard as suppressed summaries, and Taofiq will show it now."

---

### Chapter 5: Dashboard walkthrough (Taofiq, ~4:30)

Start from a fresh browser tab with the default state (Part A, item 6).

**C5-01 · Taofiq · 20 s**
**Screen:** first close any dashboard console window left open from chapter 4 (otherwise port 8765 is busy and the start fails). File Explorer, `D:\bda_claude`; double-click `run_dashboard.bat`. The console prints the address and the browser opens the dashboard by itself.
**Say:** "I am Taofiq Omar Raju, ID 0222220005101100, the Visualisation Engineer. My area is the serving layer, privacy in every output, and the D3.js dashboard. The dashboard starts with one double-click and opens signed in as the national role. D3 version 7 is bundled in the repository, so it works without internet."
**Overlay:** `TAOFIQ` lower third; corner tag "Taofiq Omar Raju" until the end of the video. Chapter title "5 · Dashboard walkthrough".

**C5-02 · Taofiq · 14 s**
**Screen:** point at the banner under the top bar, then at the grey date stamp in the header of each panel.
**Say:** "The banner says honestly that encounters are synthetic and that weather is observed up to 24 August 2025. Every panel shows how fresh its data are; here, cases are reported up to 31 December 2025."

**C5-03 · Taofiq · 35 s**
**Screen:** panel "1 · Incidence per 100,000 and alerts". Read the caption next to the Play button. Hover Dhaka for 3 s (tooltip). Point at the hatched districts and the legend item "includes suppressed weeks (<5)". Click Dhaka; it gets a dark outline. Scroll down slowly: time-series legend "Reported (Dhaka)", heatmap title "Dengue · Dhaka", the highlighted table row, and the allocation title with Dhaka's bed demand.
**Say:** "View one is the map of weekly incidence per 100,000 over the last eight complete weeks. The last week of December has only three days of data, so it is left out instead of looking like a sudden drop. Hatched districts contain suppressed weeks. When I click Dhaka, every view updates together: the time series, the heatmap, the table and the allocation. They all share one selection state."

**C5-04 · Taofiq · 30 s**
**Screen:** panel "2 · Weekly cases, forecast and alert threshold" (Dhaka, dengue). Hover the week of 3 Nov 2025 (reported vs threshold). Point at the red ▲ marks in early December. Hover the week of 22 Dec (nowcast), then the week of 5 Jan 2026 (forecast and interval). Drag across June-November on the lower strip, then scroll up to show the new dates in the map caption.
**Say:** "View two is the time series. The dotted red line is the historical limit, expected plus two standard deviations; in the week of 3 November Dhaka reported 4,126 cases against 3,526. Triangles mark days when CUSUM crossed its threshold of 17. The dashed line is the nowcast, the bands are 50, 80 and 90 percent forecast intervals, and brushing a period here changes the map."

**C5-05 · Taofiq · 15 s**
**Screen:** panel "3 · Seasonal heatmap". Hover 2023 / W31, then 2025 / W35. Click one cell, show the map caption, then click "Reset selection".
**Say:** "View three is the seasonal heatmap, epi-week by year. Dhaka's dengue peak moves from year to year, for example week 31 in 2023 and week 35 in 2025, which is why we need a preparedness calendar."

**C5-06 · Taofiq · 25 s**
**Screen:** panel "4 · Forecast demand, stock and recommended allocation", Resource = ORS sachet. Hover the Dhaka row (tooltip). Switch Resource to IV saline (the blue bars disappear), then back to ORS. Click the Chattogram bar.
**Say:** "View four compares forecast demand with its 80 percent interval, current stock as a diamond, the optimiser's shipment in blue, and the population-share baseline as a dashed box. At the default budget the plan ships only ORS: per taka it protects about four times more patient-equivalents than IV saline, so saline only enters from an 80 percent budget."

**C5-07 · Taofiq · 15 s**
**Screen:** panel "5 · Ranked early-warning table". Read the caption (at threshold 17 every row is "○ Normal"). Point at the Signal column. Click a row; the map and time series jump to it.
**Say:** "View five is the ranked early-warning table. Each status has an icon and a label, not just a colour; at our threshold everything is normal on the last day of data, and I'll show an alert in a moment. Clicking a row selects it in every view."

**C5-08 · Taofiq · 50 s**
**Screen:** panel "6 · What-if". (1) Drag Alert threshold from 17 to 6. (2) Set Disease to Influenza-like illness. (3) Scroll up: a red marker appears on Kushtia. (4) Show the table caption "1 alert(s) … at threshold 6" and click the Kushtia row. (5) Set Alert threshold back to 17 and Disease back to Dengue. Budget slider far left (৳101k), then far right (৳607k), then back to ৳303k, watching the "Districts < 50% covered" tile. (6) Move the Equity floor α slider to 0 and hover the **lower edge** of the large selected point (the α = 0.1 point is drawn on top of it); hold only when the tooltip title says "Equity floor α = 0". Move α to 0.9 and hover again ("Districts < 50% covered" becomes 15); hover the baseline square; set α back to 0.1.
**Say:** "The what-if panel has three sliders: budget, alert threshold and equity floor. On the last day of data nothing is alerting at our threshold of 17, so I lower it to 6 and switch to influenza-like illness: Kushtia now shows an alert on the map and at the top of the table. On the efficiency-equity curve, alpha 0 leaves 1,487 patient-equivalents of need unmet, alpha 0.1 leaves 1,632, and the population-share plan 6,727. All 288 combinations were solved beforehand, so the sliders respond instantly."

**C5-09 · Taofiq · 12 s** *(optional)*
**Screen:** click ◐ (dark mode), point at a status chip, press Tab until a district has a focus ring, press Enter, then click ◐ again.
**Say:** "The palette is colour-blind safe in light and dark mode, and districts and table rows can be selected with the keyboard."

**C5-10 · Taofiq · 20 s**
**Screen:** click "▶ Play spread" and let the caption step through the weeks of 2025 (about 18 s; press "■ Stop" just before the last frame, the incomplete week of 29 Dec, which looks pale). Then press F12 → Ctrl+Shift+M, choose a phone preset (about 390 px wide), scroll once, and close DevTools.
**Say:** "Play spread animates the last 52 weeks on the map. At phone width the layout becomes a single column, so a district officer can use it on a phone."

**C5-11 · Taofiq · 28 s**
**Screen:** top bar → "View as" → "District officer - Dhaka" (the page reloads). Point at the role text, the division-coloured areas with dashed borders, and the hover "Sylhet (shown as Sylhet division)". Open the Area menu (Bangladesh, the eight divisions, and Dhaka as the only district). Show the table with Dhaka only. Then "View as" → "National (all districts)".
**Say:** "Role-based views: as the Dhaka district officer I see Dhaka in detail and every other area only as its division. The server enforces this: a district token gets a smaller bundle, 0.21 megabytes instead of 1.77. The View as switch exists only in this demo mode."

**C5-12 · Taofiq · 26 s**
**Screen:** VS Code: `serving/suppress.py` lines 22-36, then 43-88; `serving/server.py` lines 68-78, then 89-112; then `data/runs/access_audit.log`, scrolled to the end.
**Say:** "In the code, primary suppression turns every count from one to four into 'less than five'. Complementary suppression then hides a second cell whenever a district, division or national total would reveal the hidden one. Automated tests scan every export for counts from one to four and re-check the published totals. The server checks the role token on every data request, refuses requests without a valid one, and writes each request to this audit log."

---

### Chapter 6: Recommendations, ethics and limitations (Taofiq, ~1:40)

**C6-01 · Taofiq · 50 s**
**Screen:** report PDF Appendix A, the policy brief, at "Top three recommendations". For recommendation 3, switch to the dashboard panel "Preparedness calendar" (Dengue) and hover the Rajshahi row.
**Say:** "Our policy brief gives three recommendations. First, a weekly warning review from the ranked list: in our simulated test it caught 88 percent of local outbreaks, nine days after onset on median, and each alert is confirmed by phone before a team moves. Second, ship supplies with the optimised plan and the 0.1 equity floor: at 60 percent of the gap budget it fills 57.4 percent of the shortfall, against 22.2 for a population share. Third, follow the calendar: start dengue prevention between mid-March and late April and pre-position supplies by mid-April to mid-May. Peaks come first in Sylhet and Mymensingh, in July, and last in Rajshahi, in late October."
**Overlay:** chapter title "6 · Recommendations, ethics and limitations".

**C6-02 · Taofiq · 30 s**
**Screen:** report PDF Section VIII "Ethical, Societal and Sustainability Considerations" (page 10, right column; frame the shot to start at the VIII heading, because the unfilled usability placeholder sits just above it). Optionally cut back for 3 s to a hatched area on the map.
**Say:** "On ethics: identifiers never leave bronze, and only suppressed summaries are published, because small counts can stigmatise a community. The equity floor makes fairness an explicit number that policymakers choose, and role-based, audited access with visible uncertainty keeps humans in the loop."

**C6-03 · Taofiq · 35 s**
**Screen:** report PDF Section IX "Limitations and Future Work", then VS Code `AI_USAGE.md` (fill in its "Team contribution / verification" column and remove the "[add any other tool…]" row before recording).
**Say:** "Limitations: the patient data are synthetic, so real accuracy will be lower; weather after 24 August 2025 is climatology; and the streaming latency was measured on an earlier build. We used Claude through Claude Code, as the course allows: it generated the first version of the code and drafted the report, the brief and this script, as AI_USAGE.md records. Thank you for watching."

### End card (5 s, no voice)
**Screen:** card made in the edit: repository URL, the three names and IDs, "Thank you".

---

## Part C: recording the voices

- **One audio file per clip**, named exactly by the clip ID plus the speaker, for example `C4-07_Tahmid.wav`. Record each clip only after its screen clip, so you know what the screen shows.
- **Equipment:** phone voice recorder or Audacity, quiet room, the same place and microphone for all of your clips, about 20-30 cm from the mouth. WAV or high-quality M4A.
- **Pauses:** leave 1 second of silence at the start and end of every file. If you make a mistake, pause and say the sentence again in the same file; the editor will cut the bad take.
- **Speak, don't read:** explain it as if to a classmate. If your sentence ends up longer or shorter than the clip time above, that's fine. The edit adapts the screen to your voice, not the other way round.
- **Screen clips:** record each clip as its own file in OBS Studio (Settings → Video: 1920×1080, 30 fps; Output: MP4 or MKV; no microphone). Name the file the same way as its voice file, for example `C4-07_screen.mp4`. Move slowly and hold the final view for about 3 seconds.
- **Folder layout:** put everything in `D:\episcope_video\screen\` and `D:\episcope_video\voice\`, with this script beside them.

---

## Part D: putting voice and screen together with Claude Code

Claude Code cannot listen to audio or watch video, but it can do the whole edit with two free tools on this PC, and check the result by looking at still frames:

- **ffmpeg** (video tool): cuts, speeds up, joins clips, puts each voice file under its screen clip, draws the name lower-thirds and corner tags, and exports 1080p MP4. Install: `winget install --id Gyan.FFmpeg -e`.
- **faster-whisper** (offline speech-to-text): gives the exact time of every spoken word. Claude uses it to start each lower third on the word "I am", and to produce the chapter timestamps. Install: `pip install faster-whisper` (in a virtual environment).

How the sync works:
- Each clip ID has one screen file and one voice file, and Claude matches them one by one.
- **Voice longer than the screen:** Claude holds the last frame.
- **Screen longer than the voice:** Claude speeds up only the waiting parts, such as the pipeline run, and keeps the timestamps readable. It never cuts out errors, because the brief forbids hiding failures.
- **Audio levels:** it evens out the loudness of the three voices.
- **Checking:** it exports still frames at every overlay so you can see that the names and IDs are right before the final export.
- **Chapter list:** it prints a timestamp list for the README and the YouTube description.

You don't need a plugin, connector or paid editor for this. Don't use AI voice generation, voice cloning or dubbing tools. The brief requires each member's own voice, and a generated voice would count as "not by the member" under R8.
