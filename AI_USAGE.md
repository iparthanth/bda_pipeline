# Team contributions

This file records how the team divided the work on EpiScope, and how AI tools supported that work.

## Who did what

The role split below is the team's declared division of labour (see README and the report's task table).

### Partha Nath (0222220005101104) — Data Engineer

- Decomposed the CEP brief into the requirement checklist and planned the repository architecture.
- Owned the data foundation (ING-1..7, DQ-1..4, PRV-1): public-data fetchers (S1-S3, S7), the seeded synthetic
  generator (S4-S6, ground truth, drops), the bronze/silver/gold Spark lakehouse, quality rules, dedup,
  lineage, MERGE-based idempotent incremental loads, streaming, Delta time travel, the DAG orchestrator
  with retries, and the environment (pinned deps, Makefile, Docker).
- Ran the SF=1 and scaled builds, inspected the Spark UI, produced the ENG optimisation experiments,
  and verified idempotency by re-running incremental drops (`orchestration/rehearse.py`).
- Led the re-audit against the brief that caught the labelled-fallback results, missing ING-7 evidence and the
  requirement-count error; replaced S2/S3 with pinned real snapshots (NOAA GSOD, Meta/CIESIN HRSL) and
  regenerated every result; integrated and edited the final report, proposal and policy brief.

### Tahmid Jamal (0222220005101083) — Analytics Engineer

- Owned the analytics layer (AN-1..AN-10, PRV-4): EARS C1/C2/C3 + CUSUM outbreak detection, the detection
  evaluation and operating point, GLM/NB forecasting with a rolling-origin backtest, demand estimation,
  the MILP allocation optimiser with a population baseline, the efficiency-equity curve, the STL
  preparedness calendar, SEIR scenario modelling, and the k-anonymity checks.
- Made and defended the analytical decisions: chose the final operating point, changed the sensitivity
  tie-break to shorter median delay after reviewing the real-data results, verified the backtest has no
  look-ahead, and reviewed the LP constraints and objective weights.
- Reviewed and corrected AI-drafted analytics work: rejected the degenerate optimiser objective (unmet demand
  valued at purchase cost), replaced the invalid small-file compaction experiment, and removed a claimed
  caching speed-up that the measurements did not support.
- Validates results against baselines and presents the analytics run in the video.

### Taofiq Omar Raju (0222220005101100) — Visualisation Engineer

- Owned the dashboard (VIS-1..7) and the serving-layer privacy UX (PRV-2/3): D3 v7 views (map, timeseries,
  heatmap, allocation, table, calendar, SEIR, data quality, evaluation), the what-if panel and linked
  selections, all on vendored libraries.
- Audited the dashboard end-to-end in a real browser, found and drove the fix for the JS syntax error in the
  time-series view, and ran the usability pass with non-CS users that produced the polish changes.
- Redesigned the marker experience after finding the copy-a-token flow awkward: the dashboard now opens
  signed-in via one-command demo mode with the View-as role switcher, keeping role restriction on the server.
- Ensures exported bundles pass the small-cell suppression audit and presents the dashboard in the video.

### All members

- Read, ran and can explain the parts of the system they present; sign-off recorded in Appendix B.
- Every result in the report comes from runs the team executed and inspected.
- The error list in "Errors we caught and fixed" below is the record of the team's review loop: each item was
  found, diagnosed and corrected by a team member.

## AI assistance

AI tools supported the team's work under the members' direction and review:

| Tool | Used for |
|---|---|
| Claude (Anthropic) via Claude Code | First-draft code from the team's design, debugging runs, drafting report/brief text |

- The AI produced first drafts; the team reviewed, corrected, re-verified and owns every part of the system.
  The table below and the error list are the evidence of that loop.
- Representative prompts: task decompositions of the CEP brief, diagnosis requests such as "the CUSUM detector
  gives ~15 false alerts per district-year at SF=1" and "the optimiser returns a zero budget - why?", and
  end-to-end re-audits of the brief's requirements.
- No real patient data was ever given to the AI; all encounter data are synthetic. Salt and access tokens are
  generated locally (`make env`) and were never shared. Results in the report come from running the code,
  not from the AI's own estimates.

## What the AI drafted vs. what the team did

| Area | AI first draft | Team work / verification |
|---|---|---|
| Synthetic generator (S4-S6) | Seeded NB model, outbreak injection, artefacts, stock simulation | Partha: pinned the parameters, checked seasonality against OpenDengue, re-ran the determinism test |
| Spark lakehouse (ING-*, DQ-*, PRV-1) | Bronze/silver/gold code, quality rules, MERGE-based idempotency, lineage columns | Partha: ran SF=1 and the scaled build, inspected the Spark UI, verified idempotency by re-running a drop |
| Analytics (AN-*) | EARS/CUSUM implementation, evaluation code, GLM forecasting, optimiser, STL calendar, SEIR | Tahmid: chose the operating point, checked the backtest has no look-ahead, reviewed the LP constraints |
| Dashboard (VIS-*, PRV-2/3) | D3 views, suppression module, token server | Taofiq: ran the usability test with non-CS users and changed the design afterwards |
| Report and brief | First draft of sections from the generated results | Section owners edited their own areas (Partha: architecture/ingestion; Tahmid: analytics/results; Taofiq: dashboard); Partha did final integration and the brief-fidelity re-audit |
| Video and Appendix C | Voice-over script timed to the screen recordings; first drafts of the three reflections from the project record; video editing (cuts, captions, noise reduction) | Each member recorded the script in their own voice and is responsible for checking their own reflection |

## Errors we caught and fixed (the team's review loop)

- CSV columns were read by position, which would have broken on a drop with re-ordered columns -> now by name.
- Spark 3.5's Arrow does not support Java 21 -> the project pins Java 17.
- Several intermediate detection designs had far too many false alerts (see report, Section 6.1).
- A JavaScript syntax error in the time-series view, found by rendering the dashboard in a headless browser.
- The synthetic reporting artefacts (a silent-then-back-filling facility, a facility that starts reporting mid-way) leaked
  through injected duplicate visits, so the artefact guard looked broken; fixed in the generator and re-evaluated.
- The first optimiser valued unmet demand at purchase cost, which made the LP degenerate (a high-need district could
  receive nothing); replaced by patient-equivalent weights and a convex "first half of each gap first" penalty.
- A proposed small-file compaction experiment turned out to be invalid (the copied table had no small files) and was
  replaced by the columnar-storage experiment; a claimed caching speed-up was not supported by the measurements and
  was removed from the report.
- The first handed-over results used the labelled synthetic fallback for population and weather because the build
  environment could not reach Open-Meteo or HDX; a re-audit against the brief caught this, and S2/S3 were replaced by
  pinned real snapshots (NOAA GSOD, Meta/CIESIN HRSL) and every result regenerated.
- The report stated 21 Must / 11 Should / 5 Could requirements; the brief has 23 / 9 / 4 (corrected).
- ING-7 was claimed although Delta time travel was never actually read; `orchestration/inspect_lake.py asof` now uses it.
- The dashboard first required copying an access token, which made the one-command run (H1) awkward for a marker;
  it now opens signed in, with the role restriction kept on the server.
- The detection operating-point rule broke sensitivity ties by fewer false alerts, which on a sensitivity plateau
  drifted to the highest (slowest) threshold; after the real-data rebuild this was noticed and the tie-break changed
  to shorter median delay (documented in the report, Section 2) - a rule change made after seeing results.
- A hand-written incremental-load evidence file predated the quarantine fix; the evidence is now produced by
  `orchestration/rehearse.py`.
