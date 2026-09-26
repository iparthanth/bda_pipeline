# AI usage disclosure (required by Section 10 of the CEP brief)

AI coding assistants are allowed in CSE 4345 provided their use is listed here and every team member can
explain any part of the code. This file is the group's honest record. **Every member must read, run and be
able to explain the parts of the system they present and sign off in Appendix B of the report.**

## Tools

| Tool | Version / access | Used for |
|---|---|---|
| Claude (Anthropic) via Claude Code | cloud session, September 2026 | Reading and decomposing the CEP brief into a requirement checklist; generating the first version of the repository (generator, Spark pipeline, analytics, serving layer, D3 dashboard, tests, Makefile/Docker); debugging runs; drafting the report, proposal, policy brief and video script |
| [add any other tool the team used, e.g. GitHub Copilot, ChatGPT] | | |

## What the AI produced vs. what the team did

| Area | AI contribution | Team contribution / verification (fill in) |
|---|---|---|
| Synthetic generator (S4-S6) | Initial implementation of the seeded NB model, outbreak injection, artefacts, stock simulation | [who reviewed the parameters, checked seasonality against OpenDengue, re-ran determinism test] |
| Spark lakehouse (ING-*, DQ-*, PRV-1) | Bronze/silver/gold code, quality rules, MERGE-based idempotency, lineage columns | [who ran SF=1, inspected the Spark UI, verified idempotency by re-running a drop] |
| Analytics (AN-*) | EARS/CUSUM implementation, evaluation code, GLM forecasting, optimiser, STL calendar, SEIR | [who chose the operating point, checked the backtest has no look-ahead, reviewed the LP constraints] |
| Dashboard (VIS-*, PRV-2/3) | D3 views, suppression module, token server | [who ran the usability test with non-CS users and changed the design afterwards] |
| Report and brief | First draft of all sections from the generated results | [who rewrote/edited which sections] |

## Representative prompts

1. "Read the whole CEP instructions end to end, then plan, then execute, don't miss any instruction... verify
   that you followed every instruction." (initial task statement, with the PDF attached)
2. "Diagnose why the CUSUM detector gives ~15 false alerts per district-year at SF=1." (the assistant found that
   the level adjustment was clipped too tightly, that overlapping 7-day sums made the CUSUM increments
   autocorrelated, and that the synthetic back-fill artefact leaked through duplicate visits - each fix is
   documented in the code comments and in the report, Section 6)
3. "The optimiser returns a zero budget - why?" (stock levels in the generator were unrealistically high; fixed)
4. "Make the dashboard exports pass the small-cell suppression audit, including totals."

## Things the AI got wrong that we caught and fixed (keep this list honest)

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

## Limits of AI use

- No real patient data was ever given to the AI; all encounter data are synthetic.
- The salt and access tokens are generated locally (`make env`) and were never shared.
- Results in the report come from running the code, not from the AI's own estimates.
