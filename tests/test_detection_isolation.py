"""The detection code must never read the ground-truth outbreak file (Section 5.2)."""
from __future__ import annotations

from pipeline.common.config import REPO_ROOT

FORBIDDEN = ("ground_truth", "reporting_artefacts", "patient_pairs")
GUARDED = ["analytics/detection", "analytics/forecast", "analytics/demand", "analytics/optimise",
           "analytics/calendar", "analytics/scenario", "pipeline", "serving", "dashboard/js"]


def test_detection_and_pipeline_do_not_reference_ground_truth():
    offenders = []
    for d in GUARDED:
        for f in (REPO_ROOT / d).rglob("*"):
            if f.suffix in {".py", ".js"} and "experiments" not in f.parts:
                text = f.read_text(encoding="utf-8")
                if any(tok in text for tok in FORBIDDEN):
                    offenders.append(f.relative_to(REPO_ROOT).as_posix())
    assert offenders == [], f"ground truth referenced in: {offenders}"


def test_only_evaluation_reads_ground_truth():
    readers = [f.relative_to(REPO_ROOT).as_posix() for f in (REPO_ROOT / "analytics").rglob("*.py")
               if "ground_truth" in f.read_text(encoding="utf-8") or "patient_pairs" in f.read_text(encoding="utf-8")]
    assert set(readers) <= {"analytics/evaluation/detection_eval.py", "analytics/run_analytics.py"}
