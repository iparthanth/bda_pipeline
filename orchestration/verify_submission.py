"""Pre-submission checklist for the H1 code handover (run before tagging v1.0).

    python -m orchestration.verify_submission

Checks what can be checked mechanically; prints PASS/FAIL/TODO per item. TODO items need a human
(video link, member names, signatures, usability results, reflections).
"""
from __future__ import annotations

import os
import re
import subprocess
import sys

from pipeline.common.config import REPO_ROOT

REQ_IDS = [f"ING-{i}" for i in range(1, 8)] + [f"DQ-{i}" for i in range(1, 5)] + [f"PRV-{i}" for i in range(1, 5)] + \
          [f"AN-{i}" for i in range(1, 11)] + [f"VIS-{i}" for i in range(1, 8)] + [f"ENG-{i}" for i in range(1, 5)]
LAYOUT = ["generator", "pipeline", "analytics", "serving", "dashboard", "tests", "docs"]
FILES = ["README.md", "AI_USAGE.md", "docs/proposal.md", "docs/risk_log.md", "requirements.txt", "docker-compose.yml", ".env.example",
         "Makefile", "docs/EpiScope_report.pdf"]


def git(*args: str) -> str:
    return subprocess.run(["git", *args], cwd=REPO_ROOT, capture_output=True, text=True).stdout


def main() -> int:
    results: list[tuple[str, str, str]] = []

    def check(name: str, ok: bool, detail: str = "", todo: bool = False) -> None:
        results.append(("TODO" if (todo and not ok) else "PASS" if ok else "FAIL", name, detail))

    for d in LAYOUT:
        check(f"folder {d}/", (REPO_ROOT / d).is_dir())
    for f in FILES:
        check(f"file {f}", (REPO_ROOT / f).is_file())
    readme = (REPO_ROOT / "README.md").read_text()
    missing = [r for r in REQ_IDS if f"| {r} |" not in readme]
    check("README traceability table has every requirement ID", not missing, ", ".join(missing))
    for key in ("Prerequisites", "One-command run", "hidden 14-day test drop", "Architecture summary"):
        check(f"README section: {key}", key.lower() in readme.lower())
    check("README video link filled in", "[VIDEO LINK" not in readme.split("\n", 5)[2], "replace the placeholder", todo=True)
    check("member names / group number filled in", "[MEMBER 1 NAME" not in readme and "[GROUP NUMBER]" not in readme, todo=True)
    tracked = git("ls-files").split("\n")
    big = [f for f in tracked if f and (REPO_ROOT / f).is_file() and (REPO_ROOT / f).stat().st_size > 50 * 1024 * 1024]
    check("no tracked file above 50 MB", not big, ", ".join(big))
    check(".env not tracked", ".env" not in tracked)
    secrets = []
    env = REPO_ROOT / ".env"
    if env.exists():
        values = [line.split("=", 1)[1].strip() for line in env.read_text().splitlines() if "=" in line and not line.startswith("#")]
        for f in tracked:
            p = REPO_ROOT / f
            if f and p.is_file() and p.stat().st_size < 5_000_000:
                text = p.read_text(errors="ignore")
                secrets += [f for v in values if len(v) >= 16 and v in text]
    check("no salt/token from .env in tracked files", not secrets, ", ".join(sorted(set(secrets))))
    check("no line-level data tracked", not any(re.match(r"data/(landing|lake|drops|ground_truth)/", f) for f in tracked))
    authors = [line.split("\t")[-1] for line in git("shortlog", "-sne", "HEAD").strip().split("\n") if line.strip()]
    check("commits from three members", len(authors) >= 3, f"{len(authors)} author(s): {', '.join(authors)}", todo=True)
    check("tag checkpoint exists (M2)", "checkpoint" in git("tag").split(), "git push origin checkpoint")
    check("tag v1.0 exists", "v1.0" in git("tag").split(), "git tag v1.0 && git push origin v1.0", todo=True)
    report_tex = (REPO_ROOT / "docs" / "report" / "EpiScope_report.tex")
    if report_tex.exists():
        t = report_tex.read_text() + "".join((REPO_ROOT / "docs" / "report" / f).read_text() for f in
                                            ("appendix_contribution.tex", "appendix_reflections.tex"))
        n = t.count("\\todo{")
        check("report placeholders (names, IDs, links, signatures, reflections, usability) completed", n == 0, f"{n} \\todo left", todo=True)
    width = max(len(r[1]) for r in results)
    for status, name, detail in results:
        print(f"[{status}] {name:<{width}}  {detail}")
    return 1 if any(r[0] == "FAIL" for r in results) else 0


if __name__ == "__main__":
    os.chdir(REPO_ROOT)
    sys.exit(main())
