"""PRV-2 small-cell suppression: primary rule, complementary suppression and a scan of every export."""
from __future__ import annotations

import json

import numpy as np
import pytest

from pipeline.common.config import REPO_ROOT
from serving.suppress import COUNT_KEYS, audit, hierarchical, primary

OUT = REPO_ROOT / "serving" / "out"   # the committed export (tests run with a temporary data root)


@pytest.mark.parametrize("v, out", [(0, 0), (1, None), (4, None), (4.4, None), (4.6, 5), (5, 5), (6, 6), (None, None), (float("nan"), None)])
def test_primary(v, out):
    assert primary(v) == out


def _publish(child, groups):
    cm, parent, pm, nat = hierarchical(child, groups)
    c = [None if m else int(v) for v, m in zip(child, cm)]
    p = [None if m else int(v) for v, m in zip(parent, pm)]
    n = None if nat[1] else int(nat[0])
    return c, p, n


@pytest.mark.parametrize("seed", range(300))
def test_hierarchy_never_recoverable(seed):
    rng = np.random.default_rng(seed)
    n = rng.integers(3, 20)
    groups = np.sort(rng.integers(0, rng.integers(1, 5), n))
    groups = np.unique(groups, return_inverse=True)[1]
    child = rng.choice([0, 0, 1, 2, 3, 4, 5, 6, 9, 20, 100], n)
    c, p, nat = _publish(child, groups)
    assert audit(c, groups, p, nat) == []


def test_single_suppressed_child_forces_a_second():
    c, p, n = _publish(np.array([3, 50, 60, 70]), np.array([0, 0, 0, 0]))
    assert c.count(None) >= 2 and c[0] is None


def _walk(obj, key=None):
    if isinstance(obj, dict):
        for k, v in obj.items():
            yield from _walk(v, k)
    elif isinstance(obj, list):
        for v in obj:
            yield from _walk(v, key)
    else:
        yield key, obj


def test_exports_have_no_small_counts_and_fit_budget():
    out = OUT
    if not (out / "national").exists():
        pytest.skip("no export yet (run make all)")
    for bundle in [d for d in out.iterdir() if d.is_dir()]:
        size = sum(f.stat().st_size for f in bundle.glob("*.json"))
        size += (REPO_ROOT / "dashboard" / "data" / "geo" / "districts.topo.json").stat().st_size
        assert size <= 10 * 1024 * 1024, f"{bundle.name} is {size / 1e6:.1f} MB"
        for f in bundle.glob("*.json"):
            data = json.loads(f.read_text())
            series = data["series"] if f.name == "weekly.json" else data if f.name == "thresholds.json" else {}
            for k, vals in series.items():            # every value of these series is a count
                assert all(v is None or v == 0 or v >= 5 for v in vals), f"{bundle.name}/{f.name}:{k}"
            for key, val in _walk(data):
                if key in COUNT_KEYS and isinstance(val, (int, float)) and not isinstance(val, bool):
                    assert not (1 <= round(val) <= 4), f"{bundle.name}/{f.name}: {key}={val}"


def test_exported_hierarchy_is_safe():
    out = OUT / "national"
    if not out.exists():
        pytest.skip("no export yet")
    w = json.loads((out / "weekly.json").read_text())
    meta = json.loads((out / "meta.json").read_text())
    dists = meta["districts"]
    divs = sorted({d["adm1_pcode"] for d in dists})
    groups = [divs.index(d["adm1_pcode"]) for d in dists]
    for dis in meta["diseases"]:
        for t in range(len(w["weeks"])):
            child = [w["series"][f"{d['adm2_pcode']}|{dis}"][t] for d in dists]
            parent = [w["series"][f"{v}|{dis}"][t] for v in divs]
            assert audit(child, groups, parent, w["series"][f"BD|{dis}"][t]) == [], (dis, w["weeks"][t])
