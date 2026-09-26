"""Configuration loading shared by every EpiScope stage.

The YAML file is the single source of truth. Any key can be overridden with an
environment variable ``EPISCOPE__<SECTION>__<KEY>`` (double underscores), e.g.
``EPISCOPE__GENERATOR__SCALE_FACTOR=0.25``. Values are parsed as YAML so numbers
and lists keep their types.
"""
from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = Path(os.environ.get("EPISCOPE_CONFIG", REPO_ROOT / "config" / "episcope.yaml"))


def _load_dotenv() -> None:
    """Load ``.env`` (git-ignored) so the salt and tokens never live in the repo."""
    env_file = REPO_ROOT / ".env"
    if not env_file.exists():
        return
    for line in env_file.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def _apply_overrides(cfg: dict[str, Any]) -> dict[str, Any]:
    for key, raw in os.environ.items():
        if not key.startswith("EPISCOPE__"):
            continue
        parts = [p.lower() for p in key.split("__")[1:]]
        node = cfg
        for p in parts[:-1]:
            node = node.setdefault(p, {})
        node[parts[-1]] = yaml.safe_load(raw)
    return cfg


@lru_cache(maxsize=1)
def load_config() -> dict[str, Any]:
    _load_dotenv()
    with open(CONFIG_PATH) as fh:
        cfg = yaml.safe_load(fh)
    return _apply_overrides(cfg)


def path(key: str) -> str:
    """Resolve a ``paths.*`` entry to an absolute path (or pass through s3a:// URIs)."""
    value = str(load_config()["paths"][key])
    if "://" in value:
        return value.rstrip("/")
    # EPISCOPE_DATA_ROOT relocates every working path (not the committed reference data), so several
    # scale factors can be built side by side for the scalability experiment (ENG-2).
    root = os.environ.get("EPISCOPE_DATA_ROOT")
    if root and key != "reference" and (value == "data" or value.startswith("data/")):
        value = root.rstrip("/") + value[len("data"):]
    p = Path(value)
    return str(p if p.is_absolute() else REPO_ROOT / p)


def lake(layer: str, table: str) -> str:
    """Location of a lake table, e.g. ``lake('silver', 'encounters')``."""
    return f"{path('lake_root')}/{layer}/{table}"


def disease_code_map() -> dict[str, str]:
    """ICD-10 3-character category -> disease group."""
    out: dict[str, str] = {}
    for group, spec in load_config()["diseases"].items():
        for code in spec["codes"]:
            out[code] = group
    return out
