"""Privacy helpers (PRV-1): salted hashing and age banding.

The salt is read from the ``EPISCOPE_SALT`` environment variable (loaded from the git-ignored ``.env``).
It is never written to the repository, the lake or any log. ``make env`` creates a random one.
"""
from __future__ import annotations

import os

from pyspark.sql import Column
from pyspark.sql import functions as F

from pipeline.common.config import load_config


class MissingSaltError(RuntimeError):
    pass


def get_salt() -> str:
    load_config()  # makes sure .env has been read
    salt = os.environ.get("EPISCOPE_SALT", "")
    if len(salt) < 16:
        raise MissingSaltError("EPISCOPE_SALT is missing or shorter than 16 characters. Run `make env` "
                               "(creates .env with a random salt) - the salt must stay outside the repository.")
    return salt


def hash_id(col: str, salt: str) -> Column:
    """SHA-256 of salt || id; NULL/blank ids stay NULL (they are quarantined anyway)."""
    c = F.trim(F.col(col))
    return F.when(c.isNull() | (c == ""), F.lit(None)).otherwise(F.sha2(F.concat(F.lit(salt), c), 256))


def age_band(col: Column) -> Column:
    """Exact age -> coarse band (the exact age never leaves bronze)."""
    bands = load_config()["privacy"]["age_bands"]
    expr = F.lit(None).cast("string")
    # ascending order, so the outermost WHEN tests the highest lower bound first
    for lo, label in zip(bands, age_band_labels()):
        expr = F.when(col >= lo, F.lit(label)).otherwise(expr)
    return expr


def age_band_labels() -> list[str]:
    bands = load_config()["privacy"]["age_bands"]
    return [f"{lo}-{hi - 1}" for lo, hi in zip(bands[:-1], bands[1:])] + [f"{bands[-1]}+"]
