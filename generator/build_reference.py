"""Build the pinned real public-data snapshots committed under ``data/reference`` (S2, S3).

The pipeline reads S2 (population) and S3 (weather) from these snapshots by default, so every machine that
clones the repository generates exactly the same synthetic epidemiology as the report (see
``generator/fetch_public.py`` for the resolution order). This script documents and reproduces how the
snapshots were made; it is run once by the maintainers, not by ``make all``.

S3 weather - NOAA Global Surface Summary of the Day (GSOD), public domain, AWS Open Data mirror
    ``s3://noaa-gsod-pds/<year>/<station>.csv``: daily mean temperature, dew point and precipitation observed
    at the Bangladesh Meteorological Department synoptic stations (WMO 418xx/419xx). The station files are
    stored unmodified (gzip) under ``data/reference/gsod/<year>/``; interpolation to district centroids
    happens downstream (``fetch_public.gsod_to_districts``).

S2 population - Meta / CIESIN High Resolution Settlement Layer (HRSL) population maps, CC BY 4.0, AWS Open
    Data mirror ``s3://dataforgood-fb-data/demographic_csvs/<layer>/``: population per ~30 m cell for the
    total population, men, women, children under five, youth 15-24 and elderly 60+ (constrained to the
    national census total). The ~12 GB of cell files are streamed, filtered to Bangladesh, summed on a
    0.01-degree grid and assigned to districts by point-in-polygon on the OCHA COD-AB ADM2 boundaries; the
    result (one row per district and layer) is ``data/reference/population_hrsl_adm2.csv``.

Usage::

    python -m generator.build_reference weather       # ~12 MB download
    python -m generator.build_reference population    # ~12 GB streamed, ~10 min
"""
from __future__ import annotations

import argparse
import datetime as dt
import gzip
import io
import json
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import requests

from pipeline.common.config import load_config, path

GSOD_BUCKET = "https://noaa-gsod-pds.s3.amazonaws.com"
HRSL_BUCKET = "https://dataforgood-fb-data.s3.amazonaws.com/demographic_csvs"
HRSL_LAYERS = ["population", "men", "women", "children_under_five", "youth_15_24", "elderly_60_plus"]
HRSL_TILES = ["lat_20_lon_80", "lat_20_lon_90"]           # 10-degree tiles that cover Bangladesh
BBOX = (20.5, 26.8, 88.0, 92.8)                           # lat_min, lat_max, lon_min, lon_max
GRID = 0.01                                               # degrees


def _ref() -> Path:
    return Path(path("reference"))


# ---------------------------------------------------------------- S3: NOAA GSOD station files
def build_weather(years: list[int]) -> None:
    out_root = _ref() / "gsod"
    n = 0
    for y in years:
        keys = []
        for prefix in ("418", "419"):                     # WMO block 41 = Bangladesh (and neighbours)
            xml = requests.get(f"{GSOD_BUCKET}/?list-type=2&prefix={y}/{prefix}&max-keys=1000", timeout=60).text
            keys += re.findall(r"<Key>([^<]+\.csv)</Key>", xml)
        for key in keys:
            raw = requests.get(f"{GSOD_BUCKET}/{key}", timeout=120).content
            first = pd.read_csv(io.BytesIO(raw), nrows=1)
            if not str(first["NAME"].iloc[0]).rstrip().endswith(("BG", "BD")):
                continue                                  # keep Bangladesh stations only
            dest = out_root / key.replace(".csv", ".csv.gz")
            dest.parent.mkdir(parents=True, exist_ok=True)
            with gzip.GzipFile(dest, "wb", mtime=0) as fh:  # mtime=0: byte-identical re-builds
                fh.write(raw)
            n += 1
    (out_root / "SOURCE.json").write_text(json.dumps({
        "source": "NOAA Global Surface Summary of the Day (GSOD)", "licence": "public domain (U.S. Government)",
        "url": f"{GSOD_BUCKET}/<year>/<station>.csv", "retrieved_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "files": n, "years": years, "note": "unmodified station files, gzip-compressed"}, indent=2))
    print(f"S3: {n} station-year files -> {out_root}")


# ---------------------------------------------------------------- S2: Meta HRSL population
def _rings(geom: dict) -> list[np.ndarray]:
    polys = geom["coordinates"] if geom["type"] == "MultiPolygon" else [geom["coordinates"]]
    return [np.asarray(ring, dtype=float) for poly in polys for ring in poly]


def points_in_rings(lon: np.ndarray, lat: np.ndarray, rings: list[np.ndarray]) -> np.ndarray:
    """Even-odd ray casting over all rings of a (multi)polygon (holes toggle membership)."""
    inside = np.zeros(lon.shape, dtype=bool)
    for ring in rings:
        x1, y1 = ring[:-1, 0], ring[:-1, 1]
        x2, y2 = ring[1:, 0], ring[1:, 1]
        for a, b, c, d in zip(x1, y1, x2, y2):
            crosses = (b > lat) != (d > lat)
            if not crosses.any():
                continue
            xint = a + (lat - b) * (c - a) / ((d - b) if d != b else 1e-12)
            inside ^= crosses & (lon < xint)
    return inside


def district_of_grid(geojson: Path) -> tuple[np.ndarray, list[str]]:
    """Map every 0.01-degree grid cell centre in the Bangladesh box to a district index (-1 = outside)."""
    lat0, lat1, lon0, lon1 = BBOX
    ny, nx = int(round((lat1 - lat0) / GRID)), int(round((lon1 - lon0) / GRID))
    glat = lat0 + (np.arange(ny) + 0.5) * GRID
    glon = lon0 + (np.arange(nx) + 0.5) * GRID
    owner = np.full((ny, nx), -1, dtype=np.int16)
    feats = json.loads(geojson.read_text())["features"]
    codes = [f["properties"]["adm2_pcode"] for f in feats]
    for k, f in enumerate(feats):
        rings = _rings(f["geometry"])
        allpts = np.vstack(rings)
        (xmin, ymin), (xmax, ymax) = allpts.min(axis=0), allpts.max(axis=0)
        iy = np.where((glat >= ymin) & (glat <= ymax))[0]
        ix = np.where((glon >= xmin) & (glon <= xmax))[0]
        if not len(iy) or not len(ix):
            continue
        LON, LAT = np.meshgrid(glon[ix], glat[iy])
        hit = points_in_rings(LON.ravel(), LAT.ravel(), rings).reshape(LAT.shape)
        sub = owner[np.ix_(iy, ix)]
        sub[hit & (sub < 0)] = k
        owner[np.ix_(iy, ix)] = sub
    return owner, codes


def _stream_layer(layer: str) -> np.ndarray:
    lat0, lat1, lon0, lon1 = BBOX
    ny, nx = int(round((lat1 - lat0) / GRID)), int(round((lon1 - lon0) / GRID))
    grid = np.zeros(ny * nx)
    for tile in HRSL_TILES:
        url = f"{HRSL_BUCKET}/{layer}/{layer}_{tile}.csv"
        with requests.get(url, stream=True, timeout=300) as r:
            r.raise_for_status()
            r.raw.decode_content = True
            for chunk in pd.read_csv(r.raw, chunksize=4_000_000, dtype="float64"):
                la, lo, v = chunk.iloc[:, 0].to_numpy(), chunk.iloc[:, 1].to_numpy(), chunk.iloc[:, 2].to_numpy()
                m = (la >= lat0) & (la < lat1) & (lo >= lon0) & (lo < lon1) & np.isfinite(v)
                iy = ((la[m] - lat0) / GRID).astype(np.int64)
                ix = ((lo[m] - lon0) / GRID).astype(np.int64)
                np.add.at(grid, iy * nx + ix, v[m])
        print(f"  {layer} {tile} done", flush=True)
    return grid.reshape(ny, nx)


def build_population(geojson: Path) -> None:
    owner, codes = district_of_grid(geojson)
    print(f"grid assigned: {(owner >= 0).sum()} cells in {len(set(owner[owner >= 0].tolist()))} districts", flush=True)
    rows = {c: {"adm2_pcode": c} for c in codes}
    outside = {}
    for layer in HRSL_LAYERS:
        grid = _stream_layer(layer)
        inside = owner >= 0
        sums = np.bincount(owner[inside].astype(np.int64), weights=grid[inside], minlength=len(codes))
        outside[layer] = float(grid[~inside].sum())
        for k, c in enumerate(codes):
            rows[c][layer] = round(float(sums[k]), 1)
    df = pd.DataFrame(rows.values()).sort_values("adm2_pcode")
    out = _ref() / "population_hrsl_adm2.csv"
    df.to_csv(out, index=False)
    (_ref() / "population_hrsl_SOURCE.json").write_text(json.dumps({
        "source": "Meta / CIESIN High Resolution Settlement Layer population maps (Data for Good)",
        "licence": "CC BY 4.0", "url": f"{HRSL_BUCKET}/<layer>/<layer>_<tile>.csv", "layers": HRSL_LAYERS,
        "method": f"cells in {BBOX} summed on a {GRID}-degree grid, grid centres assigned to OCHA COD-AB ADM2 "
                  "districts (point in polygon); cells outside every district (India/Myanmar/sea) dropped",
        "outside_districts_in_box": outside,
        "retrieved_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")}, indent=2))
    print(f"S2: {len(df)} districts, total population {df['population'].sum():,.0f} -> {out}")


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description="Build the pinned real S2/S3 snapshots in data/reference")
    ap.add_argument("what", choices=["weather", "population"])
    ap.add_argument("--geojson", default=str(Path(path("landing")) / "public" / "boundaries" / "bgd_admin2.geojson"))
    args = ap.parse_args(argv)
    if args.what == "weather":
        g = load_config()["generator"]
        y0 = dt.date.fromisoformat(g["start_date"]).year - 1
        y1 = dt.date.fromisoformat(g["history_end"]).year + 1
        build_weather(list(range(y0, y1 + 1)))
    else:
        build_population(Path(args.geojson))


if __name__ == "__main__":
    sys.exit(main())
