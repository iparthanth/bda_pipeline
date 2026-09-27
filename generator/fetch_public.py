"""Acquire the public data sources S1, S2, S3 and S7 and cache the raw responses in the landing zone.

Raw responses are cached unmodified under ``data/landing/public/<source>/`` so that the bronze
ingestion (ING-1) stores exactly what the provider returned, and re-runs do not hit the APIs again
(Open-Meteo rate limits are respected with a pause between calls and exponential back-off).

Resolution order per source (recorded in ``data/landing/public/provenance.json`` and shown on the
dashboard, so a viewer always knows whether a series is real or synthetic):

1. ``real`` (pinned)    - S2 and S3 come from real snapshots committed under ``data/reference`` (built by
   ``generator/build_reference.py``). This is the default so that every clone generates exactly the same
   synthetic epidemiology as the report, with or without internet access;
2. ``real`` (download) - ``--refresh`` (or a missing snapshot) queries the providers below and caches the raw
   responses; S1 and S7 are always downloaded when reachable and otherwise read from ``data/reference``;
3. ``synthetic_fallback`` - only when neither is available. A loud warning is printed, the provenance file
   says so and the dashboard shows a banner.

Sources
-------
S1  OCHA COD-AB Bangladesh ADM2 (HDX, CC BY-IGO), v03 2023-05-21, pinned GitHub mirror of the GeoJSON.
S2  pinned: Meta/CIESIN High Resolution Settlement Layer population (CC BY 4.0) aggregated to districts;
    download: OCHA COD-PS Bangladesh ADM2 population (HDX), from the BBS Population & Housing Census 2022.
S3  pinned: NOAA GSOD daily station observations (public domain, BMD synoptic stations), inverse-distance
    weighted to district centroids; download: Open-Meteo historical weather archive (CC BY 4.0).
S7  OpenDengue V1.3 national extract, SEARO (CC BY 4.0) - Bangladesh rows.
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import gzip
import shutil
import io
import json
import sys
import time
import warnings
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd
import requests

from pipeline.common.config import load_config, path

BOUNDARY_URL = (
    "https://raw.githubusercontent.com/meetshaks/bangladesh-administrative-boundaries-json/"
    "7dbe1a69296f4f296e0c973318fb8782806a6a09/bgd_admin2.geojson"
)
HDX_POP_PACKAGE = "https://data.humdata.org/api/3/action/package_show?id=cod-ps-bgd"
OPEN_METEO_URL = "https://archive-api.open-meteo.com/v1/archive"
OPENDENGUE_URL = (
    "https://raw.githubusercontent.com/OpenDengue/master-repo/"
    "f2232b8f1c82bfc38006d8ecebd93677b9ac75d0/assets/National_extract_SEARO_V1_3.zip"
)
WEATHER_VARS = ["precipitation_sum", "temperature_2m_mean", "relative_humidity_2m_mean"]
TIMEOUT = 60


def _public_dir(source: str) -> Path:
    p = Path(path("landing")) / "public" / source
    p.mkdir(parents=True, exist_ok=True)
    return p


def _now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")


def _get(url: str, retries: int = 4, **kw) -> requests.Response:
    delay = 2.0
    for attempt in range(retries):
        try:
            r = requests.get(url, timeout=TIMEOUT, **kw)
            if r.status_code == 429:  # rate limited -> back off
                raise requests.HTTPError("429 rate limited")
            r.raise_for_status()
            return r
        except Exception as exc:  # noqa: BLE001 - network errors are expected offline
            if attempt == retries - 1:
                raise
            print(f"  retry {attempt + 1} for {url[:80]}...: {exc}", file=sys.stderr)
            time.sleep(delay)
            delay *= 2
    raise RuntimeError("unreachable")


def districts() -> pd.DataFrame:
    return pd.read_csv(Path(path("reference")) / "districts.csv")


# ---------------------------------------------------------------- S1 boundaries
def fetch_boundaries(offline: bool) -> dict:
    out = _public_dir("boundaries") / "bgd_admin2.geojson"
    if out.exists():
        return {"status": "real", "file": str(out), "url": BOUNDARY_URL, "note": "cached"}
    if not offline:
        try:
            r = _get(BOUNDARY_URL)
            out.write_bytes(r.content)
            return {"status": "real", "file": str(out), "url": BOUNDARY_URL, "retrieved_at": _now()}
        except Exception as exc:  # noqa: BLE001
            print(f"WARNING S1 download failed ({exc}); using committed reference", file=sys.stderr)
    return {
        "status": "reference",
        "file": str(Path(path("reference")) / "districts.csv"),
        "note": "district list/centroids from OCHA COD-AB v03 committed in data/reference",
    }


# ---------------------------------------------------------------- S2 population
AGE_BANDS = ["0-4", "5-14", "15-24", "25-44", "45-64", "65+"]


def _band_of(start_age: int) -> str:
    for lo, label in zip([65, 45, 25, 15, 5, 0], ["65+", "45-64", "25-44", "15-24", "5-14", "0-4"]):
        if start_age >= lo:
            return label
    return "0-4"


def parse_cod_ps(raw: pd.DataFrame) -> pd.DataFrame:
    """Normalise an OCHA COD-PS ADM2 table to (adm2_pcode, age_band, sex, population)."""
    cols = {c.lower(): c for c in raw.columns}
    pcode_col = next(cols[c] for c in cols if "adm2" in c and "pcode" in c)
    records = []
    for c in raw.columns:
        lc = c.lower()
        # typical COD-PS age columns: F_00_04, M_05_09, F_80plus, T_... (T = total, skipped)
        parts = lc.split("_")
        if len(parts) >= 2 and parts[0] in ("f", "m") and parts[1][:2].isdigit():
            start = int(parts[1][:2])
            records.append((c, "F" if parts[0] == "f" else "M", _band_of(start)))
    if not records:
        raise ValueError("no age/sex columns found in COD-PS table")
    rows = []
    for _, r in raw.iterrows():
        pc = str(r[pcode_col]).strip()
        for c, sex, band in records:
            val = pd.to_numeric(str(r[c]).replace(",", ""), errors="coerce")
            if pd.notna(val):
                rows.append((pc, band, sex, float(val)))
    df = pd.DataFrame(rows, columns=["adm2_pcode", "age_band", "sex", "population"])
    return df.groupby(["adm2_pcode", "age_band", "sex"], as_index=False)["population"].sum()


HRSL_FILE = "population_hrsl_adm2.csv"
# Assumptions for the age bands HRSL does not publish (documented in the report, S2): the remainder
# (5-14 and 25-59) is split in these proportions, and 35% of the 60+ population is aged 60-64.
HRSL_REMAINDER_SPLIT = {"5-14": 0.19, "25-44": 0.30, "45-64": 0.1275}
HRSL_SHARE_60_64 = 0.35


def parse_hrsl(raw: pd.DataFrame) -> pd.DataFrame:
    """District HRSL layers -> (adm2_pcode, age_band, sex, population).

    Real per district: total, men/women, under-5, 15-24 and 60+. The 5-14 / 25-44 / 45-64 split of the
    remainder and the 60-64 share of the 60+ are the stated assumptions above; the sex ratio of each
    district is applied to every band."""
    rows = []
    for r in raw.itertuples(index=False):
        u5, youth, old = r.children_under_five, r.youth_15_24, r.elderly_60_plus
        rest = max(r.population - u5 - youth - old, 0.0)
        w = sum(HRSL_REMAINDER_SPLIT.values())
        bands = {"0-4": u5, "15-24": youth, "65+": old * (1 - HRSL_SHARE_60_64)}
        for band, share in HRSL_REMAINDER_SPLIT.items():
            bands[band] = bands.get(band, 0.0) + rest * share / w
        bands["45-64"] += old * HRSL_SHARE_60_64
        male = r.men / (r.men + r.women) if (r.men + r.women) > 0 else 0.5
        for band, v in bands.items():
            rows += [(r.adm2_pcode, band, "M", v * male), (r.adm2_pcode, band, "F", v * (1 - male))]
    return pd.DataFrame(rows, columns=["adm2_pcode", "age_band", "sex", "population"])


def fetch_population(offline: bool, refresh: bool = False) -> dict:
    d = _public_dir("population")
    pinned = Path(path("reference")) / HRSL_FILE
    if pinned.exists() and not refresh:
        for f in d.glob("*.csv"):
            if f.name != HRSL_FILE:
                f.unlink()                      # one S2 file in the landing zone at a time
        shutil.copyfile(pinned, d / HRSL_FILE)
        src = json.loads((Path(path("reference")) / "population_hrsl_SOURCE.json").read_text())
        return {"status": "real", "file": str(d / HRSL_FILE), "source": src["source"], "licence": src["licence"],
                "url": src["url"], "note": "pinned snapshot in data/reference (generator/build_reference.py)"}
    cached = sorted(f for f in d.glob("*.csv") if not f.name.startswith("SYNTHETIC_FALLBACK") and f.name != HRSL_FILE)
    if cached:
        return {"status": "real", "file": str(cached[0]), "note": "cached"}
    if not offline:
        try:
            meta = _get(HDX_POP_PACKAGE).json()["result"]
            res = [
                r for r in meta["resources"]
                if "adm2" in (r.get("name", "") + r.get("url", "")).lower()
                and (r.get("format", "").lower() == "csv" or r.get("url", "").lower().endswith(".csv"))
            ]
            if not res:
                raise ValueError("no ADM2 CSV resource in HDX COD-PS package")
            url = res[0]["url"]
            content = _get(url).content
            raw = pd.read_csv(io.BytesIO(content), encoding_errors="replace")
            if str(raw.iloc[0, 0]).startswith("#"):
                raw = raw.iloc[1:]  # HXL hashtag row
            parse_cod_ps(raw)  # validate before caching
            out = d / Path(url).name
            out.write_bytes(content)
            return {"status": "real", "file": str(out), "url": url, "retrieved_at": _now(),
                    "licence": "CC BY-IGO (OCHA COD-PS, BBS Census 2022)"}
        except Exception as exc:  # noqa: BLE001
            print(f"WARNING S2 population download failed ({exc})", file=sys.stderr)
    out = d / "SYNTHETIC_FALLBACK_population.csv"
    synth_population().to_csv(out, index=False)
    return {"status": "synthetic_fallback", "file": str(out),
            "note": "HDX unreachable - synthetic denominators; re-run with internet for BBS 2022 data"}


def synth_population() -> pd.DataFrame:
    """Offline fallback only: plausible denominators scaled to the census national total (169.8M)."""
    rng = np.random.default_rng(7)
    dist = districts()
    big = {"BD3026": 12.0, "BD2015": 5.0, "BD3033": 3.5, "BD3067": 3.0, "BD4047": 1.8,
           "BD5081": 1.6, "BD6091": 1.8, "BD2019": 3.2, "BD4561": 2.8, "BD5585": 1.8}
    weight = dist["area_sqkm"].to_numpy() ** 0.6 * rng.lognormal(0, 0.25, len(dist))
    weight *= dist["adm2_pcode"].map(big).fillna(1.0).to_numpy()
    hill = dist["adm2_pcode"].isin(["BD2003", "BD2084", "BD2046"]).to_numpy()
    weight[hill] *= 0.25
    totals = 169_828_911 * weight / weight.sum()
    shares = {"0-4": 0.09, "5-14": 0.19, "15-24": 0.19, "25-44": 0.30, "45-64": 0.17, "65+": 0.06}
    rows = []
    for pc, tot in zip(dist["adm2_pcode"], totals):
        for band, s in shares.items():
            for sex, sx in (("M", 0.495), ("F", 0.505)):
                rows.append((pc, band, sex, round(tot * s * sx)))
    return pd.DataFrame(rows, columns=["adm2_pcode", "age_band", "sex", "population"])


# ---------------------------------------------------------------- S3 weather
def weather_range() -> tuple[str, str]:
    g = load_config()["generator"]
    start = (dt.date.fromisoformat(g["start_date"]) - dt.timedelta(days=180)).isoformat()
    end = dt.date.fromisoformat(g["history_end"]) + dt.timedelta(days=int(g["drop_days"]) + 30)
    end = min(end, dt.date.today() - dt.timedelta(days=6))
    return start, end.isoformat()


def _f_to_c(v: pd.Series) -> pd.Series:
    return (v - 32.0) * 5.0 / 9.0


def read_gsod(files: list[tuple[str, bytes]]) -> pd.DataFrame:
    """Raw GSOD station files (name, bytes; gzip or plain) -> station-day table in SI units."""
    frames = []
    for name, content in files:
        if name.endswith(".gz"):
            content = gzip.decompress(content)
        frames.append(pd.read_csv(io.BytesIO(content), dtype={"STATION": str}))
    g = pd.concat(frames, ignore_index=True)
    out = pd.DataFrame({"station": g["STATION"], "date": pd.to_datetime(g["DATE"]),
                        "lat": g["LATITUDE"].astype(float), "lon": g["LONGITUDE"].astype(float)})
    out["temp_c"] = _f_to_c(g["TEMP"].where(g["TEMP"] < 9000))
    dew = _f_to_c(g["DEWP"].where(g["DEWP"] < 9000))
    # relative humidity from temperature and dew point (Magnus formula)
    a, b = 17.625, 243.04
    out["rh"] = (100 * np.exp(a * dew / (b + dew)) / np.exp(a * out["temp_c"] / (b + out["temp_c"]))).clip(0, 100)
    out["rain_mm"] = g["PRCP"].where(g["PRCP"] < 99) * 25.4
    return out.drop_duplicates(["station", "date"])


def _haversine_km(lat1, lon1, lat2, lon2):
    p1, p2 = np.radians(lat1), np.radians(lat2)
    dlat, dlon = p2 - p1, np.radians(lon2) - np.radians(lon1)
    h = np.sin(dlat / 2) ** 2 + np.cos(p1) * np.cos(p2) * np.sin(dlon / 2) ** 2
    return 6371.0 * 2 * np.arcsin(np.sqrt(h))


def gsod_to_districts(st: pd.DataFrame, dist: pd.DataFrame, start: str, end: str,
                      k: int = 4, radius_km: float = 150.0) -> pd.DataFrame:
    """Inverse-distance-squared weighting of the k nearest stations with a valid value that day (within
    ``radius_km``, else the nearest valid one). Days after the last observation (or with no valid station)
    get the district day-of-year climatology of the same stations and ``source_status = climatology_fill``."""
    days = pd.date_range(start, end, freq="D")
    loc = st.groupby("station")[["lat", "lon"]].median()
    D = _haversine_km(dist["center_lat"].to_numpy()[:, None], dist["center_lon"].to_numpy()[:, None],
                      loc["lat"].to_numpy()[None, :], loc["lon"].to_numpy()[None, :])      # [district, station]
    out = {}
    for var in ("rain_mm", "temp_c", "rh"):
        M = st.pivot(index="date", columns="station", values=var).reindex(index=days, columns=loc.index).to_numpy()
        valid = np.isfinite(M)                                                            # [day, station]
        res = np.full((len(days), len(dist)), np.nan)
        for i in range(len(dist)):
            order = np.argsort(D[i])
            Vi, Mi = valid[:, order], M[:, order]
            rank = np.cumsum(Vi, axis=1)                                                  # k nearest valid
            use = Vi & (rank <= k) & ((D[i, order] <= radius_km)[None, :] | (rank == 1))
            w = np.where(use, 1.0 / np.maximum(D[i, order], 1.0) ** 2, 0.0)
            ws = w.sum(axis=1)
            res[:, i] = np.where(ws > 0, np.nansum(np.where(use, Mi, 0.0) * w, axis=1) / np.where(ws > 0, ws, 1), np.nan)
        out[var] = res
    obs = np.isfinite(out["temp_c"]) & np.isfinite(out["rain_mm"])
    doy = np.minimum(days.dayofyear.to_numpy(), 365)
    for var, res in out.items():                                                          # climatology fill
        clim = np.full((366, len(dist)), np.nan)
        for d0 in range(1, 366):
            win = (np.abs(doy - d0) <= 7) | (np.abs(doy - d0) >= 358)                     # +-7 days, wraps
            with warnings.catch_warnings():                                               # all-NaN windows
                warnings.simplefilter("ignore", RuntimeWarning)
                clim[d0] = np.nanmean(np.where(obs[win], res[win], np.nan), axis=0)
        fill = ~obs | ~np.isfinite(res)
        res[fill] = clim[doy[:, None].repeat(len(dist), 1)[fill], np.nonzero(fill)[1]]
    frames = []
    for i, pc in enumerate(dist["adm2_pcode"]):
        frames.append(pd.DataFrame({
            "adm2_pcode": pc, "date": days.strftime("%Y-%m-%d"),
            "precipitation_sum": out["rain_mm"][:, i].round(1), "temperature_2m_mean": out["temp_c"][:, i].round(1),
            "relative_humidity_2m_mean": out["rh"][:, i].round(0),
            "source_status": np.where(obs[:, i], "real", "climatology_fill")}))
    return pd.concat(frames, ignore_index=True)


def fetch_weather(offline: bool, refresh: bool = False) -> dict:
    d = _public_dir("weather")
    start, end = weather_range()
    dist = districts()
    pinned = sorted((Path(path("reference")) / "gsod").rglob("*.csv.gz"))
    if pinned and not refresh:
        for f in d.glob("*"):
            f.unlink()                          # one S3 source in the landing zone at a time
        for f in pinned:                        # unmodified station files -> bronze (ING-1)
            shutil.copyfile(f, d / f"gsod_{f.parent.name}_{f.name}")
        st = read_gsod([(f.name, f.read_bytes()) for f in pinned])
        last = st.loc[st["temp_c"].notna(), "date"].max().date().isoformat()
        src = json.loads((Path(path("reference")) / "gsod" / "SOURCE.json").read_text())
        return {"status": "real", "dir": str(d), "source": src["source"], "licence": src["licence"], "url": src["url"],
                "stations": int(st["station"].nunique()), "range": [start, end], "observed_until": last,
                "note": f"station observations interpolated to district centroids; days after {last} "
                        "(end of the NOAA mirror) use the stations' day-of-year climatology (source_status "
                        "= climatology_fill)"}
    for f in d.glob("gsod_*"):
        f.unlink()                              # --refresh replaces the pinned station files
    ok, failed = 0, 0
    if not offline:
        for _, r in dist.iterrows():
            out = d / f"open_meteo_{r.adm2_pcode}.json"
            if out.exists():
                ok += 1
                continue
            params = {"latitude": round(r.center_lat, 4), "longitude": round(r.center_lon, 4),
                      "start_date": start, "end_date": end, "daily": ",".join(WEATHER_VARS),
                      "timezone": "Asia/Dhaka"}
            try:
                resp = _get(OPEN_METEO_URL, params=params)
                payload = resp.json()
                payload["_episcope"] = {"adm2_pcode": r.adm2_pcode, "retrieved_at": _now(), "url": resp.url}
                out.write_text(json.dumps(payload))
                ok += 1
                time.sleep(1.0)  # respect the free-tier rate limit
            except Exception as exc:  # noqa: BLE001
                failed += 1
                print(f"WARNING S3 weather {r.adm2_pcode} failed: {exc}", file=sys.stderr)
                if failed >= 3 and ok == 0:
                    break
    if ok == len(dist):
        return {"status": "real", "dir": str(d), "url": OPEN_METEO_URL, "range": [start, end],
                "licence": "CC BY 4.0 Open-Meteo.com"}
    out = d / "SYNTHETIC_FALLBACK_weather.csv"
    synth_weather(start, end).to_csv(out, index=False)
    return {"status": "synthetic_fallback", "file": str(out),
            "note": f"Open-Meteo unreachable ({ok}/{len(dist)} districts) - synthetic monsoon climatology"}


def synth_weather(start: str, end: str) -> pd.DataFrame:
    """Offline fallback only: monsoon climatology with day-to-day noise and extreme-rain events."""
    days = pd.date_range(start, end, freq="D")
    doy = days.dayofyear.to_numpy()
    rows = []
    for i, r in districts().iterrows():
        rng = np.random.default_rng(1000 + i)
        wet = 1.0 + 0.6 * (r.adm1_name in ("Sylhet", "Chattogram")) - 0.3 * (r.adm1_name == "Rajshahi")
        seasonal = 0.4 + 11.5 * np.exp(-0.5 * ((doy - 212) / 42) ** 2) * wet + 2.0 * np.exp(-0.5 * ((doy - 120) / 15) ** 2)
        yearly = np.repeat(rng.lognormal(0, 0.15, len(days) // 365 + 2), 366)[: len(days)]
        rain = rng.gamma(0.45, seasonal * yearly / 0.45)
        rain[rng.random(len(days)) < 0.35] = 0.0
        extreme = rng.random(len(days)) < 0.004 * (seasonal / seasonal.max())
        rain[extreme] += rng.gamma(3, 45, extreme.sum())
        lat_adj = (r.center_lat - 23.5) * -0.6
        temp = 25.5 + lat_adj + 4.8 * np.sin(2 * np.pi * (doy - 100) / 365) - 1.5 * np.cos(4 * np.pi * doy / 365) + rng.normal(0, 1.1, len(days))
        hum = np.clip(65 + 20 * (seasonal / seasonal.max()) + rng.normal(0, 5, len(days)), 35, 99)
        rows.append(pd.DataFrame({"adm2_pcode": r.adm2_pcode, "date": days.date.astype(str),
                                  "precipitation_sum": rain.round(1), "temperature_2m_mean": temp.round(1),
                                  "relative_humidity_2m_mean": hum.round(0)}))
    return pd.concat(rows, ignore_index=True)


# ---------------------------------------------------------------- S7 OpenDengue
def fetch_opendengue(offline: bool) -> dict:
    d = _public_dir("opendengue")
    out = d / "National_extract_SEARO_V1_3.zip"
    if not out.exists() and not offline:
        try:
            out.write_bytes(_get(OPENDENGUE_URL).content)
        except Exception as exc:  # noqa: BLE001
            print(f"WARNING S7 OpenDengue failed ({exc}); using committed reference", file=sys.stderr)
    if out.exists():
        return {"status": "real", "file": str(out), "url": OPENDENGUE_URL, "licence": "CC BY 4.0"}
    return {"status": "reference", "file": str(Path(path("reference")) / "opendengue_bgd_national_V1_3.csv")}


# ---------------------------------------------------------------- loaders used by the generator
def load_weather_daily() -> pd.DataFrame:
    """Parsed daily weather per district (adm2_pcode, date, precipitation_sum, temperature_2m_mean, ...)."""
    d = _public_dir("weather")
    gsod = sorted(d.glob("gsod_*.csv.gz"))
    if gsod:
        start, end = weather_range()
        return gsod_to_districts(read_gsod([(f.name, f.read_bytes()) for f in gsod]), districts(), start, end)
    files = sorted(d.glob("open_meteo_*.json"))
    if len(files) == len(districts()):
        frames = []
        for f in files:
            p = json.loads(f.read_text())
            df = pd.DataFrame(p["daily"]).rename(columns={"time": "date"})
            df["adm2_pcode"] = p["_episcope"]["adm2_pcode"]
            frames.append(df)
        return pd.concat(frames, ignore_index=True)
    return pd.read_csv(d / "SYNTHETIC_FALLBACK_weather.csv")


def load_population() -> pd.DataFrame:
    prov = json.loads((Path(path("landing")) / "public" / "provenance.json").read_text())
    f = Path(prov["S2_population"]["file"])
    if f.name == HRSL_FILE:
        return parse_hrsl(pd.read_csv(f))
    if prov["S2_population"]["status"] == "real":
        raw = pd.read_csv(f, encoding_errors="replace")
        if str(raw.iloc[0, 0]).startswith("#"):
            raw = raw.iloc[1:]
        return parse_cod_ps(raw)
    return pd.read_csv(f)


def load_opendengue_monthly() -> pd.DataFrame:
    ref = Path(path("reference")) / "opendengue_bgd_national_V1_3.csv"
    df = pd.read_csv(ref)
    df = df[df["T_res"] == "Month"].copy()
    df["month"] = pd.to_datetime(df["calendar_start_date"]).dt.month
    return df


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--offline", action="store_true", help="skip network calls (use cache/reference/fallback)")
    ap.add_argument("--refresh", action="store_true",
                    help="ignore the pinned S2/S3 snapshots and download from HDX / Open-Meteo instead")
    args = ap.parse_args(argv)
    prov = {
        "S1_boundaries": fetch_boundaries(args.offline),
        "S2_population": fetch_population(args.offline, args.refresh),
        "S3_weather": fetch_weather(args.offline, args.refresh),
        "S7_opendengue": fetch_opendengue(args.offline),
        "generated_at": _now(),
    }
    out = Path(path("landing")) / "public" / "provenance.json"
    out.write_text(json.dumps(prov, indent=2))
    for k, v in prov.items():
        if isinstance(v, dict):
            flag = "  <-- NOT REAL DATA" if v["status"] == "synthetic_fallback" else ""
            print(f"{k:16s} {v['status']}{flag}")


if __name__ == "__main__":
    main()
