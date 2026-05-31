#!/usr/bin/env python3
"""
Download and ingest 2025 SPP Day-Ahead Market data into lmp_analysis.duckdb.

Data sources (no auth required):
  LMP:   https://portal.spp.org/file-browser-api/download/da-lmp-by-settlement-location
  BC:    https://portal.spp.org/file-browser-api/download/da-binding-constraints

Run:
    python3 load_spp_data.py [--year 2025] [--workers 12]

After this script completes, run:
    python3 build_analytics_v2.py --rto SPP
"""

import argparse
import binascii
import concurrent.futures
import io
import time
from datetime import date, timedelta
from pathlib import Path

import duckdb
import pandas as pd
import requests

BASE_DIR = Path(__file__).parent
DB_PATH  = BASE_DIR / "lmp_analysis.duckdb"

LMP_URL = (
    "https://portal.spp.org/file-browser-api/download/"
    "da-lmp-by-settlement-location?path=/{y}/{m:02d}/By_Day/DA-LMP-SL-{y}{m:02d}{d:02d}0100.csv"
)
BC_URL = (
    "https://portal.spp.org/file-browser-api/download/"
    "da-binding-constraints?path=/{y}/{m:02d}/By_Day/DA-BC-{y}{m:02d}{d:02d}0100.csv"
)

# ── Utility reference ─────────────────────────────────────────────────────────
# Map settlement-location prefix → (utility_ca, utility_name, zone_label, zone_color)
# Zones: SPP North / SPP Central / SPP South  (mimics MISO's LRZ grouping)
_NORTH_COLOR  = "#2ca02c"
_CENTRAL_COLOR = "#1f77b4"
_SOUTH_COLOR   = "#ff7f0e"

SPP_UTILITIES: dict[str, tuple[str, str, str, str]] = {
    # prefix: (ca_code, full_name, zone_label, zone_color)
    "AEC":   ("AEC",   "Associated Electric Cooperative",              "SPP Central", _CENTRAL_COLOR),
    "AECC":  ("AECC",  "Arkansas Electric Cooperative Corp.",          "SPP South",   _SOUTH_COLOR),
    "BEPM":  ("BEPM",  "Basin Electric Power Marketing",               "SPP North",   _NORTH_COLOR),
    "BLKW":  ("BLKW",  "Black Hills Energy",                           "SPP North",   _NORTH_COLOR),
    "BRAZ":  ("BRAZ",  "Brazos Electric Power Cooperative",            "SPP South",   _SOUTH_COLOR),
    "CSWS":  ("CSWS",  "AEP/SWEPCo",                                   "SPP South",   _SOUTH_COLOR),
    "EDE":   ("EDE",   "Empire District Electric (Liberty Utilities)",  "SPP Central", _CENTRAL_COLOR),
    "GRDA":  ("GRDA",  "Grand River Dam Authority",                    "SPP South",   _SOUTH_COLOR),
    "INDN":  ("INDN",  "City of Independence Power & Light",           "SPP Central", _CENTRAL_COLOR),
    "KCPL":  ("KCPL",  "Evergy Metro / Kansas City Power & Light",     "SPP Central", _CENTRAL_COLOR),
    "KMEA":  ("KMEA",  "Kansas Municipal Energy Agency",               "SPP Central", _CENTRAL_COLOR),
    "LEPA":  ("LEPA",  "Louisiana Energy & Power Authority",           "SPP South",   _SOUTH_COLOR),
    "LES":   ("LES",   "Lincoln Electric System",                      "SPP North",   _NORTH_COLOR),
    "MEAN":  ("MEAN",  "Municipal Energy Agency of Nebraska",          "SPP North",   _NORTH_COLOR),
    "MPS":   ("MPS",   "Evergy Missouri / Missouri Public Service",    "SPP Central", _CENTRAL_COLOR),
    "NPPD":  ("NPPD",  "Nebraska Public Power District",               "SPP North",   _NORTH_COLOR),
    "OKGE":  ("OKGE",  "Oklahoma Gas & Electric",                      "SPP South",   _SOUTH_COLOR),
    "OMPA":  ("OMPA",  "Oklahoma Municipal Power Authority",           "SPP South",   _SOUTH_COLOR),
    "OPPD":  ("OPPD",  "Omaha Public Power District",                  "SPP North",   _NORTH_COLOR),
    "PSO":   ("PSO",   "AEP / Public Service Oklahoma",                "SPP South",   _SOUTH_COLOR),
    "SECI":  ("SECI",  "Sunflower Electric Cooperative Inc.",          "SPP Central", _CENTRAL_COLOR),
    "SPS":   ("SPS",   "Xcel Energy / Southwestern Public Service",    "SPP South",   _SOUTH_COLOR),
    "SPRM":  ("SPRM",  "City Utilities of Springfield (Sprint Middle)", "SPP Central", _CENTRAL_COLOR),
    "WAUE":  ("WAUE",  "Western Area Power (Upper Great Plains-East)", "SPP North",   _NORTH_COLOR),
    "WFEC":  ("WFEC",  "Western Farmers Electric Cooperative",         "SPP South",   _SOUTH_COLOR),
    "WR":    ("WR",    "Evergy Kansas / Westar Energy",                "SPP Central", _CENTRAL_COLOR),
}

# Settlement-location prefixes that are external-market interfaces (excluded from analytics)
_INTERFACE_PREFIXES = {
    "ALTW", "AMRN", "ERCOTE", "ERCOTN", "ISNE", "MISO", "NBSO", "NYIS",
    "ONT", "OVEC", "PJM", "SOCO", "TVA", "MHEB",
    # MISO native utilities appearing as SPP bilateral points
    "DPC", "DUK", "FPC", "FPL", "GRE", "MDU", "NSP", "OTP",
}


def _prefix(node: str) -> str:
    """First segment before '.' or '_'."""
    for ch in (".", "_"):
        if ch in node:
            return node.split(ch)[0]
    return node


def _node_type(node: str) -> str:
    p = _prefix(node).upper()
    if "." in node:
        return "Generator"
    if p in _INTERFACE_PREFIXES:
        return "Interface"
    if p in SPP_UTILITIES:
        return "Loadzone"
    return "Other"


def _utility_ca(node: str) -> str | None:
    p = _prefix(node).upper()
    entry = SPP_UTILITIES.get(p)
    return entry[0] if entry else None


def _constraint_id(name: str) -> int:
    """Stable 31-bit integer from constraint name via CRC32."""
    return binascii.crc32(name.encode()) & 0x7FFFFFFF


def _branch_type(monitored_facility: str) -> str:
    mf = str(monitored_facility).upper()
    if mf.startswith("LN "):
        return "LN"
    if mf.startswith("XFMR "):
        return "XF"
    return "OT"


# ── Download helpers ──────────────────────────────────────────────────────────

_SESSION = requests.Session()
_SESSION.headers["User-Agent"] = "Mozilla/5.0 (compatible; congestion-tool/1.0)"


def _download(url: str, retries: int = 3) -> bytes | None:
    for attempt in range(retries):
        try:
            r = _SESSION.get(url, timeout=30)
            if r.status_code == 200 and len(r.content) > 100:
                return r.content
            if r.status_code == 404:
                return None          # file genuinely missing (holiday/weekend quirk)
        except Exception as exc:
            if attempt == retries - 1:
                print(f"  WARN: {url[-40:]} → {exc}")
    return None


# ── Per-day parsers ───────────────────────────────────────────────────────────

def _parse_lmp(raw: bytes, d: date) -> pd.DataFrame:
    """Return long-format LMP rows for one day."""
    df = pd.read_csv(io.BytesIO(raw), dtype=str)
    # Normalise column names (strip spaces)
    df.columns = df.columns.str.strip()

    # Hour ending: extract from Interval "MM/DD/YYYY HH:MM:SS"
    hour = df["Interval"].str.split(" ").str[1].str.split(":").str[0].astype(int)

    node     = df["Settlement Location"].str.strip()
    node_t   = node.map(_node_type)
    util_ca  = node.map(_utility_ca)

    # Unpivot MCC / MEC / MLC / LMP
    rows = []
    for comp in ("MCC", "MEC", "MLC", "LMP"):
        col = df[comp].astype(float)
        sub = pd.DataFrame({
            "rto":        "SPP",
            "market_day": d,
            "node":       node,
            "node_type":  node_t,
            "component":  comp,
            "hour_ending": hour,
            "value":      col,
            "utility_ca": util_ca,
        })
        rows.append(sub)

    return pd.concat(rows, ignore_index=True)


def _parse_bc(raw: bytes, d: date) -> pd.DataFrame:
    """Return binding-constraint rows for one day."""
    df = pd.read_csv(io.BytesIO(raw), dtype=str)
    df.columns = df.columns.str.strip()

    # Filter to BINDING hours (shadow_price column is numeric)
    df["Shadow Price"] = pd.to_numeric(df["Shadow Price"], errors="coerce")
    df = df.dropna(subset=["Shadow Price"])

    hour = df["Interval"].str.split(" ").str[1].str.split(":").str[0].astype(int)
    cnames = df["Constraint Name"].str.strip()

    return pd.DataFrame({
        "rto":                     "SPP",
        "market_date":             d,
        "constraint_id":           cnames.map(_constraint_id),
        "constraint_name":         cnames,
        "branch_name":             df["Monitored Facility"].str.strip(),
        "branch_type":             df["Monitored Facility"].str.strip().map(_branch_type),
        "from_ca":                 None,   # EIA 411 will fill these (Phase 5)
        "to_ca":                   None,
        "contingency_description": df["Contingency Name"].str.strip(),
        "hour_of_occurrence":      hour,
        "shadow_price":            df["Shadow Price"],
        "constraint_description":  df.get("Constraint Type", pd.Series("", index=df.index)).str.strip(),
        "override":                None,
        "curve_type":              None,
        "bp1": None, "pc1": None, "bp2": None, "pc2": None,
    })


# ── Main download loop ────────────────────────────────────────────────────────

def _all_dates(year: int) -> list[date]:
    d = date(year, 1, 1)
    end = date(year + 1, 1, 1)
    out = []
    while d < end:
        out.append(d)
        d += timedelta(days=1)
    return out


def _download_all(year: int, workers: int) -> tuple[list[pd.DataFrame], list[pd.DataFrame]]:
    dates = _all_dates(year)
    lmp_dfs: list[pd.DataFrame] = []
    bc_dfs:  list[pd.DataFrame] = []
    errors = 0

    def fetch_day(d: date) -> tuple[date, bytes | None, bytes | None]:
        lmp_url = LMP_URL.format(y=d.year, m=d.month, d=d.day)
        bc_url  = BC_URL .format(y=d.year, m=d.month, d=d.day)
        return d, _download(lmp_url), _download(bc_url)

    print(f"  Downloading {len(dates)} days ({workers} workers) ...")
    done = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(fetch_day, d): d for d in dates}
        for fut in concurrent.futures.as_completed(futures):
            d, lmp_raw, bc_raw = fut.result()
            done += 1
            if lmp_raw:
                try:
                    lmp_dfs.append(_parse_lmp(lmp_raw, d))
                except Exception as exc:
                    print(f"  WARN LMP parse {d}: {exc}")
                    errors += 1
            if bc_raw:
                try:
                    bc_dfs.append(_parse_bc(bc_raw, d))
                except Exception as exc:
                    print(f"  WARN BC parse {d}: {exc}")
                    errors += 1
            if done % 50 == 0:
                print(f"    {done}/{len(dates)} days done …")

    print(f"  Download complete. {errors} errors.")
    return lmp_dfs, bc_dfs


# ── Database writes ───────────────────────────────────────────────────────────

def _ingest_lmp(con: duckdb.DuckDBPyConnection, dfs: list[pd.DataFrame]) -> None:
    if not dfs:
        print("  No LMP data to ingest.")
        return
    print("  Concatenating LMP data …")
    lmp = pd.concat(dfs, ignore_index=True)
    lmp["market_day"] = pd.to_datetime(lmp["market_day"])
    lmp["hour_ending"] = lmp["hour_ending"].astype("int8")
    lmp["value"] = lmp["value"].astype("float64")
    print(f"  Deleting existing SPP rows from da_lmp …")
    con.execute("DELETE FROM da_lmp WHERE rto = 'SPP'")
    print(f"  Inserting {len(lmp):,} LMP rows …")
    con.register("_spp_lmp", lmp)
    con.execute("""
        INSERT INTO da_lmp
        SELECT rto, market_day::DATE, node, node_type, component,
               hour_ending::TINYINT, value, utility_ca
        FROM _spp_lmp
    """)
    con.unregister("_spp_lmp")
    n = con.execute("SELECT COUNT(*) FROM da_lmp WHERE rto='SPP'").fetchone()[0]
    print(f"  da_lmp (SPP): {n:,} rows")


def _ingest_bc(con: duckdb.DuckDBPyConnection, dfs: list[pd.DataFrame]) -> None:
    if not dfs:
        print("  No binding constraint data to ingest.")
        return
    print("  Concatenating BC data …")
    bc = pd.concat(dfs, ignore_index=True)
    bc["market_date"] = pd.to_datetime(bc["market_date"])
    bc["shadow_price"] = bc["shadow_price"].astype("float64")
    print(f"  Deleting existing SPP rows from da_binding_constraints …")
    con.execute("DELETE FROM da_binding_constraints WHERE rto = 'SPP'")
    print(f"  Inserting {len(bc):,} BC rows …")
    con.register("_spp_bc", bc)
    con.execute("""
        INSERT INTO da_binding_constraints
        SELECT rto, market_date::DATE, constraint_id::INTEGER, constraint_name,
               branch_name, branch_type, from_ca, to_ca,
               contingency_description, hour_of_occurrence::TINYINT,
               shadow_price, constraint_description,
               override::INTEGER, curve_type,
               bp1::DOUBLE, pc1::DOUBLE, bp2::DOUBLE, pc2::DOUBLE
        FROM _spp_bc
    """)
    con.unregister("_spp_bc")
    n = con.execute("SELECT COUNT(*) FROM da_binding_constraints WHERE rto='SPP'").fetchone()[0]
    print(f"  da_binding_constraints (SPP): {n:,} rows")


def _build_ca_reference(con: duckdb.DuckDBPyConnection) -> None:
    print("  Updating ca_reference for SPP …")
    con.execute("DELETE FROM ca_reference WHERE rto = 'SPP'")
    rows = [
        ("SPP", ca, name, None, zone_label, zone_color)
        for _, (ca, name, zone_label, zone_color) in SPP_UTILITIES.items()
    ]
    con.executemany("""
        INSERT INTO ca_reference (rto, utility_ca, utility_name, lrz, zone_label, zone_color)
        VALUES (?, ?, ?, ?, ?, ?)
    """, rows)
    n = con.execute("SELECT COUNT(*) FROM ca_reference WHERE rto='SPP'").fetchone()[0]
    print(f"  ca_reference (SPP): {n:,} rows")


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--year",    type=int, default=2025)
    parser.add_argument("--workers", type=int, default=12)
    args = parser.parse_args()

    if not DB_PATH.exists():
        print(f"ERROR: {DB_PATH} not found — run migrate_to_multi_rto.py first.")
        raise SystemExit(1)

    t0 = time.time()
    print(f"\n=== SPP {args.year} data loader ===\n")

    print("Step 1/4  Downloading data from portal.spp.org …")
    lmp_dfs, bc_dfs = _download_all(args.year, args.workers)

    con = duckdb.connect(str(DB_PATH))
    try:
        print("\nStep 2/4  Ingesting LMP data …")
        _ingest_lmp(con, lmp_dfs)

        print("\nStep 3/4  Ingesting binding constraints …")
        _ingest_bc(con, bc_dfs)

        print("\nStep 4/4  Updating ca_reference …")
        _build_ca_reference(con)

    finally:
        con.close()

    elapsed = time.time() - t0
    print(f"\nDone in {elapsed/60:.1f} min.")
    print("Next: python3 build_analytics_v2.py --rto SPP")


if __name__ == "__main__":
    main()
