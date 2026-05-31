#!/usr/bin/env python3
"""
Phase 4b (NYISO): Load NYISO DA LMP data into lmp_analysis.duckdb.

Data Source  (fully public, no auth required)
----------------------------------------------
LMPs    : https://mis.nyiso.com/public/csv/damlbmp/
          {YYYYMM}01damlbmp_zone_csv.zip  — monthly ZIPs
          Each ZIP contains daily CSVs: {YYYYMMDD}damlbmp_zone.csv
          We load the 11 NY load zones (skip external interfaces).

Binding Constraints (P-511A)
-----------------------------
Day-ahead binding constraint shadow prices are available from:
  https://mis.nyiso.com/public/csv/DAMLimitingConstraints/
  {YYYYMM}01DAMLimitingConstraints_csv.zip  — monthly ZIPs
  Each ZIP contains daily CSVs with columns:
    Time Stamp, Time Zone, Limiting Facility, Facility PTID,
    Contingency, Constraint Cost($)
Constraint-to-zone mapping is performed via NYISO_CONSTRAINT_ZONE_MAP
(~50 top constraints mapped; remainder have NULL from_ca/to_ca).

NYISO Load Zones
-----------------
  WEST   Zone A — Western NY
  GENESE Zone B — Genesee
  CENTRL Zone C — Central
  NORTH  Zone D — North
  MHK VL Zone E — Mohawk Valley
  CAPITL Zone F — Capital
  HUD VL Zone G — Hudson Valley
  MILLWD Zone H — Millwood
  DUNWOD Zone I — Dunwoodie
  N.Y.C. Zone J — New York City
  LONGIL Zone K — Long Island

Run:
    python3 load_nyiso_data.py [--year 2025] [--workers 6]

After completion, run:
    python3 build_analytics_v2.py --rto NYISO
"""

import argparse
import binascii
import concurrent.futures
import io
import time
import zipfile
from datetime import date
from pathlib import Path

import duckdb
import pandas as pd
import requests

BASE_DIR = Path(__file__).parent
DB_PATH  = BASE_DIR / "lmp_analysis.duckdb"

ZIP_URL = (
    "https://mis.nyiso.com/public/csv/damlbmp/"
    "{y}{m:02d}01damlbmp_zone_csv.zip"
)

BC_URL = (
    "https://mis.nyiso.com/public/csv/DAMLimitingConstraints/"
    "{y}{m:02d}01DAMLimitingConstraints_csv.zip"
)

_SESSION = requests.Session()
_SESSION.headers["User-Agent"] = "Mozilla/5.0 (compatible; congestion-tool/1.0)"

# ── Zone definitions ──────────────────────────────────────────────────────────
# zone_name_in_file → (full_name, zone_label, zone_color)
NYISO_ZONES: dict[str, tuple[str, str, str]] = {
    # Upstate (green)
    "WEST":   ("Zone A — Western NY",      "NYISO Upstate", "#2ca02c"),
    "GENESE": ("Zone B — Genesee",         "NYISO Upstate", "#56cc56"),
    "CENTRL": ("Zone C — Central",         "NYISO Upstate", "#98df8a"),
    "NORTH":  ("Zone D — North",           "NYISO Upstate", "#44aa44"),
    "MHK VL": ("Zone E — Mohawk Valley",   "NYISO Upstate", "#77cc77"),
    # Capital / Hudson Valley (blue)
    "CAPITL": ("Zone F — Capital",         "NYISO Capital/Hudson", "#1f77b4"),
    "HUD VL": ("Zone G — Hudson Valley",   "NYISO Capital/Hudson", "#4a90d9"),
    # NYC Metro (orange/red)
    "MILLWD": ("Zone H — Millwood",        "NYISO NYC Metro", "#ff7f0e"),
    "DUNWOD": ("Zone I — Dunwoodie",       "NYISO NYC Metro", "#ffbb78"),
    "N.Y.C.": ("Zone J — New York City",   "NYISO NYC Metro", "#d62728"),
    "LONGIL": ("Zone K — Long Island",     "NYISO NYC Metro", "#ff9896"),
}

_ZONE_SET = set(NYISO_ZONES.keys())


# ── Constraint → zone lookup table ───────────────────────────────────────────
# Maps constraint_name (from "Limiting Facility" column) → (from_ca, to_ca).
# Zone codes must match the utility_ca values in NYISO_ZONES above.
# Constraints not listed here remain NULL (no in-territory assignment).
#
# Sources: NYISO P-511A day-ahead limiting constraints archive, NYISO
# transmission planning documents, and substation geography.
NYISO_CONSTRAINT_ZONE_MAP: dict[str, tuple[str, str]] = {

    # ── Interface / scheduled constraints (OT) ───────────────────────────────
    # PJM–NYISO interface (physical entry at Dunwoodie/Linden area – Zone I)
    "SCH - PJ - NY":                          ("DUNWOD", "DUNWOD"),
    # ISONE–NYISO interface (physical entry near Capital district – Zone F)
    "SCH - NE - NY":                          ("CAPITL", "CAPITL"),
    # Ontario–NYISO (Niagara / Moses-Adirondack – Zone A / Zone D)
    "SCH - OH - NY":                          ("WEST",   "WEST"),
    "SCH - CLCO - NE":                        ("WEST",   "WEST"),
    "SCH - CLCO - OH":                        ("WEST",   "WEST"),
    # Quebec–NYISO (HQ imports enter Zone D at northern NY border)
    "SCH - HQ - NY":                          ("NORTH",  "NORTH"),
    "SCH - HQ_IMPORT_EXPORT - NY":            ("NORTH",  "NORTH"),
    # Central East interface — the primary upstate→downstate constraint
    "CENTRAL EAST - VC":                      ("CENTRL", "CAPITL"),
    "CENTRAL EAST":                           ("CENTRL", "CAPITL"),
    # Total East / broader upstate-to-southeast constraint
    "TOTAL EAST":                             ("CENTRL", "CAPITL"),
    "TOTAL EAST - VC":                        ("CENTRL", "CAPITL"),
    # Moses South: power path from upstate (Zone J/A side) into Long Island
    "MOSES SOUTH":                            ("N.Y.C.", "LONGIL"),
    "MOSES SOUTH - VC":                       ("N.Y.C.", "LONGIL"),
    # Neptune DC cable (undersea, NJ → Long Island)
    "NEPTUNE":                                ("DUNWOD", "LONGIL"),
    "NEPTUNE - VC":                           ("DUNWOD", "LONGIL"),
    # Cross Sound Cable (CT → Long Island; enters Zone K from ISONE)
    "CROSS SOUND CABLE":                      ("CAPITL", "LONGIL"),
    "CSC":                                    ("CAPITL", "LONGIL"),
    # Upstate–Southeast interface (generalized)
    "UPSTATE NY-SE":                          ("CENTRL", "CAPITL"),
    # North–South (Zone D → south)
    "NORTH-SOUTH":                            ("NORTH",  "CAPITL"),

    # ── Zone J — New York City (Con Edison territory) ─────────────────────────
    # Astoria Annex 138 kV – Astoria East (Queens); both Zone J
    "ASTANNEX 138 ASTORIAE 138 1":            ("N.Y.C.", "N.Y.C."),
    "ASTANNEX 138 ASTORIAE 138 2":            ("N.Y.C.", "N.Y.C."),
    # Greenwood (Brooklyn) – Vernon (Brooklyn/Queens border)
    "GREENWD 138 VERNON 138 1":               ("N.Y.C.", "N.Y.C."),
    "GREENWD 138 VERNON 138 2":               ("N.Y.C.", "N.Y.C."),
    # Goethals (Staten Island) – Gowanus (Brooklyn)
    "GOETHALS 345 GOWANUS 345 1":             ("N.Y.C.", "N.Y.C."),
    "GOETHALS 345 GOWANUS 345 2":             ("N.Y.C.", "N.Y.C."),
    # Astoria Annex (Queens) – East 13th Street (Manhattan)
    "ASTANNEX 345 E13THSTA 345 1":            ("N.Y.C.", "N.Y.C."),
    "ASTANNEX 345 E13THSTA 345 2":            ("N.Y.C.", "N.Y.C."),
    # Spring Creek (Brooklyn/Queens) – Linden (Brooklyn)
    "SPRNCRK 345 LINDEN 345 1":               ("N.Y.C.", "N.Y.C."),
    "SPRNCRK 345 GOWANUS 345 1":              ("N.Y.C.", "N.Y.C."),
    # Blissville (LIC/Queens) – Astoria Annex
    "BLISSVL 345 ASTANNEX 345 1":             ("N.Y.C.", "N.Y.C."),
    # Mott Haven (South Bronx) – East 13th Street
    "MTTHAVEN 345 E13THSTA 345 1":            ("N.Y.C.", "N.Y.C."),
    "MTTHAVEN 345 E13THSTA 345 2":            ("N.Y.C.", "N.Y.C."),
    # Greenwood – Kingsbridge (Brooklyn to Bronx)
    "GREENWD 138 KINGSBRG 138 1":             ("N.Y.C.", "N.Y.C."),
    # Gowanus internal
    "GOWANUS 345 GOWANUS 138 1":              ("N.Y.C.", "N.Y.C."),
    # East 13th Street internal (Manhattan)
    "E13THSTA 345 E13THSTA 138 1":            ("N.Y.C.", "N.Y.C."),
    # Goethals internal (Staten Island)
    "GOETHALS 345 GOETHALS 138 1":            ("N.Y.C.", "N.Y.C."),
    # Astoria (Queens) substations
    "ASTORIA 345 ASTORIA 138 1":              ("N.Y.C.", "N.Y.C."),
    "ASTANNEX 345 ASTORIA 345 1":             ("N.Y.C.", "N.Y.C."),
    # Poletti / Waterside (Manhattan generation area)
    "POLETTI 345 E13THSTA 345 1":             ("N.Y.C.", "N.Y.C."),
    # Rainey (Brentwood-area 138 kV) – Vernon (Brooklyn); intra-Zone-J-K path
    "RAINEY 138 VERNON 138 1":                ("N.Y.C.", "N.Y.C."),
    # Fox Hills (Staten Island) – Greenwood (Brooklyn)
    "FOXHILLS 138 GREENWD 138 1":             ("N.Y.C.", "N.Y.C."),
    "FOXHILLS 138 GREENWD 138 2":             ("N.Y.C.", "N.Y.C."),
    # East 179th Street (Bronx) – Hellgate (Queens)
    "E179THST 138 HELLGATE 138 1":            ("N.Y.C.", "N.Y.C."),
    "E179THST 138 HELLGATE 138 2":            ("N.Y.C.", "N.Y.C."),
    # Astoria West 138 kV – Hellgate (Queens)
    "ASTORIAW 138 HELLGATE 138 1":            ("N.Y.C.", "N.Y.C."),
    # Fresh Kills – Willowbrook (both Staten Island)
    "FRESHKLS 138 WILLWBRK 138 1":            ("N.Y.C.", "N.Y.C."),
    "FRESHKLS 138 WILLWBRK 138 2":            ("N.Y.C.", "N.Y.C."),
    # Goethals (Staten Island) – Linden CG (NJ border)
    "GOETHALS 345 LINDN_CG 345 1":            ("N.Y.C.", "N.Y.C."),

    # ── Zone I — Dunwoodie (Con Edison, southern Westchester) ─────────────────
    # Dunwoodie (Yonkers) – Shore Road (Bronx)
    "DUNWODIE 345 SHORE_RD 345 1":            ("DUNWOD", "DUNWOD"),
    "DUNWODIE 345 SHORE_RD 345 2":            ("DUNWOD", "DUNWOD"),
    # Dunwoodie transformer (345→138 kV)
    "DUNWODIE 345 DUNWODIE 138 1":            ("DUNWOD", "DUNWOD"),
    # Pelham Bay (eastern Bronx / Pelham area)
    "PELHAM 345 SHORE_RD 345 1":              ("DUNWOD", "DUNWOD"),

    # ── Zone H — Millwood (Con Edison / Orange & Rockland, northern Westchester)
    # Millwood transformer
    "MILLWD 345 MILLWD 138 1":                ("MILLWD", "MILLWD"),
    # Kensico (near Valhalla, Westchester) – Millwood
    "KENSICO 345 MILLWD 345 1":               ("MILLWD", "MILLWD"),
    # Pleasant Ridge (northern Westchester)
    "PLEASAN 345 MILLWD 345 1":               ("MILLWD", "MILLWD"),

    # ── Zone I — Lake Success to Bronx (LI→NYC Metro path) ──────────────────
    # Lake Success (Nassau County/Zone K) – Shore Road 138 (Bronx/Zone I)
    "LAKSUCSS 138 SHORE_RD 138 1":            ("LONGIL", "DUNWOD"),
    "LAKSUCSS 138 SHORE_RD 138 2":            ("LONGIL", "DUNWOD"),

    # ── Zone K — Long Island (LIPA / PSEG-LI) ────────────────────────────────
    # East Garden City – Valley Stream (Nassau County)
    "EGRDNCTY 138 VALLYSTR 138 1":            ("LONGIL", "LONGIL"),
    "EGRDNCTY 138 VALLYSTR 138 2":            ("LONGIL", "LONGIL"),
    # Northport (Suffolk County, 345 kV hub)
    "NORTHP 345 PILGRIM 345 1":               ("LONGIL", "LONGIL"),
    "NORTHP 138 NORTHP 345 1":                ("LONGIL", "LONGIL"),
    "NRTHPORT 138 PILGRIM 138 3":             ("LONGIL", "LONGIL"),
    "NRTHPORT 138 PILGRIM 138 1":             ("LONGIL", "LONGIL"),
    "NRTHPORT 138 PILGRIM 138 2":             ("LONGIL", "LONGIL"),
    # Rainey 345 kV (central LI, Brentwood area)
    "RAINEY 138 RAINEY 345 1":                ("LONGIL", "LONGIL"),
    "RAINEY 345 PILGRIM 345 1":               ("LONGIL", "LONGIL"),
    # Farmingdale (Nassau/Suffolk border)
    "FARMINGD 138 FARMINGD 345 1":            ("LONGIL", "LONGIL"),
    # Valley Stream internal
    "VALLYSTR 138 VALLYSTR 345 1":            ("LONGIL", "LONGIL"),
    # Neptune cable landing (Long Island side)
    "NEPTUNE_LI":                             ("LONGIL", "LONGIL"),
    # MSC/Moses-Shand cable area
    "MOSES 345 SHORE_RD 345 1":               ("N.Y.C.", "LONGIL"),
    # Spring Brook (Nassau) – East Garden City (Nassau)
    "SPRNBRK 345 EGRDNCTR 345 1":             ("LONGIL", "LONGIL"),
    "SPRNBRK 345 EGRDNCTR 345 2":             ("LONGIL", "LONGIL"),
    "SPRNBRK 345 UNIONHBS 345 1":             ("LONGIL", "LONGIL"),
    # Stewart Avenue – Valley Stream (Nassau County)
    "STEWRTAV 138 VALLYSTR 138 1":            ("LONGIL", "LONGIL"),
    "STEWRTAV 138 VALLYSTR 138 2":            ("LONGIL", "LONGIL"),
    # Carle Place – Stewart Avenue (Nassau County)
    "CARLPLCE 138 STEWRTAV 138 1":            ("LONGIL", "LONGIL"),
    # Newbridge – Stewart Avenue (Nassau County)
    "NEWBRDGE 138 STEWRTAV 138 3":            ("LONGIL", "LONGIL"),
    "NEWBRDGE 138 STEWRTAV 138 1":            ("LONGIL", "LONGIL"),
    "NEWBRDGE 138 STEWRTAV 138 2":            ("LONGIL", "LONGIL"),
    # Central Islip – Hauppauge (Suffolk County)
    "C._ISLIP 138 HAUPPAUG 138 1":            ("LONGIL", "LONGIL"),
    # Elwood – Pulaski LI (Suffolk County)
    "ELWOOD 69 PULASKLI 69 1":                ("LONGIL", "LONGIL"),
    "ELWOOD 69 PULASKLI 69 2":                ("LONGIL", "LONGIL"),

    # ── Zone G — Hudson Valley (Con Edison / O&R) ─────────────────────────────
    # Storm King – Lovett (Cornwall to Tomkins Cove area)
    "STORMKNG 345 LOVETT 345 1":              ("HUD VL", "HUD VL"),
    # Newburgh (Orange County)
    "NEWBRGH 345 NEWBRGH 138 1":              ("HUD VL", "HUD VL"),
    # Poughkeepsie (Dutchess County)
    "POUGHKPS 345 POUGHKPS 138 1":            ("HUD VL", "HUD VL"),
    # Cedar Hill (Columbia County, Hudson Valley)
    "CEDAR_HL 345 MILLWD 345 1":              ("HUD VL", "MILLWD"),
    # Crossroads (Dutchess County)
    "CROSSRDS 345 MILLWD 345 1":              ("HUD VL", "MILLWD"),

    # ── Zone F — Capital (National Grid, Albany/Schenectady area) ────────────
    # Rotterdam (Schenectady County)
    "ROTRDAM 345 ROTRDAM 138 1":              ("CAPITL", "CAPITL"),
    # Beekman (Dutchess County – eastern Zone F / Zone G border)
    "BEEKMAN 345 MILLWD 345 1":               ("CAPITL", "MILLWD"),
    "BEEKMAN 345 CROSSRDS 345 1":             ("CAPITL", "HUD VL"),
    # Marcy–Rotterdam (Mohawk Valley to Capital interface)
    "MARCY 345 ROTRDAM 345 1":                ("MHK VL", "CAPITL"),
    # Scotia (Schenectady)
    "SCOTIA 345 ROTRDAM 345 1":               ("CAPITL", "CAPITL"),
    # Gordon Road (New Baltimore, Greene County) – Rotterdam (Schenectady)
    "GORDONRD 230 ROTTRDAM 230 1":            ("CAPITL", "CAPITL"),
    "GORDONRD 230 ROTTRDAM 230 2":            ("CAPITL", "CAPITL"),
    # Cricket Valley (Dutchess County) – Pleasantville (Westchester)
    "CRICKVLY 345 PLSNTVLY 345 1":            ("HUD VL", "MILLWD"),
    "CRICKVLY 345 PLSNTVLY 345 2":            ("HUD VL", "MILLWD"),
    # NPX/ISONE interface (New England Power Exchange, Zone F connection)
    "SCH - NPX_1385":                         ("CAPITL", "CAPITL"),
    "SCH - NPX_AC":                           ("CAPITL", "CAPITL"),

    # ── Zone E — Mohawk Valley (National Grid) ────────────────────────────────
    # Marcy substation (Oneida County, major 345 kV hub)
    "MARCY 345 MARCY 138 1":                  ("MHK VL", "MHK VL"),
    "MARCY 345 MARCY 115 1":                  ("MHK VL", "MHK VL"),

    # ── Zone D — North (National Grid, St. Lawrence / Plattsburgh) ───────────
    # Massena (St. Lawrence County, Ontario/Quebec border)
    "MASSENA 115 MASSENA 345 1":              ("NORTH",  "NORTH"),
    "MASSENA WEST 115 MASSENA 115 1":         ("NORTH",  "NORTH"),
    # Plattsburgh (Clinton County)
    "PLTSBRG 115 PLTSBRG 345 1":              ("NORTH",  "NORTH"),
    # Grass River (St. Lawrence County) – Moses-Saunders substation (Zone D)
    "GRASRIVR 115 MOSES 115 1":               ("NORTH",  "NORTH"),
    "GRASRIVR 115 MOSES 115 2":               ("NORTH",  "NORTH"),
    # Codington – Montreal Falls (northern NY, Clinton/St. Lawrence area)
    "CODINGTN 115 MONTRFL 115 1":             ("NORTH",  "NORTH"),

    # ── Zone C — Central (National Grid, Syracuse area) ───────────────────────
    # Onondaga (Syracuse)
    "ONONDGA 115 ONONDGA 345 1":              ("CENTRL", "CENTRL"),
    # Clay (Onondaga County, near Syracuse)
    "CLAY 345 ONONDGA 345 1":                 ("CENTRL", "CENTRL"),
    # Volney (Oswego County, nuclear area)
    "VOLNEY 345 CLAY 345 1":                  ("CENTRL", "CENTRL"),
    # Scriba – Volney (Oswego County, FitzPatrick/Ginna area)
    "SCRIBA 345 VOLNEY 345 1":                ("CENTRL", "CENTRL"),
    "SCRIBA 345 VOLNEY 345 2":                ("CENTRL", "CENTRL"),
    # Deposit (Delaware County) – Indian Head (Greene County), southern tier
    "DEPOSIT 69 INDIANHD 69 1":               ("CENTRL", "CENTRL"),

    # ── Zone B — Genesee (National Grid, Rochester area) ─────────────────────
    # Lockport (Niagara County)
    "LOCKPORT 345 LOCKPORT 115 1":            ("GENESE", "GENESE"),
    # Rochester area
    "BATAVIA 345 LOCKPORT 345 1":             ("GENESE", "GENESE"),

    # ── Zone A — Western NY (National Grid / NYSEG, Buffalo/Niagara) ─────────
    # Niagara (Niagara Falls, Ontario border)
    "NIAGARA 345 NIAGARA 115 1":              ("WEST",   "WEST"),
    "NIAGARA 345 PACKARD 345 1":              ("WEST",   "WEST"),
    # Tonawanda (Erie County, north of Buffalo)
    "TONAWDA 345 ELMWOOD 345 1":              ("WEST",   "WEST"),
    # Elmwood (Buffalo)
    "ELMWOOD 345 ELMWOOD 115 1":              ("WEST",   "WEST"),
    # Packard (Niagara area)
    "PACKARD 345 PACKARD 115 1":              ("WEST",   "WEST"),
}

# Interface-type constraint prefix patterns → branch_type "OT"
_INTERFACE_NAMES = ("SCH -", "CENTRAL EAST", "TOTAL EAST", "MOSES SOUTH",
                    "NEPTUNE", "CROSS SOUND", "NORTH-SOUTH", "UPSTATE NY",
                    "NPX")


def _constraint_id(name: str) -> int:
    """Stable integer ID for a constraint name (CRC32)."""
    return binascii.crc32(name.encode()) & 0x7FFFFFFF


def _branch_type(name: str) -> str:
    return "OT" if any(name.startswith(p) for p in _INTERFACE_NAMES) else "LN"


# ── Download / parse ──────────────────────────────────────────────────────────

def _download_month(year: int, month: int) -> list[pd.DataFrame]:
    """Download one monthly ZIP and parse all daily CSVs inside it."""
    url = ZIP_URL.format(y=year, m=month)
    try:
        r = _SESSION.get(url, timeout=60)
        if r.status_code == 404:
            return []
        r.raise_for_status()
    except Exception as exc:
        print(f"  WARN {year}-{month:02d}: {exc}")
        return []

    frames = []
    try:
        with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
            for name in sorted(zf.namelist()):
                if not name.endswith(".csv"):
                    continue
                with zf.open(name) as f:
                    parsed = _parse(f.read())
                    if not parsed.empty:
                        frames.append(parsed)
    except Exception as exc:
        print(f"  WARN parse {year}-{month:02d}: {exc}")

    return frames


def _parse(content: bytes) -> pd.DataFrame:
    """Parse one NYISO DA zone LMP daily CSV → long-format rows."""
    try:
        df = pd.read_csv(io.BytesIO(content), dtype=str)
    except Exception:
        return pd.DataFrame()

    if df.empty:
        return pd.DataFrame()

    # Strip column name whitespace
    df.columns = df.columns.str.strip()

    # Expected columns
    needed = ["Time Stamp", "Name", "PTID",
              "LBMP ($/MWHr)",
              "Marginal Cost Losses ($/MWHr)",
              "Marginal Cost Congestion ($/MWHr)"]
    if not all(c in df.columns for c in needed):
        return pd.DataFrame()

    df["Name"] = df["Name"].str.strip()

    # Keep only the 11 NY load zones
    df = df[df["Name"].isin(_ZONE_SET)].copy()
    if df.empty:
        return pd.DataFrame()

    # Parse timestamp → market_day + hour_ending
    ts = pd.to_datetime(df["Time Stamp"].str.strip(), format="%m/%d/%Y %H:%M", errors="coerce")
    df["market_day"]  = ts.dt.date
    # NYISO timestamps are interval-beginning: 00:00 → hour_ending 1
    df["hour_ending"] = (ts.dt.hour + 1).astype("int8")

    # Numeric prices
    # NOTE: NYISO reports "Marginal Cost Congestion" with the OPPOSITE sign
    # from PJM/MISO/ISONE/SPP.  Negating gives the standard convention
    # (positive = import-constrained / more expensive than system lambda,
    #  negative = export-constrained / cheaper).
    # Proof: negating MCC makes MEC = LMP - MLC - MCC constant at $56.27
    # across all 11 zones — the system-wide energy lambda.
    df["lmp"] = pd.to_numeric(df["LBMP ($/MWHr)"],                        errors="coerce")
    df["mlc"] = pd.to_numeric(df["Marginal Cost Losses ($/MWHr)"],         errors="coerce")
    df["mcc"] = -pd.to_numeric(df["Marginal Cost Congestion ($/MWHr)"],    errors="coerce")
    df["mec"] = df["lmp"] - df["mlc"] - df["mcc"]   # energy = LMP - losses - congestion

    df["node"]       = df["Name"]
    df["node_type"]  = "Loadzone"
    df["utility_ca"] = df["Name"]           # zone code IS the CA code for NYISO
    df["rto"]        = "NYISO"

    # Melt → long format
    component_map = {"lmp": "LMP", "mcc": "MCC", "mlc": "MLC", "mec": "MEC"}
    frames = []
    for col, comp in component_map.items():
        tmp = df[["rto", "market_day", "node", "node_type",
                  "hour_ending", "utility_ca", col]].copy()
        tmp.rename(columns={col: "value"}, inplace=True)
        tmp["component"] = comp
        frames.append(tmp)

    out = pd.concat(frames, ignore_index=True)
    return out[["rto", "market_day", "node", "node_type", "component",
                "hour_ending", "value", "utility_ca"]]


# ── Binding Constraints (P-511A) ─────────────────────────────────────────────

def _download_bc_month(year: int, month: int) -> bytes | None:
    """Download the monthly P-511A (DAMLimitingConstraints) ZIP."""
    url = BC_URL.format(y=year, m=month)
    try:
        r = _SESSION.get(url, timeout=60)
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.content
    except Exception as exc:
        print(f"  WARN BC {year}-{month:02d}: {exc}")
        return None


def _parse_bc_zip(content: bytes) -> pd.DataFrame:
    """Parse one monthly P-511A ZIP → da_binding_constraints rows."""
    frames: list[pd.DataFrame] = []
    try:
        with zipfile.ZipFile(io.BytesIO(content)) as zf:
            for name in sorted(zf.namelist()):
                if not name.endswith(".csv"):
                    continue
                with zf.open(name) as f:
                    parsed = _parse_bc_csv(f.read())
                    if not parsed.empty:
                        frames.append(parsed)
    except Exception as exc:
        print(f"  WARN BC parse: {exc}")
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _parse_bc_csv(content: bytes) -> pd.DataFrame:
    """Parse one NYISO P-511A daily CSV → binding constraint rows.

    Columns: Time Stamp, Time Zone, Limiting Facility, Facility PTID,
             Contingency, Constraint Cost($)
    """
    try:
        df = pd.read_csv(io.BytesIO(content), dtype=str)
    except Exception:
        return pd.DataFrame()

    if df.empty:
        return pd.DataFrame()

    df.columns = df.columns.str.strip()

    needed = ["Time Stamp", "Limiting Facility", "Constraint Cost($)"]
    if not all(c in df.columns for c in needed):
        return pd.DataFrame()

    # Parse timestamp — NYISO BC format is "MM/DD/YYYY HH:MM" (no seconds)
    # interval-beginning, so 00:00 → hour_of_occurrence 1
    df["_ts"] = pd.to_datetime(
        df["Time Stamp"].str.strip(),
        format="%m/%d/%Y %H:%M",
        errors="coerce",
    )
    df = df.dropna(subset=["_ts"])
    if df.empty:
        return pd.DataFrame()
    df["market_date"]        = df["_ts"].dt.date
    df["hour_of_occurrence"] = (df["_ts"].dt.hour + 1).astype("int8")
    df.drop(columns=["_ts"], inplace=True)

    # NYISO pads substation names to fixed width — normalize to single spaces
    df["constraint_name"] = (
        df["Limiting Facility"].str.strip().str.replace(r"\s+", " ", regex=True)
    )
    df["branch_name"]     = df["constraint_name"]
    df["shadow_price"]    = pd.to_numeric(df["Constraint Cost($)"], errors="coerce")

    df["contingency_description"] = (
        df["Contingency"].str.strip() if "Contingency" in df.columns else None
    )

    df["branch_type"]    = df["constraint_name"].map(_branch_type)
    df["constraint_id"]  = df["constraint_name"].map(_constraint_id)
    df["rto"]            = "NYISO"

    df["from_ca"] = df["constraint_name"].map(
        lambda n: NYISO_CONSTRAINT_ZONE_MAP.get(n, (None, None))[0]
    )
    df["to_ca"] = df["constraint_name"].map(
        lambda n: NYISO_CONSTRAINT_ZONE_MAP.get(n, (None, None))[1]
    )

    for col in ("constraint_description", "override", "curve_type",
                "bp1", "pc1", "bp2", "pc2"):
        df[col] = None

    return df[[
        "rto", "market_date", "constraint_id", "constraint_name",
        "branch_name", "branch_type", "from_ca", "to_ca",
        "contingency_description", "hour_of_occurrence", "shadow_price",
        "constraint_description", "override", "curve_type",
        "bp1", "pc1", "bp2", "pc2",
    ]].dropna(subset=["market_date", "shadow_price"])


# ── CA reference ──────────────────────────────────────────────────────────────

def _build_ca_reference() -> pd.DataFrame:
    return pd.DataFrame([
        {
            "rto":          "NYISO",
            "utility_ca":   zone_code,
            "utility_name": full_name,
            "lrz":          None,
            "zone_label":   zone_label,
            "zone_color":   color,
        }
        for zone_code, (full_name, zone_label, color) in NYISO_ZONES.items()
    ])


# ── DB write ──────────────────────────────────────────────────────────────────

def _write_to_db(
    con: duckdb.DuckDBPyConnection,
    lmp_df: pd.DataFrame,
    bc_df: pd.DataFrame,
) -> None:
    # ca_reference
    print("  Writing ca_reference …")
    ca_df = _build_ca_reference()
    con.execute("DELETE FROM ca_reference WHERE rto = 'NYISO'")
    con.register("_ca", ca_df)
    con.execute("INSERT INTO ca_reference SELECT * FROM _ca")
    con.unregister("_ca")
    print(f"  {len(ca_df)} NYISO zones inserted")

    # da_lmp
    if not lmp_df.empty:
        print(f"  Writing {len(lmp_df):,} LMP rows …")
        con.execute("DELETE FROM da_lmp WHERE rto = 'NYISO'")
        con.register("_lmp", lmp_df)
        con.execute("INSERT INTO da_lmp SELECT * FROM _lmp")
        con.unregister("_lmp")

    # da_binding_constraints
    con.execute("DELETE FROM da_binding_constraints WHERE rto = 'NYISO'")
    if not bc_df.empty:
        print(f"  Writing {len(bc_df):,} BC rows …")
        con.register("_bc", bc_df)
        con.execute("INSERT INTO da_binding_constraints SELECT * FROM _bc")
        con.unregister("_bc")
    else:
        print("  da_binding_constraints: no BC data loaded")

    # Summary
    lmp_row = con.execute("""
        SELECT COUNT(*) AS lmp_rows,
               COUNT(DISTINCT node) AS nodes,
               MIN(market_day) AS first_day,
               MAX(market_day) AS last_day
        FROM da_lmp WHERE rto = 'NYISO'
    """).fetchone()
    bc_row = con.execute("""
        SELECT COUNT(*) AS bc_rows, COUNT(DISTINCT constraint_name) AS constraints
        FROM da_binding_constraints WHERE rto = 'NYISO'
    """).fetchone()
    print(f"\n  LMP: {lmp_row[0]:,} rows | {lmp_row[1]} zones | {lmp_row[2]} → {lmp_row[3]}")
    print(f"  BC : {bc_row[0]:,} rows | {bc_row[1]} distinct constraints")


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Load NYISO DA zone LMP data")
    parser.add_argument("--year",    type=int, default=2025)
    parser.add_argument("--workers", type=int, default=6,
                        help="Parallel month downloads")
    args = parser.parse_args()

    if not DB_PATH.exists():
        print(f"ERROR: {DB_PATH} not found.")
        raise SystemExit(1)

    t0 = time.time()
    print(f"\n=== NYISO data load ({args.year}) ===\n")

    # Cap months to those already complete
    today = date.today()
    months = [
        m for m in range(1, 13)
        if date(args.year, m, 1) <= today
    ]
    print(f"Step 1/3  Downloading {len(months)} LMP monthly ZIPs ({args.workers} workers) …")

    all_frames: list[pd.DataFrame] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = {ex.submit(_download_month, args.year, m): m for m in months}
        for fut in concurrent.futures.as_completed(futs):
            month = futs[fut]
            month_frames = fut.result()
            if month_frames:
                all_frames.extend(month_frames)
                row_count = sum(len(f) for f in month_frames)
                print(f"  {args.year}-{month:02d}: {row_count:,} rows ({len(month_frames)} days)")
            else:
                print(f"  {args.year}-{month:02d}: no data")

    lmp_df = pd.concat(all_frames, ignore_index=True) if all_frames else pd.DataFrame()
    print(f"  Total LMP rows: {len(lmp_df):,}")

    # ── Step 2: Binding Constraints (P-511A) ─────────────────────────────────
    print(f"\nStep 2/3  Downloading BC for {len(months)} months ({args.workers} workers) …")
    bc_frames: list[pd.DataFrame] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as ex:
        bc_futs = {ex.submit(_download_bc_month, args.year, m): m for m in months}
        for fut in concurrent.futures.as_completed(bc_futs):
            month = bc_futs[fut]
            content = fut.result()
            if content:
                parsed = _parse_bc_zip(content)
                if not parsed.empty:
                    bc_frames.append(parsed)
                    print(f"  {args.year}-{month:02d}: {len(parsed):,} BC rows")
                else:
                    print(f"  {args.year}-{month:02d}: BC ZIP downloaded but no rows parsed")
            else:
                print(f"  {args.year}-{month:02d}: no BC data")

    bc_df = pd.concat(bc_frames, ignore_index=True) if bc_frames else pd.DataFrame()
    print(f"  Total BC rows: {len(bc_df):,}")
    if not bc_df.empty:
        mapped = bc_df["from_ca"].notna().sum()
        pct = 100 * mapped / len(bc_df)
        print(f"  Mapped to zones: {mapped:,} / {len(bc_df):,} ({pct:.1f}%)")

    print("\nStep 3/3  Writing to lmp_analysis.duckdb …")
    con = duckdb.connect(str(DB_PATH))
    try:
        _write_to_db(con, lmp_df, bc_df)
    finally:
        con.close()

    print(f"\nDone in {time.time() - t0:.0f}s.")
    print("Next: python3 build_analytics_v2.py --rto NYISO")


if __name__ == "__main__":
    main()
