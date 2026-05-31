#!/usr/bin/env python3
"""
Phase 4a (ISONE): Load ISO New England DA LMP and DA Binding Constraint
data into lmp_analysis.duckdb.

Data Sources  (no authentication required)
-------------------------------------------
LMPs    : https://www.iso-ne.com/static-transform/csv/histRpts/da-lmp/
          WW_DALMP_ISO_YYYYMMDD.csv  — fully public static files
          We keep only LOAD ZONE rows (8 zones).

BindCon : https://www.iso-ne.com/transform/csv/hourlydayaheadconstraints
          ?start=YYYYMMDD&end=YYYYMMDD
          Requires a session cookie established by visiting the reports
          page first (no login — just a browser-style session).  The
          script handles this automatically.

BC Coverage Note
-----------------
ISONE has far fewer binding constraints than MISO/SPP (~10-80 per day).
from_ca / to_ca are populated via CONSTRAINT_ZONE_MAP — a geographic lookup
table that maps constraint names to load zones using substation geography
and ISONE planning documents.  ~80 of the top constraints by shadow price
are mapped; the remainder remain NULL (very low impact).

ISONE Load Zones
-----------------
  ME     .Z.MAINE          Maine
  NH     .Z.NEWHAMPSHIRE   New Hampshire
  VT     .Z.VERMONT        Vermont
  CT     .Z.CONNECTICUT    Connecticut
  RI     .Z.RHODEISLAND    Rhode Island
  SEMASS .Z.SEMASS         Southeast Massachusetts
  WCMASS .Z.WCMASS         West/Central Massachusetts
  NEMA   .Z.NEMASSBOST     Northeast Mass / Boston

Run:
    python3 load_isone_data.py [--year 2025] [--workers 15]

After completion, run:
    python3 build_analytics_v2.py --rto ISONE
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
    "https://www.iso-ne.com/static-transform/csv/histRpts/da-lmp/"
    "WW_DALMP_ISO_{y}{m:02d}{d:02d}.csv"
)
BC_URL  = (
    "https://www.iso-ne.com/transform/csv/hourlydayaheadconstraints"
    "?start={start}&end={end}"
)
BC_PORTAL_URL = "https://www.iso-ne.com/isoexpress/web/reports/grid/-/tree/constraint-da"

_LMP_SESSION = requests.Session()
_LMP_SESSION.headers["User-Agent"] = "Mozilla/5.0 (compatible; congestion-tool/1.0)"

_BC_SESSION  = requests.Session()
_BC_SESSION.headers["User-Agent"] = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# ── Zone definitions ──────────────────────────────────────────────────────────
# location_name_in_file → (ca_code, full_name, zone_label, zone_color)
ISONE_ZONES: dict[str, tuple[str, str, str, str]] = {
    ".Z.MAINE":        ("ME",     "Maine",                       "ISONE North",         "#2ca02c"),
    ".Z.NEWHAMPSHIRE": ("NH",     "New Hampshire",               "ISONE North",         "#56cc56"),
    ".Z.VERMONT":      ("VT",     "Vermont",                     "ISONE North",         "#98df8a"),
    ".Z.CONNECTICUT":  ("CT",     "Connecticut",                 "ISONE Southeast",     "#1f77b4"),
    ".Z.RHODEISLAND":  ("RI",     "Rhode Island",                "ISONE Southeast",     "#aec7e8"),
    ".Z.SEMASS":       ("SEMASS", "Southeast Massachusetts",     "ISONE Massachusetts", "#ff7f0e"),
    ".Z.WCMASS":       ("WCMASS", "West/Central Massachusetts",  "ISONE Massachusetts", "#ffbb78"),
    ".Z.NEMASSBOST":   ("NEMA",   "Northeast Mass / Boston",     "ISONE Massachusetts", "#d45f00"),
}

_LOC_TO_CA = {loc: code for loc, (code, *_) in ISONE_ZONES.items()}


# ── Constraint → zone lookup table ───────────────────────────────────────────
# Maps constraint_name → (from_ca, to_ca) using ISONE zone codes.
# For intra-zone lines both sides are the same zone.
# For cross-zone interfaces from_ca and to_ca differ.
# Constraints not listed here remain NULL (no in-territory assignment).
#
# Sources: ISONE transmission planning documents, substation geography,
# and NERC constraint naming conventions.
CONSTRAINT_ZONE_MAP: dict[str, tuple[str, str]] = {

    # ── Vermont ──────────────────────────────────────────────────────────────
    # Burlington area interface
    "BURG":                    ("VT",    "VT"),
    # HQ Phase 1 DC tie at Highgate, Franklin County VT
    "Node_Highgate_Import":    ("VT",    "VT"),
    "Node_Highgate_Export":    ("VT",    "VT"),
    # Shoreham 345 kV substation, Addison County VT (HQ Phase 1 area)
    "Node_Shoreham_Export":    ("VT",    "VT"),
    "Node_Shoreham_Import":    ("VT",    "VT"),
    # Northwest Vermont Interface (VELCO system)
    "NWVT_I":                  ("VT",    "VT"),
    # Sheffield–Highgate corridor, both in northern VT
    "SHFHGE":                  ("VT",    "VT"),
    # Sheffield Wind area, Caledonia County VT (VELCO 115 kV)
    "SHEF":                    ("VT",    "VT"),
    # Larrabee Point substation at Shoreham VT (Lake Champlain shore)
    "LARRABEE64ALN":           ("VT",    "VT"),

    # ── HQ Phase 2 — enters ISONE at Comerford/Moore dam, NH ────────────────
    "Node_Phase2_Import":      ("NH",    "NH"),
    "Node_Phase2_Export":      ("NH",    "NH"),

    # ── New Hampshire ────────────────────────────────────────────────────────
    # Berlin, Coös County NH — major northern NH transmission hub
    "BERLIN1771ALN":           ("NH",    "NH"),
    "BERLIN_1771_A_LN":        ("NH",    "NH"),
    "BERLIN1670-2ALN":         ("NH",    "NH"),
    "BERLN_NHN110ALN":         ("NH",    "NH"),
    # Epping substation, Rockingham County NH
    "EPPING_T61BHEALN":        ("NH",    "NH"),
    "EPPING_T59BHE-2ALN":      ("NH",    "NH"),
    # Mason substation, Hillsborough County NH
    "MASON68ALN":              ("NH",    "NH"),
    "MASON81-1ALN":            ("NH",    "NH"),
    "MASON_81-1_A_LN":         ("NH",    "NH"),
    "MASON_68_A_LN":           ("NH",    "NH"),
    # Manchester, NH (115 kV sub)
    "MANCHSTR1310ALN":         ("NH",    "NH"),
    # Shaw's Hill substation, northern NH (Colebrook/Pittsburg area)
    "SHAWS_HL1272ALN":         ("NH",    "NH"),
    # Salem substation, Rockingham County NH (NH–MA border, NH side)
    "SALEMT146E-1ALN":         ("NH",    "NH"),

    # ── NH–ME interface ──────────────────────────────────────────────────────
    "NHME":                    ("NH",    "ME"),
    "MENH":                    ("ME",    "NH"),

    # ── NH–VT corridor ───────────────────────────────────────────────────────
    # Keene (NH) – Rutland (VT) export path
    "KR-EXP":                  ("NH",    "VT"),

    # ── Maine ────────────────────────────────────────────────────────────────
    # Down East Maine interface
    "DNEAST":                  ("ME",    "ME"),
    # Starks substation, Somerset County ME
    "STARKS278-1ALN":          ("ME",    "ME"),
    "STARKS_278-1_A_LN":       ("ME",    "ME"),
    "STARKS63-2ALN":           ("ME",    "ME"),
    # Orrington substation, Penobscot County ME (near Bangor)
    "ORRINGTN249ALN":          ("ME",    "ME"),
    "ORRINGTN248ALN":          ("ME",    "ME"),
    # Orrington-South direction interface
    "ORR-SO":                  ("ME",    "ME"),
    # Westbrook substation, Cumberland County ME (Portland area)
    "WESTBRK233ALN":           ("ME",    "ME"),
    # Wyman / Yarmouth area, ME (Emera Maine / Versant system)
    "WYMAN_HY215ALN":          ("ME",    "ME"),
    "WYMAN_HY_222-2_OLD_A_LN": ("ME",    "ME"),
    "WYMAN_HY83-2ALN":         ("ME",    "ME"),
    # Wyman Export interface (Maine coast / Portland area)
    "WYM-EX":                  ("ME",    "ME"),
    "WYM-EX STAB":             ("ME",    "ME"),
    # Rumford Import, Oxford County ME
    "RUMFIP":                  ("ME",    "ME"),
    # Livermore Falls substation, Androscoggin County ME
    "LVERMORE289ALN":          ("ME",    "ME"),
    # Bunker Hill area, ME (likely Waldo/Knox County)
    "BUNKR_HL1029-2ALN":       ("ME",    "ME"),
    # Kibby Wind, Franklin County ME
    "KIBW":                    ("ME",    "ME"),
    # Northport, Knox County ME (former Northport Power Station)
    "Node_Northport_Import":   ("ME",    "ME"),
    "Node_Northport_Export":   ("ME",    "ME"),
    # Bearswamp area, ME
    "BEARSWMPE205WALN":        ("ME",    "ME"),
    # Orrington Import
    "OR-IMP":                  ("ME",    "ME"),

    # ── Connecticut ──────────────────────────────────────────────────────────
    # Salisbury, Litchfield County CT (NW corner CT–NY border)
    "Node_Salisbury_Export":   ("CT",    "CT"),
    "Node_Salisbury_Import":   ("CT",    "CT"),
    "SALISBRY690/FVALN":       ("CT",    "CT"),
    # Hartford Ave substations, Hartford CT
    "HARTF_AVE105ALN":         ("CT",    "CT"),
    "HARTF_AV_E105_A_LN":      ("CT",    "CT"),
    "HARTF_AV_F106_A_LN":      ("CT",    "CT"),
    "HARTF_AVF106ALN":         ("CT",    "CT"),
    # NY–NE interface (enters through SW Connecticut — Cos Cob area)
    "NYNE":                    ("CT",    "CT"),
    "NENY":                    ("CT",    "CT"),
    # Connecticut–Millstone corridor (Millstone nuclear, Waterford CT)
    "COMI-S":                  ("CT",    "CT"),
    # Killingly substation, Windham County CT
    "KILLNGLY347ALN":          ("CT",    "CT"),
    # Naugatuck Valley, New Haven County CT
    "S_NAUGTK1580ALN":         ("CT",    "CT"),
    # Woodmont substation, Milford CT (New Haven County)
    "WOODMONT89005B-1ALN":     ("CT",    "CT"),
    # Grand Avenue substation, New Haven CT
    "GRAND_AV89003B-1ALN":     ("CT",    "CT"),
    # Pequonnock substation, Trumbull/Bridgeport CT (Fairfield County)
    "PEQN_OLDTEMP-PEQNALN":    ("CT",    "CT"),
    # Shunpike Road substation, Cromwell CT (Hartford County)
    "SHUNPIKET172S-4ALN":      ("CT",    "CT"),
    "SHUNPIKET172S-7ALN":      ("CT",    "CT"),
    # Shunock substation, eastern CT (Windham County area)
    "SHUNOCK1870S-1ALN":       ("CT",    "CT"),
    # Farmington substation, Hartford County CT
    "FRMINGTNT5HALN":          ("CT",    "CT"),
    # Devon substation, Shelton/Milford CT (New Haven County)
    "DEVON_RR1790-2ALN":       ("CT",    "CT"),
    # Guilford substation, New Haven County CT
    "GUILFORD286ALN":          ("CT",    "CT"),
    # Clifton substation (likely Fairfield County CT)
    "CLIFTON54BHEALN":         ("CT",    "CT"),
    # Graham substation (southern CT)
    "GRAHAM66BHEALN":          ("CT",    "CT"),
    # Connecticut West area interface (Killingly–CT-West corridor)
    "KCW":                     ("CT",    "CT"),

    # ── Rhode Island ─────────────────────────────────────────────────────────
    # Tiverton substation, Newport County RI
    "TIVERTONL14-6ALN":        ("RI",    "RI"),
    "TIVERTONM13-6ALN":        ("RI",    "RI"),
    "TIVERTON_M13-6_A_LN":     ("RI",    "RI"),
    # Johnston substation, Providence County RI
    "JOHNST_TT172S-3ALN":      ("RI",    "RI"),
    # Interface Rhode Island–Massachusetts Forward East
    "IRMF-E":                  ("RI",    "SEMASS"),
    # Bell Rock substation (RI–SEMASS border area)
    "BELL_RKL14-4ALN":         ("RI",    "SEMASS"),

    # ── NEMA — Northeast Massachusetts / Boston ──────────────────────────────
    # Baker Street substation, Burlington/Wilmington MA area (National Grid)
    "BAKER_ST110D110DXF":      ("NEMA",  "NEMA"),
    "BAKER_ST110C110CXF":      ("NEMA",  "NEMA"),
    "BAKER_ST110-511-4ALN":    ("NEMA",  "NEMA"),
    "BAKER_ST_110C_110C_XF":   ("NEMA",  "NEMA"),
    "BAKER_ST_110D_110D_XF":   ("NEMA",  "NEMA"),
    "BAKER_ST110-510-4ALN":    ("NEMA",  "NEMA"),
    "BAKER_ST496-529ALN":      ("NEMA",  "NEMA"),
    "BAKER_ST_110-510-4_A_LN": ("NEMA",  "NEMA"),
    "BAKER_ST_110-511-4_A_LN": ("NEMA",  "NEMA"),
    # Waltham substation, Middlesex County MA (NEMA zone)
    "WALTHAM282-521-2ALN":     ("NEMA",  "NEMA"),
    "WALTHAM_282-521-2_A_LN":  ("NEMA",  "NEMA"),
    "WALTHAM282-520-2ALN":     ("NEMA",  "NEMA"),
    # Part of Waltham 115 kV circuit system
    "ELECTRIC282-521-1ALN":    ("NEMA",  "NEMA"),
    # Fort Hill substation, Roxbury/Boston MA
    "FORT_HIL1090ALN":         ("NEMA",  "NEMA"),
    # Brighton substation, Boston MA
    "BRIGHTON329-531ALN":      ("NEMA",  "NEMA"),
    # Tewksbury substation, Middlesex County MA
    "TEWKSBRY338ALN":          ("NEMA",  "NEMA"),
    "TEWKSBRYO215ALN":         ("NEMA",  "NEMA"),
    # Colburn substation (likely northeast MA)
    "COLBURN514-510ALN":       ("NEMA",  "NEMA"),
    # Baseline and Woodland (north/northeast MA area)
    "BASELINE98BHEALN":        ("NEMA",  "NEMA"),
    "WOODLAND1371ALN":         ("NEMA",  "NEMA"),

    # ── SEMASS — Southeast Massachusetts ────────────────────────────────────
    # Canal generating station, Sandwich MA (Cape Cod Canal area)
    "CANAL1":                  ("SEMASS", "SEMASS"),
    "CANAL":                   ("SEMASS", "SEMASS"),
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _constraint_id(name: str) -> int:
    return binascii.crc32(name.encode()) & 0x7FFFFFFF


def _parse_isone_csv(content: bytes) -> tuple[list[str], pd.DataFrame]:
    """Parse ISONE's C/H/D/T format CSV.

    Returns (header_columns, data_dataframe).
    Files have variable-width rows: C=2 fields, H/D/T=N fields.
    We fix the width by pre-specifying enough column slots.
    """
    # Peek at max width
    try:
        lines = content.decode("utf-8", errors="replace").splitlines()
    except Exception:
        return [], pd.DataFrame()

    max_cols = max((len(l.split(",")) for l in lines if l.strip()), default=0)
    if max_cols == 0:
        return [], pd.DataFrame()

    raw = pd.read_csv(
        io.BytesIO(content),
        header=None,
        dtype=str,
        names=range(max_cols),
        on_bad_lines="warn",
    )

    h_mask = raw.iloc[:, 0].str.strip() == "H"
    d_mask = raw.iloc[:, 0].str.strip() == "D"

    if not h_mask.any() or not d_mask.any():
        return [], pd.DataFrame()

    # Take first H row for column names (more descriptive than second)
    headers = raw[h_mask].iloc[0, 1:].str.strip().tolist()

    data = raw[d_mask].iloc[:, 1:].copy()
    data = data.iloc[:, : len(headers)]
    data.columns = headers
    for col in data.select_dtypes("object").columns:
        data[col] = data[col].str.strip()

    return headers, data


# ── LMP ───────────────────────────────────────────────────────────────────────

def _download_lmp(d: date) -> bytes | None:
    url = LMP_URL.format(y=d.year, m=d.month, d=d.day)
    for attempt in range(3):
        try:
            r = _LMP_SESSION.get(url, timeout=30)
            if r.status_code == 200 and len(r.content) > 200:
                return r.content
            if r.status_code == 404:
                return None
        except Exception as exc:
            if attempt == 2:
                print(f"  WARN LMP {d}: {exc}")
    return None


def _parse_lmp(content: bytes) -> pd.DataFrame:
    """Parse one ISONE LMP daily file → long-format rows for LOAD ZONE nodes."""
    _, data = _parse_isone_csv(content)
    if data.empty:
        return pd.DataFrame()

    needed = {"Location Type", "Location Name", "Date", "Hour Ending",
              "Locational Marginal Price", "Energy Component",
              "Congestion Component", "Marginal Loss Component"}
    if not needed.issubset(data.columns):
        return pd.DataFrame()

    lz = data[data["Location Type"] == "LOAD ZONE"].copy()
    lz["utility_ca"] = lz["Location Name"].map(_LOC_TO_CA)
    lz = lz.dropna(subset=["utility_ca"])
    if lz.empty:
        return pd.DataFrame()

    lz["market_day"]  = pd.to_datetime(lz["Date"], format="%m/%d/%Y").dt.date
    # ISONE uses "02X" for the repeated 2 AM hour on DST fall-back day — strip the X
    lz["hour_ending"] = (
        lz["Hour Ending"].str.replace(r"[^0-9]", "", regex=True)
        .astype(int).astype("int8")
    )
    lz["node"]        = lz["Location Name"]
    lz["node_type"]   = "Loadzone"
    lz["rto"]         = "ISONE"

    price_cols = {
        "Locational Marginal Price": "LMP",
        "Energy Component":          "MEC",
        "Congestion Component":      "MCC",
        "Marginal Loss Component":   "MLC",
    }
    for col in price_cols:
        lz[col] = pd.to_numeric(lz[col], errors="coerce")

    frames = []
    for raw_col, comp in price_cols.items():
        tmp = lz[["rto", "market_day", "node", "node_type",
                  "hour_ending", "utility_ca", raw_col]].copy()
        tmp.rename(columns={raw_col: "value"}, inplace=True)
        tmp["component"] = comp
        frames.append(tmp)

    out = pd.concat(frames, ignore_index=True)
    return out[["rto", "market_day", "node", "node_type", "component",
                "hour_ending", "value", "utility_ca"]]


# ── Binding Constraints ───────────────────────────────────────────────────────

def _establish_bc_session() -> bool:
    """Visit the reports page to get session cookies for the BC endpoint."""
    try:
        r = _BC_SESSION.get(BC_PORTAL_URL, timeout=20)
        return r.status_code == 200
    except Exception as exc:
        print(f"  WARN: Could not establish BC session: {exc}")
        return False


def _download_bc_month(year: int, month: int) -> bytes | None:
    """Download all BC data for one calendar month in a single request."""
    start = date(year, month, 1)
    if month == 12:
        end = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        end = date(year, month + 1, 1) - timedelta(days=1)

    url = BC_URL.format(
        start=start.strftime("%Y%m%d"),
        end=end.strftime("%Y%m%d"),
    )
    for attempt in range(3):
        try:
            r = _BC_SESSION.get(url, timeout=60)
            if r.status_code == 200 and len(r.content) > 100:
                return r.content
            if r.status_code == 403:
                # Session may have expired — re-establish and retry
                print(f"  Session expired for BC {year}-{month:02d}, re-establishing …")
                _establish_bc_session()
                time.sleep(1)
        except Exception as exc:
            if attempt == 2:
                print(f"  WARN BC {year}-{month:02d}: {exc}")
    return None


def _parse_bc(content: bytes) -> pd.DataFrame:
    """Parse one ISONE BC file → da_binding_constraints rows."""
    _, data = _parse_isone_csv(content)
    if data.empty:
        return pd.DataFrame()

    # Expected columns from first H row:
    # "Local Date","Hour Ending","Constraint Name","Contingency Name",
    # "Interface Flag","Marginal Value"
    needed = {"Local Date", "Hour Ending", "Constraint Name",
              "Contingency Name", "Interface Flag", "Marginal Value"}
    if not needed.issubset(data.columns):
        return pd.DataFrame()

    df = data.copy()
    dt = pd.to_datetime(df["Local Date"], format="%m/%d/%Y %H:%M:%S", errors="coerce")
    df["market_date"]       = dt.dt.date
    df["hour_of_occurrence"] = df["Hour Ending"].astype(int).astype("int8")
    df["constraint_name"]   = df["Constraint Name"].str.strip()
    df["branch_name"]       = df["constraint_name"]
    df["shadow_price"]      = pd.to_numeric(df["Marginal Value"], errors="coerce")
    df["contingency_description"] = df["Contingency Name"].str.strip()

    # Interface Flag: Y → "OT" (interface/flowgate), N → "LN" (line)
    df["branch_type"] = df["Interface Flag"].map({"Y": "OT", "N": "LN"}).fillna("LN")

    df["constraint_id"] = df["constraint_name"].map(_constraint_id)
    df["rto"]           = "ISONE"

    # from_ca / to_ca: apply geographic lookup table; unmapped constraints → NULL
    df["from_ca"] = df["constraint_name"].map(
        lambda n: CONSTRAINT_ZONE_MAP.get(n, (None, None))[0]
    )
    df["to_ca"] = df["constraint_name"].map(
        lambda n: CONSTRAINT_ZONE_MAP.get(n, (None, None))[1]
    )

    # Unused columns
    for col in ("constraint_description", "override", "curve_type",
                "bp1", "pc1", "bp2", "pc2"):
        df[col] = None

    return df[[
        "rto", "market_date", "constraint_id", "constraint_name",
        "branch_name", "branch_type", "from_ca", "to_ca",
        "contingency_description", "hour_of_occurrence", "shadow_price",
        "constraint_description", "override", "curve_type",
        "bp1", "pc1", "bp2", "pc2",
    ]]


# ── CA reference ──────────────────────────────────────────────────────────────

def _build_ca_reference() -> pd.DataFrame:
    return pd.DataFrame([
        {
            "rto":          "ISONE",
            "utility_ca":   code,
            "utility_name": name,
            "lrz":          None,
            "zone_label":   zone_label,
            "zone_color":   color,
        }
        for _, (code, name, zone_label, color) in ISONE_ZONES.items()
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
    con.execute("DELETE FROM ca_reference WHERE rto = 'ISONE'")
    con.register("_ca", ca_df)
    con.execute("INSERT INTO ca_reference SELECT * FROM _ca")
    con.unregister("_ca")
    print(f"  {len(ca_df)} ISONE zones inserted")

    # da_lmp
    if not lmp_df.empty:
        print(f"  Writing {len(lmp_df):,} LMP rows …")
        con.execute("DELETE FROM da_lmp WHERE rto = 'ISONE'")
        con.register("_lmp", lmp_df)
        con.execute("INSERT INTO da_lmp SELECT * FROM _lmp")
        con.unregister("_lmp")

    # da_binding_constraints
    con.execute("DELETE FROM da_binding_constraints WHERE rto = 'ISONE'")
    if not bc_df.empty:
        print(f"  Writing {len(bc_df):,} BC rows …")
        con.register("_bc", bc_df)
        con.execute("INSERT INTO da_binding_constraints SELECT * FROM _bc")
        con.unregister("_bc")
    else:
        print("  da_binding_constraints: no BC data loaded")

    # Summary
    lmp_row = con.execute("""
        SELECT COUNT(*) AS lmp_rows, COUNT(DISTINCT node) AS nodes,
               MIN(market_day) AS first_day, MAX(market_day) AS last_day
        FROM da_lmp WHERE rto = 'ISONE'
    """).fetchone()
    bc_row = con.execute("""
        SELECT COUNT(*) AS bc_rows, COUNT(DISTINCT constraint_name) AS constraints
        FROM da_binding_constraints WHERE rto = 'ISONE'
    """).fetchone()
    print(f"\n  LMP: {lmp_row[0]:,} rows | {lmp_row[1]} zones | {lmp_row[2]} → {lmp_row[3]}")
    print(f"  BC : {bc_row[0]:,} rows | {bc_row[1]} distinct constraints")


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Load ISONE DA LMP + BC data")
    parser.add_argument("--year",    type=int, default=2025)
    parser.add_argument("--workers", type=int, default=15)
    args = parser.parse_args()

    if not DB_PATH.exists():
        print(f"ERROR: {DB_PATH} not found.")
        raise SystemExit(1)

    t0 = time.time()
    print(f"\n=== ISONE data load ({args.year}) ===\n")

    today = date.today()

    # ── Step 1: LMP ─────────────────────────────────────────────────────────
    dates = [
        date(args.year, 1, 1) + timedelta(days=i)
        for i in range(366)
        if date(args.year, 1, 1) + timedelta(days=i) < min(today, date(args.year + 1, 1, 1))
    ]
    print(f"Step 1/3  Downloading {len(dates)} daily LMP files ({args.workers} workers) …")

    lmp_frames: list[pd.DataFrame] = []
    done = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = {ex.submit(_download_lmp, d): d for d in dates}
        for fut in concurrent.futures.as_completed(futs):
            content = fut.result()
            done += 1
            if content:
                parsed = _parse_lmp(content)
                if not parsed.empty:
                    lmp_frames.append(parsed)
            if done % 50 == 0:
                print(f"  {done}/{len(dates)} LMP files …")

    lmp_df = pd.concat(lmp_frames, ignore_index=True) if lmp_frames else pd.DataFrame()
    print(f"  Total LMP rows: {len(lmp_df):,}")

    # ── Step 2: Binding Constraints ──────────────────────────────────────────
    months = [m for m in range(1, 13) if date(args.year, m, 1) <= today]
    print(f"\nStep 2/3  Downloading BC for {len(months)} months (session-cookie approach) …")

    print("  Establishing ISO-NE session …")
    if not _establish_bc_session():
        print("  WARN: Could not establish session; BC download may fail")

    bc_frames: list[pd.DataFrame] = []
    for month in months:
        content = _download_bc_month(args.year, month)
        if content:
            parsed = _parse_bc(content)
            if not parsed.empty:
                bc_frames.append(parsed)
                print(f"  BC {args.year}-{month:02d}: {len(parsed):,} rows")
            else:
                print(f"  BC {args.year}-{month:02d}: 0 binding constraints")
        else:
            print(f"  BC {args.year}-{month:02d}: download failed")
        time.sleep(0.3)   # be polite — data is tiny so speed doesn't matter

    bc_df = pd.concat(bc_frames, ignore_index=True) if bc_frames else pd.DataFrame()
    print(f"  Total BC rows: {len(bc_df):,}")

    # ── Step 3: Write ────────────────────────────────────────────────────────
    print("\nStep 3/3  Writing to lmp_analysis.duckdb …")
    con = duckdb.connect(str(DB_PATH))
    try:
        _write_to_db(con, lmp_df, bc_df)
    finally:
        con.close()

    print(f"\nDone in {time.time() - t0:.0f}s.")
    print("Next: python3 build_analytics_v2.py --rto ISONE")


if __name__ == "__main__":
    main()
