#!/usr/bin/env python3
"""
Phase 3 (PJM): Load PJM DA LMP and DA Transmission Constraint data
into lmp_analysis.duckdb from local CSV exports.

Data Sources  (local CSV files in the project directory)
---------------------------------------------------------
LMPs    : "PJM 2025 da_hrl_lmps.csv"
          Downloaded from https://dataminer2.pjm.com/feed/da_hrl_lmps
          Columns: datetime_beginning_ept, pnode_name, type, zone,
                   total_lmp_da, congestion_price_da,
                   marginal_loss_price_da, system_energy_price_da
          All rows are type=ZONE; the `zone` column is empty —
          pnode_name IS the zone code.

BindCon : "PJM 2025 constraint da_marginal_value.csv"
          Downloaded from https://dataminer2.pjm.com/feed/da_marginal_value
          Columns: datetime_beginning_ept, datetime_ending_ept,
                   monitored_facility, contingency_facility, shadow_price
          One row per binding constraint per hour.
          Raw shadow_price values are negative (PJM sign convention:
          negative = binding congestion cost).  Negated on load to match
          the positive-shadow-price convention used by MISO/SPP/ISONE/NYISO.

pnode_name → utility_ca mapping
---------------------------------
  Most pnode_names match PJM zone codes directly.
  MID-ATL/APS  → APS  (APS zone alternate label in DataMiner2)
  PJM-RTO      → skipped  (system-wide hub, not a load zone)

Run:
    python3 load_pjm_data.py [--year 2025]
    python3 load_pjm_data.py --lmp-csv "path/to/lmps.csv" --bc-csv "path/to/bc.csv"

After completion, run:
    python3 build_analytics_v2.py --rto PJM
"""

import argparse
import binascii
import re
import time
from pathlib import Path

import duckdb
import pandas as pd

BASE_DIR = Path(__file__).parent
DB_PATH  = BASE_DIR / "lmp_analysis.duckdb"

# Default CSV file names (in BASE_DIR)
DEFAULT_LMP_CSV = BASE_DIR / "PJM 2025 da_hrl_lmps.csv"
DEFAULT_BC_CSV  = BASE_DIR / "PJM 2025 constraint da_marginal_value.csv"

# EPT timestamp format used in both files
_EPT_FMT = "%m/%d/%Y %I:%M:%S %p"   # e.g. "1/1/2025 12:00:00 AM"


# ── PJM Zone / CA Reference ───────────────────────────────────────────────────
# zone_code → (full_name, zone_label, zone_color)
PJM_ZONES: dict[str, tuple[str, str, str]] = {
    # ── West (orange palette) ─────────────────────────────────────────────
    "AEP":     ("American Electric Power (OH/IN/WV/VA)",   "PJM West", "#e65c00"),
    "APS":     ("Appalachian Power Service (AEP VA/WV)",   "PJM West", "#ff8c42"),
    "ATSI":    ("AEP Transmission Service (OH)",           "PJM West", "#ff7f0e"),
    "COMED":   ("Commonwealth Edison (IL)",                "PJM West", "#d45f00"),
    "DAY":     ("AES Ohio (Dayton Power & Light)",         "PJM West", "#ffbb78"),
    "DEOK":    ("Duke Energy Ohio / Kentucky",             "PJM West", "#e06010"),
    "DUKE":    ("Duke Energy Carolinas",                   "PJM West", "#ffa040"),
    "EKPC":    ("East Kentucky Power Cooperative",         "PJM West", "#ffcc88"),
    "OVEC":    ("Ohio Valley Electric Corporation",        "PJM West", "#e08000"),
    # ── Mid-Atlantic (blue palette) ───────────────────────────────────────
    "AECO":    ("Atlantic City Electric (NJ)",             "PJM Mid-Atlantic", "#5599cc"),
    "DUQ":     ("Duquesne Light (Pittsburgh PA)",          "PJM Mid-Atlantic", "#1f77b4"),
    "JCPL":    ("Jersey Central Power & Light (NJ)",       "PJM Mid-Atlantic", "#aec7e8"),
    "METED":   ("Metropolitan Edison (FirstEnergy PA)",    "PJM Mid-Atlantic", "#4a90d9"),
    "PECO":    ("PECO Energy (Philadelphia PA)",           "PJM Mid-Atlantic", "#2255aa"),
    "PENELEC": ("Pennsylvania Electric (FirstEnergy PA)",  "PJM Mid-Atlantic", "#6aaed6"),
    "PPL":     ("PPL Electric Utilities (PA)",             "PJM Mid-Atlantic", "#3399cc"),
    "PSEG":    ("Public Service Enterprise Group (NJ)",    "PJM Mid-Atlantic", "#88bbdd"),
    "RECO":    ("Rockland Electric (Orange/Rockland NJ)",  "PJM Mid-Atlantic", "#99ccee"),
    # ── Southeast (green palette) ─────────────────────────────────────────
    "DOM":     ("Dominion Energy Virginia",                "PJM Southeast", "#2ca02c"),
    "BGE":     ("Baltimore Gas & Electric (MD)",           "PJM Southeast", "#56cc56"),
    "DPL":     ("Delmarva Power & Light (MD/DE)",          "PJM Southeast", "#98df8a"),
    "PEPCO":   ("Potomac Electric Power (DC/MD)",          "PJM Southeast", "#44aa44"),
}

_ZONE_SET = set(PJM_ZONES.keys())

# pnode_name → utility_ca  (includes aliases in DataMiner2)
_PNODE_TO_CA: dict[str, str] = {
    **{k: k for k in PJM_ZONES},
    "MID-ATL/APS": "APS",   # DataMiner2 alias for the APS zone
    # "PJM-RTO" intentionally omitted → filtered out as system hub
}

# ── Constraint zone lookup (from_ca / to_ca) ─────────────────────────────────
# Maps monitored_facility name → (from_ca, to_ca) using PJM zone codes.
# These are the most-binding constraints by occurrence in 2025 data.
# Sources: PJM transmission planning docs + substation geography.
PJM_CONSTRAINT_ZONE_MAP: dict[str, tuple[str, str]] = {

    # ── Interface / scheduled ────────────────────────────────────────────────
    "WEST":                                         ("AEP",   "AEP"),
    "EAST":                                         ("DOM",   "DOM"),
    "Western Interface":                            ("AEP",   "AEP"),
    "Eastern Interface":                            ("DOM",   "DOM"),
    # AEP–DOM interface (major East-West congestion boundary)
    "AEP-DOM":                                      ("AEP",   "DOM"),
    "DOM-AEP":                                      ("DOM",   "AEP"),
    # COMED–AEP interface
    "COMED-AEP":                                    ("COMED", "AEP"),
    "AEP-COMED":                                    ("AEP",   "COMED"),
    # ATSI–AEP
    "ATSI-AEP":                                     ("ATSI",  "AEP"),
    # PSEG–PEPCO / NJ–DC corridor
    "PSEG-PEPCO":                                   ("PSEG",  "PEPCO"),
    # DOM–PEPCO
    "DOM-PEPCO":                                    ("DOM",   "PEPCO"),
    # PJM–NYISO / NJ–NY interface
    "SCH - PJ - NY":                                ("PSEG",  "PSEG"),
    "PJM-NYISO":                                    ("PSEG",  "PSEG"),

    # ── New Jersey (PSEG / JCPL / RECO) ─────────────────────────────────────
    # Bergen (Ridgefield Park, NJ) – Hudson (Jersey City, NJ)
    "BERGEN  230 KV  BER-HUD":                      ("PSEG",  "PSEG"),
    "BERGEN_230KVBER-HUD_1_LN":                     ("PSEG",  "PSEG"),
    # Cedar Grove – Clifton-Linden, NJ (Essex/Passaic County)
    "CEDARGRO_230KVCED-CLIB_1_LN":                  ("PSEG",  "PSEG"),
    # Darley Road – N. Ambler Substation (NJ/PA border)
    "DARLEYRD_69KVDAR-NAA_1_LN":                    ("PSEG",  "JCPL"),
    # Linvale – VFT1, NJ (JCPL)
    "LINVFT_230KVLIN-VFT1_1_LN":                    ("JCPL",  "JCPL"),
    # Lenox–NME Shopping Ctr (Mahwah/Paramus area, Bergen County NJ)
    "LENOX-NMESHOPP NML 1090     B  115 KV":        ("PSEG",  "PSEG"),
    # Millville, NJ (Atlantic County)
    "MILLVILL_138KVMIL-SLE_1_LN":                   ("AECO",  "AECO"),

    # ── Maryland / DC (BGE / PEPCO) ───────────────────────────────────────────
    # Nottingham substation, Baltimore County MD
    "NOTTINGH230 KV  2-3":                          ("BGE",   "BGE"),
    # Graceton substation, Harford County MD
    "GRACETON230 KV  GRA-MANO":                     ("BGE",   "BGE"),
    # Gore–Stoney Creek (APS/BGE border, western MD)
    "GORE_APS_138KVGOR-STO_1_LN":                   ("APS",   "BGE"),
    # Haviland–Timberville, VA (APS territory)
    "HAVILAND_138KVHAV-TIM_1_LN":                   ("APS",   "APS"),
    # Bedington–Doubs 500kV (BGE/APS, Washington County MD)
    "L500.Bedington-Doubs":                         ("BGE",   "APS"),
    # Keeney–Rock Springs 500kV (APS/DOM, eastern WV/VA)
    "L500.Keeney-RockSprings.5025":                 ("APS",   "DOM"),
    # FrontRoyal–WarrenCo 500kV (DOM, northern VA)
    "L500.FrontRoyal-WarrenCo.592 + FrontRoyal.CC": ("DOM",   "DOM"),

    # ── Virginia / DOM territory ──────────────────────────────────────────────
    # Ashburn–Goose Creek (Loudoun County VA)
    "ASHBURN-GOOSECRE 227D       B  230 KV":        ("DOM",   "DOM"),
    "ASHBURN-GOOSECRE 227D       A  230 KV":        ("DOM",   "DOM"),
    # Pleasantville–Ashburn (PEPCO/DOM border, Loudoun County VA)
    "PLEASNTV-ASHBURN  274D      A  230 KV":        ("PEPCO", "DOM"),
    "PLEASNTV-ASHBURN  274D      B  230 KV":        ("PEPCO", "DOM"),
    # DOEX530 transformer (Dominion 345/230kV)
    "DOEX530_T2_345203T_XF":                        ("DOM",   "DOM"),

    # ── Pennsylvania (PPL / PENELEC / METED / PECO / DUQ) ───────────────────
    # Gardners–Texas substation (PA, Franklin County)
    "GARDNERS_115KVGAR-TEX_1_LN":                   ("PENELEC","PENELEC"),
    "GARDNERS115 KV  GAR-TEX":                      ("PENELEC","PENELEC"),
    # Carl substation (Carlisle PA, Cumberland County) – PPL/PENELEC
    "CARL PN 115 KV  CAR-GAR":                      ("PPL",   "PPL"),
    # Hanover 69kV (PA, York County)
    "HANOVER 69 KV   HAN-PAR":                      ("PPL",   "PPL"),
    # Blanco (Berks County PA)
    "BLANCO_69KVBLA-NEWM_1_LN":                     ("METED", "METED"),
    # North substation 114 (PPL, Luzerne County PA)
    "114_NORT_138KV114111_1_LN":                    ("PPL",   "PPL"),
    # Haurd substation 94 (PENELEC area PA)
    "94 HAURD-11323    11323     A  138 KV":        ("PENELEC","PENELEC"),
    # Higgins 71 (RECO area, Rockland/Orange County NJ)
    "71_HIGGI_138KV4607_1_LN":                      ("RECO",  "RECO"),
    # Kewan 74 (PPL area)
    "74 KEWAN B1Z1 DIS              138 KV":        ("PPL",   "PPL"),

    # ── Ohio / Indiana (ATSI / AEP / COMED / DAY) ────────────────────────────
    # Dresden nuclear site (Grundy County IL) – COMED
    "12 DRESDEN  45TR81 CT       H  345 KV":        ("COMED", "COMED"),
    # Chicago-Praxair 138kV, Whiting/Hammond IN area – COMED
    "Chicago-Praxair3 138 kV l/o Wilton Center-Dumont 765 kV": ("COMED","COMED"),
    # Duneacre–Michigan City (La Porte County IN) – COMED
    "Duneacre - Michigan City 13839 138 kV l/o Wilton Center - Dumont 765 kV": ("COMED","COMED"),
    # Jordan–West Frankfort (Franklin County IL) – COMED/AEP border
    "Jordan - WFrankfort E 138kV l/o Jordan - Massac 345kV":  ("COMED", "AEP"),
    # East Lima–Maddox Creek (Allen County OH) – ATSI
    "L345.EastLima-MaddoxCreek":                    ("ATSI",  "ATSI"),
    # APSOUTH contingency (APS south territory)
    "APSOUTH contingency 66":                       ("APS",   "APS"),
    # BED-BLA contingency (Belington–Blacksville WV area, AEP/APS)
    "BED-BLA contingency 57":                       ("APS",   "APS"),
}

# Regex to catch "ZONE1-ZONE2" patterns at start of constraint name
_ZONE_RE = re.compile(
    r"^(" + "|".join(sorted(_ZONE_SET, key=len, reverse=True)) + r")"
    r"(?:[-_\s](" + "|".join(sorted(_ZONE_SET, key=len, reverse=True)) + r"))?",
    re.IGNORECASE,
)

# Interface / scheduled type keywords
_INTERFACE_KEYWORDS = ("WEST", "EAST", "INTERFACE", "SCH -", "PJM-NYISO")


# ── Helpers ───────────────────────────────────────────────────────────────────

def _constraint_id(name: str) -> int:
    return binascii.crc32(name.encode()) & 0x7FFFFFFF


def _branch_type(name: str) -> str:
    u = name.upper()
    if any(k in u for k in ("XFMR", "XFM", "_XF", " XF", "TRANS")):
        return "XF"
    if any(k in u for k in _INTERFACE_KEYWORDS):
        return "OT"
    return "LN"


def _extract_cas(name: str) -> tuple[str | None, str | None]:
    """Try PJM_CONSTRAINT_ZONE_MAP first, then regex zone-code pattern."""
    pair = PJM_CONSTRAINT_ZONE_MAP.get(name)
    if pair:
        return pair
    m = _ZONE_RE.match(str(name).strip())
    if m:
        z1 = m.group(1).upper()
        z2 = (m.group(2) or z1).upper()
        return z1, z2
    return None, None


# ── LMP ───────────────────────────────────────────────────────────────────────

def _parse_lmp(path: Path) -> pd.DataFrame:
    """Read the da_hrl_lmps CSV and return long-format da_lmp rows."""
    print(f"  Reading {path.name} …")
    raw = pd.read_csv(path, dtype=str)
    print(f"  {len(raw):,} raw rows")

    # Filter to ZONE type
    raw = raw[raw["type"].str.strip() == "ZONE"].copy()

    # Map pnode_name → utility_ca
    raw["utility_ca"] = raw["pnode_name"].str.strip().map(_PNODE_TO_CA)
    raw = raw.dropna(subset=["utility_ca"])   # drops PJM-RTO and unmapped
    print(f"  {len(raw):,} ZONE rows after mapping ({raw['utility_ca'].nunique()} zones)")

    # Parse EPT timestamp: "1/1/2025 12:00:00 AM"
    ept = pd.to_datetime(raw["datetime_beginning_ept"].str.strip(),
                         format=_EPT_FMT, errors="coerce")
    raw = raw[ept.notna()].copy()
    ept  = ept[ept.notna()]

    raw["market_day"]  = ept.dt.date
    raw["hour_ending"] = (ept.dt.hour + 1).astype("int8")
    raw["node"]        = raw["pnode_name"].str.strip()
    raw["node_type"]   = "Loadzone"
    raw["rto"]         = "PJM"

    component_map = {
        "total_lmp_da":           "LMP",
        "congestion_price_da":    "MCC",
        "marginal_loss_price_da": "MLC",
        "system_energy_price_da": "MEC",
    }
    for col in component_map:
        raw[col] = pd.to_numeric(raw[col], errors="coerce")

    frames = []
    for raw_col, comp_name in component_map.items():
        tmp = raw[["rto", "market_day", "node", "node_type",
                   "hour_ending", "utility_ca", raw_col]].copy()
        tmp.rename(columns={raw_col: "value"}, inplace=True)
        tmp["component"] = comp_name
        frames.append(tmp)

    out = pd.concat(frames, ignore_index=True)
    return out[["rto", "market_day", "node", "node_type", "component",
                "hour_ending", "value", "utility_ca"]]


# ── Binding Constraints ───────────────────────────────────────────────────────

def _parse_bc(path: Path) -> pd.DataFrame:
    """Read the da_marginal_value CSV → da_binding_constraints rows.

    One row per binding constraint per hour.  Shadow prices are negative
    in PJM's raw export (binding cost convention); we negate on load so
    positive = congested, matching MISO/SPP/ISONE/NYISO convention.
    """
    print(f"  Reading {path.name} …")
    raw = pd.read_csv(path, dtype=str)
    print(f"  {len(raw):,} rows")

    ept = pd.to_datetime(raw["datetime_beginning_ept"].str.strip(),
                         format=_EPT_FMT, errors="coerce")
    raw = raw[ept.notna()].copy()
    ept  = ept[ept.notna()]

    raw["market_date"]        = ept.dt.date
    raw["hour_of_occurrence"] = (ept.dt.hour + 1).astype("int8")
    raw["constraint_name"]    = raw["monitored_facility"].str.strip()
    raw["branch_name"]        = raw["constraint_name"]
    raw["contingency_description"] = raw["contingency_facility"].str.strip()

    # Negate: PJM stores shadow prices as negative; positive = congested
    raw["shadow_price"] = -pd.to_numeric(raw["shadow_price"], errors="coerce")

    raw["constraint_id"] = raw["constraint_name"].map(_constraint_id)
    raw["branch_type"]   = raw["constraint_name"].map(_branch_type)

    cas = raw["constraint_name"].map(_extract_cas)
    raw["from_ca"] = cas.map(lambda x: x[0])
    raw["to_ca"]   = cas.map(lambda x: x[1])
    raw["rto"]     = "PJM"

    for col in ("constraint_description", "override", "curve_type",
                "bp1", "pc1", "bp2", "pc2"):
        raw[col] = None

    out = raw[[
        "rto", "market_date", "constraint_id", "constraint_name",
        "branch_name", "branch_type", "from_ca", "to_ca",
        "contingency_description", "hour_of_occurrence", "shadow_price",
        "constraint_description", "override", "curve_type",
        "bp1", "pc1", "bp2", "pc2",
    ]]
    print(f"  {out['constraint_name'].nunique()} unique constraints")
    mapped = out["from_ca"].notna().sum()
    print(f"  Zone-mapped: {mapped:,} / {len(out):,} ({100*mapped/len(out):.1f}%)")
    return out


# ── CA Reference ──────────────────────────────────────────────────────────────

def _build_ca_reference() -> pd.DataFrame:
    return pd.DataFrame([
        {
            "rto":          "PJM",
            "utility_ca":   code,
            "utility_name": name,
            "lrz":          None,
            "zone_label":   zone_label,
            "zone_color":   color,
        }
        for code, (name, zone_label, color) in PJM_ZONES.items()
    ])


# ── DB Write ──────────────────────────────────────────────────────────────────

def _write_to_db(
    con: duckdb.DuckDBPyConnection,
    lmp_df: pd.DataFrame,
    bc_df: pd.DataFrame,
) -> None:
    # ca_reference
    print("  Writing ca_reference …")
    ca_df = _build_ca_reference()
    con.execute("DELETE FROM ca_reference WHERE rto = 'PJM'")
    con.register("_ca", ca_df)
    con.execute("INSERT INTO ca_reference SELECT * FROM _ca")
    con.unregister("_ca")
    print(f"  {len(ca_df)} PJM zones inserted")

    # da_lmp
    if not lmp_df.empty:
        print(f"  Writing {len(lmp_df):,} LMP rows …")
        con.execute("DELETE FROM da_lmp WHERE rto = 'PJM'")
        con.register("_lmp", lmp_df)
        con.execute("INSERT INTO da_lmp SELECT * FROM _lmp")
        con.unregister("_lmp")

    # da_binding_constraints
    con.execute("DELETE FROM da_binding_constraints WHERE rto = 'PJM'")
    if not bc_df.empty:
        print(f"  Writing {len(bc_df):,} BC rows …")
        con.register("_bc", bc_df)
        con.execute("INSERT INTO da_binding_constraints SELECT * FROM _bc")
        con.unregister("_bc")
    else:
        print("  da_binding_constraints: no BC data")

    # Summary
    lmp_row = con.execute("""
        SELECT COUNT(*) AS lmp_rows,
               COUNT(DISTINCT node) AS zones,
               MIN(market_day) AS first_day,
               MAX(market_day) AS last_day
        FROM da_lmp WHERE rto = 'PJM'
    """).fetchone()
    bc_row = con.execute("""
        SELECT COUNT(*) AS bc_rows,
               COUNT(DISTINCT constraint_name) AS constraints,
               COUNT(*) FILTER(WHERE from_ca IS NOT NULL) AS mapped
        FROM da_binding_constraints WHERE rto = 'PJM'
    """).fetchone()
    print(f"\n  LMP: {lmp_row[0]:,} rows | {lmp_row[1]} zones | {lmp_row[2]} → {lmp_row[3]}")
    print(f"  BC : {bc_row[0]:,} rows | {bc_row[1]} constraints | {bc_row[2]:,} zone-mapped")


# ── Entry Point ───────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Load PJM DA LMP + BC from local CSVs")
    parser.add_argument("--lmp-csv", type=Path, default=DEFAULT_LMP_CSV,
                        help="Path to da_hrl_lmps CSV")
    parser.add_argument("--bc-csv",  type=Path, default=DEFAULT_BC_CSV,
                        help="Path to da_transconstraints CSV")
    args = parser.parse_args()

    for p in (args.lmp_csv, args.bc_csv):
        if not p.exists():
            print(f"ERROR: {p} not found.")
            raise SystemExit(1)

    if not DB_PATH.exists():
        print(f"ERROR: {DB_PATH} not found.")
        raise SystemExit(1)

    t0 = time.time()
    print(f"\n=== PJM data load (from CSV) ===\n")

    print("Step 1/3  Parsing LMP CSV …")
    lmp_df = _parse_lmp(args.lmp_csv)
    print(f"  Total LMP rows: {len(lmp_df):,}\n")

    print("Step 2/3  Parsing BC CSV (da_marginal_value — one row per hour) …")
    bc_df = _parse_bc(args.bc_csv)
    print(f"  Total BC rows: {len(bc_df):,}\n")

    print("Step 3/3  Writing to lmp_analysis.duckdb …")
    con = duckdb.connect(str(DB_PATH))
    try:
        _write_to_db(con, lmp_df, bc_df)
    finally:
        con.close()

    print(f"\nDone in {time.time() - t0:.1f}s.")
    print("Next: python3 build_analytics_v2.py --rto PJM")


if __name__ == "__main__":
    main()
