#!/usr/bin/env python3
"""
Migrate miso_2025.duckdb → lmp_analysis.duckdb (multi-RTO unified database).

Creates lmp_analysis.duckdb with rto column on every table, adds zone_label /
zone_color to ca_reference, and copies MISO raw data.  Analytics tables are
intentionally NOT migrated — run build_analytics_v2.py after this script.

Safe to re-run: existing lmp_analysis.duckdb will be deleted and recreated.
"""

import sys
from pathlib import Path
import duckdb

BASE_DIR = Path(__file__).parent

SRC_DB = BASE_DIR / "miso_2025.duckdb"
DST_DB = BASE_DIR / "lmp_analysis.duckdb"

# ── MISO zone labels and colours (same as app.py) ─────────────────────────────
LRZ_LABELS = {
    1:  "LRZ 1 — MN/Dakotas",
    2:  "LRZ 2 — Wisconsin/UP Michigan",
    3:  "LRZ 3 — Iowa/Western",
    4:  "LRZ 4 — Illinois/Central",
    5:  "LRZ 5 — Missouri",
    6:  "LRZ 6 — Indiana/Kentucky",
    7:  "LRZ 7 — Michigan",
    8:  "LRZ 8 — Arkansas",
    9:  "LRZ 9 — Louisiana",
    10: "LRZ 10 — Mississippi",
}
LRZ_COLORS = {
    1:  "#1f77b4",
    2:  "#2ca02c",
    3:  "#d62728",
    4:  "#9467bd",
    5:  "#8c564b",
    6:  "#e377c2",
    7:  "#7f7f7f",
    8:  "#bcbd22",
    9:  "#17becf",
    10: "#ff7f0e",
}


def _row_count(con: duckdb.DuckDBPyConnection, table: str) -> int:
    return con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


def main() -> None:
    if not SRC_DB.exists():
        print(f"ERROR: source database not found: {SRC_DB}")
        sys.exit(1)

    # Remove old destination so we get a clean slate
    if DST_DB.exists():
        print(f"Removing existing {DST_DB.name} ...")
        DST_DB.unlink()

    print(f"Opening {SRC_DB.name} (read-only) ...")
    con = duckdb.connect(str(DST_DB))
    con.execute(f"ATTACH '{SRC_DB}' AS miso (READ_ONLY)")

    # ── 1. ca_reference ───────────────────────────────────────────────────────
    print("\n[1/3] Building ca_reference ...")

    # Build a VALUES table for the LRZ → zone_label / zone_color mapping
    lrz_rows = ", ".join(
        f"({lrz}, '{label}', '{LRZ_COLORS[lrz]}')"
        for lrz, label in LRZ_LABELS.items()
    )

    con.execute(f"""
        CREATE TABLE ca_reference AS
        SELECT
            'MISO'                                          AS rto,
            src.utility_ca,
            src.utility_name,
            src.lrz,
            COALESCE(z.zone_label, 'Non-LBA')              AS zone_label,
            COALESCE(z.zone_color, '#aaaaaa')               AS zone_color
        FROM miso.ca_reference src
        LEFT JOIN (
            VALUES {lrz_rows}
        ) z(lrz_id, zone_label, zone_color)
            ON src.lrz = z.lrz_id
        ORDER BY src.lrz, src.utility_ca
    """)
    n = _row_count(con, "ca_reference")
    print(f"  ca_reference: {n:,} rows")

    # ── 2. da_binding_constraints ─────────────────────────────────────────────
    print("\n[2/3] Copying da_binding_constraints (~122 K rows) ...")
    con.execute("""
        CREATE TABLE da_binding_constraints AS
        SELECT 'MISO' AS rto, * FROM miso.da_binding_constraints
    """)
    n = _row_count(con, "da_binding_constraints")
    print(f"  da_binding_constraints: {n:,} rows")

    # ── 3. da_lmp ─────────────────────────────────────────────────────────────
    print("\n[3/3] Copying da_lmp (~65 M rows — this will take a few minutes) ...")
    con.execute("""
        CREATE TABLE da_lmp AS
        SELECT 'MISO' AS rto, * FROM miso.da_lmp
    """)
    n = _row_count(con, "da_lmp")
    print(f"  da_lmp: {n:,} rows")

    con.execute("DETACH miso")
    con.close()

    print(f"\nMigration complete → {DST_DB}")
    print("Next step: python3 build_analytics_v2.py")


if __name__ == "__main__":
    main()
