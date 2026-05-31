#!/usr/bin/env python3
"""Load MISO 2025 DA LMP and binding constraint data into DuckDB."""

import re
import duckdb
import pandas as pd
from pathlib import Path

RAW_DIR = Path(
    "/Users/maxtuttman/Library/CloudStorage/"
    "GoogleDrive-max@theadhocgroup.com/Shared drives/Heimdall [INTERNAL]/"
    "[INT] Heimdall Claude Projects/Heimdall LMP Analysis/raw_data_2025"
)

DB_PATH = Path("/Users/maxtuttman/Documents/Claude/Claude Code/miso_2025.duckdb")

DA_LMP_FILES = [
    RAW_DIR / "DA.csv",
    RAW_DIR / "DA 2.csv",
    RAW_DIR / "DA 3.csv",
    RAW_DIR / "DA 4.csv",
]

DA_BC_FILE = RAW_DIR / "2025_da_bc_HIST.csv"

# ── Regex to extract branch CA codes from strings like:
#    "COOPER ST_JOCOOPE34_1 A (LN/NPPD/MPS)"  →  LN, NPPD, MPS
#    "OVER X345 XFMR_1_345 (XF/AMMO/*)"       →  XF, AMMO, *
BRANCH_CA_RE = re.compile(r"\(([A-Z]+)/([A-Z*]+)/([A-Z*]+)\)")


def parse_shadow_price(s: str) -> float | None:
    """Convert MISO accounting-format shadow price to float.

    "$1.34"    →  1.34
    "($41.94)" → -41.94
    """
    s = s.strip()
    if not s:
        return None
    negative = s.startswith("(")
    clean = re.sub(r"[$()\s,]", "", s)
    try:
        v = float(clean)
        return -v if negative else v
    except ValueError:
        return None


def create_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("""
        CREATE TABLE IF NOT EXISTS da_lmp (
            market_day      DATE     NOT NULL,
            node            VARCHAR  NOT NULL,
            node_type       VARCHAR  NOT NULL,
            component       VARCHAR  NOT NULL,  -- LMP | MCC | MLC
            hour_ending     TINYINT  NOT NULL,  -- 1-24
            value           DOUBLE,
            utility_ca      VARCHAR             -- CA prefix of node (first dot-segment)
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS da_binding_constraints (
            market_date             DATE     NOT NULL,
            constraint_id           INTEGER,
            constraint_name         VARCHAR,
            branch_name             VARCHAR,
            branch_type             VARCHAR,  -- LN | XF | etc., parsed from branch_name
            from_ca                 VARCHAR,  -- parsed from branch_name
            to_ca                   VARCHAR,  -- parsed from branch_name
            contingency_description VARCHAR,
            hour_of_occurrence      TINYINT,  -- 1-24
            shadow_price            DOUBLE,
            constraint_description  VARCHAR,
            override                INTEGER,
            curve_type              VARCHAR,
            bp1                     DOUBLE,
            pc1                     DOUBLE,
            bp2                     DOUBLE,
            pc2                     DOUBLE
        )
    """)
    print("Schema created.")


def load_da_lmp(con: duckdb.DuckDBPyConnection) -> None:
    """Unpivot wide HE1-HE24 columns and insert into da_lmp."""

    he_cols = ", ".join(f'"HE{i}"' for i in range(1, 25))

    for f in DA_LMP_FILES:
        print(f"  Loading {f.name} ...", end=" ", flush=True)
        path_str = str(f).replace("'", "\\'")

        con.execute(f"""
            INSERT INTO da_lmp
            SELECT
                strptime(market_day, '%m/%d/%Y')                    AS market_day,
                node,
                node_type,
                component,
                CAST(regexp_replace(hour_str, 'HE', '') AS TINYINT) AS hour_ending,
                TRY_CAST(lmp_value AS DOUBLE)                       AS value,
                CASE
                    WHEN position('.' IN node) > 0
                        THEN split_part(node, '.', 1)
                    ELSE node
                END                                                 AS utility_ca
            FROM (
                UNPIVOT (
                    SELECT
                        MARKET_DAY  AS market_day,
                        NODE        AS node,
                        TYPE        AS node_type,
                        "VALUE"     AS component,
                        {he_cols}
                    FROM read_csv(
                        '{path_str}',
                        header      = true,
                        quote       = '"',
                        all_varchar = true
                    )
                    -- drop stray repeated header rows that appear in some exports
                    WHERE TYPE  NOT IN ('TYPE',  '')
                      AND "VALUE" NOT IN ('VALUE', '')
                )
                ON {he_cols}
                INTO NAME hour_str VALUE lmp_value
            )
            WHERE lmp_value IS NOT NULL
              AND TRY_CAST(lmp_value AS DOUBLE) IS NOT NULL
        """)

        n = con.execute(
            "SELECT COUNT(*) FROM da_lmp WHERE market_day >= (SELECT MAX(market_day) - 1 FROM da_lmp)"
        ).fetchone()[0]
        total = con.execute("SELECT COUNT(*) FROM da_lmp").fetchone()[0]
        print(f"done  (total rows: {total:,})")

    print(f"DA LMP load complete.")


def load_da_bc(con: duckdb.DuckDBPyConnection) -> None:
    """Parse DA binding constraints CSV and insert into da_binding_constraints."""
    print(f"  Loading {DA_BC_FILE.name} ...", end=" ", flush=True)

    # Row 0: title; row 1: publish date; row 2: column header; then data.
    # Last two rows: version string + legal disclaimer — filter by date parse failure.
    df = pd.read_csv(
        DA_BC_FILE,
        skiprows=2,
        dtype=str,
        keep_default_na=False,
    )

    # Drop footer rows (those with non-date Market Date values)
    df = df[pd.to_datetime(df["Market Date"], format="%m/%d/%Y", errors="coerce").notna()].copy()

    df["market_date"] = pd.to_datetime(df["Market Date"], format="%m/%d/%Y").dt.date

    df["constraint_id"] = pd.to_numeric(df["Constraint_ID"], errors="coerce").astype("Int64")

    # Parse shadow price from accounting format
    df["shadow_price"] = df["Shadow Price"].apply(parse_shadow_price)

    # Parse hour: " 01" → 1
    df["hour_of_occurrence"] = pd.to_numeric(
        df["Hour of Occurrence"].str.strip(), errors="coerce"
    ).astype("Int64")

    # Parse CA codes from branch_name: "(LN/NPPD/MPS)" → branch_type, from_ca, to_ca
    def extract_cas(branch: str):
        m = BRANCH_CA_RE.search(str(branch))
        if m:
            return m.group(1), m.group(2), m.group(3)
        return None, None, None

    cas = df["Branch Name ( Branch Type / From CA / To CA )"].apply(extract_cas)
    df["branch_type"] = [c[0] for c in cas]
    df["from_ca"]     = [c[1] for c in cas]
    df["to_ca"]       = [c[2] for c in cas]

    # Numeric BP/PC columns
    for col in ["BP1", "PC1", "BP2", "PC2"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["override"] = pd.to_numeric(df["Override"], errors="coerce").astype("Int64")

    insert_df = df[[
        "market_date", "constraint_id",
        "Constraint Name", "Branch Name ( Branch Type / From CA / To CA )",
        "branch_type", "from_ca", "to_ca",
        "Contingency Description", "hour_of_occurrence",
        "shadow_price", "Constraint Description", "override",
        "Curve Type", "BP1", "PC1", "BP2", "PC2",
    ]].rename(columns={
        "Constraint Name":                              "constraint_name",
        "Branch Name ( Branch Type / From CA / To CA )": "branch_name",
        "Contingency Description":                      "contingency_description",
        "Constraint Description":                       "constraint_description",
        "Curve Type":                                   "curve_type",
        "BP1": "bp1", "PC1": "pc1", "BP2": "bp2", "PC2": "pc2",
    })

    con.register("bc_staging", insert_df)
    con.execute("INSERT INTO da_binding_constraints SELECT * FROM bc_staging")

    total = con.execute("SELECT COUNT(*) FROM da_binding_constraints").fetchone()[0]
    print(f"done  ({total:,} rows)")


def print_summary(con: duckdb.DuckDBPyConnection) -> None:
    print("\n── da_lmp ──────────────────────────────────────")
    print(con.execute("""
        SELECT
            MIN(market_day)  AS first_day,
            MAX(market_day)  AS last_day,
            COUNT(*)         AS total_rows,
            COUNT(DISTINCT node) AS nodes,
            COUNT(DISTINCT node_type) AS node_types,
            COUNT(DISTINCT component) AS components
        FROM da_lmp
    """).df().to_string(index=False))

    print("\n── da_lmp by node_type ─────────────────────────")
    print(con.execute("""
        SELECT node_type, COUNT(*) AS rows
        FROM da_lmp
        GROUP BY node_type
        ORDER BY rows DESC
    """).df().to_string(index=False))

    print("\n── da_binding_constraints ──────────────────────")
    print(con.execute("""
        SELECT
            MIN(market_date)  AS first_date,
            MAX(market_date)  AS last_date,
            COUNT(*)          AS total_rows,
            COUNT(DISTINCT constraint_id) AS unique_constraints,
            AVG(shadow_price) AS avg_shadow_price,
            MIN(shadow_price) AS min_shadow_price,
            MAX(shadow_price) AS max_shadow_price
        FROM da_binding_constraints
    """).df().to_string(index=False))

    print("\n── top 10 CAs by avg positive MCC (load zones) ─")
    print(con.execute("""
        SELECT
            utility_ca,
            ROUND(AVG(value), 2)          AS avg_mcc,
            ROUND(AVG(CASE WHEN value > 0 THEN value END), 2) AS avg_positive_mcc,
            COUNT(CASE WHEN value > 5 THEN 1 END) * 100.0
                / COUNT(*)                AS pct_hrs_above_5,
            COUNT(DISTINCT node)          AS load_zones
        FROM da_lmp
        WHERE node_type = 'Loadzone'
          AND component = 'MCC'
        GROUP BY utility_ca
        ORDER BY avg_positive_mcc DESC NULLS LAST
        LIMIT 10
    """).df().to_string(index=False))


def main() -> None:
    if DB_PATH.exists():
        print(f"Removing existing DB at {DB_PATH}")
        DB_PATH.unlink()

    con = duckdb.connect(str(DB_PATH))
    try:
        create_schema(con)

        print("\nLoading DA LMP data:")
        load_da_lmp(con)

        print("\nLoading DA binding constraints:")
        load_da_bc(con)

        print_summary(con)

        print(f"\nDatabase written to: {DB_PATH}")
    finally:
        con.close()


if __name__ == "__main__":
    main()
