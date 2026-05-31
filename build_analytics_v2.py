#!/usr/bin/env python3
"""
Build pre-computed analytics tables in lmp_analysis.duckdb for a given RTO.

Usage:
    python3 build_analytics_v2.py [--rto MISO]

Each function is idempotent: it deletes existing rows for the given RTO then
inserts fresh rows, so adding a second RTO never clobbers the first.
"""

import argparse
import duckdb
from pathlib import Path

BASE_DIR = Path(__file__).parent
DB_PATH = BASE_DIR / "lmp_analysis.duckdb"

MCC_CONGESTION_THRESHOLD = 5.0  # $/MWh


# ── DDL: create tables if they don't exist yet ───────────────────────────────

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS ca_lmp_monthly (
    rto           VARCHAR,
    utility_ca    VARCHAR,
    month         DATE,
    component     VARCHAR,
    n_obs         BIGINT,
    avg_value     DOUBLE,
    avg_positive  DOUBLE,
    avg_negative  DOUBLE,
    sum_positive  DOUBLE,
    sum_negative  DOUBLE,
    pct_above_5   DOUBLE,
    pct_above_10  DOUBLE,
    max_value     DOUBLE,
    min_value     DOUBLE,
    p90_value     DOUBLE,
    median_value  DOUBLE
);

CREATE TABLE IF NOT EXISTS ca_lmp_by_hour (
    rto          VARCHAR,
    utility_ca   VARCHAR,
    cal_month    INTEGER,
    hour_ending  INTEGER,
    component    VARCHAR,
    n_obs        BIGINT,
    avg_value    DOUBLE,
    avg_positive DOUBLE,
    pct_above_5  DOUBLE,
    max_value    DOUBLE
);

CREATE TABLE IF NOT EXISTS constraint_summary (
    rto                  VARCHAR,
    constraint_id        INTEGER,
    constraint_name      VARCHAR,
    branch_name          VARCHAR,
    branch_type          VARCHAR,
    from_ca              VARCHAR,
    from_ca_name         VARCHAR,
    to_ca                VARCHAR,
    to_ca_name           VARCHAR,
    binding_hours        BIGINT,
    days_binding         BIGINT,
    months_binding       BIGINT,
    avg_sp               DOUBLE,
    min_sp               DOUBLE,
    max_sp               DOUBLE,
    total_abs_sp         DOUBLE,
    avg_abs_sp           DOUBLE,
    p90_abs_sp           DOUBLE,
    most_common_hour     INTEGER
);

CREATE TABLE IF NOT EXISTS mcc_constraint_correlation (
    rto                      VARCHAR,
    utility_ca               VARCHAR,
    ca_name                  VARCHAR,
    constraint_id            INTEGER,
    constraint_name          VARCHAR,
    branch_type              VARCHAR,
    from_ca                  VARCHAR,
    from_ca_name             VARCHAR,
    to_ca                    VARCHAR,
    to_ca_name               VARCHAR,
    co_occurrence_hours      BIGINT,
    avg_ca_mcc_when_binding  DOUBLE,
    avg_sp                   DOUBLE,
    min_sp                   DOUBLE,
    max_sp                   DOUBLE,
    total_abs_sp             DOUBLE,
    avg_abs_sp               DOUBLE
);

CREATE TABLE IF NOT EXISTS screener (
    rto                              VARCHAR,
    zone_label                       VARCHAR,
    zone_color                       VARCHAR,
    lrz                              TINYINT,
    utility_ca                       VARCHAR,
    utility_name                     VARCHAR,
    n_load_zones                     BIGINT,
    avg_mcc                          DOUBLE,
    avg_positive_mcc                 DOUBLE,
    sum_positive_mcc                 DOUBLE,
    pct_hours_above_5                DOUBLE,
    pct_hours_above_10               DOUBLE,
    max_mcc                          DOUBLE,
    owned_binding_events             BIGINT,
    owned_unique_constraints         BIGINT,
    owned_total_abs_sp               DOUBLE,
    owned_avg_abs_sp                 DOUBLE,
    corr_unique_constraints          BIGINT,
    corr_total_hours                 BIGINT,
    corr_total_abs_sp                DOUBLE,
    corr_max_single_constraint_hrs   BIGINT,
    corr_in_territory_constraints    BIGINT,
    corr_in_territory_hours          BIGINT,
    corr_in_territory_ln_constraints BIGINT,
    corr_in_territory_ln_hours       BIGINT,
    top_corr_constraint              VARCHAR
);
"""


def ensure_schema(con: duckdb.DuckDBPyConnection) -> None:
    for stmt in _SCHEMA_SQL.strip().split(";"):
        stmt = stmt.strip()
        if stmt:
            con.execute(stmt)


def _upsert(con: duckdb.DuckDBPyConnection, name: str, rto: str, sql: str) -> int:
    """Delete existing RTO rows, insert new ones, return row count."""
    con.execute(f"DELETE FROM {name} WHERE rto = ?", [rto])
    con.execute(sql)
    n = con.execute(f"SELECT COUNT(*) FROM {name} WHERE rto = ?", [rto]).fetchone()[0]
    print(f"  {name} ({rto}): {n:,} rows")
    return n


# ── Analytics builders ────────────────────────────────────────────────────────

def build_ca_lmp_monthly(con: duckdb.DuckDBPyConnection, rto: str) -> None:
    _upsert(con, "ca_lmp_monthly", rto, f"""
        INSERT INTO ca_lmp_monthly
        SELECT
            '{rto}'                                             AS rto,
            utility_ca,
            DATE_TRUNC('month', market_day)::DATE               AS month,
            component,
            COUNT(*)                                            AS n_obs,
            ROUND(AVG(value), 4)                               AS avg_value,
            ROUND(AVG(CASE WHEN value > 0 THEN value END), 4)  AS avg_positive,
            ROUND(AVG(CASE WHEN value < 0 THEN value END), 4)  AS avg_negative,
            ROUND(SUM(CASE WHEN value > 0 THEN value ELSE 0 END), 2) AS sum_positive,
            ROUND(SUM(CASE WHEN value < 0 THEN value ELSE 0 END), 2) AS sum_negative,
            ROUND(
                COUNT(*) FILTER (WHERE value >  5) * 100.0 / COUNT(*), 2
            )                                                   AS pct_above_5,
            ROUND(
                COUNT(*) FILTER (WHERE value > 10) * 100.0 / COUNT(*), 2
            )                                                   AS pct_above_10,
            ROUND(MAX(value), 2)                                AS max_value,
            ROUND(MIN(value), 2)                                AS min_value,
            ROUND(PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY value), 2) AS p90_value,
            ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY value), 2) AS median_value
        FROM da_lmp
        WHERE rto = '{rto}' AND node_type = 'Loadzone'
        GROUP BY utility_ca, month, component
        ORDER BY utility_ca, month, component
    """)


def build_ca_lmp_by_hour(con: duckdb.DuckDBPyConnection, rto: str) -> None:
    _upsert(con, "ca_lmp_by_hour", rto, f"""
        INSERT INTO ca_lmp_by_hour
        SELECT
            '{rto}'                                             AS rto,
            utility_ca,
            MONTH(market_day)                                   AS cal_month,
            hour_ending,
            component,
            COUNT(*)                                            AS n_obs,
            ROUND(AVG(value), 4)                               AS avg_value,
            ROUND(AVG(CASE WHEN value > 0 THEN value END), 4)  AS avg_positive,
            ROUND(
                COUNT(*) FILTER (WHERE value > 5) * 100.0 / COUNT(*), 2
            )                                                   AS pct_above_5,
            ROUND(MAX(value), 2)                                AS max_value
        FROM da_lmp
        WHERE rto = '{rto}' AND node_type = 'Loadzone'
        GROUP BY utility_ca, cal_month, hour_ending, component
        ORDER BY utility_ca, cal_month, hour_ending, component
    """)


def build_constraint_summary(con: duckdb.DuckDBPyConnection, rto: str) -> None:
    _upsert(con, "constraint_summary", rto, f"""
        INSERT INTO constraint_summary
        SELECT
            '{rto}'                                             AS rto,
            bc.constraint_id,
            ANY_VALUE(bc.constraint_name)                       AS constraint_name,
            ANY_VALUE(bc.branch_name)                           AS branch_name,
            ANY_VALUE(bc.branch_type)                           AS branch_type,
            ANY_VALUE(bc.from_ca)                               AS from_ca,
            ANY_VALUE(r_from.utility_name)                      AS from_ca_name,
            ANY_VALUE(bc.to_ca)                                 AS to_ca,
            ANY_VALUE(r_to.utility_name)                        AS to_ca_name,
            COUNT(*)                                            AS binding_hours,
            COUNT(DISTINCT bc.market_date)                      AS days_binding,
            COUNT(DISTINCT MONTH(bc.market_date))               AS months_binding,
            ROUND(AVG(bc.shadow_price), 2)                      AS avg_sp,
            ROUND(MIN(bc.shadow_price), 2)                      AS min_sp,
            ROUND(MAX(bc.shadow_price), 2)                      AS max_sp,
            ROUND(SUM(ABS(bc.shadow_price)), 0)                 AS total_abs_sp,
            ROUND(AVG(ABS(bc.shadow_price)), 2)                 AS avg_abs_sp,
            ROUND(PERCENTILE_CONT(0.9) WITHIN GROUP
                (ORDER BY ABS(bc.shadow_price)), 2)             AS p90_abs_sp,
            MODE() WITHIN GROUP (ORDER BY bc.hour_of_occurrence) AS most_common_hour
        FROM da_binding_constraints bc
        LEFT JOIN ca_reference r_from
            ON bc.from_ca = r_from.utility_ca AND r_from.rto = '{rto}'
        LEFT JOIN ca_reference r_to
            ON bc.to_ca = r_to.utility_ca AND r_to.rto = '{rto}'
        WHERE bc.rto = '{rto}'
        GROUP BY bc.constraint_id
        ORDER BY total_abs_sp DESC
    """)


def build_mcc_constraint_correlation(con: duckdb.DuckDBPyConnection, rto: str) -> None:
    _upsert(con, "mcc_constraint_correlation", rto, f"""
        INSERT INTO mcc_constraint_correlation
        WITH ca_hourly_mcc AS (
            SELECT
                utility_ca,
                market_day,
                hour_ending,
                AVG(value) AS avg_mcc,
                COUNT(*)   AS n_zones
            FROM da_lmp
            WHERE rto = '{rto}' AND node_type = 'Loadzone' AND component = 'MCC'
            GROUP BY utility_ca, market_day, hour_ending
        ),
        high_mcc_hours AS (
            SELECT * FROM ca_hourly_mcc
            WHERE avg_mcc > {MCC_CONGESTION_THRESHOLD}
        )
        SELECT
            '{rto}'                                             AS rto,
            h.utility_ca,
            r_ca.utility_name                                   AS ca_name,
            bc.constraint_id,
            ANY_VALUE(bc.constraint_name)                       AS constraint_name,
            ANY_VALUE(bc.branch_type)                           AS branch_type,
            ANY_VALUE(bc.from_ca)                               AS from_ca,
            ANY_VALUE(r_from.utility_name)                      AS from_ca_name,
            ANY_VALUE(bc.to_ca)                                 AS to_ca,
            ANY_VALUE(r_to.utility_name)                        AS to_ca_name,
            COUNT(*)                                            AS co_occurrence_hours,
            ROUND(AVG(h.avg_mcc), 2)                            AS avg_ca_mcc_when_binding,
            ROUND(AVG(bc.shadow_price), 2)                      AS avg_sp,
            ROUND(MIN(bc.shadow_price), 2)                      AS min_sp,
            ROUND(MAX(bc.shadow_price), 2)                      AS max_sp,
            ROUND(SUM(ABS(bc.shadow_price)), 0)                 AS total_abs_sp,
            ROUND(AVG(ABS(bc.shadow_price)), 2)                 AS avg_abs_sp
        FROM high_mcc_hours h
        JOIN da_binding_constraints bc
            ON  h.market_day  = bc.market_date
            AND h.hour_ending = bc.hour_of_occurrence
            AND bc.rto = '{rto}'
        LEFT JOIN ca_reference r_ca
            ON h.utility_ca = r_ca.utility_ca AND r_ca.rto = '{rto}'
        LEFT JOIN ca_reference r_from
            ON bc.from_ca = r_from.utility_ca AND r_from.rto = '{rto}'
        LEFT JOIN ca_reference r_to
            ON bc.to_ca = r_to.utility_ca AND r_to.rto = '{rto}'
        GROUP BY h.utility_ca, r_ca.utility_name, bc.constraint_id
        ORDER BY h.utility_ca, co_occurrence_hours DESC
    """)


def build_screener(con: duckdb.DuckDBPyConnection, rto: str) -> None:
    _upsert(con, "screener", rto, f"""
        INSERT INTO screener
        WITH lmp_stats AS (
            SELECT
                utility_ca,
                ROUND(AVG(CASE WHEN component='MCC' THEN value END), 2)
                                                                AS avg_mcc,
                ROUND(AVG(CASE WHEN component='MCC' AND value > 0 THEN value END), 2)
                                                                AS avg_positive_mcc,
                ROUND(SUM(CASE WHEN component='MCC' AND value > 0 THEN value ELSE 0 END), 0)
                                                                AS sum_positive_mcc,
                ROUND(
                    COUNT(*) FILTER (WHERE component='MCC' AND value > 5) * 100.0
                    / NULLIF(COUNT(*) FILTER (WHERE component='MCC'), 0), 1
                )                                               AS pct_hours_above_5,
                ROUND(
                    COUNT(*) FILTER (WHERE component='MCC' AND value > 10) * 100.0
                    / NULLIF(COUNT(*) FILTER (WHERE component='MCC'), 0), 1
                )                                               AS pct_hours_above_10,
                ROUND(MAX(CASE WHEN component='MCC' THEN value END), 2)
                                                                AS max_mcc,
                COUNT(DISTINCT node)                            AS n_load_zones
            FROM da_lmp
            WHERE rto = '{rto}' AND node_type = 'Loadzone'
            GROUP BY utility_ca
        ),
        bc_owner_stats AS (
            SELECT
                from_ca                                         AS utility_ca,
                COUNT(*)                                        AS owned_binding_events,
                COUNT(DISTINCT constraint_id)                   AS owned_unique_constraints,
                ROUND(SUM(ABS(shadow_price)), 0)                AS owned_total_abs_sp,
                ROUND(AVG(ABS(shadow_price)), 2)                AS owned_avg_abs_sp
            FROM da_binding_constraints
            WHERE rto = '{rto}' AND from_ca IS NOT NULL AND from_ca != '*'
            GROUP BY from_ca
        ),
        bc_corr_stats AS (
            SELECT
                utility_ca,
                COUNT(DISTINCT constraint_id)                   AS corr_unique_constraints,
                SUM(co_occurrence_hours)                        AS corr_total_hours,
                ROUND(SUM(total_abs_sp), 0)                     AS corr_total_abs_sp,
                MAX(co_occurrence_hours)                        AS corr_max_single_constraint_hrs,
                COUNT(DISTINCT CASE
                    WHEN from_ca = utility_ca OR to_ca = utility_ca
                    THEN constraint_id END)                     AS corr_in_territory_constraints,
                COALESCE(SUM(CASE
                    WHEN from_ca = utility_ca OR to_ca = utility_ca
                    THEN co_occurrence_hours ELSE 0 END), 0)
                                                                AS corr_in_territory_hours,
                COUNT(DISTINCT CASE
                    WHEN (from_ca = utility_ca OR to_ca = utility_ca)
                     AND branch_type = 'LN'
                    THEN constraint_id END)                     AS corr_in_territory_ln_constraints,
                COALESCE(SUM(CASE
                    WHEN (from_ca = utility_ca OR to_ca = utility_ca)
                     AND branch_type = 'LN'
                    THEN co_occurrence_hours ELSE 0 END), 0)
                                                                AS corr_in_territory_ln_hours,
                ANY_VALUE(constraint_name) FILTER (WHERE rn = 1) AS top_corr_constraint
            FROM (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY utility_ca
                        ORDER BY co_occurrence_hours DESC
                    ) AS rn
                FROM mcc_constraint_correlation
                WHERE rto = '{rto}'
            )
            GROUP BY utility_ca
        )
        SELECT
            '{rto}'                                             AS rto,
            r.zone_label,
            r.zone_color,
            r.lrz,
            r.utility_ca,
            r.utility_name,
            l.n_load_zones,
            l.avg_mcc,
            l.avg_positive_mcc,
            l.sum_positive_mcc,
            l.pct_hours_above_5,
            l.pct_hours_above_10,
            l.max_mcc,
            COALESCE(o.owned_binding_events,      0)            AS owned_binding_events,
            COALESCE(o.owned_unique_constraints,  0)            AS owned_unique_constraints,
            COALESCE(o.owned_total_abs_sp,        0)            AS owned_total_abs_sp,
            o.owned_avg_abs_sp,
            COALESCE(c.corr_unique_constraints,   0)            AS corr_unique_constraints,
            COALESCE(c.corr_total_hours,          0)            AS corr_total_hours,
            COALESCE(c.corr_total_abs_sp,         0)            AS corr_total_abs_sp,
            c.corr_max_single_constraint_hrs,
            COALESCE(c.corr_in_territory_constraints,     0)    AS corr_in_territory_constraints,
            COALESCE(c.corr_in_territory_hours,          0)    AS corr_in_territory_hours,
            COALESCE(c.corr_in_territory_ln_constraints, 0)    AS corr_in_territory_ln_constraints,
            COALESCE(c.corr_in_territory_ln_hours,       0)    AS corr_in_territory_ln_hours,
            c.top_corr_constraint
        FROM ca_reference r
        LEFT JOIN lmp_stats       l USING (utility_ca)
        LEFT JOIN bc_owner_stats  o USING (utility_ca)
        LEFT JOIN bc_corr_stats   c USING (utility_ca)
        WHERE r.rto = '{rto}'
        ORDER BY avg_mcc DESC NULLS LAST
    """)


def print_summary(con: duckdb.DuckDBPyConnection, rto: str) -> None:
    print(f"\n── Screener ({rto}, top 15 by avg MCC) ────────────────")
    print(con.execute(f"""
        SELECT zone_label, utility_ca, utility_name,
               avg_mcc, avg_positive_mcc, pct_hours_above_5,
               owned_total_abs_sp, corr_unique_constraints, corr_total_hours
        FROM screener
        WHERE rto = '{rto}' AND avg_mcc IS NOT NULL
        ORDER BY avg_mcc DESC
        LIMIT 15
    """).df().to_string(index=False))

    print(f"\n── Top 10 constraints ({rto}) by total |SP| ─────────────")
    print(con.execute(f"""
        SELECT constraint_id, constraint_name, branch_type,
               from_ca, to_ca, binding_hours, days_binding, total_abs_sp, avg_abs_sp
        FROM constraint_summary
        WHERE rto = '{rto}'
        LIMIT 10
    """).df().to_string(index=False))


def main() -> None:
    parser = argparse.ArgumentParser(description="Build analytics tables in lmp_analysis.duckdb")
    parser.add_argument("--rto", default="MISO", help="RTO to process (default: MISO)")
    args = parser.parse_args()
    rto = args.rto.upper()

    if not DB_PATH.exists():
        print(f"ERROR: {DB_PATH} not found. Run migrate_to_multi_rto.py first.")
        raise SystemExit(1)

    con = duckdb.connect(str(DB_PATH))
    try:
        print(f"Building analytics tables for {rto} ...\n")

        print("0/5  Ensuring table schemas exist")
        ensure_schema(con)

        print(f"1/5  ca_lmp_monthly")
        build_ca_lmp_monthly(con, rto)

        print(f"2/5  ca_lmp_by_hour")
        build_ca_lmp_by_hour(con, rto)

        print(f"3/5  constraint_summary")
        build_constraint_summary(con, rto)

        print(f"4/5  mcc_constraint_correlation  (large join — may take 30–60s)")
        build_mcc_constraint_correlation(con, rto)

        print(f"5/5  screener")
        build_screener(con, rto)

        print("\nDone. Summary:")
        print_summary(con, rto)

    finally:
        con.close()


if __name__ == "__main__":
    main()
