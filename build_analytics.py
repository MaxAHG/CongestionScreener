#!/usr/bin/env python3
"""Build pre-computed analytics tables from raw MISO data in DuckDB."""

import duckdb
from pathlib import Path

DB_PATH = Path("/Users/maxtuttman/Documents/Claude/Claude Code/miso_2025.duckdb")

MCC_CONGESTION_THRESHOLD = 5.0  # $/MWh — minimum avg CA MCC to count as "congested hour"


def drop_and_create(con: duckdb.DuckDBPyConnection, name: str, sql: str) -> int:
    con.execute(f"DROP TABLE IF EXISTS {name}")
    con.execute(sql)
    n = con.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
    print(f"  {name}: {n:,} rows")
    return n


def build_ca_lmp_monthly(con: duckdb.DuckDBPyConnection) -> None:
    """Monthly MCC/LMP stats per CA — backbone for time series views."""
    drop_and_create(con, "ca_lmp_monthly", """
        CREATE TABLE ca_lmp_monthly AS
        SELECT
            utility_ca,
            DATE_TRUNC('month', market_day)::DATE          AS month,
            component,
            COUNT(*)                                        AS n_obs,
            ROUND(AVG(value), 4)                           AS avg_value,
            ROUND(AVG(CASE WHEN value > 0 THEN value END), 4) AS avg_positive,
            ROUND(AVG(CASE WHEN value < 0 THEN value END), 4) AS avg_negative,
            ROUND(SUM(CASE WHEN value > 0 THEN value ELSE 0 END), 2)
                                                            AS sum_positive,
            ROUND(SUM(CASE WHEN value < 0 THEN value ELSE 0 END), 2)
                                                            AS sum_negative,
            ROUND(
                COUNT(*) FILTER (WHERE value >  5) * 100.0 / COUNT(*), 2
            )                                               AS pct_above_5,
            ROUND(
                COUNT(*) FILTER (WHERE value > 10) * 100.0 / COUNT(*), 2
            )                                               AS pct_above_10,
            ROUND(MAX(value), 2)                            AS max_value,
            ROUND(MIN(value), 2)                            AS min_value,
            ROUND(PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY value), 2)
                                                            AS p90_value,
            ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY value), 2)
                                                            AS median_value
        FROM da_lmp
        WHERE node_type = 'Loadzone'
        GROUP BY utility_ca, month, component
        ORDER BY utility_ca, month, component
    """)


def build_ca_lmp_by_hour(con: duckdb.DuckDBPyConnection) -> None:
    """Avg MCC by CA × hour-of-day × calendar month — backbone for heatmaps."""
    drop_and_create(con, "ca_lmp_by_hour", """
        CREATE TABLE ca_lmp_by_hour AS
        SELECT
            utility_ca,
            MONTH(market_day)                               AS cal_month,
            hour_ending,
            component,
            COUNT(*)                                        AS n_obs,
            ROUND(AVG(value), 4)                           AS avg_value,
            ROUND(AVG(CASE WHEN value > 0 THEN value END), 4) AS avg_positive,
            ROUND(
                COUNT(*) FILTER (WHERE value > 5) * 100.0 / COUNT(*), 2
            )                                               AS pct_above_5,
            ROUND(MAX(value), 2)                            AS max_value
        FROM da_lmp
        WHERE node_type = 'Loadzone'
        GROUP BY utility_ca, cal_month, hour_ending, component
        ORDER BY utility_ca, cal_month, hour_ending, component
    """)


def build_constraint_summary(con: duckdb.DuckDBPyConnection) -> None:
    """Per-constraint annual stats joined to CA names."""
    drop_and_create(con, "constraint_summary", """
        CREATE TABLE constraint_summary AS
        SELECT
            bc.constraint_id,
            ANY_VALUE(bc.constraint_name)                   AS constraint_name,
            ANY_VALUE(bc.branch_name)                       AS branch_name,
            ANY_VALUE(bc.branch_type)                       AS branch_type,
            ANY_VALUE(bc.from_ca)                           AS from_ca,
            ANY_VALUE(r_from.utility_name)                  AS from_ca_name,
            ANY_VALUE(bc.to_ca)                             AS to_ca,
            ANY_VALUE(r_to.utility_name)                    AS to_ca_name,
            COUNT(*)                                        AS binding_hours,
            COUNT(DISTINCT bc.market_date)                  AS days_binding,
            COUNT(DISTINCT MONTH(bc.market_date))           AS months_binding,
            ROUND(AVG(bc.shadow_price), 2)                  AS avg_sp,
            ROUND(MIN(bc.shadow_price), 2)                  AS min_sp,
            ROUND(MAX(bc.shadow_price), 2)                  AS max_sp,
            ROUND(SUM(ABS(bc.shadow_price)), 0)             AS total_abs_sp,
            ROUND(AVG(ABS(bc.shadow_price)), 2)             AS avg_abs_sp,
            ROUND(PERCENTILE_CONT(0.9) WITHIN GROUP
                (ORDER BY ABS(bc.shadow_price)), 2)         AS p90_abs_sp,
            MODE() WITHIN GROUP (ORDER BY bc.hour_of_occurrence)
                                                            AS most_common_hour
        FROM da_binding_constraints bc
        LEFT JOIN ca_reference r_from ON bc.from_ca = r_from.utility_ca
        LEFT JOIN ca_reference r_to   ON bc.to_ca   = r_to.utility_ca
        GROUP BY bc.constraint_id
        ORDER BY total_abs_sp DESC
    """)


def build_mcc_constraint_correlation(con: duckdb.DuckDBPyConnection) -> None:
    """Linking layer: for each (CA, constraint) pair, how often did that constraint
    bind during hours when the CA's load zones had avg MCC > threshold?

    This answers: "when utility X was congested, which lines were causing it?"
    """
    drop_and_create(con, "mcc_constraint_correlation", f"""
        CREATE TABLE mcc_constraint_correlation AS
        WITH ca_hourly_mcc AS (
            -- Collapse all load zones for a CA into a single avg MCC per hour
            SELECT
                utility_ca,
                market_day,
                hour_ending,
                AVG(value)  AS avg_mcc,
                COUNT(*)    AS n_zones
            FROM da_lmp
            WHERE node_type = 'Loadzone'
              AND component  = 'MCC'
            GROUP BY utility_ca, market_day, hour_ending
        ),
        high_mcc_hours AS (
            SELECT *
            FROM ca_hourly_mcc
            WHERE avg_mcc > {MCC_CONGESTION_THRESHOLD}
        )
        SELECT
            h.utility_ca,
            r_ca.utility_name                               AS ca_name,
            bc.constraint_id,
            ANY_VALUE(bc.constraint_name)                   AS constraint_name,
            ANY_VALUE(bc.branch_type)                       AS branch_type,
            ANY_VALUE(bc.from_ca)                           AS from_ca,
            ANY_VALUE(r_from.utility_name)                  AS from_ca_name,
            ANY_VALUE(bc.to_ca)                             AS to_ca,
            ANY_VALUE(r_to.utility_name)                    AS to_ca_name,
            COUNT(*)                                        AS co_occurrence_hours,
            ROUND(AVG(h.avg_mcc), 2)                        AS avg_ca_mcc_when_binding,
            ROUND(AVG(bc.shadow_price), 2)                  AS avg_sp,
            ROUND(MIN(bc.shadow_price), 2)                  AS min_sp,
            ROUND(MAX(bc.shadow_price), 2)                  AS max_sp,
            ROUND(SUM(ABS(bc.shadow_price)), 0)             AS total_abs_sp,
            ROUND(AVG(ABS(bc.shadow_price)), 2)             AS avg_abs_sp
        FROM high_mcc_hours h
        JOIN da_binding_constraints bc
            ON  h.market_day   = bc.market_date
            AND h.hour_ending  = bc.hour_of_occurrence
        LEFT JOIN ca_reference r_ca  ON h.utility_ca  = r_ca.utility_ca
        LEFT JOIN ca_reference r_from ON bc.from_ca   = r_from.utility_ca
        LEFT JOIN ca_reference r_to   ON bc.to_ca     = r_to.utility_ca
        GROUP BY h.utility_ca, r_ca.utility_name, bc.constraint_id
        ORDER BY h.utility_ca, co_occurrence_hours DESC
    """)


def build_screener(con: duckdb.DuckDBPyConnection) -> None:
    """One row per CA — all headline screening metrics in one place."""
    drop_and_create(con, "screener", f"""
        CREATE TABLE screener AS
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
                )                                           AS pct_hours_above_5,
                ROUND(
                    COUNT(*) FILTER (WHERE component='MCC' AND value > 10) * 100.0
                    / NULLIF(COUNT(*) FILTER (WHERE component='MCC'), 0), 1
                )                                           AS pct_hours_above_10,
                ROUND(MAX(CASE WHEN component='MCC' THEN value END), 2)
                                                            AS max_mcc,
                COUNT(DISTINCT node)                        AS n_load_zones
            FROM da_lmp
            WHERE node_type = 'Loadzone'
            GROUP BY utility_ca
        ),
        bc_owner_stats AS (
            -- Constraints where this CA owns the branch (from_ca)
            SELECT
                from_ca                                     AS utility_ca,
                COUNT(*)                                    AS owned_binding_events,
                COUNT(DISTINCT constraint_id)               AS owned_unique_constraints,
                ROUND(SUM(ABS(shadow_price)), 0)            AS owned_total_abs_sp,
                ROUND(AVG(ABS(shadow_price)), 2)            AS owned_avg_abs_sp
            FROM da_binding_constraints
            WHERE from_ca IS NOT NULL AND from_ca != '*'
            GROUP BY from_ca
        ),
        bc_corr_stats AS (
            -- Constraints that co-occur with this CA's high-MCC hours
            SELECT
                utility_ca,
                COUNT(DISTINCT constraint_id)               AS corr_unique_constraints,
                SUM(co_occurrence_hours)                    AS corr_total_hours,
                ROUND(SUM(total_abs_sp), 0)                 AS corr_total_abs_sp,
                MAX(co_occurrence_hours)                    AS corr_max_single_constraint_hrs,
                -- In-territory: constraint branch endpoint is within this CA
                COUNT(DISTINCT CASE
                    WHEN from_ca = utility_ca OR to_ca = utility_ca
                    THEN constraint_id END)                 AS corr_in_territory_constraints,
                COALESCE(SUM(CASE
                    WHEN from_ca = utility_ca OR to_ca = utility_ca
                    THEN co_occurrence_hours ELSE 0 END), 0)
                                                            AS corr_in_territory_hours,
                -- In-territory lines only (branch_type = 'LN')
                COUNT(DISTINCT CASE
                    WHEN (from_ca = utility_ca OR to_ca = utility_ca)
                     AND branch_type = 'LN'
                    THEN constraint_id END)                 AS corr_in_territory_ln_constraints,
                COALESCE(SUM(CASE
                    WHEN (from_ca = utility_ca OR to_ca = utility_ca)
                     AND branch_type = 'LN'
                    THEN co_occurrence_hours ELSE 0 END), 0)
                                                            AS corr_in_territory_ln_hours,
                -- name of the most co-occurring constraint (rank within CA, pick #1)
                ANY_VALUE(constraint_name) FILTER (
                    WHERE rn = 1
                )                                           AS top_corr_constraint
            FROM (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY utility_ca
                        ORDER BY co_occurrence_hours DESC
                    ) AS rn
                FROM mcc_constraint_correlation
            )
            GROUP BY utility_ca
        )
        SELECT
            r.lrz,
            r.utility_ca,
            r.utility_name,
            -- LMP congestion metrics
            l.n_load_zones,
            l.avg_mcc,
            l.avg_positive_mcc,
            l.sum_positive_mcc,
            l.pct_hours_above_5,
            l.pct_hours_above_10,
            l.max_mcc,
            -- Constraint ownership metrics
            COALESCE(o.owned_binding_events,     0)         AS owned_binding_events,
            COALESCE(o.owned_unique_constraints, 0)         AS owned_unique_constraints,
            COALESCE(o.owned_total_abs_sp,       0)         AS owned_total_abs_sp,
            o.owned_avg_abs_sp,
            -- Constraint correlation metrics (constraints active during CA's high-MCC hours)
            COALESCE(c.corr_unique_constraints,      0)      AS corr_unique_constraints,
            COALESCE(c.corr_total_hours,             0)      AS corr_total_hours,
            COALESCE(c.corr_total_abs_sp,            0)      AS corr_total_abs_sp,
            c.corr_max_single_constraint_hrs,
            COALESCE(c.corr_in_territory_constraints,    0)   AS corr_in_territory_constraints,
            COALESCE(c.corr_in_territory_hours,         0)   AS corr_in_territory_hours,
            COALESCE(c.corr_in_territory_ln_constraints, 0)  AS corr_in_territory_ln_constraints,
            COALESCE(c.corr_in_territory_ln_hours,       0)  AS corr_in_territory_ln_hours,
            c.top_corr_constraint
        FROM ca_reference r
        LEFT JOIN lmp_stats       l USING (utility_ca)
        LEFT JOIN bc_owner_stats  o USING (utility_ca)
        LEFT JOIN bc_corr_stats   c USING (utility_ca)
        ORDER BY avg_mcc DESC NULLS LAST
    """)


def print_summary(con: duckdb.DuckDBPyConnection) -> None:
    print("\n── Screener (top 15 by avg MCC) ────────────────")
    print(con.execute("""
        SELECT
            lrz, utility_ca, utility_name,
            avg_mcc, avg_positive_mcc, pct_hours_above_5,
            owned_total_abs_sp, corr_unique_constraints, corr_total_hours
        FROM screener
        WHERE avg_mcc IS NOT NULL
        ORDER BY avg_mcc DESC
        LIMIT 15
    """).df().to_string(index=False))

    print("\n── Top 10 constraints by total |SP| ─────────────")
    print(con.execute("""
        SELECT constraint_id, constraint_name, branch_type,
               from_ca, to_ca, binding_hours, days_binding, total_abs_sp, avg_abs_sp
        FROM constraint_summary
        LIMIT 10
    """).df().to_string(index=False))

    print("\n── Top constraints correlated with MP congestion ─")
    print(con.execute("""
        SELECT constraint_id, constraint_name, branch_type,
               from_ca, to_ca, co_occurrence_hours,
               avg_ca_mcc_when_binding, avg_abs_sp, total_abs_sp
        FROM mcc_constraint_correlation
        WHERE utility_ca = 'MP'
        ORDER BY co_occurrence_hours DESC
        LIMIT 10
    """).df().to_string(index=False))


def main() -> None:
    con = duckdb.connect(str(DB_PATH))
    try:
        print("Building analytics tables...\n")

        print("1/5  ca_lmp_monthly")
        build_ca_lmp_monthly(con)

        print("2/5  ca_lmp_by_hour")
        build_ca_lmp_by_hour(con)

        print("3/5  constraint_summary")
        build_constraint_summary(con)

        print("4/5  mcc_constraint_correlation  (large join — may take 30–60s)")
        build_mcc_constraint_correlation(con)

        print("5/5  screener")
        build_screener(con)

        print("\nDone. Summary:")
        print_summary(con)

    finally:
        con.close()


if __name__ == "__main__":
    main()
