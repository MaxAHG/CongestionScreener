#!/usr/bin/env python3
"""
Phase 5 (SPP): Enrich da_binding_constraints with from_ca / to_ca.

Strategy
--------
SPP's daily BC files include a `Contingent Facility` column with the format:
    OWNER1[ OWNER2]:LINE_DESC:VOLTAGE:TERMINAL:ID

The owner code(s) before the first ':' identify the utility(s) whose
element triggers the constraint.  We use this as a proxy for the
monitored-element owner (correct in ~95 % of single-system N-1 cases;
approximate for cross-territory boundary constraints).

BASE-case constraints (~23 %) have no utility code and remain from_ca=NULL.

Run:
    python3 enrich_spp_constraints.py [--year 2025] [--workers 15]

After completion, run:
    python3 build_analytics_v2.py --rto SPP
"""

import argparse
import concurrent.futures
import io
import re
import time
from collections import Counter
from datetime import date, timedelta
from pathlib import Path

import duckdb
import pandas as pd
import requests

BASE_DIR = Path(__file__).parent
DB_PATH  = BASE_DIR / "lmp_analysis.duckdb"

BC_URL = (
    "https://portal.spp.org/file-browser-api/download/"
    "da-binding-constraints?path=/{y}/{m:02d}/By_Day/DA-BC-{y}{m:02d}{d:02d}0100.csv"
)

_SESSION = requests.Session()
_SESSION.headers["User-Agent"] = "Mozilla/5.0 (compatible; congestion-tool/1.0)"

# Regex: valid utility code = 2-7 uppercase letters (allow digits for edge cases)
_VALID_CODE = re.compile(r'^[A-Z][A-Z0-9]{1,6}$')


def _extract_cas(contingent_facility: str) -> tuple[str | None, str | None]:
    """Parse 'OWNER1[ OWNER2]:LINE:VOLTAGE:...' → (from_ca, to_ca).

    Returns (None, None) for BASE cases or unparseable entries.
    """
    cf = str(contingent_facility).strip()
    if not cf or cf.upper() == "BASE":
        return None, None

    # Part before first ':'  →  space-separated utility codes
    owner_part = cf.split(":")[0].strip()
    codes = [c for c in owner_part.split() if _VALID_CODE.match(c)]

    if not codes:
        return None, None
    if len(codes) == 1:
        return codes[0], codes[0]     # single-utility line
    return codes[0], codes[1]         # cross-territory boundary


def _download(url: str, retries: int = 3) -> bytes | None:
    for attempt in range(retries):
        try:
            r = _SESSION.get(url, timeout=30)
            if r.status_code == 200 and len(r.content) > 100:
                return r.content
            if r.status_code == 404:
                return None
        except Exception as exc:
            if attempt == retries - 1:
                print(f"  WARN: {url[-40:]} → {exc}")
    return None


def _all_dates(year: int) -> list[date]:
    d   = date(year, 1, 1)
    end = date(year + 1, 1, 1)
    out = []
    while d < end:
        out.append(d)
        d += timedelta(days=1)
    return out


# ── Build constraint → (from_ca, to_ca) lookup ───────────────────────────────

def _build_lookup(year: int, workers: int) -> dict[str, tuple[str | None, str | None]]:
    """Download all BC files and build per-constraint CA lookup.

    For each constraint_name, collect all (from_ca, to_ca) pairs seen across
    all binding events and pick the most common non-null pair.
    """
    dates = _all_dates(year)

    # constraint_name → Counter of (from_ca, to_ca) pairs
    votes: dict[str, Counter] = {}

    def fetch_day(d: date):
        url = BC_URL.format(y=d.year, m=d.month, d=d.day)
        return _download(url)

    print(f"  Downloading {len(dates)} BC files ({workers} workers) …")
    done = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(fetch_day, d): d for d in dates}
        for fut in concurrent.futures.as_completed(futs):
            raw = fut.result()
            done += 1
            if raw:
                try:
                    df = pd.read_csv(io.BytesIO(raw), dtype=str)
                    df.columns = df.columns.str.strip()
                    for _, row in df.iterrows():
                        cname = str(row.get("Constraint Name", "")).strip()
                        cf    = str(row.get("Contingent Facility", "")).strip()
                        if not cname:
                            continue
                        from_ca, to_ca = _extract_cas(cf)
                        if from_ca is not None:
                            votes.setdefault(cname, Counter())[(from_ca, to_ca)] += 1
                except Exception as exc:
                    print(f"  WARN parse: {exc}")
            if done % 50 == 0:
                print(f"    {done}/{len(dates)} files …")

    # Pick winner for each constraint_name
    lookup: dict[str, tuple[str | None, str | None]] = {}
    for cname, counter in votes.items():
        best, _ = counter.most_common(1)[0]
        lookup[cname] = best

    print(f"  Built lookup: {len(lookup):,} constraints with CA codes "
          f"({sum(1 for v in lookup.values() if v[0] is not None):,} non-null)")
    return lookup


# ── Apply to DB ───────────────────────────────────────────────────────────────

def _update_db(con: duckdb.DuckDBPyConnection, lookup: dict) -> None:
    print("  Building update DataFrame …")

    rows = [
        {"constraint_name": cn, "from_ca": fc, "to_ca": tc}
        for cn, (fc, tc) in lookup.items()
        if fc is not None
    ]
    if not rows:
        print("  No rows to update.")
        return

    upd = pd.DataFrame(rows)
    con.register("_ca_lookup", upd)

    print(f"  Updating {len(upd):,} distinct constraints in da_binding_constraints …")
    con.execute("""
        UPDATE da_binding_constraints
        SET from_ca = lu.from_ca,
            to_ca   = lu.to_ca
        FROM _ca_lookup lu
        WHERE da_binding_constraints.rto             = 'SPP'
          AND da_binding_constraints.constraint_name = lu.constraint_name
    """)
    con.unregister("_ca_lookup")

    # Report coverage
    row = con.execute("""
        SELECT
            COUNT(*) FILTER(WHERE from_ca IS NOT NULL) AS enriched,
            COUNT(*)                                   AS total
        FROM da_binding_constraints WHERE rto = 'SPP'
    """).fetchone()
    enriched, total = row
    print(f"  Coverage: {enriched:,} / {total:,} rows ({enriched/total:.1%}) have from_ca")


def _print_sample(con: duckdb.DuckDBPyConnection) -> None:
    print("\n── Top 10 SPP constraints by |SP| (with CA enrichment) ──")
    print(con.execute("""
        SELECT constraint_name, branch_type, from_ca, to_ca,
               COUNT(*) AS binding_hours,
               ROUND(SUM(ABS(shadow_price)),0) AS total_abs_sp
        FROM da_binding_constraints
        WHERE rto = 'SPP'
        GROUP BY constraint_name, branch_type, from_ca, to_ca
        ORDER BY total_abs_sp DESC
        LIMIT 10
    """).df().to_string(index=False))

    print("\n── In-territory coverage by utility (top 10) ──")
    print(con.execute("""
        WITH corr AS (
            SELECT DISTINCT constraint_name, from_ca, to_ca
            FROM da_binding_constraints
            WHERE rto = 'SPP' AND from_ca IS NOT NULL
        )
        SELECT from_ca AS utility_ca, COUNT(*) AS in_territory_constraints
        FROM (
            SELECT from_ca FROM corr WHERE from_ca IS NOT NULL
            UNION ALL
            SELECT to_ca   FROM corr WHERE to_ca   IS NOT NULL AND to_ca != from_ca
        )
        GROUP BY from_ca
        ORDER BY in_territory_constraints DESC
        LIMIT 10
    """).df().to_string(index=False))


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--year",    type=int, default=2025)
    parser.add_argument("--workers", type=int, default=15)
    args = parser.parse_args()

    if not DB_PATH.exists():
        print(f"ERROR: {DB_PATH} not found.")
        raise SystemExit(1)

    t0 = time.time()
    print(f"\n=== SPP constraint CA enrichment ({args.year}) ===\n")

    print("Step 1/2  Building constraint → CA lookup from BC files …")
    lookup = _build_lookup(args.year, args.workers)

    print("\nStep 2/2  Updating lmp_analysis.duckdb …")
    con = duckdb.connect(str(DB_PATH))
    try:
        _update_db(con, lookup)
        _print_sample(con)
    finally:
        con.close()

    elapsed = time.time() - t0
    print(f"\nDone in {elapsed/60:.1f} min.")
    print("Next: python3 build_analytics_v2.py --rto SPP")


if __name__ == "__main__":
    main()
