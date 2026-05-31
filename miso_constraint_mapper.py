"""
MISO Binding Constraint → Physical Location Mapper
====================================================
Maps MISO day-ahead binding constraints to physical transmission lines
by matching contingency description substation names against the HIFLD
Electric Power Transmission Lines dataset (SUB_1 / SUB_2 fields).

Both endpoints are matched simultaneously against a single HIFLD record,
which eliminates the cross-state false positives produced by matching each
substation independently. Matched features carry real routed line geometry.

SETUP (run once in terminal):
    pip install pandas rapidfuzz requests folium openpyxl

INPUT:
    2025_da_bc_HIST - 2025_da_bc_HIST.csv  — Google Sheet export

OUTPUT:
    hifld_lines.json             — cached HIFLD line features (auto-fetched)
    miso_constraints.geojson     — matched constraint lines (real geometry)
    miso_constraints_map.html    — interactive Folium map
"""

import re, json
import pandas as pd
import requests
from rapidfuzz import fuzz, process
from pathlib import Path
from shapely.geometry import Point, Polygon
from shapely.strtree import STRtree
from shapely.validation import make_valid

# ── STEP 1a: FETCH UTILITY TERRITORIES ───────────────────────────────────────

def fetch_utility_territories(output_path="hifld_territories.json"):
    """
    Download HIFLD Electric Retail Service Territories for MISO control area.
    Saves to JSON. Run once.
    """
    url = ("https://services3.arcgis.com/OYP7N6mAJJCyH6hd/arcgis/rest/services/"
           "Electric_Retail_Service_Territories_HIFLD/FeatureServer/0/query")
    all_features = []
    offset = 0
    batch = 2000

    print("Fetching MISO utility territories...")
    while True:
        params = {
            "where": "CNTRL_AREA LIKE '%MISO%'",
            "outFields": "NAME,STATE,TYPE,CNTRL_AREA,HOLDING_CO",
            "returnGeometry": "true",
            "outSR": "4326",
            "f": "json",
            "resultRecordCount": batch,
            "resultOffset": offset,
        }
        r = requests.get(url, params=params, timeout=60)
        data = r.json()
        if "error" in data:
            raise RuntimeError(f"Territories API error: {data['error']}")
        features = data.get("features", [])
        if not features:
            break
        all_features.extend(features)
        print(f"  {len(all_features)} territories fetched...")
        if not data.get("exceededTransferLimit"):
            break
        offset += batch

    with open(output_path, "w") as f:
        json.dump(all_features, f)
    print(f"Saved {len(all_features)} territories to {output_path}")
    return all_features


# ── STEP 1b: TERRITORY SPATIAL INDEX ────────────────────────────────────────

# Mapping from MISO owner/control-area codes → territory NAME(s) in the HIFLD
# retail service territories dataset.  Only codes whose names appear verbatim
# (or near-verbatim) in the dataset are listed; everything else falls back to
# voltage + fuzzy name matching.
OWNER_TERRITORY_NAMES: dict[str, list[str]] = {
    'ALTW':  ['INTERSTATE POWER AND LIGHT CO'],
    'ALTE':  ['INTERSTATE POWER AND LIGHT CO', 'WISCONSIN POWER & LIGHT CO'],
    'NSP':   ['NORTHERN STATES POWER CO', 'NORTHERN STATES POWER CO - MINNESOTA'],
    'NSPW':  ['NORTHERN STATES POWER CO'],
    'MP':    ['ALLETE, INC.'],
    'OTP':   ['OTTER TAIL POWER CO'],
    'MDU':   ['MONTANA-DAKOTA UTILITIES CO'],
    'MEC':   ['MIDAMERICAN ENERGY CO'],
    'DPC':   ['DAIRYLAND POWER COOPERATIVE'],
    # Entergy operating companies (MISO South)
    'EES':   ['ENTERGY LOUISIANA LLC'],        # Entergy Louisiana (previously missing)
    'EAI':   ['ENTERGY ARKANSAS LLC'],
    'ELI':   ['ENTERGY LOUISIANA LLC'],
    'EMI':   ['ENTERGY MISSISSIPPI LLC'],
    'ETX':   ['ENTERGY TEXAS INC.'],
    'ENOI':  ['ENTERGY NEW ORLEANS, LLC'],
    'EMBA':  ['ENTERGY LOUISIANA LLC', 'ENTERGY ARKANSAS LLC',  # admin/historical code
              'ENTERGY MISSISSIPPI LLC', 'ENTERGY TEXAS INC.'],
    'AMIL':  ['AMEREN ILLINOIS COMPANY'],
    'AMMO':  ['UNION ELECTRIC CO - (MO)'],
    # WAUE = WAPA Upper Great Plains: no retail territory, but its lines neighbor OTP/MDU/NSP
    'WAUE':  ['OTTER TAIL POWER CO', 'MONTANA-DAKOTA UTILITIES CO',
              'NORTHERN STATES POWER CO', 'NORTHERN STATES POWER CO - MINNESOTA'],
    'WEC':   ['WISCONSIN ELECTRIC POWER CO'],
    'WPS':   ['WISCONSIN PUBLIC SERVICE CORP'],
    'MGE':   ['MADISON GAS & ELECTRIC CO'],
    'CONS':  ['CONSUMERS ENERGY', 'CONSUMERS ENERGY CO'],
    'DECO':  ['DTE ELECTRIC COMPANY'],
    'NIPS':  ['NORTHERN INDIANA PUB SERV CO'],
    'IPL':   ['INDIANAPOLIS POWER & LIGHT CO'],
    'SIGE':  ['SOUTHERN INDIANA GAS & ELEC CO'],
    'CIN':   ['DUKE ENERGY INDIANA, LLC'],
    'CLEC':  ['CLECO POWER LLC'],
    'UPPC':  ['UPPER PENINSULA POWER COMPANY'],
    'MIUP':  ['UPPER MICHIGAN ENERGY RESOURCES CORP.'],
    'LGEE':  ['JACKSON PURCHASE ENERGY CORPORATION', 'KENERGY CORP'],
    'EKPC':  ['EAST CENTRAL ENERGY'],
    # MPS (Evergy MO/KCP&L), KCPL, AECI, EDE: not in HIFLD retail territory dataset
}


def build_territory_index(territories_path):
    """
    Load territory polygons from the cached JSON and build a Shapely STRtree.

    Returns
    -------
    territories : list of (name_upper, Polygon)
    tree        : STRtree over the polygon list (index-aligned)
    """
    with open(territories_path) as f:
        raw = json.load(f)

    territories = []
    polys_for_tree = []

    for feat in raw:
        a    = feat["attributes"]
        geom = feat.get("geometry") or {}
        rings = geom.get("rings", [])
        if not rings:
            continue
        name = (a.get("NAME") or "").upper().strip()
        exterior = [(pt[0], pt[1]) for pt in rings[0]]
        holes    = [[(pt[0], pt[1]) for pt in r] for r in rings[1:]]
        try:
            poly = Polygon(exterior, holes)
            if not poly.is_valid:
                poly = make_valid(poly)
            if poly.is_empty:
                continue
        except Exception:
            continue
        territories.append((name, poly))
        polys_for_tree.append(poly)

    tree = STRtree(polys_for_tree)
    print(f"Built territory index: {len(territories)} polygons")
    return territories, tree


def territory_for_point(lon, lat, territories, tree):
    """Return the territory NAME for a coordinate, or None."""
    pt = Point(lon, lat)
    idxs = tree.query(pt, predicate="within")
    if len(idxs):
        return territories[idxs[0]][0]
    return None


def annotate_line_territories(lines, territories, tree):
    """
    Add 'end1_territory' and 'end2_territory' keys to each line dict (in-place).
    Endpoints are the first coord of the first path and last coord of the last path.
    """
    print("Annotating line endpoints with utility territories...")
    for line in lines:
        paths = line["paths"]
        p1 = paths[0][0]       # [lon, lat]
        p2 = paths[-1][-1]     # [lon, lat]
        line["end1_territory"] = territory_for_point(p1[0], p1[1], territories, tree)
        line["end2_territory"] = territory_for_point(p2[0], p2[1], territories, tree)
    matched = sum(1 for l in lines if l["end1_territory"] or l["end2_territory"])
    print(f"  {matched}/{len(lines)} lines have at least one endpoint in a named territory")


def territories_for_owner(code):
    """
    Return the set of territory NAME strings expected for a given owner code.
    Returns an empty set if the code is unknown or unmapped.
    """
    names = OWNER_TERRITORY_NAMES.get(code, [])
    return {n.upper() for n in names}


def validate_territory(line, owner1, owner2):
    """
    Check whether a matched line's endpoints fall in the expected territories.
    Returns 'full' (both ends validated), 'partial' (one end), or 'none'.
    """
    t1 = (line.get("end1_territory") or "").upper()
    t2 = (line.get("end2_territory") or "").upper()
    ends = {t for t in [t1, t2] if t}

    expected1 = territories_for_owner(owner1)
    expected2 = territories_for_owner(owner2)

    hit1 = bool(ends & expected1) if expected1 else None
    hit2 = bool(ends & expected2) if expected2 else None

    if hit1 and hit2:
        return "full"
    if hit1 or hit2:
        return "partial"
    if hit1 is None and hit2 is None:
        return "unknown"   # neither owner is in our map
    return "none"


# ── STEP 1c: FETCH HIFLD TRANSMISSION LINES ──────────────────────────────────

def fetch_hifld_lines(output_path="hifld_lines.json"):
    """
    Download all HIFLD Electric Power Transmission Lines (VOLTAGE >= 100 kV)
    with full polyline geometry. Saves to JSON. Run once — takes ~2 minutes.
    """
    url = ("https://services1.arcgis.com/Hp6G80Pky0om7QvQ/arcgis/rest/"
           "services/Electric_Power_Transmission_Lines/FeatureServer/0/query")
    all_features = []
    offset = 0
    batch = 2000

    print("Fetching HIFLD transmission lines (VOLTAGE >= 100 kV)...")
    while True:
        params = {
            "where": "VOLTAGE >= 100",
            "outFields": "SUB_1,SUB_2,VOLTAGE,OWNER",
            "returnGeometry": "true",
            "outSR": "4326",
            "f": "json",
            "resultRecordCount": batch,
            "resultOffset": offset,
        }
        r = requests.get(url, params=params, timeout=60)
        data = r.json()
        if "error" in data:
            raise RuntimeError(f"HIFLD API error: {data['error']}")
        features = data.get("features", [])
        if not features:
            break
        all_features.extend(features)
        print(f"  {len(all_features)} lines fetched...")
        if not data.get("exceededTransferLimit"):
            break
        offset += batch

    with open(output_path, "w") as f:
        json.dump(all_features, f)
    print(f"Saved {len(all_features)} lines to {output_path}")
    return all_features


_UNNAMED = {"NOT AVAILABLE", "NOT AVAILABLE-1", "NOT AVAILABLE-2"}

def _is_unnamed(s):
    return not s or s.startswith("UNKNOWN") or s.startswith("TAP") or s in _UNNAMED


def load_hifld_lines(path="hifld_lines.json"):
    """
    Load HIFLD lines JSON. Returns a list of dicts (one per named line) and
    a parallel list of combined-key strings for fast fuzzy search.

    Combined key = sorted join of both endpoint names, e.g.
        SUB_1="GIBSON STATION", SUB_2="FRANCISCO"
        → "FRANCISCO | GIBSON STATION"
    Sorting makes the key direction-independent.
    """
    with open(path) as f:
        raw = json.load(f)

    lines = []
    for feat in raw:
        a = feat["attributes"]
        sub1 = str(a.get("SUB_1") or "").strip().upper()
        sub2 = str(a.get("SUB_2") or "").strip().upper()

        if _is_unnamed(sub1) or _is_unnamed(sub2):
            continue

        volt = a.get("VOLTAGE")
        if not volt:
            continue

        geom = feat.get("geometry") or {}
        paths = geom.get("paths", [])
        if not paths:
            continue

        lines.append({
            "sub1":     sub1,
            "sub2":     sub2,
            "combined": " | ".join(sorted([sub1, sub2])),
            "voltage":  float(volt),
            "owner":    str(a.get("OWNER") or ""),
            "paths":    paths,           # list of [[lon,lat], ...]
        })

    print(f"Loaded {len(lines)} named HIFLD transmission lines")
    return lines


# ── STEP 2: PARSE CONSTRAINT NAMES ───────────────────────────────────────────

def parse_contingency_description(s):
    """
    'ST JOE - FAIRPORT - COOPER 345' → (['ST JOE','FAIRPORT','COOPER'], 345)
    'WILTON CENTER-DUMONT 765 (11215)' → (['WILTON CENTER','DUMONT'], 765)
    Returns ([], None) for 'ACTUAL' or empty.
    """
    if not s or str(s).strip() in ("ACTUAL", ""):
        return [], None
    clean = re.sub(r'\s*\(\d+\)\s*$', '', str(s)).strip()
    tokens = clean.split()
    kv = None
    if tokens and re.match(r'^\d+$', tokens[-1]):
        kv = int(tokens[-1])
        clean = " ".join(tokens[:-1]).strip()
    if ' - ' in clean:
        subs = [p.strip() for p in clean.split(' - ')]
    else:
        subs = [p.strip() for p in re.split(r'(?<=[A-Z0-9])-(?=[A-Z])', clean)]
    return [s for s in subs if s and len(s) > 1], kv


def parse_constraint_name_monitored(constraint_name):
    """
    Extract monitored-element substations from a constraint name.

    'WAHPETN-HANKSON BASE'          → (['WAHPETN', 'HANKSON'], None)
    'FARGO-SHEYN FLO CTR-JAMESTOWN' → (['FARGO', 'SHEYN'], None)
    'PRES-TIBB 138 FLO ASTER-COMMO' → (['PRES', 'TIBB'], 138)

    Returns ([], None) if the name can't be parsed.
    """
    if not constraint_name:
        return [], None
    s = str(constraint_name).strip().upper()
    # Take only the part before ' FLO ' or ' BASE'
    s = re.split(r'\s+FLO\s+', s)[0]
    s = re.sub(r'\s+BASE\s*$', '', s).strip()
    # Strip trailing parenthetical IDs
    s = re.sub(r'\s*\(\d+\)\s*$', '', s).strip()
    tokens = s.split()
    kv = None
    if tokens and re.match(r'^\d+$', tokens[-1]):
        kv = int(tokens[-1])
        s = " ".join(tokens[:-1]).strip()
    # Split on ' - ' first, then fall back to run-of-hyphens between word chars
    if ' - ' in s:
        parts = [p.strip() for p in s.split(' - ')]
    else:
        parts = [p.strip() for p in re.split(r'(?<=[A-Z0-9])-(?=[A-Z])', s)]
    parts = [p for p in parts if p and len(p) > 1]
    return parts, kv


def parse_branch_name(s):
    """
    'COOPER ST_JOCOOPE34_1 A (LN/NPPD/MPS)' → {branch_type, owner1, owner2}
    """
    result = dict(branch_type=None, owner1=None, owner2=None)
    if not s or not str(s).strip():
        return result
    m = re.search(r'\((\w+)/(\w+|\*)/(\w+|\*)\)', str(s))
    if m:
        result["branch_type"] = m.group(1)
        result["owner1"]      = m.group(2)
        result["owner2"]      = m.group(3)
    return result


# ── STEP 3: MATCH LINES ───────────────────────────────────────────────────────

def _pairs(subs):
    """Return all ordered pairs from the first 3 substation names."""
    names = [s.upper().strip() for s in subs[:3]]
    for i in range(len(names)):
        for j in range(i + 1, len(names)):
            yield names[i], names[j]


def _best_fuzzy_match(subs, candidates, score_cutoff):
    """Run pair fuzzy matching over a candidate list. Returns (line, score, pair_str) or None."""
    if not candidates:
        return None
    combined_keys = [l["combined"] for l in candidates]
    best_score, best_line, best_pair = 0, None, None
    for a, b in _pairs(subs):
        query_key = " | ".join(sorted([a, b]))
        result = process.extractOne(
            query_key, combined_keys,
            scorer=fuzz.token_sort_ratio,
            score_cutoff=score_cutoff,
        )
        if result and result[1] > best_score:
            best_score = result[1]
            best_line  = candidates[result[2]]
            best_pair  = f"{a} / {b}"
    if best_line is None:
        return None
    return best_line, best_score, best_pair


def match_line(subs, kv, lines, score_cutoff=70, owner1=None, owner2=None):
    """
    Match substation names against HIFLD lines.

    Matching strategy (each tier falls back to the next if no result):

    Tier 1 — Territory filter (score_cutoff - 10):
        Keep lines where one endpoint is in owner1's territory AND the other
        is in owner2's territory.  Narrowest set → allows lower threshold.

    Tier 2 — Single-owner territory filter (score_cutoff - 5):
        Keep lines where at least one endpoint is in either owner's territory.

    Tier 3 — Voltage filter only (score_cutoff):
        Original behaviour.
    """
    if len(subs) < 2:
        return None

    # Build expected territory sets for each owner
    exp1 = territories_for_owner(owner1) if owner1 else set()
    exp2 = territories_for_owner(owner2) if owner2 else set()
    both_known = bool(exp1 and exp2)
    either_known = bool(exp1 or exp2)

    # Voltage pre-filter (applied inside each tier)
    def volt_filter(pool):
        if not kv:
            return pool
        lo, hi = kv * 0.85, kv * 1.15
        filtered = [l for l in pool if lo <= l["voltage"] <= hi]
        return filtered if len(filtered) >= 3 else pool

    # Tier 1: both-territory filter
    if both_known:
        t1_pool = [
            l for l in lines
            if ({(l.get("end1_territory") or "").upper(),
                 (l.get("end2_territory") or "").upper()} & exp1)
            and ({(l.get("end1_territory") or "").upper(),
                  (l.get("end2_territory") or "").upper()} & exp2)
        ]
        result = _best_fuzzy_match(subs, volt_filter(t1_pool), score_cutoff - 10)
        if result:
            line, score, pair = result
            return {**line, "score": score, "query_pair": pair, "match_tier": 1}

    # Tier 2: single-owner territory filter
    if either_known:
        either_exp = exp1 | exp2
        t2_pool = [
            l for l in lines
            if {(l.get("end1_territory") or "").upper(),
                (l.get("end2_territory") or "").upper()} & either_exp
        ]
        result = _best_fuzzy_match(subs, volt_filter(t2_pool), score_cutoff - 5)
        if result:
            line, score, pair = result
            return {**line, "score": score, "query_pair": pair, "match_tier": 2}

    # Tier 3: voltage-only filter limited to MISO geographic footprint
    # lon: -106 to -80, lat: 27 to 51  (Louisiana → Manitoba, Montana → Michigan)
    def in_miso_bounds(l):
        paths = l.get("paths", [])
        if not paths:
            return False
        def chk(pt):
            return -106 <= pt[0] <= -80 and 27 <= pt[1] <= 51
        return chk(paths[0][0]) or chk(paths[-1][-1])

    t3_pool = [l for l in volt_filter(lines) if in_miso_bounds(l)]
    result = _best_fuzzy_match(subs, t3_pool, score_cutoff)
    if result:
        line, score, pair = result
        return {**line, "score": score, "query_pair": pair, "match_tier": 3}

    return None


# ── STEP 4: BUILD GEOJSON ────────────────────────────────────────────────────

def build_geojson(df_constraints, lines, score_threshold=70):
    """
    Match each unique constraint and emit a GeoJSON feature using the
    HIFLD line's actual routed geometry.

    df_constraints columns required:
        "Constraint Name"
        "Branch Name ( Branch Type / From CA / To CA )"
        "Contingency Description"
        "SUM of Abs Shadow"
    """
    features = []
    stats = dict(total=0, matched=0, unmatched=0,
                 tier1=0, tier2=0, tier3=0,
                 territory_full=0, territory_partial=0, territory_none=0, territory_unknown=0)

    for _, row in df_constraints.iterrows():
        stats["total"] += 1
        branch = parse_branch_name(
            row.get("Branch Name ( Branch Type / From CA / To CA )", ""))

        if branch.get("branch_type") == "XF":
            stats["unmatched"] += 1
            continue

        contingency_desc = row.get("Contingency Description", "")
        constraint_name  = row.get("Constraint Name", "")
        is_base = str(contingency_desc).strip() in ("ACTUAL", "")

        # Primary source: contingency description (human-readable sub names)
        subs_cont, kv_cont = parse_contingency_description(contingency_desc)
        # Secondary source: monitored element from constraint name
        subs_mon, kv_mon = parse_constraint_name_monitored(constraint_name)

        owner1 = branch.get("owner1")
        owner2 = branch.get("owner2")

        m = None
        # For BASE constraints, start with monitored element; otherwise try contingency first
        if is_base:
            if len(subs_mon) >= 2:
                m = match_line(subs_mon, kv_mon, lines, score_cutoff=score_threshold,
                               owner1=owner1, owner2=owner2)
        else:
            if len(subs_cont) >= 2:
                m = match_line(subs_cont, kv_cont, lines, score_cutoff=score_threshold,
                               owner1=owner1, owner2=owner2)
            # Fallback: try monitored element if contingency failed to match
            if m is None and len(subs_mon) >= 2:
                m = match_line(subs_mon, kv_mon, lines, score_cutoff=score_threshold,
                               owner1=owner1, owner2=owner2)

        if m is None:
            stats["unmatched"] += 1
            continue

        stats["matched"] += 1
        stats[f"tier{m.get('match_tier', 3)}"] += 1

        territory_status = validate_territory(m, owner1, owner2)
        stats[f"territory_{territory_status}"] += 1

        paths = m["paths"]
        geometry = ({"type": "LineString",      "coordinates": paths[0]}
                    if len(paths) == 1 else
                    {"type": "MultiLineString", "coordinates": paths})

        features.append({
            "type": "Feature",
            "geometry": geometry,
            "properties": {
                "constraint_name":    row.get("Constraint Name", ""),
                "contingency":        row.get("Contingency Description", ""),
                "branch_type":        branch.get("branch_type"),
                "owner1":             owner1,
                "owner2":             owner2,
                "kv":                 kv_cont or kv_mon,
                "total_shadow_price": row.get("SUM of Abs Shadow", 0),
                "matched_sub1":       m["sub1"],
                "matched_sub2":       m["sub2"],
                "end1_territory":     m.get("end1_territory"),
                "end2_territory":     m.get("end2_territory"),
                "hifld_owner":        m["owner"],
                "match_score":        m["score"],
                "match_tier":         m.get("match_tier", 3),
                "query_pair":         m["query_pair"],
                "territory_status":   territory_status,
            },
        })

    print(f"\nMatching results: {stats}")
    return {"type": "FeatureCollection", "features": features}


# ── STEP 5: BUILD MAP ─────────────────────────────────────────────────────────

def build_folium_map(geojson, territories_path="hifld_territories.json",
                     output_html="miso_constraints_map.html"):
    """Interactive Folium map. Line weight = voltage (kV), color = shadow price."""
    import folium
    import branca.colormap as cm

    m = folium.Map(location=[40.5, -89], zoom_start=5, tiles="CartoDB positron")

    # ── Utility territories layer ─────────────────────────────────────────────
    if Path(territories_path).exists():
        with open(territories_path) as f:
            raw = json.load(f)

        territory_group = folium.FeatureGroup(name="Utility Territories", show=True)

        for feat in raw:
            a    = feat["attributes"]
            geom = feat.get("geometry") or {}
            name = a.get("NAME") or "Unknown"
            rings = geom.get("rings", [])
            if not rings:
                continue
            tooltip = (
                f"<b>{name}</b><br>"
                f"{a.get('STATE','')} | {a.get('TYPE','')}<br>"
                f"Control area: {a.get('CNTRL_AREA','')}"
            )
            folium.Polygon(
                locations=[[pt[1], pt[0]] for pt in rings[0]],
                color="#888888", fill=True, fill_color="#aaaaaa",
                fill_opacity=0.08, weight=0.5, opacity=0.4,
                tooltip=tooltip,
            ).add_to(territory_group)

        territory_group.add_to(m)

    import math
    prices     = [f["properties"]["total_shadow_price"] for f in geojson["features"]]
    log_prices = [math.log10(max(p, 1)) for p in prices]
    # Floor at log10(100)=2 so anything <$100 renders as darkest blue
    log_min = 2.0
    log_max = max(log_prices)

    colormap = cm.LinearColormap(
        colors=["#2166ac", "#74add1", "#abd9e9", "#f46d43", "#d73027", "#a50026"],
        vmin=log_min, vmax=log_max,
    )
    colormap.caption = "Annual Shadow Price (log scale, $100 – $%s)" % f"{int(10**log_max):,}"

    def kv_weight(kv):
        if   kv >= 765: return 9
        elif kv >= 500: return 7
        elif kv >= 345: return 5
        elif kv >= 230: return 3.5
        elif kv >= 161: return 2.5
        elif kv >= 138: return 2
        else:           return 1.5

    # ── Binding constraint lines layer ───────────────────────────────────────
    constraint_group = folium.FeatureGroup(name="Binding Constraints", show=True)

    for feat in geojson["features"]:
        props  = feat["properties"]
        geom   = feat["geometry"]
        price  = props.get("total_shadow_price", 0)
        kv     = props.get("kv") or 0
        weight = kv_weight(kv)
        color  = colormap(max(math.log10(max(price, 1)), log_min))

        t_status  = props.get("territory_status", "unknown")
        t_icons   = {"full": "✓", "partial": "~", "none": "✗", "unknown": "?"}
        t_icon    = t_icons.get(t_status, "?")
        end1_t    = props.get("end1_territory") or "—"
        end2_t    = props.get("end2_territory") or "—"
        tier      = props.get("match_tier", 3)

        tooltip = (
            f"<b>{props['constraint_name']}</b><br>"
            f"{props['matched_sub1']} → {props['matched_sub2']}<br>"
            f"{kv} kV | owners: {props.get('owner1','')} / {props.get('owner2','')}<br>"
            f"Territories: {end1_t} / {end2_t} {t_icon}<br>"
            f"Shadow price: ${price:,.0f} | score: {props['match_score']:.0f} | tier {tier}"
        )

        # Dashed line for unvalidated matches, solid for validated
        dash = "5 5" if t_status == "none" else None

        path_list = ([geom["coordinates"]] if geom["type"] == "LineString"
                     else geom["coordinates"])

        for path in path_list:
            folium.PolyLine(
                [[c[1], c[0]] for c in path],
                weight=weight, color=color, opacity=0.85,
                tooltip=tooltip, dash_array=dash,
            ).add_to(constraint_group)

    constraint_group.add_to(m)
    colormap.add_to(m)

    legend_html = """
    <div style="position:fixed;bottom:50px;left:50px;z-index:1000;
                background:white;padding:10px;border-radius:5px;
                border:1px solid #ccc;font-size:12px;line-height:1.6;">
        <b>Line weight = Voltage (kV)</b><br>
        <span style="font-size:15px;line-height:1;">━━━━━</span> 765 kV<br>
        <span style="font-size:12px;line-height:1;">━━━━━</span> 500 kV<br>
        <span style="font-size:10px;line-height:1;">━━━━━</span> 345 kV<br>
        <span style="font-size:8px; line-height:1;">━━━━━</span> 230 kV<br>
        <span style="font-size:6px; line-height:1;">━━━━━</span> &lt;230 kV
    </div>
    """
    m.get_root().html.add_child(folium.Element(legend_html))
    folium.LayerControl(collapsed=False).add_to(m)

    m.save(output_html)
    print(f"Map saved to {output_html}")
    return m


# ── MAIN ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # 1. Download HIFLD utility territories (auto-runs once, then cached)
    territories_path = "hifld_territories.json"
    if not Path(territories_path).exists():
        fetch_utility_territories(territories_path)

    # 2. Download HIFLD transmission lines (auto-runs once, then cached)
    lines_path = "hifld_lines.json"
    if not Path(lines_path).exists():
        fetch_hifld_lines(lines_path)

    # 3. Build territory spatial index and annotate line endpoints
    territories, tree = build_territory_index(territories_path)
    lines = load_hifld_lines(lines_path)
    annotate_line_territories(lines, territories, tree)

    # 3. Load constraint data
    input_csv = "2025_da_bc_HIST - 2025_da_bc_HIST.csv"
    df = pd.read_csv(input_csv, skiprows=2, low_memory=False)

    # 4. Aggregate shadow prices by unique constraint
    df["Abs Shadow"] = pd.to_numeric(
        df["Abs Shadow"].astype(str).str.replace(r'[$,]', '', regex=True),
        errors="coerce",
    )
    unique = (
        df.groupby([
            "Constraint Name",
            "Branch Name ( Branch Type / From CA / To CA )",
            "Contingency Description",
        ])
        .agg({"Abs Shadow": "sum"})
        .reset_index()
        .rename(columns={"Abs Shadow": "SUM of Abs Shadow"})
    )
    print(f"\nUnique constraints to map: {len(unique)}")

    # 5. Build GeoJSON
    geojson = build_geojson(unique, lines)

    # 6. Save GeoJSON
    with open("miso_constraints.geojson", "w") as f:
        json.dump(geojson, f, indent=2)
    print(f"Saved {len(geojson['features'])} features to miso_constraints.geojson")

    # 7. Build map
    build_folium_map(geojson, territories_path=territories_path)
