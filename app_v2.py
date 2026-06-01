"""Multi-RTO Congestion Analysis Tool — Streamlit app (v2)."""

import os
import duckdb
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from pathlib import Path

# ── Database path ─────────────────────────────────────────────────────────────
# When GCS_BUCKET is set (Cloud Run), the database is downloaded from Google
# Cloud Storage on first load and cached for the lifetime of the container.
# When running locally, it reads lmp_analysis.duckdb from this directory.
_GCS_BUCKET = os.getenv("GCS_BUCKET")
_LOCAL_DB   = Path("/tmp/lmp_analysis.duckdb") if _GCS_BUCKET else Path(__file__).parent / "lmp_analysis.duckdb"

st.set_page_config(
    page_title="RTO Congestion Tool",
    page_icon="⚡",
    layout="wide",
)


# ── Database connection ───────────────────────────────────────────────────────

@st.cache_resource(show_spinner="Loading analytics database…")
def get_connection():
    if _GCS_BUCKET and not _LOCAL_DB.exists():
        from google.cloud import storage as gcs
        print(f"Downloading lmp_analysis.duckdb from gs://{_GCS_BUCKET} …", flush=True)
        gcs.Client().bucket(_GCS_BUCKET).blob("lmp_analysis.duckdb").download_to_filename(str(_LOCAL_DB))
        print("Download complete.", flush=True)
    return duckdb.connect(str(_LOCAL_DB), read_only=True)


# ── Reference data (cached at startup) ───────────────────────────────────────

@st.cache_data
def load_zone_color_map() -> dict[str, str]:
    """zone_label → zone_color from ca_reference (all RTOs)."""
    con = get_connection()
    rows = con.execute("""
        SELECT DISTINCT zone_label, zone_color
        FROM ca_reference
        WHERE zone_label IS NOT NULL
    """).fetchall()
    return {label: color for label, color in rows} | {"Non-LBA": "#aaaaaa"}


@st.cache_data
def load_rto_list() -> list[str]:
    con = get_connection()
    return [r[0] for r in con.execute(
        "SELECT DISTINCT rto FROM screener ORDER BY rto"
    ).fetchall()]


# ── Data loaders ──────────────────────────────────────────────────────────────

@st.cache_data
def load_screener(rtos: tuple[str, ...] | None = None) -> pd.DataFrame:
    con = get_connection()
    params: list = []
    if rtos:
        placeholders = ", ".join(["?"] * len(rtos))
        rto_clause = f"AND rto IN ({placeholders})"
        params = list(rtos)
    else:
        rto_clause = ""
    df = con.execute(f"""
        SELECT
            rto,
            zone_label,
            zone_color,
            lrz,
            utility_ca,
            utility_name,
            n_load_zones,
            avg_mcc,
            avg_positive_mcc,
            pct_hours_above_5,
            pct_hours_above_10,
            sum_positive_mcc,
            max_mcc,
            owned_binding_events,
            owned_unique_constraints,
            owned_total_abs_sp,
            corr_unique_constraints,
            corr_total_hours,
            corr_total_abs_sp,
            corr_in_territory_constraints,
            corr_in_territory_hours,
            corr_in_territory_ln_constraints,
            corr_in_territory_ln_hours,
            corr_in_territory_ln_total_abs_sp,
            top_corr_constraint
        FROM screener
        WHERE avg_mcc IS NOT NULL
        {rto_clause}
    """, params).df()
    # Ensure Python strings (not Arrow-backed) for Plotly groupby
    df["zone_label"] = df["zone_label"].fillna("Non-LBA").astype(object)
    df["zone_color"] = df["zone_color"].fillna("#aaaaaa").astype(object)
    return df


@st.cache_data
def load_monthly(utility_ca: str, rto: str) -> pd.DataFrame:
    con = get_connection()
    return con.execute("""
        SELECT month, component, avg_value, avg_positive, sum_positive,
               pct_above_5, pct_above_10, max_value, p90_value
        FROM ca_lmp_monthly
        WHERE rto = ? AND utility_ca = ?
        ORDER BY month, component
    """, [rto, utility_ca]).df()


@st.cache_data
def load_hourly_heatmap(utility_ca: str, rto: str) -> pd.DataFrame:
    con = get_connection()
    return con.execute("""
        SELECT cal_month, hour_ending, avg_value
        FROM ca_lmp_by_hour
        WHERE rto = ? AND utility_ca = ? AND component = 'MCC'
        ORDER BY cal_month, hour_ending
    """, [rto, utility_ca]).df()


@st.cache_data
def load_correlated_constraints(utility_ca: str, rto: str, top_n: int = 50) -> pd.DataFrame:
    con = get_connection()
    return con.execute("""
        SELECT
            constraint_id,
            constraint_name,
            branch_type,
            from_ca,
            from_ca_name,
            to_ca,
            to_ca_name,
            co_occurrence_hours,
            avg_ca_mcc_when_binding,
            avg_abs_sp,
            total_abs_sp,
            (from_ca = ? OR to_ca = ?) AS in_territory
        FROM mcc_constraint_correlation
        WHERE rto = ? AND utility_ca = ?
        ORDER BY co_occurrence_hours DESC
        LIMIT ?
    """, [utility_ca, utility_ca, rto, utility_ca, top_n]).df()


@st.cache_data
def load_in_territory_constraints(utility_ca: str, rto: str) -> pd.DataFrame:
    """All constraints where From CA or To CA is in the target utility's territory."""
    con = get_connection()
    return con.execute("""
        SELECT
            constraint_id,
            constraint_name,
            branch_type,
            from_ca,
            from_ca_name,
            to_ca,
            to_ca_name,
            co_occurrence_hours,
            avg_ca_mcc_when_binding,
            avg_abs_sp,
            total_abs_sp
        FROM mcc_constraint_correlation
        WHERE rto = ? AND utility_ca = ?
          AND (from_ca = ? OR to_ca = ?)
        ORDER BY co_occurrence_hours DESC
    """, [rto, utility_ca, utility_ca, utility_ca]).df()


@st.cache_data
def load_constraint_monthly(utility_ca: str, rto: str) -> pd.DataFrame:
    """Monthly binding hours for top constraints correlated with a CA."""
    con = get_connection()
    return con.execute("""
        WITH top_constraints AS (
            SELECT constraint_id
            FROM mcc_constraint_correlation
            WHERE rto = ? AND utility_ca = ?
            ORDER BY co_occurrence_hours DESC
            LIMIT 8
        )
        SELECT
            DATE_TRUNC('month', bc.market_date)::DATE   AS month,
            bc.constraint_id,
            ANY_VALUE(bc.constraint_name)               AS constraint_name,
            COUNT(*)                                    AS binding_hours,
            ROUND(SUM(ABS(bc.shadow_price)), 0)         AS total_abs_sp
        FROM da_binding_constraints bc
        JOIN top_constraints t ON bc.constraint_id = t.constraint_id
        WHERE bc.rto = ?
        GROUP BY month, bc.constraint_id
        ORDER BY month, binding_hours DESC
    """, [rto, utility_ca, rto]).df()


# ── Page: Screener ────────────────────────────────────────────────────────────

def page_screener():
    st.title("⚡ RTO Congestion Screener")

    all_rtos = load_rto_list()
    zone_color_map = load_zone_color_map()

    # ── Sidebar filters ───────────────────────────────────────────────────────
    with st.sidebar:
        st.header("Filters")

        sel_rtos = st.multiselect(
            "RTO",
            options=all_rtos,
            default=all_rtos,
        )
        rtos_tuple = tuple(sel_rtos) if sel_rtos else tuple(all_rtos)

        df = load_screener(rtos_tuple)
        st.caption(f"{len(df):,} utilities loaded")

        all_zones = sorted(df["zone_label"].dropna().unique().tolist())
        sel_zones = st.multiselect(
            "Zone / LRZ",
            options=all_zones,
            default=all_zones,
        )
        include_non_lba = st.checkbox("Include Non-LBA", value=True)

        min_zones = st.slider("Min load zones", 1, 20, 1)

        st.divider()
        sort_metric = st.selectbox(
            "Sort / chart by",
            options=[
                ("avg_mcc",                      "Avg MCC ($/MWh)"),
                ("avg_positive_mcc",             "Avg Positive MCC ($/MWh)"),
                ("pct_hours_above_5",            "% Hours MCC > $5"),
                ("sum_positive_mcc",             "Total Positive MCC (annual sum)"),
                ("corr_in_territory_hours",      "In-Territory Constraint Hours"),
                ("owned_total_abs_sp",           "Owned Constraint Severity (Σ|SP|)"),
                ("corr_total_hours",             "All Correlated Congestion Hours"),
            ],
            format_func=lambda x: x[1],
        )
        metric_col, metric_label = sort_metric
        top_n = st.slider("Show top N utilities", 5, 100, 20)

    # ── Filter data ───────────────────────────────────────────────────────────
    mask = df["n_load_zones"] >= min_zones
    if sel_zones:
        zone_mask = df["zone_label"].isin(sel_zones)
        if include_non_lba:
            zone_mask = zone_mask | df["zone_label"].isna() | (df["zone_label"] == "Non-LBA")
        mask = mask & zone_mask

    filtered = df[mask].copy()
    filtered = filtered.sort_values(metric_col, ascending=False).head(top_n)

    rto_list_str = ", ".join(rtos_tuple)
    st.caption(f"RTO(s): {rto_list_str} · Load zones only · MCC = Marginal Congestion Component")

    # ── KPI row ───────────────────────────────────────────────────────────────
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Utilities shown", len(filtered))
    k2.metric("Highest avg MCC", f"${filtered['avg_mcc'].max():.2f}/MWh",
              delta=filtered.iloc[0]["utility_ca"])
    k3.metric("Most corr. constraint hours", f"{int(filtered['corr_total_hours'].max()):,}",
              delta=filtered.sort_values('corr_total_hours', ascending=False).iloc[0]["utility_ca"])
    k4.metric("Highest owned Σ|SP|",
              f"${filtered['owned_total_abs_sp'].max()/1e6:.2f}M",
              delta=filtered.sort_values('owned_total_abs_sp', ascending=False).iloc[0]["utility_ca"])

    st.divider()

    # ── Shared RTO color palette (used by bar chart and bubble chart) ─────────
    RTO_COLORS = {
        "MISO":  "#1f77b4",
        "SPP":   "#ff7f0e",
        "ISONE": "#2ca02c",
        "NYISO": "#9467bd",
        "PJM":   "#d62728",
        "CAISO": "#8c564b",
    }

    # ── Bar chart ─────────────────────────────────────────────────────────────
    chart_df = filtered.sort_values(metric_col, ascending=True).copy()
    chart_df["label"] = (chart_df["utility_ca"] + "  " + chart_df["utility_name"]).astype(str)

    fig_bar = go.Figure()
    seen_rto: set = set()
    for _, row in chart_df.iterrows():
        rto_name = str(row["rto"])
        color = RTO_COLORS.get(rto_name, "#aaaaaa")
        rto_tag = f" [{row['rto']}]" if len(rtos_tuple) > 1 else ""
        fig_bar.add_trace(go.Bar(
            name=rto_name,
            x=[row[metric_col]],
            y=[row["label"] + rto_tag],
            orientation="h",
            marker_color=color,
            showlegend=rto_name not in seen_rto,
            hovertemplate=(
                f"<b>{row['utility_ca']}</b> — {row['utility_name']} [{row['rto']}]<br>"
                f"{metric_label}: %{{x:.2f}}<extra></extra>"
            ),
        ))
        seen_rto.add(rto_name)

    fig_bar.update_layout(
        barmode="overlay",
        title=f"Top {top_n} Utilities by {metric_label}",
        xaxis_title=metric_label,
        yaxis_title="",
        legend=dict(orientation="h", yanchor="bottom", y=1.01, xanchor="left", x=0),
        margin=dict(l=0, r=20, t=60, b=20),
        yaxis=dict(tickfont=dict(size=11)),
        height=max(400, top_n * 28),
    )
    st.plotly_chart(fig_bar, use_container_width=True)

    # ── Scatter: in-territory LINE shadow price sum vs MCC intensity ──────────
    st.subheader("Congestion: In-Territory Line Constraints vs MCC Intensity")
    st.caption(
        "X = sum of shadow prices (Σ|SP|) for in-territory **line** (LN) constraints correlated "
        "with high-MCC hours. "
        "Bubble size = number of distinct in-territory lines binding. "
        "Top-right utilities have both high congestion costs AND their own lines as bottlenecks."
    )

    full_filtered = df[df["n_load_zones"] >= min_zones].copy()
    if sel_zones:
        zone_mask2 = full_filtered["zone_label"].isin(sel_zones)
        if include_non_lba:
            zone_mask2 = zone_mask2 | full_filtered["zone_label"].isna() | (
                full_filtered["zone_label"] == "Non-LBA"
            )
        full_filtered = full_filtered[zone_mask2]

    full_filtered = full_filtered.copy()
    max_ln = full_filtered["corr_in_territory_ln_constraints"].max()
    if max_ln and max_ln > 0:
        full_filtered["bubble_size"] = (
            full_filtered["corr_in_territory_ln_constraints"] / max_ln * 40 + 6
        ).clip(lower=6)
    else:
        full_filtered["bubble_size"] = 6.0

    fig_scatter = go.Figure()
    for rto_name, grp in full_filtered.groupby("rto", sort=True):
        rto_name = str(rto_name)
        color = RTO_COLORS.get(rto_name, "#aaaaaa")
        fig_scatter.add_trace(go.Scatter(
            name=rto_name,
            x=grp["corr_in_territory_ln_total_abs_sp"].tolist(),
            y=grp["avg_mcc"].tolist(),
            mode="markers+text",
            text=grp["utility_ca"].tolist(),
            textposition="top center",
            textfont=dict(size=10),
            marker=dict(
                color=color,
                size=grp["bubble_size"].tolist(),
                opacity=0.75,
                line=dict(width=1, color="white"),
            ),
            customdata=list(zip(
                grp["utility_name"],
                grp["corr_in_territory_ln_constraints"].astype(int),
                grp["corr_in_territory_ln_hours"].astype(int),
                grp["rto"],
            )),
            hovertemplate=(
                "<b>%{text}</b> — %{customdata[0]} [%{customdata[3]}]<br>"
                "In-Territory Line Σ|SP|: $%{x:,.0f}<br>"
                "Avg MCC: $%{y:.2f}/MWh<br>"
                "In-Territory Lines: %{customdata[1]:,}<br>"
                "Binding Hours: %{customdata[2]:,}<extra></extra>"
            ),
            showlegend=True,
        ))

    fig_scatter.update_layout(
        xaxis_title="In-Territory Line Constraints — Σ|Shadow Price| ($)",
        yaxis_title="Avg MCC ($/MWh)",
        legend=dict(orientation="h", yanchor="bottom", y=1.01, xanchor="left", x=0),
        margin=dict(l=0, r=20, t=20, b=40),
        height=480,
    )
    st.plotly_chart(fig_scatter, use_container_width=True)

    # ── Data table ────────────────────────────────────────────────────────────
    st.subheader("Full Data")
    display_cols = {
        "rto":                         "RTO",
        "zone_label":                  "Zone",
        "utility_ca":                  "CA",
        "utility_name":                "Utility",
        "n_load_zones":                "# Zones",
        "avg_mcc":                     "Avg MCC",
        "avg_positive_mcc":            "Avg Pos. MCC",
        "pct_hours_above_5":           "% Hrs > $5",
        "sum_positive_mcc":            "Σ Pos. MCC",
        "corr_in_territory_constraints": "In-Terr. Constraints",
        "corr_in_territory_hours":     "In-Terr. Hours",
        "corr_in_territory_ln_total_abs_sp": "In-Terr. Line Σ|SP|",
        "corr_total_hours":            "All Corr. Hours",
        "top_corr_constraint":         "Top Corr. Constraint",
    }
    table_df = (
        df[df["n_load_zones"] >= min_zones]
        .sort_values(metric_col, ascending=False)
        [list(display_cols.keys())]
        .rename(columns=display_cols)
    )
    st.dataframe(
        table_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Avg MCC":              st.column_config.NumberColumn(format="$%.2f"),
            "Avg Pos. MCC":         st.column_config.NumberColumn(format="$%.2f"),
            "% Hrs > $5":           st.column_config.NumberColumn(format="%.1f%%"),
            "Σ Pos. MCC":           st.column_config.NumberColumn(format="$%.0f"),
            "In-Terr. Line Σ|SP|":  st.column_config.NumberColumn(format="$%.0f"),
            "In-Terr. Constraints": st.column_config.NumberColumn(format="%d"),
            "In-Terr. Hours":       st.column_config.NumberColumn(format="%d"),
            "All Corr. Hours":      st.column_config.NumberColumn(format="%d"),
        },
    )

    csv = table_df.to_csv(index=False).encode()
    st.download_button("Download CSV", csv, "rto_congestion_screener.csv", "text/csv")


# ── Page: Deep Dive ───────────────────────────────────────────────────────────

def page_deep_dive():
    st.title("🔍 Utility Deep Dive")

    all_rtos = load_rto_list()

    with st.sidebar:
        st.header("Select Utility")

        sel_rto = st.selectbox("RTO", options=all_rtos, index=0)
        df = load_screener((sel_rto,))
        options = df.sort_values("avg_mcc", ascending=False)
        sel = st.selectbox(
            "Utility",
            options=options["utility_ca"].tolist(),
            format_func=lambda ca: f"{ca} — {options.set_index('utility_ca').loc[ca, 'utility_name']}",
        )

    row = df.set_index("utility_ca").loc[sel]

    # ── Header KPIs ───────────────────────────────────────────────────────────
    st.subheader(f"{sel} — {row['utility_name']}")
    zone_info = str(row["zone_label"]) if pd.notna(row.get("zone_label")) else ""
    if zone_info and zone_info != "Non-LBA":
        st.caption(f"{sel_rto} · {zone_info}")
    else:
        st.caption(sel_rto)

    k1, k2, k3, k4, k5, k6 = st.columns(6)
    k1.metric("Avg MCC",               f"${row['avg_mcc']:.2f}/MWh")
    k2.metric("Avg Positive MCC",      f"${row['avg_positive_mcc']:.2f}/MWh")
    k3.metric("% Hours > $5",          f"{row['pct_hours_above_5']:.1f}%")
    k4.metric("In-Territory Constraints",
              f"{int(row['corr_in_territory_constraints']):,}",
              help="Binding constraints where this CA is From CA or To CA, "
                   "active during high-MCC hours in this territory")
    k5.metric("In-Territory Co-occ. Hours", f"{int(row['corr_in_territory_hours']):,}")
    k6.metric("All Corr. Constraints", f"{int(row['corr_unique_constraints']):,}")

    st.divider()

    # ── Monthly trend ─────────────────────────────────────────────────────────
    monthly = load_monthly(sel, sel_rto)
    mcc_monthly = monthly[monthly["component"] == "MCC"].copy()
    mcc_monthly["month_str"] = mcc_monthly["month"].astype(str).str[:7]

    st.subheader("Monthly MCC Profile")
    col_a, col_b = st.columns([2, 1])

    with col_a:
        fig_trend = go.Figure()
        fig_trend.add_trace(go.Bar(
            x=mcc_monthly["month_str"],
            y=mcc_monthly["sum_positive"],
            name="Σ Positive MCC",
            marker_color="rgba(214,39,40,0.3)",
            yaxis="y2",
        ))
        fig_trend.add_trace(go.Scatter(
            x=mcc_monthly["month_str"],
            y=mcc_monthly["avg_value"],
            name="Avg MCC",
            line=dict(color="#1f77b4", width=2.5),
            mode="lines+markers",
        ))
        fig_trend.add_trace(go.Scatter(
            x=mcc_monthly["month_str"],
            y=mcc_monthly["p90_value"],
            name="P90 MCC",
            line=dict(color="#ff7f0e", width=1.5, dash="dot"),
            mode="lines",
        ))
        fig_trend.add_hline(y=0, line_width=1, line_color="black", opacity=0.3)
        fig_trend.update_layout(
            title="Monthly Avg / P90 MCC with Positive MCC Volume",
            yaxis=dict(title="MCC ($/MWh)"),
            yaxis2=dict(title="Σ Positive MCC", overlaying="y", side="right",
                        showgrid=False),
            legend=dict(orientation="h", y=1.12),
            height=360,
            margin=dict(l=0, r=0, t=60, b=30),
        )
        st.plotly_chart(fig_trend, use_container_width=True)

    with col_b:
        st.dataframe(
            mcc_monthly[["month_str", "avg_value", "p90_value", "pct_above_5",
                          "sum_positive"]]
            .rename(columns={
                "month_str":   "Month",
                "avg_value":   "Avg MCC",
                "p90_value":   "P90 MCC",
                "pct_above_5": "% > $5",
                "sum_positive":"Σ Pos. MCC",
            }),
            use_container_width=True,
            hide_index=True,
            column_config={
                "Avg MCC":    st.column_config.NumberColumn(format="$%.2f"),
                "P90 MCC":    st.column_config.NumberColumn(format="$%.2f"),
                "% > $5":     st.column_config.NumberColumn(format="%.1f%%"),
                "Σ Pos. MCC": st.column_config.NumberColumn(format="$%.0f"),
            },
            height=360,
        )

    # ── Hour-of-day heatmap ───────────────────────────────────────────────────
    st.subheader("Congestion Heatmap — Hour of Day × Month")
    heatmap_df = load_hourly_heatmap(sel, sel_rto)
    if not heatmap_df.empty:
        pivot = heatmap_df.pivot(index="hour_ending", columns="cal_month",
                                 values="avg_value")
        pivot.columns = [pd.Timestamp(2025, int(m), 1).strftime("%b")
                         for m in pivot.columns]
        pivot = pivot.sort_index()

        fig_heat = px.imshow(
            pivot,
            color_continuous_scale="RdYlGn_r",
            color_continuous_midpoint=0,
            labels=dict(x="Month", y="Hour Ending", color="Avg MCC ($/MWh)"),
            aspect="auto",
            height=400,
        )
        fig_heat.update_layout(margin=dict(l=0, r=0, t=20, b=20))
        st.plotly_chart(fig_heat, use_container_width=True)

    # ── Correlated constraints ─────────────────────────────────────────────────
    st.subheader("Binding Constraints Correlated with High-MCC Hours")
    st.caption(
        f"Constraints active when {sel}'s avg load zone MCC > $5 — hours when "
        f"congestion is hurting this territory."
    )

    in_terr_df = load_in_territory_constraints(sel, sel_rto)

    # ── A: In-territory — the primary upgrade opportunity view ───────────────
    st.markdown(
        f"#### 🎯 Capacity Upgrade Opportunities — Lines Within {sel}'s Territory"
    )
    st.caption(
        f"Lines where **{sel} is From CA or To CA** that bind during {sel}'s "
        f"high-MCC hours. Upgrading these lines is the most direct way to reduce "
        f"congestion costs in this territory."
    )

    if in_terr_df.empty:
        st.info(
            f"No in-territory binding constraints found for {sel}. "
            f"Congestion here is driven entirely by external lines."
        )
    else:
        in_terr_df = in_terr_df.copy()
        in_terr_df["direction"] = (
            in_terr_df["from_ca"].fillna("?").astype(str)
            + " → "
            + in_terr_df["to_ca"].fillna("?").astype(str)
        )
        col_a1, col_a2 = st.columns([3, 2])

        with col_a1:
            it_sorted = in_terr_df.sort_values("co_occurrence_hours")
            _BTYPE_COLORS = {"LN": "#d62728", "XF": "#c7341a", "ZBR": "#e87461"}
            it_bar_colors = [
                _BTYPE_COLORS.get(str(bt), "#d62728")
                for bt in it_sorted["branch_type"]
            ]
            fig_it = go.Figure()
            fig_it.add_trace(go.Bar(
                x=it_sorted["co_occurrence_hours"].tolist(),
                y=it_sorted["constraint_name"].tolist(),
                orientation="h",
                marker_color=it_bar_colors,
                customdata=list(zip(
                    it_sorted["direction"],
                    it_sorted["branch_type"].astype(str),
                    it_sorted["avg_ca_mcc_when_binding"],
                    it_sorted["avg_abs_sp"],
                    it_sorted["total_abs_sp"],
                )),
                hovertemplate=(
                    "<b>%{y}</b><br>"
                    "Direction: %{customdata[0]}<br>"
                    "Type: %{customdata[1]}<br>"
                    "Co-occurrence Hours: %{x:,}<br>"
                    "Avg CA MCC when binding: $%{customdata[2]:.2f}/MWh<br>"
                    "Avg |SP|: $%{customdata[3]:.2f}<br>"
                    "Total |SP|: $%{customdata[4]:,.0f}<extra></extra>"
                ),
                showlegend=False,
            ))
            for _lbl, _col in [("LN — Line", "#d62728"), ("XF — Transformer", "#c7341a"),
                                ("ZBR — Bus Section", "#e87461")]:
                if _lbl.split(" ")[0] in in_terr_df["branch_type"].astype(str).values:
                    fig_it.add_trace(go.Bar(
                        x=[None], y=[None], name=_lbl,
                        marker_color=_col, showlegend=True,
                    ))
            fig_it.update_layout(
                barmode="overlay",
                xaxis_title="Co-occurrence Hours",
                yaxis_title="",
                legend=dict(orientation="h", y=1.05, x=0),
                margin=dict(l=0, r=0, t=30, b=20),
                yaxis=dict(tickfont=dict(size=10)),
                height=max(300, len(in_terr_df) * 28 + 80),
            )
            st.plotly_chart(fig_it, use_container_width=True)

        with col_a2:
            st.dataframe(
                in_terr_df[["constraint_name", "branch_type", "direction",
                             "co_occurrence_hours", "avg_ca_mcc_when_binding",
                             "avg_abs_sp", "total_abs_sp"]]
                .rename(columns={
                    "constraint_name":        "Constraint",
                    "branch_type":            "Type",
                    "direction":              "From → To",
                    "co_occurrence_hours":    "Co-occ. Hrs",
                    "avg_ca_mcc_when_binding":"Avg CA MCC",
                    "avg_abs_sp":             "Avg |SP|",
                    "total_abs_sp":           "Σ|SP|",
                }),
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Avg CA MCC": st.column_config.NumberColumn(format="$%.2f"),
                    "Avg |SP|":   st.column_config.NumberColumn(format="$%.2f"),
                    "Σ|SP|":      st.column_config.NumberColumn(format="$%.0f"),
                },
                height=max(300, len(in_terr_df) * 28 + 80),
            )

    st.divider()

    # ── B: All correlated — for context (shows how much is external) ─────────
    st.markdown("#### 🌐 All Correlated Constraints (Context)")
    st.caption(
        "Shows the full picture of what constrains this territory during high-MCC hours. "
        "**Red = in-territory**, blue = external. A dominance of blue means congestion "
        "is primarily imported from outside."
    )

    top_n_c = st.slider("Show top N constraints", 5, 100, 30, key="top_n_constraints")
    corr = load_correlated_constraints(sel, sel_rto, top_n_c)

    if not corr.empty:
        corr = corr.copy()
        corr["direction"] = (
            corr["from_ca"].fillna("?").astype(str)
            + " → "
            + corr["to_ca"].fillna("?").astype(str)
        )

        col_c, col_d = st.columns([3, 2])

        with col_c:
            corr_sorted = corr.sort_values("co_occurrence_hours").copy()
            bar_colors = np.where(
                corr_sorted["in_territory"].fillna(False).astype(bool),
                "#d62728", "#aec7e8"
            ).tolist()
            fig_corr = go.Figure()
            fig_corr.add_trace(go.Bar(
                x=corr_sorted["co_occurrence_hours"].tolist(),
                y=corr_sorted["constraint_name"].tolist(),
                orientation="h",
                marker_color=bar_colors,
                customdata=list(zip(
                    corr_sorted["direction"],
                    corr_sorted["branch_type"].astype(str),
                    corr_sorted["avg_ca_mcc_when_binding"],
                    corr_sorted["avg_abs_sp"],
                )),
                hovertemplate=(
                    "<b>%{y}</b><br>"
                    "Direction: %{customdata[0]}<br>"
                    "Type: %{customdata[1]}<br>"
                    "Co-occurrence Hours: %{x:,}<br>"
                    "Avg CA MCC when binding: $%{customdata[2]:.2f}/MWh<br>"
                    "Avg |SP|: $%{customdata[3]:.2f}<extra></extra>"
                ),
                showlegend=False,
            ))
            for _lbl, _col in [("In Territory", "#d62728"), ("External", "#aec7e8")]:
                fig_corr.add_trace(go.Bar(
                    x=[None], y=[None], name=_lbl,
                    marker_color=_col, showlegend=True,
                ))
            fig_corr.update_layout(
                barmode="overlay",
                xaxis_title="Co-occurrence Hours",
                yaxis_title="",
                legend=dict(orientation="h", y=1.05, x=0),
                margin=dict(l=0, r=0, t=30, b=20),
                yaxis=dict(tickfont=dict(size=10)),
                height=max(350, top_n_c * 26),
            )
            st.plotly_chart(fig_corr, use_container_width=True)

        with col_d:
            table_corr = corr.sort_values("co_occurrence_hours", ascending=False).copy()
            table_corr["in_territory"] = table_corr["in_territory"].map(
                {True: "✓", False: ""}
            )
            st.dataframe(
                table_corr[["constraint_name", "in_territory", "branch_type",
                             "direction", "co_occurrence_hours",
                             "avg_ca_mcc_when_binding", "avg_abs_sp", "total_abs_sp"]]
                .rename(columns={
                    "constraint_name":        "Constraint",
                    "in_territory":           "In Terr.",
                    "branch_type":            "Type",
                    "direction":              "From → To",
                    "co_occurrence_hours":    "Co-occ. Hrs",
                    "avg_ca_mcc_when_binding":"Avg MCC",
                    "avg_abs_sp":             "Avg |SP|",
                    "total_abs_sp":           "Σ|SP|",
                }),
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Avg MCC": st.column_config.NumberColumn(format="$%.2f"),
                    "Avg |SP|":st.column_config.NumberColumn(format="$%.2f"),
                    "Σ|SP|":   st.column_config.NumberColumn(format="$%.0f"),
                },
                height=max(350, top_n_c * 26),
            )

    # ── Constraint seasonality ────────────────────────────────────────────────
    st.subheader("Seasonality of Top Constraints")
    c_monthly = load_constraint_monthly(sel, sel_rto)
    if not c_monthly.empty:
        c_monthly["month_str"] = c_monthly["month"].astype(str).str[:7]
        c_monthly["short_name"] = c_monthly["constraint_name"].apply(
            lambda x: str(x)[:45]
        )
        fig_season = go.Figure()
        for _name, _grp in c_monthly.groupby("short_name", sort=False):
            _name = str(_name)
            fig_season.add_trace(go.Scatter(
                x=_grp["month_str"].tolist(),
                y=_grp["binding_hours"].tolist(),
                name=_name,
                mode="lines+markers",
                hovertemplate=(
                    f"<b>{_name}</b><br>"
                    "Month: %{x}<br>"
                    "Binding Hours: %{y:,}<extra></extra>"
                ),
            ))
        fig_season.update_layout(
            xaxis_title="Month",
            yaxis_title="Binding Hours",
            legend=dict(orientation="h", y=-0.25, xanchor="left", x=0,
                        font=dict(size=10)),
            margin=dict(l=0, r=0, t=10, b=100),
            height=380,
        )
        st.plotly_chart(fig_season, use_container_width=True)


# ── Navigation ────────────────────────────────────────────────────────────────

PAGES = {
    "⚡ Screener":   page_screener,
    "🔍 Deep Dive": page_deep_dive,
}

with st.sidebar:
    st.title("⚡ RTO Congestion")
    page = st.radio("Navigation", list(PAGES.keys()), label_visibility="collapsed")
    st.divider()

PAGES[page]()
