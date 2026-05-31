"""MISO Congestion Analysis Tool — Streamlit app."""

import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from pathlib import Path

DB_PATH = Path(__file__).parent / "miso_2025.duckdb"

LRZ_LABELS = {
    1: "LRZ 1 — MN/Dakotas",
    2: "LRZ 2 — Wisconsin/UP Michigan",
    3: "LRZ 3 — Iowa/Western",
    4: "LRZ 4 — Illinois/Central",
    5: "LRZ 5 — Missouri",
    6: "LRZ 6 — Indiana/Kentucky",
    7: "LRZ 7 — Michigan",
    8: "LRZ 8 — Arkansas",
    9: "LRZ 9 — Louisiana",
    10: "LRZ 10 — Mississippi",
}

LRZ_COLORS = {
    1: "#1f77b4", 2: "#2ca02c", 3: "#d62728", 4: "#9467bd",
    5: "#8c564b", 6: "#e377c2", 7: "#7f7f7f", 8: "#bcbd22",
    9: "#17becf", 10: "#ff7f0e",
}

# Full label → color map (must be filtered to present categories before passing to Plotly)
FULL_COLOR_MAP = {v: LRZ_COLORS[k] for k, v in LRZ_LABELS.items()} | {"Non-LBA": "#aaaaaa"}


def color_map_for(series: "pd.Series") -> dict:
    """Return color map filtered to only categories present in `series`.

    Converts to Python str to avoid Arrow-backed string scalar mismatches.
    """
    present = {str(x) for x in series.dropna().unique()}
    return {k: v for k, v in FULL_COLOR_MAP.items() if k in present}

st.set_page_config(
    page_title="MISO Congestion Tool",
    page_icon="⚡",
    layout="wide",
)


@st.cache_resource
def get_connection():
    return duckdb.connect(str(DB_PATH), read_only=True)


@st.cache_data
def load_screener() -> pd.DataFrame:
    con = get_connection()
    df = con.execute("""
        SELECT
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
            top_corr_constraint
        FROM screener
        WHERE avg_mcc IS NOT NULL
    """).df()
    # Convert to plain Python str (not Arrow-backed) so Plotly groupby works correctly
    df["lrz_label"] = df["lrz"].map(LRZ_LABELS).fillna("Non-LBA").astype(object)
    df["lrz_color"] = df["lrz"].map(LRZ_COLORS).fillna("#aaaaaa").astype(object)
    return df


@st.cache_data
def load_monthly(utility_ca: str) -> pd.DataFrame:
    con = get_connection()
    return con.execute("""
        SELECT month, component, avg_value, avg_positive, sum_positive,
               pct_above_5, pct_above_10, max_value, p90_value
        FROM ca_lmp_monthly
        WHERE utility_ca = ?
        ORDER BY month, component
    """, [utility_ca]).df()


@st.cache_data
def load_hourly_heatmap(utility_ca: str) -> pd.DataFrame:
    con = get_connection()
    return con.execute("""
        SELECT cal_month, hour_ending, avg_value
        FROM ca_lmp_by_hour
        WHERE utility_ca = ? AND component = 'MCC'
        ORDER BY cal_month, hour_ending
    """, [utility_ca]).df()


@st.cache_data
def load_correlated_constraints(utility_ca: str, top_n: int = 50) -> pd.DataFrame:
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
            (from_ca = ? OR to_ca = ?)   AS in_territory
        FROM mcc_constraint_correlation
        WHERE utility_ca = ?
        ORDER BY co_occurrence_hours DESC
        LIMIT ?
    """, [utility_ca, utility_ca, utility_ca, top_n]).df()


@st.cache_data
def load_in_territory_constraints(utility_ca: str) -> pd.DataFrame:
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
        WHERE utility_ca = ?
          AND (from_ca = ? OR to_ca = ?)
        ORDER BY co_occurrence_hours DESC
    """, [utility_ca, utility_ca, utility_ca]).df()


@st.cache_data
def load_constraint_monthly(utility_ca: str) -> pd.DataFrame:
    """Monthly binding hours for top constraints correlated with a CA."""
    con = get_connection()
    return con.execute("""
        WITH top_constraints AS (
            SELECT constraint_id
            FROM mcc_constraint_correlation
            WHERE utility_ca = ?
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
        GROUP BY month, bc.constraint_id
        ORDER BY month, binding_hours DESC
    """, [utility_ca]).df()


# ── Page: Screener ────────────────────────────────────────────────────────────

def page_screener():
    st.title("⚡ MISO Congestion Screener")
    st.caption("2025 Day-Ahead Market · Load zones only · MCC = Marginal Congestion Component")

    df = load_screener()

    # ── Sidebar filters ───────────────────────────────────────────────────────
    with st.sidebar:
        st.header("Filters")

        all_lrz = sorted(df["lrz"].dropna().unique().astype(int).tolist())
        sel_lrz = st.multiselect(
            "Local Resource Zone",
            options=all_lrz,
            format_func=lambda x: LRZ_LABELS.get(x, str(x)),
            default=all_lrz,
        )
        include_non_lba = st.checkbox("Include non-LBA entries", value=True)

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
        top_n = st.slider("Show top N utilities", 5, 44, 20)

    # ── Filter data ───────────────────────────────────────────────────────────
    mask = df["n_load_zones"] >= min_zones
    if sel_lrz:
        lrz_mask = df["lrz"].isin(sel_lrz)
        if include_non_lba:
            lrz_mask = lrz_mask | df["lrz"].isna()
        mask = mask & lrz_mask

    filtered = df[mask].copy()
    filtered = filtered.sort_values(metric_col, ascending=False).head(top_n)

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

    # ── Bar chart ─────────────────────────────────────────────────────────────
    # Use go.Figure directly — px color_discrete_map has a groupby bug with
    # Arrow-backed string columns in this version of Plotly.
    chart_df = filtered.sort_values(metric_col, ascending=True).copy()
    chart_df["label"] = (chart_df["utility_ca"] + "  " + chart_df["utility_name"]).astype(str)

    fig_bar = go.Figure()
    seen_lrz = set()
    for _, row in chart_df.iterrows():
        lrz_lbl = str(row["lrz_label"])
        color = FULL_COLOR_MAP.get(lrz_lbl, "#aaaaaa")
        fig_bar.add_trace(go.Bar(
            name=lrz_lbl,
            x=[row[metric_col]],
            y=[row["label"]],
            orientation="h",
            marker_color=color,
            showlegend=lrz_lbl not in seen_lrz,
            hovertemplate=(
                f"<b>{row['utility_ca']}</b> — {row['utility_name']}<br>"
                f"{metric_label}: %{{x:.2f}}<extra></extra>"
            ),
        ))
        seen_lrz.add(lrz_lbl)

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

    # ── Scatter: in-territory LINE hours vs MCC intensity ────────────────────
    st.subheader("Congestion: In-Territory Line Constraints vs MCC Intensity")
    st.caption(
        "X = hours when an in-territory **line** (LN) was binding during high-MCC conditions. "
        "Bubble size = number of distinct in-territory lines binding. "
        "Top-right utilities have both high congestion costs AND their own lines as bottlenecks."
    )

    full_filtered = df[df["n_load_zones"] >= min_zones].copy()
    if sel_lrz:
        lrz_mask2 = full_filtered["lrz"].isin(sel_lrz)
        if include_non_lba:
            lrz_mask2 = lrz_mask2 | full_filtered["lrz"].isna()
        full_filtered = full_filtered[lrz_mask2]

    full_filtered = full_filtered.copy()
    max_ln = full_filtered["corr_in_territory_ln_constraints"].max()
    full_filtered["bubble_size"] = (
        full_filtered["corr_in_territory_ln_constraints"] / max_ln * 40 + 6
    ).clip(lower=6)

    fig_scatter = go.Figure()
    seen_lrz2: set = set()
    for lrz_lbl, grp in full_filtered.groupby("lrz_label", sort=False):
        lrz_lbl = str(lrz_lbl)
        color = FULL_COLOR_MAP.get(lrz_lbl, "#aaaaaa")
        fig_scatter.add_trace(go.Scatter(
            name=lrz_lbl,
            x=grp["corr_in_territory_ln_hours"].tolist(),
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
            )),
            hovertemplate=(
                "<b>%{text}</b> — %{customdata[0]}<br>"
                "In-Territory Line Hours: %{x:,}<br>"
                "Avg MCC: $%{y:.2f}/MWh<br>"
                "In-Territory Lines: %{customdata[1]:,}<extra></extra>"
            ),
            showlegend=lrz_lbl not in seen_lrz2,
        ))
        seen_lrz2.add(lrz_lbl)

    fig_scatter.update_layout(
        xaxis_title="In-Territory Line Correlated Binding Hours",
        yaxis_title="Avg MCC ($/MWh)",
        legend=dict(orientation="h", yanchor="bottom", y=1.01, xanchor="left", x=0),
        margin=dict(l=0, r=20, t=20, b=40),
        height=480,
    )
    st.plotly_chart(fig_scatter, use_container_width=True)

    # ── Data table ────────────────────────────────────────────────────────────
    st.subheader("Full Data")
    display_cols = {
        "lrz":                         "LRZ",
        "utility_ca":                  "CA",
        "utility_name":                "Utility",
        "n_load_zones":                "# Zones",
        "avg_mcc":                     "Avg MCC",
        "avg_positive_mcc":            "Avg Pos. MCC",
        "pct_hours_above_5":           "% Hrs > $5",
        "sum_positive_mcc":            "Σ Pos. MCC",
        "corr_in_territory_constraints": "In-Terr. Constraints",
        "corr_in_territory_hours":     "In-Terr. Hours",
        "owned_total_abs_sp":          "Owned Σ|SP|",
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
            "Owned Σ|SP|":          st.column_config.NumberColumn(format="$%.0f"),
            "In-Terr. Constraints": st.column_config.NumberColumn(format="%d"),
            "In-Terr. Hours":       st.column_config.NumberColumn(format="%d"),
            "All Corr. Hours":      st.column_config.NumberColumn(format="%d"),
        },
    )

    csv = table_df.to_csv(index=False).encode()
    st.download_button("Download CSV", csv, "miso_congestion_screener.csv", "text/csv")


# ── Page: Deep Dive ───────────────────────────────────────────────────────────

def page_deep_dive():
    st.title("🔍 Utility Deep Dive")
    st.caption("2025 Day-Ahead Market · Load zones only")

    df = load_screener()

    with st.sidebar:
        st.header("Select Utility")
        options = df.sort_values("avg_mcc", ascending=False)
        sel = st.selectbox(
            "Utility",
            options=options["utility_ca"].tolist(),
            format_func=lambda ca: f"{ca} — {options.set_index('utility_ca').loc[ca, 'utility_name']}",
        )

    row = df.set_index("utility_ca").loc[sel]

    # ── Header KPIs ───────────────────────────────────────────────────────────
    st.subheader(f"{sel} — {row['utility_name']}")
    if pd.notna(row["lrz"]):
        st.caption(LRZ_LABELS.get(int(row["lrz"]), ""))

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
    monthly = load_monthly(sel)
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
                "Avg MCC":   st.column_config.NumberColumn(format="$%.2f"),
                "P90 MCC":   st.column_config.NumberColumn(format="$%.2f"),
                "% > $5":    st.column_config.NumberColumn(format="%.1f%%"),
                "Σ Pos. MCC":st.column_config.NumberColumn(format="$%.0f"),
            },
            height=360,
        )

    # ── Hour-of-day heatmap ───────────────────────────────────────────────────
    st.subheader("Congestion Heatmap — Hour of Day × Month")
    heatmap_df = load_hourly_heatmap(sel)
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

    in_terr_df = load_in_territory_constraints(sel)

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
                if _lbl.split(" ")[0] in it_sorted["branch_type"].astype(str).values:
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
    corr = load_correlated_constraints(sel, top_n_c)

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
            bar_colors = [
                "#d62728" if t else "#aec7e8"
                for t in corr_sorted["in_territory"]
            ]
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
    c_monthly = load_constraint_monthly(sel)
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
    "⚡ Screener":    page_screener,
    "🔍 Deep Dive":  page_deep_dive,
}

with st.sidebar:
    st.image("https://www.misoenergy.org/contentassets/"
             "2e44b7c8e3484b3d98c64d86d0048a8f/miso_logo.png",
             width=140)
    st.title("MISO Congestion")
    page = st.radio("Navigation", list(PAGES.keys()), label_visibility="collapsed")
    st.divider()

PAGES[page]()
