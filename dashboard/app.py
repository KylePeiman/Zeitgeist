"""
app.py — Zeitgeist Sentiment Dashboard

Reads from SQLite in real time. Auto-refreshes every 30 seconds.
Shows leaderboards, sentiment trends, and entity-level breakdowns.
"""

import os
import sqlite3
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

load_dotenv()

SQLITE_DB_PATH = os.getenv("SQLITE_DB_PATH", "./data/zeitgeist.db")
REFRESH_INTERVAL_MS = 30_000

# ── PAGE CONFIG ───────────────────────────────────────────────
st.set_page_config(
    page_title="Zeitgeist",
    page_icon="Z",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── AUTO-REFRESH ──────────────────────────────────────────────
try:
    from streamlit_autorefresh import st_autorefresh
    st_autorefresh(interval=REFRESH_INTERVAL_MS, key="auto_refresh")
except ImportError:
    pass  # Falls back to manual refresh button

# ── STYLES ────────────────────────────────────────────────────
st.markdown("""
<style>
    .main { background-color: #0e0e0e; }
    .stMetric { background: #1a1a1a; border-radius: 8px; padding: 12px; }
    .sentiment-positive { color: #00c853; font-weight: bold; }
    .sentiment-negative { color: #ff1744; font-weight: bold; }
    .sentiment-neutral  { color: #9e9e9e; }
    h1 { color: #ffffff; }
    h2, h3 { color: #e0e0e0; }
    .leaderboard-row { padding: 4px 0; border-bottom: 1px solid #2a2a2a; }
</style>
""", unsafe_allow_html=True)


# ── DATA LOADING ──────────────────────────────────────────────
@st.cache_data(ttl=30)
def load_all_data(db_path: str) -> pd.DataFrame:
    """Load all sentiment scores joined with entity metadata."""
    if not os.path.exists(db_path):
        return pd.DataFrame()
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query("""
        SELECT
            s.id,
            e.name,
            e.category,
            e.entity_type,
            s.timestamp,
            s.sentiment,
            s.sentiment_score,
            s.confidence,
            s.intensity,
            s.mention_count,
            s.engagement_score,
            s.source,
            s.reasoning,
            s.sample_size
        FROM sentiment_scores s
        JOIN entities e ON s.entity_id = e.id
        ORDER BY s.timestamp DESC
    """, conn)
    conn.close()
    if df.empty:
        return df
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    return df


@st.cache_data(ttl=30)
def get_latest_per_entity(df: pd.DataFrame) -> pd.DataFrame:
    """Most recent score per entity."""
    if df.empty:
        return df
    return df.sort_values("timestamp").groupby("name").last().reset_index()


def sentiment_color(score: float) -> str:
    if score > 0.2:
        return "#00c853"
    elif score < -0.2:
        return "#ff1744"
    return "#9e9e9e"


def sentiment_emoji(sentiment: str) -> str:
    return {"positive": "💚", "negative": "❤️", "neutral": "⬜"}.get(sentiment, "")


# ── SIDEBAR ───────────────────────────────────────────────────
with st.sidebar:
    st.title("Zeitgeist")
    st.caption("Real-time public sentiment pipeline")
    st.divider()

    df_raw = load_all_data(SQLITE_DB_PATH)

    if df_raw.empty:
        st.warning("No data yet. Make sure the pipeline is running.")
        st.stop()

    categories = sorted(df_raw["category"].dropna().unique().tolist())
    selected_categories = st.multiselect(
        "Filter by Category",
        options=categories,
        default=categories,
    )

    sources = sorted(df_raw["source"].dropna().unique().tolist())
    selected_sources = st.multiselect(
        "Filter by Source",
        options=sources,
        default=sources,
    )

    hours_back = st.slider("Time window (hours)", min_value=1, max_value=48, value=24)

    st.divider()
    cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(hours=hours_back)
    total_entities = df_raw["name"].nunique()
    total_scores = len(df_raw)
    st.metric("Entities tracked", total_entities)
    st.metric("Score records", total_scores)
    latest_ts = df_raw["timestamp"].max()
    st.caption(f"Last updated: {latest_ts.strftime('%H:%M:%S UTC') if pd.notna(latest_ts) else 'N/A'}")

    if st.button("Force refresh"):
        st.cache_data.clear()
        st.rerun()


# ── FILTER DATA ───────────────────────────────────────────────
df = df_raw[
    (df_raw["category"].isin(selected_categories)) &
    (df_raw["source"].isin(selected_sources)) &
    (df_raw["timestamp"] >= cutoff)
].copy()

latest = get_latest_per_entity(df)

_MENTION_PRIOR = 10
if not latest.empty:
    latest = latest.copy()
    latest["adjusted_score"] = (
        latest["sentiment_score"] * latest["mention_count"]
        / (latest["mention_count"] + _MENTION_PRIOR)
    )

# ── HEADER ────────────────────────────────────────────────────
st.title("Zeitgeist — Public Sentiment Dashboard")
st.caption(f"Auto-refreshes every 30s | Showing last {hours_back}h | {len(df)} data points")

# ── TOP METRICS ROW ───────────────────────────────────────────
if not latest.empty:
    col1, col2, col3, col4 = st.columns(4)

    most_loved = latest.loc[latest["adjusted_score"].idxmax()]
    most_hated = latest.loc[latest["adjusted_score"].idxmin()]
    avg_score = latest["sentiment_score"].mean()
    pos_pct = (latest["sentiment"] == "positive").mean() * 100

    with col1:
        st.metric(
            "Most Loved",
            most_loved["name"],
            f"{most_loved['sentiment_score']:+.2f}",
            delta_color="normal",
        )
    with col2:
        st.metric(
            "Most Hated",
            most_hated["name"],
            f"{most_hated['sentiment_score']:+.2f}",
            delta_color="inverse",
        )
    with col3:
        st.metric("Avg Sentiment", f"{avg_score:+.3f}")
    with col4:
        st.metric("% Positive", f"{pos_pct:.0f}%")

st.divider()

# ── LEADERBOARDS ──────────────────────────────────────────────
col_love, col_hate = st.columns(2)

with col_love:
    st.subheader("Most Loved")
    if not latest.empty:
        top_loved = latest.nlargest(10, "adjusted_score")[
            ["name", "category", "sentiment_score", "adjusted_score", "confidence", "mention_count"]
        ]
        for _, row in top_loved.iterrows():
            color = sentiment_color(row["sentiment_score"])
            st.markdown(
                f"<div class='leaderboard-row'>"
                f"<span style='color:{color}'>●</span> "
                f"<b>{row['name']}</b> "
                f"<span style='color:{color}'>{row['sentiment_score']:+.3f}</span> "
                f"<span style='color:#666; font-size:0.85em'>[{row['category']}] "
                f"{row['mention_count']} mentions</span>"
                f"</div>",
                unsafe_allow_html=True,
            )

with col_hate:
    st.subheader("Most Hated")
    if not latest.empty:
        top_hated = latest.nsmallest(10, "adjusted_score")[
            ["name", "category", "sentiment_score", "adjusted_score", "confidence", "mention_count"]
        ]
        for _, row in top_hated.iterrows():
            color = sentiment_color(row["sentiment_score"])
            st.markdown(
                f"<div class='leaderboard-row'>"
                f"<span style='color:{color}'>●</span> "
                f"<b>{row['name']}</b> "
                f"<span style='color:{color}'>{row['sentiment_score']:+.3f}</span> "
                f"<span style='color:#666; font-size:0.85em'>[{row['category']}] "
                f"{row['mention_count']} mentions</span>"
                f"</div>",
                unsafe_allow_html=True,
            )

st.divider()

# ── SENTIMENT OVERVIEW CHART ──────────────────────────────────
st.subheader("Sentiment Score Overview")
if not latest.empty:
    chart_df = latest.sort_values("sentiment_score", ascending=True).head(30)
    colors = [sentiment_color(s) for s in chart_df["sentiment_score"]]

    fig = go.Figure(go.Bar(
        x=chart_df["sentiment_score"],
        y=chart_df["name"],
        orientation="h",
        marker_color=colors,
        text=[f"{s:+.2f}" for s in chart_df["sentiment_score"]],
        textposition="outside",
        hovertemplate="<b>%{y}</b><br>Score: %{x:.3f}<extra></extra>",
    ))
    fig.update_layout(
        plot_bgcolor="#111111",
        paper_bgcolor="#111111",
        font_color="#e0e0e0",
        height=600,
        xaxis=dict(
            range=[-1.1, 1.1],
            zeroline=True,
            zerolinecolor="#555",
            gridcolor="#2a2a2a",
        ),
        yaxis=dict(gridcolor="#2a2a2a"),
        margin=dict(l=10, r=60, t=10, b=10),
    )
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# ── TREND CHART ───────────────────────────────────────────────
st.subheader("Sentiment Trend Over Time")

if not df.empty:
    entity_options = sorted(df["name"].unique().tolist())
    default_entities = entity_options[:5] if len(entity_options) >= 5 else entity_options

    selected_entities = st.multiselect(
        "Select entities to compare",
        options=entity_options,
        default=default_entities,
    )

    if selected_entities:
        trend_df = df[df["name"].isin(selected_entities)].copy()
        trend_df = trend_df.sort_values("timestamp")

        fig2 = go.Figure()
        for entity in selected_entities:
            edf = trend_df[trend_df["name"] == entity]
            if edf.empty:
                continue
            color = sentiment_color(edf["sentiment_score"].mean())
            fig2.add_trace(go.Scatter(
                x=edf["timestamp"],
                y=edf["sentiment_score"],
                mode="lines+markers",
                name=entity,
                line=dict(width=2),
                marker=dict(size=5),
                hovertemplate=(
                    f"<b>{entity}</b><br>"
                    "Score: %{y:.3f}<br>"
                    "Time: %{x}<extra></extra>"
                ),
            ))

        fig2.add_hline(y=0, line_dash="dash", line_color="#555", line_width=1)
        fig2.add_hrect(y0=0.2, y1=1.0, fillcolor="#00c853", opacity=0.05, line_width=0)
        fig2.add_hrect(y0=-1.0, y1=-0.2, fillcolor="#ff1744", opacity=0.05, line_width=0)

        fig2.update_layout(
            plot_bgcolor="#111111",
            paper_bgcolor="#111111",
            font_color="#e0e0e0",
            height=400,
            xaxis=dict(gridcolor="#2a2a2a"),
            yaxis=dict(
                range=[-1.1, 1.1],
                gridcolor="#2a2a2a",
                zeroline=True,
                zerolinecolor="#555",
            ),
            legend=dict(bgcolor="#1a1a1a"),
            margin=dict(l=10, r=10, t=10, b=10),
        )
        st.plotly_chart(fig2, use_container_width=True)

st.divider()

# ── FULL DATA TABLE ───────────────────────────────────────────
st.subheader("All Entities — Latest Scores")

if not latest.empty:
    display_df = latest[[
        "name", "category", "entity_type", "sentiment", "sentiment_score",
        "confidence", "mention_count", "engagement_score", "source", "timestamp"
    ]].copy()

    display_df["timestamp"] = display_df["timestamp"].dt.strftime("%H:%M:%S UTC")
    display_df["sentiment_score"] = display_df["sentiment_score"].round(3)
    display_df["confidence"] = display_df["confidence"].round(2)

    def color_sentiment_score(val):
        if isinstance(val, (int, float)):
            if val > 0.2:
                return "color: #00c853"
            elif val < -0.2:
                return "color: #ff1744"
            return "color: #9e9e9e"
        return ""

    styled = display_df.style.map(color_sentiment_score, subset=["sentiment_score"])
    st.dataframe(styled, use_container_width=True, height=500)
