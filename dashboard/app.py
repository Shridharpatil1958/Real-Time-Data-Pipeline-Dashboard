#!/usr/bin/env python3
# dashboard/app.py
# Real-time Streamlit dashboard for the transaction pipeline

from __future__ import annotations

import sys
import time
from datetime import datetime

sys.path.insert(0, ".")

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ─────────────────────────────────────────────
# Page config (MUST be first Streamlit call)
# ─────────────────────────────────────────────

st.set_page_config(
    page_title="⚡ Real-Time Pipeline Dashboard",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────
# Deferred imports (depend on page config)
# ─────────────────────────────────────────────

from config.settings import DASHBOARD_REFRESH_INTERVAL
from database.mysql_writer import MySQLWriter
from database.mongo_writer import MongoWriter
from database.redis_cache import RedisCache

# ─────────────────────────────────────────────
# Custom CSS
# ─────────────────────────────────────────────

st.markdown("""
<style>
    /* Dark theme overrides */
    .stApp { background-color: #0e1117; }
    .metric-card {
        background: linear-gradient(135deg, #1a1f2e, #252b3b);
        border: 1px solid #2d3748;
        border-radius: 12px;
        padding: 1.2rem 1.5rem;
        text-align: center;
    }
    .metric-value { font-size: 2rem; font-weight: 700; color: #63b3ed; }
    .metric-label { font-size: 0.8rem; color: #718096; text-transform: uppercase; letter-spacing: 0.05em; }
    .anomaly-badge {
        background: linear-gradient(135deg, #c53030, #9b2c2c);
        border-radius: 8px;
        padding: 0.2rem 0.6rem;
        font-size: 0.75rem;
        font-weight: bold;
        color: white;
    }
    .alert-row { border-left: 4px solid #fc8181; padding-left: 8px; margin: 4px 0; }
    div[data-testid="stMetric"] { background: #1a1f2e; border-radius: 8px; padding: 12px; }
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────
# DB connections (cached)
# ─────────────────────────────────────────────

@st.cache_resource
def get_mysql():
    return MySQLWriter()

@st.cache_resource
def get_mongo():
    return MongoWriter()

@st.cache_resource
def get_redis():
    return RedisCache()


# ─────────────────────────────────────────────
# Data loaders
# ─────────────────────────────────────────────

@st.cache_data(ttl=DASHBOARD_REFRESH_INTERVAL)
def load_metrics():
    return get_mysql().fetch_metrics()

@st.cache_data(ttl=DASHBOARD_REFRESH_INTERVAL)
def load_timeseries():
    return get_mysql().fetch_timeseries(minutes=30)

@st.cache_data(ttl=DASHBOARD_REFRESH_INTERVAL)
def load_categories():
    return get_mysql().fetch_category_breakdown()

@st.cache_data(ttl=DASHBOARD_REFRESH_INTERVAL)
def load_recent_txns():
    return get_mongo().fetch_recent_transactions(limit=100)

@st.cache_data(ttl=DASHBOARD_REFRESH_INTERVAL)
def load_alerts():
    return get_mongo().fetch_recent_alerts(limit=20)

@st.cache_data(ttl=DASHBOARD_REFRESH_INTERVAL)
def load_locations():
    return get_mongo().fetch_location_stats(limit=12)

@st.cache_data(ttl=DASHBOARD_REFRESH_INTERVAL)
def load_redis_snapshot():
    return get_redis().get_recent_transactions(limit=200)


# ─────────────────────────────────────────────
# Colour helpers
# ─────────────────────────────────────────────

RISK_COLORS = {"low": "#48bb78", "medium": "#f6ad55", "high": "#fc8181", "critical": "#e53e3e"}
TYPE_COLORS = {
    "purchase": "#63b3ed", "transfer": "#9f7aea",
    "withdrawal": "#fc8181", "deposit": "#48bb78", "refund": "#f6e05e",
}


# ─────────────────────────────────────────────
# Sidebar
# ─────────────────────────────────────────────

with st.sidebar:
    st.title("⚡ Pipeline Control")
    st.markdown("---")
    auto_refresh = st.checkbox("Auto-refresh", value=True)
    refresh_sec  = st.slider("Refresh interval (s)", 1, 30, DASHBOARD_REFRESH_INTERVAL)
    st.markdown("---")
    st.markdown("**Data Sources**")
    st.success("🟢 Kafka")
    st.success("🟢 MySQL")
    st.success("🟢 MongoDB")
    st.success("🟢 Redis")
    st.markdown("---")
    st.markdown(f"**Last refresh:** {datetime.now().strftime('%H:%M:%S')}")
    if st.button("🔄 Force Refresh"):
        st.cache_data.clear()
        st.rerun()


# ─────────────────────────────────────────────
# Header
# ─────────────────────────────────────────────

st.title("⚡ Real-Time Transaction Pipeline")
st.caption("Live monitoring · Anomaly Detection · Financial Intelligence")

# ─────────────────────────────────────────────
# KPI Row
# ─────────────────────────────────────────────

metrics = load_metrics()
total   = int(metrics.get("total_txns", 0) or 0)
amount  = float(metrics.get("total_amount", 0) or 0)
anom    = int(metrics.get("anomaly_count", 0) or 0)
avg_amt = float(metrics.get("avg_amount", 0) or 0)
max_amt = float(metrics.get("max_amount", 0) or 0)
anom_rate = (anom / total * 100) if total else 0

col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("📊 Transactions (1h)", f"{total:,}")
col2.metric("💰 Volume (1h)",        f"${amount:,.0f}")
col3.metric("🚨 Anomalies (1h)",     f"{anom:,}", delta=f"{anom_rate:.1f}% rate", delta_color="inverse")
col4.metric("📈 Avg Amount",         f"${avg_amt:,.2f}")
col5.metric("🔝 Max Amount",         f"${max_amt:,.2f}")

st.divider()

# ─────────────────────────────────────────────
# Row 2 — Timeseries + Categories
# ─────────────────────────────────────────────

col_left, col_right = st.columns([2, 1])

with col_left:
    st.subheader("📉 Transaction Volume (last 30 min)")
    ts_data = load_timeseries()
    if ts_data:
        df_ts = pd.DataFrame(ts_data)
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df_ts["minute_bucket"], y=df_ts["txn_count"],
            name="Total Txns", mode="lines+markers",
            line=dict(color="#63b3ed", width=2),
            fill="tozeroy", fillcolor="rgba(99,179,237,0.1)",
        ))
        fig.add_trace(go.Scatter(
            x=df_ts["minute_bucket"], y=df_ts["anomaly_count"],
            name="Anomalies", mode="lines+markers",
            line=dict(color="#fc8181", width=2, dash="dot"),
        ))
        fig.update_layout(
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            font_color="#a0aec0", legend=dict(bgcolor="rgba(0,0,0,0)"),
            margin=dict(l=10, r=10, t=10, b=10),
            xaxis=dict(showgrid=False), yaxis=dict(showgrid=True, gridcolor="#2d3748"),
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Waiting for data …")

with col_right:
    st.subheader("🏪 Top Categories")
    cat_data = load_categories()
    if cat_data:
        df_cat = pd.DataFrame(cat_data)
        fig = px.bar(
            df_cat, x="cnt", y="merchant_category",
            orientation="h", color="cnt",
            color_continuous_scale="Blues",
        )
        fig.update_layout(
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            font_color="#a0aec0", coloraxis_showscale=False,
            margin=dict(l=10, r=10, t=10, b=10),
            xaxis=dict(showgrid=False), yaxis=dict(showgrid=False, title=""),
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Waiting for data …")

st.divider()

# ─────────────────────────────────────────────
# Row 3 — Recent txns + Alerts
# ─────────────────────────────────────────────

col_txn, col_alert = st.columns([3, 2])

with col_txn:
    st.subheader("📋 Recent Transactions")
    recent = load_recent_txns()
    if recent:
        df_r = pd.DataFrame(recent)[[
            "transaction_id", "user_id", "amount", "transaction_type",
            "merchant", "status", "risk_level", "timestamp",
        ]].copy()
        df_r["transaction_id"] = df_r["transaction_id"].str[:8] + "…"
        df_r["user_id"]        = df_r["user_id"].str[:8] + "…"
        df_r["amount"]         = df_r["amount"].apply(lambda x: f"${x:,.2f}")

        def color_risk(val):
            c = RISK_COLORS.get(val, "#a0aec0")
            return f"color: {c}; font-weight: bold"

        st.dataframe(
            df_r.style.applymap(color_risk, subset=["risk_level"]),
            use_container_width=True, height=350,
        )
    else:
        st.info("Waiting for transactions …")

with col_alert:
    st.subheader("🚨 Recent Alerts")
    alerts = load_alerts()
    if alerts:
        for alert in alerts[:10]:
            risk  = alert.get("risk_level", "low")
            color = RISK_COLORS.get(risk, "#a0aec0")
            amt   = alert.get("amount", 0)
            uid   = str(alert.get("user_id", ""))[:8]
            reasons = alert.get("reasons", [])
            with st.expander(
                f"{'🔴' if risk in ('high','critical') else '🟡'} "
                f"${amt:,.2f} — User {uid}… [{risk.upper()}]"
            ):
                for r in reasons:
                    st.markdown(f"• {r}")
    else:
        st.info("No alerts yet — pipeline is clean ✅")

st.divider()

# ─────────────────────────────────────────────
# Row 4 — Geographic + Risk distribution
# ─────────────────────────────────────────────

col_geo, col_pie = st.columns(2)

with col_geo:
    st.subheader("🌍 Transaction Geography")
    loc_data = load_locations()
    if loc_data:
        df_loc = pd.DataFrame(loc_data)
        fig = px.bar(
            df_loc, x="city", y="count", color="country",
            title="Transactions by City",
        )
        fig.update_layout(
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            font_color="#a0aec0", showlegend=False,
            margin=dict(l=10, r=10, t=30, b=10),
            xaxis=dict(showgrid=False), yaxis=dict(showgrid=True, gridcolor="#2d3748"),
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Waiting for data …")

with col_pie:
    st.subheader("⚠️ Risk Distribution")
    recent2 = load_recent_txns()
    if recent2:
        df_risk = pd.DataFrame(recent2)
        risk_counts = df_risk["risk_level"].value_counts().reset_index()
        risk_counts.columns = ["risk_level", "count"]
        fig = px.pie(
            risk_counts, names="risk_level", values="count",
            color="risk_level",
            color_discrete_map=RISK_COLORS,
            hole=0.5,
        )
        fig.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            font_color="#a0aec0",
            margin=dict(l=10, r=10, t=10, b=10),
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Waiting for data …")

# ─────────────────────────────────────────────
# Auto-refresh
# ─────────────────────────────────────────────

if auto_refresh:
    time.sleep(refresh_sec)
    st.cache_data.clear()
    st.rerun()
