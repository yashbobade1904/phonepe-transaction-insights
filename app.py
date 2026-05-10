"""
PhonePe Transaction Insights – Streamlit Dashboard
Run: streamlit run app.py
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import mysql.connector

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="PhonePe Transaction Insights",
    page_icon="📱",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ─────────────────────────────────────────────────────────────────
st.markdown("""
<style>
[data-testid="stMetricValue"] { font-size: 1.6rem; color: #5f259f; }
.stTabs [data-baseweb="tab"]  { font-size: 1rem; }
h1 { color: #5f259f; }
</style>
""", unsafe_allow_html=True)

# ── DB connection ──────────────────────────────────────────────────────────────
@st.cache_resource
def get_conn():
    return mysql.connector.connect(
        host="localhost", user="root",
        "password": "your_password", database="phonepe_pulse"
    )

@st.cache_data(ttl=3600)
def load_data():
    conn = get_conn()
    agg_txn  = pd.read_sql("SELECT * FROM aggregated_transaction", conn)
    agg_user = pd.read_sql("SELECT * FROM aggregated_user", conn)
    map_txn  = pd.read_sql("SELECT * FROM map_transaction", conn)
    map_user = pd.read_sql("SELECT * FROM map_user", conn)
    top_txn  = pd.read_sql("SELECT * FROM top_transaction", conn)
    top_user = pd.read_sql("SELECT * FROM top_user", conn)

    # Derived columns
    for df in [agg_txn, map_txn]:
        df["amount_cr"] = df["transaction_amount"] / 1e7
    for df in [agg_txn, agg_user, map_txn, map_user]:
        df["period"] = df["year"].astype(str) + "-Q" + df["quarter"].astype(str)

    return agg_txn, agg_user, map_txn, map_user, top_txn, top_user

try:
    agg_txn, agg_user, map_txn, map_user, top_txn, top_user = load_data()
    DATA_OK = True
except Exception as e:
    DATA_OK = False
    st.error(f"❌ Could not connect to MySQL: {e}")
    st.info("Run `python etl.py` first to populate the database.")
    st.stop()

# ── Sidebar filters ────────────────────────────────────────────────────────────
st.sidebar.image("https://www.logo.wine/a/logo/PhonePe/PhonePe-Logo.wine.svg", width=180)
st.sidebar.title("Filters")

all_years   = sorted(agg_txn["year"].unique())
all_states  = sorted(agg_txn["state"].unique())
all_types   = sorted(agg_txn["transaction_type"].unique())

sel_years  = st.sidebar.multiselect("Year", all_years, default=all_years)
sel_states = st.sidebar.multiselect("State", all_states, default=all_states)
sel_types  = st.sidebar.multiselect("Transaction Type", all_types, default=all_types)

# Filtered datasets
f_txn  = agg_txn[agg_txn["year"].isin(sel_years) &
                  agg_txn["state"].isin(sel_states) &
                  agg_txn["transaction_type"].isin(sel_types)]
f_user = agg_user[agg_user["year"].isin(sel_years) &
                   agg_user["state"].isin(sel_states)]

# ── Header ─────────────────────────────────────────────────────────────────────
st.title("📱 PhonePe Transaction Insights Dashboard")
st.caption("Powered by PhonePe Pulse open data | Built with Streamlit & Plotly")

# ── KPI row ────────────────────────────────────────────────────────────────────
k1, k2, k3, k4 = st.columns(4)
k1.metric("Total Transactions",
          f"{f_txn['transaction_count'].sum()/1e9:.2f}B")
k2.metric("Total Amount",
          f"₹{f_txn['amount_cr'].sum()/100:.2f}T")
k3.metric("Registered Users",
          f"{f_user['registered_users'].sum()/1e6:.1f}M")
k4.metric("App Opens",
          f"{f_user['app_opens'].sum()/1e9:.2f}B")

st.divider()

# ── Tabs ───────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📈 Trends", "🗺️ Geography", "💳 Transaction Types",
    "👥 Users", "🏆 Top Performers"
])

# ── Tab 1 : Trends ─────────────────────────────────────────────────────────────
with tab1:
    st.subheader("Transaction Trends Over Time")
    period_df = (f_txn.groupby("period")
                 .agg(txn_count=("transaction_count","sum"),
                      amount_cr=("amount_cr","sum"))
                 .reset_index())

    c1, c2 = st.columns(2)
    with c1:
        fig = px.line(period_df, x="period", y="txn_count",
                      title="Quarterly Transaction Count",
                      labels={"txn_count":"Transactions","period":"Quarter"},
                      markers=True, color_discrete_sequence=["#5f259f"])
        fig.update_xaxes(tickangle=45)
        st.plotly_chart(fig, use_container_width=True)

    with c2:
        fig = px.area(period_df, x="period", y="amount_cr",
                      title="Quarterly Transaction Value (₹ Cr)",
                      labels={"amount_cr":"Amount (₹ Cr)","period":"Quarter"},
                      color_discrete_sequence=["#00bcd4"])
        fig.update_xaxes(tickangle=45)
        st.plotly_chart(fig, use_container_width=True)

    # Year-over-year bar
    yoy = (f_txn.groupby("year")
           .agg(amount_cr=("amount_cr","sum"),
                txn_count=("transaction_count","sum"))
           .reset_index())
    fig = px.bar(yoy, x="year", y="amount_cr",
                 title="Annual Transaction Value (₹ Cr)",
                 color="amount_cr", color_continuous_scale="Purples",
                 labels={"amount_cr":"Amount (₹ Cr)"})
    st.plotly_chart(fig, use_container_width=True)

# ── Tab 2 : Geography ──────────────────────────────────────────────────────────
with tab2:
    st.subheader("Geographic Analysis")

    state_txn = (f_txn.groupby("state")
                 .agg(amount_cr=("amount_cr","sum"),
                      txn_count=("transaction_count","sum"))
                 .reset_index()
                 .sort_values("amount_cr", ascending=False))

    c1, c2 = st.columns(2)
    with c1:
        fig = px.bar(state_txn.head(15), x="amount_cr", y="state",
                     orientation="h", title="Top 15 States – Transaction Value",
                     color="amount_cr", color_continuous_scale="Reds",
                     labels={"amount_cr":"₹ Crores","state":"State"})
        fig.update_layout(yaxis={"categoryorder":"total ascending"})
        st.plotly_chart(fig, use_container_width=True)

    with c2:
        fig = px.bar(state_txn.head(15), x="txn_count", y="state",
                     orientation="h", title="Top 15 States – Transaction Count",
                     color="txn_count", color_continuous_scale="Blues",
                     labels={"txn_count":"Count","state":"State"})
        fig.update_layout(yaxis={"categoryorder":"total ascending"})
        st.plotly_chart(fig, use_container_width=True)

    # District level
    st.subheader("District-Level Deep Dive")
    sel_state = st.selectbox("Select State", sorted(map_txn["state"].unique()))
    dist_df = (map_txn[(map_txn["state"]==sel_state) &
                        (map_txn["year"].isin(sel_years))]
               .groupby("district")
               .agg(amount_cr=("amount_cr","sum"),
                    txn_count=("transaction_count","sum"))
               .reset_index()
               .sort_values("amount_cr", ascending=False)
               .head(20))
    fig = px.bar(dist_df, x="district", y="amount_cr",
                 title=f"Top Districts in {sel_state.title()} (₹ Cr)",
                 color="amount_cr", color_continuous_scale="Oranges")
    fig.update_xaxes(tickangle=45)
    st.plotly_chart(fig, use_container_width=True)

# ── Tab 3 : Transaction Types ──────────────────────────────────────────────────
with tab3:
    st.subheader("Transaction Type Analysis")

    type_df = (f_txn.groupby("transaction_type")
               .agg(txn_count=("transaction_count","sum"),
                    amount_cr=("amount_cr","sum"))
               .reset_index())
    type_df["avg_value"] = type_df["amount_cr"] * 1e7 / type_df["txn_count"]

    c1, c2 = st.columns(2)
    with c1:
        fig = px.pie(type_df, names="transaction_type", values="txn_count",
                     title="Transaction Share by Count",
                     color_discrete_sequence=px.colors.qualitative.Set2)
        st.plotly_chart(fig, use_container_width=True)

    with c2:
        fig = px.pie(type_df, names="transaction_type", values="amount_cr",
                     title="Transaction Share by Value (₹ Cr)",
                     color_discrete_sequence=px.colors.qualitative.Set2)
        st.plotly_chart(fig, use_container_width=True)

    fig = px.bar(type_df.sort_values("avg_value", ascending=False),
                 x="transaction_type", y="avg_value",
                 title="Average Transaction Value by Type (₹)",
                 color="avg_value", color_continuous_scale="Viridis")
    st.plotly_chart(fig, use_container_width=True)

    # Stacked bar over years
    type_year = (f_txn.groupby(["year","transaction_type"])
                 ["transaction_count"].sum().reset_index())
    fig = px.bar(type_year, x="year", y="transaction_count",
                 color="transaction_type", barmode="stack",
                 title="Transaction Count by Type per Year",
                 color_discrete_sequence=px.colors.qualitative.Plotly)
    st.plotly_chart(fig, use_container_width=True)

# ── Tab 4 : Users ──────────────────────────────────────────────────────────────
with tab4:
    st.subheader("User Engagement Analysis")

    user_year = (f_user.groupby("year")
                 .agg(users=("registered_users","sum"),
                      opens=("app_opens","sum"))
                 .reset_index())
    user_year["opens_per_user"] = (user_year["opens"] /
                                    user_year["users"].replace(0, np.nan)).round(2)

    c1, c2 = st.columns(2)
    with c1:
        fig = px.line(user_year, x="year", y="users",
                      title="Registered Users by Year",
                      markers=True, color_discrete_sequence=["#4caf50"])
        st.plotly_chart(fig, use_container_width=True)

    with c2:
        fig = px.bar(user_year, x="year", y="opens_per_user",
                     title="App Opens per Registered User",
                     color="opens_per_user", color_continuous_scale="Greens")
        st.plotly_chart(fig, use_container_width=True)

    # State engagement scatter
    state_eng = (f_user.groupby("state")
                 .agg(users=("registered_users","sum"),
                      opens=("app_opens","sum"))
                 .reset_index())
    state_eng["opens_per_user"] = (state_eng["opens"] /
                                    state_eng["users"].replace(0, np.nan)).round(2)
    fig = px.scatter(state_eng, x="users", y="opens",
                     size="opens_per_user", hover_name="state",
                     color="opens_per_user", color_continuous_scale="Teal",
                     title="Registered Users vs App Opens (bubble = engagement rate)",
                     labels={"users":"Registered Users","opens":"App Opens"})
    st.plotly_chart(fig, use_container_width=True)

# ── Tab 5 : Top Performers ─────────────────────────────────────────────────────
with tab5:
    st.subheader("🏆 Top Performers")

    c1, c2 = st.columns(2)
    with c1:
        top_dist = (top_txn[top_txn["entity_type"]=="district"]
                    .groupby(["state","entity_name"])
                    .agg(amount=("transaction_amount","sum"),
                         count=("transaction_count","sum"))
                    .reset_index()
                    .sort_values("amount", ascending=False)
                    .head(10))
        top_dist["label"] = top_dist["entity_name"] + " (" + top_dist["state"] + ")"
        fig = px.bar(top_dist, x="amount", y="label",
                     orientation="h", title="Top 10 Districts – Transaction Value",
                     color="amount", color_continuous_scale="Reds")
        fig.update_layout(yaxis={"categoryorder":"total ascending"})
        st.plotly_chart(fig, use_container_width=True)

    with c2:
        top_pin = (top_txn[top_txn["entity_type"]=="pincode"]
                   .groupby("entity_name")
                   .agg(count=("transaction_count","sum"))
                   .reset_index()
                   .sort_values("count", ascending=False)
                   .head(10))
        fig = px.bar(top_pin, x="entity_name", y="count",
                     title="Top 10 Pincodes – Transaction Count",
                     color="count", color_continuous_scale="Blues")
        fig.update_xaxes(tickangle=45)
        st.plotly_chart(fig, use_container_width=True)

    # Top users by district
    top_u = (top_user[top_user["entity_type"]=="district"]
             .groupby(["state","entity_name"])
             .agg(users=("registered_users","sum"))
             .reset_index()
             .sort_values("users", ascending=False)
             .head(10))
    top_u["label"] = top_u["entity_name"] + " (" + top_u["state"] + ")"
    fig = px.funnel(top_u, x="users", y="label",
                    title="Top 10 Districts – Registered Users",
                    color_discrete_sequence=["#5f259f"])
    st.plotly_chart(fig, use_container_width=True)

st.caption("📊 Data: PhonePe Pulse | Dashboard: Streamlit + Plotly")
