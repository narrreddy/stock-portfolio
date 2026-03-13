"""Live Stock Signal Dashboard — Streamlit UI for Databricks App.

Reads from Delta tables in Unity Catalog via Databricks SQL connector.
Displays candlestick charts, technical indicators, BUY signals, and portfolio.

Environment variables (set by Databricks Apps runtime or DABs config):
  DATABRICKS_HOST             - workspace URL (auto-set by Databricks Apps)
  DATABRICKS_CLIENT_ID        - service principal client ID (auto-set)
  DATABRICKS_CLIENT_SECRET    - OAuth secret (auto-set)
  DATABRICKS_HTTP_PATH        - SQL warehouse HTTP path (optional override)
  DATABRICKS_WAREHOUSE_ID     - alternative to HTTP_PATH (auto-builds path)
  STOCK_CATALOG               - Unity Catalog catalog (default: stockapp)
  STOCK_SCHEMA                - Unity Catalog schema  (default: production)
"""

import os
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from databricks import sql as dbsql
from databricks.sdk.core import ApiClient, Config, HeaderFactory


def credential_provider() -> HeaderFactory:
    """Build OAuth credential provider for Databricks Apps service principal."""
    config = Config(
        host=os.getenv("DATABRICKS_HOST"),
        client_id=os.getenv("DATABRICKS_CLIENT_ID"),
        client_secret=os.getenv("DATABRICKS_CLIENT_SECRET"),
    )
    return config.authenticate


# ── Configuration (from env vars or defaults) ────────────────────────────
CATALOG = os.getenv("STOCK_CATALOG", "stockapp")
SCHEMA = os.getenv("STOCK_SCHEMA", "production")
BRONZE = f"{CATALOG}.{SCHEMA}.raw_market_data"
SILVER = f"{CATALOG}.{SCHEMA}.technical_indicators"
GOLD_SIG = f"{CATALOG}.{SCHEMA}.trade_signals"
GOLD_PORT = f"{CATALOG}.{SCHEMA}.portfolio_state"


def _get_server_hostname() -> str:
    """Extract bare hostname from DATABRICKS_HOST URL."""
    host = os.getenv("DATABRICKS_HOST", "")
    # Strip protocol prefix — SQL connector needs bare hostname
    return host.replace("https://", "").replace("http://", "").rstrip("/")


def _get_http_path() -> str:
    """Build SQL warehouse HTTP path from env vars."""
    explicit = os.getenv("DATABRICKS_HTTP_PATH")
    if explicit:
        return explicit
    wid = os.getenv("DATABRICKS_WAREHOUSE_ID", "")
    return f"/sql/1.0/warehouses/{wid}"


@st.cache_resource
def get_connection():
    """Establish Databricks SQL connection using OAuth (Databricks Apps)."""
    return dbsql.connect(
        server_hostname=_get_server_hostname(),
        http_path=_get_http_path(),
        credentials_provider=credential_provider(),
    )


def run_query(query: str) -> pd.DataFrame:
    """Execute SQL and return pandas DataFrame."""
    conn = get_connection()
    with conn.cursor() as cursor:
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        return pd.DataFrame(cursor.fetchall(), columns=columns)


# ── Page Config ───────────────────────────────────────────────────────────────
st.set_page_config(page_title="Stock Signal Dashboard", layout="wide")
st.title("📈 Live Stock Signal Dashboard")

# ── Sidebar: Ticker Selector ─────────────────────────────────────────────────
tickers_df = run_query(f"SELECT DISTINCT ticker FROM {BRONZE} ORDER BY ticker")
available_tickers = tickers_df["ticker"].tolist() if not tickers_df.empty else []
selected_ticker = st.sidebar.selectbox("Select Ticker", available_tickers, index=0)
st.sidebar.markdown("---")
st.sidebar.markdown("**Signal Criteria**")
st.sidebar.markdown("- SMA20 > SMA50")
st.sidebar.markdown("- RSI < 70")
st.sidebar.markdown("- Vortex+ > Vortex-")

if st.sidebar.button("🔄 Refresh Data"):
    st.cache_resource.clear()
    st.rerun()

# ── Load Data ─────────────────────────────────────────────────────────────────
ohlc = run_query(f"""
    SELECT * FROM {BRONZE}
    WHERE ticker = '{selected_ticker}'
    ORDER BY timestamp
""")

indicators = run_query(f"""
    SELECT * FROM {SILVER}
    WHERE ticker = '{selected_ticker}' AND rsi IS NOT NULL
    ORDER BY timestamp
""")

signals = run_query(f"""
    SELECT * FROM {GOLD_SIG}
    WHERE ticker = '{selected_ticker}'
    ORDER BY timestamp
""")

portfolio = run_query(f"SELECT * FROM {GOLD_PORT} ORDER BY ticker")

# ── Main Chart: Candlestick + SMA + Signals ──────────────────────────────────
if not ohlc.empty:
    fig = make_subplots(
        rows=3, cols=1, shared_xaxes=True,
        row_heights=[0.55, 0.22, 0.23],
        vertical_spacing=0.03,
        subplot_titles=(
            f"{selected_ticker} OHLC + SMA",
            "RSI (14)",
            "Vortex Indicator (14)",
        ),
    )

    fig.add_trace(
        go.Candlestick(
            x=ohlc["timestamp"], open=ohlc["open"],
            high=ohlc["high"], low=ohlc["low"], close=ohlc["close"],
            name="OHLC",
            increasing_line_color="#26a69a", decreasing_line_color="#ef5350",
        ),
        row=1, col=1,
    )

    if not indicators.empty:
        fig.add_trace(go.Scatter(
            x=indicators["timestamp"], y=indicators["sma20"],
            mode="lines", name="SMA 20", line=dict(color="#2196F3", width=1.3),
        ), row=1, col=1)
        fig.add_trace(go.Scatter(
            x=indicators["timestamp"], y=indicators["sma50"],
            mode="lines", name="SMA 50", line=dict(color="#FF9800", width=1.3),
        ), row=1, col=1)

    if not signals.empty:
        fig.add_trace(go.Scatter(
            x=signals["timestamp"], y=signals["close"],
            mode="markers", name="BUY Signal",
            marker=dict(symbol="triangle-up", size=14, color="#00E676",
                        line=dict(width=1.5, color="black")),
        ), row=1, col=1)

    if not indicators.empty:
        fig.add_trace(go.Scatter(
            x=indicators["timestamp"], y=indicators["rsi"],
            mode="lines", name="RSI", line=dict(color="#AB47BC", width=1.3),
        ), row=2, col=1)
        fig.add_hline(y=70, line_dash="dash", line_color="red",
                      annotation_text="Overbought (70)", row=2, col=1)
        fig.add_hline(y=30, line_dash="dash", line_color="green",
                      annotation_text="Oversold (30)", row=2, col=1)

        fig.add_trace(go.Scatter(
            x=indicators["timestamp"], y=indicators["vortex_positive"],
            mode="lines", name="VI+", line=dict(color="#4CAF50", width=1.3),
        ), row=3, col=1)
        fig.add_trace(go.Scatter(
            x=indicators["timestamp"], y=indicators["vortex_negative"],
            mode="lines", name="VI\u2212", line=dict(color="#F44336", width=1.3),
        ), row=3, col=1)

    fig.update_layout(
        height=800, template="plotly_dark",
        xaxis_rangeslider_visible=False,
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
        margin=dict(l=40, r=20, t=60, b=20),
    )
    st.plotly_chart(fig, use_container_width=True)
else:
    st.warning(f"No OHLC data for {selected_ticker}. Run the ingest pipeline first.")

# ── Signals Table ─────────────────────────────────────────────────────────────
st.subheader(f"🚨 Recent BUY Signals — {selected_ticker}")
if not signals.empty:
    st.dataframe(
        signals.tail(20)[["timestamp", "signal", "close", "sma20", "sma50", "rsi",
                          "vortex_positive", "vortex_negative"]],
        use_container_width=True,
    )
else:
    st.info("No BUY signals generated for this ticker yet.")

# ── Portfolio Dashboard ──────────────────────────────────────────────────────
st.subheader("💼 Portfolio Dashboard")
if not portfolio.empty:
    cols = st.columns(len(portfolio))
    for i, (_, row) in enumerate(portfolio.iterrows()):
        with cols[i % len(cols)]:
            pnl = row.get("unrealized_pnl", 0) or 0
            st.metric(
                label=row["ticker"],
                value=f"${row.get('current_price', 0):.2f}",
                delta=f"PnL: ${pnl:.2f}",
            )
            st.caption(
                f"Holdings: {row.get('holdings', 0)} shares | "
                f"Avg Entry: ${row.get('avg_entry_price', 0):.2f}"
            )
    st.markdown("---")
    st.dataframe(portfolio, use_container_width=True)
else:
    st.info("No portfolio data yet. BUY signals will create positions.")

# ── Footer ────────────────────────────────────────────────────────────────────
st.sidebar.markdown("---")
st.sidebar.caption("Powered by Databricks · Delta Lake · Spark Streaming")
