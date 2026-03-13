"""Live Stock Signal Dashboard — Streamlit UI for Databricks App.

Reads from Delta tables in Unity Catalog via Databricks SDK Statement
Execution API (REST).  Displays candlestick charts, technical indicators,
BUY signals, and portfolio.

Environment variables (auto-set by Databricks Apps runtime):
  DATABRICKS_HOST             - workspace URL
  DATABRICKS_CLIENT_ID        - service principal client ID
  DATABRICKS_CLIENT_SECRET    - OAuth secret

Environment variables (set via DABs config in resources/stock_signal_app.yml):
  DATABRICKS_WAREHOUSE_ID     - SQL warehouse ID
  STOCK_CATALOG               - Unity Catalog catalog (default: stockapp)
  STOCK_SCHEMA                - Unity Catalog schema  (default: production)
"""

import os
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

# ── Databricks SDK client (auto-detects HOST, CLIENT_ID, CLIENT_SECRET) ──
w = WorkspaceClient()
WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID", "")

# ── Table Configuration ────────────────────────────────────────────────────
CATALOG = os.getenv("STOCK_CATALOG", "stockapp")
SCHEMA = os.getenv("STOCK_SCHEMA", "production")
BRONZE = f"{CATALOG}.{SCHEMA}.raw_market_data"
SILVER = f"{CATALOG}.{SCHEMA}.technical_indicators"
GOLD_SIG = f"{CATALOG}.{SCHEMA}.trade_signals"
GOLD_PORT = f"{CATALOG}.{SCHEMA}.portfolio_state"


def run_query(query: str) -> pd.DataFrame:
    """Execute SQL via Databricks REST API and return a typed DataFrame."""
    try:
        response = w.statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID,
            statement=query,
            wait_timeout="120s",
        )
    except Exception as e:
        st.error(
            f"**Query execution error**\n\n"
            f"- `warehouse_id`: `{WAREHOUSE_ID}`\n"
            f"- `DATABRICKS_HOST`: `{os.getenv('DATABRICKS_HOST', '(not set)')}`\n"
            f"- `DATABRICKS_CLIENT_ID`: `{'set' if os.getenv('DATABRICKS_CLIENT_ID') else '(not set)'}`\n\n"
            f"**Error**: `{type(e).__name__}: {e}`"
        )
        st.stop()

    if response.status.state != StatementState.SUCCEEDED:
        msg = response.status.error.message if response.status.error else "unknown"
        st.error(f"**Query failed** ({response.status.state}): {msg}")
        st.stop()

    # Build DataFrame from response
    columns = [col.name for col in response.manifest.schema.columns]
    rows = response.result.data_array if response.result and response.result.data_array else []
    df = pd.DataFrame(rows, columns=columns)

    # Cast columns to correct types based on manifest
    for col_info in response.manifest.schema.columns:
        name = col_info.name
        type_text = (col_info.type_text or "").upper()
        if name not in df.columns or df[name].empty:
            continue
        if any(t in type_text for t in ["DOUBLE", "FLOAT", "DECIMAL"]):
            df[name] = pd.to_numeric(df[name], errors="coerce")
        elif any(t in type_text for t in ["INT", "LONG", "SHORT", "BIGINT"]):
            df[name] = pd.to_numeric(df[name], errors="coerce")
        elif "TIMESTAMP" in type_text:
            df[name] = pd.to_datetime(df[name], errors="coerce")
    return df


# ── Page Config ─────────────────────────────────────────────────────────
st.set_page_config(page_title="Stock Signal Dashboard", layout="wide")
st.title("📈 Live Stock Signal Dashboard")

# ── Sidebar ───────────────────────────────────────────────────────────
tickers_df = run_query(f"SELECT DISTINCT ticker FROM {BRONZE} ORDER BY ticker")
available_tickers = tickers_df["ticker"].tolist() if not tickers_df.empty else []
selected_ticker = st.sidebar.selectbox("Select Ticker", available_tickers, index=0)
st.sidebar.markdown("---")
st.sidebar.markdown("**Signal Criteria**")
st.sidebar.markdown("- SMA20 > SMA50")
st.sidebar.markdown("- RSI < 70")
st.sidebar.markdown("- Vortex+ > Vortex-")

if st.sidebar.button("🔄 Refresh Data"):
    st.rerun()

# ── Load Data ──────────────────────────────────────────────────────────
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

# ── Main Chart ─────────────────────────────────────────────────────────
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

# ── Signals Table ───────────────────────────────────────────────────────
st.subheader(f"🚨 Recent BUY Signals — {selected_ticker}")
if not signals.empty:
    st.dataframe(
        signals.tail(20)[["timestamp", "signal", "close", "sma20", "sma50", "rsi",
                          "vortex_positive", "vortex_negative"]],
        use_container_width=True,
    )
else:
    st.info("No BUY signals generated for this ticker yet.")

# ── Portfolio Dashboard ────────────────────────────────────────────────
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

# ── Footer ──────────────────────────────────────────────────────────────
st.sidebar.markdown("---")
st.sidebar.caption("Powered by Databricks · Delta Lake · Spark Streaming")
