"""Live Stock Signal Dashboard — Streamlit UI for Databricks App.

Reads from Delta tables in Unity Catalog via Databricks SDK Statement
Execution API (REST).  Displays candlestick charts, technical indicators,
BUY/SELL signals, portfolio, trade history, and performance metrics.

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
import time
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
TRADE_LOG = f"{CATALOG}.{SCHEMA}.trade_log"
DQ_EVENTS = f"{CATALOG}.{SCHEMA}.data_quality_events"


@st.cache_data(ttl=60, show_spinner=False)
def run_query(query: str, critical: bool = True) -> pd.DataFrame:
    """Execute SQL via Databricks REST API and return a typed DataFrame.
    
    Args:
        query: SQL statement to execute.
        critical: If True (default), display error and stop the app on failure.
                  If False, return an empty DataFrame on failure (for optional tables
                  like trade_log, data_quality_events that may not exist yet).
    
    Cached for 60 seconds to avoid hammering the warehouse on rapid refreshes.
    """
    try:
        response = w.statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID,
            statement=query,
            wait_timeout="50s",
        )
    except Exception as e:
        if not critical:
            return pd.DataFrame()
        st.error(
            f"**Query execution error**\n\n"
            f"- `warehouse_id`: `{WAREHOUSE_ID}`\n"
            f"- `DATABRICKS_HOST`: `{os.getenv('DATABRICKS_HOST', '(not set)')}`\n\n"
            f"**Error**: `{type(e).__name__}: {e}`"
        )
        st.stop()

    # Poll until the statement finishes (handles warehouse cold-start > 50s)
    while response.status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(3)
        response = w.statement_execution.get_statement(response.statement_id)

    if response.status.state != StatementState.SUCCEEDED:
        if not critical:
            return pd.DataFrame()
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
st.title("Live Stock Signal Dashboard")

# ── Sidebar ───────────────────────────────────────────────────────────
tickers_df = run_query(f"SELECT DISTINCT ticker FROM {BRONZE} ORDER BY ticker")
available_tickers = tickers_df["ticker"].tolist() if not tickers_df.empty else []

if not available_tickers:
    st.warning("No data available yet. Run the pipeline notebook first to ingest market data.")
    st.stop()

selected_ticker = st.sidebar.selectbox("Select Ticker", available_tickers, index=0)
st.sidebar.markdown("---")
st.sidebar.markdown("**Signal Criteria**")
st.sidebar.markdown(
    "**BUY** (all 3): SMA20 > SMA50, RSI < 70, VI+ > VI-\n\n"
    "**SELL** (any 2 of 3): SMA20 < SMA50, RSI > 70, VI+ < VI-"
)
st.sidebar.markdown("---")
st.sidebar.markdown("**Risk Management**")
st.sidebar.markdown("Stop-Loss: 3% | Take-Profit: 6% | Cooldown: 30min")

if st.sidebar.button("Refresh Data"):
    st.cache_data.clear()
    st.rerun()

# ── Data Quality Events in Sidebar (non-critical) ────────────────────
with st.sidebar.expander("Data Quality Events", expanded=False):
    dq_df = run_query(f"""
        SELECT event_type, ticker, message, detected_at
        FROM {DQ_EVENTS}
        ORDER BY detected_at DESC
        LIMIT 15
    """, critical=False)
    if not dq_df.empty:
        for _, row in dq_df.iterrows():
            icon = {
                "API_ERROR": "X", "MISSING_BARS": "!",
                "DUPLICATE_DETECTED": "D", "STALE_DATA": "S",
                "GAP_DETECTED": "G", "INVALID_PRICE": "P",
                "EXTREME_SPIKE_REMOVED": "!",
            }.get(row.get("event_type", ""), "?")
            st.caption(f"[{icon}] {row.get('ticker', '')} — {row.get('message', '')}")
    else:
        st.caption("No data quality events recorded yet.")

st.sidebar.markdown("---")
st.sidebar.caption("Powered by Databricks | Delta Lake | Spark Streaming")

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
""", critical=False)

all_signals = run_query(f"""
    SELECT * FROM {GOLD_SIG}
    WHERE ticker = '{selected_ticker}'
    ORDER BY timestamp
""", critical=False)

portfolio = run_query(f"SELECT * FROM {GOLD_PORT} ORDER BY ticker", critical=False)

# Split signals by type
buy_signals = all_signals[all_signals["signal"] == "BUY"] if not all_signals.empty else pd.DataFrame()
sell_signals = all_signals[all_signals["signal"] == "SELL"] if not all_signals.empty else pd.DataFrame()

# ── Performance Metrics Bar (non-critical) ───────────────────────────
perf = run_query(f"""
    SELECT
      COUNT(*)                                             AS total_trades,
      SUM(CASE WHEN signal = 'BUY' THEN 1 ELSE 0 END)     AS buy_count,
      SUM(CASE WHEN signal = 'SELL' THEN 1 ELSE 0 END)     AS sell_count,
      SUM(CASE WHEN signal IN ('STOP_LOSS','TAKE_PROFIT') THEN 1 ELSE 0 END) AS auto_exits,
      ROUND(SUM(COALESCE(pnl_realized, 0)), 2)             AS total_pnl,
      ROUND(AVG(CASE WHEN pnl_realized > 0 THEN pnl_realized END), 2) AS avg_win,
      ROUND(AVG(CASE WHEN pnl_realized < 0 THEN pnl_realized END), 2) AS avg_loss,
      ROUND(SUM(CASE WHEN pnl_realized > 0 THEN 1 ELSE 0 END) * 100.0 /
            NULLIF(SUM(CASE WHEN pnl_realized != 0 THEN 1 ELSE 0 END), 0), 1) AS win_rate
    FROM {TRADE_LOG}
""", critical=False)

if not perf.empty and perf["total_trades"].iloc[0] and perf["total_trades"].iloc[0] > 0:
    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Total Trades", int(perf["total_trades"].iloc[0]))
    m2.metric("Realized PnL", f"${perf['total_pnl'].iloc[0]:.2f}")
    win_rate = perf["win_rate"].iloc[0]
    m3.metric("Win Rate", f"{win_rate:.1f}%" if pd.notna(win_rate) else "N/A")
    avg_w = perf["avg_win"].iloc[0]
    m4.metric("Avg Win", f"${avg_w:.2f}" if pd.notna(avg_w) else "N/A")
    avg_l = perf["avg_loss"].iloc[0]
    m5.metric("Avg Loss", f"${avg_l:.2f}" if pd.notna(avg_l) else "N/A")
    st.markdown("---")

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

    # BUY signals: green triangle-up
    if not buy_signals.empty:
        hover_text = []
        for _, s in buy_signals.iterrows():
            score = s.get("signal_score", None)
            sl = s.get("stop_loss", None)
            tp = s.get("take_profit", None)
            text = f"BUY @ ${s['close']:.2f}"
            if pd.notna(score):
                text += f"<br>Score: {score:.0f}/100"
            if pd.notna(sl):
                text += f"<br>SL: ${sl:.2f}"
            if pd.notna(tp):
                text += f"<br>TP: ${tp:.2f}"
            hover_text.append(text)

        fig.add_trace(go.Scatter(
            x=buy_signals["timestamp"], y=buy_signals["close"],
            mode="markers", name="BUY Signal",
            marker=dict(symbol="triangle-up", size=14, color="#00E676",
                        line=dict(width=1.5, color="black")),
            hovertext=hover_text, hoverinfo="text",
        ), row=1, col=1)

    # SELL signals: red triangle-down
    if not sell_signals.empty:
        hover_text = []
        for _, s in sell_signals.iterrows():
            score = s.get("signal_score", None)
            text = f"SELL @ ${s['close']:.2f}"
            if pd.notna(score):
                text += f"<br>Score: {score:.0f}/100"
            hover_text.append(text)

        fig.add_trace(go.Scatter(
            x=sell_signals["timestamp"], y=sell_signals["close"],
            mode="markers", name="SELL Signal",
            marker=dict(symbol="triangle-down", size=14, color="#FF1744",
                        line=dict(width=1.5, color="black")),
            hovertext=hover_text, hoverinfo="text",
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
            mode="lines", name="VI-", line=dict(color="#F44336", width=1.3),
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

# ── Tabbed Section: Signals | Trade Log | Equity Curve ──────────────────
tab1, tab2, tab3 = st.tabs(["Signals", "Trade Log", "Equity Curve"])

with tab1:
    st.subheader(f"Recent Signals — {selected_ticker}")
    if not all_signals.empty:
        display_cols = ["timestamp", "signal", "signal_score", "close", "sma20", "sma50",
                        "rsi", "vortex_positive", "vortex_negative", "stop_loss", "take_profit"]
        display_cols = [c for c in display_cols if c in all_signals.columns]
        styled = all_signals.tail(30)[display_cols].copy()
        st.dataframe(styled, use_container_width=True)
    else:
        st.info("No signals generated for this ticker yet.")

with tab2:
    st.subheader("Trade Log (All Tickers)")
    trades = run_query(f"""
        SELECT trade_id, ticker, signal, signal_score, price, quantity,
               pnl_realized, stop_loss, take_profit, timestamp
        FROM {TRADE_LOG}
        ORDER BY timestamp DESC
        LIMIT 50
    """, critical=False)
    if not trades.empty:
        st.dataframe(trades, use_container_width=True)
    else:
        st.info("No trades recorded yet. Run the pipeline to generate signals.")

with tab3:
    st.subheader("Cumulative Realized PnL")
    equity = run_query(f"""
        SELECT timestamp,
               pnl_realized,
               SUM(COALESCE(pnl_realized, 0)) OVER (ORDER BY timestamp) AS cumulative_pnl
        FROM {TRADE_LOG}
        WHERE pnl_realized IS NOT NULL AND pnl_realized != 0
        ORDER BY timestamp
    """, critical=False)
    if not equity.empty and len(equity) > 1:
        eq_fig = go.Figure()
        eq_fig.add_trace(go.Scatter(
            x=equity["timestamp"], y=equity["cumulative_pnl"],
            mode="lines+markers", name="Cumulative PnL",
            line=dict(color="#00E5FF", width=2),
            fill="tozeroy",
            fillcolor="rgba(0, 229, 255, 0.1)",
        ))
        eq_fig.add_hline(y=0, line_dash="dash", line_color="gray")
        eq_fig.update_layout(
            height=400, template="plotly_dark",
            yaxis_title="Cumulative PnL ($)",
            xaxis_title="Time",
            margin=dict(l=40, r=20, t=20, b=40),
        )
        st.plotly_chart(eq_fig, use_container_width=True)
    else:
        st.info("Not enough closed trades to plot equity curve yet.")

# ── Portfolio Dashboard ────────────────────────────────────────────────
st.subheader("Portfolio Dashboard")
if not portfolio.empty:
    # Show metrics for active positions (holdings > 0)
    active = portfolio[portfolio["holdings"] > 0] if "holdings" in portfolio.columns else portfolio
    if not active.empty:
        cols = st.columns(min(len(active), 4))
        for i, (_, row) in enumerate(active.iterrows()):
            with cols[i % len(cols)]:
                unrealized = row.get("unrealized_pnl", 0) or 0
                realized = row.get("realized_pnl", 0) or 0
                trades_count = row.get("total_trades", 0) or 0
                st.metric(
                    label=row["ticker"],
                    value=f"${row.get('current_price', 0):.2f}",
                    delta=f"Unrealized: ${unrealized:.2f}",
                )
                st.caption(
                    f"Holdings: {row.get('holdings', 0)} shares | "
                    f"Entry: ${row.get('avg_entry_price', 0):.2f} | "
                    f"Realized: ${realized:.2f} | "
                    f"Trades: {trades_count}"
                )
    else:
        st.info("No active positions. All positions are closed or flat.")

    st.markdown("---")
    st.dataframe(portfolio, use_container_width=True)
else:
    st.info("No portfolio data yet. Signals will create positions.")
