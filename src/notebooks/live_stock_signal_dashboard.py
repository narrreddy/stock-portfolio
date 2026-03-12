# Databricks notebook source

# MAGIC %md
# MAGIC ## Live Stock Signal Dashboard
# MAGIC
# MAGIC **Medallion Architecture** streaming pipeline with real-time technical indicators and BUY signal generation.
# MAGIC
# MAGIC | Layer | Table | Description |
# MAGIC |-------|-------|-------------|
# MAGIC | Bronze | `raw_market_data` | Raw OHLCV from Yahoo Finance |
# MAGIC | Silver | `technical_indicators` | SMA20, SMA50, RSI, Vortex per ticker |
# MAGIC | Gold | `trade_signals` | BUY signals (SMA20>SMA50, RSI<70, VI+>VI-) |
# MAGIC | Gold | `portfolio_state` | Holdings, avg entry, unrealized PnL |
# MAGIC
# MAGIC **Stack**: Spark Structured Streaming · `applyInPandasWithState` · Delta Lake · Plotly · Streamlit UI

# COMMAND ----------

# MAGIC %pip install yfinance plotly --quiet

# COMMAND ----------

# ── Configuration ────────────────────────────────────────────────
CATALOG   = "stockapp"
SCHEMA    = "production"
BRONZE    = f"{CATALOG}.{SCHEMA}.raw_market_data"
SILVER    = f"{CATALOG}.{SCHEMA}.technical_indicators"
GOLD_SIG  = f"{CATALOG}.{SCHEMA}.trade_signals"
GOLD_PORT = f"{CATALOG}.{SCHEMA}.portfolio_state"
CHECKPOINT_VOL = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints"

TICKERS = ["AAPL", "MSFT", "NVDA", "GOOG", "TSLA"]

# Indicator parameters
SMA_SHORT  = 20
SMA_LONG   = 50
RSI_PERIOD = 14
VORTEX_PERIOD = 14
RSI_OVERBOUGHT = 70
BUY_SHARES = 100  # shares per BUY signal

print(f"Config loaded: {CATALOG}.{SCHEMA} | Tickers: {TICKERS}")

# COMMAND ----------

# ── Create Unity Catalog Resources ───────────────────────────────
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# Bronze: raw OHLCV
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {BRONZE} (
  ticker  STRING,
  timestamp TIMESTAMP,
  open    DOUBLE,
  high    DOUBLE,
  low     DOUBLE,
  close   DOUBLE,
  volume  LONG
) USING DELTA
""")

# Silver: technical indicators
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER} (
  ticker          STRING,
  timestamp       TIMESTAMP,
  close           DOUBLE,
  sma20           DOUBLE,
  sma50           DOUBLE,
  rsi             DOUBLE,
  vortex_positive DOUBLE,
  vortex_negative DOUBLE
) USING DELTA
""")

# Gold: trade signals
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_SIG} (
  ticker          STRING,
  timestamp       TIMESTAMP,
  signal          STRING,
  close           DOUBLE,
  sma20           DOUBLE,
  sma50           DOUBLE,
  rsi             DOUBLE,
  vortex_positive DOUBLE,
  vortex_negative DOUBLE
) USING DELTA
""")

# Gold: portfolio state
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_PORT} (
  ticker          STRING,
  holdings        LONG,
  avg_entry_price DOUBLE,
  current_price   DOUBLE,
  unrealized_pnl  DOUBLE,
  last_signal     STRING,
  last_signal_time TIMESTAMP
) USING DELTA
""")

# Volume for streaming checkpoints
spark.sql(f"""
CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.checkpoints
COMMENT 'Streaming pipeline checkpoint storage'
""")

print(f"All UC resources created: {CATALOG}.{SCHEMA}")
print(f"Tables: raw_market_data, technical_indicators, trade_signals, portfolio_state")
print(f"Volume: checkpoints")

# COMMAND ----------

# ── Yahoo Finance Batch Ingest → Bronze ────────────────────
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta

def ingest_tickers(tickers, period="5d", interval="5m"):
    """Fetch OHLCV data for all tickers and write to Bronze Delta table."""
    frames = []
    for t in tickers:
        try:
            hist = yf.Ticker(t).history(period=period, interval=interval)
            if hist.empty:
                print(f"  ⚠ {t}: no data returned")
                continue
            df = hist.reset_index().rename(columns={
                "Datetime": "timestamp", "Date": "timestamp",
                "Open": "open", "High": "high", "Low": "low",
                "Close": "close", "Volume": "volume"
            })
            df["ticker"] = t
            df = df[["ticker", "timestamp", "open", "high", "low", "close", "volume"]]
            # Remove timezone info for Delta compatibility
            df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.tz_localize(None)
            df["volume"] = df["volume"].astype("int64")
            frames.append(df)
            print(f"  ✓ {t}: {len(df)} rows")
        except Exception as e:
            print(f"  ✗ {t}: {e}")

    if not frames:
        print("No data ingested.")
        return 0

    pdf = pd.concat(frames, ignore_index=True)
    sdf = spark.createDataFrame(pdf)
    sdf.write.format("delta").mode("append").saveAsTable(BRONZE)
    print(f"\n✔ Ingested {len(pdf)} total rows to {BRONZE}")
    return len(pdf)

# Run ingest: 5 days of 5-minute bars
print("Ingesting Yahoo Finance data...")
row_count = ingest_tickers(TICKERS, period="5d", interval="5m")

# COMMAND ----------

# ── Technical Indicator Processor (applyInPandasWithState) ──────
import json
from typing import Iterator, Tuple
import pandas as pd
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
    DoubleType, LongType
)

# State schema: serialized rolling window as JSON
STATE_SCHEMA = StructType([
    StructField("prices_json", StringType()),
])

# Output schema: indicator values
INDICATOR_SCHEMA = StructType([
    StructField("ticker",          StringType()),
    StructField("timestamp",       TimestampType()),
    StructField("close",           DoubleType()),
    StructField("sma20",           DoubleType()),
    StructField("sma50",           DoubleType()),
    StructField("rsi",             DoubleType()),
    StructField("vortex_positive", DoubleType()),
    StructField("vortex_negative", DoubleType()),
])


def compute_indicators(
    key: Tuple[str],
    pdf_iter: Iterator[pd.DataFrame],
    state
) -> Iterator[pd.DataFrame]:
    """
    Stateful processor: maintains rolling window per ticker,
    computes SMA(20), SMA(50), RSI(14), Vortex(14).
    """
    ticker = key[0]

    # Load existing state (rolling price window)
    if state.exists:
        prices = json.loads(state.get[0])
    else:
        prices = []  # each: [ts_epoch_ms, high, low, close]

    # Collect all new rows from this micro-batch
    all_new = []
    for pdf in pdf_iter:
        for _, r in pdf.iterrows():
            all_new.append([
                int(r["timestamp"].timestamp() * 1000),
                float(r["high"]),
                float(r["low"]),
                float(r["close"]),
            ])

    # Merge, sort, keep last SMA_LONG records
    combined = prices + all_new
    combined.sort(key=lambda x: x[0])
    combined = combined[-SMA_LONG:]

    # Persist updated state
    state.update((json.dumps(combined),))

    # Extract arrays
    n = len(combined)
    ts_arr    = [c[0] for c in combined]
    highs_arr = [c[1] for c in combined]
    lows_arr  = [c[2] for c in combined]
    close_arr = [c[3] for c in combined]

    # Only emit indicators for NEW rows
    new_ts_set = {nr[0] for nr in all_new}
    records = []

    for i in range(n):
        if ts_arr[i] not in new_ts_set:
            continue  # skip already-emitted historical rows

        # --- SMA ---
        sma20 = sum(close_arr[max(0, i - SMA_SHORT + 1):i + 1]) / min(SMA_SHORT, i + 1)
        sma50 = sum(close_arr[max(0, i - SMA_LONG + 1):i + 1])  / min(SMA_LONG, i + 1)

        # --- RSI (14-period) ---
        rsi = None
        if i >= RSI_PERIOD:
            gains, losses = [], []
            for j in range(i - RSI_PERIOD + 1, i + 1):
                chg = close_arr[j] - close_arr[j - 1]
                gains.append(max(chg, 0.0))
                losses.append(max(-chg, 0.0))
            avg_gain = sum(gains) / RSI_PERIOD
            avg_loss = sum(losses) / RSI_PERIOD
            rsi = 100.0 if avg_loss == 0 else 100.0 - 100.0 / (1.0 + avg_gain / avg_loss)

        # --- Vortex Indicator (14-period) ---
        #   TR  = max(H-L, |H - prevClose|, |L - prevClose|)
        #   VM+ = |H_i - L_{i-1}|       VM- = |L_i - H_{i-1}|
        #   VI+ = sum(VM+,14)/sum(TR,14) VI- = sum(VM-,14)/sum(TR,14)
        vp, vn = None, None
        if i >= VORTEX_PERIOD:
            tr_sum = vm_plus = vm_minus = 0.0
            for j in range(i - VORTEX_PERIOD + 1, i + 1):
                tr = max(
                    highs_arr[j] - lows_arr[j],
                    abs(highs_arr[j] - close_arr[j - 1]),
                    abs(lows_arr[j]  - close_arr[j - 1])
                )
                tr_sum   += tr
                vm_plus  += abs(highs_arr[j] - lows_arr[j - 1])
                vm_minus += abs(lows_arr[j]  - highs_arr[j - 1])
            if tr_sum > 0:
                vp = vm_plus  / tr_sum
                vn = vm_minus / tr_sum

        records.append({
            "ticker":          ticker,
            "timestamp":       pd.Timestamp(ts_arr[i], unit="ms"),
            "close":           close_arr[i],
            "sma20":           sma20,
            "sma50":           sma50,
            "rsi":             rsi,
            "vortex_positive": vp,
            "vortex_negative": vn,
        })

    if records:
        yield pd.DataFrame(records)


print("compute_indicators() defined (applyInPandasWithState).")

# COMMAND ----------

# ── Run Streaming Pipeline: Bronze → Silver (indicators) + Gold (signals) ──
from pyspark.sql import functions as F

def write_indicators_and_signals(batch_df, epoch_id):
    """foreachBatch: write indicators to Silver, BUY signals to Gold."""
    if batch_df.isEmpty():
        return

    # --- Write to Silver ---
    batch_df.write.format("delta").mode("append").saveAsTable(SILVER)

    # --- Generate BUY signals ---
    signals = batch_df.filter(
        (F.col("sma20") > F.col("sma50")) &
        (F.col("rsi").isNotNull()) & (F.col("rsi") < RSI_OVERBOUGHT) &
        (F.col("vortex_positive").isNotNull()) &
        (F.col("vortex_positive") > F.col("vortex_negative"))
    ).withColumn("signal", F.lit("BUY"))

    sig_count = signals.count()
    if sig_count > 0:
        signals.select(
            "ticker", "timestamp", "signal", "close",
            "sma20", "sma50", "rsi", "vortex_positive", "vortex_negative"
        ).write.format("delta").mode("append").saveAsTable(GOLD_SIG)
        print(f"  Epoch {epoch_id}: {batch_df.count()} indicators, {sig_count} BUY signals")
    else:
        print(f"  Epoch {epoch_id}: {batch_df.count()} indicators, 0 signals")


# Read Bronze as stream
bronze_stream = spark.readStream.format("delta").table(BRONZE)

# Apply stateful processing: group by ticker, compute indicators
indicator_stream = (
    bronze_stream
    .groupBy("ticker")
    .applyInPandasWithState(
        func=compute_indicators,
        outputStructType=INDICATOR_SCHEMA,
        stateStructType=STATE_SCHEMA,
        outputMode="append",
        timeoutConf="NoTimeout",
    )
)

# Write stream with foreachBatch
query = (
    indicator_stream
    .writeStream
    .foreachBatch(write_indicators_and_signals)
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_VOL}/indicators")
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()
print("\n✔ Streaming pipeline completed (availableNow batch).")

# COMMAND ----------

# ── Portfolio Tracking: MERGE from signals + latest prices ────────

# Step 1: Build source data (latest price per ticker + latest BUY signal)
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW portfolio_updates AS
WITH latest_price AS (
  SELECT ticker, close AS current_price, timestamp
  FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY timestamp DESC) AS rn
    FROM {BRONZE}
  ) WHERE rn = 1
),
latest_signal AS (
  SELECT ticker, signal, close AS signal_price, timestamp AS signal_time
  FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY timestamp DESC) AS rn
    FROM {GOLD_SIG}
  ) WHERE rn = 1
)
SELECT
  p.ticker,
  p.current_price,
  s.signal        AS last_signal,
  s.signal_price,
  s.signal_time   AS last_signal_time
FROM latest_price p
LEFT JOIN latest_signal s ON p.ticker = s.ticker
""")

# Step 2: MERGE into portfolio_state
spark.sql(f"""
MERGE INTO {GOLD_PORT} AS target
USING portfolio_updates AS source
ON target.ticker = source.ticker
WHEN MATCHED THEN UPDATE SET
  target.current_price   = source.current_price,
  target.unrealized_pnl  = (source.current_price - target.avg_entry_price) * target.holdings,
  target.last_signal     = COALESCE(source.last_signal, target.last_signal),
  target.last_signal_time = COALESCE(source.last_signal_time, target.last_signal_time)
WHEN NOT MATCHED AND source.last_signal = 'BUY' THEN INSERT (
  ticker, holdings, avg_entry_price, current_price, unrealized_pnl,
  last_signal, last_signal_time
) VALUES (
  source.ticker, {BUY_SHARES}, source.signal_price, source.current_price,
  (source.current_price - source.signal_price) * {BUY_SHARES},
  source.last_signal, source.last_signal_time
)
""")

print(f"✔ Portfolio updated in {GOLD_PORT}")
display(spark.table(GOLD_PORT))

# COMMAND ----------

# ── Verification: Pipeline Results ────────────────────────────
print("=== Bronze: Raw Market Data ===")
display(spark.sql(f"SELECT ticker, COUNT(*) AS rows, MIN(timestamp) AS earliest, MAX(timestamp) AS latest FROM {BRONZE} GROUP BY ticker ORDER BY ticker"))

print("\n=== Silver: Technical Indicators (sample) ===")
display(spark.sql(f"SELECT * FROM {SILVER} WHERE rsi IS NOT NULL ORDER BY timestamp DESC LIMIT 15"))

print("\n=== Gold: Trade Signals ===")
display(spark.sql(f"SELECT ticker, signal, COUNT(*) AS count FROM {GOLD_SIG} GROUP BY ticker, signal ORDER BY ticker"))

print("\n=== Gold: Portfolio State ===")
display(spark.table(GOLD_PORT))

# COMMAND ----------

# ── Plotly Candlestick Chart with Indicators & Signals ───────
import plotly.graph_objects as go
from plotly.subplots import make_subplots

def plot_ticker_dashboard(ticker_symbol):
    """Generate a 3-panel chart: OHLC+SMA, RSI, Vortex with BUY markers."""
    # Fetch data
    ohlc = spark.sql(f"""
        SELECT * FROM {BRONZE}
        WHERE ticker = '{ticker_symbol}'
        ORDER BY timestamp
    """).toPandas()

    ind = spark.sql(f"""
        SELECT * FROM {SILVER}
        WHERE ticker = '{ticker_symbol}' AND rsi IS NOT NULL
        ORDER BY timestamp
    """).toPandas()

    sigs = spark.sql(f"""
        SELECT * FROM {GOLD_SIG}
        WHERE ticker = '{ticker_symbol}'
        ORDER BY timestamp
    """).toPandas()

    if ohlc.empty:
        print(f"No data for {ticker_symbol}")
        return

    fig = make_subplots(
        rows=3, cols=1, shared_xaxes=True,
        row_heights=[0.55, 0.22, 0.23],
        vertical_spacing=0.03,
        subplot_titles=(f"{ticker_symbol} OHLC + SMA", "RSI (14)", "Vortex (14)")
    )

    # Candlestick
    fig.add_trace(go.Candlestick(
        x=ohlc["timestamp"], open=ohlc["open"],
        high=ohlc["high"], low=ohlc["low"], close=ohlc["close"],
        name="OHLC", increasing_line_color="#26a69a", decreasing_line_color="#ef5350"
    ), row=1, col=1)

    # SMA overlays
    if not ind.empty:
        fig.add_trace(go.Scatter(
            x=ind["timestamp"], y=ind["sma20"],
            mode="lines", name="SMA20", line=dict(color="#2196F3", width=1.2)
        ), row=1, col=1)
        fig.add_trace(go.Scatter(
            x=ind["timestamp"], y=ind["sma50"],
            mode="lines", name="SMA50", line=dict(color="#FF9800", width=1.2)
        ), row=1, col=1)

    # BUY signal markers
    if not sigs.empty:
        fig.add_trace(go.Scatter(
            x=sigs["timestamp"], y=sigs["close"],
            mode="markers", name="BUY Signal",
            marker=dict(symbol="triangle-up", size=12, color="#00E676",
                        line=dict(width=1, color="black"))
        ), row=1, col=1)

    # RSI panel
    if not ind.empty:
        fig.add_trace(go.Scatter(
            x=ind["timestamp"], y=ind["rsi"],
            mode="lines", name="RSI", line=dict(color="#AB47BC", width=1.2)
        ), row=2, col=1)
        fig.add_hline(y=70, line_dash="dash", line_color="red",
                      annotation_text="Overbought", row=2, col=1)
        fig.add_hline(y=30, line_dash="dash", line_color="green",
                      annotation_text="Oversold", row=2, col=1)

    # Vortex panel
    if not ind.empty:
        fig.add_trace(go.Scatter(
            x=ind["timestamp"], y=ind["vortex_positive"],
            mode="lines", name="VI+", line=dict(color="#4CAF50", width=1.2)
        ), row=3, col=1)
        fig.add_trace(go.Scatter(
            x=ind["timestamp"], y=ind["vortex_negative"],
            mode="lines", name="VI-", line=dict(color="#F44336", width=1.2)
        ), row=3, col=1)

    fig.update_layout(
        height=800, template="plotly_dark",
        title_text=f"{ticker_symbol} — Live Signal Dashboard",
        xaxis_rangeslider_visible=False,
        legend=dict(orientation="h", yanchor="bottom", y=1.02)
    )
    fig.show()

# Plot first ticker
plot_ticker_dashboard("AAPL")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deployment Instructions
# MAGIC
# MAGIC ### 1. Run the Pipeline Locally
# MAGIC Execute cells 2–10 sequentially. The pipeline ingests data, computes indicators, generates signals, and updates the portfolio.
# MAGIC
# MAGIC ### 2. Git Repository Structure
# MAGIC Organize the project files for DABs deployment:
# MAGIC ```
# MAGIC stock-portfolio/                       # git repo root
# MAGIC ├── databricks.yml                     # Bundle config (includes resources/*.yml)
# MAGIC ├── resources/                         # DABs resource definitions
# MAGIC │   ├── stock_signal_app.yml            # Streamlit app resource
# MAGIC │   └── stock_signal_pipeline.yml       # Scheduled job resource
# MAGIC ├── src/                               # Source code
# MAGIC │   ├── notebooks/                     # Pipeline notebooks
# MAGIC │   │   └── live_stock_signal_dashboard.py
# MAGIC │   ├── pipelines/                     # DLT pipelines (future)
# MAGIC │   └── dashboards/                    # AI/BI dashboards (future)
# MAGIC ├── app/                               # Streamlit app
# MAGIC │   ├── streamlit_app.py
# MAGIC │   ├── app.yaml
# MAGIC │   └── requirements.txt
# MAGIC └── .github/workflows/deploy.yml       # CI/CD
# MAGIC ```
# MAGIC
# MAGIC ### 3. Deploy via Databricks Asset Bundles (CLI)
# MAGIC ```bash
# MAGIC pip install databricks-cli
# MAGIC databricks auth login --host https://<workspace>.azuredatabricks.net
# MAGIC databricks bundle validate
# MAGIC databricks bundle deploy -t dev --var="warehouse_id=<YOUR_WAREHOUSE_ID>"
# MAGIC databricks bundle run stock_signal_app -t dev
# MAGIC ```
# MAGIC
# MAGIC ### 4. Deploy via GitHub Actions
# MAGIC Add these **GitHub Secrets** to your repository:
# MAGIC | Secret | Value |
# MAGIC |--------|-------|
# MAGIC | `DATABRICKS_TOKEN` | Service principal access token |
# MAGIC | `DATABRICKS_HOST` | `https://<workspace>.azuredatabricks.net` |
# MAGIC | `DATABRICKS_WAREHOUSE_ID` | SQL warehouse ID |
# MAGIC
# MAGIC Then:
# MAGIC * Push to `dev` branch → auto-deploys to **dev** target
# MAGIC * Push to `main` branch → auto-deploys to **prod** target
# MAGIC * Open a PR → validates the bundle (no deploy)
# MAGIC * Use **Actions → Run workflow** for manual target selection
# MAGIC
# MAGIC ### 5. Continuous Streaming Mode
# MAGIC Change `trigger(availableNow=True)` to `trigger(processingTime="60 seconds")` in Cell 7 for live streaming.
# MAGIC
# MAGIC ### 6. Note on Stateful API
# MAGIC This notebook uses `applyInPandasWithState` (PySpark 3.4+). For Spark 4.0+ runtimes (DBR 16.2+), upgrade to `transformWithStateInPandas` with `StatefulProcessor` for ListState, MapState, TTL, and timers.
