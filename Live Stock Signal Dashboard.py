# Databricks notebook source
# DBTITLE 1,Project Initialization and Delta Setup
# Unity Catalog Discovery Step
# Find available catalogs, then use a valid one for table creation

display(spark.sql("SHOW CATALOGS"))

# After you identify your catalog, set catalog_name and rerun schema/table creation.
# For example:
#   catalog_name = 'your_catalog' (from the output above)
# Continue once the catalog name is known.

# COMMAND ----------

# DBTITLE 1,Create Unity Catalog, Schema, and Delta Tables
# Setup Unity Catalog, Schema, and Delta Tables

# Names
catalog_name = "stockapp"
schema_name = "production"

# Create new catalog and schema
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")

# Bronze table: raw market data
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.raw_market_data (
  ticker STRING,
  timestamp TIMESTAMP,
  open DOUBLE,
  high DOUBLE,
  low DOUBLE,
  close DOUBLE,
  volume LONG
) USING DELTA
""")

# Silver table: technical indicators
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.technical_indicators (
  ticker STRING,
  timestamp TIMESTAMP,
  sma20 DOUBLE,
  sma50 DOUBLE,
  rsi DOUBLE,
  vortex_positive DOUBLE,
  vortex_negative DOUBLE
) USING DELTA
""")

# Gold table: signals
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.trade_signals (
  ticker STRING,
  timestamp TIMESTAMP,
  signal STRING,
  sma20 DOUBLE,
  sma50 DOUBLE,
  rsi DOUBLE,
  vortex_positive DOUBLE,
  vortex_negative DOUBLE
) USING DELTA
""")

# Gold table: portfolio state
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.portfolio_state (
  ticker STRING,
  holdings LONG,
  avg_entry_price DOUBLE,
  current_price DOUBLE,
  unrealized_pnl DOUBLE,
  last_signal STRING,
  last_signal_time TIMESTAMP
) USING DELTA
""")

print(f"Catalog: {catalog_name}, Schema: {schema_name} and all Delta tables were created.")

# COMMAND ----------

# DBTITLE 1,Modular Structure and Streaming Ingest from Yahoo Finance
# ── Configuration ────────────────────────────────────────────────
CATALOG   = "stockapp"
SCHEMA    = "production"
BRONZE    = f"{CATALOG}.{SCHEMA}.raw_market_data"
SILVER    = f"{CATALOG}.{SCHEMA}.technical_indicators"
GOLD_SIG  = f"{CATALOG}.{SCHEMA}.trade_signals"
GOLD_PORT = f"{CATALOG}.{SCHEMA}.portfolio_state"
CHECKPOINT_VOL = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints"

# Ticker mapping: display_name → Yahoo Finance symbol
# Canadian ETFs need .TO (TSX); MSFT/NVDA use US listing
TICKER_MAP = {
    "MSFT": "MSFT",        # Microsoft CDR (CAD Hedged) — US ticker
    "NVDA": "NVDA",        # Nvidia CDR (CAD Hedged) — US ticker
    "TEC":  "TEC.TO",      # TD Global Tech Leaders Index ETF
    "VCNS": "VCNS.TO",     # Vanguard Conservative ETF Portfolio
    "XAW":  "XAW.TO",      # iShares Core MSCI All Country World ex Canada
    "XDG":  "XDG.TO",      # iShares Core MSCI Global Quality Dividend
    "XDV":  "XDV.TO",      # iShares Canadian Select Dividend Index ETF
    "XEG":  "XEG.TO",      # iShares S&P/TSX Capped Energy Index ETF
    "XGD":  "XGD.TO",      # iShares S&P/TSX Global Gold Index ETF
    "XIC":  "XIC.TO",      # iShares Core S&P/TSX Capped Composite
    "XUS":  "XUS.TO",      # iShares Core S&P 500 Index ETF (CAD)
    "ZEB":  "ZEB.TO",      # BMO Equal Weight Banks Index ETF
    "ZRE":  "ZRE.TO",      # BMO Equal Weight REITs Index ETF
}
TICKERS = list(TICKER_MAP.keys())  # display names used in Delta tables

# Indicator parameters
SMA_SHORT      = 20
SMA_LONG       = 50
RSI_PERIOD     = 14
VORTEX_PERIOD  = 14
RSI_OVERBOUGHT = 70
RSI_OVERSOLD   = 30
BUY_SHARES     = 100  # shares per BUY signal

print(f"Config loaded: {CATALOG}.{SCHEMA} | Tickers: {TICKERS}")

# COMMAND ----------

# DBTITLE 1,requirements.txt content
# ── Create Unity Catalog Resources ───────────────────────────
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
  ticker    STRING,
  timestamp TIMESTAMP,
  close     DOUBLE,
  sma20     DOUBLE,
  sma50     DOUBLE,
  rsi       DOUBLE,
  vortex_positive DOUBLE,
  vortex_negative DOUBLE
) USING DELTA
""")

# Gold: trade signals (BUY + SELL)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_SIG} (
  ticker    STRING,
  timestamp TIMESTAMP,
  signal    STRING,
  close     DOUBLE,
  sma20     DOUBLE,
  sma50     DOUBLE,
  rsi       DOUBLE,
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

# Checkpoint volume
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.checkpoints")

# ── Clear old data (switching tickers) ───────────────────────
for tbl in [BRONZE, SILVER, GOLD_SIG, GOLD_PORT]:
    spark.sql(f"TRUNCATE TABLE {tbl}")
    print(f"  Truncated {tbl}")

# Clear streaming checkpoint so stateful processor resets
import shutil, os
chk_path = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints/indicators"
if os.path.exists(chk_path):
    shutil.rmtree(chk_path)
    print(f"  Cleared checkpoint: {chk_path}")

print(f"\nAll UC resources ready: {CATALOG}.{SCHEMA}")
print(f"Tables: raw_market_data, technical_indicators, trade_signals, portfolio_state")

# COMMAND ----------

# DBTITLE 1,Install Pypi dependencies (pip)
# ── Yahoo Finance Batch Ingest → Bronze ────────────────────
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta

def ingest_tickers(ticker_map, period="7d", interval="5m"):
    """Fetch OHLCV data for all tickers and write to Bronze Delta table.
    
    Args:
        ticker_map: dict mapping display_name -> Yahoo Finance symbol
        period: yfinance period string (e.g. '7d' for past week)
        interval: bar interval (e.g. '5m' for 5-minute bars)
    """
    frames = []
    for display_name, yf_symbol in ticker_map.items():
        try:
            hist = yf.Ticker(yf_symbol).history(period=period, interval=interval)
            if hist.empty:
                print(f"  ⚠ {display_name} ({yf_symbol}): no data returned")
                continue
            df = hist.reset_index().rename(columns={
                "Datetime": "timestamp", "Date": "timestamp",
                "Open": "open", "High": "high", "Low": "low",
                "Close": "close", "Volume": "volume"
            })
            # Store clean display name (not Yahoo suffix)
            df["ticker"] = display_name
            df = df[["ticker", "timestamp", "open", "high", "low", "close", "volume"]]
            # Remove timezone info for Delta compatibility
            df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.tz_localize(None)
            df["volume"] = df["volume"].astype("int64")
            frames.append(df)
            print(f"  ✓ {display_name} ({yf_symbol}): {len(df)} rows")
        except Exception as e:
            print(f"  ✗ {display_name} ({yf_symbol}): {e}")

    if not frames:
        print("No data ingested.")
        return 0

    pdf = pd.concat(frames, ignore_index=True)
    sdf = spark.createDataFrame(pdf)
    sdf.write.format("delta").mode("append").saveAsTable(BRONZE)
    print(f"\n✔ Ingested {len(pdf)} total rows to {BRONZE}")
    return len(pdf)

# Run ingest: 7 days of 5-minute bars
print("Ingesting Yahoo Finance data (past week)...")
row_count = ingest_tickers(TICKER_MAP, period="7d", interval="5m")

# COMMAND ----------

# DBTITLE 1,Asset Structure Definition
# Project Structure
"""
stockapp/
├── src/
│   ├── streaming/
│   │   ├── yahoo_ingest.py           # Streaming ingest from Yahoo Finance
│   │   ├── stateful_indicators.py    # TransformWithState indicator logic
│   │   └── signal_portfolio.py       # Signal detection and portfolio management
│   ├── indicators/indicators.py      # SMA, RSI, Vortex calculation
│   └── utils/delta_utils.py          # Delta table utilities
├── streamlit_app.py                  # Databricks Streamlit UI (dashboard)
├── requirements.txt                  # Python dependencies
└── README.md                         # Run & deployment instructions
"""

# Each module will be implemented and tested in notebook cells.
# Once verified, modules can then be copied to file assets for final deployment.
print("Project structure defined. Ready for modular asset implementation.")

# COMMAND ----------

# DBTITLE 1,Stateful Indicator Logic and Streaming (TransformWithState)
# ── Run Streaming Pipeline: Bronze → Silver (indicators) + Gold (signals) ──
from pyspark.sql import functions as F

def write_indicators_and_signals(batch_df, epoch_id):
    """foreachBatch: write indicators to Silver, BUY+SELL signals to Gold."""
    if batch_df.isEmpty():
        return

    # --- Write to Silver ---
    batch_df.write.format("delta").mode("append").saveAsTable(SILVER)

    # --- Generate BUY signals ---
    buy_signals = batch_df.filter(
        (F.col("sma20") > F.col("sma50")) &
        (F.col("rsi").isNotNull()) & (F.col("rsi") < RSI_OVERBOUGHT) &
        (F.col("vortex_positive").isNotNull()) &
        (F.col("vortex_positive") > F.col("vortex_negative"))
    ).withColumn("signal", F.lit("BUY"))

    # --- Generate SELL signals ---
    sell_signals = batch_df.filter(
        (F.col("sma20") < F.col("sma50")) &
        (F.col("rsi").isNotNull()) & (F.col("rsi") > RSI_OVERSOLD) &
        (F.col("vortex_positive").isNotNull()) &
        (F.col("vortex_positive") < F.col("vortex_negative"))
    ).withColumn("signal", F.lit("SELL"))

    # Combine and write all signals
    all_signals = buy_signals.unionByName(sell_signals)
    sig_count = all_signals.count()
    buy_count = buy_signals.count()
    sell_count = sell_signals.count()

    if sig_count > 0:
        all_signals.select(
            "ticker", "timestamp", "signal", "close",
            "sma20", "sma50", "rsi", "vortex_positive", "vortex_negative"
        ).write.format("delta").mode("append").saveAsTable(GOLD_SIG)

    print(f"  Epoch {epoch_id}: {batch_df.count()} indicators, "
          f"{buy_count} BUY + {sell_count} SELL signals")


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

# DBTITLE 1,Create Unity Catalog Volume: silver_checkpoints
# ── Portfolio Tracking: MERGE from signals + latest prices ────────────

# Step 1: Build source data (latest price per ticker + latest signal)
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
# BUY  → open new position (if not exists) or keep existing
# SELL → close position (set holdings to 0)
spark.sql(f"""
MERGE INTO {GOLD_PORT} AS target
USING portfolio_updates AS source
ON target.ticker = source.ticker
WHEN MATCHED AND source.last_signal = 'SELL' THEN UPDATE SET
  target.current_price    = source.current_price,
  target.unrealized_pnl   = 0,
  target.holdings         = 0,
  target.last_signal      = source.last_signal,
  target.last_signal_time = source.last_signal_time
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
