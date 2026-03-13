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

# DBTITLE 1,Create Missing Audit Tables (SQL)
# MAGIC %sql
# MAGIC -- Verify all tables exist and check row counts.
# MAGIC -- Safe to re-run anytime. No data modification.
# MAGIC
# MAGIC SELECT 'raw_market_data' AS tbl, COUNT(*) AS rows FROM stockapp.production.raw_market_data
# MAGIC UNION ALL
# MAGIC SELECT 'technical_indicators', COUNT(*) FROM stockapp.production.technical_indicators
# MAGIC UNION ALL
# MAGIC SELECT 'trade_signals', COUNT(*) FROM stockapp.production.trade_signals
# MAGIC UNION ALL
# MAGIC SELECT 'portfolio_state', COUNT(*) FROM stockapp.production.portfolio_state
# MAGIC UNION ALL
# MAGIC SELECT 'trade_log', COUNT(*) FROM stockapp.production.trade_log
# MAGIC UNION ALL
# MAGIC SELECT 'data_quality_events', COUNT(*) FROM stockapp.production.data_quality_events

# COMMAND ----------

# DBTITLE 1,Create Unity Catalog, Schema, and Delta Tables
# MAGIC %pip install yfinance numpy --quiet

# COMMAND ----------

# DBTITLE 1,Modular Structure and Streaming Ingest from Yahoo Finance
# ── Configuration ────────────────────────────────────────────────
CATALOG   = "stockapp"
SCHEMA    = "production"
BRONZE    = f"{CATALOG}.{SCHEMA}.raw_market_data"
SILVER    = f"{CATALOG}.{SCHEMA}.technical_indicators"
GOLD_SIG  = f"{CATALOG}.{SCHEMA}.trade_signals"
GOLD_PORT = f"{CATALOG}.{SCHEMA}.portfolio_state"
TRADE_LOG = f"{CATALOG}.{SCHEMA}.trade_log"
DQ_EVENTS = f"{CATALOG}.{SCHEMA}.data_quality_events"
CHECKPOINT_VOL = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints"

# Ticker mapping: display_name → Yahoo Finance symbol
# CDRs use .NE (NEO Exchange); TSX ETFs use .TO
TICKER_MAP = {
    "MSFT": "MSFT.NE",    # Microsoft CDR (CAD Hedged) — NEO Exchange
    "NVDA": "NVDA.NE",    # Nvidia CDR (CAD Hedged) — NEO Exchange
    "TEC":  "TEC.TO",     # TD Global Tech Leaders Index ETF
    "VCNS": "VCNS.TO",    # Vanguard Conservative ETF Portfolio
    "XAW":  "XAW.TO",     # iShares Core MSCI All Country World ex Canada Index ETF
    "XDG":  "XDG.TO",     # iShares Core MSCI Global Quality Dividend Index ETF
    "XDV":  "XDV.TO",     # iShares Canadian Select Dividend Index ETF
    "XEG":  "XEG.TO",     # iShares S&P/TSX Capped Energy Index ETF
    "XGD":  "XGD.TO",     # iShares S&P/TSX Global Gold Index ETF
    "XIC":  "XIC.TO",     # iShares Core S&P/TSX Capped Composite Index ETF
    "XUS":  "XUS.TO",     # iShares Core S&P 500 Index ETF (CAD)
    "ZEB":  "ZEB.TO",     # BMO Equal Weight Banks Index ETF
    "ZRE":  "ZRE.TO",     # BMO Equal Weight REITs Index ETF
}
TICKERS = list(TICKER_MAP.keys())  # display names used in Delta tables

# ── Data Parameters ───────────────────────────────────────────
INGEST_PERIOD    = "1mo"   # Past 1 month of history
BAR_INTERVAL_MIN = 10      # 10-minute bars (resampled from 5m yfinance data)

# Indicator parameters
SMA_SHORT      = 20
SMA_LONG       = 50
RSI_PERIOD     = 14
VORTEX_PERIOD  = 14
RSI_OVERBOUGHT = 70
RSI_OVERSOLD   = 30
BUY_SHARES     = 100  # shares per BUY signal

# ── Risk Management Parameters ──────────────────────────────────
STOP_LOSS_PCT    = 0.03   # 3% stop-loss from entry price
TAKE_PROFIT_PCT  = 0.06   # 6% take-profit from entry price (2:1 reward-risk)
SIGNAL_COOLDOWN_BARS = 3  # Suppress same-direction signals within 3 bars (30 min at 10m)
MAX_POSITION_PCT = 0.15   # Max 15% portfolio allocation per ticker

# ── Signal Scoring Weights (must sum to 1.0) ────────────────────
W_SMA    = 0.35   # SMA crossover strength
W_RSI    = 0.35   # RSI distance from threshold
W_VORTEX = 0.30   # Vortex spread

print(f"Config loaded: {CATALOG}.{SCHEMA} | {len(TICKERS)} tickers: {TICKERS}")
print(f"CDRs: MSFT.NE, NVDA.NE | TSX ETFs: *.TO")
print(f"Data: {INGEST_PERIOD} history, {BAR_INTERVAL_MIN}m bars (resampled from 5m)")
print(f"Risk: SL={STOP_LOSS_PCT:.0%}, TP={TAKE_PROFIT_PCT:.0%}, Cooldown={SIGNAL_COOLDOWN_BARS} bars ({SIGNAL_COOLDOWN_BARS * BAR_INTERVAL_MIN}min)")

# COMMAND ----------

# DBTITLE 1,requirements.txt content
# ── Create Unity Catalog Resources (SAFE — idempotent, no data loss) ────
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# Bronze: raw OHLCV
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {BRONZE} (
  ticker    STRING,
  timestamp TIMESTAMP,
  open      DOUBLE,
  high      DOUBLE,
  low       DOUBLE,
  close     DOUBLE,
  volume    LONG
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

# Gold: trade signals — with signal_score + risk levels
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_SIG} (
  ticker          STRING,
  timestamp       TIMESTAMP,
  signal          STRING,
  signal_score    DOUBLE   COMMENT 'Composite score 0-100 from indicator distances',
  close           DOUBLE,
  sma20           DOUBLE,
  sma50           DOUBLE,
  rsi             DOUBLE,
  vortex_positive DOUBLE,
  vortex_negative DOUBLE,
  stop_loss       DOUBLE   COMMENT 'Auto-exit price: entry * (1 -/+ stop_loss_pct)',
  take_profit     DOUBLE   COMMENT 'Auto-exit price: entry * (1 +/- take_profit_pct)'
) USING DELTA
""")

# Gold: portfolio state — with realized PnL + trade count
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_PORT} (
  ticker          STRING,
  holdings        LONG,
  avg_entry_price DOUBLE,
  current_price   DOUBLE,
  unrealized_pnl  DOUBLE,
  realized_pnl    DOUBLE   COMMENT 'Cumulative realized PnL from closed trades',
  total_trades    INT      COMMENT 'Total BUY+SELL trades executed',
  last_signal     STRING,
  last_signal_time TIMESTAMP
) USING DELTA
""")

# Trade log — append-only audit trail (NEVER truncated)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TRADE_LOG} (
  trade_id        BIGINT GENERATED ALWAYS AS IDENTITY,
  ticker          STRING,
  signal          STRING   COMMENT 'BUY, SELL, STOP_LOSS, TAKE_PROFIT',
  signal_score    DOUBLE,
  price           DOUBLE,
  quantity        LONG,
  pnl_realized    DOUBLE   COMMENT 'PnL for SELL/STOP_LOSS/TAKE_PROFIT trades',
  stop_loss       DOUBLE,
  take_profit     DOUBLE,
  timestamp       TIMESTAMP,
  created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) USING DELTA
COMMENT 'Immutable trade log. Never truncated. Source of truth for performance metrics.'
""")

# Data quality events — operational observability (NEVER truncated)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {DQ_EVENTS} (
  event_id    BIGINT GENERATED ALWAYS AS IDENTITY,
  event_type  STRING   COMMENT 'MISSING_BARS, STALE_DATA, API_ERROR, DUPLICATE_DETECTED, GAP_DETECTED, INVALID_PRICE, EXTREME_SPIKE_REMOVED',
  ticker      STRING,
  message     STRING,
  bar_count   INT      COMMENT 'Expected vs actual bar count context',
  detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) USING DELTA
COMMENT 'Data quality monitoring events. Append-only.'
""")

# Checkpoint volume
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.checkpoints")

# ── Schema Evolution: add new columns to existing tables safely ──────
try:
    spark.sql(f"ALTER TABLE {GOLD_SIG} ADD COLUMNS (signal_score DOUBLE, stop_loss DOUBLE, take_profit DOUBLE)")
    print("  Added new columns to trade_signals")
except Exception:
    pass  # Columns already exist

try:
    spark.sql(f"ALTER TABLE {GOLD_PORT} ADD COLUMNS (realized_pnl DOUBLE, total_trades INT)")
    print("  Added new columns to portfolio_state")
except Exception:
    pass  # Columns already exist

print(f"\n✔ All UC resources ready: {CATALOG}.{SCHEMA}")
print(f"Tables: raw_market_data, technical_indicators, trade_signals, portfolio_state")
print(f"Audit: trade_log (immutable), data_quality_events (immutable)")
print(f"\n⚠ To reset transient data, run the DANGER ZONE cell below (requires confirmation).")

# COMMAND ----------

# DBTITLE 1,DANGER ZONE: Reset Transient Data
# ─────────────────────────────────────────────────────────────────
# ⚠️  DANGER ZONE: TRUNCATE ALL TRANSIENT TABLES + CLEAR CHECKPOINTS
#     This DESTROYS all market data, indicators, signals, and portfolio state.
#     trade_log and data_quality_events are NEVER truncated (audit trail).
#
#     Set CONFIRM_RESET = True to enable. Default is False (safe).
# ─────────────────────────────────────────────────────────────────

CONFIRM_RESET = False  # ← Change to True and run cell to reset

if CONFIRM_RESET:
    import shutil, os
    
    print("⚠️  RESETTING ALL TRANSIENT DATA...")
    
    # Truncate transient tables (Bronze, Silver, Gold signals, Gold portfolio)
    for tbl in [BRONZE, SILVER, GOLD_SIG, GOLD_PORT]:
        spark.sql(f"TRUNCATE TABLE {tbl}")
        print(f"  ✗ Truncated {tbl}")
    
    # Clear streaming checkpoints so stateful processor resets
    chk_path = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints/indicators"
    if os.path.exists(chk_path):
        shutil.rmtree(chk_path)
        print(f"  ✗ Cleared checkpoint: {chk_path}")
    
    print(f"\n⚠️  Reset complete. trade_log and data_quality_events were NOT touched.")
    print(f"  Re-run the pipeline (cells 5→8→9) to repopulate data.")
    
    # Auto-reset to prevent accidental re-runs
    CONFIRM_RESET = False
else:
    print("✓ CONFIRM_RESET is False — no data was deleted.")
    print("  Set CONFIRM_RESET = True and re-run this cell to reset transient data.")

# COMMAND ----------

# DBTITLE 1,Install Pypi dependencies (pip)
# ── Yahoo Finance Batch Ingest → Bronze (DEDUP MERGE + CIRCUIT BREAKER) ────
# Fetches 5m data from yfinance and resamples to 10m bars (no native 10m interval).
import yfinance as yf
import pandas as pd
import numpy as np
import time as _time
from datetime import datetime, timedelta

# ── Circuit Breaker Thresholds ────────────────────────────────────────
MAX_BAR_PCT_CHANGE = 0.20   # Flag bars with >20% price change from previous bar
MIN_VALID_PRICE    = 0.01   # Reject rows with price <= this (catches zeros and negatives)

def _fetch_with_retry(yf_symbol, period, interval, max_retries=3):
    """Fetch from Yahoo Finance with exponential backoff."""
    for attempt in range(max_retries):
        try:
            hist = yf.Ticker(yf_symbol).history(period=period, interval=interval)
            return hist
        except Exception as e:
            if attempt < max_retries - 1:
                wait = 2 ** attempt  # 1s, 2s, 4s
                print(f"    Retry {attempt+1}/{max_retries} for {yf_symbol} in {wait}s: {e}")
                _time.sleep(wait)
            else:
                raise

def _resample_to_10m(df):
    """Resample 5-minute bars to 10-minute OHLCV bars.
    
    yfinance has no native 10m interval, so we fetch 5m and aggregate:
      Open   = first open in 10m window
      High   = max high in 10m window
      Low    = min low in 10m window
      Close  = last close in 10m window
      Volume = sum of volume in 10m window
    """
    df = df.sort_values("timestamp")
    df = df.set_index("timestamp")
    
    resampled = df.resample("10min").agg({
        "ticker": "first",
        "open":   "first",
        "high":   "max",
        "low":    "min",
        "close":  "last",
        "volume": "sum",
    }).dropna(subset=["close"])  # Drop empty windows (non-trading hours)
    
    resampled = resampled.reset_index()
    return resampled

def _apply_circuit_breaker(df, ticker):
    """Validate price data and flag/remove anomalous rows.
    
    Returns (clean_df, dq_events_list) where:
      - clean_df has invalid rows removed
      - dq_events_list contains logged anomalies
    """
    dq = []
    original_count = len(df)
    
    # 1) Remove NaN/null prices (yfinance can return these)
    nan_mask = df[["open", "high", "low", "close"]].isna().any(axis=1)
    nan_count = nan_mask.sum()
    if nan_count > 0:
        dq.append({
            "event_type": "INVALID_PRICE",
            "ticker": ticker,
            "message": f"{nan_count} rows with NaN prices removed",
            "bar_count": int(nan_count)
        })
        df = df[~nan_mask].copy()
    
    # 2) Remove rows with zero/negative prices
    invalid_price_mask = (
        (df["close"] <= MIN_VALID_PRICE) |
        (df["open"]  <= MIN_VALID_PRICE) |
        (df["high"]  <= MIN_VALID_PRICE) |
        (df["low"]   <= MIN_VALID_PRICE)
    )
    invalid_count = invalid_price_mask.sum()
    if invalid_count > 0:
        dq.append({
            "event_type": "INVALID_PRICE",
            "ticker": ticker,
            "message": f"{invalid_count} rows with price <= {MIN_VALID_PRICE} removed",
            "bar_count": int(invalid_count)
        })
        df = df[~invalid_price_mask].copy()
    
    # 3) Remove rows with negative volume
    neg_vol_mask = df["volume"] < 0
    neg_vol_count = neg_vol_mask.sum()
    if neg_vol_count > 0:
        dq.append({
            "event_type": "INVALID_VOLUME",
            "ticker": ticker,
            "message": f"{neg_vol_count} rows with negative volume removed",
            "bar_count": int(neg_vol_count)
        })
        df = df[~neg_vol_mask].copy()
    
    # 4) OHLC consistency: high must be >= low
    ohlc_bad = (df["high"] < df["low"])
    ohlc_count = ohlc_bad.sum()
    if ohlc_count > 0:
        dq.append({
            "event_type": "OHLC_INCONSISTENCY",
            "ticker": ticker,
            "message": f"{ohlc_count} rows where high < low removed",
            "bar_count": int(ohlc_count)
        })
        df = df[~ohlc_bad].copy()
    
    # 5) Flag anomalous price swings; remove extreme spikes (>50%)
    if len(df) > 1:
        df = df.sort_values("timestamp")
        pct_change = df["close"].pct_change().abs()
        spike_mask = pct_change > MAX_BAR_PCT_CHANGE
        spike_count = spike_mask.sum()
        if spike_count > 0:
            spike_values = pct_change[spike_mask].tolist()
            worst = max(spike_values)
            extreme_mask = pct_change > 0.50
            extreme_count = extreme_mask.sum()
            if extreme_count > 0:
                dq.append({
                    "event_type": "EXTREME_SPIKE_REMOVED",
                    "ticker": ticker,
                    "message": f"{extreme_count} rows with >50% bar-to-bar change removed (max: {worst:.1%})",
                    "bar_count": int(extreme_count)
                })
                df = df[~extreme_mask].copy()
            if spike_count - extreme_count > 0:
                dq.append({
                    "event_type": "PRICE_SPIKE_FLAGGED",
                    "ticker": ticker,
                    "message": f"{spike_count - extreme_count} bars with {MAX_BAR_PCT_CHANGE:.0%}-50% change flagged (kept). Worst: {worst:.1%}",
                    "bar_count": int(spike_count - extreme_count)
                })
    
    removed = original_count - len(df)
    if removed > 0:
        print(f"    ⚠ Circuit breaker: removed {removed}/{original_count} invalid rows for {ticker}")
    
    return df, dq


def ingest_tickers(ticker_map, period="1mo", interval="5m"):
    """Fetch OHLCV data, resample to 10m bars, and MERGE into Bronze.
    
    Pipeline: Fetch 5m → Resample 10m → Circuit Breaker → MERGE (dedup) → DQ Log
    
    Args:
        ticker_map: dict mapping display_name -> Yahoo Finance symbol
        period: yfinance period (default '1mo' for past month)
        interval: yfinance fetch interval (default '5m', resampled to 10m)
    """
    frames = []
    dq_events = []  # Collect data quality issues
    
    for display_name, yf_symbol in ticker_map.items():
        try:
            hist = _fetch_with_retry(yf_symbol, period, interval)
            if hist.empty:
                print(f"  ⚠ {display_name} ({yf_symbol}): no data returned")
                dq_events.append({
                    "event_type": "MISSING_BARS",
                    "ticker": display_name,
                    "message": f"Yahoo Finance returned no data for {yf_symbol} (period={period})",
                    "bar_count": 0
                })
                continue
            
            raw_count = len(hist)
            df = hist.reset_index().rename(columns={
                "Datetime": "timestamp", "Date": "timestamp",
                "Open": "open", "High": "high", "Low": "low",
                "Close": "close", "Volume": "volume"
            })
            df["ticker"] = display_name
            df = df[["ticker", "timestamp", "open", "high", "low", "close", "volume"]]
            df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.tz_localize(None)
            df["volume"] = df["volume"].astype("int64")
            
            # ── RESAMPLE: 5m → 10m bars ──────────────────────────────
            df = _resample_to_10m(df)
            df["volume"] = df["volume"].astype("int64")
            
            # ── CIRCUIT BREAKER: validate before writing ───────────────
            df, ticker_dq = _apply_circuit_breaker(df, display_name)
            dq_events.extend(ticker_dq)
            
            if df.empty:
                print(f"  ✗ {display_name}: all rows removed by circuit breaker")
                continue
            
            frames.append(df)
            print(f"  ✓ {display_name} ({yf_symbol}): {raw_count} 5m bars → {len(df)} 10m bars")
            
        except Exception as e:
            print(f"  ✗ {display_name} ({yf_symbol}): {e}")
            dq_events.append({
                "event_type": "API_ERROR",
                "ticker": display_name,
                "message": f"Yahoo Finance fetch failed after retries: {str(e)[:200]}",
                "bar_count": 0
            })

    if not frames:
        print("No data ingested.")
        return 0

    pdf = pd.concat(frames, ignore_index=True)
    sdf = spark.createDataFrame(pdf)
    
    # ── DEDUP MERGE instead of blind append ──────────────────────────
    sdf.createOrReplaceTempView("_bronze_staging")
    
    merge_result = spark.sql(f"""
    MERGE INTO {BRONZE} AS target
    USING _bronze_staging AS source
    ON target.ticker = source.ticker AND target.timestamp = source.timestamp
    WHEN MATCHED THEN UPDATE SET
      target.open   = source.open,
      target.high   = source.high,
      target.low    = source.low,
      target.close  = source.close,
      target.volume = source.volume
    WHEN NOT MATCHED THEN INSERT *
    """)
    
    # Extract merge metrics
    merge_metrics = merge_result.collect()[0]
    inserted = merge_metrics["num_inserted_rows"]
    updated  = merge_metrics["num_updated_rows"]
    
    if updated > 0:
        print(f"  ⚠ {updated} duplicate bars detected and updated (not re-inserted)")
        dq_events.append({
            "event_type": "DUPLICATE_DETECTED",
            "ticker": "ALL",
            "message": f"{updated} duplicate (ticker,timestamp) rows merged instead of duplicated",
            "bar_count": updated
        })
    
    # ── Log data quality events ─────────────────────────────────
    if dq_events:
        dq_df = spark.createDataFrame(dq_events)
        dq_df.write.format("delta").mode("append").saveAsTable(DQ_EVENTS)
        print(f"  Logged {len(dq_events)} data quality event(s) to {DQ_EVENTS}")
    
    print(f"\n✔ MERGE complete: {inserted} new + {updated} updated = {len(pdf)} total 10m bars processed")
    return len(pdf)

# Run ingest: 1 month of 10-minute bars (fetched as 5m, resampled)
print(f"Ingesting Yahoo Finance data ({INGEST_PERIOD} history, {BAR_INTERVAL_MIN}m bars)...")
row_count = ingest_tickers(TICKER_MAP, period=INGEST_PERIOD, interval="5m")

# COMMAND ----------

# DBTITLE 1,Data Quality Validation
# ── Data Quality Validation ───────────────────────────────────────
from pyspark.sql import functions as F
from datetime import datetime, timezone

# Expected: ~39 bars/day (6.5 trading hours × 6 bars/hr at 10m) × ~22 trading days/month
EXPECTED_BARS_PER_DAY = 39
TRADING_DAYS = 22  # ~1 month of trading days
MIN_EXPECTED_BARS = int(EXPECTED_BARS_PER_DAY * TRADING_DAYS * 0.7)  # 70% threshold
GAP_THRESHOLD_MINUTES = 25  # Flag gaps > 2 consecutive 10m bars

print(f"\n── Data Quality Checks ({BAR_INTERVAL_MIN}m bars, {INGEST_PERIOD} history) ──")
dq_issues = []

# 1️⃣ Bar count per ticker
bar_counts = spark.sql(f"""
  SELECT ticker,
         COUNT(*) AS bar_count,
         MIN(timestamp) AS first_bar,
         MAX(timestamp) AS last_bar
  FROM {BRONZE}
  GROUP BY ticker
  ORDER BY ticker
""").collect()

print(f"\n  Bar counts (min expected: {MIN_EXPECTED_BARS}):")
for row in bar_counts:
    status = "✓" if row["bar_count"] >= MIN_EXPECTED_BARS else "⚠"
    print(f"    {status} {row['ticker']}: {row['bar_count']} bars "
          f"({row['first_bar']} → {row['last_bar']})")
    if row["bar_count"] < MIN_EXPECTED_BARS:
        dq_issues.append({
            "event_type": "MISSING_BARS",
            "ticker": row["ticker"],
            "message": f"Only {row['bar_count']} bars (expected >={MIN_EXPECTED_BARS})",
            "bar_count": row["bar_count"]
        })

# 2️⃣ Gap detection: find gaps > GAP_THRESHOLD_MINUTES between consecutive bars
gaps = spark.sql(f"""
  WITH ordered AS (
    SELECT ticker, timestamp,
           LAG(timestamp) OVER (PARTITION BY ticker ORDER BY timestamp) AS prev_ts
    FROM {BRONZE}
  )
  SELECT ticker, prev_ts, timestamp,
         TIMESTAMPDIFF(MINUTE, prev_ts, timestamp) AS gap_minutes
  FROM ordered
  WHERE prev_ts IS NOT NULL
    AND TIMESTAMPDIFF(MINUTE, prev_ts, timestamp) > {GAP_THRESHOLD_MINUTES}
    -- Exclude overnight gaps (only flag intraday gaps)
    AND CAST(prev_ts AS DATE) = CAST(timestamp AS DATE)
  ORDER BY gap_minutes DESC
  LIMIT 20
""").collect()

if gaps:
    print(f"\n  ⚠ Intraday gaps > {GAP_THRESHOLD_MINUTES} min detected:")
    for g in gaps[:10]:  # Show top 10
        print(f"    {g['ticker']}: {g['gap_minutes']}min gap "
              f"({g['prev_ts']} → {g['timestamp']})")
        dq_issues.append({
            "event_type": "GAP_DETECTED",
            "ticker": g["ticker"],
            "message": f"{g['gap_minutes']}min intraday gap: {g['prev_ts']} to {g['timestamp']}",
            "bar_count": g["gap_minutes"]
        })
else:
    print(f"\n  ✓ No intraday gaps > {GAP_THRESHOLD_MINUTES}min detected")

# 3️⃣ Stale data check: is the latest bar recent enough?
latest_bar = spark.sql(f"SELECT MAX(timestamp) AS latest FROM {BRONZE}").collect()[0]["latest"]
if latest_bar:
    now_utc = datetime.now(timezone.utc).replace(tzinfo=None)
    staleness_hours = (now_utc - latest_bar).total_seconds() / 3600
    if staleness_hours > 24:
        print(f"\n  ⚠ STALE DATA: Latest bar is {staleness_hours:.1f} hours old ({latest_bar})")
        dq_issues.append({
            "event_type": "STALE_DATA",
            "ticker": "ALL",
            "message": f"Latest bar is {staleness_hours:.1f}h old. Last: {latest_bar}",
            "bar_count": 0
        })
    else:
        print(f"\n  ✓ Data freshness OK: latest bar {staleness_hours:.1f}h ago ({latest_bar})")

# 4️⃣ Duplicate check (should be zero after MERGE)
duplicates = spark.sql(f"""
  SELECT ticker, timestamp, COUNT(*) AS cnt
  FROM {BRONZE}
  GROUP BY ticker, timestamp
  HAVING cnt > 1
""").count()

if duplicates > 0:
    print(f"\n  ✗ CRITICAL: {duplicates} duplicate (ticker, timestamp) rows found!")
    dq_issues.append({
        "event_type": "DUPLICATE_DETECTED",
        "ticker": "ALL",
        "message": f"{duplicates} duplicate rows in Bronze after MERGE (unexpected)",
        "bar_count": duplicates
    })
else:
    print(f"\n  ✓ Zero duplicates in Bronze (MERGE working correctly)")

# Log all DQ events
if dq_issues:
    dq_df = spark.createDataFrame(dq_issues)
    dq_df.write.format("delta").mode("append").saveAsTable(DQ_EVENTS)
    print(f"\n  Logged {len(dq_issues)} data quality event(s) to {DQ_EVENTS}")
else:
    print(f"\n  ✔ All data quality checks passed")

print(f"\n── Summary: {len(bar_counts)} tickers, {len(dq_issues)} issue(s) ──")

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
# ── Streaming Pipeline: Bronze → Silver (indicators) + Gold (signals) ─────────
# Enhanced with: Silver dedup MERGE, 2-of-3 SELL logic, signal scoring,
#                cooldown, stop-loss/take-profit
# FIX: ignoreDeletes + ignoreChanges to survive TRUNCATE & MERGE on Bronze

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType, LongType
)
import numpy as np

# ── Schemas for applyInPandasWithState ────────────────────────────
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

STATE_SCHEMA = StructType([
    StructField("closes", StringType()),    # JSON-encoded list of close prices
    StructField("highs",  StringType()),    # JSON-encoded list of high prices
    StructField("lows",   StringType()),    # JSON-encoded list of low prices
])

def compute_indicators(key, pdf_iter, state):
    """Stateful indicator computation per ticker.
    
    Maintains rolling history of OHLC in state to compute:
    - SMA(20), SMA(50)
    - RSI(14) using avg gain/loss
    - Vortex Indicator(14): VI+, VI-
    """
    import json
    ticker = key[0]
    
    # Restore state
    if state.exists:
        s = state.get
        closes = json.loads(s[0])
        highs  = json.loads(s[1])
        lows   = json.loads(s[2])
    else:
        closes, highs, lows = [], [], []

    for pdf in pdf_iter:
        pdf = pdf.sort_values("timestamp")
        results = []

        for _, row in pdf.iterrows():
            closes.append(float(row["close"]))
            highs.append(float(row["high"]))
            lows.append(float(row["low"]))

            # Keep rolling window (SMA_LONG is the longest lookback)
            max_hist = max(SMA_LONG, RSI_PERIOD + 1, VORTEX_PERIOD + 1) + 5
            if len(closes) > max_hist:
                closes = closes[-max_hist:]
                highs  = highs[-max_hist:]
                lows   = lows[-max_hist:]

            n = len(closes)
            c_arr = np.array(closes)

            # SMA
            sma20 = float(c_arr[-SMA_SHORT:].mean()) if n >= SMA_SHORT else None
            sma50 = float(c_arr[-SMA_LONG:].mean())  if n >= SMA_LONG  else None

            # RSI
            rsi = None
            if n > RSI_PERIOD:
                deltas = np.diff(c_arr[-(RSI_PERIOD + 1):])
                gains  = np.where(deltas > 0, deltas, 0)
                losses = np.where(deltas < 0, -deltas, 0)
                avg_gain = gains.mean()
                avg_loss = losses.mean()
                if avg_loss > 0:
                    rs  = avg_gain / avg_loss
                    rsi = float(100.0 - 100.0 / (1.0 + rs))
                else:
                    rsi = 100.0

            # Vortex Indicator
            vi_pos, vi_neg = None, None
            if n > VORTEX_PERIOD + 1:
                h = np.array(highs[-(VORTEX_PERIOD + 1):])
                l = np.array(lows[-(VORTEX_PERIOD + 1):])
                c_v = np.array(closes[-(VORTEX_PERIOD + 1):])

                tr = np.maximum(
                    h[1:] - l[1:],
                    np.maximum(
                        np.abs(h[1:] - c_v[:-1]),
                        np.abs(l[1:] - c_v[:-1])
                    )
                )
                vm_plus  = np.abs(h[1:] - l[:-1])
                vm_minus = np.abs(l[1:] - h[:-1])
                tr_sum = tr.sum()
                if tr_sum > 0:
                    vi_pos = float(vm_plus.sum()  / tr_sum)
                    vi_neg = float(vm_minus.sum() / tr_sum)

            results.append({
                "ticker":          ticker,
                "timestamp":       row["timestamp"],
                "close":           float(row["close"]),
                "sma20":           sma20,
                "sma50":           sma50,
                "rsi":             rsi,
                "vortex_positive": vi_pos,
                "vortex_negative": vi_neg,
            })

        if results:
            import pandas as _pd
            yield _pd.DataFrame(results)

    # Save state
    import json as _json
    state.update((
        _json.dumps(closes[-max_hist:]),
        _json.dumps(highs[-max_hist:]),
        _json.dumps(lows[-max_hist:]),
    ))


# ── Signal Scoring Function ────────────────────────────────────────
@F.udf(DoubleType())
def compute_signal_score(signal, sma20, sma50, rsi, vi_pos, vi_neg):
    """Compute composite signal strength score (0-100)."""
    if any(v is None for v in [sma20, sma50, rsi, vi_pos, vi_neg]):
        return 0.0
    
    if signal == "BUY":
        sma_spread = min((sma20 - sma50) / sma50 * 100, 5.0) / 5.0 * 100
        rsi_score = max(0, (70 - rsi) / 40) * 100
        vortex_spread = min((vi_pos - vi_neg), 0.3) / 0.3 * 100
    elif signal == "SELL":
        sma_spread = min((sma50 - sma20) / sma50 * 100, 5.0) / 5.0 * 100
        rsi_score = max(0, (rsi - 30) / 40) * 100
        vortex_spread = min((vi_neg - vi_pos), 0.3) / 0.3 * 100
    else:
        return 0.0
    
    sma_spread    = max(0.0, min(100.0, sma_spread))
    rsi_score     = max(0.0, min(100.0, rsi_score))
    vortex_spread = max(0.0, min(100.0, vortex_spread))
    
    return round(0.35 * sma_spread + 0.35 * rsi_score + 0.30 * vortex_spread, 1)


def write_indicators_and_signals(batch_df, epoch_id):
    """foreachBatch: MERGE indicators to Silver, scored signals to Gold."""
    if batch_df.isEmpty():
        return

    # --- SILVER: DEDUP MERGE instead of append ---
    batch_df.createOrReplaceTempView("_silver_staging")
    spark.sql(f"""
    MERGE INTO {SILVER} AS target
    USING _silver_staging AS source
    ON target.ticker = source.ticker AND target.timestamp = source.timestamp
    WHEN MATCHED THEN UPDATE SET
      target.close           = source.close,
      target.sma20           = source.sma20,
      target.sma50           = source.sma50,
      target.rsi             = source.rsi,
      target.vortex_positive = source.vortex_positive,
      target.vortex_negative = source.vortex_negative
    WHEN NOT MATCHED THEN INSERT *
    """)

    # --- Generate BUY signals (strict: ALL 3 conditions) ---
    buy_candidates = batch_df.filter(
        (F.col("sma20") > F.col("sma50")) &
        (F.col("rsi").isNotNull()) & (F.col("rsi") < RSI_OVERBOUGHT) &
        (F.col("vortex_positive").isNotNull()) &
        (F.col("vortex_positive") > F.col("vortex_negative"))
    ).withColumn("signal", F.lit("BUY"))

    # --- Generate SELL signals (relaxed: ANY 2 of 3 conditions) ---
    sell_base = batch_df.filter(
        F.col("sma20").isNotNull() & F.col("sma50").isNotNull() &
        F.col("rsi").isNotNull() &
        F.col("vortex_positive").isNotNull() & F.col("vortex_negative").isNotNull()
    ).withColumn(
        "_sell_sma",    F.when(F.col("sma20") < F.col("sma50"), F.lit(1)).otherwise(F.lit(0))
    ).withColumn(
        "_sell_rsi",    F.when(F.col("rsi") > RSI_OVERBOUGHT, F.lit(1)).otherwise(F.lit(0))
    ).withColumn(
        "_sell_vortex", F.when(F.col("vortex_positive") < F.col("vortex_negative"), F.lit(1)).otherwise(F.lit(0))
    ).withColumn(
        "_sell_count",  F.col("_sell_sma") + F.col("_sell_rsi") + F.col("_sell_vortex")
    )
    
    sell_candidates = sell_base.filter(
        F.col("_sell_count") >= 2
    ).withColumn(
        "signal", F.lit("SELL")
    ).drop("_sell_sma", "_sell_rsi", "_sell_vortex", "_sell_count")

    all_candidates = buy_candidates.unionByName(sell_candidates)
    
    if all_candidates.isEmpty():
        print(f"  Epoch {epoch_id}: {batch_df.count()} indicators merged to Silver, 0 signals")
        return

    # --- Compute signal strength score ---
    scored = all_candidates.withColumn(
        "signal_score",
        compute_signal_score(
            F.col("signal"), F.col("sma20"), F.col("sma50"),
            F.col("rsi"), F.col("vortex_positive"), F.col("vortex_negative")
        )
    )

    # --- Add stop-loss / take-profit levels ---
    scored = scored.withColumn(
        "stop_loss",
        F.when(F.col("signal") == "BUY",
               F.round(F.col("close") * (1 - STOP_LOSS_PCT), 4))
         .otherwise(F.round(F.col("close") * (1 + STOP_LOSS_PCT), 4))
    ).withColumn(
        "take_profit",
        F.when(F.col("signal") == "BUY",
               F.round(F.col("close") * (1 + TAKE_PROFIT_PCT), 4))
         .otherwise(F.round(F.col("close") * (1 - TAKE_PROFIT_PCT), 4))
    )

    # --- Cooldown filter: suppress signals within N bars of last same signal ---
    try:
        last_signals = spark.sql(f"""
            SELECT ticker, signal, MAX(timestamp) AS last_signal_time
            FROM {GOLD_SIG}
            GROUP BY ticker, signal
        """)
        
        scored = scored.join(
            last_signals.select(
                F.col("ticker").alias("_ls_ticker"),
                F.col("signal").alias("_ls_signal"),
                F.col("last_signal_time")
            ),
            (scored["ticker"] == F.col("_ls_ticker")) &
            (scored["signal"] == F.col("_ls_signal")),
            "left"
        )
        
        # FIX: Use BAR_INTERVAL_MIN (10m) instead of hardcoded 5m
        cooldown_seconds = SIGNAL_COOLDOWN_BARS * BAR_INTERVAL_MIN * 60
        scored = scored.filter(
            F.col("last_signal_time").isNull() |
            (F.unix_timestamp(F.col("timestamp")) - F.unix_timestamp(F.col("last_signal_time"))
             > cooldown_seconds)
        ).drop("_ls_ticker", "_ls_signal", "last_signal_time")
        
    except Exception:
        # First run: no existing signals, skip cooldown
        pass

    # --- Write surviving signals to Gold ---
    final_signals = scored.select(
        "ticker", "timestamp", "signal", "signal_score", "close",
        "sma20", "sma50", "rsi", "vortex_positive", "vortex_negative",
        "stop_loss", "take_profit"
    )
    
    sig_count = final_signals.count()
    if sig_count > 0:
        final_signals.write.format("delta").mode("append").saveAsTable(GOLD_SIG)
    
    buy_count  = final_signals.filter(F.col("signal") == "BUY").count()
    sell_count = final_signals.filter(F.col("signal") == "SELL").count()
    print(f"  Epoch {epoch_id}: {batch_df.count()} indicators merged, "
          f"{buy_count} BUY + {sell_count} SELL signals (after cooldown)")


# ── Read Bronze as stream ───────────────────────────────────────
# FIX: ignoreDeletes + ignoreChanges to survive TRUNCATE and MERGE on Bronze.
# Without these, the stream crashes with DELTA_SOURCE_IGNORE_DELETE after
# any TRUNCATE (which deletes Parquet files) or MERGE (which updates rows).
# The Silver MERGE dedup ensures no duplicate processing from reprocessed rows.
bronze_stream = (
    spark.readStream
    .format("delta")
    .option("ignoreDeletes", True)
    .option("ignoreChanges", True)
    .table(BRONZE)
)

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
# ── Portfolio Tracking: Trade Log + Stop-Loss/Take-Profit + Realized PnL ─────

# ======================== STEP 1: STOP-LOSS / TAKE-PROFIT AUTO-EXITS ========================
# Before processing new signals, check if any open positions have breached
# their stop-loss or take-profit levels based on current market prices.

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW _sl_tp_triggers AS
WITH current_prices AS (
  SELECT ticker, close AS current_price
  FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY timestamp DESC) AS rn
    FROM {BRONZE}
  ) WHERE rn = 1
),
open_positions AS (
  SELECT p.ticker, p.holdings, p.avg_entry_price, p.realized_pnl,
         p.total_trades, cp.current_price
  FROM {GOLD_PORT} p
  JOIN current_prices cp ON p.ticker = cp.ticker
  WHERE p.holdings > 0
),
active_signals AS (
  -- Get the most recent BUY signal per ticker (which has stop_loss/take_profit)
  SELECT ticker, stop_loss, take_profit, signal
  FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY timestamp DESC) AS rn
    FROM {GOLD_SIG}
    WHERE signal = 'BUY' AND stop_loss IS NOT NULL
  ) WHERE rn = 1
)
SELECT
  op.ticker,
  op.holdings,
  op.avg_entry_price,
  op.realized_pnl,
  op.total_trades,
  op.current_price,
  CASE
    WHEN op.current_price <= COALESCE(sig.stop_loss, 0)  THEN 'STOP_LOSS'
    WHEN op.current_price >= COALESCE(sig.take_profit, 999999) THEN 'TAKE_PROFIT'
  END AS exit_reason,
  (op.current_price - op.avg_entry_price) * op.holdings AS exit_pnl
FROM open_positions op
LEFT JOIN active_signals sig ON op.ticker = sig.ticker
WHERE op.current_price <= COALESCE(sig.stop_loss, 0)
   OR op.current_price >= COALESCE(sig.take_profit, 999999)
""")

# Log stop-loss / take-profit exits to trade_log
sl_tp_exits = spark.table("_sl_tp_triggers")
sl_tp_count = sl_tp_exits.count()

if sl_tp_count > 0:
    print(f"  ⚠ {sl_tp_count} stop-loss/take-profit trigger(s) detected:")
    
    # Write exit trades to trade_log
    exit_trades = sl_tp_exits.selectExpr(
        "ticker",
        "exit_reason AS signal",
        "NULL AS signal_score",
        "current_price AS price",
        "holdings AS quantity",
        "exit_pnl AS pnl_realized",
        "NULL AS stop_loss",
        "NULL AS take_profit",
        "current_timestamp() AS timestamp"
    )
    exit_trades.write.format("delta").mode("append").saveAsTable(TRADE_LOG)
    
    # Close positions in portfolio_state
    for row in sl_tp_exits.collect():
        prev_realized = row["realized_pnl"] or 0.0
        prev_trades   = row["total_trades"] or 0
        spark.sql(f"""
        UPDATE {GOLD_PORT}
        SET holdings        = 0,
            current_price   = {row['current_price']},
            unrealized_pnl  = 0,
            realized_pnl    = {prev_realized + row['exit_pnl']},
            total_trades    = {prev_trades + 1},
            last_signal     = '{row['exit_reason']}',
            last_signal_time = current_timestamp()
        WHERE ticker = '{row['ticker']}'
        """)
        print(f"    {row['exit_reason']}: {row['ticker']} @ ${row['current_price']:.2f} "
              f"(PnL: ${row['exit_pnl']:.2f})")
else:
    print("  ✓ No stop-loss/take-profit triggers")


# ======================== STEP 2: PROCESS NEW SIGNALS ========================
# Build source data: latest price + latest signal per ticker
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW _portfolio_updates AS
WITH latest_price AS (
  SELECT ticker, close AS current_price, timestamp
  FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY timestamp DESC) AS rn
    FROM {BRONZE}
  ) WHERE rn = 1
),
latest_signal AS (
  SELECT ticker, signal, signal_score, close AS signal_price,
         stop_loss, take_profit, timestamp AS signal_time
  FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY timestamp DESC) AS rn
    FROM {GOLD_SIG}
  ) WHERE rn = 1
)
SELECT
  p.ticker,
  p.current_price,
  s.signal        AS last_signal,
  s.signal_score,
  s.signal_price,
  s.stop_loss,
  s.take_profit,
  s.signal_time   AS last_signal_time
FROM latest_price p
LEFT JOIN latest_signal s ON p.ticker = s.ticker
""")

# ======================== STEP 3: LOG NEW TRADES TO TRADE_LOG ========================
# Only log signals we haven't already logged (compare with existing trade_log)
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW _new_trades AS
SELECT pu.*
FROM _portfolio_updates pu
WHERE pu.last_signal IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM {TRADE_LOG} tl
    WHERE tl.ticker = pu.ticker
      AND tl.signal = pu.last_signal
      AND tl.timestamp = pu.last_signal_time
  )
""")

new_trades_df = spark.table("_new_trades")
new_trade_count = new_trades_df.count()

if new_trade_count > 0:
    # For SELL trades, compute realized PnL from portfolio_state
    # For BUY trades on existing positions, log accumulation
    trade_entries = new_trades_df.alias("nt").join(
        spark.table(GOLD_PORT).select("ticker", "avg_entry_price", "holdings").alias("port"),
        "ticker", "left"
    ).selectExpr(
        "nt.ticker",
        "nt.last_signal AS signal",
        "nt.signal_score",
        "nt.signal_price AS price",
        f"""CASE
             WHEN nt.last_signal = 'BUY' THEN {BUY_SHARES}
             ELSE COALESCE(port.holdings, 0)
           END AS quantity""",
        f"""CASE
             WHEN nt.last_signal = 'SELL' AND COALESCE(port.holdings, 0) > 0
             THEN (nt.signal_price - COALESCE(port.avg_entry_price, 0)) * port.holdings
             ELSE 0
           END AS pnl_realized""",
        "nt.stop_loss",
        "nt.take_profit",
        "nt.last_signal_time AS timestamp"
    )
    trade_entries.write.format("delta").mode("append").saveAsTable(TRADE_LOG)
    print(f"  ✔ Logged {new_trade_count} new trade(s) to {TRADE_LOG}")
else:
    print(f"  ✓ No new trades to log")


# ======================== STEP 4: MERGE INTO PORTFOLIO STATE ========================
# FIX: Added clause ordering to handle all scenarios:
#   1. SELL with holdings → close position, record realized PnL
#   2. BUY with holdings=0 → REOPEN position (was missing — critical bug)
#   3. BUY with holdings>0 → ACCUMULATE position with weighted avg entry
#   4. Generic match → just update current price + unrealized PnL
spark.sql(f"""
MERGE INTO {GOLD_PORT} AS target
USING _portfolio_updates AS source
ON target.ticker = source.ticker

-- 1) SELL signal: close position, record realized PnL
WHEN MATCHED AND source.last_signal = 'SELL' AND target.holdings > 0 THEN UPDATE SET
  target.current_price    = source.current_price,
  target.realized_pnl     = COALESCE(target.realized_pnl, 0)
                            + (source.current_price - target.avg_entry_price) * target.holdings,
  target.unrealized_pnl   = 0,
  target.holdings         = 0,
  target.total_trades     = COALESCE(target.total_trades, 0) + 1,
  target.last_signal      = source.last_signal,
  target.last_signal_time = source.last_signal_time

-- 2) BUY signal with NO current position: reopen after previous SELL/SL/TP exit
--    (CRITICAL FIX: without this, positions could never be re-entered)
WHEN MATCHED AND source.last_signal = 'BUY' AND target.holdings = 0 THEN UPDATE SET
  target.holdings         = {BUY_SHARES},
  target.avg_entry_price  = source.signal_price,
  target.current_price    = source.current_price,
  target.unrealized_pnl   = (source.current_price - source.signal_price) * {BUY_SHARES},
  target.total_trades     = COALESCE(target.total_trades, 0) + 1,
  target.last_signal      = source.last_signal,
  target.last_signal_time = source.last_signal_time

-- 3) BUY signal with EXISTING position: accumulate with weighted-average entry price
WHEN MATCHED AND source.last_signal = 'BUY' AND target.holdings > 0 THEN UPDATE SET
  target.avg_entry_price  = (target.avg_entry_price * target.holdings + source.signal_price * {BUY_SHARES})
                            / (target.holdings + {BUY_SHARES}),
  target.holdings         = target.holdings + {BUY_SHARES},
  target.current_price    = source.current_price,
  target.unrealized_pnl   = (source.current_price
                            - (target.avg_entry_price * target.holdings + source.signal_price * {BUY_SHARES})
                              / (target.holdings + {BUY_SHARES}))
                            * (target.holdings + {BUY_SHARES}),
  target.total_trades     = COALESCE(target.total_trades, 0) + 1,
  target.last_signal      = source.last_signal,
  target.last_signal_time = source.last_signal_time

-- 4) No new signal or SELL with no holdings: just update current price
WHEN MATCHED THEN UPDATE SET
  target.current_price    = source.current_price,
  target.unrealized_pnl   = CASE WHEN target.holdings > 0
                            THEN (source.current_price - target.avg_entry_price) * target.holdings
                            ELSE 0 END,
  target.last_signal      = COALESCE(source.last_signal, target.last_signal),
  target.last_signal_time = COALESCE(source.last_signal_time, target.last_signal_time)

-- 5) Brand new ticker with BUY: open first position
WHEN NOT MATCHED AND source.last_signal = 'BUY' THEN INSERT (
  ticker, holdings, avg_entry_price, current_price, unrealized_pnl,
  realized_pnl, total_trades, last_signal, last_signal_time
) VALUES (
  source.ticker, {BUY_SHARES}, source.signal_price, source.current_price,
  (source.current_price - source.signal_price) * {BUY_SHARES},
  0, 1, source.last_signal, source.last_signal_time
)

-- 6) Brand new ticker with no BUY signal: track price only
WHEN NOT MATCHED THEN INSERT (
  ticker, holdings, avg_entry_price, current_price, unrealized_pnl,
  realized_pnl, total_trades, last_signal, last_signal_time
) VALUES (
  source.ticker, 0, 0, source.current_price, 0,
  0, 0, source.last_signal, source.last_signal_time
)
""")

# ======================== STEP 5: DISPLAY RESULTS ========================
print(f"\n✔ Portfolio updated in {GOLD_PORT}")
print(f"\n── Portfolio State ──")
display(spark.table(GOLD_PORT).orderBy("ticker"))

print(f"\n── Recent Trade Log (last 20) ──")
display(spark.sql(f"""
  SELECT trade_id, ticker, signal, signal_score, price, quantity,
         pnl_realized, stop_loss, take_profit, timestamp
  FROM {TRADE_LOG}
  ORDER BY timestamp DESC
  LIMIT 20
"""))

# Summary metrics
print(f"\n── Performance Summary ──")
spark.sql(f"""
SELECT
  COUNT(*)                                             AS total_trades,
  SUM(CASE WHEN signal = 'BUY' THEN 1 ELSE 0 END)     AS buy_count,
  SUM(CASE WHEN signal = 'SELL' THEN 1 ELSE 0 END)     AS sell_count,
  SUM(CASE WHEN signal IN ('STOP_LOSS','TAKE_PROFIT') THEN 1 ELSE 0 END) AS auto_exits,
  ROUND(SUM(COALESCE(pnl_realized, 0)), 2)             AS total_realized_pnl,
  ROUND(AVG(CASE WHEN pnl_realized > 0 THEN pnl_realized END), 2) AS avg_win,
  ROUND(AVG(CASE WHEN pnl_realized < 0 THEN pnl_realized END), 2) AS avg_loss,
  ROUND(SUM(CASE WHEN pnl_realized > 0 THEN 1 ELSE 0 END) * 100.0 / 
        NULLIF(SUM(CASE WHEN pnl_realized != 0 THEN 1 ELSE 0 END), 0), 1) AS win_rate_pct
FROM {TRADE_LOG}
""").display()
