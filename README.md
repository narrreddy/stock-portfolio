# Stock Portfolio — Live Signal Dashboard

Real-time stock signal dashboard built on Databricks. Streams OHLCV data from Yahoo Finance, computes technical indicators via Spark Structured Streaming (`applyInPandasWithState`), generates BUY signals, and renders an interactive Streamlit UI.

## Architecture

```
Yahoo Finance → Bronze (raw_market_data)
                  │
                  │  applyInPandasWithState
                  ▼
              Silver (technical_indicators)  →  SMA20, SMA50, RSI(14), Vortex(14)
                  │
                  │  BUY signal logic
                  ▼
              Gold (trade_signals)           →  SMA20 > SMA50 ∧ RSI < 70 ∧ VI+ > VI-
              Gold (portfolio_state)         →  holdings, avg entry, unrealized PnL
                  │
                  ▼
              Streamlit Dashboard            →  Plotly candlestick + indicators
```

## Repository Structure

```
stock-portfolio/                               # ← Git repo root & DABs bundle root
├── databricks.yml                             # Bundle config (includes resources/*.yml)
├── resources/                                 # DABs resource definitions
│   ├── stock_signal_app.yml                   #   Streamlit Databricks App
│   └── stock_signal_pipeline.yml              #   Scheduled pipeline job
├── src/                                       # Source code
│   ├── notebooks/                             #   Pipeline notebooks
│   │   └── live_stock_signal_dashboard.py     #     Main streaming pipeline
│   ├── pipelines/                             #   Spark declarative / DLT pipelines
│   └── dashboards/                            #   AI/BI dashboard definitions
├── app/                                       # Streamlit app source
│   ├── streamlit_app.py                       #   Dashboard UI
│   ├── app.yaml                               #   App runtime config
│   └── requirements.txt                       #   Python dependencies
├── .github/workflows/deploy.yml               # GitHub Actions CI/CD
├── .gitignore
└── README.md
```

## Tech Stack

| Component | Technology |
|-----------|------------|
| Ingestion | Yahoo Finance API (`yfinance`) |
| Processing | Spark Structured Streaming, `applyInPandasWithState` |
| Storage | Delta Lake on Unity Catalog |
| Orchestration | Databricks Asset Bundles (DABs) |
| Visualization | Plotly (notebook), Streamlit (app) |
| CI/CD | GitHub Actions |

## Quick Start

### 1. Run Pipeline in Notebook

Open the `Live Stock Signal Dashboard` notebook in Databricks and run all cells sequentially. The pipeline:

1. Creates Unity Catalog resources (catalog, schema, 4 tables, 1 volume)
2. Ingests 5 days of 5-minute OHLCV bars from Yahoo Finance
3. Computes SMA(20/50), RSI(14), and Vortex(14) via stateful streaming
4. Generates BUY signals and tracks portfolio positions
5. Renders a Plotly candlestick dashboard

### 2. Deploy via Databricks CLI

```bash
# Authenticate
databricks auth login --host https://<workspace>.azuredatabricks.net

# Validate & deploy
databricks bundle validate
databricks bundle deploy -t dev --var="warehouse_id=<YOUR_WAREHOUSE_ID>"
databricks bundle run stock_signal_app -t dev
```

### 3. Deploy via GitHub Actions

Add these secrets to your GitHub repository:

| Secret | Value |
|--------|-------|
| `DATABRICKS_TOKEN` | Service principal access token |
| `DATABRICKS_HOST` | `https://<workspace>.azuredatabricks.net` |
| `DATABRICKS_WAREHOUSE_ID` | SQL warehouse ID |

Then:

* Push to `dev` → deploys to **dev** target
* Push to `main` → deploys to **prod** target
* Open a PR → validates the bundle (no deploy)
* Use **Actions → Run workflow** for manual target selection

## Delta Tables

| Layer | Table | Description |
|-------|-------|-------------|
| Bronze | `raw_market_data` | Raw OHLCV from Yahoo Finance |
| Silver | `technical_indicators` | SMA20, SMA50, RSI, Vortex per ticker |
| Gold | `trade_signals` | BUY signals |
| Gold | `portfolio_state` | Holdings, avg entry price, unrealized PnL |

## Technical Indicators

* **SMA(20/50)** — Simple Moving Average
* **RSI(14)** — Relative Strength Index
* **Vortex(14)** — VI+ and VI- lines
  * TR = `max(H-L, |H - prevClose|, |L - prevClose|)`
  * VM+ = `|H_i - L_{i-1}|`  /  VM- = `|L_i - H_{i-1}|`
  * VI+ = `sum(VM+, 14) / sum(TR, 14)`  /  VI- = `sum(VM-, 14) / sum(TR, 14)`

## Signal Logic

A **BUY** signal fires when all three conditions hold:

1. SMA20 > SMA50 (bullish trend)
2. RSI < 70 (not overbought)
3. VI+ > VI- (positive vortex momentum)

## Bundle Targets

| Target | Mode | Catalog | Trigger |
|--------|------|---------|--------|
| `dev` | development | `stockapp_dev` | push to `dev` branch |
| `staging` | default | `stockapp_staging` | manual dispatch |
| `prod` | production | `stockapp` | push to `main` branch |

## Stateful Processing Note

This project uses `applyInPandasWithState` (PySpark 3.4+), compatible with serverless runtimes. For Spark 4.0+ (DBR 16.2+), upgrade to `transformWithStateInPandas` with `StatefulProcessor` for ListState, MapState, TTL, and timers.

## License

Internal use.
