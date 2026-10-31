# Supply Chain Inventory Optimization & Demand Forecasting

End-to-end ELT pipeline on **Databricks** (PySpark, Delta Lake, SQL) with a bronze/silver/gold medallion architecture, data-quality checks, a scheduled Workflow, and a 7-day-ahead demand forecast.

## Overview
- **Data:** synthetic but realistic daily inventory for 1,000 SKUs x 4 warehouses over 12 months (~1.46M rows), generated with a simulation of weekly/seasonal demand, reorder points and supplier lead times. Dirty records (duplicates, nulls, negative stock) are injected on purpose to exercise the data-quality layer.
- **Stack:** Databricks (serverless), Unity Catalog, Delta Lake, PySpark, Spark SQL, Spark ML, Databricks Workflows.

## Architecture
```
raw CSVs (UC Volume)
   -> bronze  : raw Delta tables + audit columns (_ingest_ts, _source_file)
   -> silver  : typed, deduplicated, validated; failing rows -> inventory_quarantine; checks logged to dq_results
   -> gold    : star schema (fact_inventory_daily + dim_date, dim_sku, dim_warehouse), v_sku_kpis view
   -> forecast: model comparison + predictions tables
```

| Notebook | Purpose |
|---|---|
| `00_setup` | catalog, schemas, volume |
| `01_generate_data` | synthetic data generator (run once) |
| `02_bronze` | ingest raw CSVs to Delta |
| `03_silver` | cast, dedupe, DQ rules, quarantine, DQ log |
| `04_gold` | star schema + KPI view |
| `05_forecast` | features, models, metrics |

The four pipeline notebooks (`02`-`05`) run as the Databricks Workflow `supply_chain_daily_pipeline` (bronze -> silver -> gold -> forecast, daily schedule).

## Data quality
Each run logs to `silver.dq_results`. Latest run: 1,462,920 raw rows -> 2,920 duplicates removed -> 4,359 rows quarantined (2,916 null `units_sold`, 1,447 negative `stock_on_hand`, 0 sold > demanded) -> **1,455,641 clean rows**, reconciling exactly to the gold fact table.

## Gold KPIs
Fill rate, stockout rate, average inventory value and inventory turns per SKU (`gold.v_sku_kpis`).

## Demand forecast
7-day-ahead forecast per SKU/warehouse. Features: lags (7/14/28), 28-day rolling mean, day-of-week, month, category, lead time, warehouse. Evaluated on a time-based holdout (last 28 days).

| Model | MAE | WAPE |
|---|---|---|
| Naive (lag-7) | 2.71 | 39.9% |
| 28-day moving average | 2.07 | 30.5% |
| Gradient-boosted trees (Spark ML) | **1.98** | **29.2%** |

GBT reduces WAPE by ~27% vs the naive baseline and ~4% vs the moving average. Demand is Poisson-distributed, so a substantial share of the error is irreducible noise.

## Limitations / next steps
- Data is synthetic; every pipeline run reprocesses the same fixed files (a daily-increment source would make it incremental).
- Next: incremental loads (Auto Loader / MERGE), safety-stock and reorder-point recommendations from forecast error, a dashboard on the gold layer.

## Run it
1. Run `00_setup`, then `01_generate_data` once.
2. Run the `supply_chain_daily_pipeline` Workflow (or notebooks `02`-`05` in order).
