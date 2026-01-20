# RGW Bucket Stats Collector

Modular, incremental bucket statistics collector for Ceph RGW.

## Key Features

- **Bootstrap mode**: Fast initial collection for cold start (50+ workers)
- **Continuous mode**: Runs as daemon, updating stale buckets automatically
- **Auto-scaling workers**: Adjusts parallelism based on bucket count (up to 100)
- **Incremental updates**: Only collects buckets not updated within threshold
- **Sync status tracking**: Optional multisite sync status collection
- **CLI Dashboard**: Rich terminal UI for monitoring (requires `rich` library)
- **Historical analytics**: Growth tracking, comparisons, capacity forecasting
- **Per-bucket comparison**: Tabular comparison showing changes over time

## Architecture

```
src/
├── models.py      # Data models (BucketStats, CollectorConfig)
├── rgw_client.py  # RGW Admin interface (stats + sync status)
├── storage.py     # DuckDB storage with incremental support  
├── collector.py   # Main collector (bootstrap/continuous/incremental)
├── analytics.py   # Historical analysis, growth, comparisons
├── dashboard.py   # CLI dashboard (requires rich)
└── cli.py         # Command line interface
```

## Quick Start

```bash
pip install duckdb
pip install rich  # Optional, for dashboard

# Bootstrap (cold start)
python main.py bootstrap --db stats.duckdb --workers 50

# Continuous mode
python main.py collect --db stats.duckdb --continuous

# Dashboard
python main.py dashboard --db stats.duckdb --view live
```

## Bootstrap Mode (Cold Start)

For initial collection when DB is empty:

```bash
python main.py bootstrap --db stats.duckdb --workers 50
python main.py bootstrap --db stats.duckdb --workers 50 --collect-sync  # With sync
```

## Continuous Mode

```bash
python main.py collect --db stats.duckdb --continuous \
    --stale-threshold 1800 \
    --max-workers 100
```

## Dashboard

```bash
python main.py dashboard --db stats.duckdb --view status
python main.py dashboard --db stats.duckdb --view compare --days 7
python main.py dashboard --db stats.duckdb --view sync
python main.py dashboard --db stats.duckdb --view live
```

## Per-Bucket Comparison

```bash
python main.py comparison --db stats.duckdb --days 7 --limit 50
```

## Analytics

```bash
python main.py analytics --db stats.duckdb --type growth --days 30
python main.py analytics --db stats.duckdb --type fastest-growing
python main.py analytics --db stats.duckdb --type freshness
python main.py analytics --db stats.duckdb --type forecast
python main.py analytics --db stats.duckdb --type sync
```

## Bucket History

```bash
python main.py history --db stats.duckdb my-bucket-name --days 30
```