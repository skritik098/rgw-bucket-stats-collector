# RGW Bucket Stats Collector

A high-performance, modular bucket statistics collector for Ceph RGW with incremental updates, historical tracking, and real-time dashboards.

## Key Features

- **Bulk Collection**: Single `radosgw-admin bucket stats` command for all buckets (~3 min for 28K buckets)
- **Continuous Mode**: Daemon that automatically updates stale buckets
- **Smart Strategy**: Auto-selects bulk vs per-bucket based on stale count
- **JSON Cache**: Lock-free dashboard access via JSON snapshot file
- **Export**: Reconstruct exact `radosgw-admin bucket stats` JSON format from DB
- **Historical Analytics**: Growth tracking, comparisons, capacity forecasting
- **CLI Dashboard**: Rich terminal UI with live monitoring
- **Full Field Capture**: Stores ALL fields from bucket stats (versioning, quotas, usage, etc.)

## Architecture

```
┌─────────────────┐         ┌──────────────────┐
│    Collector    │────────►│    DuckDB        │  (historical data, analytics)
│  (continuous)   │         │  stats.duckdb    │
│                 │         └──────────────────┘
│                 │
│                 │         ┌──────────────────┐
│                 │────────►│   JSON Cache     │  (real-time snapshot)
│                 │         │  stats_cache.json│
└─────────────────┘         └────────┬─────────┘
                                     │
                                     │ (no DB lock!)
                                     ▼
                            ┌──────────────────┐
                            │    Dashboard     │
                            │   (reads cache)  │
                            └──────────────────┘
```

### Source Files

```
src/
├── models.py      # Data models (BucketStats, CollectorConfig)
├── rgw_client.py  # RGW Admin interface (bulk stats, sync status)
├── storage.py     # DuckDB storage with schema migration
├── collector.py   # Main collector (bootstrap/continuous/bulk)
├── analytics.py   # Historical analysis, growth, export
├── cache.py       # JSON cache for lock-free dashboard
├── dashboard.py   # CLI dashboard (DB and cache modes)
└── cli.py         # Command line interface
```

## Installation

```bash
pip install duckdb
pip install rich  # Optional, for dashboard
```

## Quick Start

```bash
# Bootstrap - fast initial collection (~3 min for 28K buckets)
python main.py bootstrap --db stats.duckdb

# Check status
python main.py status --db stats.duckdb

# Continuous mode with cache for dashboard
python main.py collect --db stats.duckdb --continuous --cache stats_cache.json

# Dashboard (reads from cache - no DB lock)
python main.py dashboard --cache stats_cache.json --view live
```

## Commands

### Bootstrap (Cold Start)

Fast initial collection using single bulk command:

```bash
# Basic bootstrap
python main.py bootstrap --db stats.duckdb

# With JSON cache for dashboard
python main.py bootstrap --db stats.duckdb --cache stats_cache.json

# With multisite sync status
python main.py bootstrap --db stats.duckdb --collect-sync
```

**Performance**: ~3 minutes for 28,000 buckets (vs ~30 min with per-bucket approach)

### Collect (One-time or Continuous)

```bash
# One-time collection
python main.py collect --db stats.duckdb

# Continuous mode (daemon)
python main.py collect --db stats.duckdb --continuous

# With custom thresholds
python main.py collect --db stats.duckdb --continuous \
    --stale-threshold 1800 \
    --refresh-interval 300 \
    --cache stats_cache.json
```

**Strategy Selection**:
- If stale buckets > 500: Uses BULK mode (fetches & updates ALL buckets)
- If stale buckets ≤ 500: Uses per-bucket mode (updates only stale ones)

### Status

Check database status and data freshness:

```bash
python main.py status --db stats.duckdb
```

Output:
```
==================================================
RGW BUCKET STATS STATUS
==================================================
  Database:       stats.duckdb
  Total buckets:  28176
  Total owners:   1523
  Total objects:  15,234,567
  Total size:     4.2 TB
--------------------------------------------------
Timestamp Info:
  Oldest data:    2025-01-20 10:15:23
  Newest data:    2025-01-20 12:30:45
--------------------------------------------------
Stale Buckets:
  Stale (>1m):    0
  Stale (>5m):    0
  Stale (>10m):   0
  Stale (>1h):    0
==================================================
```

### Dashboard

Interactive CLI dashboard (requires `rich` library):

```bash
# From database
python main.py dashboard --db stats.duckdb --view status
python main.py dashboard --db stats.duckdb --view top --limit 50
python main.py dashboard --db stats.duckdb --view sync
python main.py dashboard --db stats.duckdb --view live --refresh 5

# From cache (no DB lock - use while collector is running)
python main.py dashboard --cache stats_cache.json --view status
python main.py dashboard --cache stats_cache.json --view live
```

#### View: `status` - Cluster Summary

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                              Cluster Summary                                  │
├──────────────────────────────────────────────────────────────────────────────┤
│  Total Buckets:   28,176                                                     │
│  Total Owners:    1,523                                                      │
│  Total Objects:   15,234,567                                                 │
│  Total Size:      4.2 TB                                                     │
│                                                                              │
│  Oldest Data:     2025-01-20 10:15:23                                        │
│  Newest Data:     2025-01-20 12:30:45                                        │
└──────────────────────────────────────────────────────────────────────────────┘

                              Data Freshness
┌─────────────────┬───────────┐
│ Age             │     Count │
├─────────────────┼───────────┤
│ < 10 min        │    27,892 │
│ < 1 hour        │       284 │
│ < 24 hours      │         0 │
│ > 24 hours      │         0 │
└─────────────────┴───────────┘
```

#### View: `top` - Top Buckets by Size

```
                        Top 20 Buckets by Size
┌──────────────────────────────────┬──────────────┬───────────┬──────────┬────────┐
│ Bucket                           │ Owner        │      Size │  Objects │ Shards │
├──────────────────────────────────┼──────────────┼───────────┼──────────┼────────┤
│ production-data-backup           │ admin        │  512.4 GB │  892,341 │     64 │
│ analytics-warehouse              │ data-team    │  384.2 GB │  234,521 │     32 │
│ user-uploads-2024                │ app-service  │  256.8 GB │1,234,567 │     32 │
│ logs-archive                     │ ops          │  198.3 GB │8,234,123 │     16 │
│ ml-training-datasets             │ ml-team      │  156.7 GB │   12,345 │     16 │
│ static-assets-cdn                │ frontend     │  128.4 GB │  567,890 │     16 │
│ database-backups                 │ dba          │   98.2 GB │    1,234 │     11 │
│ media-transcoded                 │ media-svc    │   87.6 GB │   45,678 │     11 │
│ customer-documents               │ app-service  │   76.5 GB │  234,567 │     11 │
│ temp-processing                  │ batch-jobs   │   65.4 GB │  123,456 │     11 │
│ ...                              │ ...          │       ... │      ... │    ... │
└──────────────────────────────────┴──────────────┴───────────┴──────────┴────────┘
```

#### View: `sync` - Multisite Sync Status

```
                           Sync Status Summary
┌─────────────────┬───────────┐
│ Status          │     Count │
├─────────────────┼───────────┤
│ synced          │    27,845 │
│ behind          │       312 │
│ error           │        19 │
└─────────────────┴───────────┘

                     Buckets Behind (Top 30)
┌──────────────────────────────────┬──────────────┬────────┬─────────────────┐
│ Bucket                           │ Owner        │ Shards │ Entries Behind  │
├──────────────────────────────────┼──────────────┼────────┼─────────────────┤
│ high-traffic-uploads             │ app-service  │      8 │          45,231 │
│ realtime-events                  │ streaming    │      4 │          12,456 │
│ user-sessions                    │ auth-svc     │      2 │           8,934 │
│ analytics-ingest                 │ data-team    │      1 │           5,672 │
│ ...                              │ ...          │    ... │             ... │
└──────────────────────────────────┴──────────────┴────────┴─────────────────┘
```

#### View: `compare` - Size Comparison Over Time

```
                    Size Comparison (7 days ago)
┌────────────────────────────┬──────────────┬───────────┬───────────┬──────────┐
│ Bucket                     │ Owner        │   Current │    7d Ago │   Change │
├────────────────────────────┼──────────────┼───────────┼───────────┼──────────┤
│ production-data-backup     │ admin        │  512.4 GB │  498.2 GB │   +2.8%  │
│ analytics-warehouse        │ data-team    │  384.2 GB │  356.8 GB │   +7.7%  │
│ user-uploads-2024          │ app-service  │  256.8 GB │  245.1 GB │   +4.8%  │
│ logs-archive               │ ops          │  198.3 GB │  187.6 GB │   +5.7%  │
│ ml-training-datasets       │ ml-team      │  156.7 GB │  156.7 GB │   +0.0%  │
│ temp-processing            │ batch-jobs   │   65.4 GB │   89.2 GB │  -26.7%  │
│ ...                        │ ...          │       ... │       ... │      ... │
└────────────────────────────┴──────────────┴───────────┴───────────┴──────────┘
```

#### View: `growth` - Growth Analysis

```
                         Cluster Growth (30 days)
┌──────────────────────────────────────────────────────────────────────────────┐
│  Period:          Last 30 days                                               │
│  Start Size:      3.8 TB                                                     │
│  Current Size:    4.2 TB                                                     │
│  Growth:          +421.5 GB (+11.1%)                                         │
│  Daily Average:   +14.1 GB/day                                               │
│  Monthly Rate:    +423 GB/month                                              │
└──────────────────────────────────────────────────────────────────────────────┘

                      Fastest Growing Buckets
┌──────────────────────────────────┬──────────────┬───────────┬──────────────┐
│ Bucket                           │ Owner        │    Growth │ Growth Rate  │
├──────────────────────────────────┼──────────────┼───────────┼──────────────┤
│ analytics-warehouse              │ data-team    │  +27.4 GB │    +7.7%     │
│ logs-archive                     │ ops          │  +10.7 GB │    +5.7%     │
│ user-uploads-2024                │ app-service  │  +11.7 GB │    +4.8%     │
│ production-data-backup           │ admin        │  +14.2 GB │    +2.8%     │
│ ...                              │ ...          │       ... │          ... │
└──────────────────────────────────┴──────────────┴───────────┴──────────────┘
```

#### View: `all` - All Buckets Listing

```
                           All Buckets (28,176 total)
┌──────────────────────────────────┬──────────────┬───────────┬──────────┬─────────┐
│ Bucket                           │ Owner        │      Size │  Objects │     Age │
├──────────────────────────────────┼──────────────┼───────────┼──────────┼─────────┤
│ aaa-test-bucket                  │ dev-user     │    1.2 MB │       12 │      2m │
│ accounting-reports               │ finance      │   45.6 GB │    2,345 │      3m │
│ admin-backups                    │ admin        │   12.3 GB │      456 │      1m │
│ analytics-warehouse              │ data-team    │  384.2 GB │  234,521 │      2m │
│ api-cache                        │ api-svc      │  234.5 MB │   12,345 │      4m │
│ ...                              │ ...          │       ... │      ... │     ... │
│ zzz-archive-2023                 │ archive      │   78.9 GB │   23,456 │      5m │
└──────────────────────────────────┴──────────────┴───────────┴──────────┴─────────┘

Sort: --sort size | objects | age | name
```

#### View: `live` - Auto-Refreshing Monitor

```
═══════════════════════════════════════════════════════════════════════════════
  RGW Stats Dashboard │ Cache: stats_cache.json │ Time: 12:34:56
═══════════════════════════════════════════════════════════════════════════════

┌─────────────────────────────────── Summary ──────────────────────────────────┐
│  Buckets: 28,176    Owners: 1,523    Objects: 15.2M    Size: 4.2 TB         │
│  Cache Updated: 2025-01-20 12:34:45 (11s ago)                                │
└──────────────────────────────────────────────────────────────────────────────┘

                              Data Freshness
┌─────────────────┬───────────┐      ┌─────────────── Top 10 by Size ─────────┐
│ Age             │     Count │      │ Bucket                      │     Size │
├─────────────────┼───────────┤      ├─────────────────────────────┼──────────┤
│ < 10 min        │    27,892 │      │ production-data-backup      │ 512.4 GB │
│ < 1 hour        │       284 │      │ analytics-warehouse         │ 384.2 GB │
│ < 24 hours      │         0 │      │ user-uploads-2024           │ 256.8 GB │
│ > 24 hours      │         0 │      │ logs-archive                │ 198.3 GB │
└─────────────────┴───────────┘      │ ml-training-datasets        │ 156.7 GB │
                                     │ ...                         │      ... │
                                     └─────────────────────────────┴──────────┘

───────────────────────────────────────────────────────────────────────────────
  Refreshing in 5s... (Ctrl+C to exit)
```

**Views Summary**:

| View | Description | Use Case |
|------|-------------|----------|
| `status` | Cluster summary and freshness | Quick health check |
| `top` | Top buckets by size | Find largest consumers |
| `sync` | Multisite sync status | Monitor replication |
| `compare` | Size comparison over time | Track changes |
| `growth` | Growth analysis | Capacity planning |
| `all` | All buckets listing | Full inventory |
| `live` | Auto-refreshing monitor | Real-time monitoring |

### Export

Export bucket stats in exact `radosgw-admin bucket stats` JSON format:

```bash
# Export all buckets
python main.py export --db stats.duckdb -o all_buckets.json

# Export single bucket
python main.py export --db stats.duckdb --bucket my-bucket

# Compact output (no indentation)
python main.py export --db stats.duckdb --compact
```

The exported JSON matches the exact format of `radosgw-admin bucket stats`:

```json
{
    "bucket": "my-bucket",
    "num_shards": 11,
    "tenant": "",
    "versioning": "off",
    "zonegroup": "...",
    "placement_rule": "default-placement",
    "explicit_placement": {...},
    "id": "...",
    "marker": "...",
    "index_type": "Normal",
    "versioned": false,
    "versioning_enabled": false,
    "object_lock_enabled": false,
    "mfa_enabled": false,
    "owner": "user123",
    "ver": "...",
    "master_ver": "...",
    "mtime": "2025-01-20T12:18:37.601118Z",
    "creation_time": "2025-01-20T12:18:37.593470Z",
    "max_marker": "...",
    "usage": {
        "rgw.main": {
            "size": 410041,
            "size_actual": 413696,
            "size_utilized": 410041,
            "size_kb": 401,
            "size_kb_actual": 404,
            "size_kb_utilized": 401,
            "num_objects": 1
        }
    },
    "bucket_quota": {
        "enabled": false,
        "check_on_raw": false,
        "max_size": -1,
        "max_size_kb": 0,
        "max_objects": -1
    }
}
```

### Analytics

Historical analysis and forecasting:

```bash
python main.py analytics --db stats.duckdb --type growth --days 30
python main.py analytics --db stats.duckdb --type fastest-growing --limit 20
python main.py analytics --db stats.duckdb --type freshness
python main.py analytics --db stats.duckdb --type forecast --forecast-days 90
python main.py analytics --db stats.duckdb --type sync
python main.py analytics --db stats.duckdb --type sync-behind --limit 50
```

### Query

Direct queries on collected data:

```bash
python main.py query --db stats.duckdb --type top-buckets --limit 20
python main.py query --db stats.duckdb --type by-owner
python main.py query --db stats.duckdb --type empty-buckets
```

### History

View historical data for a specific bucket:

```bash
python main.py history --db stats.duckdb my-bucket-name --days 30
```

### Comparison

Per-bucket comparison showing changes over time:

```bash
python main.py comparison --db stats.duckdb --days 7 --limit 50
```

### Repair

Fix database issues (NULL timestamps):

```bash
# Check what would be fixed
python main.py repair --db stats.duckdb --dry-run

# Apply fixes
python main.py repair --db stats.duckdb
```

## Configuration Options

### Collect Command

| Option | Default | Description |
|--------|---------|-------------|
| `--continuous` | false | Run continuously as daemon |
| `--refresh-interval` | 300 | Seconds between collection cycles |
| `--stale-threshold` | 1800 | Seconds after which bucket is considered stale |
| `--workers` | 4 | Parallel workers for per-bucket mode |
| `--max-workers` | 100 | Maximum workers for auto-scaling |
| `--auto-scale` | true | Auto-scale workers based on bucket count |
| `--collect-sync` | false | Collect multisite sync status (slower) |
| `--timeout` | 60 | Command timeout in seconds |
| `--cache` | none | JSON cache file path for dashboard |
| `--limit` | none | Limit buckets (for testing) |

### Bootstrap Command

| Option | Default | Description |
|--------|---------|-------------|
| `--workers` | 50 | Parallel workers (ignored, uses bulk) |
| `--collect-sync` | false | Collect multisite sync status |
| `--timeout` | 60 | Per-command timeout |
| `--cache` | none | JSON cache file path |

## Database Schema

### bucket_stats (main table)

Stores current state of each bucket with ALL fields from `radosgw-admin bucket stats`:

| Column | Type | Description |
|--------|------|-------------|
| bucket_name | VARCHAR | Primary key |
| bucket_id | VARCHAR | Bucket ID |
| marker | VARCHAR | Bucket marker |
| tenant | VARCHAR | Tenant name |
| owner | VARCHAR | Bucket owner |
| zonegroup | VARCHAR | Zonegroup ID |
| placement_rule | VARCHAR | Placement rule |
| explicit_placement | JSON | Pool placement details |
| num_shards | INTEGER | Number of index shards |
| index_type | VARCHAR | Index type (Normal, etc.) |
| versioning | VARCHAR | Versioning state (off/enabled/suspended) |
| versioned | BOOLEAN | Is versioned |
| versioning_enabled | BOOLEAN | Is versioning enabled |
| object_lock_enabled | BOOLEAN | Object lock enabled |
| mfa_enabled | BOOLEAN | MFA delete enabled |
| ver | VARCHAR | Version string |
| master_ver | VARCHAR | Master version string |
| max_marker | VARCHAR | Max marker |
| mtime | VARCHAR | Modification time |
| creation_time | VARCHAR | Creation time |
| size_bytes | BIGINT | Total size in bytes |
| size_actual_bytes | BIGINT | Actual size on disk |
| size_utilized_bytes | BIGINT | Utilized size |
| num_objects | BIGINT | Number of objects |
| usage | JSON | Full usage breakdown by storage class |
| bucket_quota | JSON | Quota configuration |
| sync_status | VARCHAR | Sync status (multisite) |
| sync_behind_shards | INTEGER | Shards behind |
| sync_behind_entries | INTEGER | Entries behind |
| sync_source_zone | VARCHAR | Source zone |
| collected_at | TIMESTAMP | Collection timestamp |
| collection_duration_ms | INTEGER | Collection duration |

### bucket_stats_history

Historical snapshots for trend analysis:

| Column | Type |
|--------|------|
| id | INTEGER |
| bucket_name | VARCHAR |
| size_bytes | BIGINT |
| num_objects | BIGINT |
| sync_behind_shards | INTEGER |
| sync_behind_entries | INTEGER |
| collected_at | TIMESTAMP |

### storage_class_usage

Per-bucket storage class breakdown:

| Column | Type |
|--------|------|
| bucket_name | VARCHAR |
| storage_class | VARCHAR |
| size_bytes | BIGINT |
| size_actual_bytes | BIGINT |
| size_utilized_bytes | BIGINT |
| num_objects | BIGINT |
| collected_at | TIMESTAMP |

## JSON Cache Format

The cache file contains pre-computed data for fast dashboard access:

```json
{
  "_cache_updated": "2025-01-20T12:30:45.123456",
  "summary": {
    "total_buckets": 28176,
    "total_owners": 1523,
    "total_size_bytes": 4500000000000,
    "total_objects": 15234567
  },
  "top_by_size": [...],
  "top_by_objects": [...],
  "freshness": {
    "fresh_10m": 28000,
    "fresh_1h": 176,
    "fresh_24h": 0,
    "stale": 0
  },
  "by_owner": [...],
  "all_buckets": [...],
  "sync_summary": {...},
  "sync_behind": [...]
}
```

## Performance

| Scenario | Time | Method |
|----------|------|--------|
| Bootstrap 28K buckets | ~3 min | Single bulk command |
| Per-bucket (old method) | ~30 min | 28K subprocess calls |
| Incremental (500 stale) | ~30 sec | Per-bucket parallel |
| Dashboard from cache | Instant | JSON file read |

## Troubleshooting

### All buckets showing as stale

Check for NULL timestamps:
```bash
python main.py status --db stats.duckdb
# Look for "[WARN] NULL timestamps: X"

# Fix with repair command
python main.py repair --db stats.duckdb
```

### Dashboard can't access DB

Use cache mode instead:
```bash
# Collector writes cache
python main.py collect --db stats.duckdb --continuous --cache stats_cache.json

# Dashboard reads from cache (no lock)
python main.py dashboard --cache stats_cache.json --view live
```

### Bulk collection hangs

The bulk command waits for complete output from `radosgw-admin`. For very large clusters:
- Default timeout is 1800s (30 min)
- Check if `radosgw-admin bucket stats` works from CLI
- Monitor with verbose mode

### Schema mismatch after upgrade

The collector auto-migrates schema on startup. If issues persist:
```bash
# Option 1: Repair
python main.py repair --db stats.duckdb

# Option 2: Fresh start
rm stats.duckdb
python main.py bootstrap --db stats.duckdb
```

## Examples

### Production Setup

```bash
# Initial bootstrap
python main.py bootstrap --db /var/lib/rgw-stats/stats.duckdb \
    --cache /var/lib/rgw-stats/cache.json

# Run as service (continuous mode)
python main.py collect --db /var/lib/rgw-stats/stats.duckdb \
    --continuous \
    --stale-threshold 3600 \
    --refresh-interval 300 \
    --cache /var/lib/rgw-stats/cache.json

# Separate terminal: Monitor with dashboard
python main.py dashboard --cache /var/lib/rgw-stats/cache.json --view live
```

### Export for External Tools

```bash
# Export all stats for Prometheus/Grafana
python main.py export --db stats.duckdb -o /tmp/bucket_stats.json

# Export specific bucket
python main.py export --db stats.duckdb --bucket production-data
```

### Capacity Planning

```bash
# 90-day forecast
python main.py analytics --db stats.duckdb --type forecast --forecast-days 90

# Find fastest growing buckets
python main.py analytics --db stats.duckdb --type fastest-growing --days 30 --limit 20
```

## License

MIT