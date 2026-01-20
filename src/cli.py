"""
Command line interface for RGW bucket stats collector.
"""

import argparse
import sys
from datetime import datetime

from .models import CollectorConfig
from .rgw_client import RGWAdminClient
from .storage import Storage
from .collector import Collector
from .analytics import Analytics


def format_bytes(b: int) -> str:
    """Format bytes to human readable."""
    if b is None:
        return "0 B"
    for unit in ['B', 'KB', 'MB', 'GB', 'TB', 'PB']:
        if b < 1024:
            return f"{b:.1f} {unit}"
        b /= 1024
    return f"{b:.1f} PB"


def cmd_collect(args):
    """Run collection (once or continuous)."""
    config = CollectorConfig(
        db_path=args.db,
        ceph_conf=args.ceph_conf,
        refresh_interval_seconds=args.refresh_interval,
        stale_threshold_seconds=args.stale_threshold,
        batch_size=args.batch_size,
        parallel_workers=args.workers,
        max_workers=args.max_workers,
        auto_scale_workers=args.auto_scale,
        command_timeout=args.timeout,
        collect_sync_status=args.collect_sync
    )
    
    collector = Collector(config, cache_path=getattr(args, 'cache', None))
    
    try:
        if args.continuous:
            collector.run_continuous(verbose=args.verbose)
        else:
            result = collector.run_once(limit=args.limit, verbose=True)
            print(f"\nCollected {result['collected']} buckets in {result['duration']:.1f}s")
    finally:
        collector.close()


def cmd_bootstrap(args):
    """Bootstrap mode for cold start - fast initial collection."""
    config = CollectorConfig(
        db_path=args.db,
        ceph_conf=args.ceph_conf,
        batch_size=args.batch_size,
        bootstrap_mode=True,
        bootstrap_workers=args.workers,
        max_workers=args.workers,
        command_timeout=args.timeout,
        collect_sync_status=args.collect_sync
    )
    
    collector = Collector(config, cache_path=getattr(args, 'cache', None))
    
    try:
        result = collector.run_bootstrap(verbose=True)
        print(f"\nBootstrap complete: {result['collected']} buckets in {result['duration']:.1f}s")
        if result.get('rate'):
            print(f"Rate: {result['rate']:.1f} buckets/sec")
    finally:
        collector.close()


def cmd_status(args):
    """Show collection status."""
    storage = Storage(args.db)
    
    summary = storage.get_summary()
    freshness = storage.get_freshness_stats()
    
    print("\n" + "=" * 50)
    print("RGW BUCKET STATS STATUS")
    print("=" * 50)
    print(f"  Database:       {args.db}")
    print(f"  Total buckets:  {summary['total_buckets']}")
    print(f"  Total owners:   {summary['total_owners']}")
    print(f"  Total objects:  {summary['total_objects']:,}")
    print(f"  Total size:     {format_bytes(summary['total_size_bytes'])}")
    
    print("-" * 50)
    print("Timestamp Info:")
    if freshness['null_collected_at']:
        print(f"  [WARN] NULL timestamps: {freshness['null_collected_at']}")
        print(f"         Run 'repair' command to fix")
    if freshness['oldest']:
        print(f"  Oldest data:    {freshness['oldest']}")
        print(f"  Newest data:    {freshness['newest']}")
    else:
        print(f"  No timestamp data found")
    
    print("-" * 50)
    print("Stale Buckets:")
    # Stale buckets
    for threshold in [60, 300, 600, 3600]:
        stale = storage.get_stale_count(threshold)
        label = f"{threshold//60}m" if threshold < 3600 else f"{threshold//3600}h"
        print(f"  Stale (>{label}):   {stale}")
    
    print("=" * 50 + "\n")
    
    storage.close()


def cmd_query(args):
    """Run queries on collected data."""
    storage = Storage(args.db)
    
    if args.type == 'top-buckets':
        results = storage.top_buckets_by_size(args.limit)
        print(f"\nTop {args.limit} Buckets by Size:")
        print("-" * 80)
        for r in results:
            print(f"  {r['bucket_name']:<40} {r['owner']:<20} "
                  f"{r['num_objects']:>10,} objs  {format_bytes(r['size_bytes']):>12}")
    
    elif args.type == 'by-owner':
        results = storage.summary_by_owner()
        print("\nSummary by Owner:")
        print("-" * 80)
        for r in results:
            print(f"  {r['owner']:<30} {r['buckets']:>6} buckets  "
                  f"{r['objects']:>12,} objs  {format_bytes(r['size_bytes']):>12}")
    
    elif args.type == 'storage-class':
        results = storage.storage_class_summary()
        print("\nStorage Class Distribution:")
        print("-" * 80)
        for r in results:
            print(f"  {r['storage_class']:<20} {r['buckets']:>6} buckets  "
                  f"{r['objects']:>12,} objs  {format_bytes(r['size_bytes']):>12}")
    
    elif args.type == 'custom':
        if not args.sql:
            print("ERROR: --sql required for custom query")
            sys.exit(1)
        result = storage.query(args.sql)
        print(result.df().to_string())
    
    storage.close()


def cmd_bucket(args):
    """Get info for specific bucket."""
    storage = Storage(args.db)
    
    stats = storage.get_bucket_stats(args.bucket_name)
    
    if not stats:
        print(f"Bucket '{args.bucket_name}' not found in database")
        storage.close()
        return
    
    print(f"\nBucket: {stats['bucket_name']}")
    print("-" * 50)
    print(f"  Owner:        {stats['owner']}")
    print(f"  Tenant:       {stats['tenant'] or '-'}")
    print(f"  Objects:      {stats['num_objects']:,}")
    print(f"  Size:         {format_bytes(stats['size_bytes'])}")
    print(f"  Actual size:  {format_bytes(stats['size_actual_bytes'])}")
    print(f"  Shards:       {stats['num_shards']}")
    print(f"  Collected at: {stats['collected_at']}")
    print("-" * 50)
    
    storage.close()


def cmd_analytics(args):
    """Run analytics queries."""
    analytics = Analytics(args.db)
    
    try:
        if args.type == 'growth':
            results = analytics.cluster_growth(days=args.days)
            print(f"\nCluster Growth (last {args.days} days):")
            print("-" * 70)
            for r in results:
                day_str = r['day'].strftime('%Y-%m-%d') if r['day'] else 'N/A'
                print(f"  {day_str}  {format_bytes(r['total_size'] or 0):>12}  "
                      f"{r['total_objects'] or 0:>12,} objs")
        
        elif args.type == 'fastest-growing':
            results = analytics.fastest_growing_buckets(days=args.days, limit=args.limit)
            print(f"\nFastest Growing Buckets (last {args.days} days):")
            print("-" * 90)
            for r in results:
                print(f"  {r['bucket_name']:<35} {r['owner']:<15} "
                      f"{format_bytes(r['start_size'] or 0):>10} -> {format_bytes(r['end_size'] or 0):>10}  "
                      f"+{format_bytes(r['growth_bytes'] or 0)}")
        
        elif args.type == 'owner-growth':
            results = analytics.fastest_growing_owners(days=args.days, limit=args.limit)
            print(f"\nFastest Growing Owners (last {args.days} days):")
            print("-" * 80)
            for r in results:
                print(f"  {r['owner']:<30} {format_bytes(r['start_size'] or 0):>12} -> "
                      f"{format_bytes(r['end_size'] or 0):>12}  +{format_bytes(r['growth_bytes'] or 0)}")
        
        elif args.type == 'compare':
            result = analytics.compare_snapshots(hours_ago=args.hours)
            if 'error' in result:
                print(f"Error: {result['error']}")
            else:
                print(f"\nComparison (now vs {args.hours}h ago):")
                print("-" * 50)
                print(f"  Size:    {format_bytes(result['current']['size_bytes'])} -> "
                      f"delta: {format_bytes(result['delta']['size_bytes'])}")
                print(f"  Objects: {result['current']['objects']:,} -> "
                      f"delta: {result['delta']['objects']:,}")
        
        elif args.type == 'changes':
            results = analytics.bucket_changes(hours=args.hours, min_change_pct=args.min_change)
            print(f"\nBuckets with >{args.min_change}% change (last {args.hours}h):")
            print("-" * 90)
            for r in results:
                sign = '+' if r['delta'] >= 0 else ''
                print(f"  {r['bucket_name']:<35} {r['owner']:<15} "
                      f"{sign}{format_bytes(r['delta'])}  ({r['change_pct']:.1f}%)")
        
        elif args.type == 'freshness':
            result = analytics.collection_freshness()
            print("\nData Freshness:")
            print("-" * 50)
            print(f"  Total buckets: {result['total']}")
            print(f"  Fresh <5m:  {result['fresh_5m']:>6} ({result['fresh_5m_pct']:.1f}%)")
            print(f"  Fresh <10m: {result['fresh_10m']:>6} ({result['fresh_10m_pct']:.1f}%)")
            print(f"  Fresh <1h:  {result['fresh_1h']:>6} ({result['fresh_1h_pct']:.1f}%)")
            print(f"  Oldest: {result['oldest']}")
            print(f"  Newest: {result['newest']}")
        
        elif args.type == 'forecast':
            result = analytics.capacity_forecast(history_days=args.days, forecast_days=args.forecast_days)
            if 'error' in result:
                print(f"Error: {result['error']}")
            else:
                print(f"\nCapacity Forecast ({args.forecast_days} days):")
                print("-" * 50)
                print(f"  Current size:    {format_bytes(result['current_bytes'])}")
                print(f"  Daily growth:    {format_bytes(result['daily_growth_bytes'])}")
                print(f"  Monthly growth:  {format_bytes(result['monthly_growth_bytes'])}")
                print(f"  Forecast ({args.forecast_days}d): {format_bytes(result['forecast_bytes'])}")
        
        elif args.type == 'sync':
            results = analytics.sync_status_summary()
            if not results:
                print("\nNo sync status data available.")
            else:
                print("\nSync Status Summary:")
                print("-" * 50)
                for r in results:
                    print(f"  {r['sync_status']:<20} {r['bucket_count']:>6} buckets  "
                          f"{r['shards_behind'] or 0:>6} shards behind")
        
        elif args.type == 'sync-behind':
            results = analytics.buckets_behind_sync(limit=args.limit)
            if not results:
                print("\nNo buckets behind in sync.")
            else:
                print("\nBuckets Behind in Sync:")
                print("-" * 80)
                for r in results:
                    print(f"  {r['bucket_name']:<40} {r['owner']:<15} "
                          f"{r['shards_behind']:>4} shards  {format_bytes(r['size_bytes'] or 0)}")
    
    finally:
        analytics.close()


def cmd_history(args):
    """Show bucket history."""
    analytics = Analytics(args.db)
    
    try:
        # Bucket details
        details = analytics.bucket_details(args.bucket_name)
        if not details:
            print(f"Bucket '{args.bucket_name}' not found")
            return
        
        print(f"\nBucket: {details['bucket_name']}")
        print("=" * 60)
        print(f"  Owner:       {details['owner']}")
        print(f"  Size:        {format_bytes(details['size_bytes'] or 0)}")
        print(f"  Objects:     {details['num_objects']:,}")
        print(f"  Shards:      {details['num_shards']}")
        print(f"  Collected:   {details['collected_at']}")
        if details['sync_status']:
            print(f"  Sync status: {details['sync_status']}")
            print(f"  Behind:      {details['sync_behind_shards']} shards")
        
        # History
        history = analytics.bucket_history(args.bucket_name, days=args.days)
        if history:
            print(f"\nHistory (last {args.days} days):")
            print("-" * 60)
            for h in history[-20:]:  # Last 20 entries
                ts = h['collected_at'].strftime('%Y-%m-%d %H:%M') if h['collected_at'] else 'N/A'
                print(f"  {ts}  {format_bytes(h['size_bytes'] or 0):>12}  "
                      f"{h['num_objects'] or 0:>10,} objs")
    
    finally:
        analytics.close()


def cmd_comparison(args):
    """Show per-bucket comparison table."""
    analytics = Analytics(args.db)
    
    try:
        results = analytics.bucket_comparison_table(days=args.days, limit=args.limit)
        
        print(f"\nPer-Bucket Comparison (vs {args.days} days ago):")
        print("-" * 110)
        print(f"  {'Bucket':<35} {'Owner':<15} {'Current':>12} {f'{args.days}d Ago':>12} {'Delta':>12} {'Change':>8}")
        print("-" * 110)
        
        for r in results:
            delta = r['size_delta'] or 0
            pct = r['size_pct_change'] or 0
            sign = '+' if delta >= 0 else ''
            
            print(f"  {r['bucket_name'][:35]:<35} {(r['owner'] or '')[:15]:<15} "
                  f"{format_bytes(r['current_size']):>12} {format_bytes(r['old_size']):>12} "
                  f"{sign}{format_bytes(delta):>11} {pct:>7.1f}%")
        
        print("-" * 110)
    
    finally:
        analytics.close()


def cmd_dashboard(args):
    """Launch CLI dashboard."""
    cache_path = getattr(args, 'cache', None)
    
    if cache_path:
        # Use cache-based dashboard (no DB lock)
        try:
            from .dashboard import run_dashboard_from_cache
            run_dashboard_from_cache(
                cache_path=cache_path,
                command=args.view,
                limit=args.limit,
                refresh=args.refresh
            )
        except ImportError:
            print("ERROR: rich library required for dashboard. Install with: pip install rich")
            sys.exit(1)
    else:
        # Use DB-based dashboard
        try:
            from .dashboard import run_dashboard
        except ImportError:
            print("ERROR: rich library required for dashboard. Install with: pip install rich")
            sys.exit(1)
        
        run_dashboard(
            db_path=args.db,
            command=args.view,
            limit=args.limit,
            days=args.days,
            sort=args.sort,
            refresh=args.refresh
        )


def cmd_export(args):
    """Export bucket stats in radosgw-admin JSON format."""
    analytics = Analytics(args.db)
    
    try:
        if args.bucket:
            # Export single bucket
            data = analytics.export_bucket_stats(args.bucket)
            if data is None:
                print(f"ERROR: Bucket '{args.bucket}' not found in database", file=sys.stderr)
                sys.exit(1)
        else:
            # Export all buckets
            data = analytics.export_all_bucket_stats(limit=args.limit)
        
        # Output
        import json
        output = json.dumps(data, indent=4 if not args.compact else None)
        
        if args.output:
            with open(args.output, 'w') as f:
                f.write(output)
            print(f"Exported to {args.output}", file=sys.stderr)
        else:
            print(output)
    
    finally:
        analytics.close()


def cmd_repair(args):
    """Repair database issues like NULL timestamps."""
    from datetime import datetime
    
    storage = Storage(args.db)
    
    print("\n" + "=" * 50)
    print("DATABASE REPAIR")
    print("=" * 50)
    
    # Check for NULL collected_at
    freshness = storage.get_freshness_stats()
    null_count = freshness.get('null_collected_at', 0) or 0
    
    if null_count == 0:
        print("  ✓ No issues found - all timestamps are valid")
        print("=" * 50 + "\n")
        storage.close()
        return
    
    print(f"  Found {null_count} buckets with NULL collected_at")
    
    if args.dry_run:
        print("  [DRY RUN] Would update these to current timestamp")
        print("  Run without --dry-run to apply fix")
    else:
        # Fix NULL timestamps
        now = datetime.utcnow()
        storage.conn.execute("""
            UPDATE bucket_stats 
            SET collected_at = ? 
            WHERE collected_at IS NULL
        """, [now])
        storage.commit()
        print(f"  ✓ Updated {null_count} buckets with timestamp: {now}")
    
    print("=" * 50 + "\n")
    storage.close()


def main():
    parser = argparse.ArgumentParser(
        description='RGW Bucket Stats Collector',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # One-time collection of all buckets
  %(prog)s collect --db stats.duckdb
  
  # Bootstrap mode (fast initial collection)
  %(prog)s bootstrap --db stats.duckdb
  
  # Continuous mode - update stale buckets every 5 min
  %(prog)s collect --db stats.duckdb --continuous
  
  # Continuous with custom intervals
  %(prog)s collect --db stats.duckdb --continuous \\
      --refresh-interval 60 --stale-threshold 300
  
  # Check status
  %(prog)s status --db stats.duckdb
  
  # Query data
  %(prog)s query --db stats.duckdb --type top-buckets
  %(prog)s query --db stats.duckdb --type by-owner
  
  # Export in radosgw-admin format
  %(prog)s export --db stats.duckdb                      # All buckets
  %(prog)s export --db stats.duckdb --bucket my-bucket   # Single bucket
  %(prog)s export --db stats.duckdb -o output.json       # To file
        """
    )
    
    parser.add_argument('--db', default='rgw_stats.duckdb',
                       help='Database path (default: rgw_stats.duckdb)')
    
    subparsers = parser.add_subparsers(dest='command', help='Command')
    
    # Collect
    collect_p = subparsers.add_parser('collect', help='Collect bucket stats')
    collect_p.add_argument('--continuous', '-c', action='store_true',
                          help='Run continuously, updating stale buckets')
    collect_p.add_argument('--refresh-interval', type=int, default=300,
                          help='Seconds between cycles in continuous mode (default: 300)')
    collect_p.add_argument('--stale-threshold', type=int, default=1800,
                          help='Seconds after which bucket is stale (default: 1800 = 30min)')
    collect_p.add_argument('--batch-size', type=int, default=100,
                          help='Buckets per batch (default: 100)')
    collect_p.add_argument('--workers', type=int, default=4,
                          help='Parallel workers (default: 4)')
    collect_p.add_argument('--max-workers', type=int, default=100,
                          help='Maximum workers for auto-scaling (default: 100)')
    collect_p.add_argument('--auto-scale', action='store_true', default=True,
                          help='Auto-scale workers based on bucket count (default: True)')
    collect_p.add_argument('--no-auto-scale', action='store_false', dest='auto_scale',
                          help='Disable auto-scaling')
    collect_p.add_argument('--collect-sync', action='store_true',
                          help='Collect multisite sync status (slower)')
    collect_p.add_argument('--timeout', type=int, default=60,
                          help='Command timeout seconds (default: 60)')
    collect_p.add_argument('--ceph-conf', default='/etc/ceph/ceph.conf',
                          help='Ceph config path')
    collect_p.add_argument('--cache', 
                          help='JSON cache file for dashboard (avoids DB lock)')
    collect_p.add_argument('--limit', type=int,
                          help='Limit buckets to collect (for testing)')
    collect_p.add_argument('--verbose', '-v', action='store_true',
                          help='Verbose output')
    collect_p.set_defaults(func=cmd_collect)
    
    # Bootstrap (cold start)
    bootstrap_p = subparsers.add_parser('bootstrap', 
                                        help='Bootstrap mode for cold start (fast initial collection)')
    bootstrap_p.add_argument('--workers', type=int, default=50,
                            help='Parallel workers (default: 50)')
    bootstrap_p.add_argument('--batch-size', type=int, default=100,
                            help='Buckets per batch (default: 100)')
    bootstrap_p.add_argument('--collect-sync', action='store_true',
                            help='Collect multisite sync status')
    bootstrap_p.add_argument('--timeout', type=int, default=60,
                            help='Command timeout seconds (default: 60)')
    bootstrap_p.add_argument('--ceph-conf', default='/etc/ceph/ceph.conf',
                            help='Ceph config path')
    bootstrap_p.add_argument('--cache',
                            help='JSON cache file for dashboard (avoids DB lock)')
    bootstrap_p.set_defaults(func=cmd_bootstrap)
    
    # Status
    status_p = subparsers.add_parser('status', help='Show collection status')
    status_p.set_defaults(func=cmd_status)
    
    # Repair
    repair_p = subparsers.add_parser('repair', help='Repair database issues (NULL timestamps)')
    repair_p.add_argument('--dry-run', action='store_true',
                         help='Show what would be fixed without making changes')
    repair_p.set_defaults(func=cmd_repair)
    
    # Query
    query_p = subparsers.add_parser('query', help='Query collected data')
    query_p.add_argument('--type', required=True,
                        choices=['top-buckets', 'by-owner', 'storage-class', 'custom'],
                        help='Query type')
    query_p.add_argument('--limit', type=int, default=20,
                        help='Limit results (default: 20)')
    query_p.add_argument('--sql', help='Custom SQL for --type custom')
    query_p.set_defaults(func=cmd_query)
    
    # Bucket
    bucket_p = subparsers.add_parser('bucket', help='Get specific bucket info')
    bucket_p.add_argument('bucket_name', help='Bucket name')
    bucket_p.set_defaults(func=cmd_bucket)
    
    # Analytics
    analytics_p = subparsers.add_parser('analytics', help='Analytics and growth analysis')
    analytics_p.add_argument('--type', required=True,
                            choices=['growth', 'fastest-growing', 'owner-growth', 'compare',
                                    'changes', 'freshness', 'forecast', 'sync', 'sync-behind'],
                            help='Analysis type')
    analytics_p.add_argument('--days', type=int, default=7,
                            help='Days of history to analyze (default: 7)')
    analytics_p.add_argument('--hours', type=int, default=24,
                            help='Hours for comparison (default: 24)')
    analytics_p.add_argument('--limit', type=int, default=20,
                            help='Limit results (default: 20)')
    analytics_p.add_argument('--min-change', type=float, default=10.0,
                            help='Minimum change percent for --type changes (default: 10)')
    analytics_p.add_argument('--forecast-days', type=int, default=90,
                            help='Days to forecast (default: 90)')
    analytics_p.set_defaults(func=cmd_analytics)
    
    # History
    history_p = subparsers.add_parser('history', help='Bucket history and details')
    history_p.add_argument('bucket_name', help='Bucket name')
    history_p.add_argument('--days', type=int, default=30,
                          help='Days of history (default: 30)')
    history_p.set_defaults(func=cmd_history)
    
    # Comparison (per-bucket tabular comparison)
    compare_p = subparsers.add_parser('comparison', help='Per-bucket comparison table')
    compare_p.add_argument('--days', type=int, default=7,
                          help='Days to compare against (default: 7)')
    compare_p.add_argument('--limit', type=int, default=50,
                          help='Limit results (default: 50)')
    compare_p.set_defaults(func=cmd_comparison)
    
    # Dashboard (rich terminal UI)
    dashboard_p = subparsers.add_parser('dashboard', help='Interactive CLI dashboard (requires rich)')
    dashboard_p.add_argument('--view', default='status',
                            choices=['status', 'top', 'compare', 'sync', 'all', 'growth', 'live'],
                            help='Dashboard view (default: status)')
    dashboard_p.add_argument('--limit', type=int, default=30,
                            help='Limit results (default: 30)')
    dashboard_p.add_argument('--days', type=int, default=7,
                            help='Days for comparison/growth (default: 7)')
    dashboard_p.add_argument('--sort', default='age',
                            choices=['size', 'objects', 'age', 'name'],
                            help='Sort order for --view all (default: age)')
    dashboard_p.add_argument('--refresh', type=int, default=5,
                            help='Refresh interval for --view live (default: 5s)')
    dashboard_p.add_argument('--cache',
                            help='Read from JSON cache file instead of DB (avoids lock)')
    dashboard_p.set_defaults(func=cmd_dashboard)
    
    # Export (reconstruct radosgw-admin bucket stats format from DB)
    export_p = subparsers.add_parser('export', help='Export bucket stats in radosgw-admin JSON format')
    export_p.add_argument('--bucket', '-b',
                         help='Specific bucket to export (default: all buckets)')
    export_p.add_argument('--output', '-o',
                         help='Output file (default: stdout)')
    export_p.add_argument('--limit', type=int,
                         help='Limit number of buckets (default: no limit)')
    export_p.add_argument('--compact', action='store_true',
                         help='Compact JSON output (no indentation)')
    export_p.set_defaults(func=cmd_export)
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    args.func(args)


if __name__ == '__main__':
    main()