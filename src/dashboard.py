"""
CLI Dashboard for RGW Bucket Stats Collector.
Uses rich library for terminal UI.

Install: pip install rich
"""

import sys
import time
import threading
from datetime import datetime, timedelta
from typing import Optional

try:
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
    from rich.layout import Layout
    from rich.live import Live
    from rich.text import Text
    from rich.progress import Progress, BarColumn, TextColumn, TimeElapsedColumn
    HAS_RICH = True
except ImportError:
    HAS_RICH = False

from .storage import Storage
from .analytics import Analytics


def format_bytes(b: int) -> str:
    """Format bytes to human readable."""
    if b is None or b == 0:
        return "0 B"
    for unit in ['B', 'KB', 'MB', 'GB', 'TB', 'PB']:
        if abs(b) < 1024:
            return f"{b:.1f} {unit}"
        b /= 1024
    return f"{b:.1f} PB"


def format_age(seconds: float) -> str:
    """Format age in human readable form."""
    if seconds is None:
        return "N/A"
    if seconds < 60:
        return f"{int(seconds)}s"
    elif seconds < 3600:
        return f"{int(seconds/60)}m"
    elif seconds < 86400:
        return f"{int(seconds/3600)}h"
    else:
        return f"{int(seconds/86400)}d"


class Dashboard:
    """CLI Dashboard for monitoring bucket stats collection."""
    
    def __init__(self, db_path: str):
        if not HAS_RICH:
            raise ImportError("rich library required: pip install rich")
        
        self.db_path = db_path
        self.console = Console()
        self.running = True
    
    def _get_storage(self) -> Storage:
        """Get storage connection (read-only)."""
        import duckdb
        conn = duckdb.connect(self.db_path, read_only=True)
        storage = Storage.__new__(Storage)
        storage.db_path = self.db_path
        storage.conn = conn
        return storage
    
    def _get_analytics(self) -> Analytics:
        """Get analytics connection."""
        return Analytics(self.db_path)
    
    def show_status(self):
        """Show current status dashboard."""
        analytics = self._get_analytics()
        
        try:
            summary = analytics.cluster_summary()
            freshness = analytics.collection_freshness()
            
            # Header
            self.console.print()
            self.console.print(Panel.fit(
                "[bold blue]RGW BUCKET STATS DASHBOARD[/bold blue]",
                border_style="blue"
            ))
            
            # Summary table
            summary_table = Table(title="Cluster Summary", show_header=False)
            summary_table.add_column("Metric", style="cyan")
            summary_table.add_column("Value", style="green")
            
            summary_table.add_row("Total Buckets", f"{summary['total_buckets']:,}")
            summary_table.add_row("Total Owners", f"{summary['total_owners']:,}")
            summary_table.add_row("Total Objects", f"{summary['total_objects']:,}")
            summary_table.add_row("Total Size", format_bytes(summary['total_size_bytes']))
            summary_table.add_row("Oldest Data", str(summary['oldest_data'] or 'N/A'))
            summary_table.add_row("Newest Data", str(summary['newest_data'] or 'N/A'))
            
            self.console.print(summary_table)
            
            # Freshness table
            fresh_table = Table(title="Data Freshness", show_header=True)
            fresh_table.add_column("Age", style="cyan")
            fresh_table.add_column("Count", style="green")
            fresh_table.add_column("Percent", style="yellow")
            
            fresh_table.add_row("< 5 min", f"{freshness['fresh_5m']:,}", f"{freshness['fresh_5m_pct']:.1f}%")
            fresh_table.add_row("< 10 min", f"{freshness['fresh_10m']:,}", f"{freshness['fresh_10m_pct']:.1f}%")
            fresh_table.add_row("< 1 hour", f"{freshness['fresh_1h']:,}", f"{freshness['fresh_1h_pct']:.1f}%")
            
            stale = freshness['total'] - freshness['fresh_1h']
            stale_pct = (stale * 100 / freshness['total']) if freshness['total'] > 0 else 0
            fresh_table.add_row("> 1 hour (stale)", f"{stale:,}", f"{stale_pct:.1f}%")
            
            self.console.print(fresh_table)
            self.console.print()
            
        finally:
            analytics.close()
    
    def show_top_buckets(self, limit: int = 20):
        """Show top buckets by size."""
        analytics = self._get_analytics()
        
        try:
            buckets = analytics.top_buckets_by_size(limit)
            
            table = Table(title=f"Top {limit} Buckets by Size")
            table.add_column("Bucket", style="cyan", max_width=40)
            table.add_column("Owner", style="blue", max_width=20)
            table.add_column("Objects", justify="right", style="green")
            table.add_column("Size", justify="right", style="yellow")
            table.add_column("Age", justify="right", style="magenta")
            
            now = datetime.utcnow()
            for b in buckets:
                age = (now - b['collected_at']).total_seconds() if b['collected_at'] else 0
                table.add_row(
                    b['bucket_name'][:40],
                    (b['owner'] or '')[:20],
                    f"{b['num_objects']:,}",
                    format_bytes(b['size_bytes']),
                    format_age(age)
                )
            
            self.console.print()
            self.console.print(table)
            self.console.print()
            
        finally:
            analytics.close()
    
    def show_comparison(self, days: int = 7, limit: int = 30):
        """Show per-bucket comparison table."""
        analytics = self._get_analytics()
        
        try:
            comparison = analytics.bucket_comparison_table(days=days, limit=limit)
            
            table = Table(title=f"Bucket Comparison (vs {days} days ago)")
            table.add_column("Bucket", style="cyan", max_width=35)
            table.add_column("Owner", style="blue", max_width=15)
            table.add_column("Current", justify="right")
            table.add_column(f"{days}d Ago", justify="right")
            table.add_column("Delta", justify="right")
            table.add_column("Change %", justify="right")
            
            for b in comparison:
                delta = b['size_delta'] or 0
                pct = b['size_pct_change'] or 0
                
                # Color code delta
                if delta > 0:
                    delta_str = f"[green]+{format_bytes(delta)}[/green]"
                    pct_str = f"[green]+{pct:.1f}%[/green]"
                elif delta < 0:
                    delta_str = f"[red]{format_bytes(delta)}[/red]"
                    pct_str = f"[red]{pct:.1f}%[/red]"
                else:
                    delta_str = "0 B"
                    pct_str = "0%"
                
                table.add_row(
                    b['bucket_name'][:35],
                    (b['owner'] or '')[:15],
                    format_bytes(b['current_size']),
                    format_bytes(b['old_size']),
                    delta_str,
                    pct_str
                )
            
            self.console.print()
            self.console.print(table)
            self.console.print()
            
        finally:
            analytics.close()
    
    def show_sync_status(self, limit: int = 30):
        """Show sync status comparison."""
        analytics = self._get_analytics()
        
        try:
            sync_summary = analytics.sync_status_summary()
            
            if not sync_summary:
                self.console.print("\n[yellow]No sync status data available (not multisite or not collected)[/yellow]\n")
                return
            
            # Summary
            summary_table = Table(title="Sync Status Summary")
            summary_table.add_column("Status", style="cyan")
            summary_table.add_column("Buckets", justify="right", style="green")
            summary_table.add_column("Shards Behind", justify="right", style="yellow")
            
            for s in sync_summary:
                status = s['sync_status']
                if status == 'behind':
                    status = f"[red]{status}[/red]"
                elif status == 'synced':
                    status = f"[green]{status}[/green]"
                
                summary_table.add_row(
                    status,
                    f"{s['bucket_count']:,}",
                    f"{s['shards_behind'] or 0:,}"
                )
            
            self.console.print()
            self.console.print(summary_table)
            
            # Detailed buckets behind
            sync_detail = analytics.sync_comparison_table(limit=limit)
            if sync_detail:
                detail_table = Table(title="Buckets Behind in Sync")
                detail_table.add_column("Bucket", style="cyan", max_width=35)
                detail_table.add_column("Owner", style="blue", max_width=15)
                detail_table.add_column("Status", style="yellow")
                detail_table.add_column("Shards", justify="right")
                detail_table.add_column("Entries", justify="right")
                detail_table.add_column("Avg 24h", justify="right", style="dim")
                
                for b in sync_detail:
                    if b['shards_behind'] > 0 or b['entries_behind'] > 0:
                        detail_table.add_row(
                            b['bucket_name'][:35],
                            (b['owner'] or '')[:15],
                            b['sync_status'] or 'unknown',
                            f"{b['shards_behind']:,}",
                            f"{b['entries_behind']:,}",
                            f"{b['avg_shards_24h']:.1f}"
                        )
                
                self.console.print(detail_table)
            
            self.console.print()
            
        finally:
            analytics.close()
    
    def show_all_buckets(self, sort_by: str = 'age', limit: int = 50):
        """Show all buckets table sorted by specified field."""
        analytics = self._get_analytics()
        
        try:
            buckets = analytics.all_buckets_summary_table(sort_by=sort_by, limit=limit)
            
            table = Table(title=f"All Buckets (sorted by {sort_by}, limit {limit})")
            table.add_column("Bucket", style="cyan", max_width=40)
            table.add_column("Owner", style="blue", max_width=15)
            table.add_column("Objects", justify="right")
            table.add_column("Size", justify="right")
            table.add_column("Sync", style="yellow")
            table.add_column("Age", justify="right")
            
            for b in buckets:
                age = b['age_seconds'] or 0
                
                # Color code age
                if age > 3600:
                    age_str = f"[red]{format_age(age)}[/red]"
                elif age > 600:
                    age_str = f"[yellow]{format_age(age)}[/yellow]"
                else:
                    age_str = f"[green]{format_age(age)}[/green]"
                
                # Sync status
                sync = b['sync_status'] or '-'
                if b['shards_behind'] and b['shards_behind'] > 0:
                    sync = f"[red]{sync} ({b['shards_behind']})[/red]"
                
                table.add_row(
                    b['bucket_name'][:40],
                    (b['owner'] or '')[:15],
                    f"{b['num_objects']:,}",
                    format_bytes(b['size_bytes']),
                    sync,
                    age_str
                )
            
            self.console.print()
            self.console.print(table)
            self.console.print()
            
        finally:
            analytics.close()
    
    def show_growth(self, days: int = 7, limit: int = 20):
        """Show growth analysis."""
        analytics = self._get_analytics()
        
        try:
            # Fastest growing buckets
            growing = analytics.fastest_growing_buckets(days=days, limit=limit)
            
            table = Table(title=f"Fastest Growing Buckets (last {days} days)")
            table.add_column("Bucket", style="cyan", max_width=35)
            table.add_column("Owner", style="blue", max_width=15)
            table.add_column("Start", justify="right")
            table.add_column("End", justify="right")
            table.add_column("Growth", justify="right", style="green")
            
            for b in growing:
                growth = b['growth_bytes'] or 0
                if growth > 0:
                    growth_str = f"[green]+{format_bytes(growth)}[/green]"
                elif growth < 0:
                    growth_str = f"[red]{format_bytes(growth)}[/red]"
                else:
                    growth_str = "0 B"
                
                table.add_row(
                    b['bucket_name'][:35],
                    (b['owner'] or '')[:15],
                    format_bytes(b['start_size']),
                    format_bytes(b['end_size']),
                    growth_str
                )
            
            self.console.print()
            self.console.print(table)
            
            # Capacity forecast
            forecast = analytics.capacity_forecast(history_days=days, forecast_days=90)
            if 'error' not in forecast:
                forecast_panel = Panel(
                    f"Current: {format_bytes(forecast['current_bytes'])}\n"
                    f"Daily Growth: {format_bytes(forecast['daily_growth_bytes'])}\n"
                    f"Monthly Growth: {format_bytes(forecast['monthly_growth_bytes'])}\n"
                    f"90-day Forecast: {format_bytes(forecast['forecast_bytes'])}",
                    title="Capacity Forecast",
                    border_style="green"
                )
                self.console.print(forecast_panel)
            
            self.console.print()
            
        finally:
            analytics.close()
    
    def live_monitor(self, refresh_seconds: int = 5):
        """Live monitoring dashboard with auto-refresh."""
        self.console.print("\n[bold]Live Monitor[/bold] (Press Ctrl+C to exit)\n")
        
        try:
            while self.running:
                self.console.clear()
                
                # Header
                self.console.print(Panel.fit(
                    f"[bold blue]RGW BUCKET STATS - LIVE MONITOR[/bold blue]\n"
                    f"[dim]Last update: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}[/dim]",
                    border_style="blue"
                ))
                
                analytics = self._get_analytics()
                
                try:
                    # Quick summary
                    summary = analytics.cluster_summary()
                    freshness = analytics.collection_freshness()
                    
                    # Stats line
                    stale = freshness['total'] - freshness['fresh_5m']
                    stats_text = (
                        f"[cyan]Buckets:[/cyan] {summary['total_buckets']:,}  "
                        f"[cyan]Size:[/cyan] {format_bytes(summary['total_size_bytes'])}  "
                        f"[cyan]Objects:[/cyan] {summary['total_objects']:,}  "
                        f"[green]Fresh (<5m):[/green] {freshness['fresh_5m']:,}  "
                        f"[yellow]Stale:[/yellow] {stale:,}"
                    )
                    self.console.print(stats_text)
                    self.console.print()
                    
                    # Oldest buckets (stale)
                    oldest = analytics.stale_buckets(minutes=5, limit=10)
                    if oldest:
                        self.console.print("[bold]Oldest Data (needs refresh):[/bold]")
                        for b in oldest[:5]:
                            age = (datetime.utcnow() - b['collected_at']).total_seconds() if b['collected_at'] else 0
                            self.console.print(f"  [red]{format_age(age):>6}[/red]  {b['bucket_name']}")
                    else:
                        self.console.print("[green]All buckets up to date![/green]")
                    
                    self.console.print()
                    
                    # Top 5 by size
                    top = analytics.top_buckets_by_size(5)
                    self.console.print("[bold]Top 5 by Size:[/bold]")
                    for b in top:
                        self.console.print(f"  {format_bytes(b['size_bytes']):>12}  {b['bucket_name'][:40]}")
                    
                finally:
                    analytics.close()
                
                self.console.print(f"\n[dim]Refreshing in {refresh_seconds}s... (Ctrl+C to exit)[/dim]")
                
                # Sleep with interrupt check
                for _ in range(refresh_seconds):
                    if not self.running:
                        break
                    time.sleep(1)
                    
        except KeyboardInterrupt:
            self.running = False
            self.console.print("\n[yellow]Monitor stopped.[/yellow]")


def run_dashboard(db_path: str, command: str = 'status', **kwargs):
    """Run dashboard command."""
    if not HAS_RICH:
        print("ERROR: rich library required. Install with: pip install rich")
        sys.exit(1)
    
    dashboard = Dashboard(db_path)
    
    if command == 'status':
        dashboard.show_status()
    elif command == 'top':
        dashboard.show_top_buckets(limit=kwargs.get('limit', 20))
    elif command == 'compare':
        dashboard.show_comparison(days=kwargs.get('days', 7), limit=kwargs.get('limit', 30))
    elif command == 'sync':
        dashboard.show_sync_status(limit=kwargs.get('limit', 30))
    elif command == 'all':
        dashboard.show_all_buckets(sort_by=kwargs.get('sort', 'age'), limit=kwargs.get('limit', 50))
    elif command == 'growth':
        dashboard.show_growth(days=kwargs.get('days', 7), limit=kwargs.get('limit', 20))
    elif command == 'live':
        dashboard.live_monitor(refresh_seconds=kwargs.get('refresh', 5))
    else:
        print(f"Unknown command: {command}")