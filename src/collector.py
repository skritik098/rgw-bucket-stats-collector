"""
Bucket stats collector with continuous incremental updates.
"""

import signal
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import List, Optional, Callable

from .models import BucketStats, CollectorConfig
from .rgw_client import RGWAdminClient
from .storage import Storage


class CollectorState:
    """Shared state for collector with thread-safe shutdown."""
    
    def __init__(self):
        self.running = True
        self.stats = {
            'total_collected': 0,
            'total_errors': 0,
            'last_cycle_buckets': 0,
            'last_cycle_time': 0,
            'cycles_completed': 0,
            'current_workers': 0,
            'collection_rate': 0.0
        }
        self._lock = threading.Lock()
    
    def stop(self):
        """Signal collector to stop."""
        self.running = False
    
    def is_running(self) -> bool:
        """Check if collector should continue."""
        return self.running
    
    def increment_collected(self, count: int = 1):
        with self._lock:
            self.stats['total_collected'] += count
    
    def increment_errors(self, count: int = 1):
        with self._lock:
            self.stats['total_errors'] += count
    
    def record_cycle(self, buckets: int, duration: float, workers: int):
        with self._lock:
            self.stats['last_cycle_buckets'] = buckets
            self.stats['last_cycle_time'] = duration
            self.stats['cycles_completed'] += 1
            self.stats['current_workers'] = workers
            self.stats['collection_rate'] = buckets / duration if duration > 0 else 0
    
    def get_stats(self) -> dict:
        with self._lock:
            return self.stats.copy()


class Collector:
    """
    Bucket stats collector with continuous incremental updates.
    
    Key features:
    - Bootstrap mode: Fast initial collection with bulk command
    - Auto-scaling workers: Adjusts based on bucket count
    - Continuous mode: Runs as daemon, updating stale buckets
    - Incremental updates: Only collects buckets older than threshold
    - Sync collection: Optional multisite sync status
    - Graceful shutdown: Ctrl+C stops cleanly
    - JSON cache: Exports stats for lock-free dashboard access
    """
    
    def __init__(self, config: CollectorConfig, 
                 rgw_client: RGWAdminClient = None,
                 storage: Storage = None,
                 output_callback: Callable[[str], None] = None,
                 cache_path: str = None):
        self.config = config
        self.rgw_client = rgw_client or RGWAdminClient(
            ceph_conf=config.ceph_conf,
            timeout=config.command_timeout
        )
        self.storage = storage or Storage(config.db_path)
        self.state = CollectorState()
        self.output = output_callback or print
        
        # JSON cache for dashboard (avoids DB lock)
        self.cache_path = cache_path
        self._cache = None
        if cache_path:
            from .cache import StatsCache
            self._cache = StatsCache(cache_path)
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _update_cache(self):
        """Update JSON cache for dashboard access."""
        if not self._cache:
            return
        
        try:
            from .cache import build_cache_data
            data = build_cache_data(self.storage)
            self._cache.write(data)
        except Exception as e:
            self.output(f"  Warning: Failed to update cache: {e}")
    
    def _signal_handler(self, signum, frame):
        """Handle interrupt signals."""
        self.output("\n⚠️  Shutdown signal received, stopping after current batch...")
        self.state.stop()
    
    def _collect_single_bucket(self, bucket_name: str) -> Optional[BucketStats]:
        """Collect stats for a single bucket."""
        if not self.state.is_running():
            return None
        
        if self.config.collect_sync_status:
            return self.rgw_client.get_bucket_stats_with_sync(
                bucket_name, collect_sync=True
            )
        else:
            return self.rgw_client.get_bucket_stats(bucket_name)
    
    def _collect_batch(self, buckets: List[str], workers: int) -> List[BucketStats]:
        """Collect stats for a batch of buckets in parallel."""
        results = []
        
        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_bucket = {
                executor.submit(self._collect_single_bucket, b): b 
                for b in buckets
            }
            
            for future in as_completed(future_to_bucket):
                if not self.state.is_running():
                    # Cancel remaining futures
                    for f in future_to_bucket:
                        f.cancel()
                    break
                
                bucket = future_to_bucket[future]
                try:
                    stats = future.result(timeout=self.config.command_timeout)
                    if stats:
                        results.append(stats)
                        self.state.increment_collected()
                    else:
                        self.state.increment_errors()
                except Exception:
                    self.state.increment_errors()
        
        return results
    
    def collect_buckets(self, bucket_names: List[str], workers: int = None,
                        verbose: bool = False) -> int:
        """
        Collect stats for specific buckets.
        Returns: number of buckets successfully collected
        """
        if not bucket_names:
            return 0
        
        # Calculate workers
        if workers is None:
            workers = self.config.calculate_workers(len(bucket_names))
        
        collected = 0
        batch_num = 0
        
        # Process in batches
        for i in range(0, len(bucket_names), self.config.batch_size):
            if not self.state.is_running():
                break
            
            batch = bucket_names[i:i + self.config.batch_size]
            batch_num += 1
            batch_start = time.time()
            
            if verbose:
                self.output(f"  Batch {batch_num}: {len(batch)} buckets with {workers} workers...")
            
            results = self._collect_batch(batch, workers)
            
            # Save to storage
            for stats in results:
                self.storage.upsert_bucket_stats(stats)
            self.storage.commit()
            
            collected += len(results)
            
            if verbose:
                batch_time = time.time() - batch_start
                rate = len(results) / batch_time if batch_time > 0 else 0
                self.output(f"    Done: {len(results)}/{len(batch)} in {batch_time:.1f}s ({rate:.1f}/sec)")
        
        return collected
    
    def run_bootstrap(self, verbose: bool = True) -> dict:
        """
        Bootstrap mode: Fast initial collection for cold start.
        
        Uses single radosgw-admin command to fetch ALL buckets at once.
        Much faster than per-bucket: ~3 min for 28K buckets.
        """
        start_time = time.time()
        
        self.output("\n" + "=" * 60)
        self.output("BOOTSTRAP MODE - Bulk Collection")
        self.output("=" * 60)
        self.output("  Method: radosgw-admin bucket stats (single command)")
        self.output("  Updates: ALL buckets")
        self.output("=" * 60)
        
        result = self.run_bulk_refresh(verbose=verbose)
        
        # Update cache for dashboard
        self._update_cache()
        if self._cache and verbose:
            self.output(f"  Cache updated: {self.cache_path}")
        
        total_duration = time.time() - start_time
        rate = result['collected'] / total_duration if total_duration > 0 else 0
        
        self.output("\n" + "=" * 60)
        self.output("BOOTSTRAP COMPLETE")
        self.output("=" * 60)
        self.output(f"  Buckets collected: {result['collected']}")
        self.output(f"  Total time:        {total_duration:.1f}s")
        self.output(f"  Rate:              {rate:.0f} buckets/sec")
        if result.get('errors', 0) > 0:
            self.output(f"  Errors:            {result['errors']}")
        self.output("=" * 60 + "\n")
        
        return {
            'collected': result['collected'], 
            'duration': total_duration, 
            'rate': rate,
            'errors': result.get('errors', 0),
            'mode': 'bootstrap'
        }
    
    def run_bulk_refresh(self, verbose: bool = True) -> dict:
        """
        Refresh ALL buckets using single bulk command.
        
        When bulk is used, we update ALL buckets because:
        1. We already fetched all data (can't fetch partial)
        2. Writing to DB is fast compared to fetch time
        3. Keeps all data fresh
        
        Args:
            verbose: Print progress
        """
        start_time = time.time()
        
        if verbose:
            self.output("  Bulk mode: Fetching and updating ALL buckets...")
        
        # Get all bucket stats in ONE command
        log_func = self.output if verbose else None
        all_stats = self.rgw_client.get_all_bucket_stats_bulk(timeout=1800, log_callback=log_func)
        
        if not all_stats:
            return {
                'fetched': 0,
                'collected': 0,
                'errors': 0,
                'duration': time.time() - start_time,
                'mode': 'bulk'
            }
        
        # Save ALL to database
        if verbose:
            self.output(f"  Saving {len(all_stats)} buckets to database...")
        
        save_start = time.time()
        saved = 0
        errors = 0
        
        for stats in all_stats:
            if not self.state.is_running():
                if verbose:
                    self.output(f"  Interrupted! Saved {saved} buckets before stop.")
                break
            
            try:
                self.storage.upsert_bucket_stats(stats)
                saved += 1
                self.state.increment_collected()
                
                # Commit every 500 buckets
                if saved % 500 == 0:
                    self.storage.commit()
                    if verbose:
                        self.output(f"    Saved: {saved}/{len(all_stats)} buckets...")
            except Exception as e:
                errors += 1
                if verbose and errors <= 5:
                    self.output(f"    ERROR saving {stats.bucket_name}: {e}")
        
        self.storage.commit()
        
        save_time = time.time() - save_start
        total_duration = time.time() - start_time
        
        if verbose:
            self.output(f"  Summary: Fetched {len(all_stats)}, Saved {saved}, Errors {errors}")
            self.output(f"  Save time: {save_time:.1f}s, Total time: {total_duration:.1f}s")
        
        return {
            'fetched': len(all_stats),
            'collected': saved,
            'errors': errors,
            'duration': total_duration,
            'mode': 'bulk'
        }
    
    def run_once(self, limit: int = None, verbose: bool = False,
                 bulk_threshold: int = 500) -> dict:
        """
        Run a single collection cycle.
        
        Strategy decision:
        - If stale buckets > bulk_threshold: Use BULK (updates ALL buckets)
        - If stale buckets <= bulk_threshold: Use per-bucket (updates only stale)
        
        Note: Bulk mode always updates ALL buckets because:
        1. radosgw-admin returns all-or-nothing (can't fetch partial)
        2. If we fetch all, might as well update all
        3. Keeps entire dataset fresh
        
        Args:
            limit: Max buckets for per-bucket mode (ignored in bulk mode)
            verbose: Print progress
            bulk_threshold: Use bulk if more than this many stale (default 500)
        """
        cycle_start = time.time()
        
        if verbose:
            self.output("\n" + "=" * 60)
            self.output("COLLECTION CYCLE")
            self.output("=" * 60)
            self.output(f"  Stale threshold: {self.config.stale_threshold_seconds}s")
            self.output(f"  Bulk threshold:  {bulk_threshold} buckets")
        
        # Get all bucket names from RGW
        if verbose:
            self.output("  Fetching bucket list from RGW...")
        
        all_buckets = self.rgw_client.get_all_buckets()
        total_buckets = len(all_buckets)
        
        if verbose:
            self.output(f"  Total buckets in RGW: {total_buckets}")
        
        # Check DB status
        summary = self.storage.get_summary()
        in_db = summary.get('total_buckets', 0) or 0
        
        if verbose:
            self.output(f"  Buckets in DB: {in_db}")
        
        # Get uncollected buckets (not in DB)
        uncollected = self.storage.get_uncollected_buckets(all_buckets, limit=total_buckets)
        
        if verbose:
            self.output(f"  Uncollected (not in DB): {len(uncollected)}")
        
        # Get stale buckets (in DB but old)
        stale_buckets = self.storage.get_stale_buckets(
            self.config.stale_threshold_seconds, 
            limit=total_buckets
        )
        
        if verbose:
            self.output(f"  Stale (older than {self.config.stale_threshold_seconds}s): {len(stale_buckets)}")
        
        # Total needing update
        total_to_update = len(uncollected) + len(stale_buckets)
        
        if total_to_update == 0:
            if verbose:
                self.output("  ✓ All buckets are up to date!")
            return {
                'collected': 0, 
                'duration': time.time() - cycle_start,
                'total_buckets': total_buckets,
                'in_db': in_db,
                'stale': 0,
                'mode': 'none'
            }
        
        # Decide strategy
        use_bulk = total_to_update > bulk_threshold
        
        if use_bulk:
            # BULK MODE: Fetch all, update all
            if verbose:
                self.output(f"  Strategy: BULK (stale {total_to_update} > threshold {bulk_threshold})")
                self.output(f"  Note: Bulk mode updates ALL {total_buckets} buckets")
            
            result = self.run_bulk_refresh(verbose=verbose)
            result['total_buckets'] = total_buckets
            result['in_db'] = in_db
            result['stale'] = total_to_update
            
        else:
            # PER-BUCKET MODE: Update only stale buckets
            if verbose:
                self.output(f"  Strategy: Per-bucket (stale {total_to_update} <= threshold {bulk_threshold})")
            
            # Combine uncollected + stale
            buckets_to_update = list(set(uncollected) | set(stale_buckets))
            
            # Apply limit if specified
            if limit and len(buckets_to_update) > limit:
                buckets_to_update = buckets_to_update[:limit]
            
            if verbose:
                self.output(f"  Will collect: {len(buckets_to_update)} buckets")
            
            # Calculate workers
            workers = self.config.calculate_workers(len(buckets_to_update))
            if verbose:
                self.output(f"  Using {workers} workers")
                self.output("-" * 60)
            
            # Collect per-bucket
            collected = self.collect_buckets(buckets_to_update, workers=workers, verbose=verbose)
            
            cycle_duration = time.time() - cycle_start
            self.state.record_cycle(collected, cycle_duration, workers)
            
            result = {
                'collected': collected, 
                'duration': cycle_duration, 
                'workers': workers,
                'total_buckets': total_buckets,
                'in_db': in_db,
                'stale': total_to_update,
                'mode': 'per-bucket'
            }
        
        if verbose:
            self.output("-" * 60)
            self.output(f"  Collected: {result['collected']} buckets")
            self.output(f"  Duration:  {result['duration']:.1f}s")
            if result['duration'] > 0:
                self.output(f"  Rate:      {result['collected']/result['duration']:.1f} buckets/sec")
            self.output(f"  Mode:      {result['mode']}")
            self.output("=" * 60 + "\n")
        
        return result
    
    def run_continuous(self, verbose: bool = False):
        """
        Run continuous collection.
        
        Each cycle:
        1. Find buckets older than stale_threshold (or not in DB)
        2. Collect them
        3. Update JSON cache (for dashboard access)
        4. Sleep for refresh_interval
        5. Repeat
        """
        self.output("\n" + "=" * 60)
        self.output("CONTINUOUS COLLECTION MODE")
        self.output("=" * 60)
        self.output(f"  Stale threshold:  {self.config.stale_threshold_seconds}s "
                   f"({self.config.stale_threshold_seconds // 60} min)")
        self.output(f"  Refresh interval: {self.config.refresh_interval_seconds}s "
                   f"({self.config.refresh_interval_seconds // 60} min)")
        self.output(f"  Max workers:      {self.config.max_workers}")
        self.output(f"  Collect sync:     {self.config.collect_sync_status}")
        if self._cache:
            self.output(f"  Cache file:       {self.cache_path}")
        self.output("=" * 60)
        self.output("  Press Ctrl+C to stop\n")
        
        cycle = 0
        while self.state.is_running():
            cycle += 1
            
            self.output(f"[Cycle {cycle}] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
            # Collect all stale buckets (no artificial limit)
            result = self.run_once(limit=None, verbose=verbose)
            
            if not self.state.is_running():
                break
            
            # Update cache for dashboard
            self._update_cache()
            
            # Summary
            if result['collected'] == 0:
                self.output(f"  ✓ All {result.get('in_db', 0)} buckets are fresh. "
                           f"Sleeping {self.config.refresh_interval_seconds}s...")
            else:
                rate = result['collected'] / result['duration'] if result['duration'] > 0 else 0
                self.output(f"  Collected {result['collected']} in {result['duration']:.1f}s "
                           f"({rate:.1f}/sec)")
            
            # Sleep with interrupt check
            sleep_until = time.time() + self.config.refresh_interval_seconds
            while time.time() < sleep_until and self.state.is_running():
                time.sleep(1)
        
        self.output("\n" + "=" * 60)
        self.output("COLLECTION STOPPED")
        self.output("=" * 60)
        stats = self.state.get_stats()
        self.output(f"  Total collected:  {stats['total_collected']}")
        self.output(f"  Total errors:     {stats['total_errors']}")
        self.output(f"  Cycles completed: {stats['cycles_completed']}")
        self.output("=" * 60 + "\n")
    
    def get_status(self) -> dict:
        """Get current collector status."""
        summary = self.storage.get_summary()
        stale = self.storage.get_stale_count(self.config.stale_threshold_seconds)
        
        return {
            'summary': summary,
            'stale_buckets': stale,
            'collector_stats': self.state.get_stats(),
            'config': {
                'refresh_interval': self.config.refresh_interval_seconds,
                'stale_threshold': self.config.stale_threshold_seconds,
                'batch_size': self.config.batch_size,
                'max_workers': self.config.max_workers,
                'auto_scale': self.config.auto_scale_workers
            }
        }
    
    def close(self):
        """Clean up resources."""
        if self.storage:
            self.storage.close()