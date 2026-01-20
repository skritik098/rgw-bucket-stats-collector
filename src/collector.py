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
    - Bootstrap mode: Fast initial collection with max parallelism
    - Auto-scaling workers: Adjusts based on bucket count
    - Continuous mode: Runs as daemon, updating stale buckets
    - Incremental updates: Only collects buckets older than threshold
    - Sync collection: Optional multisite sync status
    - Graceful shutdown: Ctrl+C stops cleanly
    """
    
    def __init__(self, config: CollectorConfig, 
                 rgw_client: RGWAdminClient = None,
                 storage: Storage = None,
                 output_callback: Callable[[str], None] = None):
        self.config = config
        self.rgw_client = rgw_client or RGWAdminClient(
            ceph_conf=config.ceph_conf,
            timeout=config.command_timeout
        )
        self.storage = storage or Storage(config.db_path)
        self.state = CollectorState()
        self.output = output_callback or print
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
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
        Uses maximum parallelism and ignores stale checks.
        """
        start_time = time.time()
        
        self.output("\n" + "=" * 60)
        self.output("BOOTSTRAP MODE - Initial Collection")
        self.output("=" * 60)
        self.output(f"  Max workers:     {self.config.bootstrap_workers}")
        self.output(f"  Batch size:      {self.config.batch_size}")
        self.output(f"  Collect sync:    {self.config.collect_sync_status}")
        self.output("=" * 60)
        
        # Get all buckets
        self.output("  Fetching bucket list...")
        all_buckets = self.rgw_client.get_all_buckets()
        total = len(all_buckets)
        self.output(f"  Found {total} total buckets")
        
        if total == 0:
            return {'collected': 0, 'duration': 0, 'mode': 'bootstrap'}
        
        # Check what we already have
        existing = len(self.storage.get_summary().get('total_buckets', 0) or 0)
        uncollected = self.storage.get_uncollected_buckets(all_buckets, limit=total)
        
        self.output(f"  Already in DB:   {existing}")
        self.output(f"  Need to collect: {len(uncollected)}")
        
        if not uncollected:
            self.output("  All buckets already collected!")
            return {'collected': 0, 'duration': time.time() - start_time, 'mode': 'bootstrap'}
        
        # Calculate optimal workers
        workers = self.config.bootstrap_workers
        self.output(f"  Using {workers} workers")
        self.output("-" * 60)
        
        # Collect all uncollected buckets
        collected = self.collect_buckets(uncollected, workers=workers, verbose=verbose)
        
        duration = time.time() - start_time
        rate = collected / duration if duration > 0 else 0
        
        self.output("\n" + "=" * 60)
        self.output("BOOTSTRAP COMPLETE")
        self.output("=" * 60)
        self.output(f"  Collected:    {collected} buckets")
        self.output(f"  Duration:     {duration:.1f}s")
        self.output(f"  Rate:         {rate:.1f} buckets/sec")
        self.output("=" * 60 + "\n")
        
        return {'collected': collected, 'duration': duration, 'rate': rate, 'mode': 'bootstrap'}
    
    def run_once(self, limit: int = None, verbose: bool = False) -> dict:
        """
        Run a single collection cycle.
        
        Collects buckets that are:
        - Not in DB (uncollected), OR
        - In DB but older than stale_threshold
        
        Args:
            limit: Max buckets to collect (None = all stale buckets)
            verbose: Print progress
        """
        cycle_start = time.time()
        
        if verbose:
            self.output("\n" + "=" * 60)
            self.output("COLLECTION CYCLE")
            self.output("=" * 60)
            self.output(f"  Stale threshold: {self.config.stale_threshold_seconds}s")
            if limit:
                self.output(f"  *** LIMIT: Max {limit} buckets ***")
        
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
            self.output(f"  Buckets already in DB: {in_db}")
        
        # Get uncollected count (for debugging)
        uncollected = self.storage.get_uncollected_buckets(all_buckets, limit=total_buckets)
        
        if verbose:
            self.output(f"  Uncollected (not in DB): {len(uncollected)}")
        
        # Get stale count (for debugging)
        stale_count = self.storage.get_stale_count(self.config.stale_threshold_seconds)
        
        if verbose:
            self.output(f"  Stale (older than {self.config.stale_threshold_seconds}s): {stale_count}")
        
        # Get buckets to update
        # If no limit, get all that need updating
        effective_limit = limit if limit else (total_buckets + 1000)
        
        buckets_to_update = self.storage.get_buckets_to_update(
            all_buckets,
            self.config.stale_threshold_seconds,
            limit=effective_limit
        )
        
        if verbose:
            self.output(f"  Will collect: {len(buckets_to_update)} buckets")
        
        if not buckets_to_update:
            if verbose:
                self.output("  ✓ All buckets are up to date!")
            return {
                'collected': 0, 
                'duration': time.time() - cycle_start,
                'total_buckets': total_buckets,
                'in_db': in_db,
                'uncollected': len(uncollected),
                'stale': stale_count,
                'workers': 0
            }
        
        # Calculate workers
        workers = self.config.calculate_workers(len(buckets_to_update))
        if verbose:
            self.output(f"  Using {workers} workers")
            self.output("-" * 60)
        
        # Collect
        collected = self.collect_buckets(buckets_to_update, workers=workers, verbose=verbose)
        
        cycle_duration = time.time() - cycle_start
        self.state.record_cycle(collected, cycle_duration, workers)
        
        if verbose:
            self.output("-" * 60)
            self.output(f"  Collected: {collected} buckets")
            self.output(f"  Duration:  {cycle_duration:.1f}s")
            if cycle_duration > 0:
                self.output(f"  Rate:      {collected/cycle_duration:.1f} buckets/sec")
            self.output("=" * 60 + "\n")
        
        return {
            'collected': collected, 
            'duration': cycle_duration, 
            'workers': workers,
            'total_buckets': total_buckets,
            'in_db': in_db,
            'uncollected': len(uncollected),
            'stale': stale_count
        }
    
    def run_continuous(self, verbose: bool = False):
        """
        Run continuous collection.
        
        Each cycle:
        1. Find buckets older than stale_threshold (or not in DB)
        2. Collect them
        3. Sleep for refresh_interval
        4. Repeat
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
        self.storage.close()