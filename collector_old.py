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
            'cycles_completed': 0
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
    
    def record_cycle(self, buckets: int, duration: float):
        with self._lock:
            self.stats['last_cycle_buckets'] = buckets
            self.stats['last_cycle_time'] = duration
            self.stats['cycles_completed'] += 1
    
    def get_stats(self) -> dict:
        with self._lock:
            return self.stats.copy()


class Collector:
    """
    Bucket stats collector with continuous incremental updates.
    
    Key features:
    - Continuous mode: runs as daemon, updating stale buckets
    - Incremental updates: only collects buckets older than threshold
    - Parallel collection: configurable worker threads
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
        return self.rgw_client.get_bucket_stats(bucket_name)
    
    def _collect_batch(self, buckets: List[str]) -> List[BucketStats]:
        """Collect stats for a batch of buckets in parallel."""
        results = []
        
        with ThreadPoolExecutor(max_workers=self.config.parallel_workers) as executor:
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
    
    def collect_buckets(self, bucket_names: List[str], verbose: bool = False) -> int:
        """
        Collect stats for specific buckets.
        Returns: number of buckets successfully collected
        """
        if not bucket_names:
            return 0
        
        collected = 0
        
        # Process in batches
        for i in range(0, len(bucket_names), self.config.batch_size):
            if not self.state.is_running():
                break
            
            batch = bucket_names[i:i + self.config.batch_size]
            batch_start = time.time()
            
            if verbose:
                self.output(f"  Collecting batch {i//self.config.batch_size + 1} "
                          f"({len(batch)} buckets)...")
            
            results = self._collect_batch(batch)
            
            # Save to storage
            for stats in results:
                self.storage.upsert_bucket_stats(stats)
            self.storage.commit()
            
            collected += len(results)
            
            if verbose:
                batch_time = time.time() - batch_start
                self.output(f"    Done: {len(results)}/{len(batch)} in {batch_time:.1f}s")
        
        return collected
    
    def run_once(self, limit: int = None, verbose: bool = False) -> dict:
        """
        Run a single collection cycle.
        
        Args:
            limit: Max buckets to collect (None for all stale)
            verbose: Print progress
        
        Returns:
            Statistics about the collection
        """
        cycle_start = time.time()
        
        if verbose:
            self.output("\n" + "=" * 60)
            self.output("COLLECTION CYCLE")
            self.output("=" * 60)
        
        # Get all bucket names from RGW
        if verbose:
            self.output("  Fetching bucket list...")
        
        all_buckets = self.rgw_client.get_all_buckets()
        
        if verbose:
            self.output(f"  Found {len(all_buckets)} total buckets")
        
        # Find buckets that need updating
        buckets_to_update = self.storage.get_buckets_to_update(
            all_buckets,
            self.config.stale_threshold_seconds,
            limit=limit or len(all_buckets)
        )
        
        if verbose:
            self.output(f"  Buckets needing update: {len(buckets_to_update)}")
        
        if not buckets_to_update:
            if verbose:
                self.output("  All buckets are up to date!")
            return {'collected': 0, 'duration': time.time() - cycle_start}
        
        # Collect
        collected = self.collect_buckets(buckets_to_update, verbose)
        
        cycle_duration = time.time() - cycle_start
        self.state.record_cycle(collected, cycle_duration)
        
        if verbose:
            self.output("-" * 60)
            self.output(f"  Collected: {collected} buckets")
            self.output(f"  Duration:  {cycle_duration:.1f}s")
            self.output(f"  Rate:      {collected/cycle_duration:.1f} buckets/sec" if cycle_duration > 0 else "")
            self.output("=" * 60 + "\n")
        
        return {'collected': collected, 'duration': cycle_duration}
    
    def run_continuous(self, verbose: bool = False):
        """
        Run continuous collection - keeps updating stale buckets.
        
        This mode:
        1. Finds buckets not updated within stale_threshold_seconds
        2. Updates them in parallel batches
        3. Sleeps for refresh_interval_seconds
        4. Repeats until interrupted
        """
        self.output("\n" + "=" * 60)
        self.output("CONTINUOUS COLLECTION MODE")
        self.output("=" * 60)
        self.output(f"  Refresh interval: {self.config.refresh_interval_seconds}s")
        self.output(f"  Stale threshold:  {self.config.stale_threshold_seconds}s")
        self.output(f"  Batch size:       {self.config.batch_size}")
        self.output(f"  Workers:          {self.config.parallel_workers}")
        self.output("=" * 60)
        self.output("  Press Ctrl+C to stop\n")
        
        cycle = 0
        while self.state.is_running():
            cycle += 1
            
            self.output(f"[Cycle {cycle}] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
            result = self.run_once(verbose=verbose)
            
            if not self.state.is_running():
                break
            
            # Get status
            summary = self.storage.get_summary()
            stale = self.storage.get_stale_count(self.config.stale_threshold_seconds)
            
            self.output(f"  Status: {summary['total_buckets']} buckets in DB, "
                       f"{stale} stale")
            
            if result['collected'] == 0 and stale == 0:
                self.output(f"  All up to date. Sleeping {self.config.refresh_interval_seconds}s...")
            else:
                self.output(f"  Collected {result['collected']} buckets in {result['duration']:.1f}s")
            
            # Sleep with interrupt check
            sleep_until = time.time() + self.config.refresh_interval_seconds
            while time.time() < sleep_until and self.state.is_running():
                time.sleep(1)
        
        self.output("\n" + "=" * 60)
        self.output("COLLECTION STOPPED")
        self.output("=" * 60)
        stats = self.state.get_stats()
        self.output(f"  Total collected: {stats['total_collected']}")
        self.output(f"  Total errors:    {stats['total_errors']}")
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
                'workers': self.config.parallel_workers
            }
        }
    
    def close(self):
        """Clean up resources."""
        self.storage.close()
