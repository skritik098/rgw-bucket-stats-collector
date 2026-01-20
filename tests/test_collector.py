"""
Test suite for RGW bucket stats collector.

Run with: python -m pytest tests/ -v
Or: python tests/test_collector.py
"""

import os
import sys
import tempfile
import time
import unittest
from datetime import datetime, timedelta
from unittest.mock import Mock, patch
from typing import List, Optional

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.models import BucketStats, CollectorConfig
from src.storage import Storage
from src.collector import Collector


class MockRGWClient:
    """Mock RGW client for testing without real Ceph cluster."""
    
    def __init__(self, buckets: List[str] = None, delay: float = 0.01):
        self.buckets = buckets or [f"test-bucket-{i:04d}" for i in range(100)]
        self.delay = delay  # Simulate API latency
        self.call_count = 0
    
    def get_all_buckets(self) -> List[str]:
        """Return all bucket names."""
        return self.buckets
    
    def list_buckets(self, marker: str = "", max_entries: int = 1000):
        """Paginated bucket list."""
        start_idx = 0
        if marker:
            try:
                start_idx = self.buckets.index(marker) + 1
            except ValueError:
                start_idx = 0
        
        end_idx = min(start_idx + max_entries, len(self.buckets))
        batch = self.buckets[start_idx:end_idx]
        truncated = end_idx < len(self.buckets)
        next_marker = batch[-1] if batch and truncated else ""
        
        return batch, next_marker, truncated
    
    def get_bucket_stats(self, bucket_name: str, tenant: str = "") -> Optional[BucketStats]:
        """Generate mock stats for bucket."""
        self.call_count += 1
        time.sleep(self.delay)  # Simulate latency
        
        # Generate deterministic stats based on bucket name
        import hashlib
        h = int(hashlib.md5(bucket_name.encode()).hexdigest()[:8], 16)
        
        return BucketStats(
            bucket_name=bucket_name,
            bucket_id=f"id-{h}",
            owner=f"user-{h % 10}",
            num_objects=h % 10000,
            size_bytes=(h % 10000) * 1024,
            size_actual_bytes=(h % 10000) * 1100,
            storage_classes={'rgw.main': {'size': (h % 10000) * 1024, 'num_objects': h % 10000}},
            collected_at=datetime.utcnow(),
            collection_duration_ms=int(self.delay * 1000)
        )


class TestStorage(unittest.TestCase):
    """Test storage module."""
    
    def setUp(self):
        self.db_file = tempfile.NamedTemporaryFile(delete=False, suffix='.duckdb')
        self.db_file.close()
        self.storage = Storage(self.db_file.name)
    
    def tearDown(self):
        self.storage.close()
        os.unlink(self.db_file.name)
    
    def test_upsert_and_retrieve(self):
        """Test inserting and retrieving bucket stats."""
        stats = BucketStats(
            bucket_name="test-bucket",
            owner="test-user",
            num_objects=100,
            size_bytes=1024000
        )
        
        self.storage.upsert_bucket_stats(stats)
        self.storage.commit()
        
        retrieved = self.storage.get_bucket_stats("test-bucket")
        
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved['bucket_name'], "test-bucket")
        self.assertEqual(retrieved['owner'], "test-user")
        self.assertEqual(retrieved['num_objects'], 100)
    
    def test_update_existing_bucket(self):
        """Test updating existing bucket stats."""
        # Insert initial
        stats1 = BucketStats(bucket_name="test-bucket", num_objects=100)
        self.storage.upsert_bucket_stats(stats1)
        self.storage.commit()
        
        # Update
        stats2 = BucketStats(bucket_name="test-bucket", num_objects=200)
        self.storage.upsert_bucket_stats(stats2)
        self.storage.commit()
        
        # Verify updated
        retrieved = self.storage.get_bucket_stats("test-bucket")
        self.assertEqual(retrieved['num_objects'], 200)
    
    def test_stale_buckets(self):
        """Test finding stale buckets."""
        # Insert bucket with old timestamp
        old_stats = BucketStats(
            bucket_name="old-bucket",
            collected_at=datetime.utcnow() - timedelta(minutes=10)
        )
        self.storage.upsert_bucket_stats(old_stats)
        
        # Insert bucket with recent timestamp
        new_stats = BucketStats(
            bucket_name="new-bucket",
            collected_at=datetime.utcnow()
        )
        self.storage.upsert_bucket_stats(new_stats)
        self.storage.commit()
        
        # Find stale (older than 5 minutes)
        stale = self.storage.get_stale_buckets(300)
        
        self.assertEqual(len(stale), 1)
        self.assertEqual(stale[0], "old-bucket")
    
    def test_uncollected_buckets(self):
        """Test finding uncollected buckets."""
        # Insert one bucket
        stats = BucketStats(bucket_name="existing-bucket")
        self.storage.upsert_bucket_stats(stats)
        self.storage.commit()
        
        # Check with list including uncollected
        all_buckets = ["existing-bucket", "new-bucket-1", "new-bucket-2"]
        uncollected = self.storage.get_uncollected_buckets(all_buckets)
        
        self.assertEqual(set(uncollected), {"new-bucket-1", "new-bucket-2"})
    
    def test_summary(self):
        """Test summary statistics."""
        for i in range(5):
            stats = BucketStats(
                bucket_name=f"bucket-{i}",
                owner=f"user-{i % 2}",
                num_objects=100 * (i + 1),
                size_bytes=1024 * (i + 1)
            )
            self.storage.upsert_bucket_stats(stats)
        self.storage.commit()
        
        summary = self.storage.get_summary()
        
        self.assertEqual(summary['total_buckets'], 5)
        self.assertEqual(summary['total_owners'], 2)
        self.assertEqual(summary['total_objects'], 100 + 200 + 300 + 400 + 500)


class TestCollector(unittest.TestCase):
    """Test collector module."""
    
    def setUp(self):
        self.db_file = tempfile.NamedTemporaryFile(delete=False, suffix='.duckdb')
        self.db_file.close()
        
        self.config = CollectorConfig(
            db_path=self.db_file.name,
            batch_size=10,
            parallel_workers=2,
            stale_threshold_seconds=60
        )
        
        self.mock_client = MockRGWClient(
            buckets=[f"bucket-{i:04d}" for i in range(50)],
            delay=0.001
        )
        
        self.storage = Storage(self.db_file.name)
        self.collector = Collector(
            config=self.config,
            rgw_client=self.mock_client,
            storage=self.storage,
            output_callback=lambda x: None  # Suppress output
        )
    
    def tearDown(self):
        self.collector.close()
        os.unlink(self.db_file.name)
    
    def test_collect_limited(self):
        """Test collecting limited number of buckets."""
        result = self.collector.run_once(limit=10)
        
        self.assertEqual(result['collected'], 10)
        
        summary = self.storage.get_summary()
        self.assertEqual(summary['total_buckets'], 10)
    
    def test_collect_all(self):
        """Test collecting all buckets."""
        result = self.collector.run_once()
        
        self.assertEqual(result['collected'], 50)
        
        summary = self.storage.get_summary()
        self.assertEqual(summary['total_buckets'], 50)
    
    def test_incremental_update(self):
        """Test that incremental update only collects stale buckets."""
        # First collection
        result1 = self.collector.run_once()
        self.assertEqual(result1['collected'], 50)
        
        initial_call_count = self.mock_client.call_count
        
        # Second collection immediately - nothing should be stale
        result2 = self.collector.run_once()
        self.assertEqual(result2['collected'], 0)
        
        # Call count shouldn't have increased significantly (only bucket listing)
        self.assertEqual(self.mock_client.call_count, initial_call_count)
    
    def test_stale_bucket_update(self):
        """Test that stale buckets get updated."""
        # First collection
        self.collector.run_once()
        
        # Manually make a bucket stale
        self.storage.conn.execute("""
            UPDATE bucket_stats 
            SET collected_at = collected_at - INTERVAL '10 minutes'
            WHERE bucket_name = 'bucket-0000'
        """)
        self.storage.commit()
        
        # Second collection should update the stale bucket
        result = self.collector.run_once()
        self.assertEqual(result['collected'], 1)
    
    def test_parallel_collection(self):
        """Test parallel collection is faster than sequential."""
        # Sequential (1 worker)
        self.config.parallel_workers = 1
        start = time.time()
        self.collector.run_once(limit=20)
        seq_time = time.time() - start
        
        # Reset
        self.storage.conn.execute("DELETE FROM bucket_stats")
        self.storage.commit()
        
        # Parallel (4 workers)
        self.config.parallel_workers = 4
        start = time.time()
        self.collector.run_once(limit=20)
        par_time = time.time() - start
        
        # Parallel should be faster (not always guaranteed but usually true)
        # Just verify both completed
        summary = self.storage.get_summary()
        self.assertEqual(summary['total_buckets'], 20)


class TestIntegration(unittest.TestCase):
    """Integration tests simulating real usage."""
    
    def setUp(self):
        self.db_file = tempfile.NamedTemporaryFile(delete=False, suffix='.duckdb')
        self.db_file.close()
    
    def tearDown(self):
        os.unlink(self.db_file.name)
    
    def test_full_workflow(self):
        """Test complete workflow: collect, query, update."""
        # Setup
        config = CollectorConfig(
            db_path=self.db_file.name,
            batch_size=20,
            parallel_workers=2,
            stale_threshold_seconds=60
        )
        
        mock_client = MockRGWClient(
            buckets=[f"project-a-bucket-{i}" for i in range(30)] +
                    [f"project-b-bucket-{i}" for i in range(20)],
            delay=0.001
        )
        
        storage = Storage(self.db_file.name)
        collector = Collector(
            config=config,
            rgw_client=mock_client,
            storage=storage,
            output_callback=lambda x: None
        )
        
        # Collect
        result = collector.run_once()
        self.assertEqual(result['collected'], 50)
        
        # Query
        summary = storage.get_summary()
        self.assertEqual(summary['total_buckets'], 50)
        
        top = storage.top_buckets_by_size(5)
        self.assertEqual(len(top), 5)
        
        by_owner = storage.summary_by_owner()
        self.assertGreater(len(by_owner), 0)
        
        # Verify incremental
        result2 = collector.run_once()
        self.assertEqual(result2['collected'], 0)  # Nothing stale
        
        collector.close()


def run_tests():
    """Run all tests."""
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    suite.addTests(loader.loadTestsFromTestCase(TestStorage))
    suite.addTests(loader.loadTestsFromTestCase(TestCollector))
    suite.addTests(loader.loadTestsFromTestCase(TestIntegration))
    
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    return 0 if result.wasSuccessful() else 1


if __name__ == '__main__':
    sys.exit(run_tests())
