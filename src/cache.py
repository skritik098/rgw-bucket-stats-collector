"""
JSON cache for real-time dashboard access.

Avoids DuckDB lock contention by exporting stats to a JSON file
that can be read without locking.
"""

import json
import os
from datetime import datetime
from typing import Dict, Any, Optional
from pathlib import Path


class StatsCache:
    """
    JSON cache for bucket stats.
    
    The collector writes to this cache after each collection.
    The dashboard reads from this cache (no DB lock needed).
    """
    
    def __init__(self, cache_path: str = "rgw_stats_cache.json"):
        self.cache_path = cache_path
        self.temp_path = cache_path + ".tmp"
    
    def write(self, data: Dict[str, Any]):
        """
        Write stats to cache file (atomic write).
        
        Uses temp file + rename for atomic update.
        """
        data['_cache_updated'] = datetime.utcnow().isoformat()
        
        # Write to temp file first
        with open(self.temp_path, 'w') as f:
            json.dump(data, f, indent=2, default=str)
        
        # Atomic rename
        os.replace(self.temp_path, self.cache_path)
    
    def read(self) -> Optional[Dict[str, Any]]:
        """Read stats from cache file."""
        if not os.path.exists(self.cache_path):
            return None
        
        try:
            with open(self.cache_path, 'r') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return None
    
    def get_age_seconds(self) -> Optional[float]:
        """Get cache age in seconds."""
        data = self.read()
        if not data or '_cache_updated' not in data:
            return None
        
        try:
            updated = datetime.fromisoformat(data['_cache_updated'])
            return (datetime.utcnow() - updated).total_seconds()
        except:
            return None
    
    def exists(self) -> bool:
        """Check if cache file exists."""
        return os.path.exists(self.cache_path)


def build_cache_data(storage) -> Dict[str, Any]:
    """
    Build cache data from storage.
    
    Extracts all data needed for dashboard display.
    """
    # Get summary
    summary = storage.get_summary()
    
    # Get top buckets by size
    top_by_size = storage.conn.execute("""
        SELECT bucket_name, owner, size_bytes, size_actual_bytes, 
               num_objects, num_shards, collected_at
        FROM bucket_stats 
        ORDER BY size_bytes DESC 
        LIMIT 100
    """).fetchall()
    
    # Get top buckets by objects
    top_by_objects = storage.conn.execute("""
        SELECT bucket_name, owner, size_bytes, num_objects, collected_at
        FROM bucket_stats 
        ORDER BY num_objects DESC 
        LIMIT 100
    """).fetchall()
    
    # Get freshness distribution
    freshness = storage.conn.execute("""
        SELECT 
            CASE 
                WHEN collected_at >= NOW() - INTERVAL '10 minutes' THEN 'fresh_10m'
                WHEN collected_at >= NOW() - INTERVAL '1 hour' THEN 'fresh_1h'
                WHEN collected_at >= NOW() - INTERVAL '24 hours' THEN 'fresh_24h'
                ELSE 'stale'
            END as freshness,
            COUNT(*) as count
        FROM bucket_stats
        GROUP BY 1
    """).fetchall()
    
    # Get owner summary
    by_owner = storage.conn.execute("""
        SELECT owner, COUNT(*) as bucket_count, 
               SUM(size_bytes) as total_size,
               SUM(num_objects) as total_objects
        FROM bucket_stats
        GROUP BY owner
        ORDER BY total_size DESC
        LIMIT 50
    """).fetchall()
    
    # Get all buckets basic info
    all_buckets = storage.conn.execute("""
        SELECT bucket_name, owner, size_bytes, num_objects, 
               collected_at, num_shards
        FROM bucket_stats
        ORDER BY bucket_name
    """).fetchall()
    
    # Get sync status if available
    sync_status = storage.conn.execute("""
        SELECT sync_status, COUNT(*) as count
        FROM bucket_stats
        WHERE sync_status IS NOT NULL AND sync_status != ''
        GROUP BY sync_status
    """).fetchall()
    
    # Buckets behind on sync
    sync_behind = storage.conn.execute("""
        SELECT bucket_name, owner, sync_status, 
               sync_behind_shards, sync_behind_entries
        FROM bucket_stats
        WHERE sync_behind_shards > 0 OR sync_behind_entries > 0
        ORDER BY sync_behind_entries DESC
        LIMIT 50
    """).fetchall()
    
    return {
        'summary': {
            'total_buckets': summary.get('total_buckets', 0),
            'total_owners': summary.get('total_owners', 0),
            'total_size_bytes': summary.get('total_size_bytes', 0),
            'total_objects': summary.get('total_objects', 0),
            'oldest_collection': str(summary.get('oldest_collection', '')),
            'newest_collection': str(summary.get('newest_collection', '')),
        },
        'top_by_size': [
            {
                'bucket_name': r[0], 'owner': r[1], 'size_bytes': r[2],
                'size_actual_bytes': r[3], 'num_objects': r[4], 
                'num_shards': r[5], 'collected_at': str(r[6])
            } for r in top_by_size
        ],
        'top_by_objects': [
            {
                'bucket_name': r[0], 'owner': r[1], 'size_bytes': r[2],
                'num_objects': r[3], 'collected_at': str(r[4])
            } for r in top_by_objects
        ],
        'freshness': {r[0]: r[1] for r in freshness},
        'by_owner': [
            {
                'owner': r[0], 'bucket_count': r[1],
                'total_size': r[2], 'total_objects': r[3]
            } for r in by_owner
        ],
        'all_buckets': [
            {
                'bucket_name': r[0], 'owner': r[1], 'size_bytes': r[2],
                'num_objects': r[3], 'collected_at': str(r[4]), 
                'num_shards': r[5]
            } for r in all_buckets
        ],
        'sync_summary': {r[0]: r[1] for r in sync_status},
        'sync_behind': [
            {
                'bucket_name': r[0], 'owner': r[1], 'sync_status': r[2],
                'shards_behind': r[3], 'entries_behind': r[4]
            } for r in sync_behind
        ],
    }