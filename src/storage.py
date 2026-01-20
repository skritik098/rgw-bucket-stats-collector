"""
DuckDB storage with incremental update support.
"""

import json
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any

try:
    import duckdb
except ImportError:
    raise ImportError("DuckDB required: pip install duckdb")

from .models import BucketStats


class Storage:
    """DuckDB storage with incremental update support."""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = duckdb.connect(db_path)
        self._init_schema()
        self._migrate_schema()
    
    def _migrate_schema(self):
        """Check and migrate schema if needed."""
        # Get current columns
        try:
            result = self.conn.execute("""
                SELECT column_name FROM information_schema.columns 
                WHERE table_name = 'bucket_stats'
            """).fetchall()
            existing_cols = {r[0] for r in result}
        except:
            return  # Table doesn't exist yet
        
        # Define expected columns and their types
        expected_cols = {
            'marker': 'VARCHAR',
            'explicit_placement': 'JSON',
            'index_type': 'VARCHAR',
            'versioning': 'VARCHAR',
            'versioned': 'BOOLEAN',
            'versioning_enabled': 'BOOLEAN',
            'object_lock_enabled': 'BOOLEAN',
            'mfa_enabled': 'BOOLEAN',
            'ver': 'VARCHAR',
            'master_ver': 'VARCHAR',
            'max_marker': 'VARCHAR',
            'mtime': 'VARCHAR',
            'creation_time': 'VARCHAR',
            'size_utilized_bytes': 'BIGINT',
            'usage': 'JSON',
            'bucket_quota': 'JSON',
        }
        
        # Add missing columns
        for col, dtype in expected_cols.items():
            if col not in existing_cols:
                try:
                    default = "'{}'" if dtype == 'JSON' else ("''" if dtype == 'VARCHAR' else ('false' if dtype == 'BOOLEAN' else '0'))
                    self.conn.execute(f"ALTER TABLE bucket_stats ADD COLUMN {col} {dtype} DEFAULT {default}")
                except:
                    pass  # Column might already exist or other issue
        
        self.conn.commit()
    
    def _init_schema(self):
        """Initialize database schema."""
        # Main bucket stats table - single row per bucket (latest stats)
        # Stores ALL fields from radosgw-admin bucket stats
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS bucket_stats (
                bucket_name VARCHAR PRIMARY KEY,
                bucket_id VARCHAR,
                marker VARCHAR,
                tenant VARCHAR,
                owner VARCHAR,
                zonegroup VARCHAR,
                placement_rule VARCHAR,
                explicit_placement JSON,
                num_shards INTEGER,
                index_type VARCHAR,
                versioning VARCHAR,
                versioned BOOLEAN,
                versioning_enabled BOOLEAN,
                object_lock_enabled BOOLEAN,
                mfa_enabled BOOLEAN,
                ver VARCHAR,
                master_ver VARCHAR,
                max_marker VARCHAR,
                mtime VARCHAR,
                creation_time VARCHAR,
                size_bytes BIGINT,
                size_actual_bytes BIGINT,
                size_utilized_bytes BIGINT,
                num_objects BIGINT,
                usage JSON,
                bucket_quota JSON,
                sync_status VARCHAR,
                sync_behind_shards INTEGER,
                sync_behind_entries INTEGER,
                sync_source_zone VARCHAR,
                collected_at TIMESTAMP,
                collection_duration_ms INTEGER
            )
        """)
        
        # Historical stats for tracking changes over time
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS bucket_stats_history (
                id INTEGER PRIMARY KEY,
                bucket_name VARCHAR,
                size_bytes BIGINT,
                num_objects BIGINT,
                sync_behind_shards INTEGER,
                sync_behind_entries INTEGER,
                collected_at TIMESTAMP
            )
        """)
        
        # Auto-increment for history
        self.conn.execute("""
            CREATE SEQUENCE IF NOT EXISTS history_seq START 1
        """)
        
        # Storage class breakdown
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS storage_class_usage (
                bucket_name VARCHAR,
                storage_class VARCHAR,
                size_bytes BIGINT,
                size_actual_bytes BIGINT,
                size_utilized_bytes BIGINT,
                num_objects BIGINT,
                collected_at TIMESTAMP,
                PRIMARY KEY (bucket_name, storage_class)
            )
        """)
        
        # Indexes
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_stats_owner ON bucket_stats(owner)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_stats_collected ON bucket_stats(collected_at)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_history_bucket ON bucket_stats_history(bucket_name)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_history_time ON bucket_stats_history(collected_at)")
        
        self.conn.commit()
    
    def upsert_bucket_stats(self, stats: BucketStats, save_history: bool = True):
        """Insert or update bucket statistics."""
        # Upsert main stats with explicit column names
        self.conn.execute("""
            INSERT OR REPLACE INTO bucket_stats (
                bucket_name, bucket_id, marker, tenant, owner, zonegroup,
                placement_rule, explicit_placement, num_shards, index_type,
                versioning, versioned, versioning_enabled, object_lock_enabled,
                mfa_enabled, ver, master_ver, max_marker, mtime, creation_time,
                size_bytes, size_actual_bytes, size_utilized_bytes, num_objects,
                usage, bucket_quota, sync_status, sync_behind_shards,
                sync_behind_entries, sync_source_zone, collected_at, collection_duration_ms
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
        """, [
            stats.bucket_name,
            stats.bucket_id,
            stats.marker,
            stats.tenant,
            stats.owner,
            stats.zonegroup,
            stats.placement_rule,
            json.dumps(stats.explicit_placement) if stats.explicit_placement else '{}',
            stats.num_shards,
            stats.index_type,
            stats.versioning,
            stats.versioned,
            stats.versioning_enabled,
            stats.object_lock_enabled,
            stats.mfa_enabled,
            stats.ver,
            stats.master_ver,
            stats.max_marker,
            stats.mtime,
            stats.creation_time,
            stats.size_bytes,
            stats.size_actual_bytes,
            stats.size_utilized_bytes,
            stats.num_objects,
            json.dumps(stats.usage) if stats.usage else '{}',
            json.dumps(stats.bucket_quota) if stats.bucket_quota else '{}',
            stats.sync_status,
            stats.sync_behind_shards,
            stats.sync_behind_entries,
            stats.sync_source_zone,
            stats.collected_at,
            stats.collection_duration_ms
        ])
        
        # Save to history (sampling - not every update)
        if save_history:
            self.conn.execute("""
                INSERT INTO bucket_stats_history VALUES (
                    nextval('history_seq'), ?, ?, ?, ?, ?, ?
                )
            """, [stats.bucket_name, stats.size_bytes, stats.num_objects, 
                  stats.sync_behind_shards, stats.sync_behind_entries, stats.collected_at])
        
        # Update storage class usage
        for sc_name, sc_data in (stats.usage or {}).items():
            self.conn.execute("""
                INSERT OR REPLACE INTO storage_class_usage VALUES (?, ?, ?, ?, ?, ?, ?)
            """, [
                stats.bucket_name, sc_name,
                sc_data.get('size', 0),
                sc_data.get('size_actual', 0),
                sc_data.get('size_utilized', 0),
                sc_data.get('num_objects', 0),
                stats.collected_at
            ])
    
    def get_stale_buckets(self, threshold_seconds: int, limit: int = 100) -> List[str]:
        """Get buckets that haven't been updated within threshold."""
        threshold_time = datetime.utcnow() - timedelta(seconds=threshold_seconds)
        
        # Include NULL collected_at as stale
        result = self.conn.execute("""
            SELECT bucket_name FROM bucket_stats 
            WHERE collected_at IS NULL OR collected_at < ?
            ORDER BY collected_at ASC NULLS FIRST
            LIMIT ?
        """, [threshold_time, limit]).fetchall()
        
        return [row[0] for row in result]
    
    def get_uncollected_buckets(self, all_buckets: List[str], limit: int = 100) -> List[str]:
        """Get buckets that have never been collected."""
        if not all_buckets:
            return []
        
        # Get buckets we already have
        result = self.conn.execute("SELECT bucket_name FROM bucket_stats").fetchall()
        existing = {row[0] for row in result}
        
        # Return buckets not in DB
        uncollected = [b for b in all_buckets if b not in existing]
        return uncollected[:limit]
    
    def get_buckets_to_update(self, all_buckets: List[str], threshold_seconds: int, 
                              limit: int = 100) -> List[str]:
        """
        Get buckets that need updating (uncollected or stale).
        Prioritizes uncollected buckets first, then stale ones.
        """
        # First, get uncollected
        uncollected = self.get_uncollected_buckets(all_buckets, limit)
        
        if len(uncollected) >= limit:
            return uncollected[:limit]
        
        # Then get stale
        remaining = limit - len(uncollected)
        stale = self.get_stale_buckets(threshold_seconds, remaining)
        
        return uncollected + stale
    
    def get_bucket_stats(self, bucket_name: str) -> Optional[Dict[str, Any]]:
        """Get stats for a specific bucket."""
        result = self.conn.execute("""
            SELECT * FROM bucket_stats WHERE bucket_name = ?
        """, [bucket_name]).fetchone()
        
        if not result:
            return None
        
        columns = ['bucket_name', 'bucket_id', 'tenant', 'owner', 'zonegroup',
                   'placement_rule', 'num_shards', 'size_bytes', 'size_actual_bytes',
                   'num_objects', 'storage_classes', 'sync_status', 'sync_behind_shards',
                   'collected_at', 'collection_duration_ms']
        return dict(zip(columns, result))
    
    def get_summary(self) -> Dict[str, Any]:
        """Get overall summary statistics."""
        result = self.conn.execute("""
            SELECT 
                COUNT(*) as total_buckets,
                COUNT(DISTINCT owner) as total_owners,
                COALESCE(SUM(num_objects), 0) as total_objects,
                COALESCE(SUM(size_bytes), 0) as total_size_bytes,
                MIN(collected_at) as oldest_collection,
                MAX(collected_at) as newest_collection
            FROM bucket_stats
        """).fetchone()
        
        return {
            'total_buckets': result[0],
            'total_owners': result[1],
            'total_objects': result[2],
            'total_size_bytes': result[3],
            'oldest_collection': result[4],
            'newest_collection': result[5]
        }
    
    def get_stale_count(self, threshold_seconds: int) -> int:
        """Count how many buckets are stale."""
        threshold_time = datetime.utcnow() - timedelta(seconds=threshold_seconds)
        result = self.conn.execute("""
            SELECT COUNT(*) FROM bucket_stats 
            WHERE collected_at IS NULL OR collected_at < ?
        """, [threshold_time]).fetchone()
        return result[0]
    
    def get_freshness_stats(self) -> Dict[str, Any]:
        """Get freshness statistics for debugging."""
        result = self.conn.execute("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN collected_at IS NULL THEN 1 ELSE 0 END) as null_count,
                MIN(collected_at) as oldest,
                MAX(collected_at) as newest
            FROM bucket_stats
        """).fetchone()
        return {
            'total': result[0],
            'null_collected_at': result[1],
            'oldest': result[2],
            'newest': result[3]
        }
    
    def query(self, sql: str) -> Any:
        """Execute custom SQL query."""
        return self.conn.execute(sql)
    
    def commit(self):
        """Commit transaction."""
        self.conn.commit()
    
    def close(self):
        """Close connection."""
        self.conn.close()
    
    # Query methods
    def top_buckets_by_size(self, limit: int = 20) -> List[Dict]:
        """Get largest buckets."""
        result = self.conn.execute(f"""
            SELECT bucket_name, owner, num_objects, 
                   size_bytes, collected_at
            FROM bucket_stats
            ORDER BY size_bytes DESC
            LIMIT {limit}
        """).fetchall()
        
        return [{'bucket_name': r[0], 'owner': r[1], 'num_objects': r[2],
                 'size_bytes': r[3], 'collected_at': r[4]} for r in result]
    
    def summary_by_owner(self) -> List[Dict]:
        """Get summary grouped by owner."""
        result = self.conn.execute("""
            SELECT owner, COUNT(*) as buckets, 
                   SUM(num_objects) as objects,
                   SUM(size_bytes) as size_bytes
            FROM bucket_stats
            GROUP BY owner
            ORDER BY size_bytes DESC
        """).fetchall()
        
        return [{'owner': r[0], 'buckets': r[1], 'objects': r[2], 
                 'size_bytes': r[3]} for r in result]
    
    def storage_class_summary(self) -> List[Dict]:
        """Get storage class distribution."""
        result = self.conn.execute("""
            SELECT storage_class, 
                   COUNT(DISTINCT bucket_name) as buckets,
                   SUM(num_objects) as objects,
                   SUM(size_bytes) as size_bytes
            FROM storage_class_usage
            GROUP BY storage_class
            ORDER BY size_bytes DESC
        """).fetchall()
        
        return [{'storage_class': r[0], 'buckets': r[1], 'objects': r[2],
                 'size_bytes': r[3]} for r in result]