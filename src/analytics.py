"""
Analytics module for historical data analysis, growth tracking, and comparisons.
"""

from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional


class Analytics:
    """Analytics and historical data analysis for bucket statistics."""
    
    def __init__(self, db_path: str):
        import duckdb
        self.conn = duckdb.connect(db_path, read_only=True)
    
    def close(self):
        self.conn.close()
    
    # =========================================================================
    # SUMMARY
    # =========================================================================
    
    def cluster_summary(self) -> Dict[str, Any]:
        """Get overall cluster summary."""
        r = self.conn.execute("""
            SELECT COUNT(*), COUNT(DISTINCT owner), COALESCE(SUM(num_objects), 0),
                   COALESCE(SUM(size_bytes), 0), MIN(collected_at), MAX(collected_at)
            FROM bucket_stats
        """).fetchone()
        return {'total_buckets': r[0], 'total_owners': r[1], 'total_objects': r[2],
                'total_size_bytes': r[3], 'oldest_data': r[4], 'newest_data': r[5]}
    
    def top_buckets_by_size(self, limit: int = 20) -> List[Dict]:
        """Get largest buckets."""
        rows = self.conn.execute(f"""
            SELECT bucket_name, owner, num_objects, size_bytes, collected_at
            FROM bucket_stats ORDER BY size_bytes DESC LIMIT {limit}
        """).fetchall()
        return [{'bucket_name': r[0], 'owner': r[1], 'num_objects': r[2],
                 'size_bytes': r[3], 'collected_at': r[4]} for r in rows]
    
    def top_buckets_by_objects(self, limit: int = 20) -> List[Dict]:
        """Get buckets with most objects."""
        rows = self.conn.execute(f"""
            SELECT bucket_name, owner, num_objects, size_bytes
            FROM bucket_stats ORDER BY num_objects DESC LIMIT {limit}
        """).fetchall()
        return [{'bucket_name': r[0], 'owner': r[1], 'num_objects': r[2],
                 'size_bytes': r[3]} for r in rows]
    
    def summary_by_owner(self, limit: int = 50) -> List[Dict]:
        """Summary grouped by owner."""
        rows = self.conn.execute(f"""
            SELECT owner, COUNT(*) as buckets, SUM(num_objects), SUM(size_bytes)
            FROM bucket_stats GROUP BY owner ORDER BY SUM(size_bytes) DESC LIMIT {limit}
        """).fetchall()
        return [{'owner': r[0], 'bucket_count': r[1], 'total_objects': r[2],
                 'total_size': r[3]} for r in rows]
    
    def storage_class_distribution(self) -> List[Dict]:
        """Storage class distribution."""
        rows = self.conn.execute("""
            SELECT storage_class, COUNT(DISTINCT bucket_name), SUM(num_objects), SUM(size_bytes)
            FROM storage_class_usage GROUP BY storage_class ORDER BY SUM(size_bytes) DESC
        """).fetchall()
        return [{'storage_class': r[0], 'bucket_count': r[1], 
                 'total_objects': r[2], 'total_size': r[3]} for r in rows]
    
    # =========================================================================
    # HISTORICAL GROWTH
    # =========================================================================
    
    def bucket_history(self, bucket_name: str, days: int = 30) -> List[Dict]:
        """Historical data for a specific bucket."""
        since = datetime.utcnow() - timedelta(days=days)
        rows = self.conn.execute("""
            SELECT collected_at, size_bytes, num_objects FROM bucket_stats_history
            WHERE bucket_name = ? AND collected_at >= ? ORDER BY collected_at
        """, [bucket_name, since]).fetchall()
        return [{'collected_at': r[0], 'size_bytes': r[1], 'num_objects': r[2]} for r in rows]
    
    def cluster_growth(self, days: int = 30) -> List[Dict]:
        """Cluster-wide daily growth."""
        since = datetime.utcnow() - timedelta(days=days)
        rows = self.conn.execute("""
            SELECT date_trunc('day', collected_at) as day, SUM(size_bytes), SUM(num_objects)
            FROM bucket_stats_history WHERE collected_at >= ?
            GROUP BY day ORDER BY day
        """, [since]).fetchall()
        return [{'day': r[0], 'total_size': r[1], 'total_objects': r[2]} for r in rows]
    
    def fastest_growing_buckets(self, days: int = 7, limit: int = 20) -> List[Dict]:
        """Buckets with highest growth rate."""
        since = datetime.utcnow() - timedelta(days=days)
        rows = self.conn.execute(f"""
            WITH growth AS (
                SELECT bucket_name, MIN(size_bytes) as start_size, MAX(size_bytes) as end_size,
                       MAX(size_bytes) - MIN(size_bytes) as growth_bytes
                FROM bucket_stats_history WHERE collected_at >= ?
                GROUP BY bucket_name HAVING COUNT(*) >= 2
            )
            SELECT g.bucket_name, b.owner, g.start_size, g.end_size, g.growth_bytes
            FROM growth g JOIN bucket_stats b ON g.bucket_name = b.bucket_name
            ORDER BY g.growth_bytes DESC LIMIT {limit}
        """, [since]).fetchall()
        return [{'bucket_name': r[0], 'owner': r[1], 'start_size': r[2],
                 'end_size': r[3], 'growth_bytes': r[4]} for r in rows]
    
    def fastest_growing_owners(self, days: int = 7, limit: int = 20) -> List[Dict]:
        """Owners with highest growth rate."""
        since = datetime.utcnow() - timedelta(days=days)
        rows = self.conn.execute(f"""
            WITH owner_daily AS (
                SELECT b.owner, date_trunc('day', h.collected_at) as day, SUM(h.size_bytes) as size
                FROM bucket_stats_history h JOIN bucket_stats b ON h.bucket_name = b.bucket_name
                WHERE h.collected_at >= ? GROUP BY b.owner, day
            ),
            growth AS (
                SELECT owner, MIN(size) as start_size, MAX(size) as end_size
                FROM owner_daily GROUP BY owner
            )
            SELECT owner, start_size, end_size, end_size - start_size as growth
            FROM growth ORDER BY growth DESC LIMIT {limit}
        """, [since]).fetchall()
        return [{'owner': r[0], 'start_size': r[1], 'end_size': r[2],
                 'growth_bytes': r[3]} for r in rows]
    
    # =========================================================================
    # COMPARISON
    # =========================================================================
    
    def compare_snapshots(self, hours_ago: int = 24) -> Dict[str, Any]:
        """Compare current vs historical state."""
        cutoff = datetime.utcnow() - timedelta(hours=hours_ago)
        
        current = self.conn.execute("""
            SELECT SUM(size_bytes), SUM(num_objects), COUNT(*) FROM bucket_stats
        """).fetchone()
        
        historical = self.conn.execute("""
            SELECT SUM(size_bytes), SUM(num_objects), COUNT(DISTINCT bucket_name)
            FROM bucket_stats_history
            WHERE collected_at <= ? AND collected_at >= ? - INTERVAL '1 hour'
        """, [cutoff, cutoff]).fetchone()
        
        return {
            'current': {'size_bytes': current[0] or 0, 'objects': current[1] or 0, 'buckets': current[2]},
            'historical': {'size_bytes': historical[0] or 0, 'objects': historical[1] or 0, 'buckets': historical[2] or 0},
            'delta': {
                'size_bytes': (current[0] or 0) - (historical[0] or 0),
                'objects': (current[1] or 0) - (historical[1] or 0)
            },
            'period_hours': hours_ago
        }
    
    def bucket_changes(self, hours: int = 24, min_change_pct: float = 10.0) -> List[Dict]:
        """Buckets with significant changes."""
        since = datetime.utcnow() - timedelta(hours=hours)
        rows = self.conn.execute(f"""
            WITH changes AS (
                SELECT bucket_name,
                       FIRST(size_bytes ORDER BY collected_at) as old_size,
                       LAST(size_bytes ORDER BY collected_at) as new_size
                FROM bucket_stats_history WHERE collected_at >= ?
                GROUP BY bucket_name HAVING COUNT(*) >= 2
            )
            SELECT c.bucket_name, b.owner, c.old_size, c.new_size, c.new_size - c.old_size,
                   CASE WHEN c.old_size > 0 THEN ((c.new_size - c.old_size) * 100.0 / c.old_size) ELSE 0 END
            FROM changes c JOIN bucket_stats b ON c.bucket_name = b.bucket_name
            WHERE ABS(CASE WHEN c.old_size > 0 THEN ((c.new_size - c.old_size) * 100.0 / c.old_size) ELSE 0 END) >= {min_change_pct}
            ORDER BY ABS(c.new_size - c.old_size) DESC
        """, [since]).fetchall()
        return [{'bucket_name': r[0], 'owner': r[1], 'old_size': r[2], 'new_size': r[3],
                 'delta': r[4], 'change_pct': r[5]} for r in rows]
    
    # =========================================================================
    # BUCKET DETAILS & SYNC
    # =========================================================================
    
    def bucket_details(self, bucket_name: str) -> Optional[Dict]:
        """Detailed info for a bucket."""
        r = self.conn.execute("""
            SELECT bucket_name, bucket_id, tenant, owner, zonegroup, placement_rule,
                   num_shards, size_bytes, size_actual_bytes, num_objects, storage_classes,
                   sync_status, sync_behind_shards, collected_at
            FROM bucket_stats WHERE bucket_name = ?
        """, [bucket_name]).fetchone()
        if not r:
            return None
        return {'bucket_name': r[0], 'bucket_id': r[1], 'tenant': r[2], 'owner': r[3],
                'zonegroup': r[4], 'placement_rule': r[5], 'num_shards': r[6],
                'size_bytes': r[7], 'size_actual_bytes': r[8], 'num_objects': r[9],
                'storage_classes': r[10], 'sync_status': r[11], 
                'sync_behind_shards': r[12], 'collected_at': r[13]}
    
    def sync_status_summary(self) -> List[Dict]:
        """Sync status summary for multisite."""
        rows = self.conn.execute("""
            SELECT sync_status, COUNT(*), SUM(sync_behind_shards)
            FROM bucket_stats WHERE sync_status IS NOT NULL AND sync_status != ''
            GROUP BY sync_status ORDER BY COUNT(*) DESC
        """).fetchall()
        return [{'sync_status': r[0], 'bucket_count': r[1], 'shards_behind': r[2]} for r in rows]
    
    def buckets_behind_sync(self, limit: int = 50) -> List[Dict]:
        """Buckets behind in sync."""
        rows = self.conn.execute(f"""
            SELECT bucket_name, owner, sync_status, sync_behind_shards, size_bytes
            FROM bucket_stats WHERE sync_behind_shards > 0
            ORDER BY sync_behind_shards DESC LIMIT {limit}
        """).fetchall()
        return [{'bucket_name': r[0], 'owner': r[1], 'sync_status': r[2],
                 'shards_behind': r[3], 'size_bytes': r[4]} for r in rows]
    
    # =========================================================================
    # DATA FRESHNESS
    # =========================================================================
    
    def collection_freshness(self) -> Dict[str, Any]:
        """Data freshness statistics."""
        r = self.conn.execute("""
            SELECT COUNT(*),
                   SUM(CASE WHEN collected_at >= CURRENT_TIMESTAMP - INTERVAL '5 minutes' THEN 1 ELSE 0 END),
                   SUM(CASE WHEN collected_at >= CURRENT_TIMESTAMP - INTERVAL '10 minutes' THEN 1 ELSE 0 END),
                   SUM(CASE WHEN collected_at >= CURRENT_TIMESTAMP - INTERVAL '1 hour' THEN 1 ELSE 0 END),
                   MIN(collected_at), MAX(collected_at)
            FROM bucket_stats
        """).fetchone()
        total = r[0] or 1
        return {'total': r[0], 'fresh_5m': r[1], 'fresh_5m_pct': (r[1] or 0)*100/total,
                'fresh_10m': r[2], 'fresh_10m_pct': (r[2] or 0)*100/total,
                'fresh_1h': r[3], 'fresh_1h_pct': (r[3] or 0)*100/total,
                'oldest': r[4], 'newest': r[5]}
    
    def stale_buckets(self, minutes: int = 10, limit: int = 100) -> List[Dict]:
        """Buckets with stale data."""
        threshold = datetime.utcnow() - timedelta(minutes=minutes)
        rows = self.conn.execute(f"""
            SELECT bucket_name, owner, collected_at FROM bucket_stats
            WHERE collected_at < ? ORDER BY collected_at LIMIT {limit}
        """, [threshold]).fetchall()
        return [{'bucket_name': r[0], 'owner': r[1], 'collected_at': r[2]} for r in rows]
    
    # =========================================================================
    # PER-BUCKET HISTORICAL COMPARISON (TABULAR)
    # =========================================================================
    
    def bucket_comparison_table(self, days: int = 7, limit: int = 50) -> List[Dict]:
        """
        Per-bucket comparison showing current vs historical in tabular format.
        Shows: bucket, owner, current_size, size_7d_ago, delta, pct_change
        """
        since = datetime.utcnow() - timedelta(days=days)
        
        rows = self.conn.execute(f"""
            WITH current AS (
                SELECT bucket_name, owner, size_bytes, num_objects, collected_at
                FROM bucket_stats
            ),
            historical AS (
                SELECT bucket_name,
                       FIRST(size_bytes ORDER BY collected_at) as old_size,
                       FIRST(num_objects ORDER BY collected_at) as old_objects,
                       MIN(collected_at) as old_time
                FROM bucket_stats_history
                WHERE collected_at >= ? AND collected_at <= ? + INTERVAL '1 day'
                GROUP BY bucket_name
            )
            SELECT 
                c.bucket_name,
                c.owner,
                c.size_bytes as current_size,
                c.num_objects as current_objects,
                COALESCE(h.old_size, c.size_bytes) as old_size,
                COALESCE(h.old_objects, c.num_objects) as old_objects,
                c.size_bytes - COALESCE(h.old_size, c.size_bytes) as size_delta,
                c.num_objects - COALESCE(h.old_objects, c.num_objects) as obj_delta,
                CASE WHEN COALESCE(h.old_size, 0) > 0 
                     THEN ((c.size_bytes - h.old_size) * 100.0 / h.old_size)
                     ELSE 0 END as size_pct_change,
                c.collected_at as current_time,
                h.old_time
            FROM current c
            LEFT JOIN historical h ON c.bucket_name = h.bucket_name
            ORDER BY ABS(c.size_bytes - COALESCE(h.old_size, c.size_bytes)) DESC
            LIMIT {limit}
        """, [since, since]).fetchall()
        
        return [{
            'bucket_name': r[0], 'owner': r[1],
            'current_size': r[2], 'current_objects': r[3],
            'old_size': r[4], 'old_objects': r[5],
            'size_delta': r[6], 'obj_delta': r[7],
            'size_pct_change': r[8],
            'current_time': r[9], 'old_time': r[10]
        } for r in rows]
    
    def sync_comparison_table(self, limit: int = 50) -> List[Dict]:
        """
        Per-bucket sync status comparison showing current vs historical.
        """
        rows = self.conn.execute(f"""
            WITH current AS (
                SELECT bucket_name, owner, sync_status, sync_behind_shards, 
                       sync_behind_entries, size_bytes, collected_at
                FROM bucket_stats
                WHERE sync_status IS NOT NULL AND sync_status != ''
            ),
            historical AS (
                SELECT bucket_name,
                       AVG(sync_behind_shards) as avg_shards_behind,
                       AVG(sync_behind_entries) as avg_entries_behind,
                       MAX(sync_behind_shards) as max_shards_behind
                FROM bucket_stats_history
                WHERE collected_at >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
                GROUP BY bucket_name
            )
            SELECT 
                c.bucket_name, c.owner, c.sync_status,
                c.sync_behind_shards, c.sync_behind_entries,
                c.size_bytes,
                COALESCE(h.avg_shards_behind, 0) as avg_shards_24h,
                COALESCE(h.max_shards_behind, 0) as max_shards_24h,
                c.collected_at
            FROM current c
            LEFT JOIN historical h ON c.bucket_name = h.bucket_name
            ORDER BY c.sync_behind_shards DESC
            LIMIT {limit}
        """).fetchall()
        
        return [{
            'bucket_name': r[0], 'owner': r[1], 'sync_status': r[2],
            'shards_behind': r[3], 'entries_behind': r[4], 'size_bytes': r[5],
            'avg_shards_24h': r[6], 'max_shards_24h': r[7], 'collected_at': r[8]
        } for r in rows]
    
    def all_buckets_summary_table(self, sort_by: str = 'size', limit: int = 100) -> List[Dict]:
        """
        All buckets summary table with current stats and age.
        sort_by: 'size', 'objects', 'age', 'name'
        """
        order_map = {
            'size': 'size_bytes DESC',
            'objects': 'num_objects DESC',
            'age': 'collected_at ASC',
            'name': 'bucket_name ASC'
        }
        order = order_map.get(sort_by, 'size_bytes DESC')
        
        rows = self.conn.execute(f"""
            SELECT bucket_name, owner, num_objects, size_bytes, 
                   sync_status, sync_behind_shards, collected_at,
                   EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - collected_at)) as age_seconds
            FROM bucket_stats
            ORDER BY {order}
            LIMIT {limit}
        """).fetchall()
        
        return [{
            'bucket_name': r[0], 'owner': r[1], 'num_objects': r[2],
            'size_bytes': r[3], 'sync_status': r[4], 'shards_behind': r[5],
            'collected_at': r[6], 'age_seconds': r[7]
        } for r in rows]
    
    # =========================================================================
    # CAPACITY PLANNING
    # =========================================================================
    
    def capacity_forecast(self, history_days: int = 30, forecast_days: int = 90) -> Dict[str, Any]:
        """Simple linear capacity forecast."""
        since = datetime.utcnow() - timedelta(days=history_days)
        rows = self.conn.execute("""
            SELECT date_trunc('day', collected_at) as day, SUM(size_bytes)
            FROM bucket_stats_history WHERE collected_at >= ?
            GROUP BY day ORDER BY day
        """, [since]).fetchall()
        
        if len(rows) < 2:
            return {'error': 'Insufficient data for forecast'}
        
        # Linear regression
        n = len(rows)
        x = list(range(n))
        y = [r[1] or 0 for r in rows]
        
        sum_x, sum_y = sum(x), sum(y)
        sum_xy = sum(i*v for i, v in zip(x, y))
        sum_x2 = sum(i*i for i in x)
        
        slope = (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x * sum_x)
        
        current = y[-1]
        forecast = current + slope * forecast_days
        
        return {
            'current_bytes': current,
            'forecast_bytes': max(0, forecast),
            'forecast_days': forecast_days,
            'daily_growth_bytes': slope,
            'monthly_growth_bytes': slope * 30,
            'data_points': n
        }
    
    # =========================================================================
    # CUSTOM QUERY
    # =========================================================================
    
    def custom_query(self, sql: str) -> List[Dict]:
        """Execute custom SQL."""
        result = self.conn.execute(sql)
        cols = [d[0] for d in result.description]
        return [dict(zip(cols, row)) for row in result.fetchall()]
    
    # =========================================================================
    # EXPORT - Reconstruct radosgw-admin bucket stats format from DB
    # =========================================================================
    
    def export_bucket_stats(self, bucket_name: str) -> Optional[Dict[str, Any]]:
        """
        Export a single bucket's stats in exact radosgw-admin format.
        
        This reconstructs the original JSON format from stored DB data.
        
        Args:
            bucket_name: Name of the bucket
        
        Returns:
            Dict matching radosgw-admin bucket stats output, or None if not found
        """
        import json
        
        row = self.conn.execute("""
            SELECT bucket_name, bucket_id, marker, tenant, owner, zonegroup,
                   placement_rule, explicit_placement, num_shards, index_type,
                   versioning, versioned, versioning_enabled, object_lock_enabled,
                   mfa_enabled, ver, master_ver, max_marker, mtime, creation_time,
                   usage, bucket_quota
            FROM bucket_stats WHERE bucket_name = ?
        """, [bucket_name]).fetchone()
        
        if not row:
            return None
        
        # Parse JSON fields
        explicit_placement = json.loads(row[7]) if row[7] else {"data_pool": "", "data_extra_pool": "", "index_pool": ""}
        usage = json.loads(row[20]) if row[20] else {}
        bucket_quota = json.loads(row[21]) if row[21] else {
            "enabled": False, "check_on_raw": False,
            "max_size": -1, "max_size_kb": 0, "max_objects": -1
        }
        
        # Reconstruct exact radosgw-admin format
        return {
            "bucket": row[0],
            "num_shards": row[8],
            "tenant": row[3] or "",
            "versioning": row[10] or "off",
            "zonegroup": row[5] or "",
            "placement_rule": row[6] or "",
            "explicit_placement": explicit_placement,
            "id": row[1] or "",
            "marker": row[2] or row[1] or "",
            "index_type": row[9] or "Normal",
            "versioned": row[11] or False,
            "versioning_enabled": row[12] or False,
            "object_lock_enabled": row[13] or False,
            "mfa_enabled": row[14] or False,
            "owner": row[4] or "",
            "ver": row[15] or "",
            "master_ver": row[16] or "",
            "mtime": row[18] or "",
            "creation_time": row[19] or "",
            "max_marker": row[17] or "",
            "usage": usage,
            "bucket_quota": bucket_quota
        }
    
    def export_all_bucket_stats(self, limit: int = None) -> List[Dict[str, Any]]:
        """
        Export all buckets' stats in exact radosgw-admin format.
        
        This matches the output of: radosgw-admin bucket stats (without --bucket)
        
        Args:
            limit: Optional limit on number of buckets
        
        Returns:
            List of dicts, each matching radosgw-admin bucket stats format
        """
        import json
        
        query = """
            SELECT bucket_name, bucket_id, marker, tenant, owner, zonegroup,
                   placement_rule, explicit_placement, num_shards, index_type,
                   versioning, versioned, versioning_enabled, object_lock_enabled,
                   mfa_enabled, ver, master_ver, max_marker, mtime, creation_time,
                   usage, bucket_quota
            FROM bucket_stats
            ORDER BY bucket_name
        """
        if limit:
            query += f" LIMIT {limit}"
        
        rows = self.conn.execute(query).fetchall()
        results = []
        
        for row in rows:
            explicit_placement = json.loads(row[7]) if row[7] else {"data_pool": "", "data_extra_pool": "", "index_pool": ""}
            usage = json.loads(row[20]) if row[20] else {}
            bucket_quota = json.loads(row[21]) if row[21] else {
                "enabled": False, "check_on_raw": False,
                "max_size": -1, "max_size_kb": 0, "max_objects": -1
            }
            
            results.append({
                "bucket": row[0],
                "num_shards": row[8],
                "tenant": row[3] or "",
                "versioning": row[10] or "off",
                "zonegroup": row[5] or "",
                "placement_rule": row[6] or "",
                "explicit_placement": explicit_placement,
                "id": row[1] or "",
                "marker": row[2] or row[1] or "",
                "index_type": row[9] or "Normal",
                "versioned": row[11] or False,
                "versioning_enabled": row[12] or False,
                "object_lock_enabled": row[13] or False,
                "mfa_enabled": row[14] or False,
                "owner": row[4] or "",
                "ver": row[15] or "",
                "master_ver": row[16] or "",
                "mtime": row[18] or "",
                "creation_time": row[19] or "",
                "max_marker": row[17] or "",
                "usage": usage,
                "bucket_quota": bucket_quota
            })
        
        return results
    
    def export_bucket_stats_json(self, bucket_name: str = None, 
                                  indent: int = 4) -> str:
        """
        Export bucket stats as JSON string (exact radosgw-admin format).
        
        Args:
            bucket_name: Specific bucket (None = all buckets)
            indent: JSON indentation (default 4)
        
        Returns:
            JSON string matching radosgw-admin output
        """
        import json
        
        if bucket_name:
            data = self.export_bucket_stats(bucket_name)
        else:
            data = self.export_all_bucket_stats()
        
        if data is None:
            return "{}"
        
        return json.dumps(data, indent=indent)