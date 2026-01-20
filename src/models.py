"""
Data models for RGW bucket statistics.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Any, Optional


@dataclass
class BucketStats:
    """Container for bucket statistics."""
    bucket_name: str
    bucket_id: str = ""
    tenant: str = ""
    owner: str = ""
    zonegroup: str = ""
    placement_rule: str = ""
    num_shards: int = 0
    size_bytes: int = 0
    size_actual_bytes: int = 0
    num_objects: int = 0
    storage_classes: Dict[str, Any] = field(default_factory=dict)
    # Sync status fields
    sync_status: str = ""  # 'synced', 'behind', 'error', 'unknown'
    sync_behind_shards: int = 0
    sync_behind_entries: int = 0
    sync_source_zone: str = ""
    sync_last_updated: datetime = None
    # Collection metadata
    collected_at: datetime = field(default_factory=datetime.utcnow)
    collection_duration_ms: int = 0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'bucket_name': self.bucket_name,
            'bucket_id': self.bucket_id,
            'tenant': self.tenant,
            'owner': self.owner,
            'zonegroup': self.zonegroup,
            'placement_rule': self.placement_rule,
            'num_shards': self.num_shards,
            'size_bytes': self.size_bytes,
            'size_actual_bytes': self.size_actual_bytes,
            'num_objects': self.num_objects,
            'storage_classes': self.storage_classes,
            'sync_status': self.sync_status,
            'sync_behind_shards': self.sync_behind_shards,
            'sync_behind_entries': self.sync_behind_entries,
            'sync_source_zone': self.sync_source_zone,
            'collected_at': self.collected_at.isoformat(),
            'collection_duration_ms': self.collection_duration_ms
        }


@dataclass
class CollectorConfig:
    """Configuration for the collector."""
    db_path: str = "rgw_stats.duckdb"
    ceph_conf: str = "/etc/ceph/ceph.conf"
    
    # Collection settings
    refresh_interval_seconds: int = 300  # 5 minutes
    batch_size: int = 100
    parallel_workers: int = 4
    max_workers: int = 100  # Maximum allowed workers
    command_timeout: int = 60
    
    # For large clusters - auto-scale workers
    auto_scale_workers: bool = True
    buckets_per_worker: int = 50  # Target buckets per worker for auto-scaling
    
    # Stale threshold - default 30 min for large clusters
    stale_threshold_seconds: int = 1800  # 30 minutes default
    
    # Sync collection (multisite)
    collect_sync_status: bool = False
    
    # Bootstrap mode for cold start
    bootstrap_mode: bool = False
    bootstrap_workers: int = 50  # Aggressive parallelism for bootstrap
    
    def calculate_workers(self, bucket_count: int) -> int:
        """Calculate optimal worker count based on bucket count."""
        if self.bootstrap_mode:
            return min(self.bootstrap_workers, self.max_workers)
        
        if not self.auto_scale_workers:
            return self.parallel_workers
        
        # Auto-scale: aim for buckets_per_worker buckets per worker
        optimal = max(1, bucket_count // self.buckets_per_worker)
        return min(optimal, self.max_workers)
    
    def __post_init__(self):
        # Ensure reasonable bounds
        self.parallel_workers = min(self.parallel_workers, self.max_workers)
        self.bootstrap_workers = min(self.bootstrap_workers, self.max_workers)