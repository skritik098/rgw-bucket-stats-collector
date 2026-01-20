"""
Data models for RGW bucket statistics.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Any, Optional


@dataclass
class BucketStats:
    """Container for bucket statistics - mirrors radosgw-admin bucket stats output."""
    # Core identifiers
    bucket_name: str
    bucket_id: str = ""
    marker: str = ""
    tenant: str = ""
    owner: str = ""
    
    # Placement
    zonegroup: str = ""
    placement_rule: str = ""
    explicit_placement: Dict[str, str] = field(default_factory=dict)  # data_pool, data_extra_pool, index_pool
    
    # Sharding & indexing
    num_shards: int = 0
    index_type: str = "Normal"
    
    # Versioning & features
    versioning: str = "off"  # "off", "enabled", "suspended"
    versioned: bool = False
    versioning_enabled: bool = False
    object_lock_enabled: bool = False
    mfa_enabled: bool = False
    
    # Version markers
    ver: str = ""
    master_ver: str = ""
    max_marker: str = ""
    
    # Timestamps
    mtime: str = ""
    creation_time: str = ""
    
    # Usage stats (aggregated)
    size_bytes: int = 0
    size_actual_bytes: int = 0
    size_utilized_bytes: int = 0
    num_objects: int = 0
    
    # Detailed usage per storage class
    # Format: {"rgw.main": {"size": X, "size_actual": Y, ...}, ...}
    usage: Dict[str, Any] = field(default_factory=dict)
    
    # Quota
    bucket_quota: Dict[str, Any] = field(default_factory=dict)
    
    # Sync status fields (for multisite)
    sync_status: str = ""  # 'synced', 'behind', 'error', 'unknown'
    sync_behind_shards: int = 0
    sync_behind_entries: int = 0
    sync_source_zone: str = ""
    
    # Collection metadata (not from RGW, added by us)
    collected_at: datetime = field(default_factory=datetime.utcnow)
    collection_duration_ms: int = 0

    def to_rgw_json(self) -> Dict[str, Any]:
        """
        Convert to exact radosgw-admin bucket stats JSON format.
        This reconstructs the original output format.
        """
        return {
            "bucket": self.bucket_name,
            "num_shards": self.num_shards,
            "tenant": self.tenant,
            "versioning": self.versioning,
            "zonegroup": self.zonegroup,
            "placement_rule": self.placement_rule,
            "explicit_placement": self.explicit_placement or {
                "data_pool": "",
                "data_extra_pool": "",
                "index_pool": ""
            },
            "id": self.bucket_id,
            "marker": self.marker or self.bucket_id,
            "index_type": self.index_type,
            "versioned": self.versioned,
            "versioning_enabled": self.versioning_enabled,
            "object_lock_enabled": self.object_lock_enabled,
            "mfa_enabled": self.mfa_enabled,
            "owner": self.owner,
            "ver": self.ver,
            "master_ver": self.master_ver,
            "mtime": self.mtime,
            "creation_time": self.creation_time,
            "max_marker": self.max_marker,
            "usage": self.usage,
            "bucket_quota": self.bucket_quota or {
                "enabled": False,
                "check_on_raw": False,
                "max_size": -1,
                "max_size_kb": 0,
                "max_objects": -1
            }
        }

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary (internal format with all fields)."""
        return {
            'bucket_name': self.bucket_name,
            'bucket_id': self.bucket_id,
            'marker': self.marker,
            'tenant': self.tenant,
            'owner': self.owner,
            'zonegroup': self.zonegroup,
            'placement_rule': self.placement_rule,
            'explicit_placement': self.explicit_placement,
            'num_shards': self.num_shards,
            'index_type': self.index_type,
            'versioning': self.versioning,
            'versioned': self.versioned,
            'versioning_enabled': self.versioning_enabled,
            'object_lock_enabled': self.object_lock_enabled,
            'mfa_enabled': self.mfa_enabled,
            'ver': self.ver,
            'master_ver': self.master_ver,
            'max_marker': self.max_marker,
            'mtime': self.mtime,
            'creation_time': self.creation_time,
            'size_bytes': self.size_bytes,
            'size_actual_bytes': self.size_actual_bytes,
            'size_utilized_bytes': self.size_utilized_bytes,
            'num_objects': self.num_objects,
            'usage': self.usage,
            'bucket_quota': self.bucket_quota,
            'sync_status': self.sync_status,
            'sync_behind_shards': self.sync_behind_shards,
            'sync_behind_entries': self.sync_behind_entries,
            'sync_source_zone': self.sync_source_zone,
            'collected_at': self.collected_at.isoformat() if self.collected_at else None,
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