"""
RGW Admin client interface.
"""

import json
import subprocess
import time
import re
from typing import List, Tuple, Optional, Dict, Any

from .models import BucketStats


class RGWAdminClient:
    """Interface to radosgw-admin CLI."""
    
    def __init__(self, ceph_conf: str = "/etc/ceph/ceph.conf", timeout: int = 60):
        self.ceph_conf = ceph_conf
        self.timeout = timeout
        self.rgw_admin = "radosgw-admin"
    
    def _run_command(self, cmd: List[str], timeout: int = None) -> Tuple[bool, str, str]:
        """
        Run radosgw-admin command.
        Returns: (success, stdout, stderr)
        """
        full_cmd = [self.rgw_admin, "-c", self.ceph_conf] + cmd + ["--format=json"]
        use_timeout = timeout or self.timeout
        
        try:
            result = subprocess.run(
                full_cmd,
                capture_output=True,
                text=True,
                timeout=use_timeout
            )
            return result.returncode == 0, result.stdout, result.stderr
        except subprocess.TimeoutExpired:
            return False, "", f"Command timed out after {use_timeout}s"
        except Exception as e:
            return False, "", str(e)
    
    def get_all_buckets(self) -> List[str]:
        """
        Get all bucket names.
        Note: radosgw-admin bucket list returns all buckets at once (no pagination).
        """
        # Use longer timeout for bucket listing (can be slow with many buckets)
        success, stdout, stderr = self._run_command(
            ["bucket", "list"], 
            timeout=max(self.timeout, 600)  # At least 10 minutes
        )
        
        if not success:
            print(f"ERROR: Failed to list buckets: {stderr}")
            return []
        
        if not stdout or not stdout.strip():
            return []
        
        try:
            data = json.loads(stdout)
            
            # Must be a list
            if not isinstance(data, list):
                print(f"ERROR: Unexpected bucket list format: {type(data)}")
                return []
            
            # Extract bucket names - each item should be a string
            buckets = []
            for item in data:
                if isinstance(item, str):
                    buckets.append(item)
                elif isinstance(item, dict):
                    # Some versions return dicts with 'bucket' key
                    name = item.get('bucket') or item.get('name', '')
                    if name:
                        buckets.append(name)
            
            return buckets
            
        except json.JSONDecodeError as e:
            print(f"ERROR: Failed to parse bucket list JSON: {e}")
            return []
    
    def get_all_bucket_stats_bulk(self, timeout: int = 1800, log_callback=None) -> List[BucketStats]:
        """
        Get stats for ALL buckets in a single command.
        
        This is MUCH faster than per-bucket commands:
        - Single process spawn
        - Single RADOS authentication  
        - Single metadata scan
        
        For 28K buckets: ~3 minutes vs ~30 minutes with per-bucket calls.
        
        Note: radosgw-admin outputs all data at once (not streaming),
        so we wait for complete output then parse.
        
        Args:
            timeout: Command timeout (default 30 min for large clusters)
            log_callback: Optional function(message) for progress logging
        
        Returns:
            List of BucketStats for all buckets
        """
        import time as time_module
        
        def log(msg):
            if log_callback:
                log_callback(msg)
        
        cmd = [self.rgw_admin, "-c", self.ceph_conf, "bucket", "stats", "--format=json"]
        
        log(f"  [BULK] Executing: radosgw-admin bucket stats")
        log(f"  [BULK] Waiting for response (typically 2-5 minutes for large clusters)...")
        
        start_time = time_module.time()
        
        try:
            # Simple subprocess.run - wait for complete output
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout
            )
            
            fetch_time = time_module.time() - start_time
            log(f"  [BULK] Command completed in {fetch_time:.1f}s")
            
            if result.returncode != 0:
                log(f"  [BULK] ERROR: Command failed: {result.stderr}")
                return []
            
            if not result.stdout or not result.stdout.strip():
                log(f"  [BULK] WARNING: Empty response")
                return []
            
            # Parse JSON
            log(f"  [BULK] Parsing JSON response...")
            parse_start = time_module.time()
            
            data = json.loads(result.stdout)
            
            if not isinstance(data, list):
                data = [data]
            
            parse_time = time_module.time() - parse_start
            log(f"  [BULK] Parsed {len(data)} buckets in {parse_time:.1f}s")
            
            # Convert to BucketStats objects
            log(f"  [BULK] Converting to BucketStats objects...")
            results = []
            
            for i, raw in enumerate(data):
                if isinstance(raw, dict) and raw.get('bucket'):
                    try:
                        stats = self._parse_stats(raw, 0)
                        results.append(stats)
                    except Exception as e:
                        log(f"  [BULK] ERROR parsing bucket {raw.get('bucket', 'unknown')}: {e}")
                
                # Progress every 5000 buckets
                if (i + 1) % 5000 == 0:
                    log(f"  [BULK] Processed {i + 1}/{len(data)} buckets...")
            
            total_time = time_module.time() - start_time
            log(f"  [BULK] Complete: {len(results)} buckets in {total_time:.1f}s")
            
            return results
            
        except subprocess.TimeoutExpired:
            elapsed = time_module.time() - start_time
            log(f"  [BULK] ERROR: Command timed out after {elapsed:.0f}s (limit: {timeout}s)")
            return []
        except json.JSONDecodeError as e:
            log(f"  [BULK] ERROR: Failed to parse JSON: {e}")
            return []
        except Exception as e:
            log(f"  [BULK] ERROR: {e}")
            return []
    
    def get_bucket_stats(self, bucket_name: str, tenant: str = "") -> Optional[BucketStats]:
        """Get statistics for a single bucket."""
        start_time = time.time()
        
        cmd = ["bucket", "stats", "--bucket", bucket_name]
        if tenant:
            cmd.extend(["--tenant", tenant])
        
        success, stdout, stderr = self._run_command(cmd)
        
        if not success:
            return None
        
        try:
            raw = json.loads(stdout) if stdout.strip() else None
            if not raw:
                return None
            
            return self._parse_stats(raw, time.time() - start_time)
            
        except json.JSONDecodeError:
            return None
    
    def get_bucket_sync_status(self, bucket_name: str) -> Dict[str, Any]:
        """
        Get sync status for a bucket (multisite).
        Returns dict with sync_status, behind_shards, behind_entries, source_zone
        """
        cmd = ["bucket", "sync", "status", "--bucket", bucket_name]
        success, stdout, stderr = self._run_command(cmd, timeout=30)
        
        result = {
            'sync_status': 'unknown',
            'behind_shards': 0,
            'behind_entries': 0,
            'source_zone': ''
        }
        
        if not success:
            if 'not in a multisite' in stderr.lower() or 'no sync' in stderr.lower():
                result['sync_status'] = 'not_multisite'
            return result
        
        try:
            # Try JSON parse first
            if stdout.strip().startswith('{') or stdout.strip().startswith('['):
                data = json.loads(stdout)
                # Parse JSON format sync status
                return self._parse_sync_json(data)
            else:
                # Parse text format
                return self._parse_sync_text(stdout)
        except:
            return result
    
    def _parse_sync_json(self, data: Any) -> Dict[str, Any]:
        """Parse JSON format sync status."""
        result = {
            'sync_status': 'synced',
            'behind_shards': 0,
            'behind_entries': 0,
            'source_zone': ''
        }
        
        if isinstance(data, list):
            for zone_info in data:
                if isinstance(zone_info, dict):
                    source = zone_info.get('source_zone', '')
                    if source:
                        result['source_zone'] = source
                    
                    shards = zone_info.get('shards_behind', 0) or 0
                    entries = zone_info.get('entries_behind', 0) or 0
                    result['behind_shards'] += shards
                    result['behind_entries'] += entries
        
        if result['behind_shards'] > 0 or result['behind_entries'] > 0:
            result['sync_status'] = 'behind'
        
        return result
    
    def _parse_sync_text(self, text: str) -> Dict[str, Any]:
        """Parse text format sync status."""
        result = {
            'sync_status': 'synced',
            'behind_shards': 0,
            'behind_entries': 0,
            'source_zone': ''
        }
        
        # Look for "behind" indicators
        behind_match = re.search(r'(\d+)\s+shards?\s+behind', text, re.IGNORECASE)
        if behind_match:
            result['behind_shards'] = int(behind_match.group(1))
            result['sync_status'] = 'behind'
        
        entries_match = re.search(r'(\d+)\s+entries?\s+behind', text, re.IGNORECASE)
        if entries_match:
            result['behind_entries'] = int(entries_match.group(1))
            result['sync_status'] = 'behind'
        
        # Look for source zone
        zone_match = re.search(r'source\s+zone[:\s]+(\S+)', text, re.IGNORECASE)
        if zone_match:
            result['source_zone'] = zone_match.group(1)
        
        # Check for sync errors
        if 'error' in text.lower():
            result['sync_status'] = 'error'
        elif 'caught up' in text.lower() or 'in sync' in text.lower():
            result['sync_status'] = 'synced'
        
        return result
    
    def get_bucket_stats_with_sync(self, bucket_name: str, tenant: str = "",
                                    collect_sync: bool = False) -> Optional[BucketStats]:
        """Get bucket stats optionally with sync status."""
        stats = self.get_bucket_stats(bucket_name, tenant)
        
        if stats and collect_sync:
            sync_info = self.get_bucket_sync_status(bucket_name)
            stats.sync_status = sync_info['sync_status']
            stats.sync_behind_shards = sync_info['behind_shards']
            stats.sync_behind_entries = sync_info['behind_entries']
            stats.sync_source_zone = sync_info['source_zone']
        
        return stats
    
    def _parse_stats(self, raw: Dict[str, Any], duration_seconds: float) -> BucketStats:
        """Parse raw JSON into BucketStats - captures ALL fields."""
        from datetime import datetime
        
        usage = raw.get('usage', {})
        
        # Calculate totals from usage
        total_size = 0
        total_size_actual = 0
        total_size_utilized = 0
        total_objects = 0
        
        for key, value in usage.items():
            if isinstance(value, dict):
                total_size += value.get('size', 0) or 0
                total_size_actual += value.get('size_actual', 0) or 0
                total_size_utilized += value.get('size_utilized', 0) or 0
                total_objects += value.get('num_objects', 0) or 0
        
        return BucketStats(
            # Basic identifiers
            bucket_name=raw.get('bucket', ''),
            bucket_id=raw.get('id', ''),
            marker=raw.get('marker', ''),
            tenant=raw.get('tenant', ''),
            owner=raw.get('owner', ''),
            
            # Placement
            zonegroup=raw.get('zonegroup', ''),
            placement_rule=raw.get('placement_rule', ''),
            explicit_placement=raw.get('explicit_placement', {}),
            
            # Sharding & indexing
            num_shards=raw.get('num_shards', 0) or 0,
            index_type=raw.get('index_type', 'Normal'),
            
            # Versioning & features
            versioning=raw.get('versioning', 'off'),
            versioned=raw.get('versioned', False),
            versioning_enabled=raw.get('versioning_enabled', False),
            object_lock_enabled=raw.get('object_lock_enabled', False),
            mfa_enabled=raw.get('mfa_enabled', False),
            
            # Version markers
            ver=raw.get('ver', ''),
            master_ver=raw.get('master_ver', ''),
            max_marker=raw.get('max_marker', ''),
            
            # Timestamps
            mtime=raw.get('mtime', ''),
            creation_time=raw.get('creation_time', ''),
            
            # Usage totals
            size_bytes=total_size,
            size_actual_bytes=total_size_actual,
            size_utilized_bytes=total_size_utilized,
            num_objects=total_objects,
            
            # Full usage data
            usage=usage,
            
            # Quota
            bucket_quota=raw.get('bucket_quota', {}),
            
            # Collection metadata - EXPLICITLY set timestamp
            collected_at=datetime.utcnow(),
            collection_duration_ms=int(duration_seconds * 1000)
        )