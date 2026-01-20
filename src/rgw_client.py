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
        """Parse raw JSON into BucketStats."""
        usage = raw.get('usage', {})
        
        # Extract storage classes and calculate totals
        storage_classes = {}
        total_size = 0
        total_size_actual = 0
        total_objects = 0
        
        for key, value in usage.items():
            if isinstance(value, dict):
                size = value.get('size', 0) or 0
                size_actual = value.get('size_actual', 0) or 0
                num_objects = value.get('num_objects', 0) or 0
                
                storage_classes[key] = {
                    'size': size,
                    'size_actual': size_actual,
                    'num_objects': num_objects
                }
                
                total_size += size
                total_size_actual += size_actual
                total_objects += num_objects
        
        return BucketStats(
            bucket_name=raw.get('bucket', ''),
            bucket_id=raw.get('id', ''),
            tenant=raw.get('tenant', ''),
            owner=raw.get('owner', ''),
            zonegroup=raw.get('zonegroup', ''),
            placement_rule=raw.get('placement_rule', ''),
            num_shards=raw.get('num_shards', 0) or 0,
            size_bytes=total_size,
            size_actual_bytes=total_size_actual,
            num_objects=total_objects,
            storage_classes=storage_classes,
            collection_duration_ms=int(duration_seconds * 1000)
        )