"""
KV Cache Transfer Monitor Service

This service monitors KV cache transfer operations between instances
and exposes metrics for Prometheus monitoring.
"""

import time
import logging
from typing import Dict, Optional
from dataclasses import dataclass
from threading import Lock

from vllm_router.services.metrics_service import (
    kv_cache_transfer_time,
    kv_cache_transfer_count,
    kv_cache_transfer_throughput,
    kv_cache_transfer_size,
)
from vllm_router.utils import SingletonMeta

logger = logging.getLogger(__name__)


@dataclass
class TransferStats:
    """Statistics for a single KV cache transfer operation"""
    transfer_id: str
    start_time: float
    end_time: Optional[float] = None
    transfer_type: str = "unknown"  # "send", "receive", "p2p"
    size_bytes: int = 0
    source_instance: str = ""
    target_instance: str = ""
    
    @property
    def duration_ms(self) -> float:
        """Get transfer duration in milliseconds"""
        if self.end_time is None:
            return 0.0
        return (self.end_time - self.start_time) * 1000
    
    @property
    def throughput_gbps(self) -> float:
        """Get transfer throughput in GB/s"""
        if self.duration_ms == 0 or self.size_bytes == 0:
            return 0.0
        duration_s = self.duration_ms / 1000.0
        return (self.size_bytes / duration_s) / (2**30)  # Convert to GB/s


class KVTransferMonitor(metaclass=SingletonMeta):
    """
    Monitor for KV cache transfer operations between instances.
    
    This class tracks transfer operations and exposes metrics for monitoring
    the time spent transferring KV caches between different instances.
    """
    
    def __init__(self):
        self._active_transfers: Dict[str, TransferStats] = {}
        self._completed_transfers: Dict[str, TransferStats] = {}
        self._lock = Lock()
        
        # Aggregated statistics
        self._total_transfers = 0
        self._total_transfer_time_ms = 0.0
        self._total_transfer_size_bytes = 0
        
    def start_transfer(
        self, 
        transfer_id: str, 
        transfer_type: str = "unknown",
        size_bytes: int = 0,
        source_instance: str = "",
        target_instance: str = ""
    ) -> None:
        """
        Start tracking a KV cache transfer operation.
        
        Args:
            transfer_id: Unique identifier for the transfer
            transfer_type: Type of transfer ("send", "receive", "p2p")
            size_bytes: Size of the data being transferred
            source_instance: Source instance identifier
            target_instance: Target instance identifier
        """
        with self._lock:
            transfer_stats = TransferStats(
                transfer_id=transfer_id,
                start_time=time.perf_counter(),
                transfer_type=transfer_type,
                size_bytes=size_bytes,
                source_instance=source_instance,
                target_instance=target_instance
            )
            self._active_transfers[transfer_id] = transfer_stats
            
            logger.debug(
                f"Started KV cache transfer {transfer_id} "
                f"(type: {transfer_type}, size: {size_bytes} bytes, "
                f"source: {source_instance} -> target: {target_instance})"
            )
    
    def end_transfer(self, transfer_id: str) -> Optional[TransferStats]:
        """
        End tracking a KV cache transfer operation.
        
        Args:
            transfer_id: Unique identifier for the transfer
            
        Returns:
            TransferStats object if found, None otherwise
        """
        with self._lock:
            if transfer_id not in self._active_transfers:
                logger.warning(f"Transfer {transfer_id} not found in active transfers")
                return None
                
            transfer_stats = self._active_transfers.pop(transfer_id)
            transfer_stats.end_time = time.perf_counter()
            
            # Move to completed transfers
            self._completed_transfers[transfer_id] = transfer_stats
            
            # Update aggregated statistics
            self._total_transfers += 1
            self._total_transfer_time_ms += transfer_stats.duration_ms
            self._total_transfer_size_bytes += transfer_stats.size_bytes
            
            logger.debug(
                f"Completed KV cache transfer {transfer_id} "
                f"in {transfer_stats.duration_ms:.2f}ms "
                f"(throughput: {transfer_stats.throughput_gbps:.2f} GB/s)"
            )
            
            return transfer_stats
    
    def get_transfer_stats(self, transfer_id: str) -> Optional[TransferStats]:
        """Get statistics for a specific transfer"""
        with self._lock:
            return self._completed_transfers.get(transfer_id)
    
    def get_aggregated_stats(self) -> Dict[str, float]:
        """Get aggregated statistics for all transfers"""
        with self._lock:
            if self._total_transfers == 0:
                return {
                    "total_transfers": 0,
                    "avg_transfer_time_ms": 0.0,
                    "total_transfer_time_ms": 0.0,
                    "total_transfer_size_bytes": 0,
                    "avg_throughput_gbps": 0.0
                }
            
            avg_transfer_time_ms = self._total_transfer_time_ms / self._total_transfers
            total_duration_s = self._total_transfer_time_ms / 1000.0
            avg_throughput_gbps = (
                (self._total_transfer_size_bytes / total_duration_s) / (2**30)
                if total_duration_s > 0 else 0.0
            )
            
            return {
                "total_transfers": self._total_transfers,
                "avg_transfer_time_ms": avg_transfer_time_ms,
                "total_transfer_time_ms": self._total_transfer_time_ms,
                "total_transfer_size_bytes": self._total_transfer_size_bytes,
                "avg_throughput_gbps": avg_throughput_gbps
            }
    
    def update_prometheus_metrics(self, server: str) -> None:
        """
        Update Prometheus metrics with current transfer statistics.
        
        Args:
            server: Server identifier for labeling metrics
        """
        with self._lock:
            # Group transfers by type for metrics
            transfers_by_type = {}
            for transfer in self._completed_transfers.values():
                transfer_type = transfer.transfer_type
                if transfer_type not in transfers_by_type:
                    transfers_by_type[transfer_type] = {
                        "count": 0,
                        "total_time_ms": 0.0,
                        "total_size_bytes": 0,
                        "total_throughput_gbps": 0.0
                    }
                
                stats = transfers_by_type[transfer_type]
                stats["count"] += 1
                stats["total_time_ms"] += transfer.duration_ms
                stats["total_size_bytes"] += transfer.size_bytes
                stats["total_throughput_gbps"] += transfer.throughput_gbps
            
            # Update Prometheus metrics
            for transfer_type, stats in transfers_by_type.items():
                if stats["count"] > 0:
                    avg_time_ms = stats["total_time_ms"] / stats["count"]
                    avg_throughput_gbps = stats["total_throughput_gbps"] / stats["count"]
                    
                    kv_cache_transfer_time.labels(
                        server=server, 
                        transfer_type=transfer_type
                    ).set(avg_time_ms)
                    
                    kv_cache_transfer_count.labels(
                        server=server, 
                        transfer_type=transfer_type
                    ).set(stats["count"])
                    
                    kv_cache_transfer_throughput.labels(
                        server=server, 
                        transfer_type=transfer_type
                    ).set(avg_throughput_gbps)
                    
                    kv_cache_transfer_size.labels(
                        server=server, 
                        transfer_type=transfer_type
                    ).set(stats["total_size_bytes"])
    
    def clear_old_transfers(self, max_age_seconds: int = 3600) -> None:
        """
        Clear old completed transfers to prevent memory buildup.
        
        Args:
            max_age_seconds: Maximum age of transfers to keep
        """
        current_time = time.perf_counter()
        cutoff_time = current_time - max_age_seconds
        
        with self._lock:
            old_transfers = [
                transfer_id for transfer_id, transfer in self._completed_transfers.items()
                if transfer.start_time < cutoff_time
            ]
            
            for transfer_id in old_transfers:
                del self._completed_transfers[transfer_id]
            
            if old_transfers:
                logger.debug(f"Cleared {len(old_transfers)} old transfer records")


# Global instance
_kv_transfer_monitor = None

def get_kv_transfer_monitor() -> KVTransferMonitor:
    """Get the global KV transfer monitor instance"""
    global _kv_transfer_monitor
    if _kv_transfer_monitor is None:
        _kv_transfer_monitor = KVTransferMonitor()
    return _kv_transfer_monitor
