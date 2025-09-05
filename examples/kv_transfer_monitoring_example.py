#!/usr/bin/env python3
"""
KV Cache Transfer Monitoring Example

This example demonstrates how to monitor KV cache transfer times
between instances in the production-stack system.
"""

import asyncio
import time
import logging
from typing import Dict, Any

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Import the monitoring system
from vllm_router.services.kv_transfer_monitor import get_kv_transfer_monitor


async def simulate_kv_cache_transfer(
    transfer_id: str,
    transfer_type: str,
    size_bytes: int,
    duration_ms: float,
    source_instance: str,
    target_instance: str
) -> None:
    """
    Simulate a KV cache transfer operation.
    
    Args:
        transfer_id: Unique identifier for the transfer
        transfer_type: Type of transfer ("send", "receive", "p2p")
        size_bytes: Size of the data being transferred
        duration_ms: Duration of the transfer in milliseconds
        source_instance: Source instance identifier
        target_instance: Target instance identifier
    """
    monitor = get_kv_transfer_monitor()
    
    # Start monitoring the transfer
    monitor.start_transfer(
        transfer_id=transfer_id,
        transfer_type=transfer_type,
        size_bytes=size_bytes,
        source_instance=source_instance,
        target_instance=target_instance
    )
    
    logger.info(f"Started transfer {transfer_id} ({transfer_type}) - {size_bytes} bytes")
    
    # Simulate the transfer work
    await asyncio.sleep(duration_ms / 1000.0)
    
    # End monitoring
    transfer_stats = monitor.end_transfer(transfer_id)
    
    if transfer_stats:
        logger.info(
            f"Completed transfer {transfer_id} in {transfer_stats.duration_ms:.2f}ms "
            f"(throughput: {transfer_stats.throughput_gbps:.2f} GB/s)"
        )


async def main():
    """Main function to demonstrate KV cache transfer monitoring."""
    
    # Get the monitor instance
    monitor = get_kv_transfer_monitor()
    
    logger.info("Starting KV Cache Transfer Monitoring Example")
    
    # Simulate multiple transfers
    transfers = [
        {
            "transfer_id": "transfer_001",
            "transfer_type": "send",
            "size_bytes": 1024 * 1024 * 100,  # 100 MB
            "duration_ms": 50.0,
            "source_instance": "instance_1",
            "target_instance": "instance_2"
        },
        {
            "transfer_id": "transfer_002",
            "transfer_type": "receive",
            "size_bytes": 1024 * 1024 * 200,  # 200 MB
            "duration_ms": 100.0,
            "source_instance": "instance_2",
            "target_instance": "instance_1"
        },
        {
            "transfer_id": "transfer_003",
            "transfer_type": "p2p",
            "size_bytes": 1024 * 1024 * 50,   # 50 MB
            "duration_ms": 25.0,
            "source_instance": "instance_1",
            "target_instance": "instance_3"
        },
        {
            "transfer_id": "transfer_004",
            "transfer_type": "send",
            "size_bytes": 1024 * 1024 * 150,  # 150 MB
            "duration_ms": 75.0,
            "source_instance": "instance_3",
            "target_instance": "instance_2"
        }
    ]
    
    # Run transfers concurrently
    tasks = []
    for transfer in transfers:
        task = asyncio.create_task(
            simulate_kv_cache_transfer(**transfer)
        )
        tasks.append(task)
    
    # Wait for all transfers to complete
    await asyncio.gather(*tasks)
    
    # Display aggregated statistics
    logger.info("\n" + "="*50)
    logger.info("AGGREGATED TRANSFER STATISTICS")
    logger.info("="*50)
    
    stats = monitor.get_aggregated_stats()
    for key, value in stats.items():
        logger.info(f"{key}: {value}")
    
    # Display individual transfer statistics
    logger.info("\n" + "="*50)
    logger.info("INDIVIDUAL TRANSFER STATISTICS")
    logger.info("="*50)
    
    for transfer in transfers:
        transfer_stats = monitor.get_transfer_stats(transfer["transfer_id"])
        if transfer_stats:
            logger.info(
                f"Transfer {transfer_stats.transfer_id}: "
                f"{transfer_stats.duration_ms:.2f}ms, "
                f"{transfer_stats.throughput_gbps:.2f} GB/s, "
                f"{transfer_stats.size_bytes} bytes"
            )
    
    # Simulate updating Prometheus metrics
    logger.info("\n" + "="*50)
    logger.info("UPDATING PROMETHEUS METRICS")
    logger.info("="*50)
    
    for instance in ["instance_1", "instance_2", "instance_3"]:
        monitor.update_prometheus_metrics(server=instance)
        logger.info(f"Updated metrics for {instance}")
    
    logger.info("\nExample completed successfully!")


def print_usage():
    """Print usage information."""
    print("""
KV Cache Transfer Monitoring Example

This example demonstrates how to monitor KV cache transfer times
between instances in the production-stack system.

Usage:
    python kv_transfer_monitoring_example.py

The example will:
1. Simulate multiple KV cache transfers between instances
2. Track transfer times, throughput, and sizes
3. Display aggregated and individual statistics
4. Update Prometheus metrics

Available transfer types:
- send: Sending KV cache to another instance
- receive: Receiving KV cache from another instance  
- p2p: Peer-to-peer KV cache sharing

Metrics exposed:
- vllm:kv_cache_transfer_time_ms: Average transfer time per type
- vllm:kv_cache_transfer_count_total: Total number of transfers per type
- vllm:kv_cache_transfer_throughput_gbps: Average throughput per type
- vllm:kv_cache_transfer_size_bytes: Total size transferred per type
""")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] in ["-h", "--help"]:
        print_usage()
        sys.exit(0)
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Example interrupted by user")
    except Exception as e:
        logger.error(f"Example failed: {e}")
        sys.exit(1)
