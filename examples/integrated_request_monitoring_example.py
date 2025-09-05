#!/usr/bin/env python3
"""
集成請求監控示例

展示如何將 request_timing.py 與 KV cache 傳輸監控集成，
並將所有指標保存到 CSV 文件中。
"""

import asyncio
import time
import random
import logging
from typing import Dict, Any

# 設置日誌
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 導入監控模組
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../src'))

from vllm_router.monitoring.request_timing import get_request_timing_monitor, RequestTimingData
from vllm_router.monitoring.realtime_csv_exporter import setup_realtime_csv_export
from vllm_router.services.kv_transfer_monitor import get_kv_transfer_monitor


class IntegratedRequestMonitor:
    """集成請求監控器"""
    
    def __init__(self, output_dir: str = "/tmp/integrated_monitoring"):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        
        # 初始化監控器
        self.timing_monitor = get_request_timing_monitor()
        self.kv_transfer_monitor = get_kv_transfer_monitor()
        self.csv_exporter = setup_realtime_csv_export(
            export_interval=30,  # 30秒導出一次
            max_records_per_file=100
        )
        
        logger.info(f"集成監控器已初始化，輸出目錄: {output_dir}")
    
    async def simulate_request_with_kv_cache_transfer(
        self, 
        request_id: str,
        endpoint: str,
        model: str,
        has_kv_cache_transfer: bool = True
    ) -> RequestTimingData:
        """
        模擬帶有 KV cache 傳輸的請求
        
        Args:
            request_id: 請求 ID
            endpoint: 端點
            model: 模型名稱
            has_kv_cache_transfer: 是否有 KV cache 傳輸
            
        Returns:
            請求時間數據
        """
        # 開始請求追蹤
        timing_data = self.timing_monitor.start_request(
            request_id=request_id,
            endpoint=endpoint,
            model=model,
            session_id=f"session_{random.randint(1, 100)}"
        )
        
        # 模擬路由器處理
        self.timing_monitor.record_router_processing_start(timing_data)
        await asyncio.sleep(random.uniform(0.01, 0.05))  # 10-50ms
        
        # 模擬路由決策
        routing_decision_time = random.uniform(0.005, 0.02)  # 5-20ms
        self.timing_monitor.record_step_time(timing_data, "routing_decision_time", routing_decision_time)
        
        # 模擬 KV cache 查找
        kv_lookup_time = random.uniform(0.001, 0.01)  # 1-10ms
        kv_hit = random.random() < 0.7  # 70% 命中率
        self.timing_monitor.record_kv_cache_lookup(timing_data, kv_lookup_time, kv_hit)
        
        # 模擬 KV cache 傳輸（如果需要）
        if has_kv_cache_transfer and not kv_hit:
            transfer_time = random.uniform(0.01, 0.1)  # 10-100ms
            transfer_size = random.randint(1024*1024, 100*1024*1024)  # 1-100MB
            transfer_type = random.choice(["send", "receive", "p2p"])
            throughput_gbps = transfer_size / (transfer_time * 1024**3)  # GB/s
            
            self.timing_monitor.record_kv_cache_transfer(
                timing_data, transfer_time, transfer_type, transfer_size, throughput_gbps
            )
            
            # 同時記錄到 KV transfer monitor
            transfer_id = f"transfer_{request_id}_{int(time.time())}"
            self.kv_transfer_monitor.start_transfer(
                transfer_id=transfer_id,
                transfer_type=transfer_type,
                size_bytes=transfer_size,
                source_instance="instance_1",
                target_instance="instance_2"
            )
            
            # 模擬傳輸時間
            await asyncio.sleep(transfer_time)
            
            self.kv_transfer_monitor.end_transfer(transfer_id)
        
        self.timing_monitor.record_router_processing_end(timing_data)
        
        # 模擬後端處理
        self.timing_monitor.record_backend_processing_start(timing_data)
        await asyncio.sleep(random.uniform(0.1, 0.5))  # 100-500ms
        self.timing_monitor.record_backend_processing_end(timing_data)
        
        # 模擬響應流
        self.timing_monitor.record_response_streaming_start(timing_data)
        
        # 模擬第一個 token
        first_token_delay = random.uniform(0.05, 0.2)  # 50-200ms
        await asyncio.sleep(first_token_delay)
        self.timing_monitor.record_first_token(timing_data)
        
        # 模擬解碼過程
        decode_time = random.uniform(0.5, 2.0)  # 500ms-2s
        await asyncio.sleep(decode_time)
        self.timing_monitor.record_last_token(timing_data)
        
        self.timing_monitor.record_response_streaming_end(timing_data)
        
        # 完成請求
        self.timing_monitor.complete_request(
            timing_data=timing_data,
            server_url=f"http://backend-{random.randint(1, 3)}:8000",
            status_code=200
        )
        
        # 添加到實時 CSV 導出器
        self.csv_exporter.add_record(timing_data)
        
        return timing_data
    
    async def run_monitoring_simulation(self, num_requests: int = 100):
        """
        運行監控模擬
        
        Args:
            num_requests: 請求數量
        """
        logger.info(f"開始模擬 {num_requests} 個請求")
        
        # 創建請求任務
        tasks = []
        for i in range(num_requests):
            request_id = f"req_{i:04d}"
            endpoint = random.choice([
                "/v1/chat/completions",
                "/v1/completions", 
                "/v1/embeddings"
            ])
            model = random.choice([
                "llama-2-7b-chat",
                "llama-2-13b-chat",
                "mistral-7b-instruct"
            ])
            has_kv_transfer = random.random() < 0.3  # 30% 有 KV cache 傳輸
            
            task = asyncio.create_task(
                self.simulate_request_with_kv_cache_transfer(
                    request_id, endpoint, model, has_kv_transfer
                )
            )
            tasks.append(task)
        
        # 並發執行請求
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 統計結果
        successful_requests = [r for r in results if isinstance(r, RequestTimingData)]
        failed_requests = [r for r in results if isinstance(r, Exception)]
        
        logger.info(f"模擬完成: {len(successful_requests)} 成功, {len(failed_requests)} 失敗")
        
        # 顯示統計信息
        if successful_requests:
            total_times = [r.total_request_time for r in successful_requests]
            ttft_times = [r.ttft for r in successful_requests]
            kv_transfer_times = [r.kv_cache_transfer_time for r in successful_requests if r.kv_cache_transfer_time > 0]
            
            logger.info(f"平均總請求時間: {sum(total_times)/len(total_times):.3f}s")
            logger.info(f"平均 TTFT: {sum(ttft_times)/len(ttft_times):.3f}s")
            if kv_transfer_times:
                logger.info(f"平均 KV cache 傳輸時間: {sum(kv_transfer_times)/len(kv_transfer_times):.3f}s")
        
        return successful_requests
    
    def get_monitoring_stats(self) -> Dict[str, Any]:
        """獲取監控統計信息"""
        timing_stats = self.timing_monitor.get_stats()
        kv_transfer_stats = self.kv_transfer_monitor.get_aggregated_stats()
        csv_exporter_stats = self.csv_exporter.get_stats()
        
        return {
            "timing_monitor": timing_stats,
            "kv_transfer_monitor": kv_transfer_stats,
            "csv_exporter": csv_exporter_stats,
        }
    
    def stop_monitoring(self):
        """停止監控"""
        self.csv_exporter.stop_exporting()
        logger.info("監控已停止")


async def main():
    """主函數"""
    # 創建集成監控器
    monitor = IntegratedRequestMonitor()
    
    try:
        # 運行模擬
        results = await monitor.run_monitoring_simulation(num_requests=50)
        
        # 等待一段時間讓 CSV 導出完成
        await asyncio.sleep(60)
        
        # 顯示統計信息
        stats = monitor.get_monitoring_stats()
        logger.info("監控統計:")
        for category, data in stats.items():
            logger.info(f"{category}: {data}")
        
    finally:
        # 停止監控
        monitor.stop_monitoring()


if __name__ == "__main__":
    asyncio.run(main())
