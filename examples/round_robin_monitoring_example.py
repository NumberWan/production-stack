#!/usr/bin/env python3
"""
Round-Robin 路由監控示例

展示如何在 round-robin 路由模式下使用 KV cache 傳輸監控。
"""

import asyncio
import time
import random
import logging
from typing import Dict, Any, List

# 設置日誌
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 導入監控模組
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../src'))

from vllm_router.monitoring.request_timing import get_request_timing_monitor, RequestTimingData
from vllm_router.monitoring.realtime_csv_exporter import setup_realtime_csv_export


class RoundRobinBackend:
    """模擬 Round-Robin 後端"""
    
    def __init__(self, backend_id: str, has_kv_cache: bool = True):
        self.backend_id = backend_id
        self.has_kv_cache = has_kv_cache
        self.request_count = 0
        self.kv_cache_data = {}  # 模擬 KV cache 存儲
    
    def process_request(self, request_id: str, prompt: str) -> Dict[str, Any]:
        """處理請求"""
        self.request_count += 1
        
        # 模擬 KV cache 查找
        cache_key = hash(prompt[:100])  # 使用 prompt 前100字符作為 key
        kv_hit = cache_key in self.kv_cache_data
        
        if not kv_hit and self.has_kv_cache:
            # 模擬從其他後端獲取 KV cache
            self.kv_cache_data[cache_key] = f"kv_cache_data_for_{request_id}"
            kv_transfer_time = random.uniform(0.01, 0.1)  # 10-100ms
            kv_transfer_size = random.randint(1024*1024, 50*1024*1024)  # 1-50MB
        else:
            kv_transfer_time = 0.0
            kv_transfer_size = 0
        
        # 模擬處理時間
        processing_time = random.uniform(0.1, 0.5)
        
        return {
            "backend_id": self.backend_id,
            "processing_time": processing_time,
            "kv_hit": kv_hit,
            "kv_transfer_time": kv_transfer_time,
            "kv_transfer_size": kv_transfer_size,
            "request_count": self.request_count
        }


class RoundRobinMonitor:
    """Round-Robin 監控器"""
    
    def __init__(self, output_dir: str = "/tmp/round_robin_monitoring"):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        
        # 初始化監控器
        self.timing_monitor = get_request_timing_monitor()
        self.csv_exporter = setup_realtime_csv_export(
            export_interval=30,
            max_records_per_file=50
        )
        
        # 創建多個後端（模擬 round-robin）
        self.backends = [
            RoundRobinBackend("backend-1", has_kv_cache=True),
            RoundRobinBackend("backend-2", has_kv_cache=True),
            RoundRobinBackend("backend-3", has_kv_cache=False),  # 這個後端沒有 KV cache
        ]
        self.current_backend_index = 0
        
        logger.info(f"Round-Robin 監控器已初始化，{len(self.backends)} 個後端")
    
    def get_next_backend(self) -> RoundRobinBackend:
        """獲取下一個後端（Round-Robin 邏輯）"""
        backend = self.backends[self.current_backend_index]
        self.current_backend_index = (self.current_backend_index + 1) % len(self.backends)
        return backend
    
    async def simulate_round_robin_request(
        self, 
        request_id: str,
        endpoint: str,
        model: str,
        prompt: str
    ) -> RequestTimingData:
        """
        模擬 Round-Robin 路由的請求
        
        Args:
            request_id: 請求 ID
            endpoint: 端點
            model: 模型名稱
            prompt: 輸入提示
            
        Returns:
            請求時間數據
        """
        # 開始請求追蹤
        timing_data = self.timing_monitor.start_request(
            request_id=request_id,
            endpoint=endpoint,
            model=model,
            session_id=f"rr_session_{random.randint(1, 100)}"
        )
        
        # 設置路由邏輯為 round-robin
        timing_data.routing_logic = "round_robin"
        
        # 模擬路由器處理
        self.timing_monitor.record_router_processing_start(timing_data)
        await asyncio.sleep(random.uniform(0.01, 0.05))  # 10-50ms
        
        # 模擬路由決策（Round-Robin）
        routing_decision_time = random.uniform(0.005, 0.02)  # 5-20ms
        self.timing_monitor.record_step_time(timing_data, "routing_decision_time", routing_decision_time)
        
        # 選擇後端（Round-Robin）
        selected_backend = self.get_next_backend()
        logger.info(f"請求 {request_id} 路由到 {selected_backend.backend_id}")
        
        # 模擬後端連接
        backend_connection_time = random.uniform(0.001, 0.01)  # 1-10ms
        self.timing_monitor.record_step_time(timing_data, "backend_connection_time", backend_connection_time)
        
        # 模擬 KV cache 查找
        kv_lookup_time = random.uniform(0.001, 0.01)  # 1-10ms
        self.timing_monitor.record_kv_cache_lookup(timing_data, kv_lookup_time, False)  # 先設為未命中
        
        # 處理請求
        result = selected_backend.process_request(request_id, prompt)
        
        # 記錄 KV cache 傳輸（如果發生）
        if result["kv_transfer_time"] > 0:
            logger.info(f"KV cache 傳輸: {result['kv_transfer_time']:.3f}s, {result['kv_transfer_size']/1024/1024:.1f}MB")
            
            self.timing_monitor.record_kv_cache_transfer(
                timing_data,
                transfer_time=result["kv_transfer_time"],
                transfer_type="p2p",  # Round-robin 中也可以使用 P2P
                transfer_size_bytes=result["kv_transfer_size"],
                throughput_gbps=result["kv_transfer_size"] / (result["kv_transfer_time"] * 1024**3)
            )
            
            # 更新 KV cache 命中狀態
            timing_data.kv_cache_hit = True
            timing_data.kv_cache_miss = False
        
        self.timing_monitor.record_router_processing_end(timing_data)
        
        # 模擬後端處理
        self.timing_monitor.record_backend_processing_start(timing_data)
        await asyncio.sleep(result["processing_time"])
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
            server_url=f"http://{selected_backend.backend_id}:8000",
            status_code=200
        )
        
        # 添加到實時 CSV 導出器
        self.csv_exporter.add_record(timing_data)
        
        return timing_data
    
    async def run_round_robin_simulation(self, num_requests: int = 30):
        """
        運行 Round-Robin 模擬
        
        Args:
            num_requests: 請求數量
        """
        logger.info(f"開始 Round-Robin 路由監控模擬，共 {num_requests} 個請求")
        
        # 創建請求任務
        tasks = []
        prompts = [
            "What is the capital of France?",
            "Explain quantum computing in simple terms.",
            "How does machine learning work?",
            "What are the benefits of renewable energy?",
            "Describe the process of photosynthesis."
        ]
        
        for i in range(num_requests):
            request_id = f"rr_req_{i:04d}"
            endpoint = random.choice([
                "/v1/chat/completions",
                "/v1/completions"
            ])
            model = random.choice([
                "llama-2-7b-chat",
                "llama-2-13b-chat",
                "mistral-7b-instruct"
            ])
            prompt = random.choice(prompts)
            
            task = asyncio.create_task(
                self.simulate_round_robin_request(
                    request_id, endpoint, model, prompt
                )
            )
            tasks.append(task)
        
        # 並發執行請求
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 統計結果
        successful_requests = [r for r in results if isinstance(r, RequestTimingData)]
        failed_requests = [r for r in results if isinstance(r, Exception)]
        
        logger.info(f"Round-Robin 模擬完成: {len(successful_requests)} 成功, {len(failed_requests)} 失敗")
        
        # 顯示 Round-Robin 統計信息
        if successful_requests:
            round_robin_requests = [r for r in successful_requests if r.routing_logic == "round_robin"]
            kv_transfer_requests = [r for r in successful_requests if r.kv_cache_transfer_time > 0]
            
            logger.info(f"Round-Robin 請求: {len(round_robin_requests)}")
            logger.info(f"KV cache 傳輸請求: {len(kv_transfer_requests)}")
            
            if kv_transfer_requests:
                kv_times = [r.kv_cache_transfer_time for r in kv_transfer_requests]
                kv_sizes = [r.kv_cache_transfer_size_bytes / (1024*1024) for r in kv_transfer_requests]
                
                logger.info(f"平均 KV cache 傳輸時間: {sum(kv_times)/len(kv_times):.3f}s")
                logger.info(f"平均 KV cache 傳輸大小: {sum(kv_sizes)/len(kv_sizes):.1f}MB")
            
            # 後端使用統計
            backend_usage = {}
            for request in successful_requests:
                backend_id = request.server_url.split("//")[1].split(":")[0]
                backend_usage[backend_id] = backend_usage.get(backend_id, 0) + 1
            
            logger.info("後端使用統計:")
            for backend_id, count in backend_usage.items():
                logger.info(f"  {backend_id}: {count} 個請求")
        
        return successful_requests
    
    def get_round_robin_stats(self) -> Dict[str, Any]:
        """獲取 Round-Robin 統計信息"""
        return {
            "output_directory": self.output_dir,
            "backend_count": len(self.backends),
            "current_backend_index": self.current_backend_index,
            "csv_exporter_stats": self.csv_exporter.get_stats(),
            "timing_monitor_stats": self.timing_monitor.get_stats(),
        }
    
    def stop_monitoring(self):
        """停止監控"""
        self.csv_exporter.stop_exporting()
        logger.info("Round-Robin 監控已停止")


async def main():
    """主函數"""
    # 創建 Round-Robin 監控器
    monitor = RoundRobinMonitor()
    
    try:
        # 運行 Round-Robin 模擬
        results = await monitor.run_round_robin_simulation(num_requests=20)
        
        # 等待 CSV 導出完成
        logger.info("等待 CSV 導出完成...")
        await asyncio.sleep(60)
        
        # 顯示統計信息
        stats = monitor.get_round_robin_stats()
        logger.info("Round-Robin 監控統計:")
        for category, data in stats.items():
            logger.info(f"{category}: {data}")
        
        # 顯示 CSV 文件位置
        logger.info(f"CSV 文件保存在: {monitor.output_dir}")
        logger.info("文件包括:")
        for file in os.listdir(monitor.output_dir):
            if file.endswith('.csv'):
                file_path = os.path.join(monitor.output_dir, file)
                file_size = os.path.getsize(file_path)
                logger.info(f"  - {file} ({file_size} bytes)")
        
    finally:
        # 停止監控
        monitor.stop_monitoring()


if __name__ == "__main__":
    print("Round-Robin 路由監控示例")
    print("=" * 50)
    print("此示例展示如何在 Round-Robin 路由模式下監控 KV cache 傳輸")
    print("支持多後端負載均衡和 KV cache 共享")
    print("=" * 50)
    
    asyncio.run(main())


