#!/usr/bin/env python3
"""
P2P KV Cache 監控示例

專門針對 P2P KV cache 傳輸的監控示例，展示如何記錄和保存 P2P 傳輸時間到 CSV。
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


class P2PKVCacheMonitor:
    """P2P KV Cache 監控器"""
    
    def __init__(self, output_dir: str = "/tmp/p2p_monitoring"):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        
        # 初始化監控器
        self.timing_monitor = get_request_timing_monitor()
        self.csv_exporter = setup_realtime_csv_export(
            export_interval=30,  # 30秒導出一次
            max_records_per_file=50  # 每個文件50條記錄
        )
        
        logger.info(f"P2P KV Cache 監控器已初始化，輸出目錄: {output_dir}")
    
    async def simulate_p2p_request(
        self, 
        request_id: str,
        endpoint: str,
        model: str,
        p2p_transfer_size_mb: float = 50.0,
        p2p_throughput_gbps: float = 2.0
    ) -> RequestTimingData:
        """
        模擬帶有 P2P KV cache 傳輸的請求
        
        Args:
            request_id: 請求 ID
            endpoint: 端點
            model: 模型名稱
            p2p_transfer_size_mb: P2P 傳輸大小 (MB)
            p2p_throughput_gbps: P2P 傳輸吞吐量 (GB/s)
            
        Returns:
            請求時間數據
        """
        # 開始請求追蹤
        timing_data = self.timing_monitor.start_request(
            request_id=request_id,
            endpoint=endpoint,
            model=model,
            session_id=f"p2p_session_{random.randint(1, 100)}"
        )
        
        # 模擬路由器處理
        self.timing_monitor.record_router_processing_start(timing_data)
        await asyncio.sleep(random.uniform(0.01, 0.05))  # 10-50ms
        
        # 模擬路由決策
        routing_decision_time = random.uniform(0.005, 0.02)  # 5-20ms
        self.timing_monitor.record_step_time(timing_data, "routing_decision_time", routing_decision_time)
        
        # 模擬 KV cache 查找
        kv_lookup_time = random.uniform(0.001, 0.01)  # 1-10ms
        kv_hit = random.random() < 0.3  # 30% 命中率（P2P 場景下命中率較低）
        self.timing_monitor.record_kv_cache_lookup(timing_data, kv_lookup_time, kv_hit)
        
        # 模擬 P2P KV cache 傳輸
        if not kv_hit:  # 如果未命中，需要 P2P 傳輸
            p2p_transfer_size_bytes = int(p2p_transfer_size_mb * 1024 * 1024)
            p2p_transfer_time = p2p_transfer_size_bytes / (p2p_throughput_gbps * 1024**3)  # 計算傳輸時間
            
            logger.info(f"開始 P2P 傳輸: {p2p_transfer_size_mb:.1f}MB, 預期時間: {p2p_transfer_time:.3f}s")
            
            # 記錄 P2P 傳輸開始
            self.timing_monitor.record_kv_cache_transfer(
                timing_data,
                transfer_time=p2p_transfer_time,
                transfer_type="p2p",  # 關鍵：使用 P2P 類型
                transfer_size_bytes=p2p_transfer_size_bytes,
                throughput_gbps=p2p_throughput_gbps
            )
            
            # 模擬實際 P2P 傳輸時間
            await asyncio.sleep(p2p_transfer_time)
            
            logger.info(f"P2P 傳輸完成: {p2p_transfer_time:.3f}s")
        else:
            logger.info("KV cache 命中，無需 P2P 傳輸")
        
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
            server_url=f"http://p2p-backend-{random.randint(1, 3)}:8000",
            status_code=200
        )
        
        # 添加到實時 CSV 導出器
        self.csv_exporter.add_record(timing_data)
        
        return timing_data
    
    async def run_p2p_monitoring_simulation(self, num_requests: int = 30):
        """
        運行 P2P 監控模擬
        
        Args:
            num_requests: 請求數量
        """
        logger.info(f"開始 P2P KV Cache 監控模擬，共 {num_requests} 個請求")
        
        # 創建請求任務
        tasks = []
        for i in range(num_requests):
            request_id = f"p2p_req_{i:04d}"
            endpoint = random.choice([
                "/v1/chat/completions",
                "/v1/completions"
            ])
            model = random.choice([
                "llama-2-7b-chat",
                "llama-2-13b-chat",
                "mistral-7b-instruct"
            ])
            
            # 隨機 P2P 傳輸參數
            p2p_size_mb = random.uniform(10, 100)  # 10-100MB
            p2p_throughput = random.uniform(1.0, 5.0)  # 1-5 GB/s
            
            task = asyncio.create_task(
                self.simulate_p2p_request(
                    request_id, endpoint, model, p2p_size_mb, p2p_throughput
                )
            )
            tasks.append(task)
        
        # 並發執行請求
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 統計結果
        successful_requests = [r for r in results if isinstance(r, RequestTimingData)]
        failed_requests = [r for r in results if isinstance(r, Exception)]
        
        logger.info(f"P2P 模擬完成: {len(successful_requests)} 成功, {len(failed_requests)} 失敗")
        
        # 顯示 P2P 統計信息
        if successful_requests:
            p2p_requests = [r for r in successful_requests if r.kv_cache_transfer_type == "p2p"]
            non_p2p_requests = [r for r in successful_requests if r.kv_cache_transfer_type != "p2p"]
            
            logger.info(f"P2P 傳輸請求: {len(p2p_requests)}")
            logger.info(f"非 P2P 請求: {len(non_p2p_requests)}")
            
            if p2p_requests:
                p2p_times = [r.kv_cache_transfer_time for r in p2p_requests]
                p2p_sizes = [r.kv_cache_transfer_size_bytes / (1024*1024) for r in p2p_requests]
                p2p_throughputs = [r.kv_cache_transfer_throughput_gbps for r in p2p_requests]
                
                logger.info(f"平均 P2P 傳輸時間: {sum(p2p_times)/len(p2p_times):.3f}s")
                logger.info(f"平均 P2P 傳輸大小: {sum(p2p_sizes)/len(p2p_sizes):.1f}MB")
                logger.info(f"平均 P2P 傳輸吞吐量: {sum(p2p_throughputs)/len(p2p_throughputs):.2f}GB/s")
        
        return successful_requests
    
    def get_p2p_stats(self) -> Dict[str, Any]:
        """獲取 P2P 統計信息"""
        return {
            "output_directory": self.output_dir,
            "csv_exporter_stats": self.csv_exporter.get_stats(),
            "timing_monitor_stats": self.timing_monitor.get_stats(),
        }
    
    def stop_monitoring(self):
        """停止監控"""
        self.csv_exporter.stop_exporting()
        logger.info("P2P 監控已停止")


async def main():
    """主函數"""
    # 創建 P2P 監控器
    monitor = P2PKVCacheMonitor()
    
    try:
        # 運行 P2P 模擬
        results = await monitor.run_p2p_monitoring_simulation(num_requests=20)
        
        # 等待 CSV 導出完成
        logger.info("等待 CSV 導出完成...")
        await asyncio.sleep(60)
        
        # 顯示統計信息
        stats = monitor.get_p2p_stats()
        logger.info("P2P 監控統計:")
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
    print("P2P KV Cache 監控示例")
    print("=" * 50)
    print("此示例將模擬 P2P KV cache 傳輸並將數據保存到 CSV")
    print("每個請求都會生成一行記錄，包含 P2P 傳輸時間等指標")
    print("=" * 50)
    
    asyncio.run(main())

