# Copyright 2024-2025 The vLLM Production Stack Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import time
import csv
import os
import threading
from typing import Dict, Optional
from dataclasses import dataclass, asdict
from datetime import datetime


@dataclass
class RequestTimingData:
    """完整的請求時間追蹤數據結構"""
    request_id: str
    endpoint: str
    model: str
    session_id: Optional[str]
    server_url: str
    routing_logic: str = ""
    
    # 主要階段時間
    total_request_time: float = 0.0
    router_processing_time: float = 0.0
    backend_processing_time: float = 0.0
    response_streaming_time: float = 0.0
    # 衍生指標
    ttft: float = 0.0
    decode_time: float = 0.0
    
    # 詳細步驟時間
    request_parsing_time: float = 0.0
    model_validation_time: float = 0.0
    endpoint_discovery_time: float = 0.0
    routing_decision_time: float = 0.0
    backend_connection_time: float = 0.0
    first_token_time: float = 0.0
    last_token_time: float = 0.0
    # 令你分析 lookup 效果
    matched_kvcache_tokens: int = 0
    request_tokens: int = 0
    
    # 路由器特定時間 (如果適用)
    tokenize_time: float = 0.0
    lookup_time: float = 0.0
    hash_routing_time: float = 0.0
    instance_mapping_time: float = 0.0
    find_best_matched_time: float = 0.0
    find_best_ttft_time: float = 0.0
    fallback_time: float = 0.0
    
    # 其他信息
    uncached_prefix_tokens: Optional[int] = None
    total_tokens: int = 0
    is_streaming: bool = False
    status_code: int = 200
    error_message: Optional[str] = None
    
    # 時間戳
    start_timestamp: str = ""
    end_timestamp: str = ""
    # 內部用：請求起始 epoch（秒），用於計算相對耗時
    _start_epoch: float = 0.0


class RequestTimingMonitor:
    """完整的請求時間監控器"""
    
    def __init__(self, output_dir: str = "/home/w00917303/"):
        self.output_dir = output_dir
        self.count = 0
        self.lock = threading.Lock()
        
        # 確保輸出目錄存在
        os.makedirs(output_dir, exist_ok=True)
        
        # CSV 文件路徑
        self.csv_file = os.path.join(output_dir, "request_timing_data.csv")
        self.simple_csv_file = os.path.join(output_dir, "request_timing_simple.csv")
        
        # 初始化 CSV 文件（如果不存在）
        self._init_csv_file()
    
    def _init_csv_file(self):
        """初始化 CSV 文件，如果不存在則創建標題行"""
        if not os.path.exists(self.csv_file):
            with open(self.csv_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                # 寫入標題行（完整）
                sample_data = RequestTimingData(
                    request_id="", endpoint="", model="", session_id="", server_url=""
                )
                writer.writerow(sample_data.__dict__.keys())
        if not os.path.exists(self.simple_csv_file):
            with open(self.simple_csv_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                # 簡化輸出標題
                writer.writerow([
                    "request_id",
                    "endpoint",
                    "routing_logic",
                    "total_request_time",
                    "ttft",
                    "decode_time",
                    "routing_decision_time",
                    "backend_connection_time",
                    "lookup_time",
                    "matched_kvcache_tokens",
                    "request_tokens",
                    "status_code",
                    "start_timestamp",
                    "end_timestamp",
                ])
    
    def start_request(self, request_id: str, endpoint: str, model: str = "", 
                     session_id: str = None) -> RequestTimingData:
        """開始追蹤一個新請求"""
        timing_data = RequestTimingData(
            request_id=request_id,
            endpoint=endpoint,
            model=model,
            session_id=session_id,
            server_url="",
            start_timestamp=datetime.now().isoformat(),
            _start_epoch=time.time(),
        )
        return timing_data
    
    def record_router_processing_start(self, timing_data: RequestTimingData):
        """記錄路由器處理開始時間"""
        timing_data.router_processing_time = time.time()
    
    def record_router_processing_end(self, timing_data: RequestTimingData):
        """記錄路由器處理結束時間"""
        if timing_data.router_processing_time > 0:
            timing_data.router_processing_time = time.time() - timing_data.router_processing_time
    
    def record_backend_processing_start(self, timing_data: RequestTimingData):
        """記錄後端處理開始時間"""
        timing_data.backend_processing_time = time.time()
    
    def record_backend_processing_end(self, timing_data: RequestTimingData):
        """記錄後端處理結束時間"""
        if timing_data.backend_processing_time > 0:
            timing_data.backend_processing_time = time.time() - timing_data.backend_processing_time
    
    def record_response_streaming_start(self, timing_data: RequestTimingData):
        """記錄響應流開始時間"""
        if timing_data._start_epoch:
            timing_data.response_streaming_time = time.time() - timing_data._start_epoch
    
    def record_response_streaming_end(self, timing_data: RequestTimingData):
        """記錄響應流結束時間"""
        if timing_data._start_epoch and timing_data.response_streaming_time >= 0:
            # 以相對請求開始的耗時來表示 decode 結束，再由 complete 計 decode_time 差值
            timing_data.response_streaming_time = time.time() - timing_data._start_epoch
    
    def record_first_token(self, timing_data: RequestTimingData):
        """記錄第一個 token 時間"""
        # 保存相對於請求開始的耗時（秒）
        if timing_data._start_epoch:
            timing_data.first_token_time = time.time() - timing_data._start_epoch
    
    def record_last_token(self, timing_data: RequestTimingData):
        """記錄最後一個 token 時間"""
        # 保存相對於請求開始的耗時（秒）
        if timing_data._start_epoch:
            timing_data.last_token_time = time.time() - timing_data._start_epoch
    
    def record_step_time(self, timing_data: RequestTimingData, step_name: str, duration: float):
        """記錄特定步驟的時間"""
        if hasattr(timing_data, step_name):
            setattr(timing_data, step_name, duration)
    
    def update_router_timing(self, timing_data: RequestTimingData, router_timing: Dict[str, float]):
        """更新路由器特定的時間數據"""
        for key, value in router_timing.items():
            if hasattr(timing_data, key):
                setattr(timing_data, key, value)
    
    def complete_request(self, timing_data: RequestTimingData, server_url: str = "", 
                        status_code: int = 200, error_message: str = None):
        """完成請求追蹤並保存數據"""
        timing_data.server_url = server_url
        timing_data.status_code = status_code
        timing_data.error_message = error_message
        timing_data.end_timestamp = datetime.now().isoformat()
        
        # 計算總請求時間
        if timing_data.start_timestamp and timing_data.end_timestamp:
            start_dt = datetime.fromisoformat(timing_data.start_timestamp)
            end_dt = datetime.fromisoformat(timing_data.end_timestamp)
            timing_data.total_request_time = (end_dt - start_dt).total_seconds()
        # 設定 TTFT（從請求開始到第一個 token 的時間）
        if timing_data.first_token_time and timing_data.first_token_time > 0:
            timing_data.ttft = timing_data.first_token_time
        
        # 設定 decode_time（從第一個 token 到最後一個 token 的時間）
        if timing_data.last_token_time and timing_data.first_token_time and timing_data.last_token_time > timing_data.first_token_time:
            timing_data.decode_time = timing_data.last_token_time - timing_data.first_token_time
        elif timing_data.response_streaming_time and timing_data.first_token_time:
            # 如果沒有 last_token_time，用 response_streaming_time 減去 first_token_time
            timing_data.decode_time = timing_data.response_streaming_time - timing_data.first_token_time
        
        # 保存到 CSV（完整 + 簡化）
        self._save_to_csv(timing_data)
        self._save_simple_csv(timing_data)
    
    def _save_to_csv(self, timing_data: RequestTimingData):
        """保存時間數據到 CSV 文件"""
        with self.lock:
            self.count += 1
            
            # 每 100 個請求保存一次，避免頻繁寫入
            if self.count % 100 == 0 or self.count <= 100:
                with open(self.csv_file, 'a', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow(asdict(timing_data).values())

    def _save_simple_csv(self, timing_data: RequestTimingData):
        """保存簡化時間數據到簡化 CSV 文件"""
        with self.lock:
            # 每 100 個請求保存一次，避免頻繁寫入
            if self.count % 100 == 0 or self.count <= 100:
                # 若檔案存在但為空（或剛被清空），先寫入表頭
                need_header = (not os.path.exists(self.simple_csv_file)) or (os.path.getsize(self.simple_csv_file) == 0)
                with open(self.simple_csv_file, 'a', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    if need_header:
                    writer.writerow([
                        "request_id",
                        "endpoint",
                        "routing_logic",
                        "total_request_time",
                        "ttft",
                        "decode_time",
                        "routing_decision_time",
                        "backend_connection_time",
                        "lookup_time",
                        "tokenize_time",
                        "hash_routing_time",
                        "instance_mapping_time",
                        "find_best_matched_time",
                        "find_best_ttft_time",
                        "fallback_time",
                        "matched_kvcache_tokens",
                        "request_tokens",
                        "status_code",
                        "start_timestamp",
                        "end_timestamp",
                    ])
                    writer.writerow([
                        timing_data.request_id,
                        timing_data.endpoint,
                        timing_data.routing_logic,
                        f"{timing_data.total_request_time:.6f}",
                        f"{timing_data.ttft:.6f}",
                        f"{timing_data.decode_time:.6f}",
                        f"{timing_data.routing_decision_time:.6f}",
                        f"{timing_data.backend_connection_time:.6f}",
                        f"{timing_data.lookup_time:.6f}",
                        f"{timing_data.tokenize_time:.6f}",
                        f"{timing_data.hash_routing_time:.6f}",
                        f"{timing_data.instance_mapping_time:.6f}",
                        f"{timing_data.find_best_matched_time:.6f}",
                        f"{timing_data.find_best_ttft_time:.6f}",
                        f"{timing_data.fallback_time:.6f}",
                        timing_data.matched_kvcache_tokens,
                        timing_data.request_tokens,
                        timing_data.status_code,
                        timing_data.start_timestamp,
                        timing_data.end_timestamp,
                    ])
    
    def get_stats(self) -> Dict[str, float]:
        """獲取統計信息"""
        # 這裡可以添加讀取 CSV 文件並計算統計信息的邏輯
        return {
            "total_requests": self.count,
            "csv_file": self.csv_file
        }


# 全局實例
_request_timing_monitor = None


def get_request_timing_monitor() -> RequestTimingMonitor:
    """獲取全局請求時間監控器實例"""
    global _request_timing_monitor
    if _request_timing_monitor is None:
        _request_timing_monitor = RequestTimingMonitor()
    return _request_timing_monitor
