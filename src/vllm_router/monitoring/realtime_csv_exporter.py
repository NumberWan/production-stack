#!/usr/bin/env python3
"""
實時 CSV 導出器

提供實時監控和導出請求時間數據到 CSV 的功能，
支持按時間間隔、請求數量等條件觸發導出。
"""

import os
import time
import threading
import csv
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Callable
from dataclasses import asdict
import json
import pandas as pd

from .request_timing import RequestTimingData, RequestTimingMonitor


class RealtimeCSVExporter:
    """實時 CSV 導出器"""
    
    def __init__(self, 
                 output_dir: str = "/tmp/realtime_csv",
                 export_interval: int = 60,  # 秒
                 max_records_per_file: int = 1000,
                 enable_compression: bool = True):
        """
        初始化實時導出器
        
        Args:
            output_dir: 輸出目錄
            export_interval: 導出間隔（秒）
            max_records_per_file: 每個文件最大記錄數
            enable_compression: 是否啟用壓縮
        """
        self.output_dir = output_dir
        self.export_interval = export_interval
        self.max_records_per_file = max_records_per_file
        self.enable_compression = enable_compression
        
        # 確保輸出目錄存在
        os.makedirs(output_dir, exist_ok=True)
        
        # 緩衝區
        self.buffer: List[RequestTimingData] = []
        self.buffer_lock = threading.Lock()
        
        # 導出狀態
        self.is_exporting = False
        self.export_thread = None
        self.stop_event = threading.Event()
        
        # 統計信息
        self.total_exported = 0
        self.last_export_time = None
        
        # 回調函數
        self.export_callbacks: List[Callable] = []
    
    def add_export_callback(self, callback: Callable[[str, int], None]):
        """
        添加導出回調函數
        
        Args:
            callback: 回調函數，參數為 (file_path, record_count)
        """
        self.export_callbacks.append(callback)
    
    def add_record(self, timing_data: RequestTimingData):
        """
        添加記錄到緩衝區
        
        Args:
            timing_data: 請求時間數據
        """
        with self.buffer_lock:
            self.buffer.append(timing_data)
            
            # 檢查是否需要立即導出
            if len(self.buffer) >= self.max_records_per_file:
                self._trigger_export()
    
    def start_exporting(self):
        """開始實時導出"""
        if self.is_exporting:
            return
        
        self.is_exporting = True
        self.stop_event.clear()
        self.export_thread = threading.Thread(target=self._export_loop, daemon=True)
        self.export_thread.start()
        print(f"實時 CSV 導出已啟動，間隔: {self.export_interval}秒")
    
    def stop_exporting(self):
        """停止實時導出"""
        if not self.is_exporting:
            return
        
        self.is_exporting = False
        self.stop_event.set()
        
        if self.export_thread:
            self.export_thread.join(timeout=5)
        
        # 導出剩餘的緩衝區數據
        self._trigger_export()
        print("實時 CSV 導出已停止")
    
    def _export_loop(self):
        """導出循環"""
        while not self.stop_event.is_set():
            self.stop_event.wait(self.export_interval)
            
            if not self.stop_event.is_set():
                self._trigger_export()
    
    def _trigger_export(self):
        """觸發導出"""
        with self.buffer_lock:
            if not self.buffer:
                return
            
            # 創建導出文件
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            file_path = os.path.join(self.output_dir, f"request_timing_{timestamp}.csv")
            
            # 導出數據
            record_count = self._export_to_csv(file_path, self.buffer.copy())
            
            # 清空緩衝區
            self.buffer.clear()
            
            # 更新統計
            self.total_exported += record_count
            self.last_export_time = datetime.now()
            
            # 觸發回調
            for callback in self.export_callbacks:
                try:
                    callback(file_path, record_count)
                except Exception as e:
                    print(f"回調函數執行失敗: {e}")
            
            print(f"導出完成: {file_path} ({record_count} 條記錄)")
    
    def _export_to_csv(self, file_path: str, records: List[RequestTimingData]) -> int:
        """
        導出記錄到 CSV 文件
        
        Args:
            file_path: 輸出文件路徑
            records: 記錄列表
            
        Returns:
            導出的記錄數
        """
        if not records:
            return 0
        
        # 創建 DataFrame
        data = [asdict(record) for record in records]
        df = pd.DataFrame(data)
        
        # 導出到 CSV
        df.to_csv(file_path, index=False, encoding='utf-8')
        
        # 如果啟用壓縮，創建壓縮版本
        if self.enable_compression:
            compressed_path = file_path + '.gz'
            df.to_csv(compressed_path, index=False, encoding='utf-8', compression='gzip')
            os.remove(file_path)  # 刪除未壓縮文件
            file_path = compressed_path
        
        return len(records)
    
    def get_stats(self) -> Dict:
        """獲取統計信息"""
        with self.buffer_lock:
            return {
                "is_exporting": self.is_exporting,
                "buffer_size": len(self.buffer),
                "total_exported": self.total_exported,
                "last_export_time": self.last_export_time.isoformat() if self.last_export_time else None,
                "export_interval": self.export_interval,
                "max_records_per_file": self.max_records_per_file,
            }
    
    def export_current_buffer(self) -> str:
        """立即導出當前緩衝區"""
        with self.buffer_lock:
            if not self.buffer:
                return None
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            file_path = os.path.join(self.output_dir, f"request_timing_manual_{timestamp}.csv")
            
            record_count = self._export_to_csv(file_path, self.buffer.copy())
            self.buffer.clear()
            
            return file_path


class CSVExporterManager:
    """CSV 導出器管理器"""
    
    def __init__(self, base_output_dir: str = "/tmp/csv_exports"):
        self.base_output_dir = base_output_dir
        self.exporters: Dict[str, RealtimeCSVExporter] = {}
        self.monitor = RequestTimingMonitor()
    
    def create_exporter(self, 
                       name: str,
                       export_interval: int = 60,
                       max_records_per_file: int = 1000) -> RealtimeCSVExporter:
        """
        創建導出器
        
        Args:
            name: 導出器名稱
            export_interval: 導出間隔
            max_records_per_file: 每個文件最大記錄數
            
        Returns:
            導出器實例
        """
        output_dir = os.path.join(self.base_output_dir, name)
        exporter = RealtimeCSVExporter(
            output_dir=output_dir,
            export_interval=export_interval,
            max_records_per_file=max_records_per_file
        )
        
        self.exporters[name] = exporter
        return exporter
    
    def get_exporter(self, name: str) -> Optional[RealtimeCSVExporter]:
        """獲取導出器"""
        return self.exporters.get(name)
    
    def start_all_exporters(self):
        """啟動所有導出器"""
        for name, exporter in self.exporters.items():
            exporter.start_exporting()
            print(f"導出器 '{name}' 已啟動")
    
    def stop_all_exporters(self):
        """停止所有導出器"""
        for name, exporter in self.exporters.items():
            exporter.stop_exporting()
            print(f"導出器 '{name}' 已停止")
    
    def get_all_stats(self) -> Dict:
        """獲取所有導出器統計"""
        stats = {}
        for name, exporter in self.exporters.items():
            stats[name] = exporter.get_stats()
        return stats


# 全局管理器實例
_csv_exporter_manager = None

def get_csv_exporter_manager() -> CSVExporterManager:
    """獲取全局 CSV 導出器管理器"""
    global _csv_exporter_manager
    if _csv_exporter_manager is None:
        _csv_exporter_manager = CSVExporterManager()
    return _csv_exporter_manager


def setup_realtime_csv_export(export_interval: int = 60, 
                             max_records_per_file: int = 1000) -> RealtimeCSVExporter:
    """
    設置實時 CSV 導出
    
    Args:
        export_interval: 導出間隔（秒）
        max_records_per_file: 每個文件最大記錄數
        
    Returns:
        導出器實例
    """
    manager = get_csv_exporter_manager()
    exporter = manager.create_exporter(
        name="default",
        export_interval=export_interval,
        max_records_per_file=max_records_per_file
    )
    
    # 添加導出回調
    def export_callback(file_path: str, record_count: int):
        print(f"CSV 導出完成: {file_path} ({record_count} 條記錄)")
    
    exporter.add_export_callback(export_callback)
    exporter.start_exporting()
    
    return exporter


if __name__ == "__main__":
    # 示例使用
    exporter = setup_realtime_csv_export(export_interval=30, max_records_per_file=500)
    
    try:
        # 模擬添加一些記錄
        for i in range(10):
            timing_data = RequestTimingData(
                request_id=f"req_{i}",
                endpoint="/v1/chat/completions",
                model="test-model",
                total_request_time=1.0 + i * 0.1,
                ttft=0.5 + i * 0.05,
                decode_time=0.5 + i * 0.05,
            )
            exporter.add_record(timing_data)
            time.sleep(1)
        
        # 等待一段時間
        time.sleep(60)
        
    finally:
        exporter.stop_exporting()
        print("示例完成")
