# 按請求 CSV 監控指南

本文檔說明如何使用 production-stack 中的請求時間監控功能，將每個請求的詳細指標保存到 CSV 文件中。

## 概述

production-stack 提供了完整的請求時間監控系統，包括：

- **RequestTimingData**: 完整的請求時間數據結構
- **RequestTimingMonitor**: 請求時間監控器
- **RealtimeCSVExporter**: 實時 CSV 導出器
- **CSVAnalyzer**: CSV 數據分析工具

## 功能特性

### 1. 完整的請求時間追蹤

每個請求都會記錄以下時間指標：

- **總請求時間** (`total_request_time`)
- **TTFT** (`ttft`) - Time to First Token
- **解碼時間** (`decode_time`)
- **路由決策時間** (`routing_decision_time`)
- **後端連接時間** (`backend_connection_time`)
- **查找時間** (`lookup_time`)

### 2. KV Cache 傳輸監控

新增的 KV cache 相關指標：

- **KV Cache 傳輸時間** (`kv_cache_transfer_time`)
- **傳輸類型** (`kv_cache_transfer_type`) - send/receive/p2p
- **傳輸大小** (`kv_cache_transfer_size_bytes`)
- **傳輸吞吐量** (`kv_cache_transfer_throughput_gbps`)
- **傳輸次數** (`kv_cache_transfer_count`)
- **查找時間** (`kv_cache_lookup_time`)
- **命中狀態** (`kv_cache_hit`/`kv_cache_miss`)

### 3. 實時 CSV 導出

- 按時間間隔自動導出
- 按記錄數量觸發導出
- 支持壓縮存儲
- 實時監控和回調

## 使用方法

### 1. 基本使用

```python
from vllm_router.monitoring.request_timing import get_request_timing_monitor

# 獲取監控器
monitor = get_request_timing_monitor()

# 開始請求追蹤
timing_data = monitor.start_request(
    request_id="req_001",
    endpoint="/v1/chat/completions",
    model="llama-2-7b-chat",
    session_id="session_123"
)

# 記錄各個階段
monitor.record_router_processing_start(timing_data)
# ... 執行路由器處理 ...
monitor.record_router_processing_end(timing_data)

monitor.record_backend_processing_start(timing_data)
# ... 執行後端處理 ...
monitor.record_backend_processing_end(timing_data)

# 記錄 token 時間
monitor.record_first_token(timing_data)
# ... 解碼過程 ...
monitor.record_last_token(timing_data)

# 記錄 KV cache 傳輸
monitor.record_kv_cache_transfer(
    timing_data, 
    transfer_time=0.05,  # 50ms
    transfer_type="send",
    transfer_size_bytes=1024*1024*10,  # 10MB
    throughput_gbps=2.0
)

# 完成請求
monitor.complete_request(
    timing_data=timing_data,
    server_url="http://backend-1:8000",
    status_code=200
)
```

### 2. 實時 CSV 導出

```python
from vllm_router.monitoring.realtime_csv_exporter import setup_realtime_csv_export

# 設置實時導出
exporter = setup_realtime_csv_export(
    export_interval=60,  # 60秒導出一次
    max_records_per_file=1000  # 每個文件最多1000條記錄
)

# 添加記錄
exporter.add_record(timing_data)

# 停止導出
exporter.stop_exporting()
```

### 3. CSV 數據分析

```python
from vllm_router.monitoring.csv_analyzer import RequestTimingAnalyzer

# 創建分析器
analyzer = RequestTimingAnalyzer("request_timing_data.csv")

# 獲取基本統計
basic_stats = analyzer.get_basic_stats()
print("基本統計:", basic_stats)

# 獲取 KV cache 分析
kv_stats = analyzer.get_kv_cache_analysis()
print("KV Cache 分析:", kv_stats)

# 生成圖表
analyzer.plot_time_distribution("time_distribution.png")
analyzer.plot_kv_cache_analysis("kv_cache_analysis.png")

# 導出報告
analyzer.export_summary_report("analysis_report.json")
```

## CSV 文件格式

### 完整 CSV (`request_timing_data.csv`)

包含所有字段的完整數據：

```csv
request_id,endpoint,model,session_id,server_url,routing_logic,total_request_time,ttft,decode_time,routing_decision_time,backend_connection_time,lookup_time,matched_kvcache_tokens,request_tokens,kv_cache_transfer_time,kv_cache_transfer_type,kv_cache_transfer_size_bytes,kv_cache_transfer_throughput_gbps,kv_cache_transfer_count,kv_cache_lookup_time,kv_cache_hit,kv_cache_miss,status_code,start_timestamp,end_timestamp
req_001,/v1/chat/completions,llama-2-7b-chat,session_123,http://backend-1:8000,hash_routing,1.2345678901,0.1234567890,0.5678901234,0.0123456789,0.0234567890,0.0012345678,100,50,0.0500000000,send,10485760,2.0000000000,1,0.0010000000,True,False,200,2024-01-15T10:30:00.123456,2024-01-15T10:30:01.357890
```

### 簡化 CSV (`request_timing_simple.csv`)

只包含關鍵字段的簡化數據：

```csv
request_id,endpoint,routing_logic,total_request_time,ttft,decode_time,routing_decision_time,backend_connection_time,lookup_time,matched_kvcache_tokens,request_tokens,kv_cache_transfer_time,kv_cache_transfer_type,kv_cache_transfer_size_bytes,kv_cache_transfer_throughput_gbps,kv_cache_transfer_count,kv_cache_lookup_time,kv_cache_hit,kv_cache_miss,status_code,start_timestamp,end_timestamp
req_001,/v1/chat/completions,hash_routing,1.2345678901,0.1234567890,0.5678901234,0.0123456789,0.0234567890,0.0012345678,100,50,0.0500000000,send,10485760,2.0000000000,1,0.0010000000,True,False,200,2024-01-15T10:30:00.123456,2024-01-15T10:30:01.357890
```

## 命令行工具

### 1. CSV 分析器

```bash
# 基本分析
python -m vllm_router.monitoring.csv_analyzer request_timing_data.csv

# 指定輸出目錄
python -m vllm_router.monitoring.csv_analyzer request_timing_data.csv --output-dir ./analysis_results

# 按時間範圍過濾
python -m vllm_router.monitoring.csv_analyzer request_timing_data.csv \
    --start-time "2024-01-15 10:00:00" \
    --end-time "2024-01-15 11:00:00"

# 按路由邏輯過濾
python -m vllm_router.monitoring.csv_analyzer request_timing_data.csv \
    --routing-logic hash_routing round_robin
```

### 2. 集成監控示例

```bash
# 運行集成監控示例
python examples/integrated_request_monitoring_example.py
```

## 配置選項

### 1. RequestTimingMonitor 配置

```python
# 自定義輸出目錄
monitor = RequestTimingMonitor(output_dir="/custom/output/path")
```

### 2. RealtimeCSVExporter 配置

```python
exporter = RealtimeCSVExporter(
    output_dir="/tmp/csv_exports",
    export_interval=30,  # 30秒導出一次
    max_records_per_file=500,  # 每個文件500條記錄
    enable_compression=True  # 啟用壓縮
)
```

### 3. CSVAnalyzer 配置

```python
analyzer = RequestTimingAnalyzer("data.csv")

# 按時間範圍過濾
analyzer.filter_by_time_range("2024-01-15 10:00:00", "2024-01-15 11:00:00")

# 按路由邏輯過濾
analyzer.filter_by_routing_logic(["hash_routing", "round_robin"])
```

## 監控指標說明

### 時間指標

- **total_request_time**: 從請求開始到結束的總時間
- **ttft**: 從請求開始到第一個 token 的時間
- **decode_time**: 從第一個 token 到最後一個 token 的時間
- **routing_decision_time**: 路由決策所花費的時間
- **backend_connection_time**: 連接到後端所花費的時間
- **lookup_time**: KV cache 查找時間

### KV Cache 指標

- **kv_cache_transfer_time**: KV cache 傳輸總時間
- **kv_cache_transfer_type**: 傳輸類型（send/receive/p2p）
- **kv_cache_transfer_size_bytes**: 傳輸的數據大小（字節）
- **kv_cache_transfer_throughput_gbps**: 傳輸吞吐量（GB/s）
- **kv_cache_transfer_count**: 傳輸次數
- **kv_cache_lookup_time**: KV cache 查找時間
- **kv_cache_hit**: 是否命中 KV cache
- **kv_cache_miss**: 是否未命中 KV cache

## 最佳實踐

### 1. 性能優化

- 使用異步操作避免阻塞
- 合理設置導出間隔和文件大小
- 啟用壓縮以節省存儲空間

### 2. 數據管理

- 定期清理舊的 CSV 文件
- 使用時間範圍過濾進行分析
- 備份重要的監控數據

### 3. 監控設置

- 設置適當的警報閾值
- 監控 CSV 文件大小和導出頻率
- 定期檢查數據質量

## 故障排除

### 1. CSV 文件未生成

檢查：
- 輸出目錄權限
- 磁盤空間
- 監控器是否正確初始化

### 2. 數據不完整

檢查：
- 請求是否正確完成
- 時間記錄是否正確調用
- 異常處理是否正確

### 3. 性能問題

優化：
- 調整導出間隔
- 減少記錄字段
- 使用壓縮存儲

## 相關文檔

- [KV Cache 傳輸監控](./kv-cache-transfer-monitoring.md)
- [Prometheus 監控指南](../observability/README.md)
- [vLLM Production Stack 文檔](../README.md)

