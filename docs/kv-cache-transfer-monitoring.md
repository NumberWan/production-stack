# KV Cache Transfer Monitoring

本文檔說明如何在 vLLM Production Stack 中監控跨實例 KV cache 傳輸時間。

## 概述

當使用 LMCache 進行 KV cache 共享時，系統需要在不同的實例之間傳輸已計算好的 KV cache。這個功能提供了詳細的監控指標來追蹤這些傳輸操作的時間和性能。

## 監控指標

系統提供以下 Prometheus 指標：

### 1. `vllm:kv_cache_transfer_time_ms`
- **描述**: 跨實例傳輸 KV cache 所花費的時間（毫秒）
- **標籤**: 
  - `server`: 實例標識符
  - `transfer_type`: 傳輸類型（send, receive, p2p）
- **用途**: 監控傳輸延遲

### 2. `vllm:kv_cache_transfer_count_total`
- **描述**: KV cache 傳輸的總次數
- **標籤**: 
  - `server`: 實例標識符
  - `transfer_type`: 傳輸類型（send, receive, p2p）
- **用途**: 統計傳輸頻率

### 3. `vllm:kv_cache_transfer_throughput_gbps`
- **描述**: KV cache 傳輸的吞吐量（GB/s）
- **標籤**: 
  - `server`: 實例標識符
  - `transfer_type`: 傳輸類型（send, receive, p2p）
- **用途**: 監控傳輸效率

### 4. `vllm:kv_cache_transfer_size_bytes`
- **描述**: 傳輸的 KV cache 總大小（字節）
- **標籤**: 
  - `server`: 實例標識符
  - `transfer_type`: 傳輸類型（send, receive, p2p）
- **用途**: 監控傳輸數據量

## 傳輸類型

- **send**: 向另一個實例發送 KV cache
- **receive**: 從另一個實例接收 KV cache
- **p2p**: 點對點 KV cache 共享

## 使用方法

### 1. 啟用監控

在您的 vLLM 配置中啟用 LMCache 和 KV transfer 監控：

```yaml
# values.yaml
vllm:
  lmcache:
    enabled: true
    config: |
      {
        "kv_connector": "LMCacheConnectorV1",
        "kv_role": "kv_both"
      }
```

### 2. 查看指標

訪問 Prometheus 指標端點：

```bash
curl http://your-router:8000/metrics | grep kv_cache_transfer
```

### 3. Grafana 儀表板

使用提供的 Grafana 儀表板配置來可視化指標：

```bash
# 導入儀表板
kubectl apply -f observability/kv-cache-transfer-dashboard.json
```

### 4. 程式化使用

```python
from vllm_router.services.kv_transfer_monitor import get_kv_transfer_monitor

# 獲取監控實例
monitor = get_kv_transfer_monitor()

# 開始監控傳輸
monitor.start_transfer(
    transfer_id="transfer_001",
    transfer_type="send",
    size_bytes=1024*1024*100,  # 100 MB
    source_instance="instance_1",
    target_instance="instance_2"
)

# 執行傳輸操作
# ... 實際的傳輸代碼 ...

# 結束監控
transfer_stats = monitor.end_transfer("transfer_001")

# 查看統計信息
print(f"傳輸時間: {transfer_stats.duration_ms:.2f}ms")
print(f"吞吐量: {transfer_stats.throughput_gbps:.2f} GB/s")
```

## 示例

運行提供的示例來測試監控功能：

```bash
cd production-stack/examples
python kv_transfer_monitoring_example.py
```

## 故障排除

### 1. 指標未顯示

檢查以下項目：
- LMCache 是否正確配置
- KV transfer 配置是否正確
- 監控服務是否正常運行

### 2. 傳輸時間異常

可能的原因：
- 網絡延遲
- 實例間帶寬限制
- KV cache 大小過大
- 系統資源不足

### 3. 吞吐量低

優化建議：
- 檢查網絡配置
- 調整 LMCache 緩衝區大小
- 優化序列化/反序列化過程
- 考慮使用更快的存儲後端

## 配置選項

### LMCache 配置

```yaml
lmcache:
  local_cpu: true
  max_local_cpu_size: "8GB"
  local_disk: true
  max_local_disk_size: "32GB"
  remote_url: "redis://your-redis:6379"
  remote_serde: "pickle"
```

### 監控配置

```python
# 清理舊的傳輸記錄（默認1小時）
monitor.clear_old_transfers(max_age_seconds=3600)

# 獲取聚合統計
stats = monitor.get_aggregated_stats()
print(f"總傳輸次數: {stats['total_transfers']}")
print(f"平均傳輸時間: {stats['avg_transfer_time_ms']:.2f}ms")
```

## 最佳實踐

1. **定期清理**: 定期清理舊的傳輸記錄以防止內存洩漏
2. **監控趨勢**: 關注傳輸時間和吞吐量的趨勢變化
3. **設置警報**: 為異常的傳輸時間設置警報
4. **優化配置**: 根據監控數據調整 LMCache 配置
5. **容量規劃**: 根據傳輸統計進行容量規劃

## 相關文檔

- [LMCache 文檔](https://lmcache.ai/)
- [vLLM Production Stack 文檔](../README.md)
- [Prometheus 監控指南](../observability/README.md)
