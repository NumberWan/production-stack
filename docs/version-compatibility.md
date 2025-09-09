# 版本兼容性說明

## LMCache 版本需求

### 需要的 LMCache 版本

要使用 P2P KV cache 傳輸監控功能，您需要：

1. **LMCache 版本**: `full_lookup_test` 分支或更新版本
2. **Production Stack 版本**: `p2p-monitoring` 分支

### 版本對應關係

| Production Stack 分支 | LMCache 分支 | 功能支持 |
|----------------------|--------------|----------|
| `p2p-monitoring` | `p2p-monitoring` | ✅ 完整 P2P 監控 |
| `p2p-monitoring` | `full_lookup_test` | ✅ 基本監控 |
| `p2p-monitoring` | `dev` | ⚠️ 部分支持 |

### 安裝步驟

#### 1. 安裝 Production Stack (p2p-monitoring 分支)

```bash
git clone https://github.com/NumberWan/production-stack.git
cd production-stack
git checkout p2p-monitoring
pip install -e .
```

#### 2. 安裝 LMCache (p2p-monitoring 分支)

```bash
git clone https://github.com/NumberWan/LMCache.git
cd LMCache
git checkout p2p-monitoring
pip install -e .
```

### 配置要求

#### LMCache 配置

```yaml
# lmcache_config.yaml
enable_p2p: true
lookup_server_url: "http://lookup-server:8000"
distributed_url: "http://distributed-server:8001"

# 使用監控連接器
storage_backend:
  type: "monitored_nixl"  # 使用監控版本
  config:
    instance_id: "instance_1"
```

#### Production Stack 配置

```yaml
# values.yaml
vllm:
  lmcache:
    enabled: true
    config: |
      {
        "kv_connector": "LMCacheConnectorV1",
        "kv_role": "kv_both",
        "enable_p2p": true
      }
```

### 功能對應表

| 功能 | LMCache p2p-monitoring | LMCache full_lookup_test | LMCache dev |
|------|------------------------|---------------------------|-------------|
| P2P 傳輸監控 | ✅ 完整支持 | ✅ 基本支持 | ⚠️ 部分支持 |
| 自動時間追蹤 | ✅ | ✅ | ❌ |
| 實時 CSV 導出 | ✅ | ✅ | ❌ |
| Prometheus 指標 | ✅ | ✅ | ❌ |
| 傳輸類型識別 | ✅ | ✅ | ❌ |

### 故障排除

#### 如果版本不匹配會發生什麼？

1. **ImportError**: 如果 LMCache 版本太舊，會出現導入錯誤
2. **功能降級**: 監控功能會自動降級到基本模式
3. **警告日誌**: 系統會記錄版本不匹配的警告

#### 檢查版本兼容性

```python
# 檢查 LMCache 版本
import lmcache
print(f"LMCache version: {lmcache.__version__}")

# 檢查監控功能
try:
    from lmcache.v1.storage_backend.connector.monitored_nixl_connector import MonitoredNixlConnector
    print("✅ 監控功能可用")
except ImportError:
    print("❌ 監控功能不可用，請升級 LMCache")
```

### 推薦配置

**最佳實踐**：同時使用兩個 `p2p-monitoring` 分支

```bash
# 1. 克隆並切換到正確分支
git clone https://github.com/NumberWan/production-stack.git
git clone https://github.com/NumberWan/LMCache.git

cd production-stack
git checkout p2p-monitoring

cd ../LMCache  
git checkout p2p-monitoring

# 2. 安裝
cd ../production-stack
pip install -e .

cd ../LMCache
pip install -e .

# 3. 運行示例
cd ../production-stack
python examples/p2p_kv_cache_monitoring_example.py
```

這樣可以確保所有 P2P 監控功能都能正常工作。


