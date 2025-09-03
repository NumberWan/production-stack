# TTFT KVAWARE 簡單性能監控

## 概述

簡單修改了 `src/vllm_router/routers/routing_logic.py` 文件中的 `TtftRouter` 類，為 `ttft kvaware` 方法添加了基本的性能監控功能。

## 修改內容

### 1. 添加導入模塊
```python
import time
import csv
import os
```

### 2. 添加監控變量
```python
def __init__(self, ...):
    # 添加性能監控變量
    self.count = 0
    self.csv_file_path = "/home/w00917303/test_result.csv"
```

### 3. 添加 CSV 保存方法
```python
def _save_to_csv(self, timing_data):
    """保存性能數據到 CSV 文件"""
    try:
        os.makedirs(os.path.dirname(self.csv_file_path), exist_ok=True)
        
        with open(self.csv_file_path, 'a', newline='', encoding='utf-8') as csvfile:
            if self.count == 101:  # 第一次寫入標題
                csvfile.write('count,total_time,tokenize_time,lookup_time,find_best_matched_time,find_best_ttft_time,fallback_time\n')
            
            csvfile.write(f"{timing_data['count']},{timing_data['total_time']:.6f},"
                        f"{timing_data['tokenize_time']:.6f},{timing_data['lookup_time']:.6f},"
                        f"{timing_data['find_best_matched_time']:.6f},{timing_data['find_best_ttft_time']:.6f},"
                        f"{timing_data['fallback_time']:.6f}\n")
    except Exception as e:
        logger.error(f"Failed to save performance data: {e}")
```

### 4. 修改 route_request 方法
- 每次調用時 `self.count` 自增
- 測量每個主要步驟的執行時間
- 當 `count > 100` 時，保存到 CSV 文件

## CSV 文件格式
| 列名 | 描述 |
|------|------|
| count | 請求計數器 |
| total_time | 總執行時間（秒） |
| tokenize_time | Tokenize 步驟時間（秒） |
| lookup_time | Lookup 步驟時間（秒） |
| find_best_matched_time | 尋找最佳匹配時間（秒） |
| find_best_ttft_time | 尋找最佳 TTFT 時間（秒） |
| fallback_time | 回退路由時間（秒） |

## 文件保存位置
`/home/w00917303/test_result.csv`

---

*修改完成時間: 2025年2月9日*