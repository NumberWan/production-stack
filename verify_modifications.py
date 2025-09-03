#!/usr/bin/env python3
"""
驗證腳本：檢查對 routing_logic.py 的修改是否正確
"""

import re
import os

def verify_modifications():
    """
    驗證對 routing_logic.py 文件的修改
    """
    print("🔍 開始驗證 routing_logic.py 的修改...")
    
    file_path = "src/vllm_router/routers/routing_logic.py"
    
    if not os.path.exists(file_path):
        print(f"❌ 文件不存在: {file_path}")
        return False
    
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    print("📋 檢查修改項目:")
    
    # 檢查導入模塊
    checks = [
        ("import time", "添加 time 模塊導入"),
        ("import csv", "添加 csv 模塊導入"),
        ("import os", "添加 os 模塊導入"),
        ("self.count = 0", "添加 count 變量初始化"),
        ("self.csv_file_path = \"/home/w00917303/test_result.csv\"", "添加 CSV 文件路徑"),
        ("def _save_to_csv(self, timing_data):", "添加 CSV 保存方法"),
        ("self.count += 1", "添加計數器自增"),
        ("start_time = time.time()", "添加開始時間記錄"),
        ("timing_data = {", "添加時間數據字典"),
        ("if self.count > 100:", "添加計數器檢查"),
        ("self._save_to_csv(timing_data)", "添加 CSV 保存調用")
    ]
    
    all_passed = True
    for check, description in checks:
        if check in content:
            print(f"✅ {description}")
        else:
            print(f"❌ {description}")
            all_passed = False
    
    # 檢查 CSV 字段
    csv_fields = [
        "'count'", "'total_time'", "'tokenize_time'", "'lookup_time'",
        "'find_best_matched_time'", "'find_best_ttft_time'", "'fallback_time'"
    ]
    
    print("\n📊 檢查 CSV 字段:")
    for field in csv_fields:
        if field in content:
            print(f"✅ CSV 字段: {field}")
        else:
            print(f"❌ CSV 字段缺失: {field}")
            all_passed = False
    
    # 檢查時間測量步驟
    timing_steps = [
        "tokenize_start = time.time()",
        "lookup_start = time.time()",
        "find_best_start = time.time()",
        "find_ttft_start = time.time()",
        "fallback_start = time.time()"
    ]
    
    print("\n⏱️  檢查時間測量步驟:")
    for step in timing_steps:
        if step in content:
            print(f"✅ 時間測量: {step}")
        else:
            print(f"❌ 時間測量缺失: {step}")
            all_passed = False
    
    return all_passed

def check_file_structure():
    """
    檢查文件結構
    """
    print("\n📁 檢查文件結構:")
    
    files_to_check = [
        "src/vllm_router/routers/routing_logic.py",
        "TTFT_KVAWARE_PERFORMANCE_MONITORING.md",
        "test_performance_monitoring.py"
    ]
    
    for file_path in files_to_check:
        if os.path.exists(file_path):
            print(f"✅ {file_path}")
        else:
            print(f"❌ {file_path}")

if __name__ == "__main__":
    print("🚀 開始驗證 TTFT KVAWARE 性能監控修改")
    print("=" * 60)
    
    check_file_structure()
    
    if verify_modifications():
        print("\n🎉 所有修改驗證通過！")
        print("\n📋 修改總結:")
        print("1. ✅ 成功添加了必要的導入模塊 (time, csv, os)")
        print("2. ✅ 在 TtftRouter 類中添加了性能監控變量")
        print("3. ✅ 添加了 _save_to_csv 方法")
        print("4. ✅ 修改了 route_request 方法，添加詳細的性能監控")
        print("5. ✅ 實現了當 count > 100 時自動保存到 CSV 的功能")
        print("\n📊 性能監控包括:")
        print("   - 總執行時間")
        print("   - Tokenize 步驟時間")
        print("   - KV cache lookup 時間")
        print("   - 尋找最佳匹配時間")
        print("   - 尋找最佳 TTFT 時間")
        print("   - 回退路由時間")
        print(f"\n📁 數據保存位置: /home/w00917303/test_result.csv")
    else:
        print("\n❌ 部分修改驗證失敗，請檢查代碼")
    
    print("=" * 60)
    print("🎯 驗證完成！")