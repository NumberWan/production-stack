#!/usr/bin/env python3
"""
CSV 分析工具

用於分析 request_timing_data.csv 和 request_timing_simple.csv 文件，
提供統計分析和可視化功能。
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from typing import Dict, List, Optional, Tuple
import os
import argparse
from datetime import datetime, timedelta
import json


class RequestTimingAnalyzer:
    """請求時間數據分析器"""
    
    def __init__(self, csv_file_path: str):
        """
        初始化分析器
        
        Args:
            csv_file_path: CSV 文件路徑
        """
        self.csv_file_path = csv_file_path
        self.df = None
        self.load_data()
    
    def load_data(self):
        """載入 CSV 數據"""
        try:
            self.df = pd.read_csv(self.csv_file_path)
            print(f"成功載入 {len(self.df)} 條記錄")
        except Exception as e:
            print(f"載入數據失敗: {e}")
            self.df = None
    
    def get_basic_stats(self) -> Dict:
        """獲取基本統計信息"""
        if self.df is None:
            return {}
        
        stats = {
            "總請求數": len(self.df),
            "成功請求數": len(self.df[self.df['status_code'] == 200]),
            "失敗請求數": len(self.df[self.df['status_code'] != 200]),
            "平均總請求時間": self.df['total_request_time'].mean(),
            "平均 TTFT": self.df['ttft'].mean(),
            "平均解碼時間": self.df['decode_time'].mean(),
            "平均路由決策時間": self.df['routing_decision_time'].mean(),
            "平均後端連接時間": self.df['backend_connection_time'].mean(),
        }
        
        # KV Cache 相關統計
        if 'kv_cache_transfer_time' in self.df.columns:
            stats.update({
                "平均 KV Cache 傳輸時間": self.df['kv_cache_transfer_time'].mean(),
                "KV Cache 命中次數": len(self.df[self.df['kv_cache_hit'] == True]),
                "KV Cache 未命中次數": len(self.df[self.df['kv_cache_miss'] == True]),
                "平均 KV Cache 查找時間": self.df['kv_cache_lookup_time'].mean(),
                "平均 KV Cache 傳輸大小": self.df['kv_cache_transfer_size_bytes'].mean(),
                "平均 KV Cache 傳輸吞吐量": self.df['kv_cache_transfer_throughput_gbps'].mean(),
            })
        
        return stats
    
    def get_percentile_stats(self) -> Dict:
        """獲取百分位數統計"""
        if self.df is None:
            return {}
        
        percentiles = [50, 90, 95, 99]
        stats = {}
        
        time_columns = ['total_request_time', 'ttft', 'decode_time', 
                       'routing_decision_time', 'backend_connection_time']
        
        for col in time_columns:
            if col in self.df.columns:
                for p in percentiles:
                    stats[f"{col}_p{p}"] = self.df[col].quantile(p/100)
        
        # KV Cache 百分位數
        if 'kv_cache_transfer_time' in self.df.columns:
            for p in percentiles:
                stats[f"kv_cache_transfer_time_p{p}"] = self.df['kv_cache_transfer_time'].quantile(p/100)
        
        return stats
    
    def get_routing_analysis(self) -> Dict:
        """獲取路由分析"""
        if self.df is None or 'routing_logic' not in self.df.columns:
            return {}
        
        routing_stats = {}
        routing_groups = self.df.groupby('routing_logic')
        
        for logic, group in routing_groups:
            routing_stats[logic] = {
                "請求數": len(group),
                "平均總時間": group['total_request_time'].mean(),
                "平均 TTFT": group['ttft'].mean(),
                "平均解碼時間": group['decode_time'].mean(),
                "平均路由決策時間": group['routing_decision_time'].mean(),
            }
            
            if 'kv_cache_transfer_time' in group.columns:
                routing_stats[logic].update({
                    "平均 KV Cache 傳輸時間": group['kv_cache_transfer_time'].mean(),
                    "KV Cache 命中率": (group['kv_cache_hit'] == True).mean(),
                })
        
        return routing_stats
    
    def get_kv_cache_analysis(self) -> Dict:
        """獲取 KV Cache 分析"""
        if self.df is None or 'kv_cache_transfer_time' not in self.df.columns:
            return {}
        
        kv_stats = {
            "總 KV Cache 傳輸次數": self.df['kv_cache_transfer_count'].sum(),
            "平均每次請求的 KV Cache 傳輸次數": self.df['kv_cache_transfer_count'].mean(),
            "KV Cache 命中率": (self.df['kv_cache_hit'] == True).mean(),
            "KV Cache 未命中率": (self.df['kv_cache_miss'] == True).mean(),
            "平均 KV Cache 傳輸時間": self.df['kv_cache_transfer_time'].mean(),
            "平均 KV Cache 傳輸大小 (MB)": self.df['kv_cache_transfer_size_bytes'].mean() / (1024*1024),
            "平均 KV Cache 傳輸吞吐量 (GB/s)": self.df['kv_cache_transfer_throughput_gbps'].mean(),
        }
        
        # 按傳輸類型分析
        if 'kv_cache_transfer_type' in self.df.columns:
            transfer_types = self.df['kv_cache_transfer_type'].value_counts()
            kv_stats["傳輸類型分布"] = transfer_types.to_dict()
        
        return kv_stats
    
    def plot_time_distribution(self, save_path: Optional[str] = None):
        """繪製時間分布圖"""
        if self.df is None:
            return
        
        fig, axes = plt.subplots(2, 3, figsize=(15, 10))
        fig.suptitle('請求時間分布分析', fontsize=16)
        
        time_columns = [
            'total_request_time', 'ttft', 'decode_time',
            'routing_decision_time', 'backend_connection_time', 'lookup_time'
        ]
        
        for i, col in enumerate(time_columns):
            if col in self.df.columns:
                row, col_idx = i // 3, i % 3
                axes[row, col_idx].hist(self.df[col], bins=50, alpha=0.7, edgecolor='black')
                axes[row, col_idx].set_title(f'{col} 分布')
                axes[row, col_idx].set_xlabel('時間 (秒)')
                axes[row, col_idx].set_ylabel('頻率')
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
        plt.show()
    
    def plot_kv_cache_analysis(self, save_path: Optional[str] = None):
        """繪製 KV Cache 分析圖"""
        if self.df is None or 'kv_cache_transfer_time' not in self.df.columns:
            return
        
        fig, axes = plt.subplots(2, 2, figsize=(12, 10))
        fig.suptitle('KV Cache 傳輸分析', fontsize=16)
        
        # KV Cache 傳輸時間分布
        axes[0, 0].hist(self.df['kv_cache_transfer_time'], bins=50, alpha=0.7, edgecolor='black')
        axes[0, 0].set_title('KV Cache 傳輸時間分布')
        axes[0, 0].set_xlabel('時間 (秒)')
        axes[0, 0].set_ylabel('頻率')
        
        # KV Cache 命中率
        hit_rate = (self.df['kv_cache_hit'] == True).mean()
        miss_rate = (self.df['kv_cache_miss'] == True).mean()
        axes[0, 1].pie([hit_rate, miss_rate], labels=['命中', '未命中'], autopct='%1.1f%%')
        axes[0, 1].set_title('KV Cache 命中率')
        
        # 傳輸大小分布
        if 'kv_cache_transfer_size_bytes' in self.df.columns:
            transfer_sizes_mb = self.df['kv_cache_transfer_size_bytes'] / (1024*1024)
            axes[1, 0].hist(transfer_sizes_mb, bins=50, alpha=0.7, edgecolor='black')
            axes[1, 0].set_title('KV Cache 傳輸大小分布')
            axes[1, 0].set_xlabel('大小 (MB)')
            axes[1, 0].set_ylabel('頻率')
        
        # 傳輸吞吐量分布
        if 'kv_cache_transfer_throughput_gbps' in self.df.columns:
            axes[1, 1].hist(self.df['kv_cache_transfer_throughput_gbps'], bins=50, alpha=0.7, edgecolor='black')
            axes[1, 1].set_title('KV Cache 傳輸吞吐量分布')
            axes[1, 1].set_xlabel('吞吐量 (GB/s)')
            axes[1, 1].set_ylabel('頻率')
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
        plt.show()
    
    def plot_correlation_heatmap(self, save_path: Optional[str] = None):
        """繪製相關性熱力圖"""
        if self.df is None:
            return
        
        # 選擇數值列
        numeric_columns = self.df.select_dtypes(include=[np.number]).columns
        correlation_matrix = self.df[numeric_columns].corr()
        
        plt.figure(figsize=(12, 10))
        sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', center=0,
                   square=True, fmt='.2f')
        plt.title('請求時間指標相關性熱力圖')
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
        plt.show()
    
    def export_summary_report(self, output_path: str):
        """導出總結報告"""
        report = {
            "分析時間": datetime.now().isoformat(),
            "數據文件": self.csv_file_path,
            "總記錄數": len(self.df) if self.df is not None else 0,
            "基本統計": self.get_basic_stats(),
            "百分位數統計": self.get_percentile_stats(),
            "路由分析": self.get_routing_analysis(),
            "KV Cache 分析": self.get_kv_cache_analysis(),
        }
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        
        print(f"報告已導出到: {output_path}")
    
    def filter_by_time_range(self, start_time: str, end_time: str):
        """按時間範圍過濾數據"""
        if self.df is None or 'start_timestamp' not in self.df.columns:
            return
        
        self.df['start_timestamp'] = pd.to_datetime(self.df['start_timestamp'])
        start_dt = pd.to_datetime(start_time)
        end_dt = pd.to_datetime(end_time)
        
        mask = (self.df['start_timestamp'] >= start_dt) & (self.df['start_timestamp'] <= end_dt)
        self.df = self.df[mask]
        print(f"過濾後剩餘 {len(self.df)} 條記錄")
    
    def filter_by_routing_logic(self, routing_logics: List[str]):
        """按路由邏輯過濾數據"""
        if self.df is None or 'routing_logic' not in self.df.columns:
            return
        
        self.df = self.df[self.df['routing_logic'].isin(routing_logics)]
        print(f"過濾後剩餘 {len(self.df)} 條記錄")


def main():
    """主函數"""
    parser = argparse.ArgumentParser(description='請求時間數據分析工具')
    parser.add_argument('csv_file', help='CSV 文件路徑')
    parser.add_argument('--output-dir', default='./analysis_output', help='輸出目錄')
    parser.add_argument('--start-time', help='開始時間 (YYYY-MM-DD HH:MM:SS)')
    parser.add_argument('--end-time', help='結束時間 (YYYY-MM-DD HH:MM:SS)')
    parser.add_argument('--routing-logic', nargs='+', help='路由邏輯過濾')
    
    args = parser.parse_args()
    
    # 創建輸出目錄
    os.makedirs(args.output_dir, exist_ok=True)
    
    # 初始化分析器
    analyzer = RequestTimingAnalyzer(args.csv_file)
    
    if analyzer.df is None:
        print("無法載入數據，退出")
        return
    
    # 應用過濾器
    if args.start_time and args.end_time:
        analyzer.filter_by_time_range(args.start_time, args.end_time)
    
    if args.routing_logic:
        analyzer.filter_by_routing_logic(args.routing_logic)
    
    # 生成分析報告
    print("生成基本統計...")
    basic_stats = analyzer.get_basic_stats()
    for key, value in basic_stats.items():
        print(f"{key}: {value}")
    
    print("\n生成 KV Cache 分析...")
    kv_stats = analyzer.get_kv_cache_analysis()
    for key, value in kv_stats.items():
        print(f"{key}: {value}")
    
    # 生成圖表
    print("\n生成時間分布圖...")
    analyzer.plot_time_distribution(os.path.join(args.output_dir, 'time_distribution.png'))
    
    print("生成 KV Cache 分析圖...")
    analyzer.plot_kv_cache_analysis(os.path.join(args.output_dir, 'kv_cache_analysis.png'))
    
    print("生成相關性熱力圖...")
    analyzer.plot_correlation_heatmap(os.path.join(args.output_dir, 'correlation_heatmap.png'))
    
    # 導出報告
    report_path = os.path.join(args.output_dir, 'analysis_report.json')
    analyzer.export_summary_report(report_path)
    
    print(f"\n分析完成！結果保存在: {args.output_dir}")


if __name__ == "__main__":
    main()

