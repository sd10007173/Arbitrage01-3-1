#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
計算並更新 trading_pairs 表的 diff_first_date 欄位

diff_first_date 定義：
該交易對在 exchange_a 和 exchange_b 兩邊都有非null的資金費率的第一個時間點，
也就是真正有資金費率差出現的時候。
"""

import pandas as pd
import sqlite3
from datetime import datetime
from database_operations import DatabaseManager

# 嘗試導入 tqdm，如果沒有則使用普通迭代器
try:
    from tqdm import tqdm
except ImportError:
    print("⚠️ tqdm 未安裝，將使用簡化進度顯示")
    def tqdm(iterable, total=None, desc="處理中"):
        for i, item in enumerate(iterable):
            if total and i % max(1, total // 10) == 0:
                print(f"{desc}: {i}/{total}")
            yield item

class DiffFirstDateCalculator:
    
    def __init__(self):
        self.db = DatabaseManager()
        
    def get_all_trading_pairs(self):
        """獲取所有交易對"""
        query = """
        SELECT id, symbol, exchange_a, exchange_b, diff_first_date
        FROM trading_pairs
        ORDER BY symbol, exchange_a, exchange_b
        """
        
        return pd.read_sql_query(query, self.db.get_connection())
    
    def calculate_diff_first_date_for_pair(self, symbol, exchange_a, exchange_b):
        """
        計算特定交易對的首次資金費率差出現時間
        
        Args:
            symbol: 交易對符號
            exchange_a: 交易所A
            exchange_b: 交易所B
            
        Returns:
            str: 首次出現資金費率差的時間 (YYYY-MM-DD HH:MM:SS)，如果沒有找到則返回None
        """
        
        query = """
        SELECT 
            h1.timestamp_utc,
            h1.funding_rate as rate_a,
            h2.funding_rate as rate_b
        FROM funding_rate_history h1
        INNER JOIN funding_rate_history h2 
            ON h1.timestamp_utc = h2.timestamp_utc 
            AND h1.symbol = h2.symbol
        WHERE h1.symbol = ?
            AND h1.exchange = ?
            AND h2.exchange = ?
            AND h1.funding_rate IS NOT NULL
            AND h2.funding_rate IS NOT NULL
        ORDER BY h1.timestamp_utc
        LIMIT 1
        """
        
        with self.db.get_connection() as conn:
            cursor = conn.execute(query, [symbol, exchange_a, exchange_b])
            result = cursor.fetchone()
            
            if result:
                return result[0]  # timestamp_utc
            else:
                return None
    
    def update_diff_first_date(self, pair_id, diff_first_date):
        """更新交易對的 diff_first_date"""
        
        query = """
        UPDATE trading_pairs 
        SET diff_first_date = ?, updated_at = CURRENT_TIMESTAMP 
        WHERE id = ?
        """
        
        with self.db.get_connection() as conn:
            conn.execute(query, [diff_first_date, pair_id])
    
    def calculate_all_diff_first_dates(self, force_recalculate=False):
        """
        計算所有交易對的首次資金費率差出現時間
        
        Args:
            force_recalculate: 是否強制重新計算已有數據，默認False（只計算空值）
        """
        
        print("📊 開始計算交易對的首次資金費率差時間...")
        
        # 獲取所有交易對
        trading_pairs = self.get_all_trading_pairs()
        
        if trading_pairs.empty:
            print("⚠️ 沒有找到任何交易對")
            return
        
        print(f"🔍 找到 {len(trading_pairs)} 個交易對")
        
        # 過濾需要處理的記錄
        if not force_recalculate:
            # 只處理 diff_first_date 為空的記錄
            to_process = trading_pairs[trading_pairs['diff_first_date'].isna()]
            print(f"📋 需要計算的交易對: {len(to_process)} 個（跳過已有數據）")
        else:
            to_process = trading_pairs
            print(f"📋 將重新計算所有 {len(to_process)} 個交易對")
        
        if to_process.empty:
            print("✅ 所有交易對都已有 diff_first_date 數據")
            return
        
        # 統計變量
        updated_count = 0
        not_found_count = 0
        
        # 開始處理
        for _, row in tqdm(to_process.iterrows(), total=len(to_process), desc="計算進度"):
            pair_id = row['id']
            symbol = row['symbol']
            exchange_a = row['exchange_a']
            exchange_b = row['exchange_b']
            
            # 計算首次出現時間
            diff_first_date = self.calculate_diff_first_date_for_pair(
                symbol, exchange_a, exchange_b
            )
            
            if diff_first_date:
                # 更新數據庫
                self.update_diff_first_date(pair_id, diff_first_date)
                updated_count += 1
                
                if updated_count <= 5:  # 只顯示前5個示例
                    print(f"✅ {symbol}_{exchange_a}_{exchange_b}: {diff_first_date}")
            else:
                not_found_count += 1
                if not_found_count <= 3:  # 只顯示前3個未找到的示例
                    print(f"⚠️ {symbol}_{exchange_a}_{exchange_b}: 未找到資金費率差數據")
        
        # 輸出結果統計
        print(f"\n🎉 計算完成！")
        print(f"✅ 成功更新: {updated_count} 個交易對")
        print(f"⚠️ 未找到數據: {not_found_count} 個交易對")
        print(f"📊 總處理: {len(to_process)} 個交易對")
    
    def show_results_sample(self, limit=10):
        """顯示計算結果樣本"""
        
        query = f"""
        SELECT 
            symbol,
            exchange_a,
            exchange_b,
            diff_first_date,
            CASE 
                WHEN diff_first_date IS NOT NULL THEN 
                    JULIANDAY('now') - JULIANDAY(diff_first_date)
                ELSE NULL
            END as days_since_first_diff
        FROM trading_pairs
        WHERE diff_first_date IS NOT NULL
        ORDER BY diff_first_date
        LIMIT {limit}
        """
        
        df = pd.read_sql_query(query, self.db.get_connection())
        
        if not df.empty:
            print(f"\n📋 首次資金費率差時間樣本 (前{limit}個):")
            print("=" * 80)
            for _, row in df.iterrows():
                days = row['days_since_first_diff']
                print(f"{row['symbol']:>8} | {row['exchange_a']:>8} - {row['exchange_b']:>8} | "
                      f"{row['diff_first_date']} | {days:.1f} 天前")
        else:
            print("⚠️ 沒有找到任何計算結果")
    
    def get_statistics(self):
        """獲取統計信息"""
        
        query = """
        SELECT 
            COUNT(*) as total_pairs,
            COUNT(diff_first_date) as pairs_with_data,
            COUNT(*) - COUNT(diff_first_date) as pairs_without_data,
            MIN(diff_first_date) as earliest_diff_date,
            MAX(diff_first_date) as latest_diff_date
        FROM trading_pairs
        """
        
        with self.db.get_connection() as conn:
            result = conn.execute(query).fetchone()
            
            print(f"\n📊 統計信息:")
            print(f"總交易對數: {result[0]}")
            print(f"有資金費率差數據: {result[1]}")
            print(f"無資金費率差數據: {result[2]}")
            if result[3]:
                print(f"最早資金費率差時間: {result[3]}")
                print(f"最晚資金費率差時間: {result[4]}")

def main():
    """主函數"""
    import argparse
    
    parser = argparse.ArgumentParser(description='計算交易對的首次資金費率差時間')
    parser.add_argument('--force', action='store_true', 
                       help='強制重新計算所有交易對（包括已有數據）')
    parser.add_argument('--show-sample', type=int, default=10,
                       help='顯示結果樣本數量，默認10')
    parser.add_argument('--stats-only', action='store_true',
                       help='只顯示統計信息，不進行計算')
    
    args = parser.parse_args()
    
    calculator = DiffFirstDateCalculator()
    
    if args.stats_only:
        calculator.get_statistics()
        calculator.show_results_sample(args.show_sample)
    else:
        # 執行計算
        calculator.calculate_all_diff_first_dates(force_recalculate=args.force)
        
        # 顯示結果
        calculator.show_results_sample(args.show_sample)
        calculator.get_statistics()

if __name__ == "__main__":
    main() 