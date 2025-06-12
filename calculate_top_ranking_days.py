#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
策略排名持久性分析工具

功能：
分析指定策略下，進入前X名的交易對能持續停留在前Y名的平均天數。
"""

import pandas as pd
from collections import defaultdict
import argparse
from tqdm import tqdm
from database_operations import DatabaseManager

class RankingPersistenceAnalyzer:
    """
    分析策略排名的持久性
    """

    def __init__(self, strategy: str, rank_x: int, rank_y: int):
        """
        初始化分析器

        Args:
            strategy (str): 要分析的策略名稱
            rank_x (int): 觸發條件排名 (進入前X名)
            rank_y (int): 持續條件排名 (保持在前Y名內)
        """
        if rank_x > rank_y:
            raise ValueError("觸發排名(X)必須小於或等於持續排名(Y)")

        self.strategy = strategy
        self.rank_x = rank_x
        self.rank_y = rank_y
        self.db = DatabaseManager()
        self.ranking_data = None
        self.data_by_date = {}

    def load_data(self):
        """從數據庫加載策略排名數據"""
        print(f"📈 正在加載策略 '{self.strategy}' 的排名數據...")
        df = self.db.get_strategy_ranking(self.strategy)
        
        if df.empty:
            print(f"⚠️ 未找到策略 '{self.strategy}' 的任何數據。")
            return False

        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values(by=['date', 'rank_position'])
        self.ranking_data = df
        
        # 將數據按日期分組，便於快速查找
        self.data_by_date = {name.strftime('%Y-%m-%d'): group for name, group in self.ranking_data.groupby('date')}
        print(f"✅ 成功加載 {len(df)} 條數據，涵蓋 {len(self.data_by_date)} 天。")
        return True

    def analyze(self):
        """
        執行排名持久性分析
        """
        if not self.load_data():
            return

        # 在開始分析前，清除該策略的舊數據
        self.db.clear_ranking_persistence_data(self.strategy)

        processed_streaks = set()  # 存儲已經被計算過的 (trading_pair, date) 組合，防止重複計算
        event_counter = defaultdict(int) # 為每個交易對計算是第幾次觸發事件
        all_events = []

        sorted_dates = sorted(self.data_by_date.keys())

        # 使用tqdm創建進度條
        for entry_date_str in tqdm(sorted_dates, desc=f"分析策略 '{self.strategy}' 持久性"):
            daily_data = self.data_by_date[entry_date_str]
            
            # 獲取當天進入前X名的交易對
            top_x_pairs = daily_data[daily_data['rank_position'] <= self.rank_x]

            for _, row in top_x_pairs.iterrows():
                trading_pair = row['trading_pair']
                entry_date = pd.to_datetime(entry_date_str)

                # 如果這個交易對在這一天的上漲趨勢已經被處理過，則跳過
                if (trading_pair, entry_date) in processed_streaks:
                    continue

                # ================= 觸發新事件，開始追蹤 =================
                entry_rank = row['rank_position']
                consecutive_days = 0
                current_date = entry_date
                
                while True:
                    current_date_str = current_date.strftime('%Y-%m-%d')
                    
                    if current_date_str not in self.data_by_date:
                        # 如果日期超出數據範圍，中斷
                        break

                    daily_check_data = self.data_by_date[current_date_str]
                    pair_rank_info = daily_check_data[daily_check_data['trading_pair'] == trading_pair]

                    if pair_rank_info.empty or pair_rank_info.iloc[0]['rank_position'] > self.rank_y:
                        # 交易對當天排名超出了Y，或者當天沒有數據，中斷
                        break
                    
                    # 仍在Y名內，將其標記為已處理
                    processed_streaks.add((trading_pair, current_date))
                    consecutive_days += 1
                    current_date += pd.Timedelta(days=1)
                
                # ================= 追蹤結束，記錄事件 =================
                event_counter[trading_pair] += 1
                event_num = event_counter[trading_pair]
                event_id = f"{self.strategy}_{trading_pair}_({event_num})"
                
                exit_date = current_date - pd.Timedelta(days=1)
                exit_date_str = exit_date.strftime('%Y-%m-%d')

                # 獲取退出時的排名
                exit_rank = None
                if exit_date_str in self.data_by_date:
                    exit_day_data = self.data_by_date[exit_date_str]
                    exit_rank_info = exit_day_data[exit_day_data['trading_pair'] == trading_pair]
                    if not exit_rank_info.empty:
                         exit_rank = int(exit_rank_info.iloc[0]['rank_position'])


                event = {
                    "event_id": event_id,
                    "strategy": self.strategy,
                    "trading_pair": trading_pair,
                    "entry_date": entry_date_str,
                    "entry_rank": int(entry_rank),
                    "exit_date": exit_date_str,
                    "exit_rank": exit_rank,
                    "consecutive_days": consecutive_days,
                    "trigger_rank_x": self.rank_x,
                    "persistence_rank_y": self.rank_y,
                    "parameters": f"x={self.rank_x}, y={self.rank_y}"
                }
                all_events.append(event)
        
        # 新增步驟：計算每個交易對的累計持續天數
        if all_events:
            # 首先按交易對和進入日期排序，確保事件順序正確
            all_events.sort(key=lambda x: (x['trading_pair'], x['entry_date']))
            
            cumulative_days_tracker = defaultdict(int)
            for event in all_events:
                trading_pair = event['trading_pair']
                cumulative_days_tracker[trading_pair] += event['consecutive_days']
                event['cumulative_consecutive_days'] = cumulative_days_tracker[trading_pair]

        # 批量插入數據庫
        if all_events:
            self.db.insert_ranking_persistence_events(all_events)
        
        print(f"\n🎉 分析完成！共發現 {len(all_events)} 個排名持久性事件。")


def get_user_input():
    """獲取並驗證用戶的互動式輸入"""
    
    # 獲取可用策略
    db = DatabaseManager()
    available_strategies = db.get_available_strategies()
    if not available_strategies:
        print("❌ 數據庫中未找到任何可用策略。請先運行策略排名。")
        return None, None, None

    print("\n📊 可用策略列表:")
    for i, s in enumerate(available_strategies, 1):
        print(f"  {i}. {s}")
    
    strategy = ""
    while True:
        try:
            choice = input(f"請選擇要分析的策略 (輸入數字 1-{len(available_strategies)}): ").strip()
            choice_idx = int(choice) - 1
            if 0 <= choice_idx < len(available_strategies):
                strategy = available_strategies[choice_idx]
                print(f"✅ 已選擇策略: {strategy}")
                break
            else:
                print("無效的選擇，請重新輸入。")
        except (ValueError, IndexError):
            print("無效的輸入，請輸入列表中的數字。")

    def get_integer_input(prompt):
        while True:
            try:
                value = int(input(prompt).strip())
                if value > 0:
                    return value
                print("❌ 請輸入一個大於0的整數。")
            except ValueError:
                print("❌ 無效的輸入，請輸入一個整數。")

    rank_x = get_integer_input("請輸入觸發條件 (進入前X名): ")
    rank_y = get_integer_input("請輸入持續條件 (保持在前Y名內): ")
    
    return strategy, rank_x, rank_y


def main():
    """主函數入口"""
    print("="*50)
    print("🚀 策略排名持久性分析工具")
    print("="*50)
    
    strategy, rank_x, rank_y = get_user_input()

    if not all([strategy, rank_x, rank_y]):
        print("未能獲取有效的輸入，程序終止。")
        return

    try:
        analyzer = RankingPersistenceAnalyzer(
            strategy=strategy, 
            rank_x=rank_x, 
            rank_y=rank_y
        )
        analyzer.analyze()
    except ValueError as e:
        print(f"❌ 錯誤: {e}")
    except Exception as e:
        print(f"❌ 發生未知錯誤: {e}")


if __name__ == "__main__":
    main() 