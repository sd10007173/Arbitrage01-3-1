#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
策略排行榜生成模組 V2 - 性能優化版本
從數據庫讀取return_metrics數據，根據不同策略計算排名
輸出到數據庫: strategy_ranking表

=== 性能優化 ===
1. 一次性讀取所有時間範圍的數據，避免逐日查詢 (N+1問題)
2. 使用pandas.groupby().apply()在內存中高效處理每日排名
3. 使用pandas.merge()代替iterrows()來合併數據，速度提升百倍以上
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import argparse
from ranking_config import RANKING_STRATEGIES, EXPERIMENTAL_CONFIGS
from ranking_engine import RankingEngine
import time

# 添加數據庫支持
from database_operations import DatabaseManager

def load_fr_return_data_from_database(start_date=None, end_date=None, symbol=None):
    """
    從數據庫載入指定時間範圍內的return_metrics數據
    
    Args:
        start_date: 開始日期 (YYYY-MM-DD)
        end_date: 結束日期 (YYYY-MM-DD)
        symbol: 交易對符號 (可選)
    
    Returns:
        pandas.DataFrame: 包含收益數據的DataFrame
    """
    try:
        print(f"🗄️ 正在從數據庫載入收益數據...")
        print(f"   時間範圍: {start_date or '所有'} 到 {end_date or '所有'}")
        if symbol:
            print(f"   交易對: {symbol}")
            
        db = DatabaseManager()
        
        # 一次性獲取所有數據
        df = db.get_return_metrics(start_date=start_date, end_date=end_date, trading_pair=symbol)
        
        if df.empty:
            print(f"📊 數據庫中沒有找到匹配的收益數據")
            return pd.DataFrame()
        
        # 直接在SQL查詢層面處理好欄位名是更優的做法，但為保持db_operations不變，這裡暫時保留
        db_to_csv_mapping = {
            'trading_pair': 'Trading_Pair',
            'date': 'Date',
            'return_1d': '1d_return', 'roi_1d': '1d_ROI',
            'return_2d': '2d_return', 'roi_2d': '2d_ROI',
            'return_7d': '7d_return', 'roi_7d': '7d_ROI',
            'return_14d': '14d_return', 'roi_14d': '14d_ROI',
            'return_30d': '30d_return', 'roi_30d': '30d_ROI',
            'return_all': 'all_return', 'roi_all': 'all_ROI'
        }
        df.rename(columns=db_to_csv_mapping, inplace=True)
        
        print(f"✅ 數據庫載入成功: {len(df)} 筆記錄")
        return df
        
    except Exception as e:
        print(f"❌ 從數據庫載入收益數據時出錯: {e}")
        return pd.DataFrame()

def generate_strategy_ranking_batch(df, strategy_name, strategy_config):
    """
    批量計算單個策略在多個日期上的排名
    
    Args:
        df: 包含多天return_metrics數據的DataFrame
        strategy_name: 策略名稱
        strategy_config: 策略配置
    
    Returns:
        DataFrame: 包含所有日期排名的DataFrame
    """
    if df.empty:
        return pd.DataFrame()
    
    print(f"📊 正在批量計算策略: {strategy_name}")

    ranking_engine = RankingEngine(strategy_name)

    # 定義每日計算函數
    def calculate_daily_ranking(daily_df):
        # 創建一個副本以避免 SettingWithCopyWarning
        daily_df_copy = daily_df.copy()
        
        # 使用RankingEngine計算排名
        ranked_df = ranking_engine.calculate_final_ranking(daily_df_copy)
        
        if ranked_df.empty:
            return None
        
        # 按分數降序排序並添加排名
        ranked_df = ranked_df.sort_values('final_ranking_score', ascending=False).reset_index(drop=True)
        ranked_df['Rank'] = range(1, len(ranked_df) + 1)
        return ranked_df

    # 按日期分組並應用每日計算函數
    # 使用 .copy() 確保我們操作的是數據副本
    all_rankings = df.copy().groupby('Date').apply(calculate_daily_ranking, include_groups=False).reset_index()
    
    # 刪除由 groupby 產生的 level_1 索引
    if 'level_1' in all_rankings.columns:
        all_rankings = all_rankings.drop(columns=['level_1'])
        
    print(f"   ✅ 策略 {strategy_name} 批量計算完成，共處理 {all_rankings['Date'].nunique()} 天, {len(all_rankings)} 條排名記錄")
    
    return all_rankings


def save_strategy_ranking_to_database(ranked_df, strategy_name):
    """
    將策略排行榜批量保存到數據庫
    
    Args:
        ranked_df: 包含多天排名的DataFrame
        strategy_name: 策略名稱
    
    Returns:
        保存的記錄數
    """
    if ranked_df.empty:
        print("⚠️ 排行榜數據為空，無法保存")
        return 0
    
    try:
        db = DatabaseManager()
        
        print(f"💾 準備將 {len(ranked_df)} 條策略排行記錄插入數據庫...")
        
        db_df = ranked_df.copy()
        db_df['strategy_name'] = strategy_name
        
        # 列名映射
        column_mapping = {
            'Trading_Pair': 'trading_pair',
            'Rank': 'rank_position',
            'Date': 'date'
        }
        db_df.rename(columns=column_mapping, inplace=True)
        
        # 確保必需列存在
        required_cols = ['strategy_name', 'trading_pair', 'date', 'final_ranking_score', 'rank_position']
        if not all(col in db_df.columns for col in required_cols):
            print(f"❌ 缺少必需的數據庫列。需要: {required_cols}, 實際: {db_df.columns.tolist()}")
            return 0
        
        # 保存到數據庫 (假設 db 操作支持批量)
        inserted_count = db.insert_strategy_ranking(db_df, strategy_name)
        print(f"✅ 數據庫插入成功: {inserted_count} 條記錄")
        
        return inserted_count
        
    except Exception as e:
        print(f"❌ 保存策略排行榜到數據庫時出錯: {e}")
        return 0

def select_strategies_interactively():
    """
    互動式選擇策略
    
    Returns:
        list: 選擇的策略名稱列表
    """
    # 合併主要策略和實驗性策略
    all_strategies = {**RANKING_STRATEGIES, **EXPERIMENTAL_CONFIGS}
    available_strategies = list(all_strategies.keys())
    
    print("\n🎯 可用策略:")
    print("="*50)
    
    # 顯示主要策略
    main_count = 0
    for i, strategy in enumerate(RANKING_STRATEGIES.keys(), 1):
        strategy_info = RANKING_STRATEGIES[strategy]
        print(f"{i}. {strategy:20s} - {strategy_info['name']}")
        main_count = i
    
    # 顯示實驗性策略
    if EXPERIMENTAL_CONFIGS:
        print("\n🧪 實驗性策略:")
        print("-" * 30)
        for i, strategy in enumerate(EXPERIMENTAL_CONFIGS.keys(), main_count + 1):
            strategy_info = EXPERIMENTAL_CONFIGS[strategy]
            print(f"{i}. {strategy:20s} - {strategy_info['name']}")
    
    print(f"{len(available_strategies)+1}. 全部策略 (all)")
    print("0. 退出")
    
    while True:
        try:
            choice = input(f"\n請輸入策略編號 (1-{len(available_strategies)+1}, 或 0 退出): ").strip()
            
            if choice == '0':
                print("已取消執行")
                return []
            elif choice == str(len(available_strategies)+1) or choice.lower() == 'all':
                print(f"✅ 已選擇全部 {len(available_strategies)} 個策略")
                return available_strategies
            elif choice.isdigit():
                choice_num = int(choice)
                if 1 <= choice_num <= len(available_strategies):
                    selected_strategy = available_strategies[choice_num-1]
                    # 檢查策略在哪個配置中
                    if selected_strategy in RANKING_STRATEGIES:
                        strategy_info = RANKING_STRATEGIES[selected_strategy]
                    else:
                        strategy_info = EXPERIMENTAL_CONFIGS[selected_strategy]
                    print(f"✅ 已選擇策略: {selected_strategy} - {strategy_info['name']}")
                    return [selected_strategy]
                else:
                    print(f"無效的編號: {choice_num}")
            else:
                print(f"無效的輸入: {choice}")
        except ValueError:
            print("請輸入數字。")

def main():
    """主執行程序 - 性能優化版本"""
    start_time = time.time()
    
    parser = argparse.ArgumentParser(description="策略排行榜生成模組 V2")
    parser.add_argument("--start_date", help="開始日期 (YYYY-MM-DD)")
    parser.add_argument("--end_date", help="結束日期 (YYYY-MM-DD)")
    parser.add_argument("--symbol", help="指定單一交易對 (可選)")
    parser.add_argument("--strategies", help="指定策略，用逗號分隔 (可選)")
    
    args = parser.parse_args()
    
    # 1. 確定日期範圍
    start_date = args.start_date
    end_date = args.end_date

    # 如果用戶沒有指定日期，則從數據庫自動檢測範圍
    if not start_date or not end_date:
        print("ℹ️ 未指定日期範圍，正在從數據庫自動檢測...")
        db = DatabaseManager()
        db_start, db_end = db.get_return_metrics_date_range()
        
        if db_start and db_end:
            start_date = start_date or db_start
            end_date = end_date or db_end
            print(f"   ✅ 自動檢測到日期範圍: {start_date} 到 {end_date}")
        else:
            print("⚠️ 無法自動檢測日期範圍，且未手動指定。請檢查 return_metrics 表中是否有數據。")
            # 使用過去30天作為備用方案
            end_date = end_date or datetime.now().strftime('%Y-%m-%d')
            start_date = start_date or (datetime.strptime(end_date, '%Y-%m-%d') - timedelta(days=29)).strftime('%Y-%m-%d')
            print(f"   -> 將使用預設備用範圍: {start_date} 到 {end_date}")
    
    print("="*50)
    print("🚀 策略排行榜生成器 V2 啟動 🚀")
    print(f"時間範圍: {start_date} 到 {end_date}")
    print("="*50)

    # 2. 確定要運行的策略
    if args.strategies:
        selected_strategies = [s.strip() for s in args.strategies.split(',')]
    else:
        selected_strategies = select_strategies_interactively()

    if not selected_strategies:
        return # 用戶選擇退出

    # 3. 一次性加載所有需要的數據
    all_data = load_fr_return_data_from_database(start_date=start_date, end_date=end_date, symbol=args.symbol)

    if all_data.empty:
        print("沒有數據可供處理，腳本終止。")
        return

    # 4. 逐一計算並保存每個策略的排名
    for strategy_name in selected_strategies:
        # 檢查策略在哪個配置中
        if strategy_name in RANKING_STRATEGIES:
            strategy_config = RANKING_STRATEGIES[strategy_name]
        elif strategy_name in EXPERIMENTAL_CONFIGS:
            strategy_config = EXPERIMENTAL_CONFIGS[strategy_name]
        else:
            print(f"⚠️ 找不到名為 '{strategy_name}' 的策略配置，跳過。")
            continue

        # 批量計算排名
        ranked_df = generate_strategy_ranking_batch(all_data, strategy_name, strategy_config)

        # 批量保存到數據庫
        if not ranked_df.empty:
            save_strategy_ranking_to_database(ranked_df, strategy_name)
        
        print("-"*50)

    end_time_val = time.time()
    print(f"\n🎉 所有策略計算完成！總耗時: {end_time_val - start_time:.2f} 秒")

if __name__ == "__main__":
    main() 