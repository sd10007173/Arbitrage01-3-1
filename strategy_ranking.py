#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
策略排行榜生成模組
從數據庫讀取return_metrics數據，根據不同策略計算排名
輸出到數據庫: strategy_ranking表
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import argparse
from ranking_config import RANKING_STRATEGIES
from ranking_engine import RankingEngine

# 添加數據庫支持
from database_operations import DatabaseManager

def load_fr_return_data_from_database(target_date=None, symbol=None):
    """
    從數據庫載入指定日期的return_metrics數據
    
    Args:
        target_date: 目標日期 (YYYY-MM-DD)，None表示所有日期
        symbol: 交易對符號 (可選)
    
    Returns:
        pandas.DataFrame: 包含收益數據的DataFrame (CSV格式的列名)
    """
    try:
        print(f"🗄️ 正在從數據庫載入收益數據...")
        if target_date:
            print(f"   目標日期: {target_date}")
        if symbol:
            print(f"   交易對: {symbol}")
            
        db = DatabaseManager()
        
        # 從數據庫獲取return_metrics數據
        df = db.get_return_metrics(date=target_date, trading_pair=symbol)
        
        if df.empty:
            print(f"📊 數據庫中沒有找到匹配的收益數據")
            return pd.DataFrame()
        
        # 轉換數據庫欄位格式到策略引擎期望的CSV格式
        db_to_csv_mapping = {
            'trading_pair': 'Trading_Pair',
            'date': 'Date',
            'return_1d': '1d_return',
            'roi_1d': '1d_ROI',
            'return_2d': '2d_return',
            'roi_2d': '2d_ROI',
            'return_7d': '7d_return',
            'roi_7d': '7d_ROI',
            'return_14d': '14d_return',
            'roi_14d': '14d_ROI',
            'return_30d': '30d_return',
            'roi_30d': '30d_ROI',
            'return_all': 'all_return',
            'roi_all': 'all_ROI'
        }
        
        # 重命名欄位
        for db_col, csv_col in db_to_csv_mapping.items():
            if db_col in df.columns:
                df[csv_col] = df[db_col]
        
        # 選擇需要的欄位（CSV格式）
        csv_columns = ['Trading_Pair', 'Date', '1d_return', '1d_ROI', '2d_return', '2d_ROI',
                      '7d_return', '7d_ROI', '14d_return', '14d_ROI', '30d_return', '30d_ROI',
                      'all_return', 'all_ROI']
        
        available_columns = [col for col in csv_columns if col in df.columns]
        if available_columns:
            df = df[available_columns].copy()
            print(f"✅ 數據庫載入成功: {len(df)} 筆記錄")
            if target_date:
                unique_pairs = df['Trading_Pair'].nunique()
                print(f"   {target_date} 包含 {unique_pairs} 個交易對")
            return df
        else:
            print(f"❌ 數據庫中沒有找到必要的收益欄位")
            return pd.DataFrame()
        
    except Exception as e:
        print(f"❌ 從數據庫載入收益數據時出錯: {e}")
        return pd.DataFrame()

def generate_strategy_ranking(df, strategy_name, strategy_config):
    """
    根據策略配置生成排行榜
    
    Args:
        df: return_metrics數據 (CSV格式列名)
        strategy_name: 策略名稱
        strategy_config: 策略配置
    
    Returns:
        DataFrame: 排行榜數據
    """
    if df.empty:
        return pd.DataFrame()
    
    print(f"📊 正在計算策略: {strategy_name}")
    
    # 創建 df 的副本，保留所有原始 ROI 數據
    original_df = df.copy()
    
    # 使用RankingEngine計算排名
    ranking_engine = RankingEngine(strategy_name)
    
    # 計算排名
    ranked_df = ranking_engine.calculate_final_ranking(original_df)
    
    if ranked_df.empty:
        print(f"   策略 {strategy_name} 計算結果為空")
        return pd.DataFrame()
    
    # 確保保留原始的 return_metrics 數據
    roi_columns = ['1d_return', '1d_ROI', '2d_return', '2d_ROI', '7d_return', '7d_ROI', 
                   '14d_return', '14d_ROI', '30d_return', '30d_ROI', 'all_return', 'all_ROI']
    
    for col in roi_columns:
        if col in original_df.columns and col in ranked_df.columns:
            # 根據 Trading_Pair 匹配，恢復原始數據
            for idx, row in ranked_df.iterrows():
                trading_pair = row['Trading_Pair']
                original_row = original_df[original_df['Trading_Pair'] == trading_pair]
                if not original_row.empty:
                    ranked_df.at[idx, col] = original_row.iloc[0][col]
    
    # 確保有必要的列
    required_columns = ['Trading_Pair', 'final_ranking_score']
    
    # 檢查是否有trading_pair列（可能列名不同）
    if 'trading_pair' in ranked_df.columns and 'Trading_Pair' not in ranked_df.columns:
        ranked_df = ranked_df.rename(columns={'trading_pair': 'Trading_Pair'})
    
    for col in required_columns:
        if col not in ranked_df.columns:
            print(f"   警告: 排名結果缺少列 {col}")
            return pd.DataFrame()
    
    # 按分數降序排序
    ranked_df = ranked_df.sort_values('final_ranking_score', ascending=False).reset_index(drop=True)
    
    # 添加排名列
    ranked_df['Rank'] = range(1, len(ranked_df) + 1)
    
    print(f"   ✅ 策略 {strategy_name} 計算完成，共 {len(ranked_df)} 個交易對")
    
    return ranked_df

def save_strategy_ranking_to_database(ranked_df, strategy_name, target_date):
    """
    將策略排行榜保存到數據庫
    
    Args:
        ranked_df: 排行榜DataFrame
        strategy_name: 策略名稱
        target_date: 目標日期
    
    Returns:
        保存的記錄數
    """
    if ranked_df.empty:
        print("⚠️ 排行榜數據為空，無法保存")
        return 0
    
    try:
        db = DatabaseManager()
        
        print(f"📊 準備將 {len(ranked_df)} 條策略排行記錄插入數據庫...")
        
        # 準備數據庫數據
        db_df = ranked_df.copy()
        
        # 添加策略名稱和日期
        db_df['strategy_name'] = strategy_name
        db_df['date'] = target_date
        
        # 處理列名映射
        column_mapping = {
            'Trading_Pair': 'trading_pair',
            'Rank': 'rank_position'
        }
        
        # 重命名列
        for old_col, new_col in column_mapping.items():
            if old_col in db_df.columns:
                db_df[new_col] = db_df[old_col]
        
        # 檢查必需的列
        required_base_columns = ['strategy_name', 'trading_pair', 'date', 'final_ranking_score']
        
        for col in required_base_columns:
            if col not in db_df.columns:
                print(f"❌ 缺少必需列: {col}")
                return 0
        
        print(f"📊 數據樣本: Strategy={strategy_name}, Trading_Pair={db_df.iloc[0]['trading_pair']}, Date={target_date}")
        
        # 保存到數據庫
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
    available_strategies = list(RANKING_STRATEGIES.keys())
    
    print("\n🎯 可用策略:")
    print("="*50)
    
    for i, strategy in enumerate(available_strategies, 1):
        strategy_info = RANKING_STRATEGIES[strategy]
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
                    strategy_info = RANKING_STRATEGIES[selected_strategy]
                    print(f"✅ 已選擇策略: {selected_strategy} - {strategy_info['name']}")
                    return [selected_strategy]
                else:
                    print(f"❌ 請輸入 1-{len(available_strategies)+1} 之間的數字")
            else:
                print("❌ 請輸入有效的數字")
                
        except KeyboardInterrupt:
            print("\n\n已取消執行")
            return []
        except Exception as e:
            print(f"❌ 輸入錯誤: {e}")

def process_date_with_selected_strategies(target_date, selected_strategies):
    """
    處理指定日期的數據，生成選擇的策略排行榜
    
    Args:
        target_date: 目標日期 (YYYY-MM-DD)
        selected_strategies: 選擇的策略列表
    
    Returns:
        處理成功的策略數量
    """
    print(f"\n📅 正在處理日期: {target_date}")
    
    # 從數據庫載入return_metrics數據
    df = load_fr_return_data_from_database(target_date)
    
    if df.empty:
        print(f"   ⚠️ 跳過日期 {target_date}: 沒有有效數據")
        return 0
    
    # 為選定的策略生成排行榜
    results = {}
    successful_strategies = 0
    
    for strategy_name in selected_strategies:
        if strategy_name not in RANKING_STRATEGIES:
            print(f"   ⚠️ 策略 {strategy_name} 不存在，跳過")
            continue
            
        print(f"\n   🎯 處理策略: {strategy_name}")
        strategy_config = RANKING_STRATEGIES[strategy_name]
        
        # 生成策略排行榜
        ranked_df = generate_strategy_ranking(df, strategy_name, strategy_config)
        
        if not ranked_df.empty:
            # 保存到數據庫
            saved_count = save_strategy_ranking_to_database(ranked_df, strategy_name, target_date)
            
            if saved_count > 0:
                successful_strategies += 1
                results[strategy_name] = ranked_df
                print(f"   ✅ 策略 {strategy_name} 處理成功: {saved_count} 條記錄")
            else:
                print(f"   ❌ 策略 {strategy_name} 保存失敗")
        else:
            print(f"   ❌ 策略 {strategy_name} 在日期 {target_date} 沒有有效結果")
    
    # 如果選擇了多個策略，顯示前10名比較
    if len(results) > 1:
        print(f"\n📊 {target_date} 各策略前10名比較:")
        print("="*80)
        
        for strategy_name, ranked_df in results.items():
            strategy_info = RANKING_STRATEGIES[strategy_name]
            print(f"\n🎯 {strategy_name} ({strategy_info['name']}):")
            
            top_10 = ranked_df.head(10)
            for idx, row in top_10.iterrows():
                print(f"  {row['Rank']:2d}. {row['Trading_Pair'][:25]:25s} 分數: {row['final_ranking_score']:8.4f}")
    
    return successful_strategies

def get_available_dates_from_database():
    """
    從數據庫掃描return_metrics表中可用的日期
    
    Returns:
        list: 可用日期列表 (YYYY-MM-DD)
    """
    try:
        db = DatabaseManager()
        
        query = "SELECT DISTINCT date FROM return_metrics ORDER BY date"
        
        with db.get_connection() as conn:
            result = pd.read_sql_query(query, conn)
        
        if result.empty:
            print("📊 數據庫中沒有return_metrics數據")
            return []
        
        dates = result['date'].tolist()
        print(f"📅 數據庫中找到 {len(dates)} 個可用日期")
        
        if dates:
            print(f"   日期範圍: {dates[0]} 到 {dates[-1]}")
        
        return dates
        
    except Exception as e:
        print(f"❌ 掃描數據庫可用日期時出錯: {e}")
        return []

def check_existing_strategy_rankings():
    """
    檢查數據庫中已存在的策略排行榜，回傳已處理的(日期, 策略)組合
    
    Returns:
        set: 已處理的(date, strategy)元組集合
    """
    try:
        db = DatabaseManager()
        
        query = "SELECT DISTINCT date, strategy_name FROM strategy_ranking ORDER BY date, strategy_name"
        
        with db.get_connection() as conn:
            result = pd.read_sql_query(query, conn)
        
        if result.empty:
            print("📊 數據庫中沒有策略排行榜數據")
            return set()
        
        existing_combinations = set(zip(result['date'], result['strategy_name']))
        
        print(f"📊 數據庫中找到 {len(existing_combinations)} 個已處理的(日期, 策略)組合")
        
        # 統計每個策略的處理日期數
        strategy_counts = result.groupby('strategy_name')['date'].nunique()
        for strategy, count in strategy_counts.items():
            print(f"   {strategy}: {count} 個日期")
        
        return existing_combinations
        
    except Exception as e:
        print(f"⚠️ 檢查已存在策略排行榜時出錯: {e}")
        return set()

def generate_date_range(start_date, end_date):
    """
    生成日期範圍
    
    Args:
        start_date: 開始日期 (YYYY-MM-DD)
        end_date: 結束日期 (YYYY-MM-DD)
    
    Returns:
        list: 日期字符串列表
    """
    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt = datetime.strptime(end_date, '%Y-%m-%d')
    
    dates = []
    current_dt = start_dt
    
    while current_dt <= end_dt:
        dates.append(current_dt.strftime('%Y-%m-%d'))
        current_dt += timedelta(days=1)
    
    return dates

def main():
    parser = argparse.ArgumentParser(description='生成策略排行榜並保存到數據庫')
    parser.add_argument('--date', help='指定日期 (YYYY-MM-DD)')
    parser.add_argument('--start_date', help='開始日期 (YYYY-MM-DD)')
    parser.add_argument('--end_date', help='結束日期 (YYYY-MM-DD)')
    parser.add_argument('--all', action='store_true', help='處理所有可用日期')
    parser.add_argument('--strategy', help='指定策略名稱')
    parser.add_argument('--auto', action='store_true', help='自動模式 (不互動選擇)')
    parser.add_argument('--incremental', action='store_true', help='增量模式 (只處理未完成的組合)')
    
    args = parser.parse_args()
    
    print("\n" + "="*60)
    print("🎯 策略排行榜生成 (數據庫版)")
    print("="*60)
    
    # 確定要處理的策略
    selected_strategies = []
    
    if args.strategy:
        # 命令行指定策略
        if args.strategy in RANKING_STRATEGIES:
            selected_strategies = [args.strategy]
            print(f"✅ 命令行指定策略: {args.strategy}")
        else:
            print(f"❌ 策略 {args.strategy} 不存在")
            print(f"可用策略: {list(RANKING_STRATEGIES.keys())}")
            return
    elif args.auto:
        # 自動模式 - 處理所有策略
        selected_strategies = list(RANKING_STRATEGIES.keys())
        print("🤖 自動模式：處理所有策略")
    else:
        # 互動式選擇策略
        selected_strategies = select_strategies_interactively()
        
        if not selected_strategies:
            return
    
    # 確定要處理的日期
    dates_to_process = []
    
    if args.date:
        dates_to_process = [args.date]
    elif args.start_date and args.end_date:
        # 生成日期範圍
        dates_to_process = generate_date_range(args.start_date, args.end_date)
        print(f"📅 生成日期範圍: {args.start_date} 到 {args.end_date} ({len(dates_to_process)} 天)")
    elif args.all:
        dates_to_process = get_available_dates_from_database()
    else:
        # 默認處理所有可用日期
        print("沒有指定日期參數，默認處理所有可用日期...")
        dates_to_process = get_available_dates_from_database()
        
        if not dates_to_process:
            print("❌ 沒有找到任何return_metrics數據")
            print("請先運行 calculate_FR_return_list.py 生成收益數據")
            print("\n可用參數:")
            print("  --date YYYY-MM-DD  (處理單個日期)")
            print("  --start_date YYYY-MM-DD --end_date YYYY-MM-DD  (處理日期範圍)")
            print("  --all  (處理所有可用日期)")
            print("  --strategy 策略名稱  (指定特定策略)")
            print("  --auto  (自動模式，不互動選擇)")
            print("  --incremental  (增量模式)")
            return
    
    if not dates_to_process:
        print("❌ 沒有找到要處理的日期")
        return
    
    # 增量模式：過濾已處理的組合
    if args.incremental:
        print("🔄 增量模式：檢查已處理的(日期, 策略)組合...")
        existing_combinations = check_existing_strategy_rankings()
        
        # 過濾需要處理的組合
        tasks_to_process = []
        for date in dates_to_process:
            for strategy in selected_strategies:
                if (date, strategy) not in existing_combinations:
                    tasks_to_process.append((date, strategy))
        
        print(f"\n📊 增量分析結果:")
        print(f"   總組合數: {len(dates_to_process) * len(selected_strategies)}")
        print(f"   已處理: {len(existing_combinations)}")
        print(f"   待處理: {len(tasks_to_process)}")
        
        if not tasks_to_process:
            print("\n🎉 所有(日期, 策略)組合都已處理完成！")
            return
        
        # 按日期分組待處理任務
        dates_with_pending_strategies = {}
        for date, strategy in tasks_to_process:
            if date not in dates_with_pending_strategies:
                dates_with_pending_strategies[date] = []
            dates_with_pending_strategies[date].append(strategy)
        
        print(f"\n🚀 開始增量處理...")
        total_successful = 0
        
        for date in sorted(dates_with_pending_strategies.keys()):
            pending_strategies = dates_with_pending_strategies[date]
            print(f"\n📅 處理日期 {date} (待處理策略: {len(pending_strategies)})")
            successful = process_date_with_selected_strategies(date, pending_strategies)
            total_successful += successful
        
        print(f"\n🎉 增量處理完成！")
        print(f"   處理了 {len(dates_with_pending_strategies)} 個日期")
        print(f"   成功處理 {total_successful} 個策略")
        
    else:
        # 常規模式：處理所有指定的日期和策略
        print(f"\n📊 準備處理:")
        print(f"   日期數: {len(dates_to_process)}")
        print(f"   策略數: {len(selected_strategies)}")
        print(f"   總組合: {len(dates_to_process) * len(selected_strategies)}")
        
        if len(dates_to_process) <= 10:
            print(f"   日期: {', '.join(dates_to_process)}")
        else:
            print(f"   日期範圍: {dates_to_process[0]} 到 {dates_to_process[-1]}")
        
        print(f"   策略: {', '.join(selected_strategies)}")
        
        # 處理每個日期
        total_successful = 0
        total_dates_processed = 0
        
        for date in dates_to_process:
            successful = process_date_with_selected_strategies(date, selected_strategies)
            if successful > 0:
                total_dates_processed += 1
                total_successful += successful
        
        print(f"\n🎉 所有處理完成！")
        print(f"   處理了 {total_dates_processed} 個日期")
        print(f"   成功處理 {total_successful} 個策略")

if __name__ == "__main__":
    main() 