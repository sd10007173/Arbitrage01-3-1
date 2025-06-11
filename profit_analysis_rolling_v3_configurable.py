#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
資金費率差價收益分析程式 - 可配置版本
使用配置驅動的排行榜系統
讀取csv/FR_diff資料夾內的所有交易對CSV檔案，為每一天製作一個累積收益統計summary
修改：所有return值乘以-1，並支援多種排行榜策略
"""

import pandas as pd
import os
from datetime import datetime, timedelta
import glob
import numpy as np
from ranking_engine import RankingEngine, quick_test_strategy, compare_strategies
from ranking_config import RANKING_STRATEGIES, EXPERIMENTAL_CONFIGS, list_all_strategies, get_strategy_description


def get_date_input(prompt):
    """獲取使用者輸入的日期"""
    while True:
        try:
            date_str = input(prompt)
            # 支援多種日期格式
            for fmt in ['%Y/%m/%d', '%Y-%m-%d', '%Y%m%d']:
                try:
                    return datetime.strptime(date_str, fmt).date()
                except ValueError:
                    continue
            raise ValueError("日期格式不正確")
        except ValueError:
            print("請輸入正確的日期格式 (例如: 2025/05/01 或 2025-05-01)")


def get_strategy_input():
    """獲取使用者選擇的策略"""
    print("\n🎯 可用的排行榜策略:")
    print("="*50)
    
    # 顯示主要策略
    print("主要策略:")
    for i, (name, config) in enumerate(RANKING_STRATEGIES.items(), 1):
        print(f"  {i}. {name} - {config['name']}")
    
    # 顯示實驗策略
    print("\n實驗策略:")
    exp_start = len(RANKING_STRATEGIES) + 1
    for i, (name, config) in enumerate(EXPERIMENTAL_CONFIGS.items(), exp_start):
        print(f"  {i}. {name} - {config['name']}")
    
    print("\n選項:")
    print("  📋 輸入策略名稱 (例如: original)")
    print("  🔢 輸入數字選擇")
    print("  ❓ 輸入 'help' 查看策略詳細說明")
    print("  🔄 直接Enter使用原始策略")
    
    while True:
        user_input = input("\n請選擇策略: ").strip()
        
        # 直接Enter使用默認策略
        if not user_input:
            return 'original'
        
        # 幫助命令
        if user_input.lower() == 'help':
            strategy_name = input("請輸入要查看的策略名稱: ").strip()
            if strategy_name:
                print("\n" + get_strategy_description(strategy_name))
            continue
        
        # 檢查是否為數字選擇
        if user_input.isdigit():
            choice = int(user_input)
            all_strategies = list(RANKING_STRATEGIES.keys()) + list(EXPERIMENTAL_CONFIGS.keys())
            if 1 <= choice <= len(all_strategies):
                return all_strategies[choice - 1]
            else:
                print(f"❌ 請輸入 1-{len(all_strategies)} 之間的數字")
                continue
        
        # 檢查是否為有效的策略名稱
        if user_input in RANKING_STRATEGIES or user_input in EXPERIMENTAL_CONFIGS:
            return user_input
        
        print(f"❌ 未知的策略: {user_input}")
        print("請重新選擇或輸入 'help' 查看說明")


def generate_unique_filename(output_dir, current_date, create_date, strategy_name='original'):
    """生成唯一的檔案名稱，包含製作日期、策略名稱和序號"""
    base_filename = f"summary_{current_date.strftime('%Y-%m-%d')}_strategy_{strategy_name}_create_{create_date.strftime('%Y-%m-%d')}"

    # 從序號1開始嘗試
    counter = 1
    while True:
        filename = f"{base_filename}({counter}).csv"
        full_path = os.path.join(output_dir, filename)

        # 如果檔案不存在，就使用這個檔名
        if not os.path.exists(full_path):
            return filename, full_path

        counter += 1


def load_trading_pair_data(file_path):
    """載入單一交易對的資料"""
    try:
        df = pd.read_csv(file_path)

        # 檢查必要欄位是否存在
        required_columns = ['Timestamp (UTC)', 'Diff_AB']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            print(f"檔案 {file_path} 缺少必要欄位: {missing_columns}")
            return None

        # 清理資料：移除無效的時間戳記和Diff_AB值
        df_clean = df.dropna(subset=['Timestamp (UTC)', 'Diff_AB']).copy()

        if df_clean.empty:
            print(f"檔案 {file_path} 沒有有效資料")
            return None

        # 確保時間戳記是字符串格式
        df_clean['Timestamp (UTC)'] = df_clean['Timestamp (UTC)'].astype(str)

        # 嘗試轉換時間戳記為datetime格式進行驗證
        try:
            pd.to_datetime(df_clean['Timestamp (UTC)'])
        except:
            print(f"檔案 {file_path} 的時間戳記格式有問題")
            return None

        # 創建DateStr欄位用於日期匹配
        df_clean['DateStr'] = df_clean['Timestamp (UTC)'].str[:10]

        return df_clean

    except Exception as e:
        print(f"讀取檔案 {file_path} 時發生錯誤: {e}")
        return None


def calculate_cumulative_return(df, start_date, current_date):
    """計算從start_date到current_date的累積收益（乘以-1）"""
    # 轉換日期為字符串格式進行比較
    start_str = start_date.strftime('%Y-%m-%d')
    current_str = current_date.strftime('%Y-%m-%d')

    period_data = df[(df['DateStr'] >= start_str) & (df['DateStr'] <= current_str)]
    # 修改：將結果乘以-1
    return period_data['Diff_AB'].sum() * -1 if not period_data.empty else 0


def calculate_recent_return(df, days, end_date, available_start_date):
    """計算最近N天的收益（以end_date為基準往前推算，但不超過available_start_date）（乘以-1）"""
    # 計算理想的開始日期
    ideal_start_date = end_date - timedelta(days=days - 1)  # 包含end_date當天

    # 實際開始日期不能早於可用數據的開始日期
    actual_start_date = max(ideal_start_date, available_start_date)

    end_str = end_date.strftime('%Y-%m-%d')
    start_str = actual_start_date.strftime('%Y-%m-%d')

    # 篩選指定日期範圍的資料
    period_data = df[(df['DateStr'] >= start_str) & (df['DateStr'] <= end_str)]

    # 計算實際天數
    actual_days = (end_date - actual_start_date).days + 1

    # 修改：將結果乘以-1
    return_value = period_data['Diff_AB'].sum() * -1 if not period_data.empty else 0
    return return_value, actual_days


def get_trading_pair_name(filename, df):
    """從檔案名稱和資料提取交易對名稱（包含交易所資訊）"""
    basename = os.path.basename(filename)
    # 移除副檔名和後綴
    base_name = basename.replace('_FR_diff.csv', '')

    # 從資料中獲取交易對符號和交易所資訊
    if not df.empty and 'Symbol' in df.columns and 'Exchange_A' in df.columns and 'Exchange_B' in df.columns:
        # 篩選出有效的資料行
        valid_data = df.dropna(subset=['Symbol', 'Exchange_A', 'Exchange_B'])
        if not valid_data.empty:
            symbol = valid_data['Symbol'].iloc[0]
            exchange_a = valid_data['Exchange_A'].iloc[0]
            exchange_b = valid_data['Exchange_B'].iloc[0]
            return f"{symbol}_{exchange_a}_{exchange_b}"

    # 如果資料為空或無法獲取，使用檔案名稱
    return base_name


def analyze_single_day_configurable(csv_files, start_date, current_date, strategy_name='original'):
    """使用可配置策略分析單一日期的所有交易對收益"""
    results = []

    for file_path in csv_files:
        # 載入資料
        df = load_trading_pair_data(file_path)
        if df is None:
            continue

        # 獲取交易對名稱（包含交易所資訊）
        trading_pair = get_trading_pair_name(file_path, df)

        # 計算duration_days
        duration_days = (current_date - start_date).days + 1

        # 計算各期間收益（以current_date為基準）- 所有return都已經在函數內乘以-1
        all_return = calculate_cumulative_return(df, start_date, current_date)
        day_30_return, actual_30_days = calculate_recent_return(df, 30, current_date, start_date)
        day_14_return, actual_14_days = calculate_recent_return(df, 14, current_date, start_date)
        day_7_return, actual_7_days = calculate_recent_return(df, 7, current_date, start_date)
        day_2_return, actual_2_days = calculate_recent_return(df, 2, current_date, start_date)
        day_1_return, actual_1_days = calculate_recent_return(df, 1, current_date, start_date)

        # 計算年化報酬率（return值已經是乘以-1後的結果，所以ROI也會相應改變）
        all_ROI = all_return * 365 / duration_days if duration_days > 0 else 0
        day_30_ROI = day_30_return * 365 / actual_30_days if actual_30_days > 0 else 0
        day_14_ROI = day_14_return * 365 / actual_14_days if actual_14_days > 0 else 0
        day_7_ROI = day_7_return * 365 / actual_7_days if actual_7_days > 0 else 0
        day_2_ROI = day_2_return * 365 / actual_2_days if actual_2_days > 0 else 0
        day_1_ROI = day_1_return * 365 / actual_1_days if actual_1_days > 0 else 0

        # 儲存基礎數據
        result = {
            'trading_pair': trading_pair,
            'duration_days': duration_days,
            'start_date': start_date.strftime('%Y/%m/%d'),
            'end_date': current_date.strftime('%Y/%m/%d'),
            'all_return': round(all_return, 8),
            'all_ROI': round(all_ROI, 8),
            '30d_return': round(day_30_return, 8),
            '30d_ROI': round(day_30_ROI, 8),
            '14d_return': round(day_14_return, 8),
            '14d_ROI': round(day_14_ROI, 8),
            '7d_return': round(day_7_return, 8),
            '7d_ROI': round(day_7_ROI, 8),
            '2d_return': round(day_2_return, 8),
            '2d_ROI': round(day_2_ROI, 8),
            '1d_return': round(day_1_return, 8),
            '1d_ROI': round(day_1_ROI, 8)
        }

        results.append(result)

    if not results:
        return pd.DataFrame()

    # 使用配置化引擎計算排行榜
    df = pd.DataFrame(results)
    
    try:
        engine = RankingEngine(strategy_name)
        ranked_df = engine.calculate_final_ranking(df)
        return ranked_df
    except Exception as e:
        print(f"❌ 排行榜計算失敗: {e}")
        print("🔄 回退到原始計算方法...")
        
        # 回退到原始計算方法
        # 計算原始的Z分數
        df['all_ROI_Z_score'] = (df['1d_ROI'] + df['2d_ROI'] + df['7d_ROI'] + df['14d_ROI'] + df['30d_ROI'] + df['all_ROI']) / 6
        df['short_ROI_z_score'] = (df['1d_ROI'] + df['2d_ROI'] + df['7d_ROI'] + df['14d_ROI']) / 4
        df['combined_ROI_z_score'] = (df['all_ROI_Z_score'] + df['short_ROI_z_score']) / 2
        df['final_ranking_score'] = df['combined_ROI_z_score']
        
        # 排序
        df = df.sort_values('combined_ROI_z_score', ascending=False).reset_index(drop=True)
        return df


def analyze_trading_pairs_rolling_configurable(start_date, end_date, strategy_name='original'):
    """為每一天分析所有交易對的收益 - 可配置版本"""

    # 確保輸出目錄存在
    output_base_dir = 'csv/FF_profit/summary'
    os.makedirs(output_base_dir, exist_ok=True)

    # 獲取當前日期作為製作日期
    create_date = datetime.now().date()

    # 尋找所有FR_diff檔案
    input_pattern = 'csv/FR_diff/*_FR_diff.csv'
    csv_files = glob.glob(input_pattern)

    if not csv_files:
        print(f"在 {input_pattern} 路徑下未找到任何CSV檔案")
        return

    print(f"找到 {len(csv_files)} 個交易對檔案")
    print(f"使用策略: {strategy_name}")
    
    # 顯示策略說明
    try:
        print("\n" + get_strategy_description(strategy_name))
    except:
        pass

    # 產生日期範圍
    current_date = start_date
    total_days = (end_date - start_date).days + 1
    processed_days = 0

    print(f"\n開始處理 {total_days} 天的資料...")
    print(f"檔案製作日期: {create_date.strftime('%Y-%m-%d')}")
    print("-" * 80)

    while current_date <= end_date:
        processed_days += 1
        print(f"處理第 {processed_days}/{total_days} 天: {current_date.strftime('%Y-%m-%d')}")

        # 分析當天的資料
        summary_df = analyze_single_day_configurable(csv_files, start_date, current_date, strategy_name)

        if not summary_df.empty:
            # 生成唯一檔名（包含策略名稱）
            filename, output_file = generate_unique_filename(output_base_dir, current_date, create_date, strategy_name)
            summary_df.to_csv(output_file, index=False)
            
            print(f"   ✅ 儲存至: {filename}")
            print(f"   📊 {len(summary_df)} 個交易對，前3名:")
            
            # 顯示前3名
            for idx, row in summary_df.head(3).iterrows():
                score = row.get('final_ranking_score', row.get('combined_ROI_z_score', 0))
                print(f"      {idx+1}. {row['trading_pair']:25s} 分數: {score:8.4f}")
        else:
            print(f"   ⚠️ {current_date.strftime('%Y-%m-%d')} 沒有有效資料")

        # 移動到下一天
        current_date += timedelta(days=1)

    print("\n🎉 分析完成！")
    print(f"輸出目錄: {output_base_dir}")


def strategy_comparison_mode():
    """策略比較模式"""
    print("\n🔍 策略比較模式")
    print("="*50)
    
    # 讓用戶選擇一個現有的summary檔案
    summary_pattern = 'csv/FF_profit/summary/summary_*.csv'
    summary_files = glob.glob(summary_pattern)
    
    if not summary_files:
        print("❌ 找不到任何summary檔案")
        print("請先執行分析產生summary檔案")
        return
    
    # 顯示可用的檔案
    print("可用的summary檔案:")
    for i, file_path in enumerate(summary_files[:10], 1):  # 只顯示前10個
        filename = os.path.basename(file_path)
        print(f"  {i}. {filename}")
    
    if len(summary_files) > 10:
        print(f"  ... 還有 {len(summary_files) - 10} 個檔案")
    
    # 讓用戶選擇檔案
    while True:
        try:
            choice = input("\n請選擇檔案編號 (或輸入檔案名稱): ").strip()
            
            if choice.isdigit():
                file_idx = int(choice) - 1
                if 0 <= file_idx < min(10, len(summary_files)):
                    selected_file = summary_files[file_idx]
                    break
                else:
                    print("❌ 無效的編號")
                    continue
            else:
                # 嘗試找到匹配的檔案
                matching_files = [f for f in summary_files if choice in os.path.basename(f)]
                if len(matching_files) == 1:
                    selected_file = matching_files[0]
                    break
                elif len(matching_files) > 1:
                    print(f"找到多個匹配的檔案:")
                    for i, f in enumerate(matching_files[:5], 1):
                        print(f"  {i}. {os.path.basename(f)}")
                    continue
                else:
                    print("❌ 找不到匹配的檔案")
                    continue
        except ValueError:
            print("❌ 請輸入有效的數字")
    
    # 載入選中的檔案
    try:
        df = pd.read_csv(selected_file)
        print(f"\n✅ 載入檔案: {os.path.basename(selected_file)}")
        print(f"📊 包含 {len(df)} 個交易對")
        
        # 策略比較
        strategies_to_compare = ['original', 'momentum_focused', 'stability_focused', 'pure_short_term']
        compare_strategies(df, strategies_to_compare, top_n=10)
        
    except Exception as e:
        print(f"❌ 載入檔案失敗: {e}")


def interactive_strategy_test():
    """交互式策略測試"""
    print("\n🧪 交互式策略測試")
    print("="*50)
    
    # 創建測試數據或載入現有數據
    choice = input("選擇數據來源 (1: 創建測試數據, 2: 載入現有summary): ").strip()
    
    if choice == "1":
        # 創建測試數據
        print("創建測試數據...")
        test_data = {
            'trading_pair': [f'TEST{i}USDT_binance_bybit' for i in range(1, 21)],
            '1d_ROI': np.random.normal(0.05, 0.02, 20),
            '2d_ROI': np.random.normal(0.04, 0.015, 20),
            '7d_ROI': np.random.normal(0.03, 0.01, 20),
            '14d_ROI': np.random.normal(0.025, 0.008, 20),
            '30d_ROI': np.random.normal(0.02, 0.005, 20),
            'all_ROI': np.random.normal(0.015, 0.003, 20)
        }
        df = pd.DataFrame(test_data)
        
    else:
        # 載入現有數據
        summary_pattern = 'csv/FF_profit/summary/summary_*.csv'
        summary_files = glob.glob(summary_pattern)
        
        if not summary_files:
            print("❌ 找不到summary檔案，使用測試數據")
            test_data = {
                'trading_pair': [f'TEST{i}USDT_binance_bybit' for i in range(1, 11)],
                '1d_ROI': np.random.normal(0.05, 0.02, 10),
                '2d_ROI': np.random.normal(0.04, 0.015, 10),
                '7d_ROI': np.random.normal(0.03, 0.01, 10),
                '14d_ROI': np.random.normal(0.025, 0.008, 10),
                '30d_ROI': np.random.normal(0.02, 0.005, 10),
                'all_ROI': np.random.normal(0.015, 0.003, 10)
            }
            df = pd.DataFrame(test_data)
        else:
            try:
                # 使用最新的檔案
                latest_file = max(summary_files, key=os.path.getctime)
                df = pd.read_csv(latest_file)
                print(f"✅ 載入: {os.path.basename(latest_file)}")
            except Exception as e:
                print(f"❌ 載入失敗: {e}")
                return
    
    # 交互式測試
    while True:
        print("\n" + "="*50)
        print("🧪 策略測試選項:")
        print("1. 測試單一策略")
        print("2. 比較多個策略")
        print("3. 查看策略說明")
        print("4. 策略重疊分析")
        print("5. 退出")
        
        choice = input("請選擇: ").strip()
        
        if choice == "1":
            strategy = get_strategy_input()
            print(f"\n測試策略: {strategy}")
            quick_test_strategy(df, strategy)
            
        elif choice == "2":
            print("選擇要比較的策略 (用空格分隔):")
            list_all_strategies()
            strategies_input = input("策略名稱: ").strip()
            strategies = strategies_input.split()
            
            if len(strategies) >= 2:
                compare_strategies(df, strategies)
            else:
                print("❌ 請至少選擇2個策略")
                
        elif choice == "3":
            strategy = input("請輸入策略名稱: ").strip()
            if strategy:
                print(get_strategy_description(strategy))
                
        elif choice == "4":
            print("選擇要分析的策略 (用空格分隔):")
            strategies_input = input("策略名稱: ").strip()
            strategies = strategies_input.split()
            
            if len(strategies) >= 2:
                from ranking_engine import strategy_overlap_analysis
                strategy_overlap_analysis(df, strategies)
            else:
                print("❌ 請至少選擇2個策略")
                
        elif choice == "5":
            break
            
        else:
            print("❌ 無效的選擇")


def main():
    print("🚀 可配置排行榜分析工具 v3.0")
    print("="*60)
    
    print("選擇模式:")
    print("1. 📊 分析模式 - 產生新的summary檔案")
    print("2. 🔍 比較模式 - 比較現有summary的不同策略結果")
    print("3. 🧪 測試模式 - 交互式策略測試")
    print("4. 📋 查看可用策略")
    
    mode = input("請選擇模式 (1-4): ").strip()
    
    if mode == "1":
        # 分析模式
        print("\n📊 分析模式")
        print("="*30)
        
        # 獲取日期範圍
        start_date = get_date_input("請輸入開始日期: ")
        end_date = get_date_input("請輸入結束日期: ")
        
        if start_date > end_date:
            print("❌ 開始日期不能晚於結束日期")
            return
        
        # 選擇策略
        strategy_name = get_strategy_input()
        
        # 執行分析
        analyze_trading_pairs_rolling_configurable(start_date, end_date, strategy_name)
        
    elif mode == "2":
        # 比較模式
        strategy_comparison_mode()
        
    elif mode == "3":
        # 測試模式
        interactive_strategy_test()
        
    elif mode == "4":
        # 查看策略
        print("\n📋 可用策略列表:")
        list_all_strategies()
        
        strategy = input("\n輸入策略名稱查看詳細說明 (或Enter跳過): ").strip()
        if strategy:
            print(get_strategy_description(strategy))
            
    else:
        print("❌ 無效的模式選擇")


if __name__ == "__main__":
    main() 