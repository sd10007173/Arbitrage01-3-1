"""
因子策略執行腳本 (Run Factor Strategies) - 簡化版本

此腳本提供簡化的交互式界面來執行因子策略系統。
自動從數據庫檢測完整日期範圍並執行，用戶只需選擇策略即可。

使用方法：
    python factor_strategies/run_factor_strategies.py

主要特性：
- 自動從 return_metrics 表檢測完整可用日期範圍
- 簡潔的策略選擇界面
- 結果保存到 strategy_ranking 表（與既有系統整合）
- 支持批量日期範圍執行
- 完整的日期和數據驗證
"""

import sys
import os
from datetime import datetime, timedelta
import pandas as pd
import argparse

# 添加父目錄到 Python 路徑
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from factor_strategies.factor_engine import FactorEngine
from factor_strategies.factor_strategy_config import FACTOR_STRATEGIES
from database_operations import DatabaseManager

def print_header():
    """打印程式標題"""
    print("=" * 60)
    print("🧠 因子策略系統 (Factor Strategy System)")
    print("   自動日期範圍版本 - 簡化交互界面")
    print("=" * 60)

def print_available_strategies():
    """顯示所有可用策略 - 簡化版本"""
    print("\n📋 可用的因子策略:")
    print("-" * 30)
    for i, strategy_key in enumerate(FACTOR_STRATEGIES.keys(), 1):
        print(f"{i:2d}. {strategy_key}")
    
    print(f"{len(FACTOR_STRATEGIES)+1:2d}. 全部策略 (all)")
    print(" 0. 退出")

def select_strategy_interactively():
    """交互式選擇策略 - 簡化版本"""
    print_available_strategies()
    
    strategies = list(FACTOR_STRATEGIES.keys())
    
    while True:
        try:
            choice = input(f"\n請選擇要執行的策略 (0-{len(strategies)+1}): ").strip()
            
            if choice == '0':
                print("已取消執行")
                return None
            elif choice == str(len(strategies)+1) or choice.lower() == 'all':
                print(f"✅ 已選擇全部策略")
                return 'all'
            elif choice.isdigit():
                choice_num = int(choice)
                if 1 <= choice_num <= len(strategies):
                    selected_strategy = strategies[choice_num - 1]
                    print(f"✅ 已選擇策略: {selected_strategy}")
                    return selected_strategy
                else:
                    print(f"❌ 請輸入 0-{len(strategies)+1} 之間的數字")
            else:
                print("❌ 請輸入有效的數字")
                
        except KeyboardInterrupt:
            print("\n\n已取消執行")
            return None
        except Exception as e:
            print(f"❌ 輸入錯誤: {e}")

def run_date_range(engine: FactorEngine, strategy_name: str, start_date: str, end_date: str):
    """執行日期範圍內的策略計算"""
    print(f"\n🚀 執行策略: {strategy_name}")
    print(f"📅 日期範圍: {start_date} 到 {end_date}")
    
    # 生成日期列表
    start_date_obj = pd.to_datetime(start_date)
    end_date_obj = pd.to_datetime(end_date)
    date_range = pd.date_range(start=start_date_obj, end=end_date_obj, freq='D')
    
    success_count = 0
    total_count = len(date_range)
    
    for current_date in date_range:
        current_date_str = current_date.strftime('%Y-%m-%d')
        print(f"\n📅 處理日期: {current_date_str}")
        
        try:
            # 檢查數據充足性
            is_sufficient, message = engine.check_data_sufficiency(strategy_name, current_date_str)
            
            if not is_sufficient:
                print(f"⚠️ 跳過: {message}")
                continue
            
            # 執行策略
            result = engine.run_strategy(strategy_name, current_date_str)
            
            if not result.empty:
                success_count += 1
                print(f"✅ 成功: {len(result)} 個交易對")
            else:
                print("❌ 沒有結果")
                
        except Exception as e:
            print(f"❌ 失敗: {e}")
    
    print(f"\n📊 執行完成: {success_count}/{total_count} 天成功")

def main():
    """主函數 - 簡化版本"""
    print_header()
    
    # 添加命令行參數支持
    parser = argparse.ArgumentParser(description="因子策略執行系統 - 簡化版本")
    parser.add_argument("--strategy", help="指定策略名稱 (或 'all' 執行所有策略)")
    
    args = parser.parse_args()
    
    # 初始化引擎
    try:
        engine = FactorEngine()
        db_manager = engine.db_manager
    except Exception as e:
        print(f"❌ 初始化失敗: {e}")
        return
    
    # 1. 自動獲取完整的日期範圍
    print("ℹ️ 正在從數據庫檢測可用日期範圍...")
    start_date, end_date = db_manager.get_return_metrics_date_range()
    
    if not start_date or not end_date:
        print("❌ 數據庫中沒有 return_metrics 數據，請先確保數據已導入")
        return
    
    print(f"✅ 檢測到數據日期範圍: {start_date} 到 {end_date}")
    
    # 計算執行天數
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    total_days = len(date_range)
    print(f"📊 將處理 {total_days} 天的數據")
    
    # 2. 確定策略
    if args.strategy:
        if args.strategy == 'all' or args.strategy in FACTOR_STRATEGIES:
            strategy_name = args.strategy
            print(f"✅ 命令行指定策略: {strategy_name}")
        else:
            print(f"❌ 策略 {args.strategy} 不存在")
            print(f"可用策略: {list(FACTOR_STRATEGIES.keys())} 或 'all'")
            return
    else:
        strategy_name = select_strategy_interactively()
        if strategy_name is None:
            return
    
    # 3. 執行策略
    if strategy_name == 'all':
        print(f"\n正在執行所有策略從 {start_date} 到 {end_date}...")
        # 執行所有策略
        for strategy in FACTOR_STRATEGIES.keys():
            print(f"\n執行策略: {strategy}")
            run_date_range(engine, strategy, start_date, end_date)
            print(f"策略 {strategy} 執行完成")
        print(f"\n🎉 所有策略執行完成！")
    else:
        # 執行單個策略
        print(f"\n正在執行策略 '{strategy_name}' 從 {start_date} 到 {end_date}...")
        run_date_range(engine, strategy_name, start_date, end_date)
        print(f"\n🎉 策略執行完成！")
    
    # 4. 自動顯示執行結果
    print("\n📊 執行結果:")
    
    try:
        if strategy_name == 'all':
            # 如果執行了所有策略，顯示最後一個策略的結果
            strategies = list(FACTOR_STRATEGIES.keys())
            selected_strategy = strategies[-1]  # 使用最後一個策略
            print(f"顯示最後執行的策略結果: {selected_strategy}")
        else:
            selected_strategy = strategy_name
        
        # 查看結果
        print(f"\n策略: {selected_strategy}, 日期: {end_date}")
        result = db_manager.get_latest_ranking(selected_strategy, top_n=10)
        
        if not result.empty:
            print("排名前10的交易對:")
            print("-" * 60)
            print(f"{'排名':<4} {'交易對':<20} {'分數':<12}")
            print("-" * 60)
            
            for _, row in result.iterrows():
                print(f"{row['rank_position']:<4} {row['trading_pair']:<20} {row['final_ranking_score']:<12.6f}")
        else:
            print("❌ 沒有找到結果")
            
    except Exception as e:
        print(f"❌ 查看結果失敗: {e}")

if __name__ == "__main__":
    main() 