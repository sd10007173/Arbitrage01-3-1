"""
因子策略執行腳本 (Run Factor Strategies) - 智能日期範圍版本

此腳本提供簡化的交互式界面來執行因子策略系統。
自動從數據庫檢測可用日期範圍，支持單個策略執行、批量執行、結果查看等功能。

使用方法：
    python factor_strategies/run_factor_strategies.py

主要特性：
- 自動從 return_metrics 表檢測可用日期範圍
- 智能預設值，預設執行最新日期
- 結果保存到 strategy_ranking 表（與既有系統整合）
- 支持批量日期範圍執行
- 完整的日期和數據驗證
- 直觀的結果查看界面
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
    print("   智能日期範圍版本 - 自動檢測可用數據")
    print("=" * 60)

def print_available_strategies():
    """顯示所有可用策略"""
    print("\n📋 可用的因子策略:")
    print("-" * 50)
    for i, (key, config) in enumerate(FACTOR_STRATEGIES.items(), 1):
        print(f"{i:2d}. {key}")
        print(f"    名稱: {config['name']}")
        print(f"    描述: {config['description']}")
        print(f"    因子數量: {len(config['factors'])}")
        print()

def select_strategy_interactively():
    """交互式選擇策略"""
    print_available_strategies()
    
    # 獲取策略列表
    from factor_strategies.factor_strategy_config import FACTOR_STRATEGIES
    
    while True:
        strategy_input = input("\n請選擇要執行的策略 (輸入策略名稱或 'all' 執行所有策略): ").strip()
        
        if strategy_input.lower() == 'all':
            return 'all'
        elif strategy_input in FACTOR_STRATEGIES:
            return strategy_input
        else:
            # 嘗試按編號選擇
            try:
                strategies = list(FACTOR_STRATEGIES.keys())
                choice_num = int(strategy_input)
                if 1 <= choice_num <= len(strategies):
                    return strategies[choice_num - 1]
                else:
                    print(f"❌ 請輸入 1-{len(strategies)} 之間的數字，或策略名稱，或 'all'")
            except ValueError:
                print(f"❌ 無效輸入。可用策略: {list(FACTOR_STRATEGIES.keys())} 或 'all'")

def run_single_strategy(engine: FactorEngine, strategy_name: str, target_date: str):
    """執行單個策略"""
    print(f"\n🚀 執行策略: {strategy_name}")
    print(f"📅 目標日期: {target_date}")
    
    # 預檢查數據是否充足
    print("\n🔍 檢查數據充足性...")
    is_sufficient, message = engine.check_data_sufficiency(strategy_name, target_date)
    
    if not is_sufficient:
        print(f"❌ 數據量檢查失敗: {message}")
        print("\n💡 建議:")
        print("   • 選擇較晚的日期 (如最新日期)")
        print("   • 選擇數據要求較低的策略")
        print("   • 確認是否有足夠的歷史數據")
        
        # 詢問用戶是否要查看策略要求
        show_req = input("\n❓ 是否查看策略數據要求? (y/n): ").strip().lower()
        if show_req in ['y', 'yes']:
            from factor_strategies.factor_strategy_config import FACTOR_STRATEGIES
            config = FACTOR_STRATEGIES[strategy_name]
            print(f"\n📋 {config['name']} 策略要求:")
            print(f"   • 最少數據天數: {config['data_requirements']['min_data_days']} 天")
            print(f"   • 跳過前幾天: {config['data_requirements']['skip_first_n_days']} 天")
            print(f"   • 總計需要: {config['data_requirements']['min_data_days'] + config['data_requirements']['skip_first_n_days']} 天")
            
            # 顯示因子窗口要求
            print(f"   • 因子窗口:")
            for factor_name, factor_config in config['factors'].items():
                print(f"     - {factor_name}: {factor_config['window']} 天")
        
        return
    
    print(f"✅ 數據量檢查通過: {message}")
    
    try:
        result = engine.run_strategy(strategy_name, target_date)
        
        if not result.empty:
            print(f"\n✅ 策略 '{strategy_name}' 執行成功!")
            print(f"📊 共計算 {len(result)} 個交易對")
            
            # 顯示統計信息
            print(f"\n📈 分數統計:")
            print(f"   最高分: {result['final_ranking_score'].max():.6f}")
            print(f"   最低分: {result['final_ranking_score'].min():.6f}")
            print(f"   平均分: {result['final_ranking_score'].mean():.6f}")
            
        else:
            print(f"⚠️ 策略 '{strategy_name}' 沒有產生結果")
            
    except Exception as e:
        print(f"❌ 策略執行失敗: {e}")
        import traceback
        traceback.print_exc()

def run_all_strategies(engine: FactorEngine, target_date: str):
    """執行所有策略"""
    print(f"\n🚀 執行所有策略")
    print(f"📅 目標日期: {target_date}")
    
    results = engine.run_all_strategies(target_date)
    
    # 統計結果
    success_count = sum(1 for df in results.values() if not df.empty)
    total_count = len(results)
    
    print(f"\n📊 執行結果摘要:")
    print(f"   成功: {success_count}/{total_count} 個策略")
    
    for strategy_name, result_df in results.items():
        if not result_df.empty:
            print(f"   ✅ {strategy_name}: {len(result_df)} 個交易對")
        else:
            print(f"   ❌ {strategy_name}: 執行失敗")

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
    """主函數 - 智能日期範圍版本"""
    print_header()
    
    # 添加命令行參數支持
    parser = argparse.ArgumentParser(description="因子策略執行系統 - 智能日期範圍版本")
    parser.add_argument("--start_date", help="開始日期 (YYYY-MM-DD)")
    parser.add_argument("--end_date", help="結束日期 (YYYY-MM-DD)")
    parser.add_argument("--strategy", help="指定策略名稱 (或 'all' 執行所有策略)")
    parser.add_argument("--interactive", action="store_true", help="強制進入交互模式")
    
    args = parser.parse_args()
    
    # 初始化引擎
    try:
        engine = FactorEngine()
        db_manager = engine.db_manager
    except Exception as e:
        print(f"❌ 初始化失敗: {e}")
        return
    
    # 1. 自動獲取可用的日期範圍
    print("ℹ️ 正在從數據庫檢測可用日期範圍...")
    db_start_date, db_end_date = db_manager.get_return_metrics_date_range()
    
    if not db_start_date or not db_end_date:
        print("❌ 數據庫中沒有 return_metrics 數據，請先確保數據已導入")
        return
    
    print(f"✅ 檢測到數據日期範圍: {db_start_date} 到 {db_end_date}")
    
    # 2. 確定日期範圍
    if args.start_date or args.end_date or args.interactive:
        # 如果有命令行參數或強制交互模式，使用指定的日期或進入交互模式
        start_date = args.start_date
        end_date = args.end_date
        
        if not start_date or not end_date:
            print(f"\n📅 設定執行日期範圍:")
            print(f"   數據庫可用範圍: {db_start_date} 到 {db_end_date}")
            
            # 智能預設值
            default_end_date = db_end_date
            default_start_date = db_end_date  # 預設只執行最新一天
            
            if not start_date:
                while True:
                    start_input = input(f"請輸入起始日期 (YYYY-MM-DD, 預設: {default_start_date}): ").strip()
                    if not start_input:
                        start_date = default_start_date
                        break
                    else:
                        try:
                            # 驗證日期格式和範圍
                            start_date_obj = pd.to_datetime(start_input)
                            db_start_obj = pd.to_datetime(db_start_date)
                            db_end_obj = pd.to_datetime(db_end_date)
                            
                            if start_date_obj < db_start_obj:
                                print(f"❌ 起始日期不能早於數據庫最早日期 {db_start_date}")
                                continue
                            elif start_date_obj > db_end_obj:
                                print(f"❌ 起始日期不能晚於數據庫最晚日期 {db_end_date}")
                                continue
                            else:
                                start_date = start_input
                                break
                        except:
                            print("❌ 日期格式錯誤，請使用 YYYY-MM-DD 格式")
                            continue
            
            if not end_date:
                while True:
                    end_input = input(f"請輸入結束日期 (YYYY-MM-DD, 預設: {default_end_date}): ").strip()
                    if not end_input:
                        end_date = default_end_date
                        break
                    else:
                        try:
                            # 驗證日期格式和範圍
                            end_date_obj = pd.to_datetime(end_input)
                            start_date_obj = pd.to_datetime(start_date)
                            db_start_obj = pd.to_datetime(db_start_date)
                            db_end_obj = pd.to_datetime(db_end_date)
                            
                            if end_date_obj < db_start_obj:
                                print(f"❌ 結束日期不能早於數據庫最早日期 {db_start_date}")
                                continue
                            elif end_date_obj > db_end_obj:
                                print(f"❌ 結束日期不能晚於數據庫最晚日期 {db_end_date}")
                                continue
                            elif end_date_obj < start_date_obj:
                                print(f"❌ 結束日期不能早於起始日期 {start_date}")
                                continue
                            else:
                                end_date = end_input
                                break
                        except:
                            print("❌ 日期格式錯誤，請使用 YYYY-MM-DD 格式")
                            continue
    else:
        # 智能模式：直接使用最新日期
        start_date = db_end_date
        end_date = db_end_date
        print(f"🤖 智能模式：自動使用最新日期 {db_end_date}")
    
    # 3. 確定策略
    if args.strategy:
        strategy_name = args.strategy
        print(f"✅ 指定策略: {strategy_name}")
    else:
        strategy_name = select_strategy_interactively()
    
    # 4. 確認執行參數
    print(f"\n🚀 執行參數確認:")
    print(f"   策略: {strategy_name}")
    print(f"   日期範圍: {start_date} 到 {end_date}")
    
    # 計算執行天數
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    total_days = len(date_range)
    print(f"   執行天數: {total_days} 天")
    
    # 如果是批量執行多天，給出提醒
    if total_days > 7:
        confirm = input(f"\n⚠️ 將執行 {total_days} 天的數據，可能需要較長時間。是否繼續? (y/n): ").strip().lower()
        if confirm not in ['y', 'yes']:
            print("已取消執行")
            return
    
    # 5. 執行策略
    if strategy_name == 'all':
        print(f"\n正在執行所有策略從 {start_date} 到 {end_date}...")
        # 執行所有策略
        from factor_strategies.factor_strategy_config import FACTOR_STRATEGIES
        for strategy in FACTOR_STRATEGIES.keys():
            print(f"\n執行策略: {strategy}")
            run_date_range(engine, strategy, start_date, end_date)
            print(f"策略 {strategy} 執行完成")
        print(f"\n所有策略執行完成！")
    else:
        # 執行單個策略
        print(f"\n正在執行策略 '{strategy_name}' 從 {start_date} 到 {end_date}...")
        run_date_range(engine, strategy_name, start_date, end_date)
        print(f"\n策略執行完成！")
    
    # 6. 自動顯示執行結果
    print("\n📊 執行結果:")
    
    try:
        if strategy_name == 'all':
            # 如果執行了所有策略，顯示最後一個策略的結果
            from factor_strategies.factor_strategy_config import FACTOR_STRATEGIES
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
            print(f"{'排名':<4} {'交易對':<15} {'分數':<12} {'組合分數'}")
            print("-" * 60)
            
            for _, row in result.iterrows():
                # 嘗試解析 component_scores
                component_info = ""
                if 'component_scores' in row and pd.notna(row['component_scores']):
                    try:
                        import json
                        scores = json.loads(row['component_scores'])
                        component_info = f"({', '.join([f'{k}: {v:.3f}' for k, v in scores.items()])})"
                    except:
                        pass
                
                print(f"{row['rank_position']:<4} {row['trading_pair']:<15} "
                      f"{row['final_ranking_score']:<12.6f} {component_info}")
        else:
            print("❌ 沒有找到結果")
            
    except Exception as e:
        print(f"❌ 查看結果失敗: {e}")

if __name__ == "__main__":
    main() 