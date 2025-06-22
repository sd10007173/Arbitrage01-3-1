"""
因子策略系統測試腳本 (Test Factor Strategies)

此腳本用於測試因子策略系統的各個組件：
1. 因子計算函數測試
2. 因子引擎測試
3. 數據庫操作測試
4. 完整流程測試

使用方法：
    python factor_strategies/test_factor_strategies.py
"""

import sys
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# 添加父目錄到 Python 路徑
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from factor_strategies.factor_library import *
from factor_strategies.factor_engine import FactorEngine
from factor_strategies.factor_strategy_config import FACTOR_STRATEGIES
from database_operations import DatabaseManager

def test_factor_functions():
    """測試因子計算函數"""
    print("🧪 測試因子計算函數...")
    
    # 創建測試數據
    test_data = pd.Series([0.001, 0.002, -0.001, 0.003, 0.001, -0.002, 0.004, 0.002, 0.001, 0.003])
    
    tests = [
        ("趨勢斜率", calculate_trend_slope, test_data, {}),
        ("夏普比率", calculate_sharpe_ratio, test_data, {'annualizing_factor': 365}),
        ("穩定性指標", calculate_inv_std_dev, test_data, {'epsilon': 1e-9}),
        ("勝率", calculate_win_rate, test_data, {}),
        ("最大回撤", calculate_max_drawdown, test_data, {}),
        ("索提諾比率", calculate_sortino_ratio, test_data, {'annualizing_factor': 365}),
    ]
    
    results = []
    for name, func, data, params in tests:
        try:
            result = func(data, **params)
            results.append((name, result, "✅"))
            print(f"   ✅ {name}: {result:.6f}")
        except Exception as e:
            results.append((name, str(e), "❌"))
            print(f"   ❌ {name}: {e}")
    
    success_count = sum(1 for _, _, status in results if status == "✅")
    print(f"\n📊 因子函數測試結果: {success_count}/{len(tests)} 通過")
    
    return success_count == len(tests)

def test_database_operations():
    """測試數據庫操作"""
    print("\n🧪 測試數據庫操作...")
    
    try:
        db_manager = DatabaseManager()
        
        # 檢查數據庫信息
        info = db_manager.get_database_info()
        print(f"   ✅ 數據庫連接成功: {info['database_path']}")
        
        # 檢查是否有 return_metrics 數據
        start_date, end_date = db_manager.get_return_metrics_date_range()
        if end_date:
            print(f"   ✅ return_metrics 數據範圍: {start_date} 到 {end_date}")
            
            # 測試數據查詢
            sample_data = db_manager.get_return_metrics(end_date=end_date)
            print(f"   ✅ 成功查詢到 {len(sample_data)} 條樣本數據")
            
            return True
        else:
            print("   ⚠️ 數據庫中沒有 return_metrics 數據，需要先運行 calculate_FR_return_list_v2.py")
            return False
            
    except Exception as e:
        print(f"   ❌ 數據庫操作測試失敗: {e}")
        return False

def test_strategy_config():
    """測試策略配置"""
    print("\n🧪 測試策略配置...")
    
    config_tests = []
    
    for strategy_name, config in FACTOR_STRATEGIES.items():
        try:
            # 檢查必需的配置項
            required_keys = ['name', 'description', 'data_requirements', 'factors', 'ranking_logic']
            missing_keys = [key for key in required_keys if key not in config]
            
            if missing_keys:
                config_tests.append((strategy_name, f"缺少配置項: {missing_keys}", "❌"))
                continue
            
            # 檢查因子配置
            factors = config['factors']
            for factor_name, factor_config in factors.items():
                required_factor_keys = ['function', 'window', 'input_col']
                missing_factor_keys = [key for key in required_factor_keys if key not in factor_config]
                
                if missing_factor_keys:
                    config_tests.append((strategy_name, f"因子 {factor_name} 缺少配置: {missing_factor_keys}", "❌"))
                    continue
            
            # 檢查排名邏輯
            ranking_logic = config['ranking_logic']
            indicators = ranking_logic.get('indicators', [])
            weights = ranking_logic.get('weights', [])
            
            if len(indicators) != len(weights):
                config_tests.append((strategy_name, "因子數量與權重數量不匹配", "❌"))
                continue
            
            if abs(sum(weights) - 1.0) > 0.001:
                config_tests.append((strategy_name, f"權重總和不為1: {sum(weights)}", "❌"))
                continue
            
            config_tests.append((strategy_name, "配置正確", "✅"))
            print(f"   ✅ {strategy_name}: 配置正確")
            
        except Exception as e:
            config_tests.append((strategy_name, str(e), "❌"))
            print(f"   ❌ {strategy_name}: {e}")
    
    success_count = sum(1 for _, _, status in config_tests if status == "✅")
    print(f"\n📊 策略配置測試結果: {success_count}/{len(FACTOR_STRATEGIES)} 通過")
    
    return success_count == len(FACTOR_STRATEGIES)

def test_factor_engine():
    """測試因子引擎"""
    print("\n🧪 測試因子引擎...")
    
    try:
        # 初始化引擎
        engine = FactorEngine()
        print("   ✅ 因子引擎初始化成功")
        
        # 檢查數據可用性
        start_date, end_date = engine.db_manager.get_return_metrics_date_range()
        if not end_date:
            print("   ⚠️ 沒有可用數據，跳過引擎測試")
            return False
        
        # 測試簡單策略
        test_strategy = 'test_factor_simple'
        if test_strategy in FACTOR_STRATEGIES:
            print(f"   🧮 測試策略: {test_strategy}")
            
            # 不保存到數據庫的測試
            result = engine.run_strategy(test_strategy, end_date, save_to_db=False)
            
            if not result.empty:
                print(f"   ✅ 策略計算成功，得到 {len(result)} 個結果")
                print(f"   📊 分數範圍: {result['final_ranking_score'].min():.6f} ~ {result['final_ranking_score'].max():.6f}")
                return True
            else:
                print("   ❌ 策略計算沒有結果")
                return False
        else:
            print(f"   ⚠️ 測試策略 {test_strategy} 不存在")
            return False
            
    except Exception as e:
        print(f"   ❌ 因子引擎測試失敗: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_full_workflow():
    """測試完整工作流程"""
    print("\n🧪 測試完整工作流程...")
    
    try:
        engine = FactorEngine()
        
        # 檢查數據
        start_date, end_date = engine.db_manager.get_return_metrics_date_range()
        if not end_date:
            print("   ⚠️ 沒有可用數據，跳過完整流程測試")
            return False
        
        # 選擇一個簡單的策略進行測試
        test_strategy = 'test_factor_simple'
        
        if test_strategy not in FACTOR_STRATEGIES:
            print(f"   ⚠️ 測試策略 {test_strategy} 不存在")
            return False
        
        print(f"   🚀 執行完整流程測試: {test_strategy}")
        
        # 執行策略（保存到數據庫）
        result = engine.run_strategy(test_strategy, end_date, save_to_db=True)
        
        if result.empty:
            print("   ❌ 策略執行沒有結果")
            return False
        
        # 驗證數據是否已保存
        saved_result = engine.db_manager.get_latest_factor_ranking(test_strategy, top_n=5)
        
        if saved_result.empty:
            print("   ❌ 數據沒有成功保存到數據庫")
            return False
        
        print(f"   ✅ 完整流程測試成功")
        print(f"   📊 計算結果: {len(result)} 個交易對")
        print(f"   💾 數據庫保存: {len(saved_result)} 條記錄")
        
        # 顯示前5名結果
        print("   🏆 前5名結果:")
        for _, row in saved_result.head(5).iterrows():
            print(f"      {row['rank_position']:2d}. {row['trading_pair']:<20} {row['final_ranking_score']:.6f}")
        
        return True
        
    except Exception as e:
        print(f"   ❌ 完整流程測試失敗: {e}")
        import traceback
        traceback.print_exc()
        return False

def run_all_tests():
    """運行所有測試"""
    print("🧪 開始因子策略系統測試")
    print("=" * 50)
    
    tests = [
        ("因子計算函數", test_factor_functions),
        ("數據庫操作", test_database_operations),
        ("策略配置", test_strategy_config),
        ("因子引擎", test_factor_engine),
        ("完整工作流程", test_full_workflow),
    ]
    
    results = []
    
    for test_name, test_func in tests:
        try:
            success = test_func()
            results.append((test_name, "✅" if success else "❌"))
        except Exception as e:
            print(f"   ❌ 測試 '{test_name}' 執行時出錯: {e}")
            results.append((test_name, "❌"))
    
    # 顯示測試摘要
    print("\n" + "=" * 50)
    print("📊 測試結果摘要:")
    print("-" * 30)
    
    success_count = 0
    for test_name, status in results:
        print(f"{status} {test_name}")
        if status == "✅":
            success_count += 1
    
    print("-" * 30)
    print(f"總體結果: {success_count}/{len(tests)} 測試通過")
    
    if success_count == len(tests):
        print("🎉 所有測試通過！因子策略系統運行正常。")
    else:
        print("⚠️ 部分測試失敗，請檢查系統配置。")
    
    return success_count == len(tests)

if __name__ == "__main__":
    run_all_tests() 