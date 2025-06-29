"""
Phase 1 修改驗證測試

此腳本驗證以下修改是否有效：
1. input_col 從 'roi_1d' 改為 'return_1d'
2. calculate_trend_slope 函數修正（移除 cumsum）

測試內容：
- 比較使用 roi_1d 和 return_1d 的因子計算結果
- 驗證趨勢計算函數的修正
- 檢查數值範圍是否合理
"""

import sys
import os
sys.path.append('factor_strategies')

import pandas as pd
import numpy as np
from factor_strategies.factor_library import *
from factor_strategies.factor_engine import FactorEngine
from factor_strategies.factor_strategy_config import FACTOR_STRATEGIES

def test_trend_slope_fix():
    """測試趨勢斜率計算的修正"""
    print("=== 測試 1: 趨勢斜率計算修正 ===")
    
    # 測試等差數列
    test_data = pd.Series([1, 2, 3, 4, 5])
    result = calculate_trend_slope(test_data)
    print(f"輸入: [1, 2, 3, 4, 5]")
    print(f"結果: {result}")
    print(f"預期: 1.0")
    print(f"✅ 通過: {abs(result - 1.0) < 1e-10}")
    print()
    
    # 測試實際收益率範圍
    real_data = pd.Series([0.001, 0.002, 0.003, 0.004, 0.005])
    result2 = calculate_trend_slope(real_data)
    print(f"輸入: [0.001, 0.002, 0.003, 0.004, 0.005]")
    print(f"結果: {result2}")
    print(f"預期: 0.001")
    print(f"✅ 通過: {abs(result2 - 0.001) < 1e-10}")
    print()

def test_data_range_comparison():
    """比較 roi_1d 和 return_1d 的數值範圍"""
    print("=== 測試 2: 數值範圍比較 ===")
    
    try:
        # 初始化因子引擎
        engine = FactorEngine()
        
        # 獲取一些樣本數據
        strategy_config = FACTOR_STRATEGIES['cerebrum_core']
        df = engine.get_strategy_data(strategy_config, target_date='2024-06-20')
        
        if df.empty:
            print("❌ 沒有數據可供測試")
            return
        
        # 選擇一個交易對進行測試
        sample_pair = df['trading_pair'].iloc[0]
        pair_data = df[df['trading_pair'] == sample_pair].tail(90)  # 最近90天
        
        print(f"測試交易對: {sample_pair}")
        print(f"數據點數: {len(pair_data)}")
        print()
        
        # 比較 roi_1d 和 return_1d 的數值範圍
        if 'roi_1d' in pair_data.columns and 'return_1d' in pair_data.columns:
            roi_1d_stats = pair_data['roi_1d'].describe()
            return_1d_stats = pair_data['return_1d'].describe()
            
            print("roi_1d 統計:")
            print(f"  平均值: {roi_1d_stats['mean']:.6f}")
            print(f"  標準差: {roi_1d_stats['std']:.6f}")
            print(f"  最小值: {roi_1d_stats['min']:.6f}")
            print(f"  最大值: {roi_1d_stats['max']:.6f}")
            print()
            
            print("return_1d 統計:")
            print(f"  平均值: {return_1d_stats['mean']:.6f}")
            print(f"  標準差: {return_1d_stats['std']:.6f}")
            print(f"  最小值: {return_1d_stats['min']:.6f}")
            print(f"  最大值: {return_1d_stats['max']:.6f}")
            print()
            
            # 計算比例關係
            ratio = roi_1d_stats['mean'] / return_1d_stats['mean'] if return_1d_stats['mean'] != 0 else 0
            print(f"roi_1d / return_1d 比例: {ratio:.1f}")
            print(f"✅ 接近365倍: {abs(ratio - 365) < 50}")
            print()
        
    except Exception as e:
        print(f"❌ 測試失敗: {e}")
        print()

def test_factor_calculation():
    """測試因子計算結果"""
    print("=== 測試 3: 因子計算結果 ===")
    
    try:
        # 創建測試數據
        test_data = pd.DataFrame({
            'return_1d': [0.001, 0.002, -0.001, 0.003, -0.002, 0.001, 0.002] * 10,  # 70個點
            'roi_1d': [0.365, 0.73, -0.365, 1.095, -0.73, 0.365, 0.73] * 10
        })
        
        print(f"測試數據點數: {len(test_data)}")
        
        # 測試各個因子函數
        factors_to_test = [
            ('趨勢因子', calculate_trend_slope),
            ('夏普比率', lambda x: calculate_sharpe_ratio(x, annualizing_factor=365)),
            ('穩定性因子', lambda x: calculate_inv_std_dev(x, epsilon=1e-9)),
            ('勝率因子', calculate_win_rate)
        ]
        
        for factor_name, factor_func in factors_to_test:
            print(f"\n{factor_name}:")
            
            # 使用 return_1d
            result_return = factor_func(test_data['return_1d'])
            print(f"  使用 return_1d: {result_return:.6f}")
            
            # 使用 roi_1d
            result_roi = factor_func(test_data['roi_1d'])
            print(f"  使用 roi_1d: {result_roi:.6f}")
            
            # 檢查結果是否合理
            if not (np.isnan(result_return) or np.isnan(result_roi)):
                if factor_name == '趨勢因子':
                    # 趨勢因子應該保持相同的符號
                    same_sign = (result_return * result_roi) > 0
                    print(f"  ✅ 符號一致: {same_sign}")
                elif factor_name == '勝率因子':
                    # 勝率應該相同（因為只看正負）
                    same_winrate = abs(result_return - result_roi) < 1e-10
                    print(f"  ✅ 勝率相同: {same_winrate}")
            
    except Exception as e:
        print(f"❌ 測試失敗: {e}")

def main():
    """執行所有測試"""
    print("🧪 Phase 1 修改驗證測試開始")
    print("=" * 50)
    
    test_trend_slope_fix()
    test_data_range_comparison()
    test_factor_calculation()
    
    print("=" * 50)
    print("🎉 Phase 1 修改驗證測試完成")

if __name__ == "__main__":
    main() 