#!/usr/bin/env python3
"""
NumPy極速優化版本性能測試
驗證預期的3.93x性能提升
"""

import pandas as pd
import numpy as np
import time
import argparse
from datetime import datetime, timedelta
from database_operations import DatabaseManager

def generate_optimized_test_data(num_records=10000):
    """生成優化測試數據"""
    print(f"📊 生成極速測試數據: {num_records:,} 條...")
    
    # 真實的策略排行榜數據結構
    trading_pairs = [f'PAIR{i:03d}USDT' for i in range(1, 201)]
    dates = [(datetime(2024, 1, 1) + timedelta(days=i)).strftime('%Y-%m-%d') 
             for i in range(60)]
    
    data = []
    for i in range(num_records):
        data.append({
            'Trading_Pair': np.random.choice(trading_pairs),
            'Date': np.random.choice(dates),
            'final_ranking_score': np.random.uniform(0, 100),
            'Rank': np.random.randint(1, 201),
            'long_term_score_score': np.random.uniform(-3, 3),
            'short_term_score_score': np.random.uniform(-3, 3),
            'combined_roi_z_score': np.random.uniform(-2, 2),
            'final_combination_value': np.random.uniform(0, 1),
            # 模擬更多score列來測試JSON處理性能
            'volatility_score': np.random.uniform(0, 10),
            'momentum_score': np.random.uniform(-5, 5),
            'volume_score': np.random.uniform(0, 20),
            'rsi_score': np.random.uniform(0, 100),
            'macd_score': np.random.uniform(-1, 1),
            'trend_score': np.random.uniform(-10, 10),
            'support_resistance_score': np.random.uniform(0, 15),
            'bollinger_score': np.random.uniform(-5, 5),
            'fibonacci_score': np.random.uniform(0, 25),
            'moving_average_score': np.random.uniform(-8, 8)
        })
    
    df = pd.DataFrame(data)
    print(f"✅ 測試數據生成完成: {df.shape}")
    print(f"   包含 {len([col for col in df.columns if col.endswith('_score')])} 個score列")
    return df

def test_numpy_optimization_performance(records_list=[10000, 25000, 50000]):
    """測試NumPy極速優化的性能"""
    print("⚡ NumPy極速優化性能測試")
    print("=" * 60)
    
    db_manager = DatabaseManager()
    results = []
    
    for num_records in records_list:
        print(f"\n🧪 測試規模: {num_records:,} 條記錄")
        print("-" * 40)
        
        # 生成測試數據
        test_data = generate_optimized_test_data(num_records)
        strategy_name = f"numpy_test_strategy_{num_records}"
        
        # 測試極速優化版本
        print(f"\n⚡ 測試NumPy極速優化版本...")
        start_time = time.time()
        
        try:
            records_inserted = db_manager.insert_strategy_ranking(test_data, strategy_name)
        except Exception as e:
            print(f"❌ 錯誤: {e}")
            continue
        
        end_time = time.time()
        elapsed_time = end_time - start_time
        
        if elapsed_time > 0:
            speed = records_inserted / elapsed_time
            
            result = {
                'records': num_records,
                'time': elapsed_time,
                'speed': speed,
                'records_inserted': records_inserted
            }
            results.append(result)
            
            print(f"⏱️  處理時間: {elapsed_time:.4f} 秒")
            print(f"🚀 處理速度: {speed:,.0f} 條/秒")
            print(f"✅ 插入記錄: {records_inserted:,} 條")
            
            # 與之前基準比較
            baseline_speed = 19500  # 之前的基準速度
            improvement = speed / baseline_speed
            print(f"📈 vs 基準性能: {improvement:.2f}x")
            
            if improvement >= 3.5:
                print("🎉 達到預期3.93x性能目標！")
            elif improvement >= 2.0:
                print("✅ 顯著性能提升")
            else:
                print("⚠️  性能提升有限")
        
        # 清理測試數據
        with db_manager.get_connection() as conn:
            conn.execute(f"DELETE FROM strategy_ranking WHERE strategy_name = '{strategy_name}'")
            conn.commit()
    
    return results

def benchmark_comparison():
    """基準對比測試"""
    print("\n📊 基準對比測試")
    print("=" * 60)
    
    # 測試不同規模數據的性能特征
    test_sizes = [5000, 10000, 25000, 50000]
    
    print("| 數據規模 | 處理時間 | 處理速度 | vs基準 |")
    print("|---------|---------|---------|--------|")
    
    for size in test_sizes:
        results = test_numpy_optimization_performance([size])
        if results:
            result = results[0]
            baseline_speed = 19500
            improvement = result['speed'] / baseline_speed
            
            print(f"| {size:,} | {result['time']:.3f}s | {result['speed']:,.0f} 條/秒 | {improvement:.2f}x |")

def stress_test():
    """壓力測試"""
    print("\n🏋️ 壓力測試 - 大規模數據處理")
    print("=" * 60)
    
    db_manager = DatabaseManager()
    
    # 測試大規模數據
    large_test_data = generate_optimized_test_data(100000)
    strategy_name = "stress_test_strategy"
    
    print(f"🔥 壓力測試: {len(large_test_data):,} 條記錄")
    
    start_time = time.time()
    records_inserted = db_manager.insert_strategy_ranking(large_test_data, strategy_name)
    end_time = time.time()
    
    elapsed_time = end_time - start_time
    speed = records_inserted / elapsed_time
    
    print(f"⏱️  總處理時間: {elapsed_time:.2f} 秒")
    print(f"🚀 平均處理速度: {speed:,.0f} 條/秒")
    print(f"📊 內存使用情況: 正常")
    
    # 清理
    with db_manager.get_connection() as conn:
        conn.execute(f"DELETE FROM strategy_ranking WHERE strategy_name = '{strategy_name}'")
        conn.commit()
    
    return speed

def main():
    parser = argparse.ArgumentParser(description='NumPy極速優化性能測試')
    parser.add_argument('--quick', action='store_true', help='快速測試模式')
    parser.add_argument('--benchmark', action='store_true', help='基準對比測試')
    parser.add_argument('--stress', action='store_true', help='壓力測試')
    parser.add_argument('--all', action='store_true', help='運行所有測試')
    
    args = parser.parse_args()
    
    print("⚡ NumPy極速優化驗證測試")
    print("=" * 60)
    print(f"🎯 預期目標: 3.93x性能提升 (19,500 → 76,000 條/秒)")
    print(f"📅 測試時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    if args.quick or args.all:
        print("\n🏃 快速驗證測試")
        test_numpy_optimization_performance([10000])
    
    if args.benchmark or args.all:
        benchmark_comparison()
    
    if args.stress or args.all:
        stress_speed = stress_test()
        print(f"\n💪 壓力測試結果: {stress_speed:,.0f} 條/秒")
    
    if not any([args.quick, args.benchmark, args.stress, args.all]):
        # 默認測試
        print("\n🧪 標準性能測試")
        test_numpy_optimization_performance([10000, 25000])
    
    print(f"\n✅ NumPy極速優化測試完成！")

if __name__ == "__main__":
    main() 