#!/usr/bin/env python3
"""
Strategy Ranking 性能測試腳本
測試向量化優化版本與原始版本的性能對比
"""

import pandas as pd
import numpy as np
import time
import argparse
from datetime import datetime, timedelta
from database_operations import DatabaseManager

def generate_test_data(num_records=10000, num_strategies=3):
    """生成測試數據"""
    print(f"📊 生成測試數據: {num_records:,} 條記錄, {num_strategies} 個策略...")
    
    # 生成交易對
    trading_pairs = [f'PAIR{i:03d}USDT' for i in range(1, 101)]
    
    # 生成日期範圍
    start_date = datetime(2024, 1, 1)
    dates = [(start_date + timedelta(days=i)).strftime('%Y-%m-%d') 
             for i in range(num_records // len(trading_pairs) + 1)]
    
    test_datasets = {}
    
    for strategy_idx in range(num_strategies):
        strategy_name = f'test_strategy_{strategy_idx + 1}'
        
        data = []
        for i in range(num_records):
            data.append({
                'Trading_Pair': np.random.choice(trading_pairs),
                'Date': np.random.choice(dates),
                'final_ranking_score': np.random.uniform(0, 100),
                'Rank': np.random.randint(1, 101),
                'long_term_score_score': np.random.uniform(-3, 3),
                'short_term_score_score': np.random.uniform(-3, 3),
                'combined_roi_z_score': np.random.uniform(-2, 2),
                'final_combination_value': np.random.uniform(0, 1),
                'volatility_score': np.random.uniform(0, 10),
                'momentum_score': np.random.uniform(-5, 5),
                'volume_score': np.random.uniform(0, 20),
                'rsi_score': np.random.uniform(0, 100),
                'macd_score': np.random.uniform(-1, 1)
            })
        
        df = pd.DataFrame(data)
        test_datasets[strategy_name] = df
        print(f"   ✅ {strategy_name}: {len(df):,} 條記錄")
    
    return test_datasets

def test_method_performance(db_manager, test_data, method_name, use_legacy=False):
    """測試指定方法的性能"""
    print(f"\n🚀 測試方法: {method_name}")
    print("=" * 50)
    
    total_records = 0
    total_time = 0
    
    for strategy_name, df in test_data.items():
        print(f"\n📊 處理策略: {strategy_name} - {len(df):,} 條記錄")
        
        # 記錄開始時間
        start_time = time.time()
        
        try:
            if use_legacy:
                records_inserted = db_manager.insert_strategy_ranking_legacy(df, strategy_name)
            else:
                records_inserted = db_manager.insert_strategy_ranking(df, strategy_name)
        except Exception as e:
            print(f"❌ 錯誤: {e}")
            continue
        
        # 記錄結束時間
        end_time = time.time()
        elapsed_time = end_time - start_time
        
        total_records += records_inserted
        total_time += elapsed_time
        
        # 計算性能指標
        records_per_second = records_inserted / elapsed_time if elapsed_time > 0 else 0
        
        print(f"   ⏱️  處理時間: {elapsed_time:.4f} 秒")
        print(f"   📈 處理速度: {records_per_second:,.0f} 條/秒")
        print(f"   ✅ 插入記錄: {records_inserted:,} 條")
    
    # 總體統計
    if total_time > 0:
        avg_speed = total_records / total_time
        print(f"\n📊 {method_name} 總體性能:")
        print(f"   📝 總記錄數: {total_records:,} 條")
        print(f"   ⏱️  總時間: {total_time:.4f} 秒")
        print(f"   🚀 平均速度: {avg_speed:,.0f} 條/秒")
        
        return {
            'method': method_name,
            'total_records': total_records,
            'total_time': total_time,
            'avg_speed': avg_speed
        }
    
    return None

def cleanup_test_data(db_manager):
    """清理測試數據"""
    print("\n🧹 清理測試數據...")
    
    with db_manager.get_connection() as conn:
        # 刪除測試策略數據
        conn.execute("DELETE FROM strategy_ranking WHERE strategy_name LIKE 'test_strategy_%'")
        conn.commit()
        
        # 檢查剩餘記錄
        cursor = conn.execute("SELECT COUNT(*) FROM strategy_ranking WHERE strategy_name LIKE 'test_strategy_%'")
        remaining = cursor.fetchone()[0]
        
        if remaining == 0:
            print("✅ 測試數據清理完成")
        else:
            print(f"⚠️  仍有 {remaining} 條測試數據未清理")

def main():
    parser = argparse.ArgumentParser(description='Strategy Ranking 性能測試')
    parser.add_argument('--records', type=int, default=50000, help='測試記錄數量 (默認: 50000)')
    parser.add_argument('--strategies', type=int, default=3, help='測試策略數量 (默認: 3)')
    parser.add_argument('--test-legacy', action='store_true', help='也測試舊版本方法')
    parser.add_argument('--cleanup-only', action='store_true', help='僅清理測試數據')
    
    args = parser.parse_args()
    
    # 初始化數據庫
    db_manager = DatabaseManager()
    
    if args.cleanup_only:
        cleanup_test_data(db_manager)
        return
    
    print("🔬 Strategy Ranking 性能測試")
    print("=" * 60)
    print(f"📊 測試規模: {args.records:,} 條記錄 × {args.strategies} 個策略")
    print(f"📅 測試時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 生成測試數據
    test_data = generate_test_data(args.records, args.strategies)
    
    results = []
    
    # 測試向量化版本
    print("\n" + "=" * 60)
    print("🚀 測試向量化優化版本")
    print("=" * 60)
    
    result_vectorized = test_method_performance(
        db_manager, test_data, "向量化優化版本", use_legacy=False
    )
    if result_vectorized:
        results.append(result_vectorized)
    
    # 測試舊版本（如果請求）
    if args.test_legacy:
        print("\n" + "=" * 60)
        print("🐌 測試原始版本 (iterrows)")
        print("=" * 60)
        
        result_legacy = test_method_performance(
            db_manager, test_data, "原始版本 (iterrows)", use_legacy=True
        )
        if result_legacy:
            results.append(result_legacy)
    
    # 性能對比
    if len(results) >= 2:
        print("\n" + "=" * 60)
        print("📊 性能對比分析")
        print("=" * 60)
        
        vectorized = results[0]
        legacy = results[1]
        
        speed_improvement = vectorized['avg_speed'] / legacy['avg_speed']
        time_reduction = (legacy['total_time'] - vectorized['total_time']) / legacy['total_time'] * 100
        
        print(f"🚀 向量化版本: {vectorized['avg_speed']:,.0f} 條/秒")
        print(f"🐌 原始版本: {legacy['avg_speed']:,.0f} 條/秒")
        print(f"⚡ 性能提升: {speed_improvement:.2f}x")
        print(f"⏱️  時間節省: {time_reduction:.1f}%")
        
        if speed_improvement >= 2.0:
            print("🎉 向量化優化效果顯著！")
        elif speed_improvement >= 1.5:
            print("✅ 向量化優化效果良好")
        else:
            print("⚠️  向量化優化效果有限")
    
    # 清理測試數據
    cleanup_test_data(db_manager)
    
    print(f"\n✅ 性能測試完成！")

if __name__ == "__main__":
    main() 