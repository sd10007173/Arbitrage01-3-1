"""
因子策略系統演示腳本 (Demo Factor Strategies)

此腳本演示因子策略系統的主要功能：
1. 執行所有預設策略
2. 顯示結果比較
3. 展示系統能力

使用方法：
    python factor_strategies/demo_factor_strategies.py
"""

import sys
import os
import pandas as pd
from datetime import datetime

# 添加父目錄到 Python 路徑
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from factor_strategies.factor_engine import FactorEngine
from factor_strategies.factor_strategy_config import FACTOR_STRATEGIES
from database_operations import DatabaseManager

def print_header():
    """打印演示標題"""
    print("=" * 80)
    print("🧠 因子策略系統演示 (Factor Strategy System Demo)")
    print("   展示基於數據庫的量化因子策略計算")
    print("=" * 80)

def show_available_strategies():
    """顯示可用策略"""
    print("\n📋 系統中的因子策略:")
    print("-" * 60)
    
    for i, (key, config) in enumerate(FACTOR_STRATEGIES.items(), 1):
        print(f"{i}. {config['name']} ({key})")
        print(f"   📝 {config['description']}")
        print(f"   📊 因子數量: {len(config['factors'])}")
        print(f"   📅 數據要求: {config['data_requirements']['min_data_days']}天")
        print()

def show_factor_details():
    """顯示因子詳情"""
    print("\n🧮 系統支援的因子類型:")
    print("-" * 60)
    
    factors_info = [
        ("趨勢因子", "calculate_trend_slope", "計算累積回報的線性回歸斜率"),
        ("夏普比率", "calculate_sharpe_ratio", "風險調整後收益指標"),
        ("穩定性因子", "calculate_inv_std_dev", "標準差倒數，衡量穩定性"),
        ("勝率因子", "calculate_win_rate", "獲利天數比例"),
        ("最大回撤", "calculate_max_drawdown", "峰值到谷值的最大損失"),
        ("索提諾比率", "calculate_sortino_ratio", "只考慮下行風險的收益指標"),
    ]
    
    for name, func_name, desc in factors_info:
        print(f"• {name} ({func_name})")
        print(f"  {desc}")
        print()

def demo_single_strategy(engine, strategy_name):
    """演示單個策略"""
    print(f"\n🚀 演示策略: {FACTOR_STRATEGIES[strategy_name]['name']}")
    print("-" * 50)
    
    try:
        result = engine.run_strategy(strategy_name, save_to_db=True)
        
        if not result.empty:
            print(f"\n✅ 策略執行成功！")
            print(f"📊 共處理 {len(result)} 個交易對")
            
            # 顯示統計信息
            scores = result['final_ranking_score']
            print(f"\n📈 分數統計:")
            print(f"   最高分: {scores.max():.6f}")
            print(f"   最低分: {scores.min():.6f}")
            print(f"   平均分: {scores.mean():.6f}")
            print(f"   標準差: {scores.std():.6f}")
            
            # 顯示前5名
            print(f"\n🏆 前5名交易對:")
            for i, (_, row) in enumerate(result.head(5).iterrows(), 1):
                print(f"   {i}. {row['trading_pair']:<20} 分數: {row['final_ranking_score']:.6f}")
            
            return result
        else:
            print(f"⚠️ 策略沒有產生結果")
            return pd.DataFrame()
            
    except Exception as e:
        print(f"❌ 策略執行失敗: {e}")
        return pd.DataFrame()

def compare_strategies(results):
    """比較策略結果"""
    print("\n📊 策略結果比較:")
    print("=" * 80)
    
    if not results:
        print("❌ 沒有結果可供比較")
        return
    
    # 創建比較表
    comparison_data = []
    
    for strategy_name, result_df in results.items():
        if not result_df.empty:
            scores = result_df['final_ranking_score']
            comparison_data.append({
                '策略名稱': FACTOR_STRATEGIES[strategy_name]['name'],
                '交易對數量': len(result_df),
                '最高分': f"{scores.max():.6f}",
                '最低分': f"{scores.min():.6f}",
                '平均分': f"{scores.mean():.6f}",
                '標準差': f"{scores.std():.6f}",
                '前3名交易對': ', '.join(result_df.head(3)['trading_pair'].tolist())
            })
    
    if comparison_data:
        comparison_df = pd.DataFrame(comparison_data)
        print(comparison_df.to_string(index=False))
    
    # 找出共同的優秀交易對
    print(f"\n🎯 尋找各策略共同推薦的交易對:")
    print("-" * 50)
    
    if len(results) >= 2:
        # 取每個策略的前5名
        top_pairs_by_strategy = {}
        for strategy_name, result_df in results.items():
            if not result_df.empty:
                top_pairs_by_strategy[strategy_name] = set(result_df.head(5)['trading_pair'].tolist())
        
        if len(top_pairs_by_strategy) >= 2:
            # 找交集
            common_pairs = set.intersection(*top_pairs_by_strategy.values())
            
            if common_pairs:
                print(f"✅ 發現 {len(common_pairs)} 個共同推薦的交易對:")
                for pair in sorted(common_pairs):
                    print(f"   • {pair}")
            else:
                print("⚠️ 沒有發現共同推薦的交易對")
        else:
            print("⚠️ 成功執行的策略數量不足，無法比較")
    else:
        print("⚠️ 需要至少2個策略結果才能比較")

def check_system_status(db_manager):
    """檢查系統狀態"""
    print("\n🔍 系統狀態檢查:")
    print("-" * 40)
    
    # 檢查數據庫
    info = db_manager.get_database_info()
    print(f"📂 數據庫路徑: {info['database_path']}")
    
    # 檢查數據表
    important_tables = ['return_metrics', 'factor_strategy_ranking']
    for table in important_tables:
        count = info['tables'].get(table, 0)
        if count > 0:
            print(f"✅ {table}: {count} 條記錄")
        else:
            print(f"⚠️ {table}: 沒有數據")
    
    # 檢查數據日期範圍
    start_date, end_date = db_manager.get_return_metrics_date_range()
    if end_date:
        print(f"📅 數據日期範圍: {start_date} 到 {end_date}")
    else:
        print(f"❌ 沒有 return_metrics 數據")
    
    # 檢查因子策略結果
    try:
        factor_strategies = db_manager.get_available_factor_strategies()
        if factor_strategies:
            print(f"🧠 已保存的因子策略: {len(factor_strategies)} 個")
            for strategy in factor_strategies:
                print(f"   • {strategy}")
        else:
            print(f"📝 因子策略: 尚無保存的結果")
    except:
        print(f"📝 因子策略: 尚無保存的結果")

def main():
    """主演示函數"""
    print_header()
    
    # 初始化系統
    print("\n🔧 初始化因子策略系統...")
    try:
        engine = FactorEngine()
        db_manager = engine.db_manager
        print("✅ 系統初始化成功")
    except Exception as e:
        print(f"❌ 系統初始化失敗: {e}")
        return
    
    # 檢查系統狀態
    check_system_status(db_manager)
    
    # 顯示策略和因子信息
    show_available_strategies()
    show_factor_details()
    
    # 檢查是否有數據
    start_date, end_date = db_manager.get_return_metrics_date_range()
    if not end_date:
        print("\n❌ 沒有可用的 return_metrics 數據")
        print("請先運行 calculate_FR_return_list_v2.py 生成數據")
        return
    
    print(f"\n🚀 開始演示因子策略計算...")
    print(f"📅 使用數據日期: {end_date}")
    
    # 演示策略執行
    results = {}
    
    # 先演示簡單測試策略
    print(f"\n" + "="*60)
    print("階段 1: 測試簡單策略")
    print("="*60)
    
    test_result = demo_single_strategy(engine, 'test_factor_simple')
    if not test_result.empty:
        results['test_factor_simple'] = test_result
    
    # 演示核心策略
    print(f"\n" + "="*60)
    print("階段 2: 演示核心策略")
    print("="*60)
    
    core_strategies = ['cerebrum_core', 'cerebrum_momentum']
    
    for strategy_name in core_strategies:
        if strategy_name in FACTOR_STRATEGIES:
            result = demo_single_strategy(engine, strategy_name)
            if not result.empty:
                results[strategy_name] = result
    
    # 比較結果
    if len(results) > 1:
        compare_strategies(results)
    
    # 最終狀態檢查
    print(f"\n" + "="*60)
    print("演示完成 - 最終狀態")
    print("="*60)
    
    check_system_status(db_manager)
    
    print(f"\n🎉 因子策略系統演示完成！")
    print(f"✅ 成功執行 {len(results)} 個策略")
    print(f"💾 結果已保存到數據庫 factor_strategy_ranking 表")
    print(f"\n📝 後續可以使用以下方式查看結果:")
    print(f"   • 運行 python factor_strategies/run_factor_strategies.py")
    print(f"   • 直接查詢數據庫 factor_strategy_ranking 表")
    print(f"   • 使用 database_operations.py 的相關函數")

if __name__ == "__main__":
    main() 