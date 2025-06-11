#!/usr/bin/env python3
"""
Strategy Ranking 性能瓶頸深度分析
逐步分析每個操作的耗時，精確定位瓶頸
"""

import pandas as pd
import numpy as np
import time
import json
import cProfile
import pstats
from datetime import datetime, timedelta
from database_operations import DatabaseManager

class PerformanceProfiler:
    def __init__(self):
        self.db_manager = DatabaseManager()
        self.timings = {}
    
    def time_operation(self, operation_name, func, *args, **kwargs):
        """計時單個操作"""
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        elapsed = end_time - start_time
        self.timings[operation_name] = elapsed
        print(f"⏱️  {operation_name}: {elapsed:.4f} 秒")
        return result
    
    def generate_test_data(self, num_records=10000):
        """生成測試數據"""
        print(f"📊 生成測試數據: {num_records:,} 條...")
        
        # 模擬真實的策略排行榜數據結構
        trading_pairs = [f'PAIR{i:03d}USDT' for i in range(1, 101)]
        dates = [(datetime(2024, 1, 1) + timedelta(days=i)).strftime('%Y-%m-%d') 
                 for i in range(30)]
        
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
                'macd_score': np.random.uniform(-1, 1),
                'trend_score': np.random.uniform(-10, 10),
                'support_resistance_score': np.random.uniform(0, 15)
            })
        
        df = pd.DataFrame(data)
        print(f"✅ 測試數據生成完成，形狀: {df.shape}")
        return df
    
    def analyze_current_implementation(self, df, strategy_name="test_strategy"):
        """分析當前實現的性能瓶頸"""
        print("\n🔍 分析當前實現的性能瓶頸...")
        print("=" * 60)
        
        # 1. 分析 iterrows() 的耗時
        def test_iterrows():
            count = 0
            for _, row in df.iterrows():
                count += 1
            return count
        
        self.time_operation("1. iterrows() 循環", test_iterrows)
        
        # 2. 分析 component_scores 處理的耗時
        def test_component_scores():
            score_columns = [col for col in df.columns 
                           if col.endswith('_score') 
                           and col not in ['final_ranking_score', 'long_term_score_score', 'short_term_score_score']]
            
            component_scores_list = []
            for _, row in df.iterrows():
                component_scores = {}
                for col in score_columns:
                    component_scores[col] = row.get(col)
                component_scores_list.append(component_scores)
            return component_scores_list
        
        self.time_operation("2. component_scores 處理", test_component_scores)
        
        # 3. 分析 JSON 序列化的耗時
        def test_json_serialization():
            score_columns = [col for col in df.columns 
                           if col.endswith('_score') 
                           and col not in ['final_ranking_score', 'long_term_score_score', 'short_term_score_score']]
            
            json_list = []
            for _, row in df.iterrows():
                component_scores = {}
                for col in score_columns:
                    component_scores[col] = row.get(col)
                json_str = json.dumps(component_scores) if component_scores else None
                json_list.append(json_str)
            return json_list
        
        self.time_operation("3. JSON 序列化", test_json_serialization)
        
        # 4. 分析 row.get() 調用的耗時
        def test_row_get_operations():
            data_tuples = []
            for _, row in df.iterrows():
                tuple_data = (
                    strategy_name,
                    row.get('Trading_Pair', row.get('trading_pair')),
                    row['Date'] if 'Date' in row else row.get('date'),
                    row.get('final_ranking_score'),
                    row.get('Rank', row.get('rank_position')),
                    row.get('long_term_score_score', row.get('all_ROI_Z_score')),
                    row.get('short_term_score_score', row.get('short_ROI_z_score')),
                    row.get('combined_roi_z_score'),
                    row.get('final_combination_value'),
                    None  # 暫時不處理JSON
                )
                data_tuples.append(tuple_data)
            return data_tuples
        
        self.time_operation("4. row.get() 調用", test_row_get_operations)
        
        # 5. 分析完整的數據準備過程
        def test_full_data_preparation():
            score_columns = [col for col in df.columns 
                           if col.endswith('_score') 
                           and col not in ['final_ranking_score', 'long_term_score_score', 'short_term_score_score']]
            
            data_to_insert = []
            for _, row in df.iterrows():
                component_scores = {}
                for col in score_columns:
                    component_scores[col] = row.get(col)
                
                data_to_insert.append((
                    strategy_name,
                    row.get('Trading_Pair', row.get('trading_pair')),
                    row['Date'] if 'Date' in row else row.get('date'),
                    row.get('final_ranking_score'),
                    row.get('Rank', row.get('rank_position')),
                    row.get('long_term_score_score', row.get('all_ROI_Z_score')),
                    row.get('short_term_score_score', row.get('short_ROI_z_score')),
                    row.get('combined_roi_z_score'),
                    row.get('final_combination_value'),
                    json.dumps(component_scores) if component_scores else None
                ))
            return data_to_insert
        
        self.time_operation("5. 完整數據準備", test_full_data_preparation)
        
        # 6. 分析數據庫插入的耗時
        def test_database_insert():
            data_to_insert = []
            score_columns = [col for col in df.columns 
                           if col.endswith('_score') 
                           and col not in ['final_ranking_score', 'long_term_score_score', 'short_term_score_score']]
            
            for _, row in df.iterrows():
                component_scores = {}
                for col in score_columns:
                    component_scores[col] = row.get(col)
                
                data_to_insert.append((
                    strategy_name,
                    row.get('Trading_Pair', row.get('trading_pair')),
                    row['Date'] if 'Date' in row else row.get('date'),
                    row.get('final_ranking_score'),
                    row.get('Rank', row.get('rank_position')),
                    row.get('long_term_score_score', row.get('all_ROI_Z_score')),
                    row.get('short_term_score_score', row.get('short_ROI_z_score')),
                    row.get('combined_roi_z_score'),
                    row.get('final_combination_value'),
                    json.dumps(component_scores) if component_scores else None
                ))
            
            # 實際數據庫插入
            with self.db_manager.get_connection() as conn:
                conn.executemany('''
                    INSERT OR REPLACE INTO strategy_ranking 
                    (strategy_name, trading_pair, date, final_ranking_score, rank_position,
                     long_term_score, short_term_score, combined_roi_z_score, 
                     final_combination_value, component_scores)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', data_to_insert)
                conn.commit()
            
            return len(data_to_insert)
        
        self.time_operation("6. 數據庫插入", test_database_insert)
        
        return self.timings
    
    def test_vectorized_alternatives(self, df, strategy_name="test_strategy"):
        """測試向量化替代方案"""
        print("\n🚀 測試向量化替代方案...")
        print("=" * 60)
        
        # 替代方案1: 使用 pandas apply
        def test_pandas_apply():
            score_columns = [col for col in df.columns 
                           if col.endswith('_score') 
                           and col not in ['final_ranking_score', 'long_term_score_score', 'short_term_score_score']]
            
            def create_component_scores(row):
                component_scores = {}
                for col in score_columns:
                    component_scores[col] = row.get(col)
                return json.dumps(component_scores) if component_scores else None
            
            df['component_scores'] = df.apply(create_component_scores, axis=1)
            return df
        
        self.time_operation("替代方案1: pandas apply", test_pandas_apply)
        
        # 替代方案2: 預處理 + 向量化
        def test_preprocessing_vectorized():
            # 預處理列名映射
            df_clean = df.copy()
            df_clean['strategy_name'] = strategy_name
            df_clean['trading_pair'] = df_clean.get('Trading_Pair', df_clean.get('trading_pair', ''))
            df_clean['date'] = df_clean.get('Date', df_clean.get('date', ''))
            df_clean['rank_position'] = df_clean.get('Rank', df_clean.get('rank_position', 0))
            
            # 處理 component_scores
            score_columns = [col for col in df.columns 
                           if col.endswith('_score') 
                           and col not in ['final_ranking_score', 'long_term_score_score', 'short_term_score_score']]
            
            if score_columns:
                df_scores = df_clean[score_columns]
                component_scores_list = []
                for idx in range(len(df_scores)):
                    component_scores = df_scores.iloc[idx].to_dict()
                    component_scores_list.append(json.dumps(component_scores))
                df_clean['component_scores'] = component_scores_list
            else:
                df_clean['component_scores'] = None
            
            return df_clean
        
        self.time_operation("替代方案2: 預處理+向量化", test_preprocessing_vectorized)
        
        # 替代方案3: 純NumPy操作
        def test_numpy_operations():
            # 轉換為NumPy數組進行快速操作
            score_columns = [col for col in df.columns 
                           if col.endswith('_score') 
                           and col not in ['final_ranking_score', 'long_term_score_score', 'short_term_score_score']]
            
            if score_columns:
                score_array = df[score_columns].values
                component_scores_list = []
                for row_idx in range(score_array.shape[0]):
                    component_scores = dict(zip(score_columns, score_array[row_idx]))
                    component_scores_list.append(json.dumps(component_scores))
            else:
                component_scores_list = [None] * len(df)
            
            return component_scores_list
        
        self.time_operation("替代方案3: NumPy操作", test_numpy_operations)
    
    def print_analysis_summary(self):
        """打印分析總結"""
        print("\n📊 性能瓶頸分析總結")
        print("=" * 60)
        
        if not self.timings:
            print("❌ 沒有timing數據")
            return
        
        # 按耗時排序
        sorted_timings = sorted(self.timings.items(), key=lambda x: x[1], reverse=True)
        
        total_time = sum(self.timings.values())
        
        print(f"📝 總耗時: {total_time:.4f} 秒")
        print(f"📊 各操作耗時占比:")
        
        for operation, timing in sorted_timings:
            percentage = (timing / total_time) * 100
            print(f"   {operation}: {timing:.4f}s ({percentage:.1f}%)")
        
        # 識別最大瓶頸
        max_operation, max_time = sorted_timings[0]
        print(f"\n🎯 最大瓶頸: {max_operation} ({max_time:.4f}s)")
        
        # 提出優化建議
        print(f"\n💡 優化建議:")
        if "JSON 序列化" in max_operation:
            print("   - 考慮簡化 component_scores 結構")
            print("   - 或者延遲JSON序列化到查詢時進行")
        elif "component_scores" in max_operation:
            print("   - 優化 component_scores 的處理邏輯")
            print("   - 考慮預處理 score 列名")
        elif "iterrows" in max_operation:
            print("   - 考慮使用 pandas apply 或向量化操作")
        elif "row.get" in max_operation:
            print("   - 減少 row.get() 調用次數")
            print("   - 預處理列名映射")

def main():
    print("🔬 Strategy Ranking 性能瓶頸深度分析")
    print("=" * 60)
    
    profiler = PerformanceProfiler()
    
    # 生成測試數據
    test_data = profiler.generate_test_data(10000)
    
    # 分析當前實現
    profiler.analyze_current_implementation(test_data)
    
    # 測試向量化替代方案
    profiler.test_vectorized_alternatives(test_data)
    
    # 打印分析總結
    profiler.print_analysis_summary()
    
    # 清理測試數據
    print("\n🧹 清理測試數據...")
    with profiler.db_manager.get_connection() as conn:
        conn.execute("DELETE FROM strategy_ranking WHERE strategy_name = 'test_strategy'")
        conn.commit()
    
    print("\n✅ 性能分析完成！")

if __name__ == "__main__":
    main() 