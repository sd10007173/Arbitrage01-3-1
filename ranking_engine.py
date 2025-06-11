"""
排行榜計算引擎
基於配置動態計算各種指標組合
"""

import pandas as pd
import numpy as np
from ranking_config import RANKING_STRATEGIES, DEFAULT_STRATEGY, EXPERIMENTAL_CONFIGS

class RankingEngine:
    def __init__(self, strategy_name=None):
        """
        初始化排行榜引擎
        :param strategy_name: 策略名稱，如果為None則使用默認策略
        """
        if strategy_name is None:
            strategy_name = DEFAULT_STRATEGY
            
        # 嘗試從主策略中載入
        if strategy_name in RANKING_STRATEGIES:
            self.strategy = RANKING_STRATEGIES[strategy_name]
        # 嘗試從實驗策略中載入
        elif strategy_name in EXPERIMENTAL_CONFIGS:
            self.strategy = EXPERIMENTAL_CONFIGS[strategy_name]
        else:
            raise ValueError(f"未知的策略: {strategy_name}")
            
        self.strategy_name = strategy_name
        print(f"🎯 載入策略: {self.strategy['name']}")
    
    def calculate_component_score(self, df, component_name, component_config):
        """計算單一組件分數"""
        indicators = component_config['indicators']
        weights = component_config['weights']
        normalize = component_config.get('normalize', False)
        
        # 檢查指標是否存在
        missing_indicators = [ind for ind in indicators if ind not in df.columns]
        if missing_indicators:
            raise ValueError(f"缺少指標: {missing_indicators}")
        
        # 提取指標數據
        indicator_data = df[indicators].copy()
        
        # 處理無效值
        indicator_data = indicator_data.fillna(0)
        indicator_data = indicator_data.replace([np.inf, -np.inf], 0)
        
        # 標準化處理
        if normalize:
            for col in indicators:
                if indicator_data[col].std() != 0:
                    # Z-score 標準化
                    mean_val = indicator_data[col].mean()
                    std_val = indicator_data[col].std()
                    indicator_data[col] = (indicator_data[col] - mean_val) / std_val
                else:
                    # 如果標準差為0，設為0
                    indicator_data[col] = 0
        
        # 波動率懲罰 (如果啟用)
        volatility_penalty = component_config.get('volatility_penalty', False)
        if volatility_penalty:
            # 計算每個交易對在各指標上的波動率
            volatility_scores = []
            for idx in range(len(df)):
                row_data = []
                for col in indicators:
                    row_data.append(indicator_data.iloc[idx][col])
                
                # 計算這一行各指標的標準差作為波動率指標
                if len(row_data) > 1:
                    vol = np.std(row_data)
                    # 波動率越高，懲罰越大 (減少分數)
                    penalty = max(0, 1 - vol * 0.5)  # 調整係數可以修改
                else:
                    penalty = 1.0
                volatility_scores.append(penalty)
        
        # 加權計算
        weights = np.array(weights)
        weights = weights / weights.sum()  # 標準化權重
        
        score = np.zeros(len(df))
        for i, (indicator, weight) in enumerate(zip(indicators, weights)):
            score += indicator_data[indicator].values * weight
        
        # 應用波動率懲罰
        if volatility_penalty:
            score = score * np.array(volatility_scores)
        
        return score
    
    def calculate_final_ranking(self, df):
        """計算最終排行榜"""
        if df.empty:
            return df
        
        # 計算所有組件分數
        component_scores = {}
        
        for comp_name, comp_config in self.strategy['components'].items():
            try:
                score = self.calculate_component_score(df, comp_name, comp_config)
                component_scores[comp_name] = score
                print(f"✅ 計算組件分數: {comp_name}")
            except Exception as e:
                print(f"❌ 計算組件分數失敗 {comp_name}: {e}")
                component_scores[comp_name] = np.zeros(len(df))
        
        # 組合最終分數
        final_config = self.strategy['final_combination']
        final_weights = np.array(final_config['weights'])
        final_weights = final_weights / final_weights.sum()
        
        final_score = np.zeros(len(df))
        final_combination_details = []
        
        for i, (score_name, weight) in enumerate(zip(final_config['scores'], final_weights)):
            if score_name in component_scores:
                final_score += component_scores[score_name] * weight
            else:
                print(f"⚠️ 找不到組件分數: {score_name}")
        
        # 計算final_combination_value - 記錄組合的詳細信息
        for idx in range(len(df)):
            combination_parts = []
            for score_name, weight in zip(final_config['scores'], final_weights):
                if score_name in component_scores:
                    score_value = component_scores[score_name][idx]
                    weighted_value = score_value * weight
                    combination_parts.append(f"{score_name}({score_value:.4f})*{weight:.3f}")
            
            final_combination_value = " + ".join(combination_parts) + f" = {final_score[idx]:.4f}"
            final_combination_details.append(final_combination_value)
        
        # 添加組件分數到 DataFrame
        result_df = df.copy()
        for comp_name, score in component_scores.items():
            result_df[f'{comp_name}_score'] = score
        
        result_df['final_ranking_score'] = final_score
        result_df['final_combination_value'] = final_combination_details
        
        # 保留原有的計算 (為了向後兼容)
        if 'long_term_score' in component_scores and 'short_term_score' in component_scores:
            result_df['all_ROI_Z_score'] = component_scores['long_term_score']
            result_df['short_ROI_z_score'] = component_scores['short_term_score']
            result_df['combined_ROI_z_score'] = final_score
        
        # 排序
        result_df = result_df.sort_values('final_ranking_score', ascending=False).reset_index(drop=True)
        
        return result_df
    
    def get_strategy_info(self):
        """獲取策略資訊"""
        info = {
            'strategy_name': self.strategy_name,
            'strategy_description': self.strategy['name'],
            'components': list(self.strategy['components'].keys()),
            'final_combination': self.strategy['final_combination']
        }
        return info
    
    def preview_top_pairs(self, df, top_n=10):
        """預覽排行榜前N名"""
        if df.empty:
            print("⚠️ 資料為空")
            return
        
        print(f"\n📊 策略 '{self.strategy_name}' 前 {top_n} 名:")
        print("="*60)
        
        # 選擇要顯示的欄位
        display_cols = ['trading_pair', 'final_ranking_score']
        
        # 如果有組件分數，也顯示
        for comp_name in self.strategy['components'].keys():
            score_col = f'{comp_name}_score'
            if score_col in df.columns:
                display_cols.append(score_col)
        
        # 顯示前N名
        top_pairs = df[display_cols].head(top_n)
        for idx, row in top_pairs.iterrows():
            print(f"{idx+1:2d}. {row['trading_pair']:20s} 總分: {row['final_ranking_score']:8.4f}")
            
            # 顯示組件分數
            for col in display_cols[2:]:  # 跳過 trading_pair 和 final_ranking_score
                comp_name = col.replace('_score', '')
                print(f"    {comp_name:15s}: {row[col]:8.4f}")
            print()

# 方便的函數
def quick_test_strategy(df, strategy_name):
    """快速測試策略"""
    try:
        engine = RankingEngine(strategy_name)
        result = engine.calculate_final_ranking(df)
        engine.preview_top_pairs(result, 10)
        return result
    except Exception as e:
        print(f"❌ 測試策略失敗 {strategy_name}: {e}")
        return None

def compare_strategies(df, strategy_names, top_n=5):
    """比較多個策略的前N名結果"""
    results = {}
    
    print(f"🔍 比較策略結果 (前{top_n}名)")
    print("="*80)
    
    for strategy_name in strategy_names:
        try:
            engine = RankingEngine(strategy_name)
            result = engine.calculate_final_ranking(df)
            
            # 儲存結果
            top_pairs = result[['trading_pair', 'final_ranking_score']].head(top_n)
            results[strategy_name] = top_pairs
            
            print(f"\n📋 {strategy_name} ({engine.strategy['name']}):")
            for idx, row in top_pairs.iterrows():
                print(f"  {idx+1}. {row['trading_pair']:15s} {row['final_ranking_score']:8.4f}")
            
        except Exception as e:
            print(f"❌ 策略失敗 {strategy_name}: {e}")
    
    return results

def strategy_overlap_analysis(df, strategy_names, top_n=10):
    """分析不同策略的重疊度"""
    strategy_results = {}
    
    # 計算各策略的前N名
    for strategy_name in strategy_names:
        try:
            engine = RankingEngine(strategy_name)
            result = engine.calculate_final_ranking(df)
            top_pairs = set(result['trading_pair'].head(top_n).tolist())
            strategy_results[strategy_name] = top_pairs
        except Exception as e:
            print(f"❌ 分析失敗 {strategy_name}: {e}")
            continue
    
    # 分析重疊
    print(f"\n🔄 策略重疊分析 (前{top_n}名)")
    print("="*50)
    
    strategy_list = list(strategy_results.keys())
    for i, strategy1 in enumerate(strategy_list):
        for strategy2 in strategy_list[i+1:]:
            if strategy1 in strategy_results and strategy2 in strategy_results:
                overlap = strategy_results[strategy1] & strategy_results[strategy2]
                overlap_rate = len(overlap) / top_n * 100
                
                print(f"{strategy1:15s} vs {strategy2:15s}: {len(overlap):2d}/{top_n} ({overlap_rate:5.1f}%)")
                
                if overlap:
                    common_pairs = sorted(list(overlap))
                    print(f"  共同項目: {', '.join(common_pairs[:5])}")
                    if len(common_pairs) > 5:
                        print(f"           ... 還有 {len(common_pairs)-5} 個")
                print()

# 測試和調試用函數
def debug_strategy_calculation(df, strategy_name, pair_name=None):
    """調試策略計算過程"""
    try:
        engine = RankingEngine(strategy_name)
        
        if pair_name:
            # 只看特定交易對
            pair_df = df[df['trading_pair'] == pair_name].copy()
            if pair_df.empty:
                print(f"❌ 找不到交易對: {pair_name}")
                return
            df = pair_df
        
        print(f"🔍 調試策略: {engine.strategy['name']}")
        print("="*50)
        
        # 逐步計算每個組件
        for comp_name, comp_config in engine.strategy['components'].items():
            print(f"\n📊 組件: {comp_name}")
            print(f"指標: {comp_config['indicators']}")
            print(f"權重: {comp_config['weights']}")
            
            try:
                score = engine.calculate_component_score(df, comp_name, comp_config)
                print(f"分數範圍: {score.min():.4f} ~ {score.max():.4f}")
                
                if pair_name and len(score) > 0:
                    print(f"{pair_name} 得分: {score[0]:.4f}")
                    
            except Exception as e:
                print(f"❌ 計算失敗: {e}")
        
        # 最終結果
        result = engine.calculate_final_ranking(df)
        if not result.empty:
            if pair_name:
                final_score = result['final_ranking_score'].iloc[0]
                rank = result.index[result['trading_pair'] == pair_name][0] + 1 if pair_name in result['trading_pair'].values else "未找到"
                print(f"\n🎯 {pair_name} 最終得分: {final_score:.4f} (排名: {rank})")
            else:
                print(f"\n🎯 最終分數範圍: {result['final_ranking_score'].min():.4f} ~ {result['final_ranking_score'].max():.4f}")
                
    except Exception as e:
        print(f"❌ 調試失敗: {e}")

if __name__ == "__main__":
    # 簡單測試
    print("🧪 排行榜引擎測試")
    
    # 創建測試數據
    test_data = {
        'trading_pair': ['BTCUSDT_binance_bybit', 'ETHUSDT_binance_bybit', 'ADAUSDT_binance_bybit'],
        '1d_ROI': [0.05, 0.03, 0.08],
        '2d_ROI': [0.04, 0.06, 0.02],
        '7d_ROI': [0.02, 0.04, 0.06],
        '14d_ROI': [0.03, 0.05, 0.04],
        '30d_ROI': [0.06, 0.02, 0.07],
        'all_ROI': [0.04, 0.045, 0.055]
    }
    
    df = pd.DataFrame(test_data)
    print("\n測試原始策略:")
    result = quick_test_strategy(df, 'original') 