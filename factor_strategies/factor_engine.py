"""
因子計算引擎 (Factor Engine) - 數據庫版本

此檔案是因子策略系統的核心引擎，負責：
1. 從數據庫讀取 return_metrics 數據
2. 根據策略配置計算各個因子分數
3. 組合因子分數生成最終排名
4. 將結果寫入數據庫

數據庫版本修改：
- 數據來源：return_metrics 表
- 數據輸出：factor_strategy_ranking 表
- 使用 database_operations.py 進行數據庫操作
"""

import pandas as pd
import numpy as np
import json
from typing import Dict, List, Any, Optional
import sys
import os
from datetime import datetime

# 添加父目錄到 Python 路徑，以便導入核心模組
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database_operations import DatabaseManager
from factor_strategies.factor_library import *
from factor_strategies.factor_strategy_config import FACTOR_STRATEGIES

class CalculationFormatter:
    """用於格式化 calculation 數據的工具類"""
    
    @staticmethod
    def format_number(value, precision=6):
        """格式化數值，避免科學記號，統一小數位數"""
        if value is None or (isinstance(value, float) and np.isnan(value)):
            return "N/A"
        
        if isinstance(value, (int, float)):
            abs_val = abs(value)
            
            # 處理非常小的數值
            if abs_val < 1e-6 and abs_val > 0:
                return f"{value:.2e}"
            
            # 處理小數
            elif abs_val < 1:
                return f"{value:.6f}".rstrip('0').rstrip('.')
            
            # 處理大數
            elif abs_val >= 1000:
                return f"{value:,.2f}"
            
            # 處理一般數值
            else:
                return f"{value:.4f}".rstrip('0').rstrip('.')
        
        return str(value)
    
    @staticmethod
    def format_percentage(value):
        """格式化百分比"""
        if value is None:
            return "N/A"
        return f"{value * 100:.1f}%"
    
    @staticmethod
    def format_data_sample(data_sample):
        """格式化數據樣本"""
        if isinstance(data_sample, dict) and 'first_5' in data_sample:
            first_5 = [CalculationFormatter.format_number(x) for x in data_sample['first_5']]
            last_5 = [CalculationFormatter.format_number(x) for x in data_sample['last_5']]
            return f"前5個: [{', '.join(first_5)}] ... 後5個: [{', '.join(last_5)}] (共{data_sample['total_points']}個數據點)"
        elif isinstance(data_sample, list):
            formatted = [CalculationFormatter.format_number(x) for x in data_sample]
            return f"[{', '.join(formatted)}]"
        else:
            return str(data_sample)
    
    @staticmethod
    def create_formatted_calculation(raw_calculation):
        """創建格式化的 calculation 報告"""
        try:
            strategy_config = raw_calculation.get('strategy_config', {})
            raw_data = raw_calculation.get('raw_data', {})
            factor_calculations = raw_calculation.get('factor_calculations', {})
            final_calculation = raw_calculation.get('final_calculation', {})
            
            # 1. 策略概覽
            overview = {
                "策略名稱": strategy_config.get('name', 'Unknown'),
                "交易對": raw_data.get('trading_pair', 'Unknown'),
                "計算日期": raw_data.get('target_date', 'Unknown'),
                "數據期間": f"{raw_data.get('date_range', {}).get('start', 'Unknown')} 至 {raw_data.get('date_range', {}).get('end', 'Unknown')}",
                "可用數據點": raw_data.get('data_points_available', 0)
            }
            
            # 2. 因子計算結果摘要
            factor_summary = []
            factor_contributions = final_calculation.get('factor_contributions', {})
            
            for factor_name in strategy_config.get('factors', []):
                if factor_name in factor_calculations:
                    factor_data = factor_calculations[factor_name]
                    contribution_data = factor_contributions.get(factor_name, {})
                    
                    factor_info = {
                        "因子名稱": factor_name,
                        "原始分數": CalculationFormatter.format_number(factor_data.get('raw_score')),
                        "權重": CalculationFormatter.format_percentage(contribution_data.get('weight')),
                        "貢獻值": CalculationFormatter.format_number(contribution_data.get('contribution')),
                        "計算狀態": factor_data.get('calculation_status', 'unknown')
                    }
                    factor_summary.append(factor_info)
            
            # 3. 最終計算摘要
            calculation_summary = {
                "加權公式": final_calculation.get('weighted_sum_formula', ''),
                "加權總和": CalculationFormatter.format_number(final_calculation.get('calculation_breakdown', {}).get('weighted_sum')),
                "最終分數": CalculationFormatter.format_number(final_calculation.get('final_score')),
                "使用權重": CalculationFormatter.format_percentage(final_calculation.get('total_weight_used'))
            }
            
            # 4. 詳細計算資訊 (可選)
            detailed_factors = {}
            for factor_name, factor_data in factor_calculations.items():
                detailed_info = {
                    "計算函數": factor_data.get('function', ''),
                    "時間窗口": f"{factor_data.get('window', 0)}天",
                    "輸入欄位": factor_data.get('input_column', ''),
                    "使用數據點": factor_data.get('data_points_used', 0),
                    "數據樣本": CalculationFormatter.format_data_sample(factor_data.get('input_data_sample', []))
                }
                
                # 添加特定因子的額外資訊
                if 'mean_return' in factor_data:
                    detailed_info["平均收益率"] = CalculationFormatter.format_number(factor_data['mean_return'])
                if 'std_dev' in factor_data:
                    detailed_info["標準差"] = CalculationFormatter.format_number(factor_data['std_dev'])
                if 'positive_days' in factor_data:
                    detailed_info["獲利天數"] = f"{factor_data['positive_days']}/{factor_data.get('total_days', 0)}"
                
                detailed_factors[factor_name] = detailed_info
            
            # 組合成最終的格式化報告
            formatted_report = {
                "報告生成時間": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "策略概覽": overview,
                "因子計算摘要": factor_summary,
                "最終計算": calculation_summary,
                "詳細計算資訊": detailed_factors,
                "原始數據": raw_calculation  # 保留原始數據供 debug 使用
            }
            
            return formatted_report
            
        except Exception as e:
            return {
                "錯誤": f"格式化失敗: {str(e)}",
                "原始數據": raw_calculation
            }
    
    @staticmethod
    def create_readable_report(formatted_calculation):
        """創建人類可讀的文字報告"""
        try:
            overview = formatted_calculation.get('策略概覽', {})
            factor_summary = formatted_calculation.get('因子計算摘要', [])
            final_calc = formatted_calculation.get('最終計算', {})
            
            report_lines = []
            report_lines.append("=" * 60)
            report_lines.append("📊 策略執行計算報告")
            report_lines.append("=" * 60)
            
            # 基本資訊
            report_lines.append(f"策略: {overview.get('策略名稱', '')}")
            report_lines.append(f"交易對: {overview.get('交易對', '')}")
            report_lines.append(f"計算日期: {overview.get('計算日期', '')}")
            report_lines.append(f"數據期間: {overview.get('數據期間', '')}")
            report_lines.append(f"可用數據: {overview.get('可用數據點', 0)} 天")
            report_lines.append("")
            
            # 因子計算結果
            report_lines.append("📈 因子計算結果")
            report_lines.append("-" * 60)
            for factor in factor_summary:
                report_lines.append(
                    f"{factor['因子名稱']:<15} | "
                    f"分數: {factor['原始分數']:<12} | "
                    f"權重: {factor['權重']:<8} | "
                    f"貢獻: {factor['貢獻值']:<12} | "
                    f"狀態: {factor['計算狀態']}"
                )
            report_lines.append("")
            
            # 最終計算
            report_lines.append("🎯 最終計算")
            report_lines.append("-" * 60)
            report_lines.append(f"計算公式: {final_calc.get('加權公式', '')}")
            report_lines.append(f"加權總和: {final_calc.get('加權總和', '')}")
            report_lines.append(f"最終分數: {final_calc.get('最終分數', '')}")
            report_lines.append(f"權重使用: {final_calc.get('使用權重', '')}")
            report_lines.append("=" * 60)
            
            return "\n".join(report_lines)
            
        except Exception as e:
            return f"報告生成失敗: {str(e)}"

class FactorEngine:
    """因子計算引擎"""
    
    def __init__(self, db_path: str = None):
        # 如果沒有指定路徑，使用項目根目錄下的數據庫
        if db_path is None:
            # 獲取項目根目錄路徑
            current_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.dirname(current_dir)
            db_path = os.path.join(project_root, "data", "funding_rate.db")
        """
        初始化因子引擎
        
        Args:
            db_path: 數據庫文件路徑
        """
        self.db_manager = DatabaseManager(db_path)
        self.factor_functions = self._load_factor_functions()
        print(f"✅ 因子引擎初始化完成，數據庫: {db_path}")
    
    def _load_factor_functions(self) -> Dict[str, callable]:
        """載入所有可用的因子計算函數"""
        return {
            'calculate_trend_slope': calculate_trend_slope,
            'calculate_sharpe_ratio': calculate_sharpe_ratio,
            'calculate_inv_std_dev': calculate_inv_std_dev,
            'calculate_win_rate': calculate_win_rate,
            'calculate_max_drawdown': calculate_max_drawdown,
            'calculate_sortino_ratio': calculate_sortino_ratio,
        }
    
    def get_strategy_data(self, strategy_config: Dict[str, Any], target_date: str = None) -> pd.DataFrame:
        """
        獲取策略計算所需的數據
        
        Args:
            strategy_config: 策略配置
            target_date: 目標日期，None則使用最新日期
            
        Returns:
            包含所需數據的 DataFrame
        """
        # 獲取數據要求
        data_req = strategy_config['data_requirements']
        min_days = data_req['min_data_days']
        skip_days = data_req['skip_first_n_days']
        
        # 如果沒有指定日期，獲取最新日期
        if target_date is None:
            start_date, end_date = self.db_manager.get_return_metrics_date_range()
            if not end_date:
                raise ValueError("數據庫中沒有 return_metrics 數據")
            target_date = end_date
        
        # 計算需要的日期範圍
        target_date_obj = pd.to_datetime(target_date)
        start_date_obj = target_date_obj - pd.Timedelta(days=min_days + skip_days + 30)  # 額外緩衝
        start_date_str = start_date_obj.strftime('%Y-%m-%d')
        
        # 從數據庫獲取數據
        df = self.db_manager.get_return_metrics(
            start_date=start_date_str,
            end_date=target_date
        )
        
        if df.empty:
            raise ValueError(f"沒有找到日期範圍 {start_date_str} 到 {target_date} 的數據")
        
        # 轉換日期格式
        df['date'] = pd.to_datetime(df['date'])
        
        # 過濾掉新上線的幣種（跳過前N天）
        if skip_days > 0:
            # 獲取每個交易對的首次出現日期
            first_dates = df.groupby('trading_pair')['date'].min()
            valid_pairs = []
            
            for pair, first_date in first_dates.items():
                days_since_first = (target_date_obj - first_date).days
                if days_since_first >= skip_days:
                    valid_pairs.append(pair)
            
            df = df[df['trading_pair'].isin(valid_pairs)]
            print(f"📊 過濾後剩餘 {len(valid_pairs)} 個交易對 (跳過上線少於{skip_days}天的)")
        
        return df
    
    def calculate_factor_for_trading_pair(self, pair_data: pd.DataFrame, factor_config: Dict[str, Any]) -> float:
        """
        為單個交易對計算因子分數
        
        Args:
            pair_data: 單個交易對的歷史數據
            factor_config: 因子配置
            
        Returns:
            因子分數
        """
        function_name = factor_config['function']
        window = factor_config['window']
        input_col = factor_config['input_col']
        params = factor_config.get('params', {})
        
        # 檢查函數是否存在
        if function_name not in self.factor_functions:
            raise ValueError(f"未知的因子函數: {function_name}")
        
        # 檢查輸入列是否存在
        if input_col not in pair_data.columns:
            raise ValueError(f"數據中缺少列: {input_col}")
        
        # 獲取最近的數據窗口
        recent_data = pair_data.tail(window)
        
        # 檢查數據點數量，至少需要2個數據點才能計算趨勢
        min_required_points = max(2, min(window // 4, 3))  # 動態調整最小數據點要求
        if len(recent_data) < min_required_points:
            return np.nan
        
        # 獲取輸入序列
        input_series = recent_data[input_col]
        
        # 調用因子計算函數
        factor_function = self.factor_functions[function_name]
        score = factor_function(input_series, **params)
        
        return score
    
    def calculate_factor_for_trading_pair_with_details(self, pair_data: pd.DataFrame, factor_config: Dict[str, Any]) -> tuple[float, Dict[str, Any]]:
        """
        為單個交易對計算因子分數並返回詳細計算過程
        
        Args:
            pair_data: 單個交易對的歷史數據
            factor_config: 因子配置
            
        Returns:
            (因子分數, 計算詳情字典)
        """
        function_name = factor_config['function']
        window = factor_config['window']
        input_col = factor_config['input_col']
        params = factor_config.get('params', {})
        
        # 初始化計算詳情
        details = {
            'function': function_name,
            'window': window,
            'input_column': input_col,
            'params': params.copy(),
            'data_points_available': len(pair_data),
            'data_points_used': 0,
            'input_data_sample': [],
            'raw_score': np.nan,
            'calculation_status': 'failed'
        }
        
        # 檢查函數是否存在
        if function_name not in self.factor_functions:
            details['error'] = f"未知的因子函數: {function_name}"
            return np.nan, details
        
        # 檢查輸入列是否存在
        if input_col not in pair_data.columns:
            details['error'] = f"數據中缺少列: {input_col}"
            return np.nan, details
        
        # 獲取最近的數據窗口
        recent_data = pair_data.tail(window)
        details['data_points_used'] = len(recent_data)
        
        # 檢查數據點數量，至少需要2個數據點才能計算趨勢
        min_required_points = max(2, min(window // 4, 3))  # 動態調整最小數據點要求
        if len(recent_data) < min_required_points:
            details['error'] = f"數據點不足: 需要至少 {min_required_points} 個，實際 {len(recent_data)} 個"
            return np.nan, details
        
        # 獲取輸入序列
        input_series = recent_data[input_col]
        
        # 保存輸入數據樣本（前5個和後5個數據點）
        input_list = input_series.tolist()
        if len(input_list) <= 10:
            details['input_data_sample'] = input_list
        else:
            details['input_data_sample'] = {
                'first_5': input_list[:5],
                'last_5': input_list[-5:],
                'total_points': len(input_list)
            }
        
        # 根據不同因子函數收集額外詳情
        if function_name == 'calculate_sharpe_ratio':
            details['mean_return'] = float(input_series.mean())
            details['std_dev'] = float(input_series.std())
            details['annualizing_factor'] = params.get('annualizing_factor', 1)
        elif function_name == 'calculate_win_rate':
            positive_returns = (input_series > 0).sum()
            details['positive_days'] = int(positive_returns)
            details['total_days'] = len(input_series)
            details['win_rate_raw'] = positive_returns / len(input_series) if len(input_series) > 0 else 0
        elif function_name == 'calculate_inv_std_dev':
            details['std_dev'] = float(input_series.std())
            details['epsilon'] = params.get('epsilon', 1e-9)
        
        # 調用因子計算函數
        try:
            factor_function = self.factor_functions[function_name]
            score = factor_function(input_series, **params)
            details['raw_score'] = float(score) if not np.isnan(score) else np.nan
            details['calculation_status'] = 'success' if not np.isnan(score) else 'nan_result'
        except Exception as e:
            details['error'] = str(e)
            details['calculation_status'] = 'exception'
            score = np.nan
        
        return score, details
    
    def calculate_strategy_ranking(self, strategy_name: str, target_date: str = None) -> pd.DataFrame:
        """
        計算策略排名
        
        Args:
            strategy_name: 策略名稱
            target_date: 目標日期
            
        Returns:
            包含排名結果的 DataFrame
        """
        if strategy_name not in FACTOR_STRATEGIES:
            raise ValueError(f"未知的策略: {strategy_name}")
        
        strategy_config = FACTOR_STRATEGIES[strategy_name]
        print(f"🧮 開始計算因子策略: {strategy_config['name']}")
        
        # 獲取數據
        df = self.get_strategy_data(strategy_config, target_date)
        
        if target_date is None:
            target_date = df['date'].max().strftime('%Y-%m-%d')
        
        # 獲取所有交易對
        trading_pairs = df['trading_pair'].unique()
        print(f"📊 計算 {len(trading_pairs)} 個交易對的因子分數...")
        
        # 計算每個交易對的因子分數
        results = []
        
        for pair in trading_pairs:
            pair_data = df[df['trading_pair'] == pair].sort_values('date')
            
            # 計算所有因子分數並收集詳細資訊
            factor_scores = {}
            component_scores = {}
            calculation_details = {
                'strategy_config': {
                    'name': strategy_name,
                    'factors': list(strategy_config['factors'].keys()),
                    'weights': strategy_config['ranking_logic']['weights']
                },
                'raw_data': {
                    'trading_pair': pair,
                    'target_date': target_date,
                    'data_points_available': len(pair_data),
                    'date_range': {
                        'start': str(pair_data['date'].min()) if not pair_data.empty else None,
                        'end': str(pair_data['date'].max()) if not pair_data.empty else None
                    }
                },
                'factor_calculations': {},
                'final_calculation': {}
            }
            
            for factor_name, factor_config in strategy_config['factors'].items():
                try:
                    score, details = self.calculate_factor_for_trading_pair_with_details(pair_data, factor_config)
                    factor_scores[factor_name] = score
                    component_scores[factor_name] = score
                    calculation_details['factor_calculations'][factor_name] = details
                except Exception as e:
                    print(f"⚠️ 計算 {pair} 的因子 {factor_name} 時出錯: {e}")
                    factor_scores[factor_name] = np.nan
                    component_scores[factor_name] = np.nan
                    calculation_details['factor_calculations'][factor_name] = {
                        'error': str(e),
                        'calculation_status': 'exception'
                    }
            
            # 檢查是否有有效的因子分數
            valid_scores = [s for s in factor_scores.values() if not np.isnan(s)]
            if not valid_scores:
                continue
            
            # 計算最終排名分數並收集計算詳情
            final_score, final_details = self._calculate_final_score_with_details(factor_scores, strategy_config['ranking_logic'])
            calculation_details['final_calculation'] = final_details
            
            # 創建格式化的 calculation 報告
            formatted_calculation = CalculationFormatter.create_formatted_calculation(calculation_details)
            
            results.append({
                'trading_pair': pair,
                'date': target_date,
                'final_ranking_score': final_score,
                'component_scores': component_scores,
                'long_term_score': final_score,  # 暫時使用最終分數
                'short_term_score': final_score,  # 暫時使用最終分數
                'combined_roi_z_score': final_score,  # 暫時使用最終分數
                'final_combination_value': f"Factors: {list(factor_scores.keys())}",
                'calculation': formatted_calculation  # 新增格式化的計算詳情
            })
        
        # 轉換為 DataFrame
        result_df = pd.DataFrame(results)
        
        if result_df.empty:
            print("⚠️ 沒有計算出任何有效的因子分數")
            return result_df
        
        # 排序並添加排名
        result_df = result_df.sort_values('final_ranking_score', ascending=False)
        result_df['rank_position'] = range(1, len(result_df) + 1)
        
        print(f"✅ 完成因子策略計算，共 {len(result_df)} 個交易對")
        return result_df
    
    def _calculate_final_score(self, factor_scores: Dict[str, float], ranking_logic: Dict[str, Any]) -> float:
        """
        計算最終排名分數
        
        Args:
            factor_scores: 各因子分數字典
            ranking_logic: 排名邏輯配置
            
        Returns:
            最終分數
        """
        indicators = ranking_logic['indicators']
        weights = ranking_logic['weights']
        
        if len(indicators) != len(weights):
            raise ValueError("因子數量與權重數量不匹配")
        
        # 計算加權分數
        weighted_sum = 0.0
        total_weight = 0.0
        
        for indicator, weight in zip(indicators, weights):
            if indicator in factor_scores:
                score = factor_scores[indicator]
                if not np.isnan(score):
                    weighted_sum += score * weight
                    total_weight += weight
        
        if total_weight == 0:
            return np.nan
        
        # 正規化權重
        final_score = weighted_sum / total_weight
        
        return final_score
    
    def _calculate_final_score_with_details(self, factor_scores: Dict[str, float], ranking_logic: Dict[str, Any]) -> tuple[float, Dict[str, Any]]:
        """
        計算最終排名分數並返回計算詳情
        
        Args:
            factor_scores: 各因子分數字典
            ranking_logic: 排名邏輯配置
            
        Returns:
            (最終分數, 計算詳情)
        """
        indicators = ranking_logic['indicators']
        weights = ranking_logic['weights']
        
        details = {
            'indicators': indicators,
            'weights': weights,
            'factor_contributions': {},
            'weighted_sum_formula': '',
            'calculation_breakdown': {},
            'total_weight_used': 0.0,
            'final_score': np.nan
        }
        
        if len(indicators) != len(weights):
            details['error'] = "因子數量與權重數量不匹配"
            return np.nan, details
        
        # 計算加權分數
        weighted_sum = 0.0
        total_weight = 0.0
        formula_parts = []
        
        for indicator, weight in zip(indicators, weights):
            if indicator in factor_scores:
                score = factor_scores[indicator]
                if not np.isnan(score):
                    contribution = score * weight
                    weighted_sum += contribution
                    total_weight += weight
                    
                    details['factor_contributions'][indicator] = {
                        'raw_score': float(score),
                        'weight': float(weight),
                        'contribution': float(contribution)
                    }
                    
                    formula_parts.append(f"({score:.6f} × {weight})")
                else:
                    details['factor_contributions'][indicator] = {
                        'raw_score': 'NaN',
                        'weight': float(weight),
                        'contribution': 0.0,
                        'status': 'excluded_nan'
                    }
            else:
                details['factor_contributions'][indicator] = {
                    'raw_score': 'missing',
                    'weight': float(weight),
                    'contribution': 0.0,
                    'status': 'missing_factor'
                }
        
        details['weighted_sum_formula'] = ' + '.join(formula_parts) if formula_parts else 'No valid factors'
        details['total_weight_used'] = float(total_weight)
        
        if total_weight == 0:
            details['final_score'] = np.nan
            details['error'] = '沒有有效的因子分數'
            return np.nan, details
        
        # 正規化權重
        final_score = weighted_sum / total_weight
        details['final_score'] = float(final_score)
        details['calculation_breakdown'] = {
            'weighted_sum': float(weighted_sum),
            'total_weight': float(total_weight),
            'normalized_score': float(final_score)
        }
        
        return final_score, details
    
    def check_data_sufficiency(self, strategy_name: str, target_date: str = None) -> tuple[bool, str]:
        """
        檢查策略所需的數據是否充足
        
        Args:
            strategy_name: 策略名稱
            target_date: 目標日期
            
        Returns:
            (是否充足, 詳細信息)
        """
        if strategy_name not in FACTOR_STRATEGIES:
            return False, f"未知的策略: {strategy_name}"
        
        strategy_config = FACTOR_STRATEGIES[strategy_name]
        data_req = strategy_config['data_requirements']
        min_days = data_req['min_data_days']
        skip_days = data_req['skip_first_n_days']
        
        # 獲取目標日期
        if target_date is None:
            start_date, end_date = self.db_manager.get_return_metrics_date_range()
            if not end_date:
                return False, "數據庫中沒有 return_metrics 數據"
            target_date = end_date
        
        target_date_obj = pd.to_datetime(target_date)
        
        # 檢查數據庫中的數據範圍
        start_date, end_date = self.db_manager.get_return_metrics_date_range()
        if not end_date:
            return False, "數據庫中沒有 return_metrics 數據"
        
        earliest_date = pd.to_datetime(start_date)
        latest_date = pd.to_datetime(end_date)
        
        # 檢查目標日期是否在數據範圍內
        if target_date_obj > latest_date:
            return False, f"目標日期 {target_date} 超出數據範圍 (最新: {end_date})"
        
        # 檢查是否有足夠的歷史數據
        # 計算實際可用天數（包含起始和結束日期）
        available_days = (target_date_obj - earliest_date).days + 1
        required_days = min_days + skip_days
        
        if available_days < required_days:
            return False, f"數據量不足：策略需要 {required_days} 天數據，但只有 {available_days} 天可用 (從 {start_date} 到 {target_date})"
        
        # 檢查是否有交易對符合skip_days條件
        if skip_days > 0:
            # 快速檢查：計算從數據開始日期到目標日期的天數
            days_from_start = (target_date_obj - earliest_date).days + 1
            if days_from_start <= skip_days:
                return False, f"無交易對符合條件：所有交易對上線時間不足 {skip_days} 天 (實際: {days_from_start} 天)"
        
        # 檢查各因子所需的最大窗口
        max_window = 0
        factor_windows = []
        for factor_name, factor_config in strategy_config['factors'].items():
            window = factor_config['window']
            max_window = max(max_window, window)
            factor_windows.append(f"{factor_name}({window}天)")
        
        # 檢查是否有足夠的數據來計算最大窗口的因子
        total_required_days = max_window + skip_days
        if available_days < total_required_days:
            return False, f"因子計算數據不足：最大因子窗口需要 {total_required_days} 天，但只有 {available_days} 天可用。因子窗口: {', '.join(factor_windows)}"
        
        return True, f"數據充足：可用數據 {available_days} 天，滿足策略要求"

    def run_strategy(self, strategy_name: str, target_date: str = None, save_to_db: bool = True) -> pd.DataFrame:
        """
        執行策略計算並保存結果
        
        Args:
            strategy_name: 策略名稱
            target_date: 目標日期
            save_to_db: 是否保存到數據庫
            
        Returns:
            排名結果 DataFrame
        """
        print(f"\n🚀 執行因子策略: {strategy_name}")
        
        # 預檢查數據是否充足
        is_sufficient, message = self.check_data_sufficiency(strategy_name, target_date)
        if not is_sufficient:
            print(f"❌ 數據量檢查失敗: {message}")
            print("💡 建議:")
            print("   • 使用較晚的日期 (如最新日期)")
            print("   • 選擇數據要求較低的策略 (如 test_factor_simple)")
            print("   • 確保有足夠的歷史數據")
            return pd.DataFrame()  # 返回空的 DataFrame
        
        print(f"✅ 數據量檢查通過: {message}")
        
        # 計算排名
        result_df = self.calculate_strategy_ranking(strategy_name, target_date)
        
        if result_df.empty:
            print("❌ 策略計算失敗，沒有結果")
            return result_df
        
        # 保存到數據庫 (使用既有的 strategy_ranking 表)
        if save_to_db:
            count = self.db_manager.insert_strategy_ranking(result_df, strategy_name)
            print(f"💾 已保存 {count} 條記錄到 strategy_ranking 表")
        
        # 顯示前10名
        print(f"\n📊 {strategy_name} 策略前10名:")
        print("排名 | 交易對 | 最終分數")
        print("-" * 40)
        for _, row in result_df.head(10).iterrows():
            print(f"{row['rank_position']:2d}   | {row['trading_pair']:<20} | {row['final_ranking_score']:.6f}")
        
        return result_df
    
    def run_all_strategies(self, target_date: str = None) -> Dict[str, pd.DataFrame]:
        """
        執行所有策略
        
        Args:
            target_date: 目標日期
            
        Returns:
            所有策略結果的字典
        """
        results = {}
        
        for strategy_name in FACTOR_STRATEGIES.keys():
            try:
                result = self.run_strategy(strategy_name, target_date)
                results[strategy_name] = result
            except Exception as e:
                print(f"❌ 策略 {strategy_name} 執行失敗: {e}")
                results[strategy_name] = pd.DataFrame()
        
        return results

if __name__ == "__main__":
    # 測試因子引擎
    engine = FactorEngine()
    
    # 測試簡單策略
    try:
        result = engine.run_strategy('test_factor_simple')
        print(f"\n✅ 測試完成，得到 {len(result)} 個結果")
    except Exception as e:
        print(f"❌ 測試失敗: {e}")
        import traceback
        traceback.print_exc() 