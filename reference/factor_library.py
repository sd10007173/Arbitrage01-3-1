"""

因子計算函式庫 (Factor Library)

此檔案包含所有因子計算的純數學函式。
每個函式都接收一個 pandas Series (代表時間序列數據) 和可選的參數，
並返回一個單一的浮點數值作為因子分數。

這些函式是無狀態的，且與任何特定的策略或數據源無關。
"""

import pandas as pd
import numpy as np
from scipy.stats import linregress

def calculate_trend_slope(series: pd.Series, **kwargs) -> float:
    """
    計算累積回報序列的線性回歸斜率。

    Args:
        series (pd.Series): 一段時間內的回報率序列 (例如 '1d_return')。

    Returns:
        float: 計算出的回歸線斜率。如果數據不足，返回 np.nan。
    """
    if len(series) < 2:
        return np.nan
    
    # 確保 series 中的 NaN 值被處理
    series = series.dropna()
    if len(series) < 2:
        return np.nan

    cumulative_return = series.cumsum()
    time_index = np.arange(len(cumulative_return))
    
    # 使用scipy.stats.linregress計算斜率，它高效且穩定
    slope, _, _, _, _ = linregress(time_index, cumulative_return)
    
    return slope

def calculate_sharpe_ratio(series: pd.Series, annualizing_factor: int = 365, **kwargs) -> float:
    """
    計算年化夏普比率。

    Args:
        series (pd.Series): 回報率序列。
        annualizing_factor (int): 年化係數 (日數據為365，小時數據為365*24等)。

    Returns:
        float: 年化夏普比率。如果標準差為0，返回0。
    """
    # 確保 series 中的 NaN 值被處理
    series = series.dropna()
    if series.empty:
        return np.nan
        
    mean_return = series.mean()
    std_dev = series.std()

    if std_dev == 0 or np.isnan(std_dev):
        # 如果波動為0，且平均回報為正，給予一個極大的夏普值
        # 如果平均回報也為0或負，則夏普為0
        return np.inf if mean_return > 0 else 0.0

    return (mean_return / std_dev) * np.sqrt(annualizing_factor)

def calculate_inv_std_dev(series: pd.Series, epsilon: float = 1e-9, high_score: float = 1e9, **kwargs) -> float:
    """
    計算回報率標準差的倒數，作為穩定性指標。
    最終版邏輯：返回有限、明確的數值。
    """
    series = series.dropna()
    if series.empty:
        return 0.0 # 空數據返回 0

    mean_return = series.mean()
    
    # 如果平均回報為負或零，穩定性無意義，返回 0
    if mean_return <= 0:
        return 0.0

    std_dev = series.std()
    
    # 如果波動為0且平均回報為正，給予極高的、但有限的穩定性分數
    if std_dev < epsilon:
        return high_score

    return 1 / std_dev

def calculate_win_rate(series: pd.Series, **kwargs) -> float:
    """
    計算勝率，即回報大於0的天數所佔的比例。

    Args:
        series (pd.Series): 回報率序列。

    Returns:
        float: 勝率 (介於0和1之間)。
    """
    # 確保 series 中的 NaN 值被處理
    series = series.dropna()
    if len(series) == 0:
        return 0.0

    winning_days = (series > 0).sum()
    return winning_days / len(series)

# --- 您未來可以在此處添加更多因子計算函式 ---
# 例如: Sortino Ratio, Max Drawdown, etc.
#
# def calculate_sortino_ratio(series: pd.Series, annualizing_factor: int = 365, **kwargs) -> float:
#     ...
#
# def calculate_max_drawdown(series: pd.Series, **kwargs) -> float:
#     ... 