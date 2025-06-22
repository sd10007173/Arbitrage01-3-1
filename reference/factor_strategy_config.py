"""
因子策略設定檔 (Factor Strategy Configuration)

此檔案是所有基於歷史數據的因子策略的控制中心。
每個策略都在 FACTOR_STRATEGIES 字典中進行完整、獨立的定義。

您可以通過修改此檔案來：
- 新增一個全新的因子策略。
- 調整現有策略的數據准入規則 (如所需歷史天數)。
- 為策略新增、移除或修改因子。
- 調整因子計算所需的回看窗口 (window) 或特殊參數 (params)。
- 調整最終排名時各個因子的權重 (weights)。
"""

FACTOR_STRATEGIES = {
    'cerebrum_core': {
        'name': 'Cerebrum-Core v1.0',
        'description': '結合長期趨勢、風險調整回報、穩定性和勝率的綜合因子模型。',

        # --- 數據准入規則 ---
        'data_requirements': {
            'min_data_days': 30,         # 統一鍵名
            'skip_first_n_days': 3,      # 統一鍵名
        },

        # --- 因子定義 (此策略需要哪些因子以及如何計算) ---
        'factors': {
            # 因子名稱: {計算細節}
            'F_trend': {
                'function': 'calculate_trend_slope', # 對應 factor_library.py 中的函式名
                'window': 90,                        # 計算所需的回看天數
                'input_col': 'funding_rate_diff',    # 計算所需的數據欄位
            },
            'F_sharpe': {
                'function': 'calculate_sharpe_ratio',
                'window': 60,
                'input_col': 'funding_rate_diff',
                'params': {'annualizing_factor': 365} # 傳遞給計算函式的額外參數
            },
            'F_stability': {
                'function': 'calculate_inv_std_dev',
                'window': 60,
                'input_col': 'funding_rate_diff',
                'params': {'epsilon': 1e-9}
            },
            'F_winrate': {
                'function': 'calculate_win_rate',
                'window': 60,
                'input_col': 'funding_rate_diff',
            }
        },

        # --- 最終排名邏輯 (如何組合這些因子) ---
        'ranking_logic': {
            'indicators': ['F_trend', 'F_sharpe', 'F_stability', 'F_winrate'], # 必須與上面定義的因子名稱對應
            'weights': [0.10, 0.40, 0.30, 0.20] # 權重總和應為1
        }
    },

    'cerebrum_v2_experimental': {
        'name': 'Cerebrum-Core v2.0 (Experimental)',
        'description': '測試版本，更注重趨勢和穩定性，並使用更長的歷史窗口。',
        'data_requirements': {
            'min_data_days': 60,
            'skip_first_n_days': 5,
        },
        'factors': {
            'trend': {
                'func': 'calculate_regression_slope',
                'lookback': 90,
                'weight': 0.4,
                'input_col': 'funding_rate_diff'
            },
            'sharpe': {
                'func': 'calculate_sharpe_ratio',
                'lookback': 60,
                'weight': 0.6,
                'input_col': 'funding_rate_diff'
            },
        },
    },

    # --- 測試專用策略 ---
    'test_strategy': {
        'name': 'Simple Stability Test Strategy',
        'data_requirements': {
            'min_data_days': 1,      # 最小化數據需求
            'skip_first_n_days': 0,
        },
        'factors': {
            'stability_simple': {
                'func': 'calculate_inv_std_dev', # 只用一個簡單的因子
                'lookback': 3,                 # 短回看週期
                'weight': 1.0,                 # 權重為1，結果就是因子本身
                'input_col': 'funding_rate_diff'
            },
        }
    }
} 