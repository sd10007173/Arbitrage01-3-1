# 因子策略系統 (Factor Strategy System)

## 概述

因子策略系統是一個基於數據庫的量化策略框架，用於計算和排名加密貨幣交易對的因子分數。系統從現有的 `return_metrics` 表讀取數據，通過多種數學因子計算交易對的綜合評分，並將結果保存到既有的 `strategy_ranking` 表中，與原有策略系統完全整合。

## 系統架構

```
factor_strategies/
├── factor_strategy_config.py    # 策略配置文件
├── factor_library.py           # 因子計算函式庫
├── factor_engine.py            # 因子計算引擎
├── run_factor_strategies.py    # 執行腳本
├── test_factor_strategies.py   # 測試腳本
└── README.md                   # 說明文檔
```

### 核心組件

1. **策略配置 (factor_strategy_config.py)**
   - 定義所有可用的因子策略
   - 配置因子參數和權重
   - 設定數據要求和過濾規則

2. **因子函式庫 (factor_library.py)**
   - 純數學計算函數
   - 包含趨勢、風險、穩定性等因子
   - 支持自定義參數

3. **計算引擎 (factor_engine.py)**
   - 核心計算邏輯
   - 數據庫讀寫操作
   - 策略執行和結果生成

4. **執行腳本 (run_factor_strategies.py)**
   - 簡化的交互式界面
   - 支持單策略或批量執行
   - 日期範圍執行功能
   - 結果查看和分析

## 數據流程

```
return_metrics 表
      ↓
   數據讀取和過濾
      ↓
   因子計算 (多個因子)
      ↓
   加權組合和排名
      ↓
strategy_ranking 表 (與既有系統整合)
```

## 可用策略

### 1. Cerebrum-Core v1.0 (`cerebrum_core`)
- **描述**: 結合長期趨勢、風險調整回報、穩定性和勝率的綜合因子模型
- **因子**: 趨勢斜率(10%), 夏普比率(40%), 穩定性(30%), 勝率(20%)
- **數據要求**: 最少30天數據，跳過前3天

### 2. Cerebrum-Momentum v1.0 (`cerebrum_momentum`)
- **描述**: 專注於動量和趨勢的因子策略
- **因子**: 長期趨勢(50%), 短期趨勢(30%), 動量夏普(20%)
- **數據要求**: 最少60天數據，跳過前5天

### 3. Cerebrum-Stability v1.0 (`cerebrum_stability`)
- **描述**: 專注於穩定性和風險控制的因子策略
- **因子**: 長期穩定性(35%), 短期穩定性(25%), 持續勝率(25%), 穩定夏普(15%)
- **數據要求**: 最少90天數據，跳過前7天

### 4. Simple Factor Test (`test_factor_simple`)
- **描述**: 用於測試的簡單策略
- **因子**: 簡單趨勢(100%)
- **數據要求**: 最少7天數據

## 因子說明

### 趨勢因子 (Trend Factors)
- **calculate_trend_slope**: 計算累積回報的線性回歸斜率
- 正值表示上升趨勢，負值表示下降趨勢

### 風險調整因子 (Risk-Adjusted Factors)
- **calculate_sharpe_ratio**: 夏普比率，衡量風險調整後收益
- **calculate_sortino_ratio**: 索提諾比率，只考慮下行風險

### 穩定性因子 (Stability Factors)
- **calculate_inv_std_dev**: 標準差倒數，衡量收益穩定性
- **calculate_max_drawdown**: 最大回撤，衡量最大損失

### 勝率因子 (Win Rate Factors)
- **calculate_win_rate**: 獲利天數比例

## 使用方法

### 1. 快速開始（推薦）

執行交互式界面，系統會自動引導您完成所有步驟：

```bash
python factor_strategies/run_factor_strategies.py
```

**交互流程**：
1. 顯示所有可用策略
2. 選擇要執行的策略（或輸入 'all' 執行所有策略）
3. 輸入起始日期（預設為最新日期）
4. 輸入結束日期（預設為最新日期）
5. 系統自動執行並顯示進度
6. 詢問是否查看執行結果

### 2. 程式化使用

```python
from factor_strategies.factor_engine import FactorEngine

# 初始化引擎
engine = FactorEngine()

# 執行單個策略
result = engine.run_strategy('cerebrum_core')

# 執行所有策略
results = engine.run_all_strategies()
```

### 3. 測試系統

```bash
# 運行完整測試
python factor_strategies/test_factor_strategies.py
```

## 詳細使用案例

### 案例 1: 首次使用 - 執行測試策略

**場景**: 第一次使用系統，想要快速測試功能

**步驟**:
1. 執行系統：
   ```bash
   python factor_strategies/run_factor_strategies.py
   ```

2. 系統顯示可用策略：
   ```
   可用的因子策略：
   1. cerebrum_core - Cerebrum-Core v1.0
   2. cerebrum_momentum - Cerebrum-Momentum v1.0  
   3. cerebrum_stability - Cerebrum-Stability v1.0
   4. test_factor_simple - Simple Factor Test
   ```

3. 選擇測試策略：
   ```
   請選擇要執行的策略 (輸入策略名稱或 'all' 執行所有策略): test_factor_simple
   ```

4. 使用預設日期（直接按 Enter）：
   ```
   請輸入起始日期 (YYYY-MM-DD, 預設: 2024-12-22): 
   請輸入結束日期 (YYYY-MM-DD, 預設: 2024-12-22): 
   ```

5. 系統執行並顯示結果：
   ```
   正在執行策略 'test_factor_simple' 從 2024-12-22 到 2024-12-22...
   執行日期: 2024-12-22 - 成功處理 50 個交易對
   執行完成！成功: 1/1 天
   ```

6. 查看結果：
   ```
   是否要查看執行結果？(y/n): y
   
   策略: test_factor_simple, 日期: 2024-12-22
   排名前10的交易對:
   1. BTCUSDT - 分數: 0.85
   2. ETHUSDT - 分數: 0.82
   ...
   ```

### 案例 2: 生產環境 - 執行核心策略

**場景**: 在生產環境中定期執行核心策略進行選幣

**步驟**:
1. 執行系統並選擇核心策略：
   ```bash
   python factor_strategies/run_factor_strategies.py
   ```
   ```
   請選擇要執行的策略: cerebrum_core
   ```

2. 執行最近一週的數據：
   ```
   請輸入起始日期 (YYYY-MM-DD, 預設: 2024-12-22): 2024-12-16
   請輸入結束日期 (YYYY-MM-DD, 預設: 2024-12-22): 2024-12-22
   ```

3. 系統批量執行：
   ```
   正在執行策略 'cerebrum_core' 從 2024-12-16 到 2024-12-22...
   執行日期: 2024-12-16 - 成功處理 48 個交易對
   執行日期: 2024-12-17 - 成功處理 49 個交易對
   執行日期: 2024-12-18 - 成功處理 47 個交易對
   執行日期: 2024-12-19 - 跳過 (數據不足)
   執行日期: 2024-12-20 - 成功處理 50 個交易對
   執行日期: 2024-12-21 - 成功處理 51 個交易對
   執行日期: 2024-12-22 - 成功處理 52 個交易對
   執行完成！成功: 6/7 天
   ```

4. 查看最新結果：
   ```
   策略: cerebrum_core, 日期: 2024-12-22
   排名前10的交易對:
   1. SOLUSDT - 分數: 0.92 (趨勢: 0.15, 夏普: 0.88, 穩定性: 0.95, 勝率: 0.78)
   2. AVAXUSDT - 分數: 0.89 (趨勢: 0.12, 夏普: 0.85, 穩定性: 0.92, 勝率: 0.81)
   ...
   ```

### 案例 3: 策略比較 - 執行所有策略

**場景**: 比較不同策略的表現，選擇最適合的策略

**步驟**:
1. 執行所有策略：
   ```bash
   python factor_strategies/run_factor_strategies.py
   ```
   ```
   請選擇要執行的策略: all
   請輸入起始日期: 2024-12-22
   請輸入結束日期: 2024-12-22
   ```

2. 系統依序執行所有策略：
   ```
   正在執行所有策略從 2024-12-22 到 2024-12-22...
   
   執行策略: cerebrum_core
   執行日期: 2024-12-22 - 成功處理 52 個交易對
   
   執行策略: cerebrum_momentum  
   執行日期: 2024-12-22 - 成功處理 45 個交易對
   
   執行策略: cerebrum_stability
   執行日期: 2024-12-22 - 成功處理 38 個交易對
   
   執行策略: test_factor_simple
   執行日期: 2024-12-22 - 成功處理 52 個交易對
   
   所有策略執行完成！
   ```

3. 比較結果：
   ```
   是否要查看執行結果？(y/n): y
   
   選擇要查看的策略:
   1. cerebrum_core
   2. cerebrum_momentum
   3. cerebrum_stability  
   4. test_factor_simple
   請選擇 (1-4): 1
   
   策略: cerebrum_core, 日期: 2024-12-22
   前10名交易對及其各因子分數...
   ```

### 案例 4: 程式化整合

**場景**: 將因子策略整合到現有的交易系統中

```python
from factor_strategies.factor_engine import FactorEngine
from database_operations import DatabaseManager
from datetime import datetime, timedelta

# 初始化
engine = FactorEngine()
db = DatabaseManager()

# 執行策略並獲取結果
def get_daily_rankings(strategy_name='cerebrum_core', date=None):
    """獲取指定日期的策略排名"""
    if date is None:
        date = datetime.now().strftime('%Y-%m-%d')
    
    # 執行策略
    result = engine.run_strategy(strategy_name, target_date=date)
    
    if result['success']:
        # 從數據庫獲取結果
        rankings = db.get_latest_ranking(strategy_name, date)
        return rankings[:10]  # 返回前10名
    else:
        print(f"策略執行失敗: {result['error']}")
        return []

# 自動化選幣流程
def automated_coin_selection():
    """自動化選幣流程"""
    # 執行多個策略
    strategies = ['cerebrum_core', 'cerebrum_momentum', 'cerebrum_stability']
    all_rankings = {}
    
    for strategy in strategies:
        rankings = get_daily_rankings(strategy)
        all_rankings[strategy] = rankings
    
    # 找出在多個策略中都排名靠前的幣種
    common_coins = set()
    for strategy, rankings in all_rankings.items():
        top_coins = [r['trading_pair'] for r in rankings[:5]]
        if not common_coins:
            common_coins = set(top_coins)
        else:
            common_coins = common_coins.intersection(set(top_coins))
    
    print(f"多策略共同推薦的幣種: {list(common_coins)}")
    return list(common_coins)

# 執行自動化選幣
recommended_coins = automated_coin_selection()
```

### 案例 5: 歷史回測分析

**場景**: 分析策略在歷史數據上的表現

```python
from factor_strategies.factor_engine import FactorEngine
from database_operations import DatabaseManager
import pandas as pd
from datetime import datetime, timedelta

def historical_performance_analysis(strategy_name, days=30):
    """分析策略歷史表現"""
    engine = FactorEngine()
    db = DatabaseManager()
    
    # 生成日期範圍
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    # 批量執行策略
    current_date = start_date
    results = []
    
    while current_date <= end_date:
        date_str = current_date.strftime('%Y-%m-%d')
        result = engine.run_strategy(strategy_name, target_date=date_str)
        
        if result['success']:
            # 獲取當日排名
            rankings = db.get_latest_ranking(strategy_name, date_str)
            if rankings:
                top_coin = rankings[0]
                results.append({
                    'date': date_str,
                    'top_coin': top_coin['trading_pair'],
                    'score': top_coin['final_ranking_score']
                })
        
        current_date += timedelta(days=1)
    
    # 分析結果
    df = pd.DataFrame(results)
    print(f"\n{strategy_name} 策略 {days} 天歷史分析:")
    print(f"成功執行天數: {len(df)}")
    print(f"平均分數: {df['score'].mean():.4f}")
    print(f"分數標準差: {df['score'].std():.4f}")
    
    # 分析幣種穩定性
    coin_counts = df['top_coin'].value_counts()
    print(f"\n最常出現在榜首的幣種:")
    print(coin_counts.head())
    
    return df

# 執行歷史分析
performance_df = historical_performance_analysis('cerebrum_core', days=30)
```

## 配置新策略

在 `factor_strategy_config.py` 中添加新策略：

```python
'my_new_strategy': {
    'name': '我的新策略',
    'description': '策略描述',
    'data_requirements': {
        'min_data_days': 30,
        'skip_first_n_days': 3,
    },
    'factors': {
        'my_factor': {
            'function': 'calculate_trend_slope',
            'window': 60,
            'input_col': 'roi_1d',
            'params': {}
        }
    },
    'ranking_logic': {
        'indicators': ['my_factor'],
        'weights': [1.0]
    }
}
```

## 添加新因子

在 `factor_library.py` 中添加新的計算函數：

```python
def calculate_my_factor(series: pd.Series, **kwargs) -> float:
    """
    我的自定義因子計算
    
    Args:
        series: 時間序列數據
        **kwargs: 其他參數
        
    Returns:
        因子分數
    """
    # 你的計算邏輯
    return result
```

## 數據庫表結構

### strategy_ranking 表（與既有系統共用）
- `strategy_name`: 策略名稱（因子策略會使用配置中的策略名稱）
- `trading_pair`: 交易對
- `date`: 計算日期
- `final_ranking_score`: 最終排名分數
- `rank_position`: 排名位置
- `long_term_score`: 長期分數（對應因子組合分數）
- `short_term_score`: 短期分數（對應因子組合分數）
- `combined_roi_z_score`: 組合ROI Z分數
- `final_combination_value`: 最終組合值
- `component_scores`: 各因子分數 (JSON格式)

**整合優勢**：
- 因子策略結果可以使用所有既有的查詢和分析功能
- 無需額外的數據庫表和維護成本
- 可以直接與原有策略進行比較和分析

## 系統要求

- Python 3.7+
- pandas, numpy, scipy
- 現有的數據庫系統 (database_operations.py)
- return_metrics 表中的歷史數據

## 性能特點

- **高效計算**: 向量化操作，批量處理
- **靈活配置**: 易於添加新策略和因子
- **數據庫集成**: 與既有系統完全整合
- **錯誤處理**: 完善的異常處理機制
- **簡化界面**: 直觀的交互流程

## 注意事項

1. **數據依賴**: 需要 `return_metrics` 表中有足夠的歷史數據
2. **新幣過濾**: 系統會自動過濾上線時間不足的新幣
3. **因子權重**: 確保策略配置中的權重總和為1
4. **計算窗口**: 因子計算窗口不能超過可用數據天數
5. **系統整合**: 因子策略結果保存在 `strategy_ranking` 表中，與原有策略共存

## 故障排除

### 常見問題

1. **沒有數據**: 確保 `return_metrics` 表中有數據
2. **計算失敗**: 檢查策略配置是否正確
3. **權重錯誤**: 確保因子權重總和為1
4. **函數不存在**: 檢查因子函數名是否正確
5. **日期格式錯誤**: 確保日期格式為 YYYY-MM-DD

### 調試方法

```python
# 運行測試
python factor_strategies/test_factor_strategies.py

# 檢查數據庫狀態
from database_operations import DatabaseManager
db = DatabaseManager()
info = db.get_database_info()
print(info)

# 檢查特定日期的數據
data = db.get_return_metrics_by_date('2024-12-22')
print(f"可用交易對數量: {len(data)}")
```

## 更新日誌

- **v2.0** (2024-12-22): 系統整合更新
  - 數據保存到既有的 `strategy_ranking` 表
  - 簡化交互界面，直接選擇策略和日期範圍
  - 支援批量日期範圍執行
  - 移除冗餘的數據庫表和函數
  - 完全整合到既有系統架構

- **v1.0** (2024-12-22): 初始版本
  - 包含4個預設策略和6個因子函數
  - 完整的數據庫集成和測試框架 