# 因子策略系統 (Factor Strategy System)

## 📋 系統概述

因子策略系統是一個基於多因子模型的智能交易對排名系統，能夠根據歷史資金費率數據計算各種技術因子，並生成投資排名建議。系統採用模組化設計，支援多種策略配置，適用於不同風險偏好的投資決策。

## 🏗️ 系統架構

### 核心組件

1. **factor_strategy_config.py** - 策略配置中心
2. **factor_library.py** - 因子計算函式庫  
3. **factor_engine.py** - 因子計算引擎
4. **run_factor_strategies.py** - 策略執行腳本

### 數據流程

```
return_metrics 表 → 因子引擎 → 因子計算 → 策略排名 → factor_strategy_ranking 表
```

## 📄 檔案功能說明

### 1. 🎛️ **factor_strategy_config.py**
- **用途**：策略配置管理中心
- **目的**：定義所有因子策略的參數和邏輯
- **功能**：
  - 策略參數配置
  - 數據要求設定
  - 因子定義管理
  - 權重分配控制
- **程式邏輯**：配置定義 → 參數驗證 → 策略註冊 → 引擎調用

### 2. 🧮 **factor_library.py**
- **用途**：因子計算函式庫
- **目的**：提供所有技術因子的純數學計算函式
- **功能**：
  - 趨勢因子計算
  - 風險調整因子
  - 穩定性因子
  - 績效評估因子
- **程式邏輯**：數據輸入 → 數學計算 → 因子分數 → 結果輸出

### 3. ⚙️ **factor_engine.py**
- **用途**：因子計算引擎
- **目的**：核心計算引擎，整合所有因子計算並生成最終排名
- **功能**：
  - 數據庫連接管理
  - 策略數據獲取
  - 因子分數計算
  - 排名結果生成
- **程式邏輯**：數據獲取 → 因子計算 → 分數標準化 → 加權排名 → 結果保存

### 4. 🚀 **run_factor_strategies.py**
- **用途**：策略執行腳本
- **目的**：提供交互式界面執行因子策略
- **功能**：
  - 交互式策略選擇
  - 自動日期範圍檢測
  - 批量策略執行
  - 結果查看展示
- **程式邏輯**：用戶交互 → 參數配置 → 策略調用 → 結果展示

## 🚀 業務流程

### **步驟 1: 策略配置定義 - factor_strategy_config.py**

**功能**：定義因子策略的完整配置，包括數據要求、因子定義和排名邏輯

**配置結構**：
```python
FACTOR_STRATEGIES = {
    'strategy_name': {
        'name': '策略顯示名稱',
        'description': '策略說明',
        'data_requirements': {
            'min_data_days': 30,         # 最少歷史數據天數
            'skip_first_n_days': 3,      # 跳過新幣前N天
        },
        'factors': {
            'factor_name': {
                'function': 'calculate_xxx',  # 計算函式名
                'window': 60,                 # 回看窗口天數
                'input_col': 'return_1d',     # 輸入數據列
                'params': {...}               # 額外參數
            }
        },
        'ranking_logic': {
            'indicators': ['factor1', 'factor2'],
            'weights': [0.6, 0.4]            # 權重分配
        }
    }
}
```

**內建策略**：
- **cerebrum_core**: 綜合因子策略（趨勢+夏普+穩定性+勝率）
- **cerebrum_momentum**: 動量策略（長短期趨勢+動量夏普）
- **cerebrum_stability**: 穩定性策略（多層穩定性+勝率）
- **sharpe_only**: 純夏普比率策略
- **test_factor_simple**: 測試策略（簡單趨勢）

**測試重點**：
- ✅ **配置完整性**：確認所有必要參數都已定義
- ✅ **權重總和**：驗證 ranking_logic 權重總和為 1.0
- ✅ **函式對應**：檢查因子函式名是否在 factor_library 中存在
- ✅ **參數合理性**：確認窗口大小、數據要求等參數合理
- ✅ **策略多樣性**：驗證不同風險偏好的策略配置正確

---

### **步驟 2: 因子計算函式 - factor_library.py**

**功能**：提供所有技術因子的純數學計算函式，每個函式接收時間序列數據並返回因子分數

**輸入格式**：
- `series`: pandas Series（時間序列數據）
- `**kwargs`: 額外參數（如年化係數、回看窗口等）

**核心因子函式**：

1. **calculate_trend_slope()**
   - **用途**：計算線性回歸斜率
   - **輸入**：收益率序列
   - **輸出**：趨勢斜率（正值=上升趨勢，負值=下降趨勢）
   - **計算邏輯**：使用 scipy.stats.linregress 計算回歸斜率

2. **calculate_sharpe_ratio()**
   - **用途**：計算年化夏普比率
   - **輸入**：收益率序列 + 年化係數
   - **輸出**：風險調整收益率
   - **計算邏輯**：`(平均收益 / 標準差) * sqrt(年化係數)`

3. **calculate_inv_std_dev()**
   - **用途**：計算穩定性指標（標準差倒數）
   - **輸入**：收益率序列 + epsilon值
   - **輸出**：穩定性分數（值越高越穩定）
   - **計算邏輯**：`1 / 標準差`，特殊處理零波動情況

4. **calculate_win_rate()**
   - **用途**：計算勝率
   - **輸入**：收益率序列
   - **輸出**：獲利天數比例（0-1之間）
   - **計算邏輯**：`獲利天數 / 總天數`

5. **calculate_max_drawdown()**
   - **用途**：計算最大回撤
   - **輸入**：收益率序列
   - **輸出**：最大回撤比例（負值）
   - **計算邏輯**：計算累積收益的峰谷最大損失

6. **calculate_sortino_ratio()**
   - **用途**：計算索提諾比率
   - **輸入**：收益率序列 + 年化係數
   - **輸出**：下行風險調整收益率
   - **計算邏輯**：只考慮負收益的標準差

**範例計算**：
```python
# 輸入：60天的日收益率序列
series = pd.Series([0.001, -0.002, 0.003, ...])  # 60個數值

# 因子計算結果
trend_slope = 0.000012      # 微弱上升趨勢
sharpe_ratio = 1.85         # 良好的風險調整收益
inv_std_dev = 125.6         # 高穩定性
win_rate = 0.62             # 62%勝率
max_drawdown = -0.08        # 最大回撤8%
sortino_ratio = 2.34        # 優秀的下行風險控制
```

**測試重點**：
- ✅ **計算準確性**：驗證各因子函式的數學計算正確性
- ✅ **邊界情況處理**：測試空數據、單一數值、極值等邊界情況
- ✅ **NaN處理**：確認 NaN 值被正確處理或過濾
- ✅ **參數傳遞**：驗證 kwargs 參數正確傳遞和使用
- ✅ **數值穩定性**：測試極小或極大數值的計算穩定性
- ✅ **性能效率**：確認向量化計算的性能表現

---

### **步驟 3: 因子計算引擎 - factor_engine.py**

**功能**：系統核心引擎，負責從數據庫讀取數據、計算因子分數、生成策略排名並保存結果

**輸入數據源**（從 `return_metrics` 表讀取）：
- `trading_pair`: 交易對名稱
- `date`: 計算日期
- `return_1d`: 1天收益率
- `return_7d`: 7天收益率
- `roi_1d`: 1天年化收益率
- 其他收益指標...

**主要類別和方法**：

1. **FactorEngine.__init__()**
   - 初始化數據庫連接
   - 載入因子計算函式映射
   - 配置系統參數

2. **get_strategy_data()**
   - 根據策略配置獲取所需數據
   - 應用數據過濾條件（最少天數、跳過新幣等）
   - 返回符合要求的歷史數據

3. **calculate_factor_for_trading_pair()**
   - 為單個交易對計算特定因子分數
   - 應用回看窗口限制
   - 調用 factor_library 中的計算函式

4. **calculate_strategy_ranking()**
   - 計算完整策略的交易對排名
   - 標準化因子分數（0-1區間）
   - 應用權重加總計算最終分數

5. **run_strategy()**
   - 執行完整的策略計算流程
   - 保存結果到數據庫
   - 返回排名結果

**處理邏輯**：
1. **數據獲取**：根據策略配置從 return_metrics 表讀取所需數據範圍
2. **資格審查**：過濾新上線交易對，確保有足夠歷史數據
3. **因子計算**：為每個交易對計算所有配置的因子分數
4. **分數標準化**：將因子分數標準化到 0-1 區間進行排名
5. **加權組合**：根據權重配置計算最終綜合分數
6. **排名生成**：按最終分數生成交易對排名
7. **結果保存**：將排名結果保存到 factor_strategy_ranking 表

**輸出到 `factor_strategy_ranking` 表的欄位**：
- `id`: 自動遞增主鍵
- `strategy_name`: 策略名稱
- `date`: 計算日期
- `trading_pair`: 交易對名稱
- `rank_position`: 排名位置
- `final_score`: 最終綜合分數
- `F_trend`: 趨勢因子分數（如果配置）
- `F_sharpe`: 夏普因子分數（如果配置）
- `F_stability`: 穩定性因子分數（如果配置）
- `F_winrate`: 勝率因子分數（如果配置）
- 其他因子分數欄位...
- `created_at`: 記錄創建時間
- `updated_at`: 記錄更新時間

**範例結果**：
```
策略: cerebrum_core, 日期: 2025-01-31
處理交易對: 13個

計算結果:
排名  交易對                     最終分數   趨勢    夏普    穩定性  勝率
1     ADA_binance_bybit         21.273    0.85    0.92    0.78    0.65
2     ETH_binance_bybit         13.055    0.72    0.88    0.71    0.58
3     BTC_binance_bybit         11.310    0.68    0.85    0.69    0.55
4     BCH_binance_bybit          8.947    0.63    0.79    0.66    0.52
5     USDC_binance_bybit         6.234    0.58    0.75    0.63    0.48
...

權重應用: 趨勢10% + 夏普40% + 穩定性30% + 勝率20%
數據篩選: 跳過上線少於3天的交易對
有效數據: 使用60-90天歷史數據窗口
```

**測試重點**：
- ✅ **數據庫連接**：確認數據庫連接和操作正常
- ✅ **數據過濾**：驗證交易對過濾邏輯（最少天數、跳過新幣）
- ✅ **因子計算流程**：確認每個因子按配置正確計算
- ✅ **分數標準化**：驗證排名標準化邏輯正確
- ✅ **權重應用**：確認最終分數按權重正確計算
- ✅ **排名生成**：驗證排名順序正確（分數高到低）
- ✅ **數據充足性檢查**：確認數據不足時的處理機制
- ✅ **結果保存**：驗證結果正確保存到數據庫
- ✅ **錯誤處理**：測試各種異常情況的處理

---

### **步驟 4: 策略執行腳本 - run_factor_strategies.py**

**功能**：提供簡化的交互式界面來執行因子策略系統，支援單策略或批量執行

**用戶交互流程**：

1. **策略選擇界面**：
   ```
   🧠 因子策略系統 (Factor Strategy System)
   ============================================================
   
   📋 可用的因子策略:
   ------------------------------
    1. cerebrum_core
    2. cerebrum_momentum  
    3. cerebrum_stability
    4. sharpe_only
    5. test_factor_simple
    6. 全部策略 (all)
    0. 退出
   
   請選擇要執行的策略 (0-6): 
   ```

2. **自動日期範圍檢測**：
   - 從 return_metrics 表自動檢測可用日期範圍
   - 顯示將處理的總天數
   - 無需用戶手動輸入日期

3. **執行進度顯示**：
   ```
   📅 處理日期: 2025-01-31
   ⚠️ 跳過: 數據量不足：策略需要 93 天數據，但只有 31 天可用
   
   📅 處理日期: 2025-02-28
   ✅ 成功: 13 個交易對
   
   📊 執行完成: 28/59 天成功
   ```

4. **結果自動展示**：
   ```
   📊 執行結果:
   策略: cerebrum_core, 日期: 2025-02-28
   排名前10的交易對:
   ------------------------------------------------------------
   排名  交易對                     分數      趨勢    夏普    穩定性
   1     ADA_binance_bybit         21.273    0.85    0.92    0.78
   2     ETH_binance_bybit         13.055    0.72    0.88    0.71
   ```

**支援的執行模式**：

1. **交互式模式**：
   ```bash
   python factor_strategies/run_factor_strategies.py
   ```

2. **命令行模式**：
   ```bash
   # 執行單一策略
   python factor_strategies/run_factor_strategies.py --strategy cerebrum_core
   
   # 執行所有策略
   python factor_strategies/run_factor_strategies.py --strategy all
   ```

**主要函式**：

1. **select_strategy_interactively()**
   - 顯示策略選擇界面
   - 處理用戶輸入驗證
   - 返回選擇的策略名稱

2. **run_date_range()**
   - 執行指定日期範圍的策略計算
   - 處理數據充足性檢查
   - 顯示執行進度和結果

3. **main()**
   - 主執行流程控制
   - 自動檢測數據範圍
   - 協調各個組件運行

**處理邏輯**：
1. **初始化**：創建 FactorEngine 實例，建立數據庫連接
2. **日期檢測**：自動從數據庫檢測完整可用日期範圍
3. **策略選擇**：交互式或命令行方式選擇執行策略
4. **批量執行**：對日期範圍內每一天執行策略計算
5. **進度顯示**：實時顯示執行進度和成功率
6. **結果展示**：自動查詢和顯示最新排名結果

**範例執行過程**：
```
🧠 因子策略系統 (Factor Strategy System)
============================================================

✅ 檢測到數據日期範圍: 2025-01-01 到 2025-02-28
📊 將處理 59 天的數據

✅ 已選擇策略: cerebrum_core

🚀 執行策略: cerebrum_core  
📅 日期範圍: 2025-01-01 到 2025-02-28

📅 處理日期: 2025-01-01
⚠️ 跳過: 數據量不足：策略需要 93 天數據，但只有 1 天可用

[... 處理中間日期 ...]

📅 處理日期: 2025-02-28
✅ 成功: 13 個交易對

📊 執行完成: 28/59 天成功

🎉 策略執行完成！

📊 執行結果:
策略: cerebrum_core, 日期: 2025-02-28
排名前10的交易對:
------------------------------------------------------------
1. ADA_binance_bybit     (分數: 21.273)
2. ETH_binance_bybit     (分數: 13.055)  
3. BTC_binance_bybit     (分數: 11.310)
```

**測試重點**：
- ✅ **交互界面**：驗證用戶選擇界面正確顯示和響應
- ✅ **輸入驗證**：確認無效輸入的錯誤處理
- ✅ **日期自動檢測**：驗證數據庫日期範圍檢測正確
- ✅ **批量執行**：確認多日期批量處理穩定運行
- ✅ **進度顯示**：驗證執行進度和狀態正確顯示
- ✅ **錯誤處理**：測試各種異常情況的優雅處理
- ✅ **結果展示**：確認結果查詢和格式化正確
- ✅ **命令行支援**：驗證命令行參數正確解析和執行

## 🎯 系統特色

### 技術特點
- **模組化設計**：四個核心組件職責分離，便於維護和擴展
- **配置驅動**：策略配置完全外部化，支援靈活的策略定義
- **數據庫集成**：與現有數據庫系統完全整合，無縫讀寫數據
- **多因子支援**：內建6種常用技術因子，支援自定義因子擴展
- **智能過濾**：自動過濾數據不足的交易對，確保計算準確性

### 業務特點
- **多策略選擇**：提供5種內建策略，覆蓋不同風險偏好
- **風險控制**：基於歷史數據充足性的智能風險控制
- **實用導向**：排名結果直接可用於投資決策參考
- **性能優化**：向量化計算，支援批量處理大量交易對
- **用戶友好**：簡潔的交互界面，支援命令行和交互式執行

### 應用場景
- **投資決策**：為投資者提供基於多因子分析的交易對排名
- **策略研究**：支援因子策略的回測和效果驗證
- **風險管理**：通過穩定性因子識別低風險投資標的
- **績效評估**：使用夏普比率等指標評估風險調整收益

## ⚠️ 使用注意事項

### 數據依賴
- **前置數據**：系統需要 return_metrics 表中有充足的歷史數據
- **最少要求**：不同策略有不同的最少數據天數要求（7-90天）
- **數據質量**：確保輸入數據的準確性和完整性

### 策略配置
- **權重總和**：ranking_logic 中的權重必須總和為 1.0
- **函式對應**：因子函式名必須在 factor_library.py 中存在
- **參數合理性**：確認回看窗口等參數設定合理

### 系統限制
- **計算複雜度**：多因子計算可能需要較長時間，特別是大數據量時
- **內存使用**：批量處理時注意內存使用情況
- **數據庫負載**：頻繁執行時注意數據庫連接和負載管理

## 🔮 未來擴展

### 功能擴展
- **新因子開發**：可在 factor_library.py 中添加更多技術因子
- **策略優化**：支援機器學習方法優化因子權重
- **實時更新**：支援實時數據流的動態策略更新
- **多市場支援**：擴展到其他加密貨幣市場或傳統金融市場

### 技術優化  
- **並行計算**：利用多進程加速大量交易對的因子計算
- **緩存機制**：添加計算結果緩存，提升重複查詢性能
- **API接口**：提供 REST API 供外部系統調用
- **視覺化界面**：開發 Web 界面展示策略結果和趨勢分析

---

**🎯 因子策略系統為投資決策提供科學的量化分析工具，通過多因子模型幫助識別最優投資機會！** 