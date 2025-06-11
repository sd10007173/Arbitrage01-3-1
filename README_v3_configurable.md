# 可配置排行榜分析系統 v3.0

## 概述

這是原始 `profit_analysis_rolling_v2_(+1).py` 的升級版本，將原本硬編碼的排行榜計算邏輯拆分成三個模組化組件，讓你可以輕鬆實驗不同的排行榜策略。

## 系統架構

### 📋 1. `ranking_config.py` - 配方書
- **作用**: 儲存所有排行榜策略的配置
- **內容**: 定義指標組合、權重、標準化方式等
- **修改**: 要測試新策略時，只需在這裡添加新配置

### 👨‍🍳 2. `ranking_engine.py` - 計算引擎  
- **作用**: 根據配置執行實際的排行榜計算
- **功能**: 標準化、加權計算、排序等核心邏輯
- **特色**: 支援波動率懲罰、組件化計算

### 🏪 3. `profit_analysis_rolling_v3_configurable.py` - 主程式
- **作用**: 用戶交互、檔案處理、流程協調
- **模式**: 分析模式、比較模式、測試模式
- **輸出**: 包含策略名稱的 summary 檔案

## 快速開始

### 1. 安裝依賴
```bash
pip install pandas numpy matplotlib requests
```

### 2. 執行主程式
```bash
python profit_analysis_rolling_v3_configurable.py
```

### 3. 選擇模式
```
🚀 可配置排行榜分析工具 v3.0
============================================================
選擇模式:
1. 📊 分析模式 - 產生新的summary檔案
2. 🔍 比較模式 - 比較現有summary的不同策略結果  
3. 🧪 測試模式 - 交互式策略測試
4. 📋 查看可用策略
```

## 內建策略

### 主要策略

1. **original** - 原始策略
   - 複製原有的計算邏輯
   - 長期評分 + 短期評分各佔 50%

2. **momentum_focused** - 動量導向策略
   - 重視短期表現
   - 短期動量 70% + 中期動量 30%

3. **stability_focused** - 穩定性導向策略
   - 重視長期穩定性
   - 穩定性 60% + 近期表現 40%

4. **pure_short_term** - 純短期策略
   - 極度重視 1-2 天表現
   - 1天表現 80% + 2天表現 20%

5. **balanced** - 平衡策略
   - 短中長期平衡配置
   - 短期 50% + 中期 30% + 長期 20%

6. **adaptive** - 自適應策略
   - 包含波動率懲罰機制
   - 根據波動率動態調整分數

### 實驗策略

- **test_1** - 純短期測試
- **test_2** - 長短期平衡測試  
- **test_3** - 反向權重測試
- **test_4** - 極端動量測試

## 使用範例

### 1. 分析模式
```bash
# 執行程式後選擇模式 1
# 輸入日期範圍: 2025-01-01 到 2025-01-31
# 選擇策略: momentum_focused
# 自動生成: summary_2025-01-31_strategy_momentum_focused_create_2025-06-XX(1).csv
```

### 2. 比較模式
```bash
# 執行程式後選擇模式 2
# 選擇現有 summary 檔案
# 自動比較: original, momentum_focused, stability_focused, pure_short_term
# 顯示各策略的前10名差異
```

### 3. 測試模式
```bash
# 執行程式後選擇模式 3
# 可以創建測試數據或載入現有數據
# 交互式測試各種策略組合
# 支援策略重疊分析
```

## 自定義策略

### 在 `ranking_config.py` 中添加新策略：

```python
EXPERIMENTAL_CONFIGS['my_strategy'] = {
    'name': '我的策略',
    'components': {
        'my_component': {
            'indicators': ['1d_ROI', '7d_ROI'],  # 使用的指標
            'weights': [0.8, 0.2],              # 權重
            'normalize': True                   # 是否標準化
        }
    },
    'final_combination': {
        'scores': ['my_component'],
        'weights': [1.0]
    }
}
```

### 可用指標：
- `1d_ROI`, `2d_ROI`, `7d_ROI`, `14d_ROI`, `30d_ROI`, `all_ROI` (年化報酬率)
- `1d_return`, `2d_return`, `7d_return`, `14d_return`, `30d_return`, `all_return` (累積報酬)

## 進階功能

### 1. 波動率懲罰
```python
'volatility_penalty': True  # 在組件配置中啟用
```

### 2. 策略比較
```python
from ranking_engine import compare_strategies

# 比較多個策略
strategies = ['original', 'momentum_focused', 'stability_focused']
compare_strategies(df, strategies, top_n=10)
```

### 3. 重疊分析
```python
from ranking_engine import strategy_overlap_analysis

# 分析策略重疊度
strategy_overlap_analysis(df, strategies, top_n=10)
```

### 4. 調試模式
```python
from ranking_engine import debug_strategy_calculation

# 調試特定策略的計算過程
debug_strategy_calculation(df, 'momentum_focused', 'BTCUSDT_binance_bybit')
```

## 輸出檔案格式

生成的 summary 檔案會包含策略名稱：
```
summary_2025-01-31_strategy_momentum_focused_create_2025-06-XX(1).csv
```

新增的欄位：
- `final_ranking_score`: 最終排行榜分數
- `{component_name}_score`: 各組件分數
- 保留原有欄位以保持向後兼容

## 效能比較

### 原系統 vs 新系統

| 特性 | 原系統 | 新系統 |
|------|--------|--------|
| 策略數量 | 1個固定 | 6個主要 + 4個實驗 |
| 修改難度 | 需改程式碼 | 只需改配置 |
| 測試效率 | 需重新運行 | 交互式測試 |
| 擴展性 | 困難 | 容易 |
| 除錯能力 | 有限 | 完整除錯工具 |

## 故障排除

### 常見問題

1. **ModuleNotFoundError**: 
   ```bash
   pip install pandas numpy matplotlib requests
   ```

2. **找不到 FR_diff 檔案**:
   - 確保 `csv/FR_diff/` 目錄存在
   - 檢查檔案命名格式: `*_FR_diff.csv`

3. **策略計算失敗**:
   - 檢查數據是否包含必要的 ROI 欄位
   - 使用調試模式查看詳細錯誤

4. **無法比較策略**:
   - 確保 summary 檔案存在於 `csv/FF_profit/summary/`
   - 檢查檔案格式是否正確

## 更新日誌

### v3.0 (2025-06-XX)
- ✅ 模組化設計：配置、引擎、主程式分離
- ✅ 6個主要策略 + 4個實驗策略
- ✅ 交互式測試模式
- ✅ 策略比較和重疊分析
- ✅ 完整的調試工具
- ✅ 向後兼容原有功能

### v2.0 (原版)
- 基礎的滾動分析功能
- 固定的Z-score計算邏輯

## 技術支援

如有問題或建議，請：
1. 檢查本文件的故障排除章節
2. 使用 `python ranking_config.py` 測試配置系統
3. 使用測試模式進行交互式調試 