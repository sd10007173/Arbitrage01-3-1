# 測試工具更新說明

## 更新內容

已更新 `tests/` 資料夾中的工具以適應新的 CSV 資料夾結構：

### 1. remove.py 更新
- **新的檔案分類**：
  - `funding_rates`: FR_diff, FR_history 相關檔案
  - `return_analysis`: FR_return_list 相關檔案  
  - `strategy_ranking`: strategy_ranking 相關檔案
  - `backtest`: 回測相關檔案
  - `other`: 其他檔案

- **改進的識別邏輯**：
  - 基於檔案名稱和父資料夾名稱進行分類
  - 更準確地識別不同類型的 CSV 檔案

### 2. smart_backup.py 更新
- **新的資料夾結構**：
  ```
  csv/
  ├── FR_diff/
  ├── FR_history/
  ├── FR_return_list/          # 新增
  ├── strategy_ranking/        # 新增
  ├── FF_profit/
  ├── FF_profit/summary/
  ├── FF_profit/archived/
  ├── Trading pair/
  ├── Backtest/
  ├── Return/
  └── Example/
  ```

## 使用方法

### 檢查 CSV 檔案統計
```bash
python tests/remove.py --stats
```

### 清理 CSV 檔案（互動模式）
```bash
python tests/remove.py
```

### 強制清理（無確認）
```bash
python tests/remove.py --force
```

### 列出現有備份
```bash
python tests/smart_backup.py list
```

### 創建備份
```bash
python tests/smart_backup.py
```

## 測試結果

✅ remove.py 正確識別所有檔案類型：
- Funding Rates: 21 檔案
- Return Analysis: 2 檔案  
- Strategy Ranking: 12 檔案
- Backtest: 24 檔案
- Other: 2 檔案

✅ smart_backup.py 正確支援新的資料夾結構，包括：
- FR_return_list
- strategy_ranking

兩個工具都已完全適應新的 CSV 結構並正常運作。 