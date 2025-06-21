# calculate_FR_return_list_v2 手動測試指南

## 📋 測試概述

本測試計劃旨在全面驗證 `calculate_FR_return_list_v2.py` 程式的功能，包括：
- 基本收益計算功能
- 多交易對並行處理
- 空值處理邏輯
- 滑動窗口計算
- 增量處理功能
- 邊界條件處理

## 🎯 測試場景設計

### 場景1：基本功能測試
- **交易對**：TEST1/USDT (binance vs bybit)
- **時間範圍**：2024-01-01 到 2024-01-07 (7天)
- **數據特點**：包含正收益、負收益、零收益和大幅收益日
- **驗證重點**：1d、2d、7d收益計算邏輯

### 場景2：多交易對測試
- **交易對**：TEST2/USDT (高收益) 和 TEST3/USDT (低收益)
- **時間範圍**：2024-01-01 到 2024-01-02
- **驗證重點**：多交易對並行處理，互不影響

### 場景3：空值處理測試
- **交易對**：TEST4/USDT
- **數據特點**：包含NULL值的diff_ab記錄
- **驗證重點**：NULL值不影響其他正常計算

### 場景4：滑動窗口測試
- **交易對**：TEST5/USDT
- **時間範圍**：2024-02-01 到 2024-03-01 (30天)
- **數據模式**：遞增→穩定→遞減
- **驗證重點**：7d、14d、30d滑動窗口計算

### 場景5：增量處理測試
- **交易對**：TEST6/USDT
- **分階段**：先處理3天，再增加4天
- **驗證重點**：增量處理不重複計算已有日期

### 場景6：邊界條件測試
- **TEST7/USDT**：單天數據
- **TEST8/USDT**：極值數據 (±0.015)
- **驗證重點**：邊界情況的穩定性

## 📝 測試執行步驟

### 準備階段

1. **確認程式可用性**
   ```bash
   # 檢查程式文件
   ls -la calculate_FR_return_list_v2.py
   
   # 檢查數據庫連接
   python3 -c "from database_operations import DatabaseManager; db = DatabaseManager(); print('數據庫連接正常')"
   ```

2. **清理現有測試數據**
   ```sql
   -- 在數據庫中執行
   DELETE FROM funding_rate_diff WHERE symbol LIKE 'TEST%';
   DELETE FROM return_metrics WHERE trading_pair LIKE 'TEST%';
   ```

### 第一階段：基礎測試

3. **插入測試數據**
   ```bash
   # 執行測試數據SQL
   sqlite3 trading_data.db < test_data_calculate_FR_return_list_v2.sql
   ```

4. **驗證測試數據**
   ```sql
   -- 檢查數據插入情況
   SELECT symbol, COUNT(*) as count FROM funding_rate_diff WHERE symbol LIKE 'TEST%' GROUP BY symbol;
   ```

5. **執行程式測試**

   **測試1：基本功能 (場景1)**
   ```bash
   python3 calculate_FR_return_list_v2.py --start-date 2024-01-01 --end-date 2024-01-07 --symbol TEST1/USDT
   ```

   **測試2：多交易對 (場景2)**
   ```bash
   python3 calculate_FR_return_list_v2.py --start-date 2024-01-01 --end-date 2024-01-02
   ```

   **測試3：滑動窗口 (場景4)**
   ```bash
   python3 calculate_FR_return_list_v2.py --start-date 2024-02-01 --end-date 2024-03-01 --symbol TEST5/USDT
   ```

   **測試4：邊界條件 (場景6)**
   ```bash
   python3 calculate_FR_return_list_v2.py --start-date 2024-04-01 --end-date 2024-04-01
   ```

6. **驗證基礎測試結果**
   ```bash
   sqlite3 trading_data.db < test_verification_queries.sql
   ```

### 第二階段：增量處理測試

7. **執行第一階段增量測試**
   ```bash
   python3 calculate_FR_return_list_v2.py --start-date 2024-03-01 --end-date 2024-03-03 --symbol TEST6/USDT
   ```

8. **添加第二階段數據**
   ```bash
   sqlite3 trading_data.db < test_data_incremental_stage2.sql
   ```

9. **執行第二階段增量測試**
   ```bash
   python3 calculate_FR_return_list_v2.py --start-date 2024-03-01 --end-date 2024-03-07 --symbol TEST6/USDT
   ```

10. **驗證增量處理結果**
    ```sql
    -- 檢查TEST6/USDT的完整結果
    SELECT * FROM return_metrics WHERE trading_pair = 'TEST6/USDT_binance_bybit' ORDER BY date;
    ```

### 第三階段：高級功能測試

11. **測試自動檢測功能**
    ```bash
    python3 calculate_FR_return_list_v2.py --process-latest
    ```

12. **測試舊版兼容性**
    ```bash
    python3 calculate_FR_return_list_v2.py --start-date 2024-01-01 --end-date 2024-01-02 --use-legacy
    ```

## ✅ 預期結果驗證

### 場景1預期結果 (TEST1/USDT)
```
日期        | return_1d | return_2d | return_7d | return_all
2024-01-01 | 0.0002    | 0.0002    | 0.0002    | 0.0002
2024-01-02 | -0.0005   | -0.0003   | -0.0003   | -0.0003
2024-01-03 | 0.0000    | -0.0005   | -0.0003   | -0.0003
2024-01-04 | 0.0025    | 0.0025    | 0.0022    | 0.0022
2024-01-05 | 0.0004    | 0.0029    | 0.0026    | 0.0026
2024-01-06 | 0.0005    | 0.0009    | 0.0031    | 0.0031
2024-01-07 | 0.0006    | 0.0011    | 0.0037    | 0.0037
```

### 場景4關鍵檢查點 (TEST5/USDT)
```
日期        | return_7d | 說明
2024-02-07 | 0.0028    | 前7天總和: 0.0001+0.0002+...+0.0007
2024-02-14 | 0.0035    | 穩定期7天: 0.0005×7
2024-02-21 | 0.0045    | 混合期: 0.0005×6 + 0.0010×1
```

## 🚨 常見問題處理

### 問題1：程式執行失敗
```bash
# 檢查Python環境
python3 --version
pip3 list | grep pandas

# 檢查數據庫文件
ls -la *.db
```

### 問題2：數據不匹配
```sql
-- 檢查原始數據
SELECT * FROM funding_rate_diff WHERE symbol LIKE 'TEST%' ORDER BY timestamp_utc;

-- 檢查計算結果
SELECT * FROM return_metrics WHERE trading_pair LIKE 'TEST%' ORDER BY trading_pair, date;
```

### 問題3：增量處理異常
```sql
-- 清理特定測試數據重新測試
DELETE FROM return_metrics WHERE trading_pair = 'TEST6/USDT_binance_bybit';
```

## 📊 測試完成檢查清單

- [ ] 基本功能測試通過 (場景1)
- [ ] 多交易對測試通過 (場景2)
- [ ] 空值處理測試通過 (場景3)
- [ ] 滑動窗口測試通過 (場景4)
- [ ] 增量處理測試通過 (場景5)
- [ ] 邊界條件測試通過 (場景6)
- [ ] ROI計算驗證通過
- [ ] 數據完整性檢查通過
- [ ] 性能表現符合預期
- [ ] 錯誤處理機制正常

## 🧹 測試清理

測試完成後，清理測試數據：
```sql
DELETE FROM funding_rate_diff WHERE symbol LIKE 'TEST%';
DELETE FROM return_metrics WHERE trading_pair LIKE 'TEST%';
VACUUM;
```

## 📝 測試報告模板

```
=== calculate_FR_return_list_v2 測試報告 ===
測試日期: [填入日期]
測試人員: [填入姓名]

場景1 - 基本功能: [✅/❌] [備註]
場景2 - 多交易對: [✅/❌] [備註]
場景3 - 空值處理: [✅/❌] [備註]
場景4 - 滑動窗口: [✅/❌] [備註]
場景5 - 增量處理: [✅/❌] [備註]
場景6 - 邊界條件: [✅/❌] [備註]

總體評價: [通過/需要修復]
發現問題: [列出問題]
建議改進: [列出建議]
``` 