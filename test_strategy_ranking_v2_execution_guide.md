# Strategy Ranking V2 系統手動測試計畫

## 📋 測試概述

本指南專門針對 `strategy_ranking_v2.py` 性能優化版本的手動測試：
- **核心特性**：批量處理、高性能、一次性讀取所有數據
- **缺少功能**：增量處理（每次都會重新處理）
- **適用場景**：大量數據的批量計算、性能要求高的場景

## 🎯 測試目標

1. **批量處理驗證** - 驗證一次性處理多天數據的正確性
2. **性能測試** - 測試大量數據的處理速度
3. **實驗性策略** - 驗證新添加的實驗性策略功能
4. **數據完整性** - 確保批量處理不會遺漏或重複數據
5. **邊界條件** - 測試各種極端情況的處理

## 📊 測試數據準備

### 步驟1：載入測試數據
```bash
# 確認在正確目錄
pwd
# 應該顯示：/Users/waynechen/Downloads/Arbitrage01-3

# 載入測試數據（使用已有的測試數據）
sqlite3 trading_data.db < test_data_strategy_ranking.sql
```

**預期結果：**
- 插入 21 條 return_metrics 測試記錄
- 涵蓋 6 個不同日期的測試場景
- 包含 14 個不同特性的交易對

### 步驟2：驗證數據載入
```bash
sqlite3 trading_data.db -cmd ".mode table" "
SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT trading_pair) as unique_pairs,
    COUNT(DISTINCT date) as unique_dates,
    MIN(date) as start_date,
    MAX(date) as end_date
FROM return_metrics 
WHERE trading_pair LIKE 'RANK_TEST%';
"
```

**預期輸出：**
```
total_records  unique_pairs  unique_dates  start_date   end_date
21            14            6             2024-05-01   2024-05-15
```

## 🧪 測試場景設計

### 場景1：單日單策略測試
**目標**：驗證基本功能正確性

```bash
# 測試單個日期的 original 策略
python3 strategy_ranking_v2.py --start_date 2024-05-01 --end_date 2024-05-01 --strategies original
```

**預期輸出：**
```
🚀 策略排行榜生成器 V2 啟動 🚀
時間範圍: 2024-05-01 到 2024-05-01
🗄️ 正在從數據庫載入收益數據...
✅ 數據庫載入成功: 6 筆記錄
📊 正在批量計算策略: original
✅ 策略 original 批量計算完成，共處理 1 天, 6 條排名記錄
✅ 數據庫插入成功: 6 條記錄
```

**驗證查詢：**
```sql
SELECT rank_position, trading_pair, ROUND(final_ranking_score, 4) as score
FROM strategy_ranking 
WHERE strategy_name = 'original' AND date = '2024-05-01' 
AND trading_pair LIKE 'RANK_TEST%'
ORDER BY rank_position;
```

**預期排名順序：**
1. RANK_TEST6 (極值型) - 最高分
2. RANK_TEST4 (波動型) - 短期表現佳
3. RANK_TEST1 (短期強勢) - 平衡表現

### 場景2：多日單策略測試
**目標**：驗證批量處理多天數據

```bash
# 測試多天數據處理
python3 strategy_ranking_v2.py --start_date 2024-05-01 --end_date 2024-05-03 --strategies original
```

**預期輸出：**
```
時間範圍: 2024-05-01 到 2024-05-03
✅ 數據庫載入成功: 16 筆記錄
✅ 策略 original 批量計算完成，共處理 3 天, 16 條排名記錄
```

**驗證查詢：**
```sql
SELECT date, COUNT(*) as pairs_count, 
       ROUND(AVG(final_ranking_score), 4) as avg_score
FROM strategy_ranking 
WHERE strategy_name = 'original' 
AND date BETWEEN '2024-05-01' AND '2024-05-03'
AND trading_pair LIKE 'RANK_TEST%'
GROUP BY date ORDER BY date;
```

### 場景3：實驗性策略測試
**目標**：驗證新添加的實驗性策略功能

```bash
# 測試實驗性策略
python3 strategy_ranking_v2.py --start_date 2024-05-01 --end_date 2024-05-01 --strategies test_simple_1d
```

**預期輸出：**
```
📊 正在批量計算策略: test_simple_1d
🎯 載入策略: 極簡測試1: 純1天ROI，無標準化
✅ 計算組件分數: simple_1d
```

### 場景4：多策略批量測試
**目標**：測試同時處理多個策略

```bash
# 測試多個策略
python3 strategy_ranking_v2.py --start_date 2024-05-01 --end_date 2024-05-01 --strategies "original,momentum_focused,test_simple_1d"
```

**驗證不同策略的排名差異：**
```sql
SELECT 
    sr1.trading_pair,
    sr1.rank_position as original_rank,
    sr2.rank_position as momentum_rank,
    sr3.rank_position as test_rank
FROM strategy_ranking sr1
LEFT JOIN strategy_ranking sr2 ON sr1.trading_pair = sr2.trading_pair 
    AND sr1.date = sr2.date AND sr2.strategy_name = 'momentum_focused'
LEFT JOIN strategy_ranking sr3 ON sr1.trading_pair = sr3.trading_pair 
    AND sr1.date = sr3.date AND sr3.strategy_name = 'test_simple_1d'
WHERE sr1.strategy_name = 'original' AND sr1.date = '2024-05-01'
AND sr1.trading_pair LIKE 'RANK_TEST%'
ORDER BY sr1.rank_position;
```

### 場景5：邊界條件測試
**目標**：測試極值、NULL值等邊界情況

```bash
# 測試包含邊界條件的日期
python3 strategy_ranking_v2.py --start_date 2024-05-03 --end_date 2024-05-03 --strategies original
```

**驗證邊界條件處理：**
```sql
SELECT trading_pair, rank_position, ROUND(final_ranking_score, 4) as score,
CASE 
    WHEN trading_pair LIKE '%extreme_pos%' THEN '極大正值'
    WHEN trading_pair LIKE '%extreme_neg%' THEN '極大負值'
    WHEN trading_pair LIKE '%zero%' THEN '零值'
    WHEN trading_pair LIKE '%null%' THEN 'NULL值'
    ELSE '正常值'
END as data_type
FROM strategy_ranking 
WHERE strategy_name = 'original' AND date = '2024-05-03'
AND trading_pair LIKE 'RANK_TEST%'
ORDER BY rank_position;
```

### 場景6：性能測試
**目標**：測試大範圍日期的處理性能

```bash
# 測試所有測試日期的性能
time python3 strategy_ranking_v2.py --start_date 2024-05-01 --end_date 2024-05-15 --strategies original
```

**預期性能指標：**
- 處理時間 < 5 秒（21條記錄）
- 顯示批量處理的優勢

### 場景7：重複處理測試
**目標**：驗證 V2 版本會重新處理已存在的數據

```bash
# 第一次執行
python3 strategy_ranking_v2.py --start_date 2024-05-01 --end_date 2024-05-01 --strategies original

# 第二次執行（應該重新處理，不會跳過）
python3 strategy_ranking_v2.py --start_date 2024-05-01 --end_date 2024-05-01 --strategies original
```

**預期行為：**
- 兩次都會完整執行（不會顯示"已處理完成"）
- 數據會被重新插入（使用 INSERT OR REPLACE）

## 🔍 驗證檢查點

### 檢查點1：基本功能驗證
```bash
# 檢查是否正確插入數據
sqlite3 trading_data.db -cmd ".mode table" "
SELECT strategy_name, COUNT(*) as total_rankings, 
       COUNT(DISTINCT date) as covered_dates
FROM strategy_ranking 
WHERE trading_pair LIKE 'RANK_TEST%'
GROUP BY strategy_name;
"
```

### 檢查點2：排名邏輯驗證
```sql
-- 檢查排名分數是否按降序排列
SELECT strategy_name, trading_pair, rank_position, final_ranking_score,
LAG(final_ranking_score) OVER (PARTITION BY strategy_name, date ORDER BY rank_position) as prev_score,
CASE 
    WHEN LAG(final_ranking_score) OVER (PARTITION BY strategy_name, date ORDER BY rank_position) >= final_ranking_score 
    THEN 'CORRECT' ELSE 'INCORRECT'
END as order_check
FROM strategy_ranking 
WHERE trading_pair LIKE 'RANK_TEST%' AND date = '2024-05-01'
ORDER BY strategy_name, rank_position;
```

### 檢查點3：數據完整性驗證
```sql
-- 檢查是否有重複的排名位置
SELECT strategy_name, date, rank_position, COUNT(*) as duplicate_count
FROM strategy_ranking 
WHERE trading_pair LIKE 'RANK_TEST%'
GROUP BY strategy_name, date, rank_position
HAVING COUNT(*) > 1;
```

### 檢查點4：實驗性策略驗證
```bash
# 測試是否能正確執行實驗性策略
python3 strategy_ranking_v2.py --start_date 2024-05-01 --end_date 2024-05-01 --strategies test_simple_avg
```

## 🐛 常見問題排查

### 問題1：找不到實驗性策略
**症狀**：提示策略不存在
**檢查**：
```bash
python3 -c "from ranking_config import EXPERIMENTAL_CONFIGS; print(list(EXPERIMENTAL_CONFIGS.keys()))"
```

### 問題2：數據重複插入
**症狀**：重複執行導致數據異常
**解決**：V2版本使用 `INSERT OR REPLACE`，應該不會有重複問題

### 問題3：性能問題
**症狀**：處理速度慢
**檢查**：
- 確認數據量大小
- 檢查是否有數據庫鎖定問題

## 📊 測試成功標準

### ✅ 功能完整性
- [ ] 所有測試場景都能成功執行
- [ ] 沒有程式崩潰或錯誤
- [ ] 實驗性策略正常工作

### ✅ 數據正確性
- [ ] 排名邏輯符合預期
- [ ] 邊界條件正確處理
- [ ] 多策略產生不同結果

### ✅ 性能表現
- [ ] 批量處理速度明顯快於逐日處理
- [ ] 大範圍日期處理穩定
- [ ] 記憶體使用合理

### ✅ 批量處理優勢
- [ ] 一次性讀取所有數據
- [ ] 批量計算和插入
- [ ] 處理時間與數據量線性相關

## 🎯 測試執行順序

1. **準備階段**：載入測試數據，驗證環境
2. **基礎測試**：單日單策略，確保基本功能
3. **批量測試**：多日多策略，驗證核心優勢
4. **邊界測試**：極值和異常情況
5. **性能測試**：大範圍數據處理
6. **完整驗證**：執行所有驗證查詢

完成所有測試後，`strategy_ranking_v2.py` 的批量處理功能應該能夠正常工作！

## 📝 測試記錄模板

```
測試日期：____
測試環境：____
測試結果：
□ 場景1：單日單策略 - 通過/失敗
□ 場景2：多日單策略 - 通過/失敗  
□ 場景3：實驗性策略 - 通過/失敗
□ 場景4：多策略批量 - 通過/失敗
□ 場景5：邊界條件 - 通過/失敗
□ 場景6：性能測試 - 通過/失敗
□ 場景7：重複處理 - 通過/失敗

性能數據：
- 21條記錄處理時間：____ 秒
- 記憶體使用峰值：____ MB

問題記錄：
- ____________
- ____________

總體評價：通過/需要修正
``` 