# Strategy Ranking 系統詳細測試步驟

## 📋 測試前準備

### 確認環境
```bash
# 確認在正確目錄
pwd
# 應該顯示：/Users/waynechen/Downloads/Arbitrage01-3

# 確認數據庫存在
ls -la trading_data.db
# 應該顯示數據庫文件

# 確認程式文件存在
ls -la strategy_ranking.py ranking_config.py ranking_engine.py
```

## 🔧 步驟1：載入測試數據

### 1.1 執行測試數據SQL
```bash
sqlite3 trading_data.db < test_data_strategy_ranking.sql
```

**預期輸出：**
```
=== 策略排名測試數據統計 ===
total_records: 21
unique_pairs: 14
unique_dates: 6
earliest_date: 2024-05-01
latest_date: 2024-05-15
```

### 1.2 驗證數據載入
```bash
sqlite3 trading_data.db -cmd ".mode table" "SELECT COUNT(*) as test_records FROM return_metrics WHERE trading_pair LIKE 'RANK_TEST%';"
```

**預期輸出：**
```
test_records
21
```

## 🧪 步驟2：基本功能測試

### 2.1 測試original策略
```bash
python strategy_ranking.py --date 2024-05-01 --strategy original
```

**預期輸出：**
```
============================================================
🎯 策略排行榜生成 (數據庫版)
============================================================
🔄 模式: 增量處理 (預設，跳過已完成組合)
✅ 命令行指定策略: original
🗄️ 正在從數據庫載入收益數據...
   目標日期: 2024-05-01
✅ 數據庫載入成功: 6 筆記錄
   2024-05-01 包含 6 個交易對
🎯 載入策略: 原始策略
📊 正在計算策略: original
✅ 計算組件分數: long_term_score
✅ 計算組件分數: short_term_score
   ✅ 策略 original 計算完成，共 6 個交易對
📊 準備將 6 條策略排行記錄插入數據庫...
✅ 成功保存 6 條策略排行記錄到數據庫

📅 處理日期 2024-05-01
   🎯 策略: original
   📊 載入數據: 6 個交易對
   ✅ 計算完成: 6 個排名
   💾 保存成功: 6 條記錄

🎉 處理完成！
   成功處理 1 個策略
```

### 2.2 檢查排名結果
```bash
sqlite3 trading_data.db -cmd ".mode table" "SELECT rank_position, trading_pair, ROUND(final_ranking_score,4) as score FROM strategy_ranking WHERE date='2024-05-01' AND strategy_name='original' AND trading_pair LIKE 'RANK_TEST%' ORDER BY rank_position;"
```

**預期輸出：**
```
rank_position  trading_pair                     score
1              RANK_TEST6/USDT_okx_gate        [最高分]
2              RANK_TEST4/USDT_binance_gate    [次高分]
3              RANK_TEST1/USDT_binance_bybit   [第三高]
4              RANK_TEST3/USDT_bybit_okx       [第四高]
5              RANK_TEST2/USDT_binance_okx     [第五高]
6              RANK_TEST5/USDT_bybit_gate      [最低分]
```

## 🔄 步驟3：增量處理測試

### 3.1 重複執行相同命令
```bash
python strategy_ranking.py --date 2024-05-01 --strategy original
```

**預期輸出：**
```
🔄 增量模式：檢查已處理的(日期, 策略)組合...
📊 數據庫中找到 1 個已處理的(日期, 策略)組合
   original: 1 個日期

📊 增量分析結果:
   總組合數: 1
   已處理: 1
   待處理: 0

🎉 所有(日期, 策略)組合都已處理完成！
```

### 3.2 驗證增量邏輯
這證明增量處理功能正常，避免了重複計算。

## 🎯 步驟4：多策略測試

### 4.1 測試momentum_focused策略
```bash
python strategy_ranking.py --date 2024-05-01 --strategy momentum_focused
```

**預期輸出：**
```
🎯 載入策略: 動量導向策略
📊 正在計算策略: momentum_focused
✅ 計算組件分數: short_momentum
✅ 計算組件分數: medium_momentum
   ✅ 策略 momentum_focused 計算完成，共 6 個交易對
✅ 成功保存 6 條策略排行記錄到數據庫
```

### 4.2 測試stability_focused策略
```bash
python strategy_ranking.py --date 2024-05-01 --strategy stability_focused
```

### 4.3 測試balanced策略
```bash
python strategy_ranking.py --date 2024-05-01 --strategy balanced
```

### 4.4 比較不同策略的排名差異
```bash
sqlite3 trading_data.db -cmd ".mode table" "
SELECT 
    sr1.trading_pair,
    sr1.rank_position as original_rank,
    sr2.rank_position as momentum_rank,
    sr3.rank_position as stability_rank,
    sr4.rank_position as balanced_rank
FROM strategy_ranking sr1
LEFT JOIN strategy_ranking sr2 ON sr1.trading_pair = sr2.trading_pair 
    AND sr1.date = sr2.date AND sr2.strategy_name = 'momentum_focused'
LEFT JOIN strategy_ranking sr3 ON sr1.trading_pair = sr3.trading_pair 
    AND sr1.date = sr3.date AND sr3.strategy_name = 'stability_focused'
LEFT JOIN strategy_ranking sr4 ON sr1.trading_pair = sr4.trading_pair 
    AND sr1.date = sr4.date AND sr4.strategy_name = 'balanced'
WHERE sr1.strategy_name = 'original'
    AND sr1.date = '2024-05-01'
    AND sr1.trading_pair LIKE 'RANK_TEST%'
ORDER BY sr1.rank_position;
"
```

**預期結果：**
- RANK_TEST1 (短期強勢) 在momentum_focused策略中排名應該更高
- RANK_TEST2 (長期穩健) 在stability_focused策略中排名應該更高

## ⚠️ 步驟5：邊界條件測試

### 5.1 測試包含NULL值和極值的數據
```bash
python strategy_ranking.py --date 2024-05-03 --strategy original
```

**預期輸出：**
```
✅ 數據庫載入成功: 5 筆記錄
   2024-05-03 包含 5 個交易對
📊 正在計算策略: original
✅ 計算組件分數: long_term_score
✅ 計算組件分數: short_term_score
   ✅ 策略 original 計算完成，共 5 個交易對
```

### 5.2 檢查邊界條件處理結果
```bash
sqlite3 trading_data.db -cmd ".mode table" "
SELECT 
    trading_pair,
    rank_position,
    ROUND(final_ranking_score, 4) as final_score,
    CASE 
        WHEN trading_pair LIKE '%extreme_pos%' THEN '極大正值'
        WHEN trading_pair LIKE '%extreme_neg%' THEN '極大負值'
        WHEN trading_pair LIKE '%zero%' THEN '零值'
        WHEN trading_pair LIKE '%null%' THEN 'NULL值'
        ELSE '正常值'
    END as data_type
FROM strategy_ranking 
WHERE strategy_name = 'original'
    AND date = '2024-05-03'
    AND trading_pair LIKE 'RANK_TEST%'
ORDER BY rank_position;
"
```

**預期結果：**
- 極大正值應該排名最高
- NULL值和零值不會導致程式崩潰
- 極大負值排名最低

## 📈 步驟6：時間序列測試

### 6.1 測試連續3天的數據
```bash
python strategy_ranking.py --date 2024-05-10 --strategy original
python strategy_ranking.py --date 2024-05-11 --strategy original
python strategy_ranking.py --date 2024-05-12 --strategy original
```

### 6.2 檢查時間序列一致性
```bash
sqlite3 trading_data.db -cmd ".mode table" "
SELECT 
    date,
    rank_position,
    ROUND(final_ranking_score, 4) as final_score,
    CASE 
        WHEN date = '2024-05-10' THEN '初始狀態'
        WHEN date = '2024-05-11' THEN '表現提升'
        WHEN date = '2024-05-12' THEN '表現下滑'
    END as expected_trend
FROM strategy_ranking 
WHERE trading_pair = 'RANK_TEST11/USDT_time_series'
    AND strategy_name = 'original'
ORDER BY date;
"
```

**預期結果：**
- 2024-05-11 (表現提升) 排名應該比 2024-05-10 高
- 2024-05-12 (表現下滑) 排名應該比 2024-05-11 低

## 🚀 步驟7：批量自動測試

### 7.1 測試自動模式 (處理所有策略)
```bash
python strategy_ranking.py --date 2024-05-15 --auto
```

**預期輸出：**
```
🤖 自動模式：處理所有策略
📊 準備處理:
   日期數: 1
   策略數: 6
   總組合: 6
   日期: 2024-05-15
   策略: original, momentum_focused, stability_focused, adaptive, pure_short_term, balanced
```

### 7.2 驗證策略特化效果
```bash
sqlite3 trading_data.db -cmd ".mode table" "
SELECT 
    sr.strategy_name,
    sr.trading_pair,
    sr.rank_position,
    rm.roi_1d,
    rm.roi_all
FROM strategy_ranking sr
JOIN return_metrics rm ON sr.trading_pair = rm.trading_pair AND sr.date = rm.date
WHERE sr.date = '2024-05-15'
    AND sr.trading_pair LIKE 'RANK_TEST_%'
ORDER BY sr.strategy_name, sr.rank_position;
"
```

## ✅ 步驟8：完整驗證

### 8.1 執行快速驗證
```bash
sqlite3 trading_data.db < test_strategy_ranking_quick.sql
```

**預期輸出包含：**
```
🔍 測試數據載入檢查
total_records: 21
unique_pairs: 14
unique_dates: 6

📊 策略排名結果檢查
original: 6個排名
momentum_focused: 6個排名
stability_focused: 6個排名
...

✅ 數據完整性檢查
所有策略: PASS
```

### 8.2 執行完整驗證 (可選)
```bash
sqlite3 trading_data.db < test_strategy_ranking_verification.sql
```

## 🎯 步驟9：性能測試

### 9.1 測試大範圍日期處理
```bash
python strategy_ranking.py --start_date 2024-05-01 --end_date 2024-05-03 --strategy original
```

### 9.2 測試強制重新處理
```bash
python strategy_ranking.py --date 2024-05-01 --strategy original --no-incremental
```

**預期輸出：**
```
🔄 模式: 完整重新處理 (增量模式已停用)
```

## 📊 測試結果判定標準

### ✅ 成功標準
1. **功能完整性**
   - [ ] 所有命令都能成功執行
   - [ ] 沒有程式崩潰或錯誤

2. **計算正確性**
   - [ ] RANK_TEST6 (極值型) 在original策略中排名第1
   - [ ] 不同策略產生不同的排名順序
   - [ ] 邊界條件不會導致錯誤

3. **增量處理**
   - [ ] 重複執行相同命令顯示"已處理完成"
   - [ ] 新增日期/策略能正確處理

4. **數據完整性**
   - [ ] 排名位置連續 (1,2,3,4,5,6)
   - [ ] 分數按降序排列
   - [ ] 所有必要欄位都有值

### ❌ 失敗標準
- 程式執行出錯
- 排名邏輯不正確
- 增量處理失效
- 數據完整性問題

## 🐛 常見問題排查

### 問題1：程式執行失敗
```bash
# 檢查測試數據
sqlite3 trading_data.db "SELECT COUNT(*) FROM return_metrics WHERE trading_pair LIKE 'RANK_TEST%';"

# 檢查Python環境
python -c "import pandas, numpy; print('環境正常')"
```

### 問題2：排名結果異常
```bash
# 檢查數據質量
sqlite3 trading_data.db "SELECT * FROM return_metrics WHERE trading_pair LIKE 'RANK_TEST%' AND date='2024-05-01';"

# 檢查策略配置
python -c "from ranking_config import RANKING_STRATEGIES; print(list(RANKING_STRATEGIES.keys()))"
```

### 問題3：增量處理不工作
```bash
# 檢查已處理記錄
sqlite3 trading_data.db "SELECT strategy_name, date, COUNT(*) FROM strategy_ranking WHERE trading_pair LIKE 'RANK_TEST%' GROUP BY strategy_name, date;"
```

## 🎉 完成測試

完成所有步驟後，你應該能夠：
- 驗證策略排名系統的完整功能
- 確認不同策略產生合理的排名差異
- 驗證增量處理避免重複計算
- 確保邊界條件處理正確

**恭喜！策略排名系統測試完成！** 🎊 