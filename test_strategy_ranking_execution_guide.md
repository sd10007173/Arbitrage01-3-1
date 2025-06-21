# Strategy Ranking 系統手動測試執行指南

## 📋 測試概述

本指南用於手動測試策略排名系統的三個核心組件：
- **ranking_config.py** - 策略配置管理
- **ranking_engine.py** - 排名計算引擎  
- **strategy_ranking.py** - 主程式流程

## 🎯 測試目標

1. **配置正確性** - 驗證各種策略配置能正確載入和解析
2. **計算邏輯** - 驗證排名計算的數學邏輯正確性
3. **數據處理** - 驗證邊界條件和異常數據處理
4. **多策略比較** - 驗證不同策略產生合理的排名差異
5. **增量處理** - 驗證新日期的增量計算功能

## 📊 測試數據設計

### 場景1：基本策略測試 (2024-05-01)
- **RANK_TEST1**: 短期強勢型 (1d ROI: 1.83%, 長期一般)
- **RANK_TEST2**: 長期穩健型 (all ROI: 0.37%, 短期一般)  
- **RANK_TEST3**: 平衡型 (各期表現均衡)
- **RANK_TEST4**: 波動型 (表現不穩定)
- **RANK_TEST5**: 負收益型 (部分指標為負)
- **RANK_TEST6**: 極值型 (1d ROI: 7.30%)

### 場景2：多策略比較 (2024-05-02)
- 相同交易對的第二天數據
- 用於測試不同策略的排名差異

### 場景3：邊界條件 (2024-05-03)
- **RANK_TEST7**: 包含NULL值
- **RANK_TEST8**: 全零值
- **RANK_TEST9**: 極大正值 (1d ROI: 36.50%)
- **RANK_TEST10**: 極大負值 (1d ROI: -18.25%)

### 場景4：時間序列 (2024-05-10~12)
- **RANK_TEST11**: 連續3天數據，測試排名變化

### 場景5：策略特化 (2024-05-15)
- **RANK_TEST_MOMENTUM1**: 動量策略偏愛的數據
- **RANK_TEST_STABILITY1**: 穩定策略偏愛的數據
- **RANK_TEST_BALANCED1**: 平衡策略測試數據

## 🔧 執行步驟

### 步驟1：準備測試數據
```bash
# 在數據庫中執行測試數據SQL
sqlite3 trading_data.db < test_data_strategy_ranking.sql
```

**預期結果：**
- 插入約21條return_metrics測試記錄
- 涵蓋6個不同日期的測試場景
- 包含各種邊界條件數據

### 步驟2：執行基本策略測試
```bash
# 測試original策略 - 2024-05-01
python strategy_ranking.py --date 2024-05-01 --strategy original

# 測試多個策略
python strategy_ranking.py --date 2024-05-01 --strategy momentum_focused
python strategy_ranking.py --date 2024-05-01 --strategy stability_focused
python strategy_ranking.py --date 2024-05-01 --strategy balanced
```

**預期結果：**
- 每個策略成功處理6個測試交易對
- 生成完整的排名分數和位置
- 不同策略產生不同的排名順序

### 步驟3：測試邊界條件處理
```bash
# 測試包含NULL值和極值的數據
python strategy_ranking.py --date 2024-05-03 --strategy original
```

**預期結果：**
- NULL值被正確處理（填充為0）
- 極值不會導致程式崩潰
- 排名邏輯仍然正確

### 步驟4：測試時間序列處理
```bash
# 測試連續多天的數據
python strategy_ranking.py --date 2024-05-10 --strategy original
python strategy_ranking.py --date 2024-05-11 --strategy original  
python strategy_ranking.py --date 2024-05-12 --strategy original
```

**預期結果：**
- 每天都能成功計算排名
- 同一交易對在不同日期的排名會有變化
- 增量處理不會重複計算

### 步驟5：驗證計算結果
```bash
# 執行驗證查詢
sqlite3 trading_data.db < test_strategy_ranking_verification.sql
```

## ✅ 驗證檢查點

### 檢查點1：數據載入驗證
- [ ] 測試數據正確插入return_metrics表
- [ ] 所有21條測試記錄都存在
- [ ] 日期範圍為2024-05-01到2024-05-15

### 檢查點2：基本排名驗證
- [ ] original策略成功生成6個交易對的排名
- [ ] 排名位置連續（1,2,3,4,5,6）
- [ ] 排名分數按降序排列
- [ ] 極值型交易對(RANK_TEST6)排名最高

### 檢查點3：多策略比較驗證
- [ ] momentum_focused策略偏愛短期表現好的交易對
- [ ] stability_focused策略偏愛長期穩定的交易對
- [ ] 不同策略對同一交易對的排名有差異
- [ ] 所有策略都能處理相同的數據集

### 檢查點4：邊界條件驗證
- [ ] NULL值不會導致計算錯誤
- [ ] 零值數據能正常參與排名
- [ ] 極大正值不會破壞排名邏輯
- [ ] 極大負值能正確處理

### 檢查點5：組件分數驗證
- [ ] long_term_score和short_term_score都有值
- [ ] final_ranking_score是組件分數的正確組合
- [ ] 標準化處理正常工作
- [ ] 權重組合邏輯正確

### 檢查點6：時間序列驗證
- [ ] 同一交易對在不同日期有不同排名
- [ ] 表現提升的日期排名上升
- [ ] 表現下滑的日期排名下降
- [ ] 增量處理不會重複計算已有日期

### 檢查點7：策略特化驗證
- [ ] 動量策略確實偏愛短期動量強的交易對
- [ ] 穩定策略確實偏愛長期穩定的交易對
- [ ] 平衡策略在各時間段權重合理
- [ ] 策略特性符合配置預期

### 檢查點8：數據完整性驗證
- [ ] 沒有重複的排名位置
- [ ] 所有必要欄位都有值
- [ ] 排名位置連續無跳號
- [ ] 分數分布合理

## 🐛 常見問題排查

### 問題1：程式執行失敗
**可能原因：**
- 測試數據未正確載入
- 數據庫連接問題
- 策略配置錯誤

**排查方法：**
```sql
-- 檢查測試數據
SELECT COUNT(*) FROM return_metrics WHERE trading_pair LIKE 'RANK_TEST%';

-- 檢查數據庫連接
SELECT name FROM sqlite_master WHERE type='table';
```

### 問題2：排名結果異常
**可能原因：**
- 計算邏輯錯誤
- 標準化處理問題
- 權重配置錯誤

**排查方法：**
```sql
-- 檢查排名分數分布
SELECT strategy_name, MIN(final_ranking_score), MAX(final_ranking_score), AVG(final_ranking_score)
FROM strategy_ranking WHERE trading_pair LIKE 'RANK_TEST%' GROUP BY strategy_name;
```

### 問題3：策略差異不明顯
**可能原因：**
- 測試數據設計問題
- 策略配置過於相似
- 標準化掩蓋了差異

**排查方法：**
```sql
-- 比較不同策略的排名差異
SELECT trading_pair, 
       MAX(CASE WHEN strategy_name='original' THEN rank_position END) as original_rank,
       MAX(CASE WHEN strategy_name='momentum_focused' THEN rank_position END) as momentum_rank
FROM strategy_ranking WHERE trading_pair LIKE 'RANK_TEST%' AND date='2024-05-01'
GROUP BY trading_pair;
```

## 📈 預期測試結果

### 場景1預期排名 (original策略, 2024-05-01)
1. **RANK_TEST6** (極值型) - 最高分
2. **RANK_TEST4** (波動型) - 短期表現佳
3. **RANK_TEST1** (短期強勢) - 平衡表現
4. **RANK_TEST3** (平衡型) - 中等表現  
5. **RANK_TEST2** (長期穩健) - 短期一般
6. **RANK_TEST5** (負收益型) - 最低分

### 策略差異預期
- **momentum_focused**: RANK_TEST1, RANK_TEST4排名更高
- **stability_focused**: RANK_TEST2排名更高
- **balanced**: 排名相對均衡

### 邊界條件預期
- **NULL值**: 不影響正常計算
- **零值**: 排名最低但不出錯
- **極值**: 能正常參與排名但不破壞邏輯

## 🎯 測試成功標準

1. **功能完整性**: 所有測試場景都能成功執行
2. **計算正確性**: 排名邏輯符合策略配置預期
3. **異常處理**: 邊界條件不會導致程式崩潰
4. **策略差異**: 不同策略產生合理的排名差異
5. **數據完整性**: 所有必要數據都正確保存到數據庫

完成所有檢查點後，策略排名系統測試即為成功！ 