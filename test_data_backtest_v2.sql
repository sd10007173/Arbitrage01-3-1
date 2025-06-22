-- ================================================================
-- Backtest_v2.py 系統測試數據生成 SQL
-- 用途：為回測系統提供測試數據
-- 測試 backtest_v2.py 的進場、離場、PnL計算、持倉管理等核心邏輯
-- ================================================================

-- 清理現有測試數據
DELETE FROM return_metrics WHERE trading_pair LIKE 'BT_TEST%';
DELETE FROM strategy_ranking WHERE trading_pair LIKE 'BT_TEST%';

-- ================================================================
-- 場景1：基本進場測試 - 2024-06-01
-- ================================================================
-- 測試前3名進場邏輯，最大持倉3個

-- Return_metrics 數據（用於計算資金費率收益）
INSERT INTO return_metrics (trading_pair, date, return_1d, roi_1d, return_2d, roi_2d, return_7d, roi_7d, return_14d, roi_14d, return_30d, roi_30d, return_all, roi_all) VALUES
-- 高收益組
('BT_TEST1/USDT_binance_bybit', '2024-06-01', 0.0050, 1.8250, 0.0080, 1.4600, 0.0120, 0.6260, 0.0150, 0.3912, 0.0200, 0.2433, 0.0250, 0.1826),
('BT_TEST2/USDT_binance_okx', '2024-06-01', 0.0040, 1.4600, 0.0070, 1.2775, 0.0110, 0.5737, 0.0140, 0.3651, 0.0180, 0.2196, 0.0220, 0.1606),
('BT_TEST3/USDT_bybit_okx', '2024-06-01', 0.0030, 1.0950, 0.0055, 1.0038, 0.0105, 0.5476, 0.0180, 0.4695, 0.0280, 0.3416, 0.0380, 0.2774),
-- 中等收益組
('BT_TEST4/USDT_binance_gate', '2024-06-01', 0.0020, 0.7300, 0.0035, 0.6388, 0.0070, 0.3651, 0.0100, 0.2608, 0.0150, 0.1830, 0.0200, 0.1460),
('BT_TEST5/USDT_bybit_gate', '2024-06-01', 0.0015, 0.5475, 0.0025, 0.4563, 0.0050, 0.2608, 0.0080, 0.2086, 0.0120, 0.1464, 0.0160, 0.1168);

-- 策略排名數據（original策略）
INSERT INTO strategy_ranking (strategy_name, trading_pair, date, final_ranking_score, rank_position, long_term_score, short_term_score, combined_roi_z_score, final_combination_value) VALUES
('original', 'BT_TEST1/USDT_binance_bybit', '2024-06-01', 1.8250, 1, 1.8250, 1.8250, 1.8250, 'long_term_score(1.8250)*0.500 + short_term_score(1.8250)*0.500 = 1.8250'),
('original', 'BT_TEST2/USDT_binance_okx', '2024-06-01', 1.4600, 2, 1.4600, 1.4600, 1.4600, 'long_term_score(1.4600)*0.500 + short_term_score(1.4600)*0.500 = 1.4600'),
('original', 'BT_TEST3/USDT_bybit_okx', '2024-06-01', 1.0950, 3, 1.0950, 1.0950, 1.0950, 'long_term_score(1.0950)*0.500 + short_term_score(1.0950)*0.500 = 1.0950'),
('original', 'BT_TEST4/USDT_binance_gate', '2024-06-01', 0.7300, 4, 0.7300, 0.7300, 0.7300, 'long_term_score(0.7300)*0.500 + short_term_score(0.7300)*0.500 = 0.7300'),
('original', 'BT_TEST5/USDT_bybit_gate', '2024-06-01', 0.5475, 5, 0.5475, 0.5475, 0.5475, 'long_term_score(0.5475)*0.500 + short_term_score(0.5475)*0.500 = 0.5475');

-- ================================================================
-- 場景2：排名變化測試 - 2024-06-02
-- ================================================================
-- 測試排名變化導致的進場離場邏輯

-- Return_metrics 數據（第二天）
INSERT INTO return_metrics (trading_pair, date, return_1d, roi_1d, return_2d, roi_2d, return_7d, roi_7d, return_14d, roi_14d, return_30d, roi_30d, return_all, roi_all) VALUES
-- BT_TEST1 表現下滑，BT_TEST4 表現提升
('BT_TEST1/USDT_binance_bybit', '2024-06-02', 0.0010, 0.3650, 0.0060, 1.0950, 0.0130, 0.6771, 0.0160, 0.4173, 0.0210, 0.2562, 0.0260, 0.1898),
('BT_TEST2/USDT_binance_okx', '2024-06-02', 0.0035, 1.2775, 0.0075, 1.3688, 0.0115, 0.5998, 0.0145, 0.3782, 0.0185, 0.2257, 0.0225, 0.1643),
('BT_TEST3/USDT_bybit_okx', '2024-06-02', 0.0025, 0.9125, 0.0055, 1.0038, 0.0110, 0.5737, 0.0185, 0.4825, 0.0285, 0.3477, 0.0385, 0.2811),
-- BT_TEST4 大幅提升到第1名
('BT_TEST4/USDT_binance_gate', '2024-06-02', 0.0060, 2.1900, 0.0080, 1.4600, 0.0130, 0.6771, 0.0160, 0.4173, 0.0200, 0.2440, 0.0240, 0.1752),
('BT_TEST5/USDT_bybit_gate', '2024-06-02', 0.0020, 0.7300, 0.0030, 0.5475, 0.0055, 0.2869, 0.0085, 0.2217, 0.0125, 0.1525, 0.0165, 0.1205);

-- 策略排名數據（第二天）- 排名發生變化
INSERT INTO strategy_ranking (strategy_name, trading_pair, date, final_ranking_score, rank_position, long_term_score, short_term_score, combined_roi_z_score, final_combination_value) VALUES
('original', 'BT_TEST4/USDT_binance_gate', '2024-06-02', 2.1900, 1, 2.1900, 2.1900, 2.1900, 'long_term_score(2.1900)*0.500 + short_term_score(2.1900)*0.500 = 2.1900'),
('original', 'BT_TEST2/USDT_binance_okx', '2024-06-02', 1.2775, 2, 1.2775, 1.2775, 1.2775, 'long_term_score(1.2775)*0.500 + short_term_score(1.2775)*0.500 = 1.2775'),
('original', 'BT_TEST3/USDT_bybit_okx', '2024-06-02', 0.9125, 3, 0.9125, 0.9125, 0.9125, 'long_term_score(0.9125)*0.500 + short_term_score(0.9125)*0.500 = 0.9125'),
('original', 'BT_TEST5/USDT_bybit_gate', '2024-06-02', 0.7300, 4, 0.7300, 0.7300, 0.7300, 'long_term_score(0.7300)*0.500 + short_term_score(0.7300)*0.500 = 0.7300'),
('original', 'BT_TEST1/USDT_binance_bybit', '2024-06-02', 0.3650, 5, 0.3650, 0.3650, 0.3650, 'long_term_score(0.3650)*0.500 + short_term_score(0.3650)*0.500 = 0.3650');

-- ================================================================
-- 場景3：負收益測試 - 2024-06-03
-- ================================================================
-- 測試負收益情況下的PnL計算

-- Return_metrics 數據（第三天）- 包含負收益
INSERT INTO return_metrics (trading_pair, date, return_1d, roi_1d, return_2d, roi_2d, return_7d, roi_7d, return_14d, roi_14d, return_30d, roi_30d, return_all, roi_all) VALUES
-- 部分交易對出現負收益
('BT_TEST1/USDT_binance_bybit', '2024-06-03', -0.0020, -0.7300, -0.0010, -0.1825, 0.0120, 0.6260, 0.0150, 0.3912, 0.0200, 0.2433, 0.0250, 0.1826),
('BT_TEST2/USDT_binance_okx', '2024-06-03', -0.0015, -0.5475, 0.0020, 0.3650, 0.0100, 0.5217, 0.0135, 0.3521, 0.0175, 0.2135, 0.0215, 0.1569),
('BT_TEST3/USDT_bybit_okx', '2024-06-03', 0.0020, 0.7300, 0.0045, 0.8213, 0.0100, 0.5217, 0.0175, 0.4565, 0.0275, 0.3355, 0.0375, 0.2738),
('BT_TEST4/USDT_binance_gate', '2024-06-03', 0.0050, 1.8250, 0.0110, 2.0075, 0.0180, 0.9390, 0.0210, 0.5478, 0.0250, 0.3050, 0.0290, 0.2117),
('BT_TEST5/USDT_bybit_gate', '2024-06-03', 0.0030, 1.0950, 0.0050, 0.9125, 0.0080, 0.4172, 0.0110, 0.2869, 0.0150, 0.1830, 0.0190, 0.1387);

-- 策略排名數據（第三天）
INSERT INTO strategy_ranking (strategy_name, trading_pair, date, final_ranking_score, rank_position, long_term_score, short_term_score, combined_roi_z_score, final_combination_value) VALUES
('original', 'BT_TEST4/USDT_binance_gate', '2024-06-03', 1.8250, 1, 1.8250, 1.8250, 1.8250, 'long_term_score(1.8250)*0.500 + short_term_score(1.8250)*0.500 = 1.8250'),
('original', 'BT_TEST5/USDT_bybit_gate', '2024-06-03', 1.0950, 2, 1.0950, 1.0950, 1.0950, 'long_term_score(1.0950)*0.500 + short_term_score(1.0950)*0.500 = 1.0950'),
('original', 'BT_TEST3/USDT_bybit_okx', '2024-06-03', 0.7300, 3, 0.7300, 0.7300, 0.7300, 'long_term_score(0.7300)*0.500 + short_term_score(0.7300)*0.500 = 0.7300'),
('original', 'BT_TEST2/USDT_binance_okx', '2024-06-03', -0.5475, 4, -0.5475, -0.5475, -0.5475, 'long_term_score(-0.5475)*0.500 + short_term_score(-0.5475)*0.500 = -0.5475'),
('original', 'BT_TEST1/USDT_binance_bybit', '2024-06-03', -0.7300, 5, -0.7300, -0.7300, -0.7300, 'long_term_score(-0.7300)*0.500 + short_term_score(-0.7300)*0.500 = -0.7300');

-- ================================================================
-- 場景4：持倉限制測試 - 2024-06-04
-- ================================================================
-- 測試最大持倉數限制邏輯（最大3個持倉）

-- Return_metrics 數據（第四天）
INSERT INTO return_metrics (trading_pair, date, return_1d, roi_1d, return_2d, roi_2d, return_7d, roi_7d, return_14d, roi_14d, return_30d, roi_30d, return_all, roi_all) VALUES
-- 新增更多高收益交易對，測試持倉限制
('BT_TEST1/USDT_binance_bybit', '2024-06-04', 0.0080, 2.9200, 0.0100, 1.8250, 0.0140, 0.7295, 0.0170, 0.4434, 0.0210, 0.2562, 0.0250, 0.1826),
('BT_TEST2/USDT_binance_okx', '2024-06-04', 0.0070, 2.5550, 0.0095, 1.7338, 0.0125, 0.6510, 0.0155, 0.4043, 0.0195, 0.2379, 0.0235, 0.1716),
('BT_TEST3/USDT_bybit_okx', '2024-06-04', 0.0060, 2.1900, 0.0085, 1.5513, 0.0120, 0.6260, 0.0195, 0.5087, 0.0295, 0.3599, 0.0395, 0.2883),
('BT_TEST4/USDT_binance_gate', '2024-06-04', 0.0050, 1.8250, 0.0080, 1.4600, 0.0130, 0.6771, 0.0170, 0.4434, 0.0220, 0.2684, 0.0270, 0.1971),
('BT_TEST5/USDT_bybit_gate', '2024-06-04', 0.0040, 1.4600, 0.0065, 1.1863, 0.0105, 0.5476, 0.0145, 0.3782, 0.0185, 0.2257, 0.0225, 0.1643),
-- 新增第6個交易對，測試是否會超過最大持倉限制
('BT_TEST6/USDT_okx_gate', '2024-06-04', 0.0090, 3.2850, 0.0120, 2.1900, 0.0160, 0.8346, 0.0190, 0.4956, 0.0230, 0.2806, 0.0270, 0.1971);

-- 策略排名數據（第四天）- 6個交易對都有很高排名
INSERT INTO strategy_ranking (strategy_name, trading_pair, date, final_ranking_score, rank_position, long_term_score, short_term_score, combined_roi_z_score, final_combination_value) VALUES
('original', 'BT_TEST6/USDT_okx_gate', '2024-06-04', 3.2850, 1, 3.2850, 3.2850, 3.2850, 'long_term_score(3.2850)*0.500 + short_term_score(3.2850)*0.500 = 3.2850'),
('original', 'BT_TEST1/USDT_binance_bybit', '2024-06-04', 2.9200, 2, 2.9200, 2.9200, 2.9200, 'long_term_score(2.9200)*0.500 + short_term_score(2.9200)*0.500 = 2.9200'),
('original', 'BT_TEST2/USDT_binance_okx', '2024-06-04', 2.5550, 3, 2.5550, 2.5550, 2.5550, 'long_term_score(2.5550)*0.500 + short_term_score(2.5550)*0.500 = 2.5550'),
('original', 'BT_TEST3/USDT_bybit_okx', '2024-06-04', 2.1900, 4, 2.1900, 2.1900, 2.1900, 'long_term_score(2.1900)*0.500 + short_term_score(2.1900)*0.500 = 2.1900'),
('original', 'BT_TEST4/USDT_binance_gate', '2024-06-04', 1.8250, 5, 1.8250, 1.8250, 1.8250, 'long_term_score(1.8250)*0.500 + short_term_score(1.8250)*0.500 = 1.8250'),
('original', 'BT_TEST5/USDT_bybit_gate', '2024-06-04', 1.4600, 6, 1.4600, 1.4600, 1.4600, 'long_term_score(1.4600)*0.500 + short_term_score(1.4600)*0.500 = 1.4600');

-- ================================================================
-- 場景5：完整週期測試 - 2024-06-05
-- ================================================================
-- 測試完整的進場、持有、離場週期

-- Return_metrics 數據（第五天）
INSERT INTO return_metrics (trading_pair, date, return_1d, roi_1d, return_2d, roi_2d, return_7d, roi_7d, return_14d, roi_14d, return_30d, roi_30d, return_all, roi_all) VALUES
-- 大部分交易對表現下滑，測試離場邏輯
('BT_TEST1/USDT_binance_bybit', '2024-06-05', 0.0005, 0.1825, 0.0085, 1.5513, 0.0145, 0.7556, 0.0175, 0.4565, 0.0215, 0.2623, 0.0255, 0.1863),
('BT_TEST2/USDT_binance_okx', '2024-06-05', 0.0010, 0.3650, 0.0080, 1.4600, 0.0130, 0.6771, 0.0160, 0.4173, 0.0200, 0.2440, 0.0240, 0.1752),
('BT_TEST3/USDT_bybit_okx', '2024-06-05', 0.0015, 0.5475, 0.0075, 1.3688, 0.0135, 0.7034, 0.0200, 0.5217, 0.0300, 0.3660, 0.0400, 0.2920),
('BT_TEST4/USDT_binance_gate', '2024-06-05', 0.0008, 0.2920, 0.0058, 1.0585, 0.0138, 0.7191, 0.0178, 0.4643, 0.0228, 0.2782, 0.0278, 0.2030),
('BT_TEST5/USDT_bybit_gate', '2024-06-05', 0.0012, 0.4380, 0.0052, 0.9490, 0.0112, 0.5842, 0.0152, 0.3965, 0.0192, 0.2318, 0.0232, 0.1694),
('BT_TEST6/USDT_okx_gate', '2024-06-05', 0.0006, 0.2190, 0.0096, 1.7520, 0.0166, 0.8651, 0.0196, 0.5113, 0.0236, 0.2879, 0.0276, 0.2015);

-- 策略排名數據（第五天）- 排名大幅下滑，觸發離場條件
INSERT INTO strategy_ranking (strategy_name, trading_pair, date, final_ranking_score, rank_position, long_term_score, short_term_score, combined_roi_z_score, final_combination_value) VALUES
('original', 'BT_TEST3/USDT_bybit_okx', '2024-06-05', 0.5475, 1, 0.5475, 0.5475, 0.5475, 'long_term_score(0.5475)*0.500 + short_term_score(0.5475)*0.500 = 0.5475'),
('original', 'BT_TEST5/USDT_bybit_gate', '2024-06-05', 0.4380, 2, 0.4380, 0.4380, 0.4380, 'long_term_score(0.4380)*0.500 + short_term_score(0.4380)*0.500 = 0.4380'),
('original', 'BT_TEST2/USDT_binance_okx', '2024-06-05', 0.3650, 3, 0.3650, 0.3650, 0.3650, 'long_term_score(0.3650)*0.500 + short_term_score(0.3650)*0.500 = 0.3650'),
('original', 'BT_TEST4/USDT_binance_gate', '2024-06-05', 0.2920, 4, 0.2920, 0.2920, 0.2920, 'long_term_score(0.2920)*0.500 + short_term_score(0.2920)*0.500 = 0.2920'),
('original', 'BT_TEST6/USDT_okx_gate', '2024-06-05', 0.2190, 5, 0.2190, 0.2190, 0.2190, 'long_term_score(0.2190)*0.500 + short_term_score(0.2190)*0.500 = 0.2190'),
('original', 'BT_TEST1/USDT_binance_bybit', '2024-06-05', 0.1825, 6, 0.1825, 0.1825, 0.1825, 'long_term_score(0.1825)*0.500 + short_term_score(0.1825)*0.500 = 0.1825');

-- ================================================================
-- 測試數據摘要和驗證
-- ================================================================

-- 查看插入的測試數據統計
SELECT 
    '=== Backtest測試數據統計 ===' as section,
    COUNT(*) as total_records,
    COUNT(DISTINCT trading_pair) as unique_pairs,
    COUNT(DISTINCT date) as unique_dates,
    MIN(date) as earliest_date,
    MAX(date) as latest_date
FROM return_metrics 
WHERE trading_pair LIKE 'BT_TEST%';

-- 查看各測試場景的數據分布
SELECT 
    '=== 各場景數據分布 ===' as section,
    date,
    COUNT(*) as pairs_count,
    ROUND(AVG(return_1d), 6) as avg_1d_return,
    ROUND(AVG(roi_1d), 4) as avg_1d_roi,
    ROUND(MAX(roi_1d), 4) as max_1d_roi,
    ROUND(MIN(roi_1d), 4) as min_1d_roi,
    CASE 
        WHEN date = '2024-06-01' THEN '場景1: 基本進場測試'
        WHEN date = '2024-06-02' THEN '場景2: 排名變化測試'
        WHEN date = '2024-06-03' THEN '場景3: 負收益測試'
        WHEN date = '2024-06-04' THEN '場景4: 持倉限制測試'
        WHEN date = '2024-06-05' THEN '場景5: 完整週期測試'
    END as scenario
FROM return_metrics 
WHERE trading_pair LIKE 'BT_TEST%'
GROUP BY date
ORDER BY date;

-- 檢查策略排名數據
SELECT 
    '=== 策略排名數據檢查 ===' as section,
    date,
    COUNT(*) as ranking_records,
    MIN(rank_position) as min_rank,
    MAX(rank_position) as max_rank
FROM strategy_ranking 
WHERE trading_pair LIKE 'BT_TEST%'
GROUP BY date
ORDER BY date; 