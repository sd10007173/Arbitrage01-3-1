-- ================================================================
-- Strategy Ranking 系統測試數據生成 SQL
-- 用途：為策略排名系統提供測試數據
-- 測試 ranking_config.py + ranking_engine.py + strategy_ranking.py
-- ================================================================

-- 清理現有測試數據
DELETE FROM return_metrics WHERE trading_pair LIKE 'RANK_TEST%';
DELETE FROM strategy_ranking WHERE trading_pair LIKE 'RANK_TEST%';

-- ================================================================
-- 場景1：基本策略測試數據 - 6個交易對，2024-05-01
-- ================================================================
-- 設計不同特性的交易對來測試排名邏輯

-- RANK_TEST1: 短期強勢型 (1d, 2d表現佳，長期一般)
INSERT INTO return_metrics (trading_pair, date, return_1d, roi_1d, return_2d, roi_2d, return_7d, roi_7d, return_14d, roi_14d, return_30d, roi_30d, return_all, roi_all) VALUES
('RANK_TEST1/USDT_binance_bybit', '2024-05-01', 0.0050, 1.8250, 0.0080, 1.4600, 0.0120, 0.6260, 0.0150, 0.3912, 0.0200, 0.2433, 0.0250, 0.1826);

-- RANK_TEST2: 長期穩健型 (長期表現佳，短期一般)
INSERT INTO return_metrics (trading_pair, date, return_1d, roi_1d, return_2d, roi_2d, return_7d, roi_7d, return_14d, roi_14d, return_30d, roi_30d, return_all, roi_all) VALUES
('RANK_TEST2/USDT_binance_okx', '2024-05-01', 0.0015, 0.5475, 0.0025, 0.4563, 0.0080, 0.4172, 0.0180, 0.4695, 0.0350, 0.4271, 0.0500, 0.3653);

-- RANK_TEST3: 平衡型 (各期表現均衡)
INSERT INTO return_metrics (trading_pair, date, return_1d, roi_1d, return_2d, roi_2d, return_7d, roi_7d, return_14d, roi_14d, return_30d, roi_30d, return_all, roi_all) VALUES
('RANK_TEST3/USDT_bybit_okx', '2024-05-01', 0.0030, 1.0950, 0.0055, 1.0038, 0.0105, 0.5476, 0.0180, 0.4695, 0.0280, 0.3416, 0.0380, 0.2774);

-- RANK_TEST4: 波動型 (表現不穩定，有高有低)
INSERT INTO return_metrics (trading_pair, date, return_1d, roi_1d, return_2d, roi_2d, return_7d, roi_7d, return_14d, roi_14d, return_30d, roi_30d, return_all, roi_all) VALUES
('RANK_TEST4/USDT_binance_gate', '2024-05-01', 0.0080, 2.9200, 0.0060, 1.0950, 0.0040, 0.2086, 0.0020, 0.0522, 0.0010, 0.0122, 0.0005, 0.0037);

-- RANK_TEST5: 負收益型 (部分指標為負)
INSERT INTO return_metrics (trading_pair, date, return_1d, roi_1d, return_2d, roi_2d, return_7d, roi_7d, return_14d, roi_14d, return_30d, roi_30d, return_all, roi_all) VALUES
('RANK_TEST5/USDT_bybit_gate', '2024-05-01', -0.0020, -0.7300, -0.0015, -0.2738, 0.0010, 0.0522, 0.0030, 0.0783, 0.0050, 0.0610, 0.0080, 0.0584);

-- RANK_TEST6: 極值型 (包含極大值用於測試)
INSERT INTO return_metrics (trading_pair, date, return_1d, roi_1d, return_2d, roi_2d, return_7d, roi_7d, return_14d, roi_14d, return_30d, roi_30d, return_all, roi_all) VALUES
('RANK_TEST6/USDT_okx_gate', '2024-05-01', 0.0200, 7.3000, 0.0350, 6.3875, 0.0450, 2.3463, 0.0500, 1.3043, 0.0600, 0.7320, 0.0700, 0.5110);

-- ================================================================
-- 場景2：多策略比較測試數據 - 同樣6個交易對，2024-05-02
-- ================================================================
-- 第二天數據，用於測試不同策略的排名差異

-- RANK_TEST1: 短期動量減弱
INSERT INTO return_metrics (trading_pair, date, return_1d, roi_1d, return_2d, roi_2d, return_7d, roi_7d, return_14d, roi_14d, return_30d, roi_30d, return_all, roi_all) VALUES
('RANK_TEST1/USDT_binance_bybit', '2024-05-02', 0.0020, 0.7300, 0.0070, 1.2775, 0.0140, 0.7295, 0.0170, 0.4434, 0.0220, 0.2684, 0.0270, 0.1971);

-- RANK_TEST2: 長期穩健持續
INSERT INTO return_metrics (trading_pair, date, return_1d, roi_1d, return_2d, roi_2d, return_7d, roi_7d, return_14d, roi_14d, return_30d, roi_30d, return_all, roi_all) VALUES
('RANK_TEST2/USDT_binance_okx', '2024-05-02', 0.0025, 0.9125, 0.0040, 0.7300, 0.0105, 0.5476, 0.0205, 0.5347, 0.0375, 0.4575, 0.0525, 0.3833);

-- RANK_TEST3: 平衡表現持續
INSERT INTO return_metrics (trading_pair, date, return_1d, roi_1d, return_2d, roi_2d, return_7d, roi_7d, return_14d, roi_14d, return_30d, roi_30d, return_all, roi_all) VALUES
('RANK_TEST3/USDT_bybit_okx', '2024-05-02', 0.0035, 1.2775, 0.0065, 1.1863, 0.0140, 0.7295, 0.0215, 0.5608, 0.0315, 0.3843, 0.0415, 0.3029);

-- RANK_TEST4: 波動加劇
INSERT INTO return_metrics (trading_pair, date, return_1d, roi_1d, return_2d, roi_2d, return_7d, roi_7d, return_14d, roi_14d, return_30d, roi_30d, return_all, roi_all) VALUES
('RANK_TEST4/USDT_binance_gate', '2024-05-02', 0.0010, 0.3650, 0.0090, 1.6425, 0.0050, 0.2608, 0.0030, 0.0783, 0.0020, 0.0244, 0.0015, 0.0109);

-- RANK_TEST5: 轉為正收益
INSERT INTO return_metrics (trading_pair, date, return_1d, roi_1d, return_2d, roi_2d, return_7d, roi_7d, return_14d, roi_14d, return_30d, roi_30d, return_all, roi_all) VALUES
('RANK_TEST5/USDT_bybit_gate', '2024-05-02', 0.0040, 1.4600, 0.0020, 0.3650, 0.0050, 0.2608, 0.0070, 0.1826, 0.0090, 0.1098, 0.0120, 0.0876);

-- RANK_TEST6: 極值回調
INSERT INTO return_metrics (trading_pair, date, return_1d, roi_1d, return_2d, roi_2d, return_7d, roi_7d, return_14d, roi_14d, return_30d, roi_30d, return_all, roi_all) VALUES
('RANK_TEST6/USDT_okx_gate', '2024-05-02', 0.0100, 3.6500, 0.0300, 5.4750, 0.0500, 2.6040, 0.0600, 1.5652, 0.0700, 0.8540, 0.0800, 0.5840);

-- ================================================================
-- 場景3：邊界條件測試數據 - 2024-05-03
-- ================================================================
-- 包含NULL值、零值、極值等邊界情況

-- RANK_TEST7: 包含NULL值的情況
INSERT INTO return_metrics (trading_pair, date, return_1d, roi_1d, return_2d, roi_2d, return_7d, roi_7d, return_14d, roi_14d, return_30d, roi_30d, return_all, roi_all) VALUES
('RANK_TEST7/USDT_test_null', '2024-05-03', 0.0030, 1.0950, NULL, NULL, 0.0080, 0.4172, NULL, NULL, 0.0200, 0.2440, 0.0250, 0.1826);

-- RANK_TEST8: 全零值
INSERT INTO return_metrics (trading_pair, date, return_1d, roi_1d, return_2d, roi_2d, return_7d, roi_7d, return_14d, roi_14d, return_30d, roi_30d, return_all, roi_all) VALUES
('RANK_TEST8/USDT_test_zero', '2024-05-03', 0.0000, 0.0000, 0.0000, 0.0000, 0.0000, 0.0000, 0.0000, 0.0000, 0.0000, 0.0000, 0.0000, 0.0000);

-- RANK_TEST9: 極大正值
INSERT INTO return_metrics (trading_pair, date, return_1d, roi_1d, return_2d, roi_2d, return_7d, roi_7d, return_14d, roi_14d, return_30d, roi_30d, return_all, roi_all) VALUES
('RANK_TEST9/USDT_test_extreme_pos', '2024-05-03', 0.1000, 36.5000, 0.1500, 27.3750, 0.2000, 10.4280, 0.2500, 6.5217, 0.3000, 3.6600, 0.3500, 2.5550);

-- RANK_TEST10: 極大負值
INSERT INTO return_metrics (trading_pair, date, return_1d, roi_1d, return_2d, roi_2d, return_7d, roi_7d, return_14d, roi_14d, return_30d, roi_30d, return_all, roi_all) VALUES
('RANK_TEST10/USDT_test_extreme_neg', '2024-05-03', -0.0500, -18.2500, -0.0800, -14.6000, -0.1000, -5.2140, -0.1200, -3.1304, -0.1500, -1.8300, -0.2000, -1.4600);

-- ================================================================
-- 場景4：時間序列測試數據 - 多天數據用於測試增量處理
-- ================================================================
-- RANK_TEST11: 連續3天數據，測試排名變化

-- 第一天：初始狀態
INSERT INTO return_metrics (trading_pair, date, return_1d, roi_1d, return_2d, roi_2d, return_7d, roi_7d, return_14d, roi_14d, return_30d, roi_30d, return_all, roi_all) VALUES
('RANK_TEST11/USDT_time_series', '2024-05-10', 0.0025, 0.9125, 0.0040, 0.7300, 0.0080, 0.4172, 0.0120, 0.3130, 0.0180, 0.2196, 0.0220, 0.1606);

-- 第二天：表現提升
INSERT INTO return_metrics (trading_pair, date, return_1d, roi_1d, return_2d, roi_2d, return_7d, roi_7d, return_14d, roi_14d, return_30d, roi_30d, return_all, roi_all) VALUES
('RANK_TEST11/USDT_time_series', '2024-05-11', 0.0045, 1.6425, 0.0070, 1.2775, 0.0125, 0.6510, 0.0165, 0.4304, 0.0225, 0.2745, 0.0265, 0.1935);

-- 第三天：表現下滑
INSERT INTO return_metrics (trading_pair, date, return_1d, roi_1d, return_2d, roi_2d, return_7d, roi_7d, return_14d, roi_14d, return_30d, roi_30d, return_all, roi_all) VALUES
('RANK_TEST11/USDT_time_series', '2024-05-12', 0.0010, 0.3650, 0.0055, 1.0038, 0.0135, 0.7034, 0.0175, 0.4565, 0.0235, 0.2867, 0.0275, 0.2007);

-- ================================================================
-- 場景5：策略特化測試數據 - 2024-05-15
-- ================================================================
-- 專門設計來測試不同策略特性的數據

-- 動量策略偏愛：短期爆發型
INSERT INTO return_metrics (trading_pair, date, return_1d, roi_1d, return_2d, roi_2d, return_7d, roi_7d, return_14d, roi_14d, return_30d, roi_30d, return_all, roi_all) VALUES
('RANK_TEST_MOMENTUM1/USDT_test', '2024-05-15', 0.0080, 2.9200, 0.0120, 2.1900, 0.0140, 0.7295, 0.0150, 0.3912, 0.0160, 0.1952, 0.0170, 0.1241);

-- 穩定策略偏愛：長期穩健型
INSERT INTO return_metrics (trading_pair, date, return_1d, roi_1d, return_2d, roi_2d, return_7d, roi_7d, return_14d, roi_14d, return_30d, roi_30d, return_all, roi_all) VALUES
('RANK_TEST_STABILITY1/USDT_test', '2024-05-15', 0.0020, 0.7300, 0.0035, 0.6388, 0.0070, 0.3651, 0.0140, 0.3651, 0.0280, 0.3416, 0.0420, 0.3066);

-- 平衡策略測試：各時段均衡
INSERT INTO return_metrics (trading_pair, date, return_1d, roi_1d, return_2d, roi_2d, return_7d, roi_7d, return_14d, roi_14d, return_30d, roi_30d, return_all, roi_all) VALUES
('RANK_TEST_BALANCED1/USDT_test', '2024-05-15', 0.0040, 1.4600, 0.0070, 1.2775, 0.0120, 0.6260, 0.0200, 0.5217, 0.0320, 0.3904, 0.0440, 0.3212);

-- ================================================================
-- 測試數據摘要和驗證
-- ================================================================

-- 查看插入的測試數據統計
SELECT 
    '=== 策略排名測試數據統計 ===' as section,
    COUNT(*) as total_records,
    COUNT(DISTINCT trading_pair) as unique_pairs,
    COUNT(DISTINCT date) as unique_dates,
    MIN(date) as earliest_date,
    MAX(date) as latest_date
FROM return_metrics 
WHERE trading_pair LIKE 'RANK_TEST%';

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
        WHEN date = '2024-05-01' THEN '場景1: 基本策略測試'
        WHEN date = '2024-05-02' THEN '場景2: 多策略比較'
        WHEN date = '2024-05-03' THEN '場景3: 邊界條件測試'
        WHEN date BETWEEN '2024-05-10' AND '2024-05-12' THEN '場景4: 時間序列測試'
        WHEN date = '2024-05-15' THEN '場景5: 策略特化測試'
    END as scenario
FROM return_metrics 
WHERE trading_pair LIKE 'RANK_TEST%'
GROUP BY date
ORDER BY date;

-- 檢查數據質量
SELECT 
    '=== 數據質量檢查 ===' as section,
    trading_pair,
    date,
    CASE 
        WHEN return_1d IS NULL THEN 'NULL_1d'
        WHEN roi_1d IS NULL THEN 'NULL_roi_1d'
        WHEN return_1d = 0 AND roi_1d = 0 THEN 'ZERO_VALUES'
        WHEN roi_1d > 10 THEN 'EXTREME_HIGH'
        WHEN roi_1d < -10 THEN 'EXTREME_LOW'
        ELSE 'NORMAL'
    END as data_quality,
    return_1d,
    roi_1d
FROM return_metrics 
WHERE trading_pair LIKE 'RANK_TEST%'
    AND (return_1d IS NULL OR roi_1d IS NULL OR return_1d = 0 OR ABS(roi_1d) > 5)
ORDER BY date, trading_pair; 