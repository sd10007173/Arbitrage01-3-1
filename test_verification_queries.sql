-- ================================================================
-- calculate_FR_return_list_v2 測試結果驗證查詢
-- 用途：驗證程式執行結果是否正確
-- ================================================================

-- ================================================================
-- 1. 基本數據檢查
-- ================================================================

-- 檢查測試數據是否正確插入
SELECT 
    '=== 測試數據統計 ===' as section,
    symbol,
    COUNT(*) as record_count,
    MIN(DATE(timestamp_utc)) as start_date,
    MAX(DATE(timestamp_utc)) as end_date,
    ROUND(SUM(diff_ab), 6) as total_diff,
    ROUND(AVG(diff_ab), 6) as avg_diff
FROM funding_rate_diff 
WHERE symbol LIKE 'TEST%'
GROUP BY symbol
ORDER BY symbol;

-- 檢查return_metrics表是否有測試結果
SELECT 
    '=== 收益指標統計 ===' as section,
    trading_pair,
    COUNT(*) as processed_days,
    MIN(date) as start_date,
    MAX(date) as end_date,
    ROUND(MIN(return_1d), 6) as min_1d_return,
    ROUND(MAX(return_1d), 6) as max_1d_return,
    ROUND(AVG(return_1d), 6) as avg_1d_return
FROM return_metrics 
WHERE trading_pair LIKE 'TEST%'
GROUP BY trading_pair
ORDER BY trading_pair;

-- ================================================================
-- 2. 場景1：基本功能測試驗證
-- ================================================================

-- TEST1/USDT 的詳細收益計算驗證
SELECT 
    '=== 場景1：基本功能測試 ===' as section,
    date,
    return_1d,
    return_2d,
    return_7d,
    return_all,
    roi_1d,
    roi_7d
FROM return_metrics 
WHERE trading_pair = 'TEST1/USDT_binance_bybit'
ORDER BY date;

-- 手動驗證計算 (預期結果)
SELECT 
    '=== 場景1：預期結果對照 ===' as section,
    '2024-01-01' as date, 0.0002 as expected_1d, 0.0002 as expected_2d, 0.0002 as expected_7d, 0.0002 as expected_all
UNION ALL SELECT '2024-01-02', -0.0005, -0.0003, -0.0003, -0.0003
UNION ALL SELECT '2024-01-03', 0.0000, -0.0005, -0.0003, -0.0003
UNION ALL SELECT '2024-01-04', 0.0025, 0.0025, 0.0022, 0.0022
UNION ALL SELECT '2024-01-05', 0.0004, 0.0029, 0.0026, 0.0026
UNION ALL SELECT '2024-01-06', 0.0005, 0.0009, 0.0031, 0.0031
UNION ALL SELECT '2024-01-07', 0.0006, 0.0011, 0.0037, 0.0037;

-- ================================================================
-- 3. 場景2：多交易對測試驗證
-- ================================================================

-- 多交易對並行處理驗證
SELECT 
    '=== 場景2：多交易對測試 ===' as section,
    trading_pair,
    date,
    return_1d,
    return_2d,
    CASE 
        WHEN trading_pair LIKE '%TEST2%' THEN '高收益交易對'
        WHEN trading_pair LIKE '%TEST3%' THEN '低收益交易對'
    END as pair_type
FROM return_metrics 
WHERE trading_pair LIKE 'TEST2%' OR trading_pair LIKE 'TEST3%'
ORDER BY trading_pair, date;

-- ================================================================
-- 4. 場景3：空值處理測試驗證
-- ================================================================

-- 檢查NULL值處理
SELECT 
    '=== 場景3：空值處理測試 ===' as section,
    timestamp_utc,
    diff_ab,
    CASE 
        WHEN diff_ab IS NULL THEN 'NULL值'
        ELSE '正常值'
    END as value_type
FROM funding_rate_diff 
WHERE symbol = 'TEST4/USDT'
ORDER BY timestamp_utc;

-- TEST4的收益計算結果
SELECT 
    '=== 場景3：空值處理結果 ===' as section,
    date,
    return_1d,
    return_2d,
    return_all
FROM return_metrics 
WHERE trading_pair = 'TEST4/USDT_binance_bybit'
ORDER BY date;

-- ================================================================
-- 5. 場景4：滑動窗口測試驗證
-- ================================================================

-- 檢查滑動窗口計算 (選取幾個關鍵日期)
SELECT 
    '=== 場景4：滑動窗口測試 ===' as section,
    date,
    return_1d,
    return_7d,
    return_14d,
    return_30d,
    return_all,
    CASE 
        WHEN date = '2024-02-07' THEN '第7天 (7d窗口剛滿)'
        WHEN date = '2024-02-14' THEN '第14天 (14d窗口剛滿)'
        WHEN date = '2024-02-21' THEN '第21天 (進入遞減期)'
        WHEN date = '2024-03-01' THEN '第30天 (最後一天)'
    END as milestone
FROM return_metrics 
WHERE trading_pair = 'TEST5/USDT_binance_bybit'
    AND (date = '2024-02-07' OR date = '2024-02-14' OR date = '2024-02-21' OR date = '2024-03-01')
ORDER BY date;

-- 驗證滑動窗口邏輯 (7天窗口)
SELECT 
    '=== 場景4：7天滑動窗口驗證 ===' as section,
    date,
    return_7d,
    -- 手動計算前7天總和進行對照
    CASE 
        WHEN date = '2024-02-07' THEN '應為0.0028 (0.0001+...+0.0007)'
        WHEN date = '2024-02-14' THEN '應為0.0035 (0.0005*7)'
        WHEN date = '2024-02-21' THEN '應為0.0045 (0.0005*6+0.0010)'
    END as expected_calculation
FROM return_metrics 
WHERE trading_pair = 'TEST5/USDT_binance_bybit'
    AND (date = '2024-02-07' OR date = '2024-02-14' OR date = '2024-02-21')
ORDER BY date;

-- ================================================================
-- 6. 場景5：增量處理測試驗證
-- ================================================================

-- 檢查增量處理前的狀態 (第一階段)
SELECT 
    '=== 場景5：增量處理測試 (第一階段) ===' as section,
    date,
    return_1d,
    return_all
FROM return_metrics 
WHERE trading_pair = 'TEST6/USDT_binance_bybit'
ORDER BY date;

-- 注意：需要在執行第二階段數據後再次查詢以驗證增量處理

-- ================================================================
-- 7. 場景6：邊界條件測試驗證
-- ================================================================

-- 單天數據測試
SELECT 
    '=== 場景6：邊界條件測試 (單天) ===' as section,
    trading_pair,
    date,
    return_1d,
    return_2d,
    return_7d,
    return_30d,
    return_all
FROM return_metrics 
WHERE trading_pair = 'TEST7/USDT_binance_bybit'
ORDER BY date;

-- 極值數據測試
SELECT 
    '=== 場景6：邊界條件測試 (極值) ===' as section,
    trading_pair,
    date,
    return_1d,
    return_all,
    roi_1d
FROM return_metrics 
WHERE trading_pair = 'TEST8/USDT_binance_bybit'
ORDER BY date;

-- ================================================================
-- 8. 數據完整性檢查
-- ================================================================

-- 檢查是否有遺漏的日期
SELECT 
    '=== 數據完整性檢查 ===' as section,
    trading_pair,
    COUNT(*) as processed_days,
    MIN(date) as first_date,
    MAX(date) as last_date,
    -- 計算預期天數
    CASE 
        WHEN trading_pair LIKE 'TEST1%' THEN 7
        WHEN trading_pair LIKE 'TEST2%' OR trading_pair LIKE 'TEST3%' THEN 2
        WHEN trading_pair LIKE 'TEST4%' THEN 2
        WHEN trading_pair LIKE 'TEST5%' THEN 30
        WHEN trading_pair LIKE 'TEST6%' THEN 3  -- 第一階段
        WHEN trading_pair LIKE 'TEST7%' OR trading_pair LIKE 'TEST8%' THEN 1
    END as expected_days,
    CASE 
        WHEN COUNT(*) = CASE 
            WHEN trading_pair LIKE 'TEST1%' THEN 7
            WHEN trading_pair LIKE 'TEST2%' OR trading_pair LIKE 'TEST3%' THEN 2
            WHEN trading_pair LIKE 'TEST4%' THEN 2
            WHEN trading_pair LIKE 'TEST5%' THEN 30
            WHEN trading_pair LIKE 'TEST6%' THEN 3
            WHEN trading_pair LIKE 'TEST7%' OR trading_pair LIKE 'TEST8%' THEN 1
        END THEN '✅ 完整'
        ELSE '❌ 不完整'
    END as completeness_status
FROM return_metrics 
WHERE trading_pair LIKE 'TEST%'
GROUP BY trading_pair
ORDER BY trading_pair;

-- ================================================================
-- 9. 性能指標檢查
-- ================================================================

-- 檢查ROI計算是否合理
SELECT 
    '=== ROI計算檢查 ===' as section,
    trading_pair,
    date,
    return_1d,
    roi_1d,
    return_7d,
    roi_7d,
    return_30d,
    roi_30d,
    -- 驗證ROI計算公式: ROI = (return / days) * 365
    CASE 
        WHEN roi_1d IS NOT NULL AND return_1d IS NOT NULL 
        THEN ROUND(return_1d * 365, 6)
        ELSE NULL 
    END as expected_roi_1d
FROM return_metrics 
WHERE trading_pair = 'TEST1/USDT_binance_bybit'
    AND date = '2024-01-04'  -- 選取一個有明顯收益的日期
ORDER BY date;

-- ================================================================
-- 10. 總結報告
-- ================================================================

SELECT 
    '=== 測試總結報告 ===' as section,
    COUNT(DISTINCT trading_pair) as total_trading_pairs,
    COUNT(*) as total_records,
    MIN(date) as earliest_date,
    MAX(date) as latest_date,
    COUNT(CASE WHEN return_1d > 0 THEN 1 END) as positive_days,
    COUNT(CASE WHEN return_1d < 0 THEN 1 END) as negative_days,
    COUNT(CASE WHEN return_1d = 0 THEN 1 END) as zero_days,
    COUNT(CASE WHEN return_1d IS NULL THEN 1 END) as null_days,
    ROUND(AVG(return_1d), 6) as avg_daily_return,
    ROUND(MAX(return_1d), 6) as max_daily_return,
    ROUND(MIN(return_1d), 6) as min_daily_return
FROM return_metrics 
WHERE trading_pair LIKE 'TEST%'; 