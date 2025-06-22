-- ================================================================
-- Backtest_v2.py 快速驗證查詢
-- 用途：快速檢查回測系統的核心功能是否正常
-- 執行方式：在DB Browser中一次性執行所有查詢
-- ================================================================

-- 1. 檢查測試數據是否載入成功
SELECT 
    '1. 測試數據檢查' as check_item,
    (SELECT COUNT(*) FROM return_metrics WHERE trading_pair LIKE 'BT_TEST%') as return_metrics_count,
    (SELECT COUNT(*) FROM strategy_ranking WHERE trading_pair LIKE 'BT_TEST%') as strategy_ranking_count,
    CASE 
        WHEN (SELECT COUNT(*) FROM return_metrics WHERE trading_pair LIKE 'BT_TEST%') >= 28 
         AND (SELECT COUNT(*) FROM strategy_ranking WHERE trading_pair LIKE 'BT_TEST%') >= 29
        THEN '✅ 測試數據載入成功'
        ELSE '❌ 測試數據載入失敗'
    END as status;

-- 2. 檢查回測是否執行成功
SELECT 
    '2. 回測執行檢查' as check_item,
    COUNT(*) as backtest_count,
    MAX(backtest_id) as latest_backtest_id,
    CASE 
        WHEN COUNT(*) > 0 THEN '✅ 回測執行成功'
        ELSE '❌ 回測未執行或失敗'
    END as status
FROM backtest_results 
WHERE strategy_name = 'original' 
  AND start_date = '2024-06-01'
  AND end_date = '2024-06-05';

-- 3. 檢查交易事件記錄
SELECT 
    '3. 交易事件檢查' as check_item,
    COUNT(*) as total_events,
    SUM(CASE WHEN event_type = '進場' THEN 1 ELSE 0 END) as entry_events,
    SUM(CASE WHEN event_type = '離場' THEN 1 ELSE 0 END) as exit_events,
    SUM(CASE WHEN event_type = '資金費率' THEN 1 ELSE 0 END) as funding_events,
    CASE 
        WHEN COUNT(*) >= 10 THEN '✅ 交易事件記錄正常'
        ELSE '❌ 交易事件記錄異常'
    END as status
FROM backtest_trades 
WHERE backtest_id = (
    SELECT backtest_id 
    FROM backtest_results 
    WHERE strategy_name = 'original' 
      AND start_date = '2024-06-01'
      AND end_date = '2024-06-05'
    ORDER BY backtest_id DESC 
    LIMIT 1
);

-- 4. 檢查進場邏輯（應該進場前3名）
SELECT 
    '4. 進場邏輯檢查' as check_item,
    sr.trading_pair,
    sr.rank_position,
    CASE 
        WHEN bt.trading_pair IS NOT NULL THEN '✅ 已進場'
        WHEN sr.rank_position <= 3 THEN '❌ 應該進場但未進場'
        ELSE '✅ 正確未進場'
    END as entry_status
FROM strategy_ranking sr
LEFT JOIN (
    SELECT DISTINCT trading_pair
    FROM backtest_trades 
    WHERE backtest_id = (
        SELECT backtest_id 
        FROM backtest_results 
        WHERE strategy_name = 'original' 
          AND start_date = '2024-06-01'
          AND end_date = '2024-06-05'
        ORDER BY backtest_id DESC 
        LIMIT 1
    )
    AND event_type = '進場'
    AND DATE(timestamp) = '2024-06-02'
) bt ON sr.trading_pair = bt.trading_pair
WHERE sr.strategy_name = 'original' 
  AND sr.date = '2024-06-01'
ORDER BY sr.rank_position;

-- 5. 檢查持倉限制（不應超過3個）
SELECT 
    '5. 持倉限制檢查' as check_item,
    MAX(active_positions) as max_positions,
    CASE 
        WHEN MAX(active_positions) <= 3 THEN '✅ 持倉限制正常'
        ELSE '❌ 超過持倉限制'
    END as status
FROM (
    SELECT 
        DATE(timestamp) as trade_date,
        COUNT(DISTINCT trading_pair) as active_positions
    FROM (
        SELECT 
            timestamp,
            trading_pair,
            SUM(CASE WHEN event_type = '進場' THEN 1 WHEN event_type = '離場' THEN -1 ELSE 0 END) 
            OVER (PARTITION BY trading_pair ORDER BY timestamp) as position_status
        FROM backtest_trades 
        WHERE backtest_id = (
            SELECT backtest_id 
            FROM backtest_results 
            WHERE strategy_name = 'original' 
              AND start_date = '2024-06-01'
              AND end_date = '2024-06-05'
            ORDER BY backtest_id DESC 
            LIMIT 1
        )
        AND event_type IN ('進場', '離場')
    ) position_tracking
    WHERE position_status > 0
    GROUP BY DATE(timestamp)
);

-- 6. 檢查PnL計算邏輯（抽樣檢查）
SELECT 
    '6. PnL計算檢查' as check_item,
    bt.trading_pair,
    DATE(bt.timestamp) as pnl_date,
    ROUND(bt.amount, 2) as system_pnl,
    ROUND(1250 * rm.roi_1d / 100, 2) as expected_pnl,
    CASE 
        WHEN ABS(bt.amount - (1250 * rm.roi_1d / 100)) < 1 THEN '✅ PnL計算正確'
        ELSE '❌ PnL計算錯誤'
    END as calculation_check
FROM backtest_trades bt
JOIN return_metrics rm ON bt.trading_pair = rm.trading_pair 
    AND DATE(bt.timestamp) = rm.date
WHERE bt.backtest_id = (
    SELECT backtest_id 
    FROM backtest_results 
    WHERE strategy_name = 'original' 
      AND start_date = '2024-06-01'
      AND end_date = '2024-06-05'
    ORDER BY backtest_id DESC 
    LIMIT 1
)
AND bt.event_type = '資金費率'
AND DATE(bt.timestamp) = '2024-06-03'  -- 抽樣檢查第三天
LIMIT 3;

-- 7. 檢查負收益處理
SELECT 
    '7. 負收益處理檢查' as check_item,
    COUNT(*) as negative_roi_count,
    SUM(CASE 
        WHEN rm.roi_1d < 0 AND bt.amount < 0 THEN 1
        WHEN rm.roi_1d > 0 AND bt.amount > 0 THEN 1
        ELSE 0
    END) as correct_processing_count,
    CASE 
        WHEN COUNT(*) = SUM(CASE 
            WHEN rm.roi_1d < 0 AND bt.amount < 0 THEN 1
            WHEN rm.roi_1d > 0 AND bt.amount > 0 THEN 1
            ELSE 0
        END) THEN '✅ 負收益處理正確'
        ELSE '❌ 負收益處理錯誤'
    END as status
FROM backtest_trades bt
JOIN return_metrics rm ON bt.trading_pair = rm.trading_pair 
    AND DATE(bt.timestamp) = rm.date
WHERE bt.backtest_id = (
    SELECT backtest_id 
    FROM backtest_results 
    WHERE strategy_name = 'original' 
      AND start_date = '2024-06-01'
      AND end_date = '2024-06-05'
    ORDER BY backtest_id DESC 
    LIMIT 1
)
AND bt.event_type = '資金費率'
AND DATE(bt.timestamp) = '2024-06-03';

-- 8. 檢查最終績效指標
SELECT 
    '8. 績效指標檢查' as check_item,
    ROUND(final_capital, 2) as final_capital,
    ROUND(total_return, 2) as total_return,
    ROUND(total_roi * 100, 2) || '%' as total_roi_pct,
    total_days,
    ROUND(win_rate * 100, 1) || '%' as win_rate,
    profit_days + loss_days + break_even_days as calculated_days,
    CASE 
        WHEN profit_days + loss_days + break_even_days = total_days 
         AND final_capital > 0 
         AND total_days = 5
        THEN '✅ 績效指標正常'
        ELSE '❌ 績效指標異常'
    END as status
FROM backtest_results
WHERE strategy_name = 'original' 
  AND start_date = '2024-06-01'
  AND end_date = '2024-06-05'
ORDER BY backtest_id DESC 
LIMIT 1;

-- 9. 綜合檢查摘要
SELECT 
    '9. 綜合檢查摘要' as check_item,
    '回測系統功能驗證完成' as summary,
    CASE 
        WHEN (SELECT COUNT(*) FROM return_metrics WHERE trading_pair LIKE 'BT_TEST%') >= 28
         AND (SELECT COUNT(*) FROM backtest_results WHERE strategy_name = 'original' AND start_date = '2024-06-01') > 0
         AND (SELECT COUNT(*) FROM backtest_trades WHERE backtest_id = (
             SELECT backtest_id FROM backtest_results 
             WHERE strategy_name = 'original' AND start_date = '2024-06-01'
             ORDER BY backtest_id DESC LIMIT 1
         )) >= 10
        THEN '✅ 所有核心功能正常'
        ELSE '❌ 存在功能問題'
    END as overall_status;

-- 10. 測試完成確認
SELECT 
    '=== Backtest_v2.py 測試結果 ===' as test_result,
    (SELECT COUNT(*) FROM backtest_results WHERE strategy_name = 'original' AND start_date = '2024-06-01') as backtest_executions,
    (SELECT COUNT(*) FROM backtest_trades WHERE backtest_id = (
        SELECT backtest_id FROM backtest_results 
        WHERE strategy_name = 'original' AND start_date = '2024-06-01'
        ORDER BY backtest_id DESC LIMIT 1
    )) as total_trade_events,
    (SELECT ROUND(final_capital, 2) FROM backtest_results 
     WHERE strategy_name = 'original' AND start_date = '2024-06-01'
     ORDER BY backtest_id DESC LIMIT 1) as final_balance,
    '請檢查上述所有項目是否標記為 ✅' as instruction; 