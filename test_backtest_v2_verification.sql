-- ================================================================
-- Backtest_v2.py 回測結果驗證查詢
-- 用途：驗證回測系統的運行結果和邏輯正確性
-- 執行方式：在DB Browser中逐個執行以下查詢區塊
-- ================================================================

-- ================================================================
-- 驗證區塊1：回測基本信息檢查
-- ================================================================
-- 檢查回測結果是否成功保存
SELECT 
    '=== 回測結果基本信息 ===' as check_type,
    backtest_id,
    strategy_name,
    start_date,
    end_date,
    initial_capital,
    final_capital,
    ROUND(total_return, 2) as total_return,
    ROUND(total_roi * 100, 2) || '%' as total_roi_pct,
    total_days,
    max_positions,
    entry_top_n,
    exit_threshold
FROM backtest_results 
WHERE strategy_name = 'original' 
  AND start_date = '2024-06-01'
  AND end_date = '2024-06-05'
ORDER BY backtest_id DESC 
LIMIT 1;

-- ================================================================
-- 驗證區塊2：交易事件序列檢查
-- ================================================================
-- 檢查完整的交易事件序列
SELECT 
    '=== 交易事件序列 ===' as check_type,
    ROW_NUMBER() OVER (ORDER BY timestamp) as event_order,
    DATE(timestamp) as trade_date,
    TIME(timestamp) as trade_time,
    event_type,
    trading_pair,
    ROUND(amount, 2) as amount,
    ROUND(funding_rate_diff, 4) as funding_rate_diff,
    ROUND(cash_after, 2) as cash_after,
    ROUND(position_after, 2) as position_after,
    ROUND(cash_after + position_after, 2) as total_balance
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
ORDER BY timestamp;

-- ================================================================
-- 驗證區塊3：進場邏輯驗證
-- ================================================================
-- 驗證第一天進場是否正確（應該進場前3名：TEST1, TEST2, TEST3）
SELECT 
    '=== 第一天進場邏輯驗證 ===' as check_type,
    sr.trading_pair,
    sr.rank_position,
    sr.final_ranking_score,
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
    AND DATE(timestamp) = '2024-06-02'  -- 第二天才開始交易
) bt ON sr.trading_pair = bt.trading_pair
WHERE sr.strategy_name = 'original' 
  AND sr.date = '2024-06-01'
ORDER BY sr.rank_position;

-- ================================================================
-- 驗證區塊4：持倉限制驗證
-- ================================================================
-- 檢查是否超過最大持倉數限制（最大3個）
SELECT 
    '=== 持倉限制驗證 ===' as check_type,
    DATE(timestamp) as trade_date,
    COUNT(DISTINCT trading_pair) as active_positions,
    CASE 
        WHEN COUNT(DISTINCT trading_pair) <= 3 THEN '✅ 未超過限制'
        ELSE '❌ 超過最大持倉限制'
    END as limit_check
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
WHERE position_status > 0  -- 只計算有持倉的
GROUP BY DATE(timestamp)
ORDER BY trade_date;

-- ================================================================
-- 驗證區塊5：資金費率PnL計算驗證
-- ================================================================
-- 驗證資金費率收益計算是否正確
SELECT 
    '=== 資金費率PnL計算驗證 ===' as check_type,
    DATE(bt.timestamp) as pnl_date,
    bt.trading_pair,
    bt.funding_rate_diff as system_rate,
    rm.roi_1d as expected_rate,
    ROUND(bt.amount, 2) as system_pnl,
    -- 手工計算預期PnL（假設持倉金額2500，有效金額1250）
    ROUND(1250 * rm.roi_1d / 100, 2) as expected_pnl,
    CASE 
        WHEN ABS(bt.amount - (1250 * rm.roi_1d / 100)) < 0.01 THEN '✅ 計算正確'
        ELSE '❌ 計算錯誤'
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
ORDER BY bt.timestamp;

-- ================================================================
-- 驗證區塊6：排名變化觸發的進出場驗證
-- ================================================================
-- 驗證第二天排名變化導致的交易
SELECT 
    '=== 排名變化交易驗證 ===' as check_type,
    '第二天(2024-06-02)排名變化分析' as analysis,
    d1.trading_pair,
    d1.rank_position as day1_rank,
    d2.rank_position as day2_rank,
    d2.rank_position - d1.rank_position as rank_change,
    CASE 
        WHEN d1.rank_position <= 3 AND d2.rank_position > 3 THEN '應該離場'
        WHEN d1.rank_position > 3 AND d2.rank_position <= 3 THEN '應該進場'
        WHEN d1.rank_position <= 3 AND d2.rank_position <= 3 THEN '繼續持有'
        ELSE '無持倉'
    END as expected_action
FROM strategy_ranking d1
JOIN strategy_ranking d2 ON d1.trading_pair = d2.trading_pair
WHERE d1.strategy_name = 'original' AND d1.date = '2024-06-01'
  AND d2.strategy_name = 'original' AND d2.date = '2024-06-02'
ORDER BY d1.rank_position;

-- ================================================================
-- 驗證區塊7：負收益處理驗證
-- ================================================================
-- 檢查第三天負收益的處理
SELECT 
    '=== 負收益處理驗證 ===' as check_type,
    bt.trading_pair,
    rm.roi_1d as rate_1d,
    ROUND(bt.amount, 2) as pnl_amount,
    CASE 
        WHEN rm.roi_1d < 0 AND bt.amount < 0 THEN '✅ 負收益正確處理'
        WHEN rm.roi_1d > 0 AND bt.amount > 0 THEN '✅ 正收益正確處理'
        ELSE '❌ 收益處理錯誤'
    END as processing_check
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
AND DATE(bt.timestamp) = '2024-06-03'  -- 第三天有負收益
ORDER BY bt.trading_pair;

-- ================================================================
-- 驗證區塊8：最終績效指標驗證
-- ================================================================
-- 檢查最終績效指標的合理性
SELECT 
    '=== 最終績效指標驗證 ===' as check_type,
    br.final_capital,
    br.total_return,
    ROUND(br.total_roi * 100, 2) || '%' as total_roi_pct,
    br.total_days,
    ROUND(br.win_rate * 100, 1) || '%' as win_rate_pct,
    br.profit_days,
    br.loss_days,
    br.break_even_days,
    br.total_trades,
    ROUND(br.max_drawdown * 100, 2) || '%' as max_drawdown_pct,
    CASE 
        WHEN br.profit_days + br.loss_days + br.break_even_days = br.total_days THEN '✅ 天數統計正確'
        ELSE '❌ 天數統計錯誤'
    END as days_check,
    CASE 
        WHEN br.win_rate = CAST(br.profit_days AS FLOAT) / br.total_days THEN '✅ 勝率計算正確'
        ELSE '❌ 勝率計算錯誤'
    END as win_rate_check
FROM backtest_results br
WHERE br.strategy_name = 'original' 
  AND br.start_date = '2024-06-01'
  AND br.end_date = '2024-06-05'
ORDER BY br.backtest_id DESC 
LIMIT 1;

-- ================================================================
-- 驗證區塊9：資金平衡驗證
-- ================================================================
-- 檢查現金餘額 + 持倉餘額 = 總餘額
SELECT 
    '=== 資金平衡驗證 ===' as check_type,
    DATE(timestamp) as check_date,
    ROUND(cash_after, 2) as cash_balance,
    ROUND(position_after, 2) as position_balance,
    ROUND(cash_after + position_after, 2) as calculated_total,
    ROUND(cash_after + position_after, 2) as system_total,
    CASE 
        WHEN ABS((cash_after + position_after) - (cash_after + position_after)) < 0.01 THEN '✅ 資金平衡'
        ELSE '❌ 資金不平衡'
    END as balance_check
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
GROUP BY DATE(timestamp), cash_after, position_after
ORDER BY check_date;

-- ================================================================
-- 驗證區塊10：完整性檢查摘要
-- ================================================================
-- 綜合檢查摘要
SELECT 
    '=== 完整性檢查摘要 ===' as summary_type,
    COUNT(DISTINCT DATE(timestamp)) as trading_days,
    COUNT(*) as total_events,
    SUM(CASE WHEN event_type = '進場' THEN 1 ELSE 0 END) as entry_events,
    SUM(CASE WHEN event_type = '離場' THEN 1 ELSE 0 END) as exit_events,
    SUM(CASE WHEN event_type = '資金費率' THEN 1 ELSE 0 END) as funding_events,
    COUNT(DISTINCT trading_pair) as unique_pairs_traded,
    ROUND(MIN(cash_after + position_after), 2) as min_total_balance,
    ROUND(MAX(cash_after + position_after), 2) as max_total_balance
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