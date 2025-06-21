-- ================================================================
-- Strategy Ranking 系統驗證查詢 SQL
-- 用途：驗證策略排名計算結果的正確性
-- ================================================================

-- ================================================================
-- 驗證區塊1：基本數據檢查
-- ================================================================
SELECT '=== 驗證1：基本數據載入檢查 ===' as verification_block;

-- 檢查測試數據是否正確載入
SELECT 
    COUNT(*) as total_test_records,
    COUNT(DISTINCT trading_pair) as unique_pairs,
    COUNT(DISTINCT date) as unique_dates,
    MIN(date) as earliest_date,
    MAX(date) as latest_date
FROM return_metrics 
WHERE trading_pair LIKE 'RANK_TEST%';

-- 檢查是否有NULL值影響計算
SELECT 
    trading_pair,
    date,
    CASE 
        WHEN return_1d IS NULL OR roi_1d IS NULL THEN 'HAS_NULL'
        ELSE 'COMPLETE'
    END as data_status
FROM return_metrics 
WHERE trading_pair LIKE 'RANK_TEST%'
    AND (return_1d IS NULL OR roi_1d IS NULL OR return_2d IS NULL OR roi_2d IS NULL);

-- ================================================================
-- 驗證區塊2：策略排名結果檢查
-- ================================================================
SELECT '=== 驗證2：策略排名結果檢查 ===' as verification_block;

-- 檢查 original 策略的排名結果 (2024-05-01)
SELECT 
    rank_position,
    trading_pair,
    ROUND(final_ranking_score, 6) as final_score,
    ROUND(long_term_score, 6) as long_term,
    ROUND(short_term_score, 6) as short_term,
    ROUND(combined_roi_z_score, 6) as combined_z
FROM strategy_ranking 
WHERE strategy_name = 'original' 
    AND date = '2024-05-01'
    AND trading_pair LIKE 'RANK_TEST%'
ORDER BY rank_position;

-- 檢查是否所有測試交易對都有排名結果
SELECT 
    rm.trading_pair,
    rm.date,
    CASE 
        WHEN sr.trading_pair IS NOT NULL THEN 'HAS_RANKING'
        ELSE 'MISSING_RANKING'
    END as ranking_status
FROM return_metrics rm
LEFT JOIN strategy_ranking sr ON rm.trading_pair = sr.trading_pair 
    AND rm.date = sr.date 
    AND sr.strategy_name = 'original'
WHERE rm.trading_pair LIKE 'RANK_TEST%'
    AND rm.date = '2024-05-01'
ORDER BY rm.trading_pair;

-- ================================================================
-- 驗證區塊3：多策略比較驗證
-- ================================================================
SELECT '=== 驗證3：多策略比較驗證 ===' as verification_block;

-- 比較不同策略對同一交易對的排名差異 (2024-05-01)
SELECT 
    sr1.trading_pair,
    sr1.rank_position as original_rank,
    sr1.final_ranking_score as original_score,
    sr2.rank_position as momentum_rank,
    sr2.final_ranking_score as momentum_score,
    sr3.rank_position as stability_rank,
    sr3.final_ranking_score as stability_score,
    ABS(sr1.rank_position - sr2.rank_position) as rank_diff_momentum,
    ABS(sr1.rank_position - sr3.rank_position) as rank_diff_stability
FROM strategy_ranking sr1
LEFT JOIN strategy_ranking sr2 ON sr1.trading_pair = sr2.trading_pair 
    AND sr1.date = sr2.date AND sr2.strategy_name = 'momentum_focused'
LEFT JOIN strategy_ranking sr3 ON sr1.trading_pair = sr3.trading_pair 
    AND sr1.date = sr3.date AND sr3.strategy_name = 'stability_focused'
WHERE sr1.strategy_name = 'original'
    AND sr1.date = '2024-05-01'
    AND sr1.trading_pair LIKE 'RANK_TEST%'
ORDER BY sr1.rank_position;

-- 檢查策略計算覆蓋率
SELECT 
    strategy_name,
    COUNT(*) as ranked_pairs,
    COUNT(DISTINCT date) as covered_dates,
    ROUND(AVG(final_ranking_score), 6) as avg_score
FROM strategy_ranking 
WHERE trading_pair LIKE 'RANK_TEST%'
    AND date = '2024-05-01'
GROUP BY strategy_name
ORDER BY strategy_name;

-- ================================================================
-- 驗證區塊4：排名邏輯驗證
-- ================================================================
SELECT '=== 驗證4：排名邏輯驗證 ===' as verification_block;

-- 驗證排名分數與實際排名位置的一致性
SELECT 
    strategy_name,
    trading_pair,
    rank_position,
    final_ranking_score,
    LAG(final_ranking_score) OVER (PARTITION BY strategy_name ORDER BY rank_position) as prev_score,
    CASE 
        WHEN LAG(final_ranking_score) OVER (PARTITION BY strategy_name ORDER BY rank_position) >= final_ranking_score 
        THEN 'CORRECT'
        ELSE 'INCORRECT_ORDER'
    END as ranking_order_check
FROM strategy_ranking 
WHERE trading_pair LIKE 'RANK_TEST%'
    AND date = '2024-05-01'
    AND strategy_name = 'original'
ORDER BY strategy_name, rank_position;

-- 檢查極值數據的排名處理
SELECT 
    'EXTREME_VALUES_RANKING' as test_type,
    trading_pair,
    rank_position,
    final_ranking_score,
    CASE 
        WHEN trading_pair LIKE '%extreme_pos%' THEN 'EXTREME_POSITIVE'
        WHEN trading_pair LIKE '%extreme_neg%' THEN 'EXTREME_NEGATIVE'
        WHEN trading_pair LIKE '%zero%' THEN 'ZERO_VALUES'
        WHEN trading_pair LIKE '%null%' THEN 'NULL_VALUES'
        ELSE 'NORMAL'
    END as data_type
FROM strategy_ranking 
WHERE trading_pair LIKE 'RANK_TEST%'
    AND date = '2024-05-03'
    AND strategy_name = 'original'
ORDER BY rank_position;

-- ================================================================
-- 驗證區塊5：時間序列一致性驗證
-- ================================================================
SELECT '=== 驗證5：時間序列一致性驗證 ===' as verification_block;

-- 檢查同一交易對在不同日期的排名變化
SELECT 
    trading_pair,
    date,
    rank_position,
    final_ranking_score,
    LAG(rank_position) OVER (PARTITION BY trading_pair ORDER BY date) as prev_rank,
    LAG(final_ranking_score) OVER (PARTITION BY trading_pair ORDER BY date) as prev_score,
    rank_position - LAG(rank_position) OVER (PARTITION BY trading_pair ORDER BY date) as rank_change
FROM strategy_ranking 
WHERE trading_pair = 'RANK_TEST11/USDT_time_series'
    AND strategy_name = 'original'
ORDER BY date;

-- ================================================================
-- 驗證區塊6：組件分數驗證
-- ================================================================
SELECT '=== 驗證6：組件分數驗證 ===' as verification_block;

-- 檢查組件分數的合理性
SELECT 
    trading_pair,
    ROUND(long_term_score, 6) as long_term,
    ROUND(short_term_score, 6) as short_term,
    ROUND(combined_roi_z_score, 6) as combined,
    ROUND(final_ranking_score, 6) as final_score,
    -- 檢查組合邏輯：final_score 應該是 long_term 和 short_term 的加權平均
    ROUND((long_term_score + short_term_score) / 2, 6) as expected_combined,
    CASE 
        WHEN ABS(combined_roi_z_score - (long_term_score + short_term_score) / 2) < 0.0001 
        THEN 'CORRECT'
        ELSE 'INCORRECT'
    END as combination_check
FROM strategy_ranking 
WHERE trading_pair LIKE 'RANK_TEST%'
    AND date = '2024-05-01'
    AND strategy_name = 'original'
ORDER BY rank_position;

-- ================================================================
-- 驗證區塊7：策略特化驗證
-- ================================================================
SELECT '=== 驗證7：策略特化驗證 ===' as verification_block;

-- 驗證動量策略是否偏愛短期表現好的交易對
SELECT 
    'MOMENTUM_STRATEGY_TEST' as test_type,
    sr.trading_pair,
    sr.rank_position as momentum_rank,
    rm.roi_1d,
    rm.roi_2d,
    (rm.roi_1d + rm.roi_2d) / 2 as short_term_avg,
    sr.final_ranking_score as momentum_score
FROM strategy_ranking sr
JOIN return_metrics rm ON sr.trading_pair = rm.trading_pair AND sr.date = rm.date
WHERE sr.strategy_name = 'momentum_focused'
    AND sr.date = '2024-05-15'
    AND sr.trading_pair LIKE 'RANK_TEST_%'
ORDER BY sr.rank_position;

-- 驗證穩定策略是否偏愛長期表現好的交易對
SELECT 
    'STABILITY_STRATEGY_TEST' as test_type,
    sr.trading_pair,
    sr.rank_position as stability_rank,
    rm.roi_14d,
    rm.roi_30d,
    rm.roi_all,
    (rm.roi_14d + rm.roi_30d + rm.roi_all) / 3 as long_term_avg,
    sr.final_ranking_score as stability_score
FROM strategy_ranking sr
JOIN return_metrics rm ON sr.trading_pair = rm.trading_pair AND sr.date = rm.date
WHERE sr.strategy_name = 'stability_focused'
    AND sr.date = '2024-05-15'
    AND sr.trading_pair LIKE 'RANK_TEST_%'
ORDER BY sr.rank_position;

-- ================================================================
-- 驗證區塊8：數據完整性最終檢查
-- ================================================================
SELECT '=== 驗證8：數據完整性最終檢查 ===' as verification_block;

-- 檢查每個策略的數據完整性
SELECT 
    strategy_name,
    COUNT(*) as total_rankings,
    COUNT(DISTINCT trading_pair) as unique_pairs,
    COUNT(DISTINCT date) as unique_dates,
    COUNT(CASE WHEN final_ranking_score IS NULL THEN 1 END) as null_scores,
    COUNT(CASE WHEN rank_position IS NULL THEN 1 END) as null_positions,
    MIN(rank_position) as min_rank,
    MAX(rank_position) as max_rank
FROM strategy_ranking 
WHERE trading_pair LIKE 'RANK_TEST%'
GROUP BY strategy_name
ORDER BY strategy_name;

-- 檢查排名位置的連續性
SELECT 
    strategy_name,
    date,
    COUNT(*) as total_ranks,
    MIN(rank_position) as min_rank,
    MAX(rank_position) as max_rank,
    MAX(rank_position) - MIN(rank_position) + 1 as expected_count,
    CASE 
        WHEN COUNT(*) = MAX(rank_position) - MIN(rank_position) + 1 THEN 'CONTINUOUS'
        ELSE 'HAS_GAPS'
    END as continuity_check
FROM strategy_ranking 
WHERE trading_pair LIKE 'RANK_TEST%'
GROUP BY strategy_name, date
ORDER BY strategy_name, date;

-- ================================================================
-- 驗證區塊9：性能和異常檢查
-- ================================================================
SELECT '=== 驗證9：性能和異常檢查 ===' as verification_block;

-- 檢查是否有重複的排名位置
SELECT 
    strategy_name,
    date,
    rank_position,
    COUNT(*) as duplicate_count
FROM strategy_ranking 
WHERE trading_pair LIKE 'RANK_TEST%'
GROUP BY strategy_name, date, rank_position
HAVING COUNT(*) > 1;

-- 檢查分數分布的合理性
SELECT 
    strategy_name,
    date,
    COUNT(*) as pair_count,
    ROUND(MIN(final_ranking_score), 6) as min_score,
    ROUND(MAX(final_ranking_score), 6) as max_score,
    ROUND(AVG(final_ranking_score), 6) as avg_score,
    ROUND(MAX(final_ranking_score) - MIN(final_ranking_score), 6) as score_range
FROM strategy_ranking 
WHERE trading_pair LIKE 'RANK_TEST%'
GROUP BY strategy_name, date
ORDER BY strategy_name, date;

-- ================================================================
-- 驗證區塊10：交叉驗證總結
-- ================================================================
SELECT '=== 驗證10：交叉驗證總結 ===' as verification_block;

-- 總結各策略的表現特徵
SELECT 
    sr.strategy_name,
    COUNT(*) as total_rankings,
    ROUND(AVG(sr.final_ranking_score), 6) as avg_final_score,
    ROUND(STDDEV(sr.final_ranking_score), 6) as score_stddev,
    -- 計算與原始收益指標的相關性指標
    ROUND(AVG(rm.roi_1d), 4) as avg_1d_roi,
    ROUND(AVG(rm.roi_all), 4) as avg_all_roi,
    ROUND(CORR(sr.final_ranking_score, rm.roi_1d), 4) as corr_with_1d_roi,
    ROUND(CORR(sr.final_ranking_score, rm.roi_all), 4) as corr_with_all_roi
FROM strategy_ranking sr
JOIN return_metrics rm ON sr.trading_pair = rm.trading_pair AND sr.date = rm.date
WHERE sr.trading_pair LIKE 'RANK_TEST%'
GROUP BY sr.strategy_name
ORDER BY sr.strategy_name; 