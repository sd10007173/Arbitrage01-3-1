-- ================================================================
-- Strategy Ranking 快速測試查詢
-- 用途：快速驗證策略排名系統的核心功能
-- ================================================================

-- 1. 檢查測試數據載入狀態
SELECT 
    '🔍 測試數據載入檢查' as test_section,
    COUNT(*) as total_records,
    COUNT(DISTINCT trading_pair) as unique_pairs,
    COUNT(DISTINCT date) as unique_dates,
    MIN(date) as start_date,
    MAX(date) as end_date
FROM return_metrics 
WHERE trading_pair LIKE 'RANK_TEST%';

-- 2. 檢查策略排名結果生成狀態
SELECT 
    '📊 策略排名結果檢查' as test_section,
    strategy_name,
    COUNT(*) as total_rankings,
    COUNT(DISTINCT date) as covered_dates,
    COUNT(DISTINCT trading_pair) as ranked_pairs
FROM strategy_ranking 
WHERE trading_pair LIKE 'RANK_TEST%'
GROUP BY strategy_name
ORDER BY strategy_name;

-- 3. 快速查看original策略排名結果 (2024-05-01)
SELECT 
    '🏆 Original策略排名 (2024-05-01)' as test_section,
    rank_position,
    trading_pair,
    ROUND(final_ranking_score, 4) as final_score,
    ROUND(long_term_score, 4) as long_term,
    ROUND(short_term_score, 4) as short_term
FROM strategy_ranking 
WHERE strategy_name = 'original' 
    AND date = '2024-05-01'
    AND trading_pair LIKE 'RANK_TEST%'
ORDER BY rank_position;

-- 4. 檢查策略差異 (前3名對比)
SELECT 
    '🔄 策略差異對比 (前3名)' as test_section,
    sr1.trading_pair,
    sr1.rank_position as original_rank,
    COALESCE(sr2.rank_position, 'N/A') as momentum_rank,
    COALESCE(sr3.rank_position, 'N/A') as stability_rank,
    COALESCE(sr4.rank_position, 'N/A') as balanced_rank
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
    AND sr1.rank_position <= 3
ORDER BY sr1.rank_position;

-- 5. 檢查邊界條件處理 (2024-05-03)
SELECT 
    '⚠️ 邊界條件處理檢查' as test_section,
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

-- 6. 檢查時間序列一致性 (RANK_TEST11)
SELECT 
    '📈 時間序列一致性檢查' as test_section,
    date,
    rank_position,
    ROUND(final_ranking_score, 4) as final_score,
    rank_position - LAG(rank_position) OVER (ORDER BY date) as rank_change,
    CASE 
        WHEN date = '2024-05-10' THEN '初始狀態'
        WHEN date = '2024-05-11' THEN '表現提升'
        WHEN date = '2024-05-12' THEN '表現下滑'
    END as expected_trend
FROM strategy_ranking 
WHERE trading_pair = 'RANK_TEST11/USDT_time_series'
    AND strategy_name = 'original'
ORDER BY date;

-- 7. 檢查數據完整性
SELECT 
    '✅ 數據完整性檢查' as test_section,
    strategy_name,
    date,
    COUNT(*) as total_pairs,
    COUNT(CASE WHEN final_ranking_score IS NULL THEN 1 END) as null_scores,
    COUNT(CASE WHEN rank_position IS NULL THEN 1 END) as null_positions,
    MIN(rank_position) as min_rank,
    MAX(rank_position) as max_rank,
    CASE 
        WHEN COUNT(*) = MAX(rank_position) THEN 'PASS'
        ELSE 'FAIL'
    END as continuity_check
FROM strategy_ranking 
WHERE trading_pair LIKE 'RANK_TEST%'
    AND date = '2024-05-01'
GROUP BY strategy_name, date
ORDER BY strategy_name;

-- 8. 最終測試總結
SELECT 
    '📋 測試總結' as test_section,
    COUNT(DISTINCT sr.strategy_name) as tested_strategies,
    COUNT(DISTINCT sr.date) as tested_dates,
    COUNT(DISTINCT sr.trading_pair) as tested_pairs,
    COUNT(*) as total_rankings,
    ROUND(AVG(sr.final_ranking_score), 4) as avg_score,
    COUNT(CASE WHEN sr.final_ranking_score IS NULL THEN 1 END) as null_count,
    CASE 
        WHEN COUNT(CASE WHEN sr.final_ranking_score IS NULL THEN 1 END) = 0 THEN '✅ PASS'
        ELSE '❌ FAIL'
    END as overall_status
FROM strategy_ranking sr
WHERE sr.trading_pair LIKE 'RANK_TEST%'; 