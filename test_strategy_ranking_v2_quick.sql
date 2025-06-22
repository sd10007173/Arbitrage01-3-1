-- ================================================================
-- Strategy Ranking V2 系統快速驗證 SQL
-- 用途：快速驗證 strategy_ranking_v2.py 的執行結果
-- ================================================================

-- 1. 檢查測試數據載入狀況
-- ================================================================
.mode table
.headers on

SELECT '1. 測試數據載入驗證' as check_name;
SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT trading_pair) as unique_pairs,
    COUNT(DISTINCT date) as unique_dates,
    MIN(date) as start_date,
    MAX(date) as end_date
FROM return_metrics 
WHERE trading_pair LIKE 'RANK_TEST%';

-- 2. 檢查策略排名結果生成
-- ================================================================
SELECT '2. 策略排名結果統計' as check_name;
SELECT 
    strategy_name,
    COUNT(*) as total_rankings,
    COUNT(DISTINCT date) as covered_dates,
    COUNT(DISTINCT trading_pair) as ranked_pairs,
    MIN(date) as first_date,
    MAX(date) as last_date
FROM strategy_ranking 
WHERE trading_pair LIKE 'RANK_TEST%'
GROUP BY strategy_name
ORDER BY strategy_name;

-- 3. 檢查排名邏輯正確性
-- ================================================================
SELECT '3. 排名邏輯正確性檢查 (2024-05-01 original策略)' as check_name;
SELECT 
    rank_position,
    trading_pair,
    ROUND(final_ranking_score, 4) as score,
    CASE 
        WHEN rank_position = 1 THEN '最高分'
        WHEN rank_position <= 3 THEN '前三名'
        ELSE '其他'
    END as ranking_category
FROM strategy_ranking 
WHERE strategy_name = 'original' 
AND date = '2024-05-01' 
AND trading_pair LIKE 'RANK_TEST%'
ORDER BY rank_position;

-- 4. 檢查排名分數遞減邏輯
-- ================================================================
SELECT '4. 排名分數遞減邏輯檢查' as check_name;
SELECT 
    strategy_name,
    date,
    trading_pair,
    rank_position,
    ROUND(final_ranking_score, 4) as score,
    LAG(final_ranking_score) OVER (PARTITION BY strategy_name, date ORDER BY rank_position) as prev_score,
    CASE 
        WHEN LAG(final_ranking_score) OVER (PARTITION BY strategy_name, date ORDER BY rank_position) IS NULL THEN 'FIRST'
        WHEN LAG(final_ranking_score) OVER (PARTITION BY strategy_name, date ORDER BY rank_position) >= final_ranking_score 
        THEN 'CORRECT' 
        ELSE 'INCORRECT'
    END as order_check
FROM strategy_ranking 
WHERE trading_pair LIKE 'RANK_TEST%' 
AND date = '2024-05-01'
AND strategy_name = 'original'
ORDER BY strategy_name, rank_position;

-- 5. 檢查重複排名問題
-- ================================================================
SELECT '5. 重複排名檢查' as check_name;
SELECT 
    strategy_name, 
    date, 
    rank_position, 
    COUNT(*) as duplicate_count,
    GROUP_CONCAT(trading_pair) as pairs_with_same_rank
FROM strategy_ranking 
WHERE trading_pair LIKE 'RANK_TEST%'
GROUP BY strategy_name, date, rank_position
HAVING COUNT(*) > 1;

-- 如果沒有結果，表示沒有重複排名（正確）
SELECT CASE 
    WHEN (SELECT COUNT(*) FROM (
        SELECT strategy_name, date, rank_position, COUNT(*) as cnt
        FROM strategy_ranking 
        WHERE trading_pair LIKE 'RANK_TEST%'
        GROUP BY strategy_name, date, rank_position
        HAVING COUNT(*) > 1
    )) = 0 
    THEN '✅ 沒有重複排名問題' 
    ELSE '❌ 發現重複排名問題' 
END as duplicate_check_result;

-- 6. 檢查實驗性策略執行結果
-- ================================================================
SELECT '6. 實驗性策略執行檢查' as check_name;
SELECT 
    strategy_name,
    COUNT(*) as ranking_count,
    CASE 
        WHEN strategy_name LIKE 'test_%' THEN '實驗性策略'
        ELSE '主要策略'
    END as strategy_type
FROM strategy_ranking 
WHERE trading_pair LIKE 'RANK_TEST%'
GROUP BY strategy_name
ORDER BY strategy_type, strategy_name;

-- 7. 檢查多策略排名差異
-- ================================================================
SELECT '7. 多策略排名差異檢查 (前3名)' as check_name;
SELECT 
    sr1.trading_pair,
    sr1.rank_position as original_rank,
    COALESCE(sr2.rank_position, '-') as momentum_rank,
    COALESCE(sr3.rank_position, '-') as test_simple_rank,
    CASE 
        WHEN sr1.rank_position != COALESCE(sr2.rank_position, 999) 
        OR sr1.rank_position != COALESCE(sr3.rank_position, 999)
        THEN '有差異' 
        ELSE '相同'
    END as ranking_difference
FROM strategy_ranking sr1
LEFT JOIN strategy_ranking sr2 ON sr1.trading_pair = sr2.trading_pair 
    AND sr1.date = sr2.date AND sr2.strategy_name = 'momentum_focused'
LEFT JOIN strategy_ranking sr3 ON sr1.trading_pair = sr3.trading_pair 
    AND sr1.date = sr3.date AND sr3.strategy_name = 'test_simple_1d'
WHERE sr1.strategy_name = 'original' 
AND sr1.date = '2024-05-01'
AND sr1.trading_pair LIKE 'RANK_TEST%'
AND sr1.rank_position <= 3
ORDER BY sr1.rank_position;

-- 8. 檢查邊界條件處理
-- ================================================================
SELECT '8. 邊界條件處理檢查' as check_name;
SELECT 
    trading_pair,
    rank_position,
    ROUND(final_ranking_score, 4) as score,
    CASE 
        WHEN trading_pair LIKE '%TEST5%' THEN '負收益型'
        WHEN trading_pair LIKE '%TEST6%' THEN '極值型'
        WHEN final_ranking_score IS NULL THEN 'NULL值'
        WHEN final_ranking_score = 0 THEN '零值'
        WHEN final_ranking_score > 10 THEN '極大值'
        WHEN final_ranking_score < -5 THEN '極小值'
        ELSE '正常值'
    END as data_type
FROM strategy_ranking 
WHERE strategy_name = 'original' 
AND date = '2024-05-01'
AND trading_pair LIKE 'RANK_TEST%'
ORDER BY rank_position;

-- 9. 檢查批量處理完整性
-- ================================================================
SELECT '9. 批量處理完整性檢查' as check_name;
SELECT 
    date,
    COUNT(DISTINCT trading_pair) as pairs_ranked,
    COUNT(DISTINCT strategy_name) as strategies_applied,
    ROUND(AVG(final_ranking_score), 4) as avg_score,
    ROUND(MAX(final_ranking_score), 4) as max_score,
    ROUND(MIN(final_ranking_score), 4) as min_score
FROM strategy_ranking 
WHERE trading_pair LIKE 'RANK_TEST%'
GROUP BY date 
ORDER BY date;

-- 10. 總體測試結果摘要
-- ================================================================
SELECT '10. 總體測試結果摘要' as check_name;
SELECT 
    '策略排名記錄總數' as metric,
    COUNT(*) as value
FROM strategy_ranking 
WHERE trading_pair LIKE 'RANK_TEST%'
UNION ALL
SELECT 
    '涵蓋策略數量' as metric,
    COUNT(DISTINCT strategy_name) as value
FROM strategy_ranking 
WHERE trading_pair LIKE 'RANK_TEST%'
UNION ALL
SELECT 
    '涵蓋日期數量' as metric,
    COUNT(DISTINCT date) as value
FROM strategy_ranking 
WHERE trading_pair LIKE 'RANK_TEST%'
UNION ALL
SELECT 
    '涵蓋交易對數量' as metric,
    COUNT(DISTINCT trading_pair) as value
FROM strategy_ranking 
WHERE trading_pair LIKE 'RANK_TEST%';

-- ================================================================
-- 快速成功檢查清單
-- ================================================================
SELECT '✅ 快速成功檢查清單' as check_name;

-- 檢查是否有基本的排名記錄
SELECT 
    CASE 
        WHEN COUNT(*) > 0 THEN '✅ 有排名記錄生成'
        ELSE '❌ 沒有排名記錄'
    END as basic_ranking_check
FROM strategy_ranking 
WHERE trading_pair LIKE 'RANK_TEST%';

-- 檢查是否有實驗性策略記錄
SELECT 
    CASE 
        WHEN COUNT(*) > 0 THEN '✅ 實驗性策略正常執行'
        ELSE '❌ 實驗性策略未執行'
    END as experimental_strategy_check
FROM strategy_ranking 
WHERE trading_pair LIKE 'RANK_TEST%' 
AND strategy_name LIKE 'test_%';

-- 檢查排名邏輯是否正確
SELECT 
    CASE 
        WHEN COUNT(*) = 0 THEN '✅ 排名邏輯正確（無錯誤排序）'
        ELSE '❌ 排名邏輯有問題'
    END as ranking_logic_check
FROM (
    SELECT 
        strategy_name, date, trading_pair, rank_position, final_ranking_score,
        LAG(final_ranking_score) OVER (PARTITION BY strategy_name, date ORDER BY rank_position) as prev_score
    FROM strategy_ranking 
    WHERE trading_pair LIKE 'RANK_TEST%'
) t
WHERE prev_score IS NOT NULL AND prev_score < final_ranking_score;

-- ================================================================
-- 使用說明
-- ================================================================
/*
使用方法：
1. 在 DB Browser for SQLite 中開啟 trading_data.db
2. 複製此檔案的 SQL 內容到查詢視窗
3. 執行查詢，檢查結果

預期結果：
- 所有檢查項目都應該有合理的數值
- 沒有重複排名問題
- 排名分數按降序排列
- 實驗性策略正常執行
- 批量處理完整性良好

如果發現問題，請參考 test_strategy_ranking_v2_execution_guide.md 進行詳細診斷。
*/ 