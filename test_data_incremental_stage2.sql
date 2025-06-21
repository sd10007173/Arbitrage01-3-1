-- ================================================================
-- calculate_FR_return_list_v2 增量處理測試 - 第二階段數據
-- 用途：測試程式的增量處理功能
-- 使用方法：先執行主測試數據，運行程式，再執行此文件，再次運行程式
-- ================================================================

-- TEST6/USDT: 第二階段數據 (用於測試增量處理)
-- 這些數據將在第一階段處理完成後添加，以測試增量處理功能

INSERT INTO funding_rate_diff (timestamp_utc, symbol, exchange_a, funding_rate_a, exchange_b, funding_rate_b, diff_ab) VALUES
-- 繼續TEST6/USDT的數據
('2024-03-04 12:00:00', 'TEST6/USDT', 'binance', '0.0006', 'bybit', '0.0001', 0.0005),
('2024-03-05 12:00:00', 'TEST6/USDT', 'binance', '0.0007', 'bybit', '0.0001', 0.0006),
('2024-03-06 12:00:00', 'TEST6/USDT', 'binance', '0.0008', 'bybit', '0.0001', 0.0007),
('2024-03-07 12:00:00', 'TEST6/USDT', 'binance', '0.0009', 'bybit', '0.0001', 0.0008);

-- 確認第二階段數據已添加
SELECT 
    '第二階段數據添加完成' as status,
    symbol,
    COUNT(*) as total_records,
    MIN(DATE(timestamp_utc)) as start_date,
    MAX(DATE(timestamp_utc)) as end_date
FROM funding_rate_diff 
WHERE symbol = 'TEST6/USDT'
GROUP BY symbol; 