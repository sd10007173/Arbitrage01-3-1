-- ================================================================
-- calculate_FR_return_list_v2 測試數據生成 SQL
-- 用途：為資金費率收益計算程式提供測試數據
-- ================================================================

-- 清理現有測試數據
DELETE FROM funding_rate_diff WHERE symbol LIKE 'TEST%';
DELETE FROM return_metrics WHERE trading_pair LIKE 'TEST%';

-- ================================================================
-- 場景1：基本功能測試 - 單個交易對，連續7天數據
-- ================================================================
-- TEST1/USDT: binance vs bybit, 2024-01-01 到 2024-01-07
-- 設計：每天固定時間點，模擬真實資金費率差異

-- 2024-01-01: 正收益日
INSERT INTO funding_rate_diff (timestamp_utc, symbol, exchange_a, funding_rate_a, exchange_b, funding_rate_b, diff_ab) VALUES
('2024-01-01 00:00:00', 'TEST1/USDT', 'binance', '0.0001', 'bybit', '0.0002', -0.0001),
('2024-01-01 08:00:00', 'TEST1/USDT', 'binance', '0.0003', 'bybit', '0.0001', 0.0002),
('2024-01-01 16:00:00', 'TEST1/USDT', 'binance', '0.0002', 'bybit', '0.0001', 0.0001);
-- 日收益: -0.0001 + 0.0002 + 0.0001 = 0.0002

-- 2024-01-02: 負收益日
INSERT INTO funding_rate_diff (timestamp_utc, symbol, exchange_a, funding_rate_a, exchange_b, funding_rate_b, diff_ab) VALUES
('2024-01-02 00:00:00', 'TEST1/USDT', 'binance', '0.0001', 'bybit', '0.0004', -0.0003),
('2024-01-02 08:00:00', 'TEST1/USDT', 'binance', '0.0002', 'bybit', '0.0003', -0.0001),
('2024-01-02 16:00:00', 'TEST1/USDT', 'binance', '0.0001', 'bybit', '0.0002', -0.0001);
-- 日收益: -0.0003 + (-0.0001) + (-0.0001) = -0.0005

-- 2024-01-03: 零收益日
INSERT INTO funding_rate_diff (timestamp_utc, symbol, exchange_a, funding_rate_a, exchange_b, funding_rate_b, diff_ab) VALUES
('2024-01-03 00:00:00', 'TEST1/USDT', 'binance', '0.0002', 'bybit', '0.0002', 0.0000),
('2024-01-03 08:00:00', 'TEST1/USDT', 'binance', '0.0001', 'bybit', '0.0001', 0.0000),
('2024-01-03 16:00:00', 'TEST1/USDT', 'binance', '0.0003', 'bybit', '0.0003', 0.0000);
-- 日收益: 0.0000

-- 2024-01-04: 大幅正收益日
INSERT INTO funding_rate_diff (timestamp_utc, symbol, exchange_a, funding_rate_a, exchange_b, funding_rate_b, diff_ab) VALUES
('2024-01-04 00:00:00', 'TEST1/USDT', 'binance', '0.0010', 'bybit', '0.0002', 0.0008),
('2024-01-04 08:00:00', 'TEST1/USDT', 'binance', '0.0012', 'bybit', '0.0001', 0.0011),
('2024-01-04 16:00:00', 'TEST1/USDT', 'binance', '0.0009', 'bybit', '0.0003', 0.0006);
-- 日收益: 0.0008 + 0.0011 + 0.0006 = 0.0025

-- 2024-01-05 到 2024-01-07: 穩定小幅收益
INSERT INTO funding_rate_diff (timestamp_utc, symbol, exchange_a, funding_rate_a, exchange_b, funding_rate_b, diff_ab) VALUES
-- 2024-01-05
('2024-01-05 00:00:00', 'TEST1/USDT', 'binance', '0.0003', 'bybit', '0.0002', 0.0001),
('2024-01-05 08:00:00', 'TEST1/USDT', 'binance', '0.0004', 'bybit', '0.0002', 0.0002),
('2024-01-05 16:00:00', 'TEST1/USDT', 'binance', '0.0002', 'bybit', '0.0001', 0.0001),
-- 日收益: 0.0004

-- 2024-01-06
('2024-01-06 00:00:00', 'TEST1/USDT', 'binance', '0.0002', 'bybit', '0.0001', 0.0001),
('2024-01-06 08:00:00', 'TEST1/USDT', 'binance', '0.0003', 'bybit', '0.0001', 0.0002),
('2024-01-06 16:00:00', 'TEST1/USDT', 'binance', '0.0004', 'bybit', '0.0002', 0.0002),
-- 日收益: 0.0005

-- 2024-01-07
('2024-01-07 00:00:00', 'TEST1/USDT', 'binance', '0.0005', 'bybit', '0.0002', 0.0003),
('2024-01-07 08:00:00', 'TEST1/USDT', 'binance', '0.0003', 'bybit', '0.0001', 0.0002),
('2024-01-07 16:00:00', 'TEST1/USDT', 'binance', '0.0002', 'bybit', '0.0001', 0.0001);
-- 日收益: 0.0006

-- ================================================================
-- 場景2：多交易對測試 - 3個交易對，相同時間範圍
-- ================================================================

-- TEST2/USDT: binance vs okx (高收益交易對)
INSERT INTO funding_rate_diff (timestamp_utc, symbol, exchange_a, funding_rate_a, exchange_b, funding_rate_b, diff_ab) VALUES
('2024-01-01 00:00:00', 'TEST2/USDT', 'binance', '0.0005', 'okx', '0.0001', 0.0004),
('2024-01-01 08:00:00', 'TEST2/USDT', 'binance', '0.0008', 'okx', '0.0002', 0.0006),
('2024-01-01 16:00:00', 'TEST2/USDT', 'binance', '0.0006', 'okx', '0.0001', 0.0005),
('2024-01-02 00:00:00', 'TEST2/USDT', 'binance', '0.0007', 'okx', '0.0002', 0.0005),
('2024-01-02 08:00:00', 'TEST2/USDT', 'binance', '0.0009', 'okx', '0.0001', 0.0008),
('2024-01-02 16:00:00', 'TEST2/USDT', 'binance', '0.0004', 'okx', '0.0001', 0.0003);

-- TEST3/USDT: bybit vs okx (低收益交易對)  
INSERT INTO funding_rate_diff (timestamp_utc, symbol, exchange_a, funding_rate_a, exchange_b, funding_rate_b, diff_ab) VALUES
('2024-01-01 00:00:00', 'TEST3/USDT', 'bybit', '0.0002', 'okx', '0.0001', 0.0001),
('2024-01-01 08:00:00', 'TEST3/USDT', 'bybit', '0.0003', 'okx', '0.0002', 0.0001),
('2024-01-01 16:00:00', 'TEST3/USDT', 'bybit', '0.0001', 'okx', '0.0001', 0.0000),
('2024-01-02 00:00:00', 'TEST3/USDT', 'bybit', '0.0002', 'okx', '0.0001', 0.0001),
('2024-01-02 08:00:00', 'TEST3/USDT', 'bybit', '0.0001', 'okx', '0.0001', 0.0000),
('2024-01-02 16:00:00', 'TEST3/USDT', 'bybit', '0.0003', 'okx', '0.0002', 0.0001);

-- ================================================================
-- 場景3：空值處理測試 - 包含NULL值的數據
-- ================================================================

-- TEST4/USDT: 包含NULL值的情況
INSERT INTO funding_rate_diff (timestamp_utc, symbol, exchange_a, funding_rate_a, exchange_b, funding_rate_b, diff_ab) VALUES
-- 正常數據
('2024-01-01 00:00:00', 'TEST4/USDT', 'binance', '0.0003', 'bybit', '0.0001', 0.0002),
-- 包含NULL的差異 (null-null=null的情況)
('2024-01-01 08:00:00', 'TEST4/USDT', 'binance', NULL, 'bybit', NULL, NULL),
-- 正常數據
('2024-01-01 16:00:00', 'TEST4/USDT', 'binance', '0.0002', 'bybit', '0.0001', 0.0001),
-- 有數據-null的情況
('2024-01-02 00:00:00', 'TEST4/USDT', 'binance', '0.0004', 'bybit', NULL, 0.0004),
-- null-有數據的情況
('2024-01-02 08:00:00', 'TEST4/USDT', 'binance', NULL, 'bybit', '0.0002', -0.0002);

-- ================================================================
-- 場景4：滑動窗口測試 - 連續30天數據
-- ================================================================

-- TEST5/USDT: 30天連續數據，用於測試滑動窗口計算
-- 使用簡化的數據模式：每天一筆記錄，遞增模式

INSERT INTO funding_rate_diff (timestamp_utc, symbol, exchange_a, funding_rate_a, exchange_b, funding_rate_b, diff_ab) VALUES
-- 第1-10天：遞增收益 (0.0001 到 0.0010)
('2024-02-01 12:00:00', 'TEST5/USDT', 'binance', '0.0002', 'bybit', '0.0001', 0.0001),
('2024-02-02 12:00:00', 'TEST5/USDT', 'binance', '0.0003', 'bybit', '0.0001', 0.0002),
('2024-02-03 12:00:00', 'TEST5/USDT', 'binance', '0.0004', 'bybit', '0.0001', 0.0003),
('2024-02-04 12:00:00', 'TEST5/USDT', 'binance', '0.0005', 'bybit', '0.0001', 0.0004),
('2024-02-05 12:00:00', 'TEST5/USDT', 'binance', '0.0006', 'bybit', '0.0001', 0.0005),
('2024-02-06 12:00:00', 'TEST5/USDT', 'binance', '0.0007', 'bybit', '0.0001', 0.0006),
('2024-02-07 12:00:00', 'TEST5/USDT', 'binance', '0.0008', 'bybit', '0.0001', 0.0007),
('2024-02-08 12:00:00', 'TEST5/USDT', 'binance', '0.0009', 'bybit', '0.0001', 0.0008),
('2024-02-09 12:00:00', 'TEST5/USDT', 'binance', '0.0010', 'bybit', '0.0001', 0.0009),
('2024-02-10 12:00:00', 'TEST5/USDT', 'binance', '0.0011', 'bybit', '0.0001', 0.0010),

-- 第11-20天：穩定收益 (0.0005)
('2024-02-11 12:00:00', 'TEST5/USDT', 'binance', '0.0006', 'bybit', '0.0001', 0.0005),
('2024-02-12 12:00:00', 'TEST5/USDT', 'binance', '0.0006', 'bybit', '0.0001', 0.0005),
('2024-02-13 12:00:00', 'TEST5/USDT', 'binance', '0.0006', 'bybit', '0.0001', 0.0005),
('2024-02-14 12:00:00', 'TEST5/USDT', 'binance', '0.0006', 'bybit', '0.0001', 0.0005),
('2024-02-15 12:00:00', 'TEST5/USDT', 'binance', '0.0006', 'bybit', '0.0001', 0.0005),
('2024-02-16 12:00:00', 'TEST5/USDT', 'binance', '0.0006', 'bybit', '0.0001', 0.0005),
('2024-02-17 12:00:00', 'TEST5/USDT', 'binance', '0.0006', 'bybit', '0.0001', 0.0005),
('2024-02-18 12:00:00', 'TEST5/USDT', 'binance', '0.0006', 'bybit', '0.0001', 0.0005),
('2024-02-19 12:00:00', 'TEST5/USDT', 'binance', '0.0006', 'bybit', '0.0001', 0.0005),
('2024-02-20 12:00:00', 'TEST5/USDT', 'binance', '0.0006', 'bybit', '0.0001', 0.0005),

-- 第21-30天：遞減收益 (0.0010 到 0.0001)
('2024-02-21 12:00:00', 'TEST5/USDT', 'binance', '0.0011', 'bybit', '0.0001', 0.0010),
('2024-02-22 12:00:00', 'TEST5/USDT', 'binance', '0.0010', 'bybit', '0.0001', 0.0009),
('2024-02-23 12:00:00', 'TEST5/USDT', 'binance', '0.0009', 'bybit', '0.0001', 0.0008),
('2024-02-24 12:00:00', 'TEST5/USDT', 'binance', '0.0008', 'bybit', '0.0001', 0.0007),
('2024-02-25 12:00:00', 'TEST5/USDT', 'binance', '0.0007', 'bybit', '0.0001', 0.0006),
('2024-02-26 12:00:00', 'TEST5/USDT', 'binance', '0.0006', 'bybit', '0.0001', 0.0005),
('2024-02-27 12:00:00', 'TEST5/USDT', 'binance', '0.0005', 'bybit', '0.0001', 0.0004),
('2024-02-28 12:00:00', 'TEST5/USDT', 'binance', '0.0004', 'bybit', '0.0001', 0.0003),
('2024-02-29 12:00:00', 'TEST5/USDT', 'binance', '0.0003', 'bybit', '0.0001', 0.0002),
('2024-03-01 12:00:00', 'TEST5/USDT', 'binance', '0.0002', 'bybit', '0.0001', 0.0001);

-- ================================================================
-- 場景5：增量處理測試 - 分階段數據
-- ================================================================

-- TEST6/USDT: 第一階段數據 (用於測試增量處理)
INSERT INTO funding_rate_diff (timestamp_utc, symbol, exchange_a, funding_rate_a, exchange_b, funding_rate_b, diff_ab) VALUES
('2024-03-01 12:00:00', 'TEST6/USDT', 'binance', '0.0003', 'bybit', '0.0001', 0.0002),
('2024-03-02 12:00:00', 'TEST6/USDT', 'binance', '0.0004', 'bybit', '0.0001', 0.0003),
('2024-03-03 12:00:00', 'TEST6/USDT', 'binance', '0.0005', 'bybit', '0.0001', 0.0004);

-- 注意：TEST6/USDT的第二階段數據將在測試增量處理時手動添加

-- ================================================================
-- 場景6：邊界條件測試
-- ================================================================

-- TEST7/USDT: 單天數據
INSERT INTO funding_rate_diff (timestamp_utc, symbol, exchange_a, funding_rate_a, exchange_b, funding_rate_b, diff_ab) VALUES
('2024-04-01 12:00:00', 'TEST7/USDT', 'binance', '0.0010', 'bybit', '0.0005', 0.0005);

-- TEST8/USDT: 極值數據
INSERT INTO funding_rate_diff (timestamp_utc, symbol, exchange_a, funding_rate_a, exchange_b, funding_rate_b, diff_ab) VALUES
('2024-04-01 00:00:00', 'TEST8/USDT', 'binance', '0.0100', 'bybit', '-0.0050', 0.0150),  -- 極大正值
('2024-04-01 08:00:00', 'TEST8/USDT', 'binance', '-0.0080', 'bybit', '0.0070', -0.0150), -- 極大負值
('2024-04-01 16:00:00', 'TEST8/USDT', 'binance', '0.0000', 'bybit', '0.0000', 0.0000);   -- 零值

-- ================================================================
-- 測試數據摘要
-- ================================================================

-- 查看插入的測試數據統計
SELECT 
    '測試數據插入完成' as status,
    COUNT(*) as total_records,
    COUNT(DISTINCT symbol) as unique_symbols,
    MIN(DATE(timestamp_utc)) as earliest_date,
    MAX(DATE(timestamp_utc)) as latest_date
FROM funding_rate_diff 
WHERE symbol LIKE 'TEST%';

-- 查看各測試場景的數據量
SELECT 
    symbol,
    COUNT(*) as record_count,
    MIN(DATE(timestamp_utc)) as start_date,
    MAX(DATE(timestamp_utc)) as end_date,
    ROUND(AVG(diff_ab), 6) as avg_diff,
    CASE 
        WHEN symbol = 'TEST1/USDT' THEN '場景1: 基本功能測試'
        WHEN symbol = 'TEST2/USDT' THEN '場景2: 多交易對測試(高收益)'
        WHEN symbol = 'TEST3/USDT' THEN '場景2: 多交易對測試(低收益)'
        WHEN symbol = 'TEST4/USDT' THEN '場景3: 空值處理測試'
        WHEN symbol = 'TEST5/USDT' THEN '場景4: 滑動窗口測試'
        WHEN symbol = 'TEST6/USDT' THEN '場景5: 增量處理測試'
        WHEN symbol = 'TEST7/USDT' THEN '場景6: 邊界條件測試(單天)'
        WHEN symbol = 'TEST8/USDT' THEN '場景6: 邊界條件測試(極值)'
    END as description
FROM funding_rate_diff 
WHERE symbol LIKE 'TEST%'
GROUP BY symbol
ORDER BY symbol; 