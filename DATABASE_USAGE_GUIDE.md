# 🗄️ SQLite 數據庫使用指南

## 📋 概述

恭喜！您的加密貨幣資金費率套利系統已經成功整合了 SQLite 數據庫！這將大幅提升數據管理效率、查詢性能，並為未來的功能擴展打下堅實基礎。

## 🏗️ 架構說明

### 核心文件
- `database_schema.py` - 數據庫架構定義
- `database_operations.py` - 數據庫操作管理  
- `csv_to_database_migration.py` - CSV 遷移工具

### 數據庫表結構
1. **funding_rate_history** - 資金費率歷史數據
2. **funding_rate_diff** - 資金費率差異數據
3. **return_metrics** - 收益指標數據
4. **strategy_ranking** - 策略排行榜數據
5. **backtest_results** - 回測結果數據
6. **backtest_trades** - 回測交易明細

## 🚀 快速開始

### 1. 完整數據遷移
```bash
# 運行遷移工具，遷移所有 CSV 數據到數據庫
python csv_to_database_migration.py
# 選擇選項 1: 遷移所有數據
```

### 2. 基本使用示例
```python
from database_operations import DatabaseManager

# 初始化數據庫管理器
db = DatabaseManager()

# 查詢最新策略排行榜前10名
ranking = db.get_latest_ranking('original', top_n=10)
print(ranking[['trading_pair', 'final_ranking_score', 'rank_position']])

# 查詢特定交易對的績效趨勢
trend = db.get_trading_pair_performance_trend('BTCUSDT_binance_bybit', days=30)
print(trend[['date', 'roi_1d', 'roi_7d']])
```

## 📊 數據操作指南

### 插入新資金費率數據
```python
import pandas as pd
from database_operations import DatabaseManager

db = DatabaseManager()

# 假設你有新的資金費率數據
df = pd.read_csv('new_funding_rate_data.csv')
records_inserted = db.insert_funding_rate_history(df)
print(f"插入了 {records_inserted} 條記錄")
```

### 插入策略排行榜數據
```python
# 插入新的策略排行榜結果
ranking_df = pd.read_csv('new_ranking.csv')
db.insert_strategy_ranking(ranking_df, strategy_name='momentum_focused')
```

### 查詢和分析
```python
# 比較不同策略在同一日期的表現
comparison = db.compare_strategies('2024-06-01', top_n=10)

# 獲取策略回測摘要統計
summary = db.get_strategy_backtest_summary('original')
print(summary)
```

## 🔄 整合現有程式

### 修改現有 Python 腳本

將現有的 CSV 讀取操作替換為數據庫查詢：

**原來的方式：**
```python
# 舊的 CSV 讀取方式
df = pd.read_csv('csv/strategy_ranking/original_ranking_2024-06-01.csv')
```

**新的數據庫方式：**
```python
# 新的數據庫查詢方式
from database_operations import DatabaseManager
db = DatabaseManager()
df = db.get_strategy_ranking('original', date='2024-06-01')
```

### 修改範例：ranking_engine.py

在 `ranking_engine.py` 中添加數據庫輸出：

```python
from database_operations import DatabaseManager

class RankingEngine:
    def __init__(self):
        self.db = DatabaseManager()  # 添加數據庫管理器
    
    def save_ranking_results(self, df, strategy_name, date):
        """保存排行榜結果到數據庫和 CSV"""
        # 原有的 CSV 保存邏輯...
        df.to_csv(f'csv/strategy_ranking/{strategy_name}_ranking_{date}.csv')
        
        # 新增：同時保存到數據庫
        self.db.insert_strategy_ranking(df, strategy_name)
        print(f"✅ 排行榜結果已保存到數據庫: {strategy_name}")
```

## 🛠️ 常用查詢範例

### 1. 獲取最佳表現的交易對
```python
# 查詢 ROI 最高的前10個交易對
with db.get_connection() as conn:
    top_performers = pd.read_sql_query("""
        SELECT trading_pair, AVG(roi_30d) as avg_roi_30d
        FROM return_metrics 
        WHERE date >= date('now', '-30 days')
        GROUP BY trading_pair
        ORDER BY avg_roi_30d DESC
        LIMIT 10
    """, conn)
```

### 2. 策略效果對比
```python
# 比較不同策略的平均排名
with db.get_connection() as conn:
    strategy_comparison = pd.read_sql_query("""
        SELECT 
            strategy_name,
            AVG(final_ranking_score) as avg_score,
            COUNT(*) as total_records
        FROM strategy_ranking 
        WHERE date >= date('now', '-7 days')
        GROUP BY strategy_name
        ORDER BY avg_score DESC
    """, conn)
```

### 3. 資金費率趨勢分析
```python
# 分析特定交易對的資金費率趨勢
symbol = 'BTCUSDT'
with db.get_connection() as conn:
    trend = pd.read_sql_query("""
        SELECT 
            DATE(timestamp_utc) as date,
            exchange,
            AVG(funding_rate) as avg_funding_rate
        FROM funding_rate_history 
        WHERE symbol = ? AND timestamp_utc >= date('now', '-30 days')
        GROUP BY DATE(timestamp_utc), exchange
        ORDER BY date, exchange
    """, conn, params=[symbol])
```

## 🔧 維護和優化

### 定期數據庫維護
```python
# 清理超過3個月的舊數據
db.cleanup_old_data(days_to_keep=90)

# 優化數據庫性能
db.vacuum_database()
```

### 數據庫備份
```bash
# 備份數據庫
cp data/funding_rate.db data/backup_funding_rate_$(date +%Y%m%d).db

# 查看數據庫大小
ls -lh data/funding_rate.db
```

## 📈 性能優化建議

### 1. 批量插入
```python
# 對於大量數據，使用批量插入
large_df = pd.read_csv('large_dataset.csv')
db.insert_funding_rate_history(large_df, batch_size=5000)
```

### 2. 使用索引查詢
```python
# 利用已建立的索引進行快速查詢
# 按 symbol + exchange 查詢（有索引）
history = db.get_funding_rate_history(symbol='BTCUSDT', exchange='binance')
```

### 3. 限制查詢結果
```python
# 使用 limit 參數避免返回過多數據
recent_data = db.get_funding_rate_history(limit=1000)
```

## 🚨 注意事項

### 數據一致性
- 數據庫使用 `INSERT OR REPLACE` 避免重複記錄
- 時間戳格式統一為 UTC
- 外鍵約束確保數據完整性

### 錯誤處理
```python
try:
    result = db.insert_funding_rate_history(df)
    print(f"成功插入 {result} 條記錄")
except Exception as e:
    print(f"插入失敗: {e}")
    # 處理錯誤邏輯
```

### 記憶體管理
```python
# 處理大數據集時，使用 chunking
def process_large_dataset(file_path, chunk_size=10000):
    for chunk in pd.read_csv(file_path, chunksize=chunk_size):
        db.insert_funding_rate_history(chunk)
```

## 🎯 下一步建議

1. **逐步遷移**: 先遷移核心數據，測試穩定後再遷移全部
2. **修改腳本**: 逐一更新現有 Python 腳本使用數據庫
3. **監控性能**: 觀察查詢性能，必要時添加更多索引
4. **建立備份**: 設置定期數據庫備份機制
5. **擴展功能**: 考慮添加更多高級查詢和分析功能

## 📞 支援

如有任何問題或需要進一步功能開發，請查看：
- `database_schema.py` - 了解數據庫結構
- `database_operations.py` - 查看可用的操作方法
- `demo_migration.py` - 參考使用範例

**祝您使用愉快！您的套利系統現在擁有了強大的數據庫支持！** 🚀 