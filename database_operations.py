"""
加密貨幣資金費率套利系統 - 數據庫操作管理
提供完整的數據庫 CRUD 操作和復雜查詢方法
"""

import pandas as pd
import sqlite3
from typing import List, Dict, Any, Optional, Union
from database_schema import FundingRateDB
import json
from datetime import datetime, timedelta
import uuid

class DatabaseManager(FundingRateDB):
    """數據庫操作管理類，繼承自 FundingRateDB"""
    
    def __init__(self, db_path="data/funding_rate.db"):
        super().__init__(db_path)
        self.batch_size = 1000  # 默認批處理大小
    
    # ==================== 資金費率歷史數據操作 ====================
    
    def insert_funding_rate_history(self, df: pd.DataFrame) -> int:
        """插入資金費率歷史數據"""
        if df.empty:
            print("⚠️ DataFrame 為空，跳過插入")
            return 0
            
        with self.get_connection() as conn:
            data_to_insert = []
            
            for _, row in df.iterrows():
                funding_rate = None
                
                # 支持多種列名格式
                if 'funding_rate' in row:
                    funding_rate = row['funding_rate']
                    # 如果是字符串"null"或空字符串，轉為None
                    if funding_rate == "null" or funding_rate == "":
                        funding_rate = None
                    elif funding_rate is not None:
                        try:
                            funding_rate = float(funding_rate)
                        except (ValueError, TypeError):
                            funding_rate = None
                elif 'FundingRate' in row:
                    funding_rate_str = str(row['FundingRate'])
                    if funding_rate_str == "null" or funding_rate_str == "":
                        funding_rate = None
                    else:
                        try:
                            funding_rate = float(funding_rate_str)
                        except (ValueError, TypeError):
                            funding_rate = None
                
                # 處理時間戳，確保轉換為字符串格式
                timestamp = row.get('timestamp_utc') or row.get('Timestamp (UTC)')
                if pd.notna(timestamp):
                    timestamp = pd.to_datetime(timestamp).strftime('%Y-%m-%d %H:%M:%S')
                
                data_to_insert.append((
                    timestamp,
                    row.get('symbol') or row.get('Symbol'),
                    row.get('exchange') or row.get('Exchange'),
                    funding_rate
                ))
            
            conn.executemany('''
                INSERT OR REPLACE INTO funding_rate_history 
                (timestamp_utc, symbol, exchange, funding_rate)
                VALUES (?, ?, ?, ?)
            ''', data_to_insert)
            
            # 明確提交事務
            conn.commit()
            
            print(f"✅ 插入資金費率歷史數據: {len(data_to_insert)} 條")
            return len(data_to_insert)
    
    def get_funding_rate_history(self, symbol: str = None, exchange: str = None, 
                               start_date: str = None, end_date: str = None, 
                               limit: int = None) -> pd.DataFrame:
        """
        查詢資金費率歷史數據
        
        Args:
            symbol: 交易對符號
            exchange: 交易所名稱
            start_date: 開始日期
            end_date: 結束日期
            limit: 限制返回記錄數
            
        Returns:
            查詢結果 DataFrame
        """
        query = "SELECT * FROM funding_rate_history WHERE 1=1"
        params = []
        
        if symbol:
            query += " AND symbol = ?"
            params.append(symbol)
        if exchange:
            query += " AND exchange = ?"
            params.append(exchange)
        if start_date:
            query += " AND timestamp_utc >= ?"
            params.append(start_date)
        if end_date:
            query += " AND timestamp_utc <= ?"
            params.append(end_date)
            
        query += " ORDER BY timestamp_utc"
        
        if limit:
            query += f" LIMIT {limit}"
        
        return pd.read_sql_query(query, self.get_connection(), params=params)
    
    # ==================== 資金費率差異數據操作 ====================
    
    def insert_funding_rate_diff(self, df: pd.DataFrame) -> int:
        """插入資金費率差異數據"""
        if df.empty:
            return 0
            
        with self.get_connection() as conn:
            data_to_insert = []
            
            for _, row in df.iterrows():
                # 處理時間戳，確保轉換為字符串格式
                timestamp = row.get('timestamp_utc') or row.get('Timestamp (UTC)')
                if pd.notna(timestamp):
                    timestamp = pd.to_datetime(timestamp).strftime('%Y-%m-%d %H:%M:%S')
                
                # 處理 diff_ab，確保不為 None
                diff_ab = row.get('diff_ab') or row.get('Diff_AB')
                if pd.isna(diff_ab):
                    diff_ab = 0.0
                
                data_to_insert.append((
                    timestamp,
                    row.get('symbol') or row.get('Symbol'),
                    row.get('exchange_a') or row.get('Exchange_A'),
                    row.get('funding_rate_a') or str(row.get('FundingRate_A', '')),
                    row.get('exchange_b') or row.get('Exchange_B'),
                    row.get('funding_rate_b') or str(row.get('FundingRate_B', '')),
                    diff_ab
                ))
            
            conn.executemany('''
                INSERT OR REPLACE INTO funding_rate_diff 
                (timestamp_utc, symbol, exchange_a, funding_rate_a, exchange_b, funding_rate_b, diff_ab)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', data_to_insert)
            
            # 明確提交事務
            conn.commit()
            
            print(f"✅ 插入資金費率差異數據: {len(data_to_insert)} 條")
            return len(data_to_insert)
    
    def get_funding_rate_diff(self, symbol: str = None, start_date: str = None, 
                            end_date: str = None, exchange_a: str = None, 
                            exchange_b: str = None) -> pd.DataFrame:
        """查詢資金費率差異數據"""
        query = "SELECT * FROM funding_rate_diff WHERE 1=1"
        params = []
        
        if symbol:
            query += " AND symbol = ?"
            params.append(symbol)
        if exchange_a:
            query += " AND exchange_a = ?"
            params.append(exchange_a)
        if exchange_b:
            query += " AND exchange_b = ?"
            params.append(exchange_b)
        if start_date:
            query += " AND timestamp_utc >= ?"
            params.append(start_date)
        if end_date:
            query += " AND timestamp_utc <= ?"
            params.append(end_date)
            
        query += " ORDER BY timestamp_utc"
        
        return pd.read_sql_query(query, self.get_connection(), params=params)
    
    # ==================== 收益指標數據操作 ====================
    
    def insert_return_metrics(self, df: pd.DataFrame) -> int:
        """插入收益指標數據"""
        if df.empty:
            return 0
            
        with self.get_connection() as conn:
            data_to_insert = []
            
            for _, row in df.iterrows():
                data_to_insert.append((
                    row['Trading_Pair'] if 'Trading_Pair' in row else row.get('trading_pair'),
                    row['Date'] if 'Date' in row else row.get('date'),
                    row.get('return_1d'),
                    row.get('roi_1d'),
                    row.get('return_2d'),
                    row.get('roi_2d'),
                    row.get('return_7d'),
                    row.get('roi_7d'),
                    row.get('return_14d'),
                    row.get('roi_14d'),
                    row.get('return_30d'),
                    row.get('roi_30d'),
                    row.get('return_all'),
                    row.get('roi_all')
                ))
            
            conn.executemany('''
                INSERT OR REPLACE INTO return_metrics 
                (trading_pair, date, return_1d, roi_1d, return_2d, roi_2d, 
                 return_7d, roi_7d, return_14d, roi_14d, return_30d, roi_30d, 
                 return_all, roi_all)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', data_to_insert)
            
            # 明確提交事務
            conn.commit()
            
            print(f"✅ 插入收益指標數據: {len(data_to_insert)} 條")
            return len(data_to_insert)
    
    def get_return_metrics(self, trading_pair: str = None, start_date: str = None, 
                         end_date: str = None, date: str = None) -> pd.DataFrame:
        """查詢收益指標數據"""
        query = "SELECT * FROM return_metrics WHERE 1=1"
        params = []
        
        if trading_pair:
            query += " AND trading_pair = ?"
            params.append(trading_pair)
        if date:
            query += " AND date = ?"
            params.append(date)
        elif start_date and end_date:
            query += " AND date BETWEEN ? AND ?"
            params.extend([start_date, end_date])
        elif start_date:
            query += " AND date >= ?"
            params.append(start_date)
        elif end_date:
            query += " AND date <= ?"
            params.append(end_date)
            
        query += " ORDER BY date DESC"
        
        return pd.read_sql_query(query, self.get_connection(), params=params)
    
    # ==================== 策略排行榜數據操作 ====================
    
    def insert_strategy_ranking(self, df: pd.DataFrame, strategy_name: str) -> int:
        """插入策略排行榜數據"""
        if df.empty:
            return 0
            
        with self.get_connection() as conn:
            data_to_insert = []
            
            for _, row in df.iterrows():
                component_scores = {}
                for col in df.columns:
                    if col.endswith('_score') and col not in ['final_ranking_score', 'long_term_score_score', 'short_term_score_score']:
                        component_scores[col] = row.get(col)
                
                data_to_insert.append((
                    strategy_name,
                    row.get('Trading_Pair', row.get('trading_pair')),
                    row['Date'] if 'Date' in row else row.get('date'),
                    row.get('final_ranking_score'),
                    row.get('Rank', row.get('rank_position')),
                    row.get('long_term_score_score', row.get('all_ROI_Z_score')),
                    row.get('short_term_score_score', row.get('short_ROI_z_score')),
                    row.get('combined_ROI_z_score'),
                    row.get('final_combination_value'),
                    json.dumps(component_scores) if component_scores else None
                ))
            
            conn.executemany('''
                INSERT OR REPLACE INTO strategy_ranking 
                (strategy_name, trading_pair, date, final_ranking_score, rank_position,
                 long_term_score, short_term_score, combined_roi_z_score, 
                 final_combination_value, component_scores)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', data_to_insert)
            
            # 明確提交事務
            conn.commit()
            
            print(f"✅ 插入策略排行榜數據 ({strategy_name}): {len(data_to_insert)} 條")
            return len(data_to_insert)
    
    def get_strategy_ranking(self, strategy_name: str, date: str = None, top_n: int = None) -> pd.DataFrame:
        """查詢策略排行榜數據"""
        query = "SELECT * FROM strategy_ranking WHERE strategy_name = ?"
        params = [strategy_name]
        
        if date:
            query += " AND date = ?"
            params.append(date)
            
        query += " ORDER BY rank_position"
        
        if top_n:
            query += f" LIMIT {top_n}"
        
        return pd.read_sql_query(query, self.get_connection(), params=params)
    
    def get_latest_ranking(self, strategy_name: str, top_n: int = 10) -> pd.DataFrame:
        """獲取最新的策略排行榜"""
        query = """
        SELECT sr.*, rm.roi_1d, rm.roi_7d, rm.roi_30d
        FROM strategy_ranking sr
        LEFT JOIN return_metrics rm ON sr.trading_pair = rm.trading_pair AND sr.date = rm.date
        WHERE sr.strategy_name = ? 
        AND sr.date = (
            SELECT MAX(date) FROM strategy_ranking 
            WHERE strategy_name = ?
        )
        ORDER BY sr.rank_position
        LIMIT ?
        """
        
        return pd.read_sql_query(query, self.get_connection(), params=[strategy_name, strategy_name, top_n])
    
    def get_available_strategies(self) -> List[str]:
        """獲取數據庫中所有可用的策略名稱"""
        query = "SELECT DISTINCT strategy_name FROM strategy_ranking ORDER BY strategy_name"
        
        with self.get_connection() as conn:
            cursor = conn.execute(query)
            strategies = [row[0] for row in cursor.fetchall()]
            
        return strategies
    
    # ==================== 回測結果數據操作 ====================
    
    def insert_backtest_result(self, strategy_name: str, start_date: str, end_date: str, 
                             config: Dict[str, Any], results: Dict[str, Any], 
                             backtest_id: str = None) -> str:
        """
        插入回測結果
        
        Args:
            strategy_name: 策略名稱
            start_date: 回測開始日期
            end_date: 回測結束日期
            config: 回測配置參數
            results: 回測結果
            backtest_id: 可選的回測ID，如果不提供則自動生成
            
        Returns:
            backtest_id: 回測唯一標識
        """
        # 如果沒有提供backtest_id，則生成唯一的回測ID
        if backtest_id is None:
            backtest_id = f"{strategy_name}_{start_date}_{end_date}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        with self.get_connection() as conn:
            conn.execute('''
                INSERT OR REPLACE INTO backtest_results 
                (backtest_id, strategy_name, start_date, end_date, 
                 initial_capital, position_size, fee_rate, max_positions, 
                 entry_top_n, exit_threshold, final_balance, total_return, 
                 max_drawdown, win_rate, total_trades, profit_days, 
                 loss_days, avg_holding_days, sharpe_ratio, config_params, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                backtest_id,
                strategy_name,
                start_date,
                end_date,
                config.get('initial_capital'),
                config.get('position_size'),
                config.get('fee_rate'),
                config.get('max_positions'),
                config.get('entry_top_n'),
                config.get('exit_threshold'),
                results.get('final_balance'),
                results.get('total_return'),
                results.get('max_drawdown'),
                results.get('win_rate'),
                results.get('total_trades'),
                results.get('profit_days'),
                results.get('loss_days'),
                results.get('avg_holding_days'),
                results.get('sharpe_ratio'),
                json.dumps(config),
                results.get('notes')
            ))
            
            print(f"✅ 插入回測結果: {backtest_id}")
            return backtest_id
    
    def insert_backtest_trades(self, backtest_id: str, trades_data: List[Dict]) -> int:
        """插入回測交易明細"""
        if not trades_data:
            return 0
            
        with self.get_connection() as conn:
            data_to_insert = []
            
            for trade in trades_data:
                data_to_insert.append((
                    backtest_id,
                    trade.get('trade_date'),
                    trade.get('trading_pair'),
                    trade.get('action'),
                    trade.get('amount'),
                    trade.get('funding_rate_diff'),
                    trade.get('position_balance'),
                    trade.get('cash_balance'),
                    trade.get('total_balance'),
                    trade.get('rank_position'),
                    trade.get('notes')
                ))
            
            conn.executemany('''
                INSERT INTO backtest_trades 
                (backtest_id, trade_date, trading_pair, action, amount, 
                 funding_rate_diff, position_balance, cash_balance, total_balance,
                 rank_position, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', data_to_insert)
            
            print(f"✅ 插入回測交易明細: {len(data_to_insert)} 條")
            return len(data_to_insert)
    
    def get_backtest_results(self, strategy_name: str = None, 
                           start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """查詢回測結果"""
        query = "SELECT * FROM backtest_results WHERE 1=1"
        params = []
        
        if strategy_name:
            query += " AND strategy_name = ?"
            params.append(strategy_name)
        if start_date:
            query += " AND created_at >= ?"
            params.append(start_date)
        if end_date:
            query += " AND created_at <= ?"
            params.append(end_date)
            
        query += " ORDER BY created_at DESC"
        
        return pd.read_sql_query(query, self.get_connection(), params=params)
    
    def get_backtest_trades(self, backtest_id: str) -> pd.DataFrame:
        """查詢特定回測的交易明細"""
        query = "SELECT * FROM backtest_trades WHERE backtest_id = ? ORDER BY trade_date"
        return pd.read_sql_query(query, self.get_connection(), params=[backtest_id])
    
    # ==================== 高級查詢和分析方法 ====================
    
    def compare_strategies(self, date: str, top_n: int = 10) -> pd.DataFrame:
        """比較不同策略在同一日期的表現"""
        query = """
        SELECT 
            strategy_name,
            trading_pair,
            rank_position,
            final_ranking_score,
            long_term_score,
            short_term_score
        FROM strategy_ranking 
        WHERE date = ? AND rank_position <= ?
        ORDER BY strategy_name, rank_position
        """
        
        return pd.read_sql_query(query, self.get_connection(), params=[date, top_n])
    
    def get_trading_pair_performance_trend(self, trading_pair: str, days: int = 30) -> pd.DataFrame:
        """獲取交易對的績效趨勢"""
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        
        query = """
        SELECT date, roi_1d, roi_7d, roi_30d, roi_all
        FROM return_metrics 
        WHERE trading_pair = ? AND date BETWEEN ? AND ?
        ORDER BY date
        """
        
        return pd.read_sql_query(query, self.get_connection(), params=[trading_pair, start_date, end_date])
    
    def get_strategy_backtest_summary(self, strategy_name: str) -> Dict[str, Any]:
        """獲取策略的回測摘要統計"""
        with self.get_connection() as conn:
            query = """
            SELECT 
                COUNT(*) as total_backtests,
                AVG(total_return) as avg_return,
                MAX(total_return) as best_return,
                MIN(total_return) as worst_return,
                AVG(max_drawdown) as avg_drawdown,
                MIN(max_drawdown) as best_drawdown,
                AVG(win_rate) as avg_win_rate,
                AVG(sharpe_ratio) as avg_sharpe
            FROM backtest_results 
            WHERE strategy_name = ?
            """
            
            result = conn.execute(query, [strategy_name]).fetchone()
            
            if result:
                return {
                    'strategy_name': strategy_name,
                    'total_backtests': result[0],
                    'avg_return': round(result[1], 4) if result[1] else None,
                    'best_return': round(result[2], 4) if result[2] else None,
                    'worst_return': round(result[3], 4) if result[3] else None,
                    'avg_drawdown': round(result[4], 4) if result[4] else None,
                    'best_drawdown': round(result[5], 4) if result[5] else None,
                    'avg_win_rate': round(result[6], 4) if result[6] else None,
                    'avg_sharpe': round(result[7], 4) if result[7] else None
                }
            else:
                return {'strategy_name': strategy_name, 'message': '無回測記錄'}
    
    # ==================== 市值數據操作 ====================
    
    def insert_market_caps(self, df: pd.DataFrame) -> int:
        """插入市值數據"""
        if df.empty:
            return 0
            
        with self.get_connection() as conn:
            data_to_insert = []
            
            for _, row in df.iterrows():
                data_to_insert.append((
                    row.get('symbol'),
                    row.get('name'),
                    row.get('current_price'),
                    row.get('market_cap'),
                    row.get('market_cap_rank'),
                    row.get('total_volume'),
                    row.get('price_change_24h'),
                    row.get('price_change_percentage_24h'),
                    row.get('circulating_supply'),
                    row.get('total_supply'),
                    row.get('max_supply'),
                    row.get('ath'),
                    row.get('ath_change_percentage'),
                    row.get('ath_date'),
                    row.get('atl'),
                    row.get('atl_change_percentage'),
                    row.get('atl_date')
                ))
            
            conn.executemany('''
                INSERT OR REPLACE INTO market_caps 
                (symbol, name, current_price, market_cap, market_cap_rank, total_volume,
                 price_change_24h, price_change_percentage_24h, circulating_supply,
                 total_supply, max_supply, ath, ath_change_percentage, ath_date,
                 atl, atl_change_percentage, atl_date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', data_to_insert)
            
            print(f"✅ 插入市值數據: {len(data_to_insert)} 條")
            return len(data_to_insert)
    
    def get_market_caps(self, symbol: str = None, top_n: int = None) -> pd.DataFrame:
        """查詢市值數據"""
        query = "SELECT * FROM market_caps WHERE 1=1"
        params = []
        
        if symbol:
            query += " AND symbol = ?"
            params.append(symbol)
            
        query += " ORDER BY market_cap_rank"
        
        if top_n:
            query += f" LIMIT {top_n}"
        
        return pd.read_sql_query(query, self.get_connection(), params=params)
    
    # ==================== 交易對數據操作 ====================
    
    def insert_trading_pairs(self, df: pd.DataFrame) -> int:
        """插入交易對數據"""
        if df.empty:
            return 0
            
        with self.get_connection() as conn:
            data_to_insert = []
            
            for _, row in df.iterrows():
                data_to_insert.append((
                    row.get('Symbol') or row.get('symbol'),
                    row.get('Exchange_A') or row.get('exchange_a'),
                    row.get('Exchange_B') or row.get('exchange_b'),
                    row.get('Market_Cap') or row.get('market_cap'),
                    row.get('FR_Date') or row.get('fr_date')
                ))
            
            conn.executemany('''
                INSERT OR REPLACE INTO trading_pairs 
                (symbol, exchange_a, exchange_b, market_cap, fr_date)
                VALUES (?, ?, ?, ?, ?)
            ''', data_to_insert)
            
            print(f"✅ 插入交易對數據: {len(data_to_insert)} 條")
            return len(data_to_insert)
    
    def get_trading_pairs(self, symbol: str = None, min_market_cap: float = None) -> pd.DataFrame:
        """查詢交易對數據"""
        query = "SELECT * FROM trading_pairs WHERE 1=1"
        params = []
        
        if symbol:
            query += " AND symbol = ?"
            params.append(symbol)
        if min_market_cap:
            query += " AND market_cap >= ?"
            params.append(min_market_cap)
            
        query += " ORDER BY market_cap DESC"
        
        return pd.read_sql_query(query, self.get_connection(), params=params)
    
    def cleanup_old_data(self, days_to_keep: int = 90):
        """清理舊數據（可選功能）"""
        cutoff_date = (datetime.now() - timedelta(days=days_to_keep)).strftime('%Y-%m-%d')
        
        with self.get_connection() as conn:
            # 清理舊的收益指標數據
            result = conn.execute("DELETE FROM return_metrics WHERE date < ?", [cutoff_date])
            deleted_metrics = result.rowcount
            
            # 清理舊的策略排行榜數據
            result = conn.execute("DELETE FROM strategy_ranking WHERE date < ?", [cutoff_date])
            deleted_rankings = result.rowcount
            
            print(f"🧹 清理完成: 刪除了 {deleted_metrics} 條收益數據, {deleted_rankings} 條排行榜數據")
            
            # 優化數據庫
            self.vacuum_database()

    def vacuum_database(self):
        """執行 SQLite 數據庫真空處理"""
        with self.get_connection() as conn:
            result = conn.execute("VACUUM")
            records_deleted = result.rowcount
            
            print(f"✅ 數據庫維護完成")
            
            return {
                'records_deleted': records_deleted,
                'disk_space_saved': 'unknown'  # SQLite vacuum 不返回節省的空間信息
            }

if __name__ == "__main__":
    # 測試數據庫操作
    db = DatabaseManager()
    
    # 顯示數據庫信息
    info = db.get_database_info()
    print("\n📊 數據庫狀態:")
    for table, count in info['tables'].items():
        print(f"  {table}: {count} 條記錄")
    
    # 測試查詢功能（如果有數據的話）
    print("\n🔍 測試查詢功能...")
    try:
        latest_rankings = db.get_latest_ranking('original', top_n=5)
        if not latest_rankings.empty:
            print("最新 original 策略前5名:")
            print(latest_rankings[['trading_pair', 'final_ranking_score', 'rank_position']])
        else:
            print("暫無排行榜數據")
    except Exception as e:
        print(f"查詢測試: {e}") 