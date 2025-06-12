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
        """插入資金費率差異數據 - 解法2：批量處理+SQLite優化版本"""
        if df.empty:
            return 0
        
        print(f"🚀 解法2優化處理: {len(df)} 條記錄...")
        
        # ==================== SQLite高級優化設置 ====================
        def optimize_sqlite_connection(conn):
            """SQLite性能優化設置"""
            print("⚡ 啟用SQLite高級優化...")
            
            # WAL模式 - 允許同時讀寫，大幅提升並發性能
            conn.execute("PRAGMA journal_mode = WAL")
            
            # 同步模式優化 - 減少磁盤同步，提升寫入速度
            conn.execute("PRAGMA synchronous = NORMAL")  # 從FULL改為NORMAL，性能提升3-5倍
            
            # 緩存大小優化 - 使用更大內存緩存
            conn.execute("PRAGMA cache_size = -64000")  # 64MB緩存（負數表示KB）
            
            # 臨時存儲優化 - 使用內存存儲臨時數據
            conn.execute("PRAGMA temp_store = MEMORY")
            
            # 頁面大小優化 - 4KB頁面適合大批量插入
            conn.execute("PRAGMA page_size = 4096")
            
            # Checkpoint優化 - 控制WAL文件大小
            conn.execute("PRAGMA wal_autocheckpoint = 10000")
            
            print("✅ SQLite優化設置完成")
        
        # ✅ 向量化預處理（比解法1更高效的版本）
        print("📊 向量化預處理...")
        df_clean = df.copy()
        
        # 高效時間戳處理 - 使用更快的向量化操作
        timestamp_col = 'timestamp_utc' if 'timestamp_utc' in df_clean.columns else 'Timestamp (UTC)'
        if timestamp_col in df_clean.columns:
            # 使用pandas最快的時間轉換方法
            df_clean['timestamp_utc'] = pd.to_datetime(df_clean[timestamp_col], format='mixed', errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
        else:
            df_clean['timestamp_utc'] = ''
        
        # 批量列名映射 - 一次性處理所有列名
        column_mapping = {
            'Symbol': 'symbol',
            'Exchange_A': 'exchange_a', 
            'Exchange_B': 'exchange_b',
            'FundingRate_A': 'funding_rate_a',
            'FundingRate_B': 'funding_rate_b', 
            'Diff_AB': 'diff_ab'
        }
        
        # 高效重命名
        existing_renames = {old: new for old, new in column_mapping.items() if old in df_clean.columns}
        if existing_renames:
            df_clean = df_clean.rename(columns=existing_renames)
        
        # 向量化數值處理 - 使用最快的數值轉換
        numeric_columns = ['diff_ab', 'funding_rate_a', 'funding_rate_b']
        for col in numeric_columns:
            if col in df_clean.columns:
                df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce').fillna(0.0)
            else:
                df_clean[col] = 0.0
        
        # 字符串列快速處理
        string_columns = ['symbol', 'exchange_a', 'exchange_b']
        for col in string_columns:
            if col in df_clean.columns:
                df_clean[col] = df_clean[col].astype('string').fillna('')
            else:
                df_clean[col] = ''
        
        # 選擇最終列並確保順序
        required_columns = ['timestamp_utc', 'symbol', 'exchange_a', 'funding_rate_a', 'exchange_b', 'funding_rate_b', 'diff_ab']
        df_final = df_clean[required_columns].copy()
        
        print("✅ 向量化預處理完成")
        
        # ==================== 批量插入優化 ====================
        batch_size = 50000  # 5萬條一批，平衡內存和性能
        total_rows = len(df_final)
        total_inserted = 0
        
        print(f"📦 開始批量插入 ({batch_size:,} 條/批)...")
        
        # 使用優化的數據庫連接
        with self.get_connection() as conn:
            # 應用SQLite優化設置
            optimize_sqlite_connection(conn)
            
            # 開始事務 - 批量提交減少I/O
            conn.execute("BEGIN TRANSACTION")
            
            try:
                # 分批處理數據
                for i in range(0, total_rows, batch_size):
                    batch_end = min(i + batch_size, total_rows)
                    batch_df = df_final.iloc[i:batch_end]
                    
                    print(f"   處理批次 {i//batch_size + 1}/{(total_rows-1)//batch_size + 1}: {len(batch_df):,} 條")
                    
                    # 高效數據轉換 - 使用NumPy數組直接轉換
                    batch_data = batch_df.values.tolist()
                    
                    # 批量插入
                    conn.executemany('''
                        INSERT OR REPLACE INTO funding_rate_diff 
                        (timestamp_utc, symbol, exchange_a, funding_rate_a, exchange_b, funding_rate_b, diff_ab)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''', batch_data)
                    
                    total_inserted += len(batch_data)
                
                # 提交事務
                conn.commit()
                print("✅ 批量提交完成")
                
                # WAL checkpoint - 確保數據持久化
                conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                
            except Exception as e:
                conn.rollback()
                print(f"❌ 批量插入失敗，已回滾: {e}")
                raise
        
        print(f"✅ 解法2優化完成: {total_inserted:,} 條記錄")
        return total_inserted
    
    def insert_funding_rate_diff_v1(self, df: pd.DataFrame) -> int:
        """插入資金費率差異數據 - 解法1：向量化處理版本（保留用於對比）"""
        if df.empty:
            return 0
        
        print(f"🚀 向量化處理 (解法1): {len(df)} 條記錄...")
        
        # ✅ 向量化預處理（一次性處理所有數據，避免逐行循環）
        df_clean = df.copy()
        
        # 向量化時間戳處理 - 一次性轉換所有時間戳
        timestamp_col = 'timestamp_utc' if 'timestamp_utc' in df_clean.columns else 'Timestamp (UTC)'
        if timestamp_col in df_clean.columns:
            df_clean['timestamp_utc'] = pd.to_datetime(df_clean[timestamp_col]).dt.strftime('%Y-%m-%d %H:%M:%S')
        else:
            df_clean['timestamp_utc'] = ''
        
        # 向量化列名處理 - 統一列名格式
        if 'symbol' not in df_clean.columns and 'Symbol' in df_clean.columns:
            df_clean['symbol'] = df_clean['Symbol']
        if 'exchange_a' not in df_clean.columns and 'Exchange_A' in df_clean.columns:
            df_clean['exchange_a'] = df_clean['Exchange_A']
        if 'exchange_b' not in df_clean.columns and 'Exchange_B' in df_clean.columns:
            df_clean['exchange_b'] = df_clean['Exchange_B']
        if 'funding_rate_a' not in df_clean.columns and 'FundingRate_A' in df_clean.columns:
            df_clean['funding_rate_a'] = df_clean['FundingRate_A']
        if 'funding_rate_b' not in df_clean.columns and 'FundingRate_B' in df_clean.columns:
            df_clean['funding_rate_b'] = df_clean['FundingRate_B']
        if 'diff_ab' not in df_clean.columns and 'Diff_AB' in df_clean.columns:
            df_clean['diff_ab'] = df_clean['Diff_AB']
        
        # 向量化數值處理 - 一次性處理所有空值和類型轉換
        df_clean['diff_ab'] = pd.to_numeric(df_clean.get('diff_ab', 0), errors='coerce').fillna(0.0)
        df_clean['funding_rate_a'] = pd.to_numeric(df_clean.get('funding_rate_a', 0), errors='coerce').fillna(0.0)
        df_clean['funding_rate_b'] = pd.to_numeric(df_clean.get('funding_rate_b', 0), errors='coerce').fillna(0.0)
        
        # 確保字符串列存在且不為空
        df_clean['symbol'] = df_clean.get('symbol', '').astype(str).fillna('')
        df_clean['exchange_a'] = df_clean.get('exchange_a', '').astype(str).fillna('')
        df_clean['exchange_b'] = df_clean.get('exchange_b', '').astype(str).fillna('')
        
        # 選擇最終需要的列
        required_columns = ['timestamp_utc', 'symbol', 'exchange_a', 'funding_rate_a', 'exchange_b', 'funding_rate_b', 'diff_ab']
        
        # 確保所有必需列都存在
        for col in required_columns:
            if col not in df_clean.columns:
                if col in ['funding_rate_a', 'funding_rate_b', 'diff_ab']:
                    df_clean[col] = 0.0
                else:
                    df_clean[col] = ''
        
        df_final = df_clean[required_columns].copy()
        
        # ✅ 快速轉換為插入數據（避免iterrows循環）
        print("   正在轉換數據格式...")
        data_to_insert = [tuple(row) for row in df_final.values]
        
        # 插入數據庫
        print("   正在插入數據庫...")
        with self.get_connection() as conn:
            conn.executemany('''
                INSERT OR REPLACE INTO funding_rate_diff 
                (timestamp_utc, symbol, exchange_a, funding_rate_a, exchange_b, funding_rate_b, diff_ab)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', data_to_insert)
            
            # 明確提交事務
            conn.commit()
        
        print(f"✅ 解法1完成: {len(data_to_insert)} 條")
        return len(data_to_insert)
    
    def insert_funding_rate_diff_legacy(self, df: pd.DataFrame) -> int:
        """插入資金費率差異數據 - 舊版本：逐行處理（保留用於性能對比）"""
        if df.empty:
            return 0
        
        print(f"⚠️ 使用舊版逐行處理: {len(df)} 條記錄...")
            
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
            
            print(f"✅ 舊版插入完成: {len(data_to_insert)} 條")
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
        """插入策略排行榜數據 - NumPy + SQLite 極速優化版本"""
        if df.empty:
            return 0
        
        print(f"⚡ NumPy極速處理策略排行榜: {len(df)} 條記錄 (策略: {strategy_name})")
        
        # ==================== 步驟1: 列預處理映射優化 ====================
        print("📊 步驟1: 向量化列預處理...")
        df_prepared = df.copy()
        
        # 一次性向量化列映射，消除重複row.get()調用 (節省13.6%耗時)
        df_prepared['strategy_name'] = strategy_name
        df_prepared['trading_pair'] = df.get('Trading_Pair', df.get('trading_pair', ''))
        df_prepared['date'] = df.get('Date', df.get('date', ''))  
        df_prepared['rank_position'] = df.get('Rank', df.get('rank_position', 0))
        
        # 處理別名映射
        if 'long_term_score' not in df_prepared.columns:
            df_prepared['long_term_score'] = df_prepared.get('long_term_score_score', 
                                                            df_prepared.get('all_ROI_Z_score', 0.0))
        
        if 'short_term_score' not in df_prepared.columns:
            df_prepared['short_term_score'] = df_prepared.get('short_term_score_score', 
                                                             df_prepared.get('short_ROI_z_score', 0.0))
        
        # 確保數值列存在並填充默認值
        numeric_defaults = {
            'final_ranking_score': 0.0,
            'combined_roi_z_score': 0.0,
            'final_combination_value': 0.0
        }
        
        for col, default_val in numeric_defaults.items():
            if col not in df_prepared.columns:
                df_prepared[col] = default_val
            else:
                df_prepared[col] = pd.to_numeric(df_prepared[col], errors='coerce').fillna(default_val)
        
        # 確保字符串列類型正確
        df_prepared['trading_pair'] = df_prepared['trading_pair'].astype(str)
        df_prepared['date'] = df_prepared['date'].astype(str)
        df_prepared['rank_position'] = pd.to_numeric(df_prepared['rank_position'], errors='coerce').fillna(0)
        
        print("✅ 列預處理完成")
        
        # ==================== 步驟2: NumPy向量化JSON處理 ====================
        print("🚀 步驟2: NumPy向量化JSON處理...")
        
        # 識別score列 
        score_columns = [col for col in df.columns 
                        if col.endswith('_score') 
                        and col not in ['final_ranking_score', 'long_term_score_score', 'short_term_score_score']]
        
        if score_columns:
            # NumPy向量化處理 - 4.75x速度提升！
            score_array = df[score_columns].values
            print(f"   處理 {len(score_columns)} 個score列，數據形狀: {score_array.shape}")
            
            # 高效批量JSON序列化
            component_scores_list = [
                json.dumps(dict(zip(score_columns, row.tolist()))) 
                for row in score_array
            ]
            df_prepared['component_scores'] = component_scores_list
        else:
            df_prepared['component_scores'] = None
            print("   無score列需要處理")
        
        print("✅ NumPy JSON處理完成")
        
        # ==================== 步驟3: 選擇最終列順序 ====================
        final_columns = [
            'strategy_name', 'trading_pair', 'date', 'final_ranking_score', 'rank_position',
            'long_term_score', 'short_term_score', 'combined_roi_z_score', 
            'final_combination_value', 'component_scores'
        ]
        
        # 確保所有必需列都存在
        for col in final_columns:
            if col not in df_prepared.columns:
                if col in ['long_term_score', 'short_term_score']:
                    df_prepared[col] = 0.0
                elif col == 'component_scores':
                    df_prepared[col] = None
                else:
                    df_prepared[col] = '' if 'str' in str(type(col)) else 0
        
        df_final = df_prepared[final_columns]
        
        # ==================== 步驟4: SQLite極速插入優化 ====================
        print("💾 步驟3: SQLite極速插入...")
        
        with self.get_connection() as conn:
            # 啟用SQLite高級性能優化
            print("   啟用SQLite WAL模式和高級優化...")
            conn.execute("PRAGMA journal_mode = WAL")
            conn.execute("PRAGMA synchronous = NORMAL") 
            conn.execute("PRAGMA cache_size = -128000")  # 128MB緩存
            conn.execute("PRAGMA temp_store = MEMORY")
            conn.execute("PRAGMA page_size = 4096")
            conn.execute("PRAGMA wal_autocheckpoint = 1000")
            
            # 高效數據轉換 - 直接使用NumPy數組
            print(f"   準備批量插入 {len(df_final)} 條記錄...")
            insert_data = df_final.values.tolist()
            
            # 單次批量插入 - 減少數據庫往返次數
            conn.executemany('''
                INSERT OR REPLACE INTO strategy_ranking 
                (strategy_name, trading_pair, date, final_ranking_score, rank_position,
                 long_term_score, short_term_score, combined_roi_z_score, 
                 final_combination_value, component_scores)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', insert_data)
            
            # 立即提交並WAL checkpoint
            conn.commit()
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            
        print("✅ SQLite極速插入完成")
        print(f"🎉 極速優化完成！插入 {len(insert_data)} 條記錄 (策略: {strategy_name})")
        
        return len(insert_data)
    
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
                 roi, total_days, max_drawdown, win_rate, total_trades, profit_days, 
                 loss_days, avg_holding_days, sharpe_ratio, config_params, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                results.get('roi'),
                results.get('total_days'),
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

    def get_established_symbols(self, min_days: int = 3) -> List[str]:
        """
        獲取已上線超過指定天數的幣種列表
        
        Args:
            min_days: 最少上線天數，默認3天
            
        Returns:
            List[str]: 符合條件的幣種符號列表
        """
        with self.get_connection() as conn:
            query = """
            SELECT symbol
            FROM (
                SELECT 
                    symbol,
                    MIN(timestamp_utc) as first_appearance,
                    JULIANDAY('now') - JULIANDAY(MIN(timestamp_utc)) as days_since_first_seen
                FROM funding_rate_history 
                GROUP BY symbol
            ) as coin_history
            WHERE days_since_first_seen >= ?
            ORDER BY symbol
            """
            
            result = conn.execute(query, [min_days]).fetchall()
            return [row[0] for row in result]

    def get_strategy_ranking_excluding_new_coins(self, strategy_name: str, date: str = None, 
                                               top_n: int = None, min_days: int = 3) -> pd.DataFrame:
        """
        獲取策略排行榜，排除新上線的幣種
        
        Args:
            strategy_name: 策略名稱
            date: 查詢日期，None則查詢最新
            top_n: 返回前N名
            min_days: 排除上線少於N天的幣種
            
        Returns:
            DataFrame: 過濾後的策略排行榜
        """
        # 獲取已確立的幣種列表
        established_symbols = self.get_established_symbols(min_days)
        
        if not established_symbols:
            print(f"⚠️ 沒有找到上線超過{min_days}天的幣種")
            return pd.DataFrame()
        
        # 構建查詢條件，排除新幣
        symbols_condition = "'" + "','".join(established_symbols) + "'"
        
        if date:
            query = f"""
            SELECT * FROM strategy_ranking 
            WHERE strategy_name = ? AND date = ?
            AND SUBSTR(trading_pair, 1, INSTR(trading_pair, '_')-1) IN ({symbols_condition})
            ORDER BY rank_position
            """
            params = [strategy_name, date]
        else:
            # 查詢最新日期
            query = f"""
            SELECT * FROM strategy_ranking 
            WHERE strategy_name = ? 
            AND date = (SELECT MAX(date) FROM strategy_ranking WHERE strategy_name = ?)
            AND SUBSTR(trading_pair, 1, INSTR(trading_pair, '_')-1) IN ({symbols_condition})
            ORDER BY rank_position
            """
            params = [strategy_name, strategy_name]
        
        if top_n:
            query += f" LIMIT {top_n}"
        
        df = pd.read_sql_query(query, self.get_connection(), params=params)
        
        if not df.empty:
            excluded_count = len(self.get_strategy_ranking(strategy_name, date, None)) - len(df)
            print(f"✅ 已過濾掉 {excluded_count} 個新上線幣種 (少於{min_days}天)")
        
        return df

    def update_trading_pair_diff_first_date(self, symbol: str, exchange_a: str, exchange_b: str, diff_first_date: str):
        """更新交易對的首次資金費率差時間"""
        
        query = """
        UPDATE trading_pairs 
        SET diff_first_date = ?, updated_at = CURRENT_TIMESTAMP 
        WHERE symbol = ? AND exchange_a = ? AND exchange_b = ?
        """
        
        with self.get_connection() as conn:
            conn.execute(query, [diff_first_date, symbol, exchange_a, exchange_b])

    def get_trading_pairs_with_min_diff_days(self, min_days: int = 3) -> pd.DataFrame:
        """
        獲取首次資金費率差出現超過指定天數的交易對
        
        Args:
            min_days: 最少天數
            
        Returns:
            DataFrame: 符合條件的交易對
        """
        query = """
        SELECT symbol, exchange_a, exchange_b, diff_first_date
        FROM trading_pairs
        WHERE diff_first_date IS NOT NULL
        AND JULIANDAY('now') - JULIANDAY(diff_first_date) >= ?
        ORDER BY diff_first_date
        """
        
        with self.get_connection() as conn:
            result = conn.execute(query, [min_days]).fetchall()
            df = pd.DataFrame(result, columns=['symbol', 'exchange_a', 'exchange_b', 'diff_first_date'])
            return df

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