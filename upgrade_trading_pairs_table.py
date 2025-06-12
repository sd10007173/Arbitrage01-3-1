#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
升級 trading_pairs 表，添加 diff_first_date 欄位
"""

import sqlite3
from database_schema import FundingRateDB

def upgrade_trading_pairs_table():
    """為 trading_pairs 表添加 diff_first_date 欄位"""
    
    db = FundingRateDB()
    
    try:
        with db.get_connection() as conn:
            # 檢查欄位是否已存在
            cursor = conn.execute("PRAGMA table_info(trading_pairs)")
            columns = [row[1] for row in cursor.fetchall()]
            
            if 'diff_first_date' not in columns:
                print("📊 正在為 trading_pairs 表添加 diff_first_date 欄位...")
                
                # 添加新欄位
                conn.execute("""
                    ALTER TABLE trading_pairs 
                    ADD COLUMN diff_first_date TEXT
                """)
                
                print("✅ 成功添加 diff_first_date 欄位")
                
                # 檢查表結構
                cursor = conn.execute("PRAGMA table_info(trading_pairs)")
                print("\n📋 更新後的 trading_pairs 表結構:")
                for row in cursor.fetchall():
                    print(f"  {row[1]} ({row[2]})")
                    
            else:
                print("ℹ️  diff_first_date 欄位已存在，跳過升級")
                
                # 顯示當前表結構
                cursor = conn.execute("PRAGMA table_info(trading_pairs)")
                print("\n📋 當前 trading_pairs 表結構:")
                for row in cursor.fetchall():
                    print(f"  {row[1]} ({row[2]})")
                
    except sqlite3.Error as e:
        print(f"❌ 數據庫升級失敗: {e}")
        return False
    
    return True

if __name__ == "__main__":
    success = upgrade_trading_pairs_table()
    if success:
        print("\n🎉 數據庫升級完成！")
    else:
        print("\n💥 數據庫升級失敗！") 