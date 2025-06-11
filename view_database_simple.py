#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
🗄️ 簡單的數據庫查看工具
適合新手使用，不需要懂 SQL
"""

from database_operations import DatabaseManager
import pandas as pd

def main_menu():
    """主選單"""
    print("="*60)
    print("🗄️  資金費率數據庫查看器")
    print("="*60)
    print("請選擇你想查看的內容：")
    print()
    print("1. 📊 數據庫基本資訊")
    print("2. 💰 資金費率歷史數據")
    print("3. 📈 資金費率差異數據")
    print("4. 🔍 搜尋特定交易對")
    print("5. 📅 查看最新數據")
    print("0. 退出")
    print()
    
def show_database_info():
    """顯示數據庫基本資訊"""
    db = DatabaseManager()
    info = db.get_database_info()
    
    print("\n📊 數據庫基本資訊:")
    print("-" * 40)
    
    for table, count in info['tables'].items():
        status = "✅ 有數據" if count > 0 else "⚪ 空"
        print(f"{table:<25} {count:>10,} 條 {status}")
    
    print(f"\n📁 數據庫檔案位置: data/funding_rate.db")

def show_funding_rate_history():
    """顯示資金費率歷史數據樣本"""
    db = DatabaseManager()
    print("\n💰 資金費率歷史數據 (最新 10 筆):")
    print("-" * 40)
    
    # 查詢最新 10 筆數據
    df = db.get_funding_rate_history(limit=10)
    if not df.empty:
        # 只顯示重要欄位
        display_df = df[['timestamp_utc', 'symbol', 'exchange', 'funding_rate']].copy()
        print(display_df.to_string(index=False))
    else:
        print("❌ 沒有資金費率歷史數據")

def show_funding_rate_diff():
    """顯示資金費率差異數據樣本"""
    db = DatabaseManager()
    print("\n📈 資金費率差異數據 (最新 10 筆):")
    print("-" * 40)
    
    # 查詢最新 10 筆數據
    df = db.get_funding_rate_diff()
    if not df.empty:
        # 只顯示重要欄位，取最新 10 筆
        display_df = df.tail(10)[['timestamp_utc', 'symbol', 'exchange_a', 'exchange_b', 'diff_ab']].copy()
        print(display_df.to_string(index=False))
    else:
        print("❌ 沒有資金費率差異數據")

def search_trading_pair():
    """搜尋特定交易對"""
    symbol = input("\n🔍 請輸入要搜尋的交易對 (例如: BTCUSDT): ").strip().upper()
    
    if not symbol:
        print("❌ 請輸入有效的交易對名稱")
        return
    
    db = DatabaseManager()
    
    print(f"\n搜尋交易對: {symbol}")
    print("-" * 40)
    
    # 搜尋資金費率歷史
    history_df = db.get_funding_rate_history(symbol=symbol, limit=5)
    if not history_df.empty:
        print(f"✅ 找到 {len(history_df)} 筆資金費率歷史記錄 (顯示最新 5 筆):")
        display_df = history_df[['timestamp_utc', 'exchange', 'funding_rate']].copy()
        print(display_df.to_string(index=False))
    else:
        print(f"❌ 沒有找到 {symbol} 的資金費率歷史數據")
    
    # 搜尋資金費率差異
    diff_df = db.get_funding_rate_diff(symbol=symbol)
    if not diff_df.empty:
        print(f"\n✅ 找到 {len(diff_df)} 筆資金費率差異記錄 (顯示最新 5 筆):")
        display_df = diff_df.tail(5)[['timestamp_utc', 'exchange_a', 'exchange_b', 'diff_ab']].copy()
        print(display_df.to_string(index=False))
    else:
        print(f"❌ 沒有找到 {symbol} 的資金費率差異數據")

def show_latest_data():
    """顯示最新數據摘要"""
    db = DatabaseManager()
    print("\n📅 最新數據摘要:")
    print("-" * 40)
    
    # 最新的資金費率數據
    history_df = db.get_funding_rate_history(limit=1)
    if not history_df.empty:
        latest_time = history_df.iloc[0]['timestamp_utc']
        print(f"📊 最新資金費率數據時間: {latest_time}")
    
    # 最新的差異數據
    diff_df = db.get_funding_rate_diff()
    if not diff_df.empty:
        latest_diff_time = diff_df.iloc[-1]['timestamp_utc']
        print(f"📈 最新差異數據時間: {latest_diff_time}")
    
    # 統計不同交易所的數據量
    print(f"\n📊 各交易所數據統計:")
    exchanges_df = pd.read_sql_query("""
        SELECT exchange, COUNT(*) as count 
        FROM funding_rate_history 
        GROUP BY exchange 
        ORDER BY count DESC
    """, db.get_connection())
    
    for _, row in exchanges_df.iterrows():
        print(f"  {row['exchange']:<10} {row['count']:>10,} 條")

def main():
    """主程式"""
    while True:
        try:
            main_menu()
            choice = input("請選擇 (0-5): ").strip()
            
            if choice == '0':
                print("\n👋 再見！")
                break
            elif choice == '1':
                show_database_info()
            elif choice == '2':
                show_funding_rate_history()
            elif choice == '3':
                show_funding_rate_diff()
            elif choice == '4':
                search_trading_pair()
            elif choice == '5':
                show_latest_data()
            else:
                print("❌ 無效選擇，請重新輸入")
            
            input("\n按 Enter 鍵繼續...")
            
        except KeyboardInterrupt:
            print("\n\n👋 程式已中止，再見！")
            break
        except Exception as e:
            print(f"\n❌ 發生錯誤: {e}")
            input("按 Enter 鍵繼續...")

if __name__ == "__main__":
    main() 