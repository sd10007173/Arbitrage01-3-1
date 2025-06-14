#!/usr/bin/env python3
import ccxt
import json
from datetime import datetime

def check_binance_market_info():
    # 創建binance現貨實例
    binance = ccxt.binance()
    
    print("=== 加載BINANCE現貨市場資訊 ===")
    markets = binance.load_markets()
    
    # 查找幾個測試幣種（跟bybit測試一樣的）
    test_symbols = ['CPOOL', 'SOLO', 'KMNO', 'AIOZ']
    
    for symbol_name in test_symbols:
        print(f"\n{'='*60}")
        print(f"=== {symbol_name} 現貨市場資訊 ===")
        print(f"{'='*60}")
        
        # 查找對應的市場
        found_market = None
        found_symbol = None
        
        for symbol, market in markets.items():
            if symbol_name in symbol and 'USDT' in symbol:
                found_market = market
                found_symbol = symbol
                break
        
        if found_market:
            print(f"找到市場: {found_symbol}")
            print(f"市場ID: {found_market.get('id', 'N/A')}")
            print(f"基礎幣種: {found_market.get('base', 'N/A')}")
            print(f"報價幣種: {found_market.get('quote', 'N/A')}")
            print(f"活躍狀態: {found_market.get('active', 'N/A')}")
            print(f"市場類型: {found_market.get('type', 'N/A')}")
            
            # 重點：檢查info字段中的所有內容
            info = found_market.get('info', {})
            print(f"\n--- INFO 字段內容 ---")
            for key, value in info.items():
                print(f"{key}: {value}")
            
            # 特別關注可能的日期字段
            print(f"\n--- 可能的日期字段 ---")
            date_keywords = ['time', 'date', 'launch', 'list', 'create', 'start', 'begin', 'open']
            found_date_fields = False
            for key, value in info.items():
                if any(keyword in key.lower() for keyword in date_keywords):
                    print(f"🔍 {key}: {value}")
                    found_date_fields = True
                    # 如果是時間戳，嘗試轉換
                    try:
                        if isinstance(value, (int, str)) and str(value).isdigit():
                            ts = int(value)
                            if ts > 1000000000:  # 看起來像時間戳
                                if ts > 10**12:  # 毫秒
                                    dt = datetime.fromtimestamp(ts / 1000)
                                else:  # 秒
                                    dt = datetime.fromtimestamp(ts)
                                print(f"    --> 轉換後日期: {dt}")
                    except:
                        pass
            
            if not found_date_fields:
                print("❌ 沒有找到任何日期相關字段")
        else:
            print(f"❌ 未找到 {symbol_name}USDT")
    
    # 測試 exchangeInfo API 是否有其他資訊
    print(f"\n{'='*60}")
    print("=== 檢查 exchangeInfo API ===")
    print(f"{'='*60}")
    
    try:
        exchange_info = binance.publicGetExchangeinfo()
        symbols = exchange_info.get('symbols', [])
        
        # 找一個測試symbol
        test_symbol_info = None
        for symbol_info in symbols:
            if symbol_info.get('symbol') == 'CPOOLUSDT':
                test_symbol_info = symbol_info
                break
        
        if test_symbol_info:
            print("CPOOLUSDT symbol info:")
            for key, value in test_symbol_info.items():
                print(f"{key}: {value}")
        else:
            print("❌ 未找到 CPOOLUSDT 的詳細資訊")
            
    except Exception as e:
        print(f"❌ 查詢 exchangeInfo 失敗: {e}")

if __name__ == "__main__":
    check_binance_market_info() 