#!/usr/bin/env python3
import ccxt
import json
from datetime import datetime

def check_bybit_market_info():
    # 創建bybit實例
    bybit = ccxt.bybit({'options': {'defaultType': 'swap'}})
    
    print("=== 加載BYBIT期貨市場資訊 ===")
    markets = bybit.load_markets()
    
    # 查找幾個測試幣種
    test_symbols = ['CPOOL', 'SOLO', 'KMNO', 'AIOZ']
    
    for symbol_name in test_symbols:
        print(f"\n{'='*60}")
        print(f"=== {symbol_name} 期貨市場資訊 ===")
        print(f"{'='*60}")
        
        # 查找對應的市場
        found_market = None
        found_symbol = None
        
        for symbol, market in markets.items():
            if symbol_name in symbol:
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
            for key, value in info.items():
                if any(keyword in key.lower() for keyword in date_keywords):
                    print(f"🔍 {key}: {value}")
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
        else:
            print(f"❌ 未找到 {symbol_name}")

    # 檢查是否有其他API可以獲取上市資訊
    print(f"\n{'='*60}")
    print("=== 檢查其他API端點 ===")
    print(f"{'='*60}")
    
    # 嘗試直接調用instrument info API
    try:
        print("\n🔍 嘗試調用 publicGetV5MarketInstrumentsInfo...")
        
        # 獲取CPOOL的詳細資訊
        result = bybit.publicGetV5MarketInstrumentsInfo({
            'category': 'linear',  # 期貨
            'symbol': 'CPOOLUSDT'
        })
        
        print("📊 CPOOL期貨詳細資訊:")
        print(json.dumps(result, indent=2, default=str))
        
    except Exception as e:
        print(f"❌ 期貨API調用失敗: {e}")
    
    # 嘗試現貨
    try:
        print("\n🔍 嘗試調用現貨 instrument info...")
        
        result = bybit.publicGetV5MarketInstrumentsInfo({
            'category': 'spot',  # 現貨
            'symbol': 'CPOOLUSDT'
        })
        
        print("📊 CPOOL現貨詳細資訊:")
        print(json.dumps(result, indent=2, default=str))
        
    except Exception as e:
        print(f"❌ 現貨API調用失敗: {e}")

if __name__ == "__main__":
    check_bybit_market_info() 