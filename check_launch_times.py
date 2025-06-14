#!/usr/bin/env python3
import ccxt
from datetime import datetime

def check_launch_times():
    # 創建bybit期貨實例
    bybit = ccxt.bybit({'options': {'defaultType': 'swap'}})
    
    # 測試幣種
    test_symbols = ['CPOOL', 'SOLO', 'KMNO', 'AIOZ']
    
    print("=== BYBIT期貨 launchTime 檢查 ===")
    
    for symbol_name in test_symbols:
        try:
            symbol = f"{symbol_name}USDT"
            print(f"\n🔍 檢查 {symbol}...")
            
            # 調用期貨instruments info API
            result = bybit.publicGetV5MarketInstrumentsInfo({
                'category': 'linear',
                'symbol': symbol
            })
            
            if result['result']['list']:
                instrument = result['result']['list'][0]
                launch_time_ms = instrument.get('launchTime')
                
                if launch_time_ms and launch_time_ms != "0":
                    launch_time = datetime.fromtimestamp(int(launch_time_ms) / 1000)
                    print(f"✅ {symbol} launchTime: {launch_time.date()}")
                else:
                    print(f"❌ {symbol} 沒有launchTime資訊")
            else:
                print(f"❌ {symbol} 期貨不存在")
                
        except Exception as e:
            print(f"❌ {symbol_name} 查詢失敗: {e}")

if __name__ == "__main__":
    check_launch_times() 