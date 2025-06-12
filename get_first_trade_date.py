#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
查詢特定交易對在指定交易所的最早上市日期 (第一根K線時間)
"""

import ccxt.async_support as ccxt
import asyncio
import argparse
from datetime import datetime

async def get_earliest_date(exchange_id: str, symbol: str, market_type: str) -> None:
    """
    異步獲取單個交易所上特定交易對的最早K線日期

    Args:
        exchange_id (str): 交易所ID
        symbol (str): 交易對符號
        market_type (str): 市場類型 ('spot' 或 'future')
    """
    print(f"🔍 正在查詢 {exchange_id.upper()} 的 {market_type.upper()} 市場上的 {symbol}...")
    
    try:
        exchange_class = getattr(ccxt, exchange_id)
        
        # 根據市場類型配置交易所實例
        config = {}
        if market_type == 'future':
            if exchange_id == 'binance':
                # 幣安U本位合約使用 'future'
                config = {'options': {'defaultType': 'future'}}
            elif exchange_id == 'bybit':
                # Bybit 永續合約使用 'swap'
                config = {'options': {'defaultType': 'swap'}}
            # 未來可在此處為其他交易所添加配置
        
        exchange = exchange_class(config)

        # 我們需要一個非常早的日期作為查詢起點
        # CCXT 要求時間戳是毫秒級的
        since = exchange.parse8601('2015-01-01T00:00:00Z')
        
        # 嘗試獲取日K線 ('1d')，並且只獲取第一根 (limit=1)
        # 這是推斷最早上市日期的最高效方法
        try:
            ohlcv = await exchange.fetch_ohlcv(symbol, '1d', since=since, limit=1)
        except ccxt.BadSymbol:
            # 如果通用符號失敗，嘗試Bybit等交易所的特殊格式 (e.g., xxx/USDT:USDT)
            if '/' in symbol and market_type == 'future' and symbol.endswith('USDT'):
                new_symbol = f"{symbol}:{symbol.split('/')[1]}"
                print(f"  > {symbol} 未找到，自動嘗試備用名稱: {new_symbol}...")
                # 重新賦值 symbol 以便後續打印正確的名稱
                symbol = new_symbol 
                ohlcv = await exchange.fetch_ohlcv(symbol, '1d', since=since, limit=1)
            else:
                # 如果不符合修正條件，則重新拋出異常
                raise
        
        await exchange.close()
        
        if ohlcv:
            first_candle = ohlcv[0]
            timestamp_ms = first_candle[0]
            # 將毫秒時間戳轉換為人類可讀的日期時間格式
            date_str = datetime.utcfromtimestamp(timestamp_ms / 1000).strftime('%Y-%m-%d %H:%M:%S UTC')
            print(f"✅ {exchange_id.upper()} - {symbol}: 最早數據日期為 {date_str}")
        else:
            print(f"⚠️ {exchange_id.upper()} - {symbol}: 未找到任何歷史數據。可能該交易對未上市或數據不完整。")
            
    except ccxt.BadSymbol as e:
        print(f"❌ {exchange_id.upper()} - {symbol}: 交易對不存在。錯誤: {e}")
    except ccxt.NetworkError as e:
        print(f"❌ {exchange_id.upper()} - {symbol}: 網絡錯誤。錯誤: {e}")
    except Exception as e:
        print(f"❌ {exchange_id.upper()} - {symbol}: 發生未知錯誤。錯誤: {e}")


def get_user_input():
    """獲取用戶的互動式輸入"""
    print("\n" + "="*50)
    print("🚀 交互式查詢交易所最早上市日期工具")
    print("="*50)

    # 獲取交易對
    symbol_input = input("請輸入要查詢的交易對 (例如 CVCUSDT 或 CVC/USDT:USDT): ").strip()
    
    # 格式化交易對，確保是 'CVC/USDT' 或 'CVC/USDT:USDT'
    symbol = symbol_input
    if ':' not in symbol_input and '/' not in symbol_input:
        if symbol_input.upper().endswith('USDT'):
            base = symbol_input.upper().replace('USDT', '')
            symbol = f"{base}/USDT"
            print(f"  > 已自動格式化為: {symbol}")
        else:
            print(f"無法解析的交易對格式: {symbol_input}。請使用 'BASE/QUOTE' 或 'BASE/QUOTE:QUOTE' 格式。")
            return None, None, None

    # 獲取交易所
    exchanges_input = input("請輸入一個或多個交易所,用空格或逗號分隔 (例如 binance bybit): ").strip().lower()
    # 智能分割，替換掉逗號，然後按空格分割
    exchanges = [exc.strip() for exc in exchanges_input.replace(',', ' ').split() if exc.strip()]
    if not exchanges:
        print("未輸入任何交易所。")
        return None, None, None

    # 獲取市場類型
    market_type = ""
    while market_type not in ['spot', 'future']:
        market_type = input("請輸入市場類型 ('spot' 或 'future'): ").strip().lower()
        if market_type not in ['spot', 'future']:
            print("無效的輸入，請選擇 'spot' 或 'future'。")

    return symbol, exchanges, market_type


async def main():
    """主函數，處理命令行參數並並發執行查詢"""
    
    symbol, exchanges, market_type = get_user_input()

    if not all([symbol, exchanges, market_type]):
        print("\n未能獲取有效輸入，程序終止。")
        return

    print(f"\n===== 開始查詢 {symbol} 在 {market_type.upper()} 市場的最早上市日期 =====\n")
    
    # 創建並發任務
    tasks = [get_earliest_date(exchange, symbol, market_type) for exchange in exchanges]
    await asyncio.gather(*tasks)
    
    print("\n===== 查詢完畢 =====\n")


if __name__ == '__main__':
    # 設置異步事件循環並運行主函數
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n用戶中斷操作。") 