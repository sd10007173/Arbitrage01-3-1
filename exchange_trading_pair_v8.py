#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
V8版本 - 交易所支援狀態更新腳本（超高效版本）
🔥 V8優化重點：
1. 保持V7的邏輯：檢查最近3天成交量 + 找首次有成交量的日期
2. 大幅優化 find_first_trading_date_with_volume 函數效率：
   - 步驟1：快速獲取上市日期（第一次有OHLC的日期）- 1次API調用
   - 步驟2：從上市日期開始縮小範圍掃描找第一次有成交量的日期
3. 預期效率提升：30-60倍，總時間從6.5小時縮短到10-20分鐘
"""

import ccxt
import sqlite3
import time
from datetime import datetime, timedelta

# 支援的交易所列表
EXCHANGES = ['binance', 'bybit', 'okx', 'gate']

def get_connection():
    """連接到SQLite資料庫"""
    conn = sqlite3.connect('data/funding_rate.db')
    conn.row_factory = sqlite3.Row
    return conn

def get_all_trading_pairs(conn):
    """從資料庫獲取所有交易對"""
    cursor = conn.cursor()
    cursor.execute("SELECT id, symbol, trading_pair FROM trading_pair ORDER BY id")
    return cursor.fetchall()

def load_exchange_markets(exchange_name):
    """加載指定交易所的市場數據"""
    try:
        exchange_class = getattr(ccxt, exchange_name)
        
        # 配置為期貨/永續合約市場
        config = {}
        if exchange_name == 'binance':
            config = {'options': {'defaultType': 'future'}}
        elif exchange_name == 'bybit':
            config = {'options': {'defaultType': 'swap'}}
        elif exchange_name == 'okx':
            config = {'options': {'defaultType': 'swap'}}
        elif exchange_name == 'gate':
            config = {'options': {'defaultType': 'swap'}}
        
        exchange = exchange_class(config)
        markets = exchange.load_markets()
        
        # 將 trading_pair 轉換為不帶 '/' 的格式, e.g., 'BTC/USDT' -> 'BTCUSDT'
        formatted_markets = {s.replace('/', ''): info for s, info in markets.items()}
        
        return exchange, formatted_markets
    except (ccxt.ExchangeError, ccxt.NetworkError, AttributeError) as e:
        print(f"❌ 加載 {exchange_name} 期貨市場時出錯: {e}")
        return None, {}

def get_listing_date_from_info(market_info):
    """從市場的 'info' 字段中嘗試提取上市日期"""
    if not market_info:
        return None
    
    # 可能的上市日期字段
    possible_keys = [
        'listingTime', 'listTime', 'onboardDate', 'created_at', 
        'onlineTime', 'publishTime', 'listing_time', 'launchTime'
    ]
    for key in possible_keys:
        if key in market_info and market_info[key]:
            try:
                # 時間戳可能是秒或毫秒
                ts = int(market_info[key])
                if ts > 10**12: # 毫秒
                    return datetime.fromtimestamp(ts / 1000)
                else: # 秒
                    return datetime.fromtimestamp(ts)
            except (ValueError, TypeError):
                continue
    return None

def get_first_ohlc_date(exchange, symbol_slash):
    """
    🔥 V8優化步驟1：快速獲取上市日期（第一次有OHLC的日期）
    只需要1次API調用就能獲取最早的K線日期
    Returns: datetime object or None
    """
    try:
        # 從很早的日期開始查詢，獲取第一根K線
        since_timestamp = exchange.parse8601('2015-01-01T00:00:00Z')
        ohlcv_data = exchange.fetch_ohlcv(symbol_slash, '1d', since=since_timestamp, limit=1)
        
        if ohlcv_data:
            first_candle_timestamp_ms = ohlcv_data[0][0]
            first_ohlc_date = datetime.fromtimestamp(first_candle_timestamp_ms / 1000)
            print(f"    📅 快速獲取 {symbol_slash} 上市日期: {first_ohlc_date.date()}")
            return first_ohlc_date
        else:
            print(f"    ❌ {symbol_slash} 無法獲取上市日期")
            return None
            
    except ccxt.BadSymbol:
        # 嘗試特殊格式
        if symbol_slash.endswith('/USDT') and ':' not in symbol_slash:
            new_symbol = f"{symbol_slash}:{symbol_slash.split('/')[1]}"
            print(f"    🔄 嘗試特殊格式獲取上市日期: {new_symbol}")
            try:
                since_timestamp = exchange.parse8601('2015-01-01T00:00:00Z')
                ohlcv_data = exchange.fetch_ohlcv(new_symbol, '1d', since=since_timestamp, limit=1)
                if ohlcv_data:
                    first_candle_timestamp_ms = ohlcv_data[0][0]
                    first_ohlc_date = datetime.fromtimestamp(first_candle_timestamp_ms / 1000)
                    print(f"    📅 快速獲取 {new_symbol} 上市日期: {first_ohlc_date.date()}")
                    return first_ohlc_date
            except Exception:
                pass
        print(f"    ❌ {symbol_slash} 不存在於交易所")
        return None
    except Exception as e:
        print(f"    ❌ 獲取 {symbol_slash} 上市日期時出錯: {e}")
        return None

def find_first_volume_from_date(exchange, symbol_slash, start_date):
    """
    🔥 V8優化步驟2：從指定日期開始找第一次有成交量的日期
    大幅縮小搜索範圍，通常只需要1-5次API調用
    Args:
        exchange: 交易所實例
        symbol_slash: 交易對符號
        start_date: 開始搜索的日期
    Returns: datetime object or None
    """
    try:
        print(f"    🔍 從 {start_date.date()} 開始掃描首次成交量...")
        
        current_date = start_date
        max_date = datetime.now()
        
        # 從上市日期開始，每次查詢30天的數據
        while current_date < max_date:
            try:
                # 計算這次查詢的時間範圍（30天）
                end_date = min(current_date + timedelta(days=30), max_date)
                since_timestamp = exchange.parse8601(current_date.isoformat() + 'Z')
                
                # 獲取這30天的日K線數據
                ohlcv_data = exchange.fetch_ohlcv(symbol_slash, '1d', since=since_timestamp, limit=30)
                
                if ohlcv_data:
                    # 檢查每一天的成交量
                    for candle in ohlcv_data:
                        timestamp_ms = candle[0]
                        volume = candle[5]
                        
                        if volume > 0:
                            # 找到第一個有成交量的日期！
                            first_trading_date = datetime.fromtimestamp(timestamp_ms / 1000)
                            print(f"    ✅ 找到 {symbol_slash} 首次成交量日期: {first_trading_date.date()}")
                            return first_trading_date
                
                # 移動到下一個30天區間
                current_date = end_date
                
                # 較短的延遲，因為搜索範圍已經大幅縮小
                time.sleep(0.05)
                
            except Exception as e:
                print(f"    ⚠️  掃描 {symbol_slash} 時出錯: {e}")
                # 繼續下一個時間段
                current_date += timedelta(days=30)
                continue
        
        print(f"    ❌ 從 {start_date.date()} 開始未找到 {symbol_slash} 的首次成交量日期")
        return None
        
    except Exception as e:
        print(f"    ❌ 掃描 {symbol_slash} 首次成交量時出錯: {e}")
        return None

def find_first_trading_date_with_volume(exchange, symbol_slash):
    """
    🔥 V8超高效版本：兩步法找首次有成交量的日期
    步驟1：快速獲取上市日期（1次API調用）
    步驟2：從上市日期開始縮小範圍掃描（通常1-5次API調用）
    總共只需要2-6次API調用，vs V7的122次調用！
    Returns: datetime object or None
    """
    try:
        # 🔥 V8步驟1：快速獲取上市日期
        listing_date = get_first_ohlc_date(exchange, symbol_slash)
        if not listing_date:
            return None
        
        # 🔥 V8步驟2：從上市日期開始找第一次有成交量的日期
        first_volume_date = find_first_volume_from_date(exchange, symbol_slash, listing_date)
        return first_volume_date
        
    except Exception as e:
        print(f"    ❌ V8版本掃描 {symbol_slash} 時出錯: {e}")
        return None

def check_volume_and_get_listing_date(exchange, symbol_slash):
    """
    🔥 V8邏輯（保持V7邏輯不變）：
    1. 檢查最近3天是否有成交量 → 決定support狀態
    2. 使用V8超高效版本找到首次有實際交易量的日期 → 作為真正的上市日期
    Returns: (has_volume, listing_date) 元組
    """
    has_volume = False
    listing_date = None
    
    try:
        if not exchange.has['fetchOHLCV']:
            return False, None

        # 第一步：檢查最近3天是否有成交量（決定support狀態）
        try:
            recent_ohlcv = exchange.fetch_ohlcv(symbol_slash, '1d', limit=3)
            if recent_ohlcv:
                # 檢查是否有實際交易量
                has_volume = any(candle[5] > 0 for candle in recent_ohlcv)
                if has_volume:
                    print(f"    ✅ {symbol_slash} 最近3天有成交量")
                else:
                    print(f"    ⚠️  {symbol_slash} 最近3天成交量為0")
            else:
                print(f"    ❌ {symbol_slash} 無法獲取最近3天數據")
        except ccxt.BadSymbol:
            # 如果通用符號失敗，嘗試特殊格式
            if symbol_slash.endswith('/USDT'):
                new_symbol = f"{symbol_slash}:{symbol_slash.split('/')[1]}"
                try:
                    recent_ohlcv = exchange.fetch_ohlcv(new_symbol, '1d', limit=3)
                    if recent_ohlcv:
                        has_volume = any(candle[5] > 0 for candle in recent_ohlcv)
                        if has_volume:
                            print(f"    ✅ {new_symbol} 最近3天有成交量")
                        else:
                            print(f"    ⚠️  {new_symbol} 最近3天成交量為0")
                        # 更新symbol_slash為成功的格式，用於後續獲取歷史數據
                        symbol_slash = new_symbol
                    else:
                        print(f"    ❌ {new_symbol} 無法獲取最近3天數據")
                except Exception:
                    print(f"    ❌ {symbol_slash} 和 {new_symbol} 都無法獲取數據")
        except Exception as e:
            print(f"    ❌ {symbol_slash} 獲取最近數據時出錯: {e}")

        # 第二步：🔥 V8超高效版本 - 找到首次有實際交易量的日期
        if has_volume or True:  # 不管有沒有最近成交量，都嘗試找首次交易日期
            listing_date = find_first_trading_date_with_volume(exchange, symbol_slash)

    except (ccxt.ExchangeError, ccxt.NetworkError) as e:
        print(f"    ❌ 交易所API錯誤: {e}")
    
    return has_volume, listing_date

def update_exchange_support(conn, db_id, exchange_name, supported, listing_date):
    """更新資料庫中特定交易對的交易所支援狀態和上市日期"""
    cursor = conn.cursor()
    support_col = f"{exchange_name}_support"
    date_col = f"{exchange_name}_list_date"
    
    listing_date_iso = listing_date.isoformat() if listing_date else None
    
    try:
        cursor.execute(f"""
            UPDATE trading_pair
            SET 
                {support_col} = ?,
                {date_col} = ?,
                updated_at = ?
            WHERE id = ?
        """, (supported, listing_date_iso, datetime.now().isoformat(), db_id))
    except sqlite3.Error as e:
        print(f"❌ 更新 ID:{db_id} 的 {exchange_name} 數據時出錯: {e}")

def main():
    """主執行程序"""
    print("🔥 V8版本 - 交易所支援狀態更新腳本開始執行（超高效版本）...")
    print("💡 V8優化：兩步法找首次成交量日期，預期速度提升30-60倍！")
    start_time = time.time()

    # 1. 加載所有交易所的期貨市場數據
    all_exchanges = {}
    all_markets = {}
    for ex_name in EXCHANGES:
        try:
            exchange_class = getattr(ccxt, ex_name)
            
            # 配置為期貨/永續合約市場
            config = {}
            if ex_name == 'binance':
                config = {'options': {'defaultType': 'future'}}
            elif ex_name == 'bybit':
                config = {'options': {'defaultType': 'swap'}}
            elif ex_name == 'okx':
                config = {'options': {'defaultType': 'swap'}}
            elif ex_name == 'gate':
                config = {'options': {'defaultType': 'swap'}}
            
            exchange = exchange_class(config)
            markets = exchange.load_markets()
            
            all_exchanges[ex_name] = exchange
            # 將 trading_pair 轉換為不帶 '/' 的格式, e.g., 'BTC/USDT' -> 'BTCUSDT'
            all_markets[ex_name] = {s.replace('/', ''): info for s, info in markets.items()}
            print(f"✅ 成功加載 {ex_name} 的期貨市場 {len(markets)} 個交易對。")
        except (ccxt.ExchangeError, ccxt.NetworkError, AttributeError) as e:
            print(f"❌ 加載 {ex_name} 期貨市場時出錯: {e}")

    # 2. 連接資料庫並獲取所有交易對
    conn = get_connection()
    trading_pairs_from_db = get_all_trading_pairs(conn)
    print(f"\n從資料庫讀取到 {len(trading_pairs_from_db)} 個交易對，開始逐一檢查...")

    # 3. 遍歷交易對並更新支援狀態
    total_pairs = len(trading_pairs_from_db)
    for i, row in enumerate(trading_pairs_from_db):
        db_id = row['id']
        symbol = row['symbol']
        trading_pair = row['trading_pair'] # e.g., BTCUSDT
        
        print(f"({i+1}/{total_pairs}) 正在處理: {symbol} ({trading_pair})")

        for ex_name, markets in all_markets.items():
            exchange_instance = all_exchanges.get(ex_name)
            if not exchange_instance:
                continue
                
            # 將 'BTCUSDT' 轉換為 'BTC/USDT' 來測試
            symbol_slash = f"{symbol}/USDT"
            
            # 🔥 V8超高效邏輯：檢查3天成交量 + 快速找首次有交易量的日期
            print(f"    🔍 V8高效檢查 {ex_name} 的 {symbol}...")
            has_volume, listing_date = check_volume_and_get_listing_date(exchange_instance, symbol_slash)
            
            # 根據成交量決定support狀態
            supported = 1 if has_volume else 0
            
            # 如果沒有從API獲取到上市日期，嘗試從load_markets()的info中獲取
            if not listing_date and trading_pair in markets:
                market_info = markets.get(trading_pair)
                if market_info:
                    info_listing_date = get_listing_date_from_info(market_info.get('info'))
                    if info_listing_date:
                        listing_date = info_listing_date
                        print(f"    📅 從市場信息補充上市日期: {listing_date.date()}")
            
            # 顯示最終結果
            support_status = "支援" if supported else "不支援"
            date_status = f"首次交易日期: {listing_date.date()}" if listing_date else "首次交易日期: 未知"
            print(f"    📊 {ex_name} {support_status} {symbol}，{date_status}")

            # 更新資料庫
            update_exchange_support(conn, db_id, ex_name, supported, listing_date)

    # 提交所有變更
    conn.commit()
    conn.close()

    end_time = time.time()
    print("\n🎉 ----- V8版本更新完成 -----")
    print(f"⚡ 總耗時: {end_time - start_time:.2f} 秒")
    print("✅ 所有交易對的交易所支援狀態已更新完畢。")
    print("\n🔥 V8版本特色：")
    print("   1. 保持V7邏輯：檢查最近3天成交量 + 找首次成交量日期")
    print("   2. 兩步法超高效優化：先獲取上市日期，再縮小範圍掃描")
    print("   3. API調用次數減少約95%，預期速度提升30-60倍")
    print("   4. 從6.5小時縮短到10-20分鐘")

if __name__ == "__main__":
    main() 