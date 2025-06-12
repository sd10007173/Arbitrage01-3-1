import sqlite3
import ccxt
from datetime import datetime
import time

DB_PATH = "data/funding_rate.db"
EXCHANGES = ['binance', 'bybit', 'okx', 'gate']

def get_connection():
    """獲取數據庫連接"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def get_all_trading_pairs(conn):
    """從資料庫獲取所有交易對"""
    cursor = conn.cursor()
    cursor.execute("SELECT id, symbol, trading_pair FROM trading_pair")
    return cursor.fetchall()

def load_exchange_markets(exchange_name):
    """加載指定交易所的市場數據"""
    print(f"正在加載 {exchange_name} 的市場數據...")
    try:
        exchange_class = getattr(ccxt, exchange_name)
        exchange = exchange_class()
        markets = exchange.load_markets()
        print(f"✅ 成功加載 {exchange_name} 的 {len(markets)} 個市場。")
        return markets
    except (ccxt.ExchangeError, ccxt.NetworkError, AttributeError) as e:
        print(f"❌ 加載 {exchange_name} 市場時出錯: {e}")
        return None

def get_listing_date_from_info(market_info):
    """從市場的 'info' 字段中嘗試提取上市日期"""
    if not market_info:
        return None
    
    # 擴大可能的 key 列表
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

def get_listing_date_from_ohlcv(exchange, symbol_slash):
    """
    備援方案: 透過獲取最早的K線數據來確定上市日期 (參考 get_first_trade_date.py)
    """
    try:
        if not exchange.has['fetchOHLCV']:
            return None

        # CCXT 要求時間戳是毫秒級的
        since = exchange.parse8601('2015-01-01T00:00:00Z')
        
        # 嘗試獲取日線 ('1d')，並且只獲取第一根 (limit=1)
        ohlcv = exchange.fetch_ohlcv(symbol_slash, '1d', since=since, limit=1)
        
        if ohlcv:
            first_trade_timestamp_ms = ohlcv[0][0]
            return datetime.fromtimestamp(first_trade_timestamp_ms / 1000)

    except ccxt.BadSymbol:
        # 如果通用符號失敗，嘗試Bybit等交易所的特殊格式 (e.g., xxx/USDT:USDT)
        if exchange.id == 'bybit' and symbol_slash.endswith('/USDT'):
            new_symbol = f"{symbol_slash}:{symbol_slash.split('/')[0]}"
            # print(f"    - {symbol_slash} 未找到，自動嘗試備用名稱: {new_symbol}...")
            try:
                since = exchange.parse8601('2015-01-01T00:00:00Z')
                ohlcv = exchange.fetch_ohlcv(new_symbol, '1d', since=since, limit=1)
                if ohlcv:
                    first_trade_timestamp_ms = ohlcv[0][0]
                    return datetime.fromtimestamp(first_trade_timestamp_ms / 1000)
            except Exception: # 如果再次失敗，就放棄
                pass
    except (ccxt.ExchangeError, ccxt.NetworkError) as e:
        # 某些交易所可能對不存在或太早的交易對拋出錯誤，這屬於正常情況
        pass
    return None

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
    print("交易所支援狀態更新腳本開始執行...")
    start_time = time.time()

    # 1. 加載所有交易所的市場數據
    all_exchanges = {}
    all_markets = {}
    for ex_name in EXCHANGES:
        try:
            exchange_class = getattr(ccxt, ex_name)
            exchange = exchange_class()
            markets = exchange.load_markets()
            
            all_exchanges[ex_name] = exchange
            # 將 trading_pair 轉換為不帶 '/' 的格式, e.g., 'BTC/USDT' -> 'BTCUSDT'
            all_markets[ex_name] = {s.replace('/', ''): info for s, info in markets.items()}
            print(f"✅ 成功加載 {ex_name} 的 {len(markets)} 個市場。")
        except (ccxt.ExchangeError, ccxt.NetworkError, AttributeError) as e:
            print(f"❌ 加載 {ex_name} 市場時出錯: {e}")

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
            supported = trading_pair in markets
            listing_date = None
            if supported:
                market_info = markets.get(trading_pair)
                # 方案 1: 嘗試從 load_markets() 的 info 中獲取
                if market_info:
                    listing_date = get_listing_date_from_info(market_info.get('info'))
                
                # 方案 2: 如果找不到，啟用備援方案 (OHLCV)
                if not listing_date:
                    # print(f"    - 在 {ex_name} 的市場資訊中未找到上市日期，嘗試備援方案...")
                    exchange_instance = all_exchanges.get(ex_name)
                    if exchange_instance:
                        # 將 'BTCUSDT' 轉換為 'BTC/USDT'
                        symbol_slash = f"{symbol}/USDT"
                        listing_date = get_listing_date_from_ohlcv(exchange_instance, symbol_slash)
                        if listing_date:
                            print(f"    ✅ 備援方案成功: 在 {ex_name} 找到 {symbol} 的上市日期為 {listing_date.date()}")

            # 更新資料庫
            update_exchange_support(conn, db_id, ex_name, supported, listing_date)

    # 提交所有變更
    conn.commit()
    conn.close()

    end_time = time.time()
    print("\n----- 更新完成 -----")
    print(f"總耗時: {end_time - start_time:.2f} 秒")
    print("所有交易對的交易所支援狀態已更新完畢。")

if __name__ == '__main__':
    main() 