import sqlite3
import ccxt
from datetime import datetime, timedelta
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
    """加載指定交易所的期貨市場數據"""
    print(f"正在加載 {exchange_name} 的期貨市場數據...")
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
        print(f"✅ 成功加載 {exchange_name} 的期貨市場 {len(markets)} 個交易對。")
        return markets
    except (ccxt.ExchangeError, ccxt.NetworkError, AttributeError) as e:
        print(f"❌ 加載 {exchange_name} 期貨市場時出錯: {e}")
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

def test_symbol_exists_and_get_date(exchange, symbol_slash):
    """
    測試交易對是否當前活躍交易並獲取上市日期
    先檢查最近1天是否有數據（避免已下架幣種），再獲取歷史上市日期
    Returns: (exists, listing_date) 元組
    """
    try:
        if not exchange.has['fetchOHLCV']:
            return False, None

        # 第一步：檢查最近1天是否有數據（判斷是否還在活躍交易）
        recent_since = exchange.parse8601((datetime.now() - timedelta(days=1)).isoformat() + 'Z')
        
        try:
            # 檢查最近1天是否有數據
            recent_ohlcv = exchange.fetch_ohlcv(symbol_slash, '1d', since=recent_since, limit=5)
            if not recent_ohlcv:
                return False, None  # 最近沒數據，可能已下架
            
            # 如果最近有數據，獲取歷史上市日期
            historical_since = exchange.parse8601('2015-01-01T00:00:00Z')
            historical_ohlcv = exchange.fetch_ohlcv(symbol_slash, '1d', since=historical_since, limit=1)
            
            if historical_ohlcv:
                first_trade_timestamp_ms = historical_ohlcv[0][0]
                listing_date = datetime.fromtimestamp(first_trade_timestamp_ms / 1000)
                return True, listing_date
            else:
                return False, None

        except ccxt.BadSymbol:
            # 如果通用符號失敗，嘗試特殊格式 (e.g., B/USDT -> B/USDT:USDT)
            if symbol_slash.endswith('/USDT'):
                new_symbol = f"{symbol_slash}:{symbol_slash.split('/')[1]}"  # B/USDT -> B/USDT:USDT
                try:
                    # 先檢查新格式的最近1天數據
                    recent_ohlcv = exchange.fetch_ohlcv(new_symbol, '1d', since=recent_since, limit=5)
                    if not recent_ohlcv:
                        return False, None  # 最近沒數據，可能已下架
                    
                    # 如果最近有數據，獲取歷史上市日期
                    historical_since = exchange.parse8601('2015-01-01T00:00:00Z')
                    historical_ohlcv = exchange.fetch_ohlcv(new_symbol, '1d', since=historical_since, limit=1)
                    
                    if historical_ohlcv:
                        first_trade_timestamp_ms = historical_ohlcv[0][0]
                        listing_date = datetime.fromtimestamp(first_trade_timestamp_ms / 1000)
                        return True, listing_date
                except Exception:
                    pass
            return False, None

    except (ccxt.ExchangeError, ccxt.NetworkError) as e:
        # 某些交易所可能對不存在或太早的交易對拋出錯誤，這屬於正常情況
        pass
    return False, None

def get_listing_date_from_ohlcv(exchange, symbol_slash):
    """
    備援方案: 透過獲取最早的K線數據來確定上市日期 (向後兼容函數)
    """
    exists, listing_date = test_symbol_exists_and_get_date(exchange, symbol_slash)
    return listing_date if exists else None

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
            # 初始化變量
            supported = False
            listing_date = None
            market_info = None
            
            exchange_instance = all_exchanges.get(ex_name)
            if not exchange_instance:
                continue
                
            # 將 'SNTUSDT' 轉換為 'SNT/USDT' 來測試
            symbol_slash = f"{symbol}/USDT"
            
            # 🔥 修正後的邏輯：所有交易對都必須通過最近1天活躍檢查
            print(f"    🔍 檢查 {ex_name} 的 {symbol} 是否活躍交易...")
            exists, api_listing_date = test_symbol_exists_and_get_date(exchange_instance, symbol_slash)
            
            if exists:
                # 通過活躍檢查，標記為支持
                supported = True
                listing_date = api_listing_date
                print(f"    ✅ {ex_name} 支援 {symbol}，最近1天有活躍交易，上市日期: {listing_date.date() if listing_date else '未知'}")
                
                # 如果API沒有返回上市日期，嘗試從load_markets()的info中獲取
                if not listing_date and trading_pair in markets:
                    market_info = markets.get(trading_pair)
                    if market_info:
                        info_listing_date = get_listing_date_from_info(market_info.get('info'))
                        if info_listing_date:
                            listing_date = info_listing_date
                            print(f"    📅 從市場信息補充上市日期: {listing_date.date()}")
            else:
                # 未通過活躍檢查
                supported = False
                listing_date = None
                if trading_pair in markets:
                    print(f"    ❌ {ex_name} 的 {symbol} 在官方清單中但最近1天無活躍交易，可能已下架")
                else:
                    print(f"    ❌ {ex_name} 不支援 {symbol}")

            # 更新資料庫
            update_exchange_support(conn, db_id, ex_name, supported, listing_date)

    # 提交所有變更
    conn.commit()
    conn.close()

    end_time = time.time()
    print("\n----- 更新完成 -----")
    print(f"總耗時: {end_time - start_time:.2f} 秒")
    print("所有交易對的交易所支援狀態已更新完畢。")
    print("\n🔥 V3版本特色：所有交易對都必須通過最近1天活躍交易檢查才會被標記為支持！")

if __name__ == '__main__':
    main() 