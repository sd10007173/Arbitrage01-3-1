import os
import pandas as pd
import time
from database_operations import DatabaseManager

# 獲取項目根目錄
project_root = os.path.dirname(os.path.abspath(__file__))

def load_market_caps_from_database():
    """
    從數據庫讀取市值資料，並將 {SYMBOL(大寫): 最大 market_cap} 存入字典。
    
    Returns:
        dict: {symbol: max_market_cap}
    """
    db = DatabaseManager()
    df = db.get_market_caps()
    
    if df.empty:
        log_message("⚠️ 數據庫中沒有市值數據，請先運行 coingecko_market_cap.py")
        return {}

    log_message("=== 數據庫市值數據 ===")
    log_message(f"總共有 {len(df)} 個幣種的市值資料")
    
    # 顯示前5名作為示例
    top_5 = df.head(5)
    for _, row in top_5.iterrows():
        symbol = str(row['symbol']).upper()
        market_cap = row['market_cap']
        if pd.notna(market_cap):
            log_message(f"  {symbol}: ${market_cap:,.0f}")
    
    # 建立 {symbol: market_cap} 字典
    market_cap_dict = {}
    for _, row in df.iterrows():
        symbol = str(row['symbol']).upper()
        market_cap = row['market_cap']
        
        if pd.notna(market_cap) and market_cap > 0:
            # 如果已存在該 symbol，取較大的 market_cap
            if symbol in market_cap_dict:
                market_cap_dict[symbol] = max(market_cap_dict[symbol], market_cap)
            else:
                market_cap_dict[symbol] = market_cap
    
    log_message(f"✅ 從數據庫讀取市值資料，共 {len(market_cap_dict)} 個 symbol")
    return market_cap_dict

def log_message(msg):
    """打印日誌消息"""
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {msg}")

def extract_base_symbol(trading_pair):
    """
    從交易對中提取基礎幣種
    例：BTCUSDT -> BTC, ETHUSDT -> ETH
    """
    # 移除常見的計價幣種後綴
    quote_currencies = ['USDT', 'USDC', 'BUSD', 'USD', 'BTC', 'ETH']
    
    for quote in quote_currencies:
        if trading_pair.endswith(quote):
            base = trading_pair[:-len(quote)]
            if base:  # 確保提取後不為空
                return base.upper()
    
    # 如果沒有匹配到常見後綴，返回原字符串
    return trading_pair.upper()

def get_unique_symbols_by_exchange():
    """
    取得各交易所的獨特交易對 symbols
    """
    symbols_by_exchange = {
        'binance': set([
            'BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'XRPUSDT', 'ADAUSDT', 'SOLUSDT', 'DOTUSDT',
            'DOGEUSDT', 'AVAXUSDT', 'SHIBUSDT', 'LTCUSDT', 'LINKUSDT', 'UNIUSDT', 'BCHUSD',
            'XLMUSDT', 'ATOMUSDT', 'FILUSDT', 'ICPUSDT', 'VETUSDT', 'ETCUSDT', 'TRXUSDT',
            'NEARUSDT', 'ALGOUSDT', 'AAVEUSDT', 'MKRUSDT', 'THETAUSDT', 'XMRUSDT', 'EOSUSDT',
            'AXSUSDT', 'SANDUSDT', 'MANAUSDT', 'GRTUSDT', 'ENJUSDT', 'CHZUSDT', 'SNXUSDT',
            '1INCHUSDT', 'CRVUSDT', 'BATUSDT', 'ZENUSDT', 'ZRXUSDT', 'OMGUSDT', 'SUSHIUSDT',
            'COMPUSDT', 'YFIUSDT', 'ALPHAUSDT', 'SKLUSDT', 'STORJUSDT', 'AUDIOUSDT', 'CTKUSDT',
            'AKROUSDT', 'CTSIUSDT', 'DATAUSDT', 'HBARUSDT', 'OCEANUSDT', 'BNTUSDT'
        ]),
        'bybit': set([
            'BTCUSDT', 'ETHUSDT', 'XRPUSDT', 'ADAUSDT', 'SOLUSDT', 'DOTUSDT', 'DOGEUSDT',
            'AVAXUSDT', 'SHIBUSDT', 'LTCUSDT', 'LINKUSDT', 'UNIUSDT', 'BCHUSDT', 'XLMUSDT',
            'ATOMUSDT', 'FILUSDT', 'ICPUSDT', 'VETUSDT', 'ETCUSDT', 'TRXUSDT', 'NEARUSDT',
            'ALGOUSDT', 'AAVEUSDT', 'MKRUSDT', 'THETAUSDT', 'XMRUSDT', 'EOSUSDT', 'AXSUSDT',
            'SANDUSDT', 'MANAUSDT', 'GRTUSDT', 'ENJUSDT', 'CHZUSDT', 'SNXUSDT', '1INCHUSDT',
            'CRVUSDT', 'BATUSDT', 'ZENUSDT', 'ZRXUSDT', 'OMGUSDT', 'SUSHIUSDT', 'COMPUSDT',
            'YFIUSDT', 'ALPHAUSDT', 'SKLUSDT', 'STORJUSDT', 'AUDIOUSDT', 'CTKUSDT',
            'AKROUSDT', 'CTSIUSDT', 'DATAUSDT', 'HBARUSDT', 'OCEANUSDT', 'BNTUSDT'
        ]),
        'okx': set([
            'BTCUSDT', 'ETHUSDT', 'XRPUSDT', 'ADAUSDT', 'SOLUSDT', 'DOTUSDT', 'DOGEUSDT',
            'AVAXUSDT', 'SHIBUSDT', 'LTCUSDT', 'LINKUSDT', 'UNIUSDT', 'BCHUSDT', 'XLMUSDT',
            'ATOMUSDT', 'FILUSDT', 'ICPUSDT', 'VETUSDT', 'ETCUSDT', 'TRXUSDT', 'NEARUSDT',
            'ALGOUSDT', 'AAVEUSDT', 'MKRUSDT', 'THETAUSDT', 'XMRUSDT', 'EOSUSDT', 'AXSUSDT',
            'SANDUSDT', 'MANAUSDT', 'GRTUSDT', 'ENJUSDT', 'CHZUSDT', 'SNXUSDT', '1INCHUSDT',
            'CRVUSDT', 'BATUSDT', 'ZENUSDT', 'ZRXUSDT', 'OMGUSDT', 'SUSHIUSDT', 'COMPUSDT',
            'YFIUSDT', 'ALPHAUSDT', 'SKLUSDT', 'STORJUSDT', 'AUDIOUSDT', 'CTKUSDT',
            'AKROUSDT', 'CTSIUSDT', 'DATAUSDT', 'HBARUSDT', 'OCEANUSDT', 'BNTUSDT'
        ]),
        'gate.io': set([
            'BTCUSDT', 'ETHUSDT', 'XRPUSDT', 'ADAUSDT', 'SOLUSDT', 'DOTUSDT', 'DOGEUSDT',
            'AVAXUSDT', 'SHIBUSDT', 'LTCUSDT', 'LINKUSDT', 'UNIUSDT', 'BCHUSDT', 'XLMUSDT',
            'ATOMUSDT', 'FILUSDT', 'ICPUSDT', 'VETUSDT', 'ETCUSDT', 'TRXUSDT', 'NEARUSDT',
            'ALGOUSDT', 'AAVEUSDT', 'MKRUSDT', 'THETAUSDT', 'XMRUSDT', 'EOSUSDT', 'AXSUSDT',
            'SANDUSDT', 'MANAUSDT', 'GRTUSDT', 'ENJUSDT', 'CHZUSDT', 'SNXUSDT', '1INCHUSDT',
            'CRVUSDT', 'BATUSDT', 'ZENUSDT', 'ZRXUSDT', 'OMGUSDT', 'SUSHIUSDT', 'COMPUSDT',
            'YFIUSDT', 'ALPHAUSDT', 'SKLUSDT', 'STORJUSDT', 'AUDIOUSDT', 'CTKUSDT',
            'AKROUSDT', 'CTSIUSDT', 'DATAUSDT', 'HBARUSDT', 'OCEANUSDT', 'BNTUSDT'
        ])
    }
    
    return symbols_by_exchange

def generate_trading_pairs(market_caps):
    """
    生成所有可能的交易對套利組合
    """
    symbols_by_exchange = get_unique_symbols_by_exchange()
    all_exchanges = list(symbols_by_exchange.keys())
    
    pair_data = []
    
    log_message("=" * 50)
    log_message("開始生成交易對套利組合...")
    
    # 計算所有可能的兩兩交易所組合
    for i in range(len(all_exchanges)):
        for j in range(i + 1, len(all_exchanges)):
            exchange_a = all_exchanges[i]
            exchange_b = all_exchanges[j]
            
            # 找到兩個交易所共同支持的 symbols
            common_symbols = symbols_by_exchange[exchange_a] & symbols_by_exchange[exchange_b]
            
            log_message(f"交易所組合: {exchange_a.upper()} ↔ {exchange_b.upper()}")
            log_message(f"  共同支持的交易對: {len(common_symbols)} 個")
            
            for symbol in common_symbols:
                # 從交易對中提取基礎幣種 (如從 BTCUSDT 提取 BTC)
                base_symbol = extract_base_symbol(symbol)
                market_cap = market_caps.get(base_symbol, 0)
                
                pair_data.append({
                    'Symbol': symbol,
                    'Exchange_A': exchange_a,
                    'Exchange_B': exchange_b,
                    'Market_Cap': market_cap
                })
                
                # 如果有市值資料，顯示一些範例
                if market_cap > 0:
                    log_message(f"    {symbol} ({base_symbol}): ${market_cap:,.0f}")
    
    log_message("=" * 50)
    log_message(f"✅ 生成完成，總共 {len(pair_data)} 個套利組合")

    return pair_data

def save_to_database(data):
    """保存交易對數據到數據庫"""
    if not data:
        log_message("❌ 沒有數據可保存")
        return

    df = pd.DataFrame(data)
    
    # 依市值排序（高 → 低），相同市值再按 Symbol 排序
    df = df.sort_values(['Market_Cap', 'Symbol'], ascending=[False, True])
    
    # 保存到數據庫
    db = DatabaseManager()
    inserted_count = db.insert_trading_pairs(df)
    
    log_message(f"✅ 已保存交易對數據到數據庫，共 {inserted_count} 筆套利組合")

def main():
    """主要執行流程"""
    log_message("🚀 開始生成交易對套利組合列表...")
    
    # (1) 讀取市值數據（從數據庫）
    market_caps = load_market_caps_from_database()
    
    if not market_caps:
        log_message("❌ 無法讀取市值數據，程序終止")
        return
    
    # (2) 生成所有交易對套利組合
    pair_data = generate_trading_pairs(market_caps)
    
    # (3) 保存到數據庫
    save_to_database(pair_data)

if __name__ == "__main__":
    main()