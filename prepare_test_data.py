import sqlite3
import os

# --- Configuration ---
DB_FILE = "data/funding_rate.db"

# A complete test suite with pre-filled data based on the user's scenarios.
# This simulates the state AFTER exchange_trading_pair_v9.py has run.
TEST_DATA = [
    {
        "symbol": "BTC", "trading_pair": "BTCUSDT",
        "binance_support": 1, "binance_list_date": "2019-09-09",
        "bybit_support": 1, "bybit_list_date": "2019-12-25"
    },
    {
        "symbol": "NOT", "trading_pair": "NOTUSDT",
        "binance_support": 1, "binance_list_date": "2024-05-16",
        "bybit_support": 1, "bybit_list_date": "2024-05-16"
    },
    {
        "symbol": "ZRO", "trading_pair": "ZROUSDT",
        "binance_support": 1, "binance_list_date": "2024-06-20",
        "bybit_support": 1, "bybit_list_date": "2024-06-20"
    },
    {
        "symbol": "LUNA", "trading_pair": "LUNAUSDT",
        "binance_support": 0, "binance_list_date": None,
        "bybit_support": 0, "bybit_list_date": None
    },
    {
        "symbol": "KMNO", "trading_pair": "KMNOUSDT",
        "binance_support": 0, "binance_list_date": None,  # Not listed on Binance
        "bybit_support": 1, "bybit_list_date": "2024-04-30"
    }
]

def prepare_test_data():
    """
    Connects to the database and seeds it with a complete set of test data,
    pre-filling support and list_date fields to test the fetcher script directly.
    """
    if not os.path.exists(DB_FILE):
        print(f"錯誤：資料庫檔案 '{DB_FILE}' 不存在。")
        return

    conn = None
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()

        print("正在植入完整的測試資料...")

        for record in TEST_DATA:
            # Step 1: Ensure the symbol exists.
            cursor.execute(
                "INSERT OR IGNORE INTO trading_pair (symbol, trading_pair) VALUES (?, ?)",
                (record["symbol"], record["trading_pair"])
            )

            # Step 2: Update the record with the pre-filled test data.
            cursor.execute(
                """
                UPDATE trading_pair
                SET
                    binance_support = ?,
                    binance_list_date = ?,
                    bybit_support = ?,
                    bybit_list_date = ?
                WHERE
                    symbol = ?
                """,
                (
                    record["binance_support"],
                    record["binance_list_date"],
                    record["bybit_support"],
                    record["bybit_list_date"],
                    record["symbol"]
                )
            )

        conn.commit()
        
        print(f"\n成功植入或更新了 {len(TEST_DATA)} 筆測試資料。")
        print("資料庫已準備就緒，您可以直接運行 'fetch_FR_history_group_v2.py'。")

    except sqlite3.Error as e:
        print(f"操作資料庫時發生錯誤: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    prepare_test_data() 