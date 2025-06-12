# fetch_market_caps.py
import os
import time
import requests
import pandas as pd
import math
from database_operations import DatabaseManager


def log_message(message):
    """輸出日誌訊息"""
    print(f"📝 {message}")


def get_user_input():
    """獲取使用者輸入的市值排行數量"""
    while True:
        try:
            num = int(input("請輸入欲查詢市值排行前幾名的數據: "))
            if num <= 0:
                print("❌ 請輸入大於 0 的正整數")
                continue
            return num
        except ValueError:
            print("❌ 請輸入有效的數字")


def get_market_caps_from_coingecko(page=1, per_page=250, max_retries=3):
    """
    從 CoinGecko API 獲取加密貨幣市值資料
    
    Args:
        page: 頁數，從 1 開始
        per_page: 每頁資料數量，最大 250
        max_retries: 最大重試次數
    
    Returns:
        list: 包含市值資料的列表
    """
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        'vs_currency': 'usd',
        'order': 'market_cap_desc',
        'page': page,
        'per_page': per_page,
        'sparkline': 'false'
    }
    
    for attempt in range(max_retries):
        try:
            response = requests.get(url, params=params, timeout=15)
            response.raise_for_status()
            data = response.json()
            
            # 將 symbol 轉為大寫，便於後續處理
            for item in data:
                item['symbol'] = item['symbol'].upper()
                
            return data
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:  # Too Many Requests
                wait_time = (attempt + 1) * 3  # 遞增等待時間：3, 6, 9 秒
                print(f"⚠️ API 限制觸發，等待 {wait_time} 秒後重試... (嘗試 {attempt + 1}/{max_retries})")
                time.sleep(wait_time)
                continue
            else:
                print(f"❌ HTTP 錯誤: {e}")
                return []
        except requests.RequestException as e:
            print(f"❌ API 請求失敗: {e}")
            if attempt < max_retries - 1:
                print(f"⚠️ 3 秒後重試... (嘗試 {attempt + 1}/{max_retries})")
                time.sleep(3)
                continue
            return []
        except Exception as e:
            print(f"❌ 處理資料時發生錯誤: {e}")
            return []
    
    return []


def save_market_caps_to_database(all_data, count):
    """
    將從 CoinGecko 查詢取得的市值資料存入數據庫
    
    Args:
        all_data: 從 API 取得的完整資料列表
        count: 要保存的前N筆資料
    """
    if not all_data:
        print("❌ 沒有資料可保存")
        return
    
    # 取前 count 筆資料
    selected_data = all_data[:count]
    
    # 轉換為 DataFrame
    df = pd.DataFrame(selected_data)
    
    # 保存到數據庫
    db = DatabaseManager()
    inserted_count = db.insert_market_caps(df)
    
    print(f"✅ 市值資料已存入數據庫: {inserted_count} 筆")
    print(f"📊 排名範圍: 第 1 至第 {len(selected_data)} 名")


def main():
    """主程式流程"""
    print("🚀 CoinGecko 市值資料獲取工具")
    print("=" * 50)
    
    try:
        target_count = int(input("請輸入要取得的市值排名前幾名（建議 50-1000）: "))
        if target_count <= 0:
            print("❌ 請輸入大於 0 的正整數")
            return
        if target_count > 10000:
            print("⚠️ 數量過大，建議不超過 10000，繼續執行可能會很慢...")
            confirm = input("是否繼續執行？(y/N): ")
            if confirm.lower() != 'y':
                print("❌ 已取消執行")
                return
    except ValueError:
        print("❌ 請輸入有效的數字")
        return
    
    print(f"📊 開始獲取前 {target_count} 名加密貨幣市值資料...")
    
    # 計算需要幾頁
    per_page = 250
    pages_needed = (target_count + per_page - 1) // per_page
    
    if pages_needed > 1:
        print(f"📄 需要獲取 {pages_needed} 頁資料，預計耗時約 {pages_needed * 2} 秒")
    
    all_market_caps_data = []
    
    for page in range(1, pages_needed + 1):
        print(f"📄 正在獲取第 {page} 頁資料...")
        
        page_data = get_market_caps_from_coingecko(page=page, per_page=per_page)
        if page_data:
            all_market_caps_data.extend(page_data)
            print(f"✅ 第 {page} 頁獲取成功，共 {len(page_data)} 筆")
        else:
            print(f"❌ 第 {page} 頁獲取失敗")
            break
        
        # API 限制：避免請求過快，增加延遲避免 429 錯誤
        if page < pages_needed:
            delay = 2 if target_count > 1000 else 1.5
            print(f"⏱️ 等待 {delay} 秒避免 API 限制...")
            time.sleep(delay)
    
    if all_market_caps_data:
        print(f"✅ 總共獲取 {len(all_market_caps_data)} 筆市值資料")
        
        # 儲存到數據庫
        save_market_caps_to_database(all_market_caps_data, target_count)
    else:
        print("❌ 未能獲取任何資料")


if __name__ == "__main__":
    main()