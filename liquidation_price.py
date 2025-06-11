def calculate_liquidation_prices():
    """
    計算做多和做空的爆倉價格
    """
    print("=== 爆倉價計算器 ===")
    print()

    try:
        # 輸入參數
        long_entry = float(input("請輸入做多進場價: "))
        short_entry = float(input("請輸入做空進場價: "))
        leverage = float(input("請輸入槓桿倍數: "))

        # 檢查輸入是否有效
        if long_entry <= 0 or short_entry <= 0:
            print("錯誤：進場價必須大於 0")
            return

        if leverage <= 0:
            print("錯誤：槓桿倍數必須大於 0")
            return

        # 計算爆倉價
        long_liquidation = long_entry * (1 - 1 / leverage)
        short_liquidation = short_entry * (1 + 1 / leverage)

        # 輸出結果
        print("\n=== 計算結果 ===")
        print(f"做多進場價: {long_entry:.4f}")
        print(f"做多爆倉價: {long_liquidation:.4f}")
        print(f"做空進場價: {short_entry:.4f}")
        print(f"做空爆倉價: {short_liquidation:.4f}")
        print(f"槓桿倍數: {leverage}x")

        # 計算風險百分比
        long_risk_percent = (1 / leverage) * 100
        short_risk_percent = (1 / leverage) * 100

        print(f"\n風險提醒:")
        print(f"做多最大虧損: {long_risk_percent:.2f}%")
        print(f"做空最大虧損: {short_risk_percent:.2f}%")

    except ValueError:
        print("錯誤：請輸入有效的數字")
    except ZeroDivisionError:
        print("錯誤：槓桿倍數不能為 0")
    except Exception as e:
        print(f"發生錯誤: {e}")


def batch_calculate():
    """
    批量計算多組數據
    """
    print("=== 批量計算模式 ===")
    print("輸入格式: 做多進場價,做空進場價,槓桿")
    print("輸入 'quit' 結束")
    print()

    while True:
        user_input = input("請輸入數據 (或輸入 quit 結束): ").strip()

        if user_input.lower() == 'quit':
            break

        try:
            values = user_input.split(',')
            if len(values) != 3:
                print("錯誤：請輸入三個數值，用逗號分隔")
                continue

            long_entry = float(values[0].strip())
            short_entry = float(values[1].strip())
            leverage = float(values[2].strip())

            # 計算爆倉價
            long_liquidation = long_entry * (1 - 1 / leverage)
            short_liquidation = short_entry * (1 + 1 / leverage)

            print(f"做多: {long_entry} → 爆倉價 {long_liquidation:.4f}")
            print(f"做空: {short_entry} → 爆倉價 {short_liquidation:.4f}")
            print("-" * 40)

        except (ValueError, ZeroDivisionError) as e:
            print(f"輸入錯誤: {e}")
        except Exception as e:
            print(f"發生錯誤: {e}")


if __name__ == "__main__":
    print("請選擇模式:")
    print("1. 單次計算")
    print("2. 批量計算")

    choice = input("請輸入選擇 (1 或 2): ").strip()

    if choice == "1":
        calculate_liquidation_prices()
    elif choice == "2":
        batch_calculate()
    else:
        print("無效選擇，使用單次計算模式")
        calculate_liquidation_prices()