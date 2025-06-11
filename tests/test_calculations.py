# tests/test_calculations.py
import pandas as pd
import os
import tempfile
from config import TEST_CONFIG

# 嘗試 import 你的主程式
try:
    from calculate_FR_diff_v1 import compute_diff_for_pair

    print("✅ 成功載入 calculate_FR_diff_v1")
except ImportError as e:
    print(f"❌ 無法載入 calculate_FR_diff_v1: {e}")
    print("   請確認檔案存在且可正常執行")


class DirectFunctionTester:
    def __init__(self):
        self.test_results = []
        self.temp_files = []  # 記錄臨時檔案，測試完要清理

    def test_funding_rate_diff_calculation(self):
        """測試資金費率差異計算邏輯"""
        print("🧮 開始測試資金費率差異計算...")

        # === 測試案例1：基本差異計算 ===
        print("\n   測試案例1：基本差異計算")

        # 建立測試數據A（Binance）
        data_a = pd.DataFrame({
            'Timestamp (UTC)': [
                '2025-01-15 08:00:00',
                '2025-01-15 16:00:00',
                '2025-01-16 00:00:00'
            ],
            'Exchange': ['Binance', 'Binance', 'Binance'],
            'Symbol': ['TESTUSDT', 'TESTUSDT', 'TESTUSDT'],
            'FundingRate': [0.00010000, 0.00020000, 0.00030000]  # 8位小數精度
        })

        # 建立測試數據B（Bybit）
        data_b = pd.DataFrame({
            'Timestamp (UTC)': [
                '2025-01-15 08:00:00',
                '2025-01-15 16:00:00',
                '2025-01-16 00:00:00'
            ],
            'Exchange': ['Bybit', 'Bybit', 'Bybit'],
            'Symbol': ['TESTUSDT', 'TESTUSDT', 'TESTUSDT'],
            'FundingRate': [0.00030000, 0.00010000, 0.00020000]  # 8位小數精度
        })

        # 預期的差異結果（手動計算）
        expected_diffs = [
            0.00010000 - 0.00030000,  # -0.00020000
            0.00020000 - 0.00010000,  # 0.00010000
            0.00030000 - 0.00020000  # 0.00010000
        ]

        # 建立臨時測試檔案
        temp_file_a = os.path.join(TEST_CONFIG["temp_path"], "test_binance.csv")
        temp_file_b = os.path.join(TEST_CONFIG["temp_path"], "test_bybit.csv")

        data_a.to_csv(temp_file_a, index=False, float_format='%.8f')
        data_b.to_csv(temp_file_b, index=False, float_format='%.8f')

        self.temp_files.extend([temp_file_a, temp_file_b])

        try:
            # 調用你的實際函數
            result_df = compute_diff_for_pair(
                temp_file_a,
                temp_file_b,
                "TESTUSDT",
                "Binance",
                "Bybit"
            )

            print(f"     ✅ 函數執行成功，回傳 {len(result_df)} 筆資料")

            # 驗證每一筆計算結果
            all_correct = True
            for i, expected_diff in enumerate(expected_diffs):
                if i < len(result_df):
                    actual_diff = result_df.iloc[i]['Diff_AB']
                    error = abs(actual_diff - expected_diff)
                    is_correct = error <= TEST_CONFIG["precision_tolerance"]

                    if not is_correct:
                        all_correct = False

                    status = "✅" if is_correct else "❌"
                    print(
                        f"     時間點{i + 1}: 預期 {expected_diff:.8f}, 實際 {actual_diff:.8f}, 誤差 {error:.2e} {status}")
                else:
                    print(f"     ❌ 缺少第{i + 1}筆資料")
                    all_correct = False

            test_result = {
                "test_name": "基本差異計算",
                "passed": all_correct,
                "details": f"測試了{len(expected_diffs)}筆計算"
            }

            if all_correct:
                print("     🎉 測試案例1 全部通過！")
            else:
                print("     ⚠️ 測試案例1 有問題，請檢查計算邏輯")

        except Exception as e:
            print(f"     ❌ 函數執行失敗: {e}")
            test_result = {
                "test_name": "基本差異計算",
                "passed": False,
                "error": str(e)
            }

        self.test_results.append(test_result)
        return test_result["passed"]

    def test_extreme_values(self):
        """測試極端值情況"""
        print("\n🔥 開始測試極端值情況...")

        # === 測試案例2：極端值 ===
        print("   測試案例2：極端值計算")

        # 極端測試數據
        extreme_data_a = pd.DataFrame({
            'Timestamp (UTC)': [
                '2025-01-15 08:00:00',
                '2025-01-15 16:00:00'
            ],
            'Exchange': ['Binance', 'Binance'],
            'Symbol': ['EXTREMEUSDT', 'EXTREMEUSDT'],
            'FundingRate': [0.01000000, -0.00500000]  # 1% 和 -0.5%（極端值）
        })

        extreme_data_b = pd.DataFrame({
            'Timestamp (UTC)': [
                '2025-01-15 08:00:00',
                '2025-01-15 16:00:00'
            ],
            'Exchange': ['Bybit', 'Bybit'],
            'Symbol': ['EXTREMEUSDT', 'EXTREMEUSDT'],
            'FundingRate': [-0.00750000, 0.00250000]  # -0.75% 和 0.25%
        })

        expected_extreme_diffs = [
            0.01000000 - (-0.00750000),  # 0.01750000 (1.75%)
            -0.00500000 - 0.00250000  # -0.00750000 (-0.75%)
        ]

        temp_file_a = os.path.join(TEST_CONFIG["temp_path"], "test_extreme_binance.csv")
        temp_file_b = os.path.join(TEST_CONFIG["temp_path"], "test_extreme_bybit.csv")

        extreme_data_a.to_csv(temp_file_a, index=False, float_format='%.8f')
        extreme_data_b.to_csv(temp_file_b, index=False, float_format='%.8f')

        self.temp_files.extend([temp_file_a, temp_file_b])

        try:
            result_df = compute_diff_for_pair(
                temp_file_a,
                temp_file_b,
                "EXTREMEUSDT",
                "Binance",
                "Bybit"
            )

            all_correct = True
            for i, expected_diff in enumerate(expected_extreme_diffs):
                if i < len(result_df):
                    actual_diff = result_df.iloc[i]['Diff_AB']
                    error = abs(actual_diff - expected_diff)
                    is_correct = error <= TEST_CONFIG["precision_tolerance"]

                    if not is_correct:
                        all_correct = False

                    status = "✅" if is_correct else "❌"
                    print(f"     極端值{i + 1}: 預期 {expected_diff:.8f}, 實際 {actual_diff:.8f} {status}")
                else:
                    all_correct = False

            if all_correct:
                print("     🎉 極端值測試通過！")
            else:
                print("     ⚠️ 極端值測試有問題")

            test_result = {
                "test_name": "極端值計算",
                "passed": all_correct
            }

        except Exception as e:
            print(f"     ❌ 極端值測試失敗: {e}")
            test_result = {
                "test_name": "極端值計算",
                "passed": False,
                "error": str(e)
            }

        self.test_results.append(test_result)
        return test_result["passed"]

    def test_zero_differences(self):
        """測試零差異情況"""
        print("\n⚖️ 開始測試零差異情況...")

        # === 測試案例3：相同費率 ===
        print("   測試案例3：零差異計算")

        same_data = pd.DataFrame({
            'Timestamp (UTC)': ['2025-01-15 08:00:00'],
            'Exchange': ['Test'],
            'Symbol': ['ZEROUSCDT'],
            'FundingRate': [0.00015000]
        })

        temp_file_a = os.path.join(TEST_CONFIG["temp_path"], "test_zero_a.csv")
        temp_file_b = os.path.join(TEST_CONFIG["temp_path"], "test_zero_b.csv")

        same_data.to_csv(temp_file_a, index=False, float_format='%.8f')
        same_data.to_csv(temp_file_b, index=False, float_format='%.8f')

        self.temp_files.extend([temp_file_a, temp_file_b])

        try:
            result_df = compute_diff_for_pair(
                temp_file_a,
                temp_file_b,
                "ZEROUSDT",
                "ExchangeA",
                "ExchangeB"
            )

            if len(result_df) > 0:
                actual_diff = result_df.iloc[0]['Diff_AB']
                is_zero = abs(actual_diff) <= TEST_CONFIG["precision_tolerance"]

                print(f"     零差異結果: {actual_diff:.8f}")

                if is_zero:
                    print("     ✅ 零差異測試通過！")
                    test_passed = True
                else:
                    print("     ❌ 零差異測試失敗，應該要是0")
                    test_passed = False
            else:
                print("     ❌ 沒有回傳資料")
                test_passed = False

            test_result = {
                "test_name": "零差異計算",
                "passed": test_passed
            }

        except Exception as e:
            print(f"     ❌ 零差異測試失敗: {e}")
            test_result = {
                "test_name": "零差異計算",
                "passed": False,
                "error": str(e)
            }

        self.test_results.append(test_result)
        return test_result["passed"]

    def cleanup_temp_files(self):
        """清理臨時檔案"""
        print("\n🧹 清理臨時檔案...")
        for temp_file in self.temp_files:
            try:
                if os.path.exists(temp_file):
                    os.remove(temp_file)
                    print(f"   ✅ 刪除 {temp_file}")
            except Exception as e:
                print(f"   ⚠️ 無法刪除 {temp_file}: {e}")

    def run_all_tests(self):
        """執行所有測試"""
        print("🚀 開始直接函數測試")
        print("=" * 50)

        try:
            # 執行所有測試
            test1 = self.test_funding_rate_diff_calculation()
            test2 = self.test_extreme_values()
            test3 = self.test_zero_differences()

            # 統計結果
            total_tests = len(self.test_results)
            passed_tests = sum(1 for result in self.test_results if result["passed"])

            print("\n" + "=" * 50)
            print("📋 測試總結:")
            print(f"   總測試數: {total_tests}")
            print(f"   通過數: {passed_tests}")
            print(f"   通過率: {passed_tests / total_tests * 100:.1f}%")

            if passed_tests == total_tests:
                print("🎉 所有直接函數測試通過！你的計算邏輯正確")
                return True
            else:
                print("⚠️ 有測試失敗，請檢查計算邏輯")

                # 顯示失敗詳情
                print("\n❌ 失敗的測試:")
                for result in self.test_results:
                    if not result["passed"]:
                        print(f"   - {result['test_name']}")
                        if "error" in result:
                            print(f"     錯誤: {result['error']}")

                return False

        finally:
            # 一定要清理臨時檔案
            self.cleanup_temp_files()


def main():
    """主執行函數"""
    tester = DirectFunctionTester()
    success = tester.run_all_tests()

    if success:
        print("\n✨ 恭喜！你的核心計算邏輯測試通過")
        print("   可以放心使用這些函數進行交易計算")
    else:
        print("\n🔧 建議先修復發現的問題再進行實際交易")


if __name__ == "__main__":
    main()