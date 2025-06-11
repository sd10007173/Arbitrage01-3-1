#!/usr/bin/env python3
"""
最小範圍測試工具
=================
自動化執行資金費率套利系統的完整流程，用最小數據集快速驗證系統功能

執行流程：
1. coingecko_market_cap (選擇前3名)
2. get_symbol_pair_v2
3. fetch_FR_history_group_v1 (2025-06-01 到 2025-06-05)
4. calculate_FR_diff_v1
5. calculate_FR_return_list
6. strategy_ranking (全部策略)
7. backtest_v2 (全部策略, 2025-06-01 到 2025-06-05)
"""

import os
import sys
import subprocess
import time
from datetime import datetime
import tempfile
import shutil

class MinimumTestRunner:
    def __init__(self):
        self.start_time = datetime.now()
        self.test_dates = {
            'start_date': '2024-01-01',
            'end_date': '2024-01-05'
        }
        self.results = []
        self.current_step = 0
        self.total_steps = 7
        
    def print_header(self):
        """打印測試工具標題"""
        print("=" * 80)
        print("🚀 最小範圍測試工具")
        print("=" * 80)
        print(f"📅 測試日期範圍: {self.test_dates['start_date']} ~ {self.test_dates['end_date']}")
        print(f"🕐 開始時間: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"📊 總步驟數: {self.total_steps}")
        print("=" * 80)
        print()
        
    def print_step(self, step_name, description):
        """打印當前執行步驟"""
        self.current_step += 1
        print(f"📋 [{self.current_step}/{self.total_steps}] {step_name}")
        print(f"   {description}")
        print(f"   ⏰ {datetime.now().strftime('%H:%M:%S')}")
        print("-" * 60)
        
    def run_command_with_input(self, command, input_text=None, timeout=300):
        """執行命令並可選擇提供輸入"""
        try:
            if input_text:
                # 使用 Popen 來處理需要輸入的命令
                process = subprocess.Popen(
                    command,
                    shell=True,
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    cwd=os.getcwd()
                )
                stdout, stderr = process.communicate(input=input_text, timeout=timeout)
                return_code = process.returncode
            else:
                # 簡單命令執行
                result = subprocess.run(
                    command,
                    shell=True,
                    capture_output=True,
                    text=True,
                    timeout=timeout,
                    cwd=os.getcwd()
                )
                stdout = result.stdout
                stderr = result.stderr
                return_code = result.returncode
            
            return return_code, stdout, stderr
            
        except subprocess.TimeoutExpired:
            return -1, '', f'命令執行超時 ({timeout}秒)'
        except Exception as e:
            return -1, '', f'執行錯誤: {str(e)}'
    
    def modify_file_dates(self, filename, start_date_var, end_date_var, start_date, end_date):
        """修改文件中的日期參數"""
        if not os.path.exists(filename):
            print(f"❌ 文件不存在: {filename}")
            return False
            
        try:
            # 備份原文件
            backup_file = f"{filename}.backup"
            shutil.copy2(filename, backup_file)
            
            # 讀取文件內容
            with open(filename, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # 替換日期參數
            original_content = content
            if start_date_var in content:
                # 尋找並替換 DEFAULT_START_DATE 或 START_DATE
                import re
                pattern = rf'{start_date_var}\s*=\s*"[^"]*"'
                replacement = f'{start_date_var} = "{start_date}"'
                content = re.sub(pattern, replacement, content)
            
            if end_date_var in content:
                # 尋找並替換 DEFAULT_END_DATE 或 END_DATE
                import re
                pattern = rf'{end_date_var}\s*=\s*"[^"]*"'
                replacement = f'{end_date_var} = "{end_date}"'
                content = re.sub(pattern, replacement, content)
            
            # 如果有修改，寫入文件
            if content != original_content:
                with open(filename, 'w', encoding='utf-8') as f:
                    f.write(content)
                print(f"✅ 已修改 {filename} 的日期參數")
                return True
            else:
                print(f"⚠️  {filename} 中未找到需要修改的日期參數")
                return True
                
        except Exception as e:
            print(f"❌ 修改文件失敗: {e}")
            # 恢復備份
            if os.path.exists(backup_file):
                shutil.copy2(backup_file, filename)
            return False
    
    def restore_file(self, filename):
        """恢復文件的原始版本"""
        backup_file = f"{filename}.backup"
        if os.path.exists(backup_file):
            shutil.copy2(backup_file, filename)
            os.remove(backup_file)
            print(f"✅ 已恢復 {filename} 的原始內容")
    
    def step1_coingecko_market_cap(self):
        """步驟1: 執行 coingecko_market_cap，輸入3"""
        self.print_step("coingecko_market_cap", "獲取前3名市值的加密貨幣")
        
        return_code, stdout, stderr = self.run_command_with_input(
            "python3 coingecko_market_cap.py",
            input_text="3\n",
            timeout=120
        )
        
        success = return_code == 0
        self.results.append({
            'step': 'coingecko_market_cap',
            'success': success,
            'output': stdout,
            'error': stderr
        })
        
        if success:
            print("✅ coingecko_market_cap 執行成功")
        else:
            print(f"❌ coingecko_market_cap 執行失敗: {stderr}")
        
        print()
        return success
    
    def step2_get_symbol_pair_v2(self):
        """步驟2: 執行 get_symbol_pair_v2"""
        self.print_step("get_symbol_pair_v2", "生成交易對列表")
        
        return_code, stdout, stderr = self.run_command_with_input(
            "python3 get_symbol_pair_v2.py",
            timeout=60
        )
        
        success = return_code == 0
        self.results.append({
            'step': 'get_symbol_pair_v2',
            'success': success,
            'output': stdout,
            'error': stderr
        })
        
        if success:
            print("✅ get_symbol_pair_v2 執行成功")
        else:
            print(f"❌ get_symbol_pair_v2 執行失敗: {stderr}")
        
        print()
        return success
    
    def step3_fetch_fr_history(self):
        """步驟3: 執行 fetch_FR_history_group_v1"""
        self.print_step("fetch_FR_history_group_v1", f"獲取資金費率歷史數據 ({self.test_dates['start_date']} ~ {self.test_dates['end_date']})")
        
        # 修改日期參數
        file_modified = self.modify_file_dates(
            'fetch_FR_history_group_v1.py',
            'DEFAULT_START_DATE',
            'DEFAULT_END_DATE',
            self.test_dates['start_date'],
            self.test_dates['end_date']
        )
        
        if not file_modified:
            self.results.append({
                'step': 'fetch_FR_history_group_v1',
                'success': False,
                'output': '',
                'error': '無法修改日期參數'
            })
            return False
        
        return_code, stdout, stderr = self.run_command_with_input(
            "python3 fetch_FR_history_group_v1.py",
            timeout=300
        )
        
        success = return_code == 0
        self.results.append({
            'step': 'fetch_FR_history_group_v1',
            'success': success,
            'output': stdout,
            'error': stderr
        })
        
        if success:
            print("✅ fetch_FR_history_group_v1 執行成功")
        else:
            print(f"❌ fetch_FR_history_group_v1 執行失敗: {stderr}")
        
        # 恢復原始文件
        self.restore_file('fetch_FR_history_group_v1.py')
        
        print()
        return success
    
    def step4_calculate_fr_diff(self):
        """步驟4: 執行 calculate_FR_diff_v1"""
        self.print_step("calculate_FR_diff_v1", "計算交易所間資金費率差異")
        
        return_code, stdout, stderr = self.run_command_with_input(
            "python3 calculate_FR_diff_v1.py",
            timeout=120
        )
        
        success = return_code == 0
        self.results.append({
            'step': 'calculate_FR_diff_v1',
            'success': success,
            'output': stdout,
            'error': stderr
        })
        
        if success:
            print("✅ calculate_FR_diff_v1 執行成功")
        else:
            print(f"❌ calculate_FR_diff_v1 執行失敗: {stderr}")
        
        print()
        return success
    
    def step5_calculate_fr_return(self):
        """步驟5: 執行 calculate_FR_return_list"""
        self.print_step("calculate_FR_return_list", "計算各時間週期收益率")
        
        return_code, stdout, stderr = self.run_command_with_input(
            "python3 calculate_FR_return_list.py",
            timeout=120
        )
        
        success = return_code == 0
        self.results.append({
            'step': 'calculate_FR_return_list',
            'success': success,
            'output': stdout,
            'error': stderr
        })
        
        if success:
            print("✅ calculate_FR_return_list 執行成功")
        else:
            print(f"❌ calculate_FR_return_list 執行失敗: {stderr}")
        
        print()
        return success
    
    def step6_strategy_ranking(self):
        """步驟6: 執行 strategy_ranking，輸入7"""
        self.print_step("strategy_ranking", "執行全部策略排名")
        
        return_code, stdout, stderr = self.run_command_with_input(
            "python3 strategy_ranking.py",
            input_text="7\n",
            timeout=180
        )
        
        success = return_code == 0
        self.results.append({
            'step': 'strategy_ranking',
            'success': success,
            'output': stdout,
            'error': stderr
        })
        
        if success:
            print("✅ strategy_ranking 執行成功")
        else:
            print(f"❌ strategy_ranking 執行失敗: {stderr}")
        
        print()
        return success
    
    def step7_backtest_v2(self):
        """步驟7: 執行 backtest_v2"""
        self.print_step("backtest_v2", f"執行回測 ({self.test_dates['start_date']} ~ {self.test_dates['end_date']})")
        
        # 修改日期參數
        file_modified = self.modify_file_dates(
            'backtest_v2.py',
            'START_DATE',
            'END_DATE',
            self.test_dates['start_date'],
            self.test_dates['end_date']
        )
        
        if not file_modified:
            self.results.append({
                'step': 'backtest_v2',
                'success': False,
                'output': '',
                'error': '無法修改日期參數'
            })
            return False
        
        return_code, stdout, stderr = self.run_command_with_input(
            "python3 backtest_v2.py",
            input_text="7\n",
            timeout=300
        )
        
        success = return_code == 0
        self.results.append({
            'step': 'backtest_v2',
            'success': success,
            'output': stdout,
            'error': stderr
        })
        
        if success:
            print("✅ backtest_v2 執行成功")
        else:
            print(f"❌ backtest_v2 執行失敗: {stderr}")
        
        # 恢復原始文件
        self.restore_file('backtest_v2.py')
        
        print()
        return success
    
    def print_summary(self):
        """打印測試總結"""
        end_time = datetime.now()
        duration = end_time - self.start_time
        
        print("=" * 80)
        print("📊 測試結果總結")
        print("=" * 80)
        
        successful_steps = sum(1 for result in self.results if result['success'])
        total_steps = len(self.results)
        
        print(f"⏱️  總執行時間: {duration}")
        print(f"✅ 成功步驟: {successful_steps}/{total_steps}")
        print(f"❌ 失敗步驟: {total_steps - successful_steps}/{total_steps}")
        print()
        
        # 顯示各步驟狀態
        for i, result in enumerate(self.results, 1):
            status = "✅" if result['success'] else "❌"
            print(f"{status} [{i}] {result['step']}")
            if not result['success'] and result['error']:
                print(f"    錯誤: {result['error'][:100]}...")
        
        print()
        
        if successful_steps == total_steps:
            print("🎉 全部測試通過！系統運行正常")
        else:
            print("⚠️  部分測試失敗，請檢查錯誤信息")
        
        print("=" * 80)
    
    def run_all_tests(self):
        """執行所有測試步驟"""
        self.print_header()
        
        try:
            # 執行各個步驟
            steps = [
                self.step1_coingecko_market_cap,
                self.step2_get_symbol_pair_v2,
                self.step3_fetch_fr_history,
                self.step4_calculate_fr_diff,
                self.step5_calculate_fr_return,
                self.step6_strategy_ranking,
                self.step7_backtest_v2
            ]
            
            for step_func in steps:
                success = step_func()
                if not success:
                    print(f"⚠️  步驟失敗，但繼續執行後續步驟...")
                    print()
            
            self.print_summary()
            
        except KeyboardInterrupt:
            print("\n⛔ 用戶中斷測試")
            self.print_summary()
        except Exception as e:
            print(f"\n❌ 測試過程中發生未預期錯誤: {e}")
            import traceback
            traceback.print_exc()
            self.print_summary()

def main():
    """主函數"""
    if len(sys.argv) > 1 and sys.argv[1] in ['-h', '--help']:
        print(__doc__)
        return
    
    print("🚀 準備開始最小範圍測試...")
    print("📝 測試範圍: 2024-01-01 ~ 2024-01-05 (5天)")
    print("⚠️  注意: 此測試會修改部分腳本的日期參數，執行完後會自動恢復")
    print()
    
    response = input("是否開始測試? (y/N): ").strip().lower()
    if response not in ['y', 'yes']:
        print("❌ 測試已取消")
        return
    
    runner = MinimumTestRunner()
    runner.run_all_tests()

if __name__ == "__main__":
    main() 