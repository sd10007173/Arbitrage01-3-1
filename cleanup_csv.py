#!/usr/bin/env python3
"""
CSV 文件清理工具
幫助刪除不需要的舊 CSV 文件，加速數據庫遷移
"""

import os
import shutil
from datetime import datetime, timedelta
import glob

def analyze_csv_structure():
    """分析 CSV 文件結構"""
    print("📊 分析 CSV 文件結構...")
    print("=" * 60)
    
    # 統計各目錄的文件數
    directories = [
        'csv/FR_history',
        'csv/FR_diff', 
        'csv/FR_return_list',
        'csv/strategy_ranking',
        'csv/Backtest',
        'csv/Return'
    ]
    
    total_files = 0
    for directory in directories:
        if os.path.exists(directory):
            csv_files = glob.glob(os.path.join(directory, "**", "*.csv"), recursive=True)
            file_count = len(csv_files)
            total_files += file_count
            
            # 計算總大小
            total_size = sum(os.path.getsize(f) for f in csv_files) / (1024**2)  # MB
            
            print(f"{directory:<25} {file_count:>6} 個文件 ({total_size:>8.1f} MB)")
            
            # 顯示子目錄
            subdirs = glob.glob(os.path.join(directory, "*/"))
            if subdirs:
                print(f"  📁 子目錄: {len(subdirs)} 個")
                for subdir in sorted(subdirs)[:3]:  # 只顯示前3個
                    subdir_files = len(glob.glob(os.path.join(subdir, "*.csv")))
                    print(f"    - {os.path.basename(subdir.rstrip('/'))}: {subdir_files} 個文件")
                if len(subdirs) > 3:
                    print(f"    ... 還有 {len(subdirs)-3} 個子目錄")
            print()
    
    print(f"📊 總計: {total_files:,} 個 CSV 文件")

def suggest_cleanup_plan():
    """建議清理計劃"""
    print("\n🎯 建議的清理計劃:")
    print("=" * 60)
    
    today = datetime.now()
    cutoff_date = today - timedelta(days=30)
    
    print("📅 日期策略:")
    print(f"  • 保留: {today.strftime('%Y-%m-%d')} 起最近 30 天")
    print(f"  • 刪除: {cutoff_date.strftime('%Y-%m-%d')} 之前的數據")
    
    print("\n🗂️ 目錄策略:")
    print("  • FR_history: 保留最新備份目錄 + 當前文件")
    print("  • FR_return_list: 保留最近 30 天")
    print("  • strategy_ranking: 保留最近 30 天")
    print("  • 其他: 根據需要決定")

def create_cleanup_commands():
    """創建清理命令 (供用戶選擇執行)"""
    print("\n🔧 清理命令 (請選擇性執行):")
    print("=" * 60)
    
    commands = []
    
    # 1. 刪除舊的備份目錄
    print("1️⃣ 刪除舊的資金費率備份目錄:")
    backup_dirs = glob.glob("csv/FR_history/*_backup/")
    if len(backup_dirs) > 1:
        # 保留最新的，刪除其他的
        sorted_dirs = sorted(backup_dirs, reverse=True)
        for old_dir in sorted_dirs[1:]:  # 跳過最新的
            cmd = f"rm -rf '{old_dir}'"
            print(f"  {cmd}")
            commands.append(cmd)
    
    # 2. 刪除舊的收益指標文件
    print("\n2️⃣ 刪除 30 天前的收益指標文件:")
    cutoff_date = datetime.now() - timedelta(days=30)
    return_files = glob.glob("csv/FR_return_list/FR_return_list_*.csv")
    
    old_files = []
    for file in return_files:
        # 從文件名提取日期
        try:
            date_str = os.path.basename(file).replace("FR_return_list_", "").replace(".csv", "")
            file_date = datetime.strptime(date_str, "%Y-%m-%d")
            if file_date < cutoff_date:
                old_files.append(file)
        except:
            continue
    
    if old_files:
        print(f"  # 將刪除 {len(old_files)} 個舊文件")
        for file in old_files[:5]:  # 只顯示前5個
            print(f"  rm '{file}'")
        if len(old_files) > 5:
            print(f"  # ... 還有 {len(old_files)-5} 個文件")
        
        # 批量刪除命令
        cmd = f"find csv/FR_return_list -name 'FR_return_list_2025-0[1-4]-*.csv' -delete"
        print(f"  # 批量刪除命令:")
        print(f"  {cmd}")
        commands.append(cmd)
    
    # 3. 清理其他不需要的文件
    print("\n3️⃣ 清理其他文件:")
    print("  # 刪除 .DS_Store 文件")
    cmd = "find csv -name '.DS_Store' -delete"
    print(f"  {cmd}")
    commands.append(cmd)
    
    return commands

def safe_cleanup_mode():
    """安全清理模式 - 只清理明顯不需要的文件"""
    print("\n🛡️ 安全清理模式:")
    print("=" * 60)
    
    cleaned_count = 0
    
    # 只清理 .DS_Store 文件
    ds_store_files = glob.glob("csv/**/.DS_Store", recursive=True)
    if ds_store_files:
        print(f"🗑️ 清理 {len(ds_store_files)} 個 .DS_Store 文件...")
        for file in ds_store_files:
            try:
                os.remove(file)
                cleaned_count += 1
            except:
                pass
    
    print(f"✅ 安全清理完成，刪除了 {cleaned_count} 個文件")
    
    # 重新統計
    remaining_files = len(glob.glob("csv/**/*.csv", recursive=True))
    print(f"📊 剩餘 CSV 文件: {remaining_files:,} 個")

def main():
    """主函數"""
    print("🧹 CSV 文件清理工具")
    print("=" * 60)
    
    analyze_csv_structure()
    suggest_cleanup_plan()
    commands = create_cleanup_commands()
    
    print("\n⚠️  注意事項:")
    print("- 建議先備份重要數據")
    print("- 可以先執行安全清理模式")
    print("- 確認清理計劃後再執行批量刪除")
    
    print(f"\n🎯 執行建議:")
    print("1. python cleanup_csv.py")
    print("2. 確認清理計劃")
    print("3. 手動執行需要的清理命令")

if __name__ == "__main__":
    main() 