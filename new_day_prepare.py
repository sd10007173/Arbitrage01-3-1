#!/usr/bin/env python
import os
import shutil
import datetime
import argparse
from pathlib import Path


def get_yesterday_date():
    """
    獲取昨日的日期，格式為 YYYY-MM-DD
    """
    yesterday = datetime.date.today() - datetime.timedelta(days=1)
    return yesterday.strftime("%Y-%m-%d")


def create_folder(base_path, folder_name):
    """
    在指定的基礎路徑中創建資料夾
    
    Args:
        base_path: 基礎路徑 (如 csv/FR_history)
        folder_name: 資料夾名稱 (如 "backup_0606")
    
    Returns:
        創建的資料夾完整路徑
    """
    target_folder = os.path.join(base_path, folder_name)
    os.makedirs(target_folder, exist_ok=True)
    return target_folder


def should_move_file(file_name, directory_name):
    """
    判斷檔案是否應該被移動
    
    Args:
        file_name: 檔案名稱
        directory_name: 所在目錄名稱
    
    Returns:
        布林值，True表示應該移動
    """
    # 排除系統檔案
    system_files = ['.DS_Store', 'Thumbs.db', '.gitkeep']
    if file_name in system_files:
        return False
    
    # 排除隱藏檔案
    if file_name.startswith('.'):
        return False
    
    # 只移動CSV檔案
    return file_name.endswith('.csv')


def move_files_to_folder(base_path, target_folder, directory_name):
    """
    將基礎路徑中的相關檔案移動到目標資料夾中
    
    Args:
        base_path: 基礎路徑
        target_folder: 目標資料夾路徑
        directory_name: 目錄名稱（用於檔案過濾）
    
    Returns:
        移動的檔案數量
    """
    if not os.path.exists(base_path):
        print(f"⚠️ 路徑不存在，跳過: {base_path}")
        return 0
    
    moved_count = 0
    skipped_count = 0
    
    # 獲取基礎路徑中的所有檔案（不包括子資料夾）
    for item in os.listdir(base_path):
        item_path = os.path.join(base_path, item)
        
        # 只處理檔案，不處理資料夾
        if os.path.isfile(item_path):
            if should_move_file(item, directory_name):
                try:
                    destination = os.path.join(target_folder, item)
                    shutil.move(item_path, destination)
                    print(f"✅ 移動檔案: {item} -> {target_folder}")
                    moved_count += 1
                except Exception as e:
                    print(f"❌ 移動檔案失敗: {item}, 錯誤: {e}")
            else:
                print(f"⏩ 跳過檔案: {item} (不符合移動條件)")
                skipped_count += 1
        else:
            print(f"📁 跳過資料夾: {item}")
    
    if skipped_count > 0:
        print(f"ℹ️ 跳過了 {skipped_count} 個不相關檔案")
    
    return moved_count


def archive_directory(directory_name, folder_name, project_root):
    """
    歸檔指定目錄的檔案到指定資料夾
    
    Args:
        directory_name: 目錄名稱 (如 "FR_history")
        folder_name: 資料夾名稱 (如 "backup_0606")
        project_root: 專案根目錄
    
    Returns:
        移動的檔案數量
    """
    base_path = os.path.join(project_root, "csv", directory_name)
    
    print(f"\n📂 處理目錄: {base_path}")
    
    # 創建目標資料夾
    target_folder = create_folder(base_path, folder_name)
    print(f"📁 創建資料夾: {target_folder}")
    
    # 移動檔案
    moved_count = move_files_to_folder(base_path, target_folder, directory_name)
    
    if moved_count > 0:
        print(f"✅ 成功移動 {moved_count} 個檔案到 {target_folder}")
    else:
        print(f"ℹ️ 沒有符合條件的檔案需要移動")
    
    return moved_count


def get_folder_name(args):
    """
    獲取要使用的資料夾名稱
    
    Args:
        args: 命令列參數
    
    Returns:
        資料夾名稱字串
    """
    if args.date:
        # 如果提供了日期參數，使用日期作為預設建議
        folder_name = args.date
        print(f"💡 建議使用日期: {folder_name}")
    else:
        # 提供昨日日期作為建議
        yesterday = get_yesterday_date()
        print(f"💡 建議使用昨日日期: {yesterday}")
        folder_name = None
    
    # 提示用戶輸入
    while True:
        try:
            user_input = input("📝 請輸入資料夾名稱: ").strip()
            
            if user_input:
                # 檢查資料夾名稱是否合法（不包含特殊字符）
                if any(char in user_input for char in ['/', '\\', ':', '*', '?', '"', '<', '>', '|']):
                    print("❌ 資料夾名稱不能包含特殊字符: / \\ : * ? \" < > |")
                    continue
                
                return user_input
            else:
                print("❌ 請輸入有效的資料夾名稱")
                
        except KeyboardInterrupt:
            print("\n👋 用戶中斷，退出程式")
            return None


def main():
    parser = argparse.ArgumentParser(description="將檔案歸檔到指定的資料夾中")
    parser.add_argument("--date", help="建議的資料夾名稱 (格式: YYYY-MM-DD)")
    parser.add_argument("--dry-run", action="store_true", help="預覽模式，不實際移動檔案")
    args = parser.parse_args()
    
    # 取得專案根目錄
    project_root = os.path.dirname(os.path.abspath(__file__))
    
    print("🗂️  檔案歸檔工具")
    print("="*50)
    
    # 獲取資料夾名稱
    folder_name = get_folder_name(args)
    
    if not folder_name:
        print("❌ 沒有提供資料夾名稱，程式結束")
        return
    
    if args.dry_run:
        print("🔍 預覽模式 - 不會實際移動檔案")
    
    print(f"📁 專案根目錄: {project_root}")
    print(f"📝 資料夾名稱: {folder_name}")
    
    # 要處理的目錄列表
    directories = [
        "FR_history",
        "FR_diff", 
        "FR_return_list",
        "strategy_ranking",
        "Backtest"
    ]
    
    total_moved = 0
    
    # 處理每個目錄
    for directory in directories:
        if args.dry_run:
            base_path = os.path.join(project_root, "csv", directory)
            if os.path.exists(base_path):
                all_files = [f for f in os.listdir(base_path) if os.path.isfile(os.path.join(base_path, f))]
                relevant_files = [f for f in all_files if should_move_file(f, directory)]
                skipped_files = [f for f in all_files if not should_move_file(f, directory)]
                
                print(f"\n📂 {directory}: 發現 {len(all_files)} 個檔案")
                
                if relevant_files:
                    print(f"   ✅ 將移動 {len(relevant_files)} 個相關檔案:")
                    for file in relevant_files:
                        print(f"      📄 {file}")
                else:
                    print(f"   ℹ️ 沒有符合條件的檔案")
                
                if skipped_files:
                    print(f"   ⏩ 將跳過 {len(skipped_files)} 個檔案:")
                    for file in skipped_files:
                        print(f"      🚫 {file}")
                
                print(f"   👉 將創建資料夾: {os.path.join(base_path, folder_name)}")
            else:
                print(f"\n📂 {directory}: 目錄不存在")
        else:
            moved_count = archive_directory(directory, folder_name, project_root)
            total_moved += moved_count
    
    if args.dry_run:
        print(f"\n🔍 預覽完成 - 移除 --dry-run 參數來實際執行")
    else:
        print(f"\n🎉 歸檔完成！總共移動了 {total_moved} 個檔案到資料夾 {folder_name}")
        
        # 顯示最終結果
        print("\n📊 歸檔結果摘要:")
        for directory in directories:
            base_path = os.path.join(project_root, "csv", directory)
            target_folder = os.path.join(base_path, folder_name)
            if os.path.exists(target_folder):
                archived_files = [f for f in os.listdir(target_folder) if os.path.isfile(os.path.join(target_folder, f))]
                print(f"   📁 {directory}/{folder_name}: {len(archived_files)} 個檔案")


if __name__ == "__main__":
    main() 