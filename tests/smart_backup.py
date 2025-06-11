# tests/smart_backup.py
import os
import shutil
from datetime import datetime
import sys


class SmartBackupManager:
    def __init__(self):
        # 專案路徑
        self.project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.csv_path = os.path.join(self.project_root, "csv")
        self.backup_root = os.path.join(self.project_root, "csv_backup")

        # csv 子資料夾結構（需要保留的空資料夾）
        self.required_folders = [
            "FR_diff",
            "FR_history",
            "FR_return_list",
            "strategy_ranking",
            "FF_profit",
            "FF_profit/summary",
            "FF_profit/archived",
            "Trading pair",
            "Backtest",
            "Return",
            "Example"
        ]

        print(f"🏠 專案路徑: {self.project_root}")
        print(f"📂 CSV路徑: {self.csv_path}")
        print(f"💾 備份路徑: {self.backup_root}")

    def create_backup(self, note=""):
        """建立備份並清空 csv"""

        if not os.path.exists(self.csv_path):
            print("❌ 找不到 csv 資料夾！")
            return False

        # 建立備份資料夾
        timestamp = datetime.now().strftime("%Y-%m-%d")
        if note.strip():
            backup_name = f"{timestamp}_{note.strip()}"
        else:
            backup_name = f"{timestamp}_backup"

        backup_path = os.path.join(self.backup_root, backup_name)

        try:
            print(f"🚀 開始備份...")
            print(f"   備份名稱: {backup_name}")

            # 建立備份根目錄
            os.makedirs(self.backup_root, exist_ok=True)

            # 複製整個 csv 資料夾到備份位置
            if os.path.exists(backup_path):
                print(f"⚠️  備份資料夾已存在，將覆蓋: {backup_path}")
                shutil.rmtree(backup_path)

            shutil.copytree(self.csv_path, backup_path)
            print(f"✅ 資料備份完成: {backup_path}")

            # 計算備份檔案數量
            backup_files = self._count_files(backup_path)
            print(f"   備份檔案數: {backup_files} 個")

            # 清空原始 csv 但保留資料夾結構
            self._clean_csv_folder()
            print("🧹 CSV 資料夾已清空，保留必要結構")

            print(f"🎉 備份完成！")
            print(f"   備份位置: csv_backup/{backup_name}/")
            return True

        except Exception as e:
            print(f"❌ 備份失敗: {e}")
            return False

    def _clean_csv_folder(self):
        """清空 csv 資料夾但保留必要的子資料夾結構"""

        # 刪除整個 csv 資料夾
        if os.path.exists(self.csv_path):
            shutil.rmtree(self.csv_path)

        # 重新建立 csv 資料夾和必要的子資料夾
        os.makedirs(self.csv_path, exist_ok=True)

        for folder in self.required_folders:
            folder_path = os.path.join(self.csv_path, folder)
            os.makedirs(folder_path, exist_ok=True)
            print(f"   📁 建立資料夾: {folder}")

        # 建立 .gitkeep 檔案確保空資料夾被保留
        for folder in self.required_folders:
            gitkeep_path = os.path.join(self.csv_path, folder, ".gitkeep")
            with open(gitkeep_path, 'w') as f:
                f.write("# 保持資料夾結構\n")

    def _count_files(self, path):
        """計算資料夾內檔案數量"""
        count = 0
        for root, dirs, files in os.walk(path):
            count += len(files)
        return count

    def list_backups(self):
        """列出所有備份"""
        if not os.path.exists(self.backup_root):
            print("📭 還沒有任何備份")
            return

        backups = []
        for item in os.listdir(self.backup_root):
            backup_path = os.path.join(self.backup_root, item)
            if os.path.isdir(backup_path):
                file_count = self._count_files(backup_path)
                size = self._get_folder_size(backup_path)
                backups.append((item, file_count, size))

        if not backups:
            print("📭 還沒有任何備份")
            return

        print("📋 備份清單:")
        print("-" * 60)
        for name, files, size in sorted(backups):
            print(f"📦 {name}")
            print(f"   檔案數: {files} 個")
            print(f"   大小: {size:.2f} MB")
            print()

    def _get_folder_size(self, path):
        """計算資料夾大小（MB）"""
        total_size = 0
        for root, dirs, files in os.walk(path):
            for file in files:
                file_path = os.path.join(root, file)
                if os.path.exists(file_path):
                    total_size += os.path.getsize(file_path)
        return total_size / (1024 * 1024)  # 轉換為 MB


def main():
    manager = SmartBackupManager()

    if len(sys.argv) > 1 and sys.argv[1] == "list":
        manager.list_backups()
        return

    print("\n" + "=" * 50)
    print("🎯 智能備份系統")
    print("=" * 50)

    # 顯示目前 csv 狀況
    if os.path.exists(manager.csv_path):
        current_files = manager._count_files(manager.csv_path)
        print(f"📊 目前 CSV 檔案數: {current_files} 個")

    # 輸入備註
    print("\n請輸入備份備註（可留空）:")
    note = input("💬 備註: ").strip()

    # 確認備份
    print("\n⚠️  注意：備份後 CSV 資料夾會被清空！")
    confirm = input("確定要進行備份嗎？(y/N): ").strip().lower()

    if confirm in ['y', 'yes']:
        success = manager.create_backup(note)
        if success:
            print("\n🎊 備份成功！你現在可以安心測試了")
            print("💡 使用 'python tests/smart_backup.py list' 查看所有備份")
    else:
        print("❌ 備份已取消")


if __name__ == "__main__":
    main()