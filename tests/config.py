# tests/config.py
import os
import sys

# 找到專案根目錄
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

print(f"✅ 測試配置載入完成")
print(f"   專案根目錄: {project_root}")

# 測試配置
TEST_CONFIG = {
    "precision_tolerance": 1e-8,  # 計算精度要求：小數點後8位
    "reports_path": os.path.join(os.path.dirname(__file__), "reports"),
    "temp_path": os.path.join(os.path.dirname(__file__), "temp"),
    "project_root": project_root
}

# 建立必要資料夾
for path in [TEST_CONFIG["reports_path"], TEST_CONFIG["temp_path"]]:
    os.makedirs(path, exist_ok=True)