# 數據量檢查功能完整規格書

**文件版本**: v1.0  
**創建日期**: 2025-01-22  
**最後更新**: 2025-01-22  
**負責人**: Factor Strategy System Team  

---

## 📋 **目的 (Purpose)**

### **主要目的**
建立智能數據量預檢查機制，在因子策略執行前驗證數據充足性，避免無效計算並提升用戶體驗。

### **具體目標**
1. **預防性檢查**: 在策略執行前就識別數據不足問題
2. **資源優化**: 避免浪費計算資源在註定失敗的策略執行上
3. **用戶體驗**: 提供明確的失敗原因和解決建議
4. **系統穩定性**: 減少運行時錯誤和異常處理負擔
5. **智能指導**: 主動推薦可行的替代方案

### **業務價值**
- **效率提升**: 減少無效等待時間
- **成本控制**: 降低計算資源浪費
- **用戶滿意度**: 提供清晰的操作指導
- **系統可靠性**: 增強系統穩定性和可預測性

---

## 🧩 **區塊 (Components)**

### **核心模組**

#### **1. 數據充足性檢查器 (Data Sufficiency Checker)**
- **位置**: `factor_strategies/factor_engine.py`
- **主要方法**: `check_data_sufficiency()`
- **職責**: 執行所有數據量相關檢查
- **輸入**: 策略名稱、目標日期
- **輸出**: 檢查結果 (布林值) + 詳細信息 (字符串)

#### **2. 策略執行控制器 (Strategy Execution Controller)**
- **位置**: `factor_strategies/factor_engine.py`
- **主要方法**: `run_strategy()`
- **職責**: 整合數據檢查與策略執行流程
- **功能**: 執行前預檢查、條件性執行、結果處理

#### **3. 交互式界面增強器 (Interactive Interface Enhancer)**
- **位置**: `factor_strategies/run_factor_strategies.py`
- **主要方法**: `run_single_strategy()`
- **職責**: 在用戶界面層面整合數據檢查功能
- **功能**: 用戶友好的錯誤信息、詳細要求展示

#### **4. 策略配置解析器 (Strategy Configuration Parser)**
- **位置**: `factor_strategies/factor_strategy_config.py`
- **職責**: 提供策略數據要求信息
- **功能**: 策略配置讀取、數據要求計算

### **支援模組**

#### **5. 數據庫管理器 (Database Manager)**
- **位置**: `database_operations.py`
- **相關方法**: `get_return_metrics_date_range()`
- **職責**: 提供數據範圍信息

#### **6. 日期處理器 (Date Processor)**
- **使用**: `pandas.to_datetime()`, `timedelta`
- **職責**: 日期計算和驗證

---

## 🔄 **業務流程 (Business Process)**

### **主要業務場景**

#### **場景1: 用戶通過交互式界面執行策略**
```
用戶操作 → 選擇策略 → 選擇日期 → 系統預檢查 → 
[通過] → 執行策略 → 顯示結果
[失敗] → 顯示錯誤 → 提供建議 → 用戶重新選擇
```

#### **場景2: 程式化調用策略執行**
```
程式調用 → check_data_sufficiency() → 
[通過] → run_strategy() → 返回結果
[失敗] → 返回空DataFrame → 程式處理錯誤
```

#### **場景3: 批量策略執行**
```
批量請求 → 逐個檢查每個策略 → 
[通過] → 加入執行隊列
[失敗] → 記錄失敗原因 → 繼續下一個
執行隊列 → 批量執行 → 返回結果摘要
```

### **錯誤處理流程**

#### **數據量不足錯誤處理**
```
檢測到數據不足 → 計算缺少天數 → 生成具體錯誤信息 → 
提供解決建議 → 推薦替代策略 → 記錄錯誤統計
```

#### **日期範圍錯誤處理**
```
檢測到日期超出範圍 → 獲取有效日期範圍 → 
建議使用最新日期 → 提供日期選擇指導
```

#### **交易對過濾錯誤處理**
```
檢測到交易對不足 → 計算實際可用交易對 → 
建議延後執行日期 → 提供策略調整建議
```

### **用戶體驗優化流程**

#### **智能建議生成**
```
分析失敗原因 → 識別問題類型 → 生成針對性建議 → 
推薦可行策略 → 提供具體操作步驟
```

#### **詳細信息展示**
```
用戶請求詳情 → 解析策略配置 → 展示數據要求 → 
顯示因子窗口 → 計算總需求天數
```

---

## 💻 **程式流程 (Program Flow)**

### **主要執行流程**

#### **1. check_data_sufficiency() 方法流程**

```python
def check_data_sufficiency(strategy_name, target_date) -> (bool, str):
    # 步驟1: 基本驗證
    if strategy_name not in FACTOR_STRATEGIES:
        return False, "未知的策略"
    
    # 步驟2: 獲取策略配置
    strategy_config = FACTOR_STRATEGIES[strategy_name]
    data_req = strategy_config['data_requirements']
    min_days = data_req['min_data_days']
    skip_days = data_req['skip_first_n_days']
    
    # 步驟3: 處理目標日期
    if target_date is None:
        start_date, end_date = db_manager.get_return_metrics_date_range()
        if not end_date:
            return False, "數據庫中沒有數據"
        target_date = end_date
    
    target_date_obj = pd.to_datetime(target_date)
    
    # 步驟4: 獲取數據範圍
    start_date, end_date = db_manager.get_return_metrics_date_range()
    earliest_date = pd.to_datetime(start_date)
    latest_date = pd.to_datetime(end_date)
    
    # 步驟5: 日期範圍檢查
    if target_date_obj > latest_date:
        return False, f"目標日期超出範圍 (最新: {end_date})"
    
    # 步驟6: 基本數據量檢查
    available_days = (target_date_obj - earliest_date).days + 1
    required_days = min_days + skip_days
    
    if available_days < required_days:
        return False, f"數據量不足：需要{required_days}天，只有{available_days}天"
    
    # 步驟7: 交易對過濾檢查
    if skip_days > 0:
        days_from_start = available_days
        if days_from_start <= skip_days:
            return False, f"無交易對符合條件：上線時間不足{skip_days}天"
    
    # 步驟8: 因子窗口檢查
    max_window = max(factor['window'] for factor in strategy_config['factors'].values())
    total_required_days = max_window + skip_days
    
    if available_days < total_required_days:
        factor_windows = [f"{name}({config['window']}天)" 
                         for name, config in strategy_config['factors'].items()]
        return False, f"因子計算數據不足：需要{total_required_days}天，只有{available_days}天。因子窗口: {', '.join(factor_windows)}"
    
    # 步驟9: 返回成功結果
    return True, f"數據充足：可用數據{available_days}天，滿足策略要求"
```

#### **2. run_strategy() 方法整合流程**

```python
def run_strategy(strategy_name, target_date, save_to_db=True) -> pd.DataFrame:
    print(f"🚀 執行因子策略: {strategy_name}")
    
    # 步驟1: 預檢查數據充足性
    is_sufficient, message = self.check_data_sufficiency(strategy_name, target_date)
    
    if not is_sufficient:
        # 步驟2A: 數據不足處理
        print(f"❌ 數據量檢查失敗: {message}")
        print("💡 建議:")
        print("   • 使用較晚的日期")
        print("   • 選擇數據要求較低的策略")
        print("   • 確保有足夠的歷史數據")
        return pd.DataFrame()  # 返回空結果
    
    # 步驟2B: 數據充足處理
    print(f"✅ 數據量檢查通過: {message}")
    
    # 步驟3: 執行策略計算
    result_df = self.calculate_strategy_ranking(strategy_name, target_date)
    
    # 步驟4: 結果處理和保存
    if not result_df.empty and save_to_db:
        self.db_manager.insert_factor_strategy_ranking(result_df)
        print(f"💾 結果已保存到數據庫")
    
    return result_df
```

#### **3. 交互式界面整合流程**

```python
def run_single_strategy(engine, strategy_name, target_date):
    print(f"🚀 執行策略: {strategy_name}")
    print(f"📅 目標日期: {target_date}")
    
    # 步驟1: 預檢查
    print("🔍 檢查數據充足性...")
    is_sufficient, message = engine.check_data_sufficiency(strategy_name, target_date)
    
    if not is_sufficient:
        # 步驟2A: 失敗處理
        print(f"❌ 數據量檢查失敗: {message}")
        print("💡 建議:")
        print("   • 選擇較晚的日期")
        print("   • 選擇數據要求較低的策略")
        print("   • 確認是否有足夠的歷史數據")
        
        # 步驟3A: 詳細信息展示
        show_req = input("❓ 是否查看策略數據要求? (y/n): ").strip().lower()
        if show_req in ['y', 'yes']:
            display_strategy_requirements(strategy_name)
        
        return  # 提前終止
    
    # 步驟2B: 成功處理
    print(f"✅ 數據量檢查通過: {message}")
    
    # 步驟3B: 執行策略
    try:
        result = engine.run_strategy(strategy_name, target_date)
        display_results(result, strategy_name)
    except Exception as e:
        handle_execution_error(e)
```

### **數據流程圖**

```
[用戶輸入] → [策略名稱驗證] → [目標日期處理] → [數據庫連接]
     ↓
[獲取數據範圍] → [計算可用天數] → [基本數據量檢查]
     ↓
[交易對過濾檢查] → [因子窗口檢查] → [生成檢查結果]
     ↓
[檢查通過] → [執行策略] → [返回結果]
     ↓
[檢查失敗] → [生成錯誤信息] → [提供建議] → [返回空結果]
```

### **錯誤處理流程**

#### **異常捕獲層次**
1. **配置層異常**: 策略不存在、配置錯誤
2. **數據層異常**: 數據庫連接失敗、數據缺失
3. **計算層異常**: 日期計算錯誤、數值計算異常
4. **界面層異常**: 用戶輸入錯誤、顯示異常

#### **錯誤恢復策略**
```python
try:
    # 主要邏輯
    result = check_data_sufficiency(strategy_name, target_date)
except DatabaseConnectionError:
    return False, "數據庫連接失敗，請檢查數據庫狀態"
except ConfigurationError:
    return False, "策略配置錯誤，請檢查配置文件"
except DateFormatError:
    return False, "日期格式錯誤，請使用YYYY-MM-DD格式"
except Exception as e:
    logger.error(f"數據檢查時發生未預期錯誤: {e}")
    return False, "系統內部錯誤，請聯繫技術支援"
```

### **性能優化流程**

#### **快速檢查策略**
1. **配置緩存**: 緩存策略配置，避免重複讀取
2. **日期範圍緩存**: 緩存數據庫日期範圍，減少查詢
3. **早期終止**: 一旦發現問題立即返回，不進行後續檢查
4. **批量檢查**: 支援批量策略的並行檢查

#### **記憶體管理**
```python
# 避免不必要的DataFrame創建
def check_data_sufficiency_optimized():
    # 只進行必要的數據庫查詢
    date_range = db_manager.get_return_metrics_date_range()
    
    # 使用輕量級計算
    available_days = calculate_days_difference(date_range)
    
    # 早期返回
    if available_days < min_required_days:
        return False, generate_error_message()
```

---

## 📊 **技術規格 (Technical Specifications)**

### **輸入規格**

#### **check_data_sufficiency() 輸入**
```python
Parameters:
    strategy_name: str
        - 必填參數
        - 必須存在於 FACTOR_STRATEGIES 中
        - 範例: 'cerebrum_core', 'test_factor_simple'
    
    target_date: str | None
        - 可選參數，預設為 None (使用最新日期)
        - 格式: 'YYYY-MM-DD'
        - 範例: '2025-01-31'

Returns:
    tuple[bool, str]
        - bool: 檢查結果 (True=通過, False=失敗)
        - str: 詳細信息或錯誤描述
```

### **輸出規格**

#### **成功輸出格式**
```python
(True, "數據充足：可用數據 31 天，滿足策略要求")
```

#### **失敗輸出格式**
```python
# 基本數據量不足
(False, "數據量不足：策略需要 33 天數據，但只有 31 天可用 (從 2025-01-01 到 2025-01-31)")

# 交易對過濾問題
(False, "無交易對符合條件：所有交易對上線時間不足 5 天 (實際: 4 天)")

# 因子窗口不足
(False, "因子計算數據不足：最大因子窗口需要 97 天，但只有 31 天可用。因子窗口: trend_factor(90天), stability_factor(60天)")

# 日期超出範圍
(False, "目標日期 2025-02-01 超出數據範圍 (最新: 2025-01-31)")
```

### **數據要求規格**

#### **策略數據要求對照表**
```python
STRATEGY_DATA_REQUIREMENTS = {
    'test_factor_simple': {
        'min_data_days': 7,
        'skip_first_n_days': 0,
        'max_factor_window': 7,
        'total_required_days': 7
    },
    'cerebrum_core': {
        'min_data_days': 30,
        'skip_first_n_days': 3,
        'max_factor_window': 90,
        'total_required_days': 93
    },
    'cerebrum_momentum': {
        'min_data_days': 60,
        'skip_first_n_days': 5,
        'max_factor_window': 90,
        'total_required_days': 95
    },
    'cerebrum_stability': {
        'min_data_days': 90,
        'skip_first_n_days': 7,
        'max_factor_window': 90,
        'total_required_days': 97
    }
}
```

### **檢查項目規格**

#### **1. 基本數據量檢查**
```python
available_days = (target_date - earliest_date).days + 1
required_days = min_data_days + skip_first_n_days

if available_days < required_days:
    return False, error_message
```

#### **2. 交易對過濾檢查**
```python
if skip_first_n_days > 0:
    days_from_start = (target_date - earliest_date).days + 1
    if days_from_start <= skip_first_n_days:
        return False, error_message
```

#### **3. 因子窗口檢查**
```python
max_window = max(factor['window'] for factor in factors.values())
total_required_days = max_window + skip_first_n_days

if available_days < total_required_days:
    return False, error_message
```

#### **4. 日期範圍檢查**
```python
if target_date > latest_available_date:
    return False, error_message
```

---

## 🧪 **測試規格 (Testing Specifications)**

### **單元測試用例**

#### **測試類別1: 成功案例**
```python
def test_sufficient_data_simple_strategy():
    # 測試簡單策略有足夠數據
    result = engine.check_data_sufficiency('test_factor_simple', '2025-01-08')
    assert result[0] == True
    assert "數據充足" in result[1]

def test_sufficient_data_latest_date():
    # 測試使用最新日期
    result = engine.check_data_sufficiency('test_factor_simple', None)
    assert result[0] == True
```

#### **測試類別2: 失敗案例**
```python
def test_insufficient_basic_data():
    # 測試基本數據量不足
    result = engine.check_data_sufficiency('cerebrum_core', '2025-01-01')
    assert result[0] == False
    assert "數據量不足" in result[1]

def test_insufficient_factor_window():
    # 測試因子窗口數據不足
    result = engine.check_data_sufficiency('cerebrum_stability', '2025-01-10')
    assert result[0] == False
    assert "因子計算數據不足" in result[1]

def test_invalid_strategy():
    # 測試無效策略名稱
    result = engine.check_data_sufficiency('invalid_strategy', '2025-01-31')
    assert result[0] == False
    assert "未知的策略" in result[1]

def test_date_out_of_range():
    # 測試日期超出範圍
    result = engine.check_data_sufficiency('test_factor_simple', '2025-12-31')
    assert result[0] == False
    assert "超出數據範圍" in result[1]
```

#### **測試類別3: 邊界條件**
```python
def test_exact_minimum_data():
    # 測試剛好滿足最小數據要求
    result = engine.check_data_sufficiency('test_factor_simple', '2025-01-07')
    assert result[0] == True

def test_one_day_short():
    # 測試少一天數據
    result = engine.check_data_sufficiency('test_factor_simple', '2025-01-06')
    assert result[0] == False
```

### **整合測試用例**

#### **測試場景1: 完整執行流程**
```python
def test_full_execution_flow():
    # 測試從檢查到執行的完整流程
    engine = FactorEngine()
    
    # 檢查數據充足性
    is_sufficient, message = engine.check_data_sufficiency('test_factor_simple', '2025-01-08')
    assert is_sufficient == True
    
    # 執行策略
    result = engine.run_strategy('test_factor_simple', '2025-01-08', save_to_db=False)
    assert not result.empty
    assert len(result) > 0
```

#### **測試場景2: 失敗處理流程**
```python
def test_failure_handling_flow():
    # 測試失敗情況的處理流程
    engine = FactorEngine()
    
    # 檢查數據不足
    is_sufficient, message = engine.check_data_sufficiency('cerebrum_core', '2025-01-01')
    assert is_sufficient == False
    
    # 執行策略應返回空結果
    result = engine.run_strategy('cerebrum_core', '2025-01-01', save_to_db=False)
    assert result.empty
```

### **性能測試規格**

#### **響應時間要求**
- **檢查時間**: < 100ms (單個策略)
- **批量檢查**: < 500ms (所有策略)
- **記憶體使用**: < 50MB (檢查過程)

#### **壓力測試**
```python
def test_concurrent_checks():
    # 測試並發檢查性能
    import concurrent.futures
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for i in range(100):
            future = executor.submit(engine.check_data_sufficiency, 
                                   'test_factor_simple', '2025-01-08')
            futures.append(future)
        
        results = [future.result() for future in futures]
        assert all(result[0] for result in results)
```

---

## 📚 **維護規格 (Maintenance Specifications)**

### **版本控制**

#### **版本號規則**
- **主版本**: 功能重大變更
- **次版本**: 新增檢查項目或優化
- **修訂版本**: 錯誤修復和微調

#### **變更記錄格式**
```markdown
## v1.1.0 (2025-01-25)
### 新增
- 新增交易量檢查功能
- 支援自定義數據要求

### 修改
- 優化錯誤信息格式
- 改善日期計算精度

### 修復
- 修復邊界日期計算錯誤
- 修復並發檢查的競態條件
```

### **監控指標**

#### **功能指標**
- **檢查成功率**: 應 > 99%
- **錯誤預測準確率**: 應 > 95%
- **用戶滿意度**: 基於錯誤信息清晰度

#### **性能指標**
- **平均響應時間**: 應 < 50ms
- **記憶體峰值使用**: 應 < 100MB
- **並發處理能力**: 應支援 > 50 並發請求

### **故障排除指南**

#### **常見問題1: 檢查結果不準確**
```
症狀: 檢查通過但執行失敗
原因: 數據庫狀態變化或配置不同步
解決: 
1. 重新同步數據庫連接
2. 驗證策略配置一致性
3. 檢查數據庫鎖定狀態
```

#### **常見問題2: 檢查速度過慢**
```
症狀: 檢查時間超過預期
原因: 數據庫查詢效率低或網絡延遲
解決:
1. 優化數據庫查詢語句
2. 增加適當索引
3. 實施結果緩存機制
```

#### **常見問題3: 錯誤信息不清晰**
```
症狀: 用戶無法理解錯誤原因
原因: 錯誤信息模板需要優化
解決:
1. 收集用戶反饋
2. 優化錯誤信息模板
3. 增加更多上下文信息
```

### **擴展規劃**

#### **短期擴展 (1-3個月)**
- 新增交易量檢查
- 支援自定義檢查規則
- 增加檢查結果緩存

#### **中期擴展 (3-6個月)**
- 智能數據預測功能
- 支援多數據源檢查
- 增加圖形化檢查報告

#### **長期擴展 (6-12個月)**
- 機器學習驅動的檢查優化
- 自動數據修復建議
- 跨系統數據一致性檢查

---

## 🔒 **安全規格 (Security Specifications)**

### **輸入驗證**
- **策略名稱**: 白名單驗證，防止SQL注入
- **日期格式**: 嚴格格式檢查，防止格式攻擊
- **參數範圍**: 合理範圍限制，防止資源耗盡

### **錯誤信息安全**
- **敏感信息過濾**: 不暴露數據庫結構信息
- **錯誤信息標準化**: 使用預定義錯誤模板
- **日誌記錄**: 記錄所有檢查操作，便於審計

### **資源保護**
- **查詢限制**: 限制數據庫查詢頻率
- **記憶體保護**: 設置記憶體使用上限
- **超時機制**: 設置檢查操作超時時間

---

## 📈 **監控與報告 (Monitoring & Reporting)**

### **實時監控指標**
```python
MONITORING_METRICS = {
    'check_success_rate': 0.99,      # 檢查成功率
    'avg_response_time': 45,         # 平均響應時間 (ms)
    'error_prediction_accuracy': 0.96, # 錯誤預測準確率
    'user_retry_rate': 0.15,         # 用戶重試率
    'concurrent_users': 25           # 並發用戶數
}
```

### **日誌格式規範**
```json
{
    "timestamp": "2025-01-22T10:30:00Z",
    "operation": "check_data_sufficiency",
    "strategy_name": "cerebrum_core",
    "target_date": "2025-01-31",
    "result": false,
    "message": "數據量不足：策略需要 33 天數據，但只有 31 天可用",
    "execution_time_ms": 42,
    "user_id": "user_001",
    "session_id": "session_abc123"
}
```

### **報告生成**
- **每日報告**: 檢查統計、錯誤分析、性能指標
- **每週報告**: 趨勢分析、用戶行為、系統優化建議
- **每月報告**: 功能使用情況、改進建議、發展規劃

---

**📝 本規格書涵蓋了數據量檢查功能的所有技術細節和業務要求，為系統開發、測試、維護和擴展提供了完整的指導方針。** 