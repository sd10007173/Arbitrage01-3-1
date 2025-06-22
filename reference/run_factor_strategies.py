"""
因子策略排行榜生成器 (主執行腳本)

這個腳本是運行所有在 factor_strategy_config.py 中定義的
因子策略的專用入口。

功能:
- 命令行參數支持，可指定策略、日期範圍。
- 自動加載所需的歷史數據。
- 調用 FactorEngine 計算因子分數。
- 復用 RankingEngine 進行標準化和加權排名。
- 將結果保存到 csv/strategy_ranking/ 資料夾。
"""

import os
import pandas as pd
from datetime import datetime, timedelta
import argparse
import glob

# Import the strategy configurations and the engine
from factor_strategy_config import FACTOR_STRATEGIES
from factor_engine import FactorEngine

def get_required_lookback(strategy_name):
    """
    Calculates the maximum lookback period required by a given strategy.
    """
    strategy_config = FACTOR_STRATEGIES[strategy_name]
    max_lookback = 0
    if 'factors' in strategy_config:
        for factor_cfg in strategy_config['factors'].values():
            if factor_cfg.get('lookback', 0) > max_lookback:
                max_lookback = factor_cfg['lookback']
    # Add a small buffer just in case
    return max_lookback + 5

def load_historical_data(target_date: str, lookback_days: int, data_folder: str):
    """
    Loads historical data from daily CSV files within a date range.
    It will load data from `target_date - lookback_days` to `target_date`.
    """
    print(f"🚚 Loading historical data for {target_date}, looking back {lookback_days} days...")
    
    start_date = pd.to_datetime(target_date) - timedelta(days=lookback_days)
    end_date = pd.to_datetime(target_date)

    all_data = []
    
    # Use glob to find all relevant files in one go
    date_pattern = "FR_return_list_*.csv"
    files = glob.glob(os.path.join(data_folder, date_pattern))
    
    for file_path in files:
        try:
            file_date_str = os.path.basename(file_path).replace('FR_return_list_', '').replace('.csv', '')
            file_date = pd.to_datetime(file_date_str)
            
            if start_date <= file_date <= end_date:
                daily_data = pd.read_csv(file_path)
                all_data.append(daily_data)
        except (ValueError, TypeError):
            print(f"⚠️ Warning: Could not parse date from filename: {os.path.basename(file_path)}. Skipping.")
        except Exception as e:
            print(f"❌ Error reading file {file_path}: {e}")

    if not all_data:
        print("⚠️ Warning: No historical data loaded for the specified date range.")
        return pd.DataFrame()
        
    historical_df = pd.concat(all_data, ignore_index=True)
    historical_df.drop_duplicates(subset=['Trading_Pair', 'Date'], keep='last', inplace=True)
    print(f"✅ Loaded {len(historical_df)} rows from {len(all_data)} files.")
    return historical_df

def run_strategy_for_date_range(start_date: str, end_date: str, strategy_name: str):
    """
    Runs a factor strategy for each day in a given date range.
    """
    # --- Setup Paths ---
    project_root = os.path.dirname(os.path.abspath(__file__))
    data_folder = os.path.join(project_root, "csv", "FR_return_list")
    output_folder = os.path.join(project_root, "csv", "strategy_ranking", strategy_name)
    os.makedirs(output_folder, exist_ok=True)

    # --- Validate Inputs ---
    if strategy_name not in FACTOR_STRATEGIES:
        print(f"❌ Error: Strategy '{strategy_name}' is not defined in factor_strategy_config.py.")
        return

    # --- Initialize Engine ---
    print(f"⚙️ Initializing FactorEngine with strategy '{strategy_name}'...")
    engine = FactorEngine(strategy_name, FACTOR_STRATEGIES)

    # --- Process Date Range ---
    dates_to_process = pd.date_range(start=start_date, end=end_date)
    required_lookback = get_required_lookback(strategy_name)
    
    print(f"\n🚀 Processing {len(dates_to_process)} days from {start_date} to {end_date}...")
    print(f"   Strategy requires a lookback of ~{required_lookback} days.")

    for target_date in dates_to_process:
        target_date_str = target_date.strftime('%Y-%m-%d')
        
        # 1. Load data required for this specific day's calculation
        historical_data = load_historical_data(target_date_str, required_lookback, data_folder)
        
        if historical_data.empty:
            print(f"Skipping {target_date_str} due to lack of historical data.")
            continue

        # 2. Run the engine with the prepared data
        results_df = engine.run(historical_data, target_date_str)

        if results_df is not None and not results_df.empty:
            output_filename = f"{strategy_name}_ranking_{target_date_str}.csv"
            output_path = os.path.join(output_folder, output_filename)
            results_df.to_csv(output_path, index=True)
            print(f"💾 Saved results for {target_date_str} to {output_path}")
        else:
            print(f"⚠️ No results generated for {target_date_str}.")

    print("\n🎉 All dates processed.")

def get_inputs_from_user():
    """
    Interactively gets the date range and strategy from the user.
    """
    # Get date range
    while True:
        try:
            start_date_str = input("➡️  Enter start date (YYYY-MM-DD): ").strip()
            datetime.strptime(start_date_str, "%Y-%m-%d")
            break
        except ValueError:
            print("❌ Invalid date format. Please use YYYY-MM-DD.")

    while True:
        try:
            end_date_str = input("⬅️  Enter end date (YYYY-MM-DD): ").strip()
            datetime.strptime(end_date_str, "%Y-%m-%d")
            if start_date_str > end_date_str:
                print("❌ End date cannot be earlier than start date.")
                continue
            break
        except ValueError:
            print("❌ Invalid date format. Please use YYYY-MM-DD.")

    # Select strategy
    strategies = list(FACTOR_STRATEGIES.keys())
    print("\n✨ Please select a strategy:")
    for i, name in enumerate(strategies):
        print(f"  {i+1}. {name} - {FACTOR_STRATEGIES[name].get('description', '')}")

    while True:
        try:
            choice_str = input(f"Enter your choice (1-{len(strategies)}): ").strip()
            choice = int(choice_str)
            if 1 <= choice <= len(strategies):
                strategy_name = strategies[choice - 1]
                break
            else:
                print(f"❌ Invalid choice. Please enter a number between 1 and {len(strategies)}.")
        except ValueError:
            print("❌ Invalid input. Please enter a number.")
            
    return start_date_str, end_date_str, strategy_name

def main():
    parser = argparse.ArgumentParser(
        description="Factor Strategy Ranking Generator. Reads daily data from 'csv/FR_return_list/' folder.",
        epilog="If no arguments are provided, the script will run in interactive mode."
    )
    parser.add_argument("--start_date", help="Start date in YYYY-MM-DD format.")
    parser.add_argument("--end_date", help="End date in YYYY-MM-DD format.")
    parser.add_argument("--strategy", help="Name of the factor strategy to run.")
    
    args = parser.parse_args()

    if args.start_date and args.end_date and args.strategy:
        print("⚙️ Running in command-line mode...")
        run_strategy_for_date_range(args.start_date, args.end_date, args.strategy)
    else:
        print("🚀 Running in interactive mode...")
        start_date, end_date, strategy_name = get_inputs_from_user()
        run_strategy_for_date_range(start_date, end_date, strategy_name)

if __name__ == "__main__":
    main() 