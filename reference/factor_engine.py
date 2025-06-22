"""
因子計算引擎 (Factor Engine)

這個模組是將策略設定檔轉化為實際因子數據的生產線。
"""

import pandas as pd
import numpy as np
import factor_library as fl # Import the library module directly

class FactorEngine:
    """
    The Factor Engine is a pure calculation tool. 
    It receives strategy configurations and pre-loaded data to perform calculations.
    It does NOT handle file I/O.
    """
    def __init__(self, strategy_name, strategies_config):
        """
        Initializes the FactorEngine.

        Args:
            strategy_name (str): The name of the strategy to be executed.
            strategies_config (dict): The dictionary containing all strategy configurations.
        """
        if strategy_name not in strategies_config:
            raise ValueError(f"Strategy '{strategy_name}' not found in configuration.")

        self.strategy_name = strategy_name
        self.config = strategies_config[strategy_name]
        
        # No longer instantiating a class, the library is the module itself
        self.library = fl

        self.data_reqs = self.config.get('data_requirements', {})
        self.factors_config = self.config['factors']
        
    def _is_symbol_qualified(self, symbol_data, target_date):
        """
        Checks if a symbol meets the data requirements for a given date.
        `symbol_data` is a DataFrame containing all historical data for a single symbol.
        """
        min_days = self.data_reqs.get('min_data_days', 0)
        skip_days = self.data_reqs.get('skip_first_n_days', 0)

        if symbol_data.empty:
            return False

        listing_date = symbol_data['timestamp'].min()
        
        days_available = (target_date - listing_date).days
        
        first_calc_date = listing_date + pd.Timedelta(days=skip_days)

        if days_available < min_days or target_date < first_calc_date:
            return False
        
        return True

    def run(self, historical_data, date):
        """
        Runs the full factor calculation and ranking process for a single target date.
        
        Args:
            historical_data (pd.DataFrame): A DataFrame containing all necessary historical data
                                            for the calculation on the given date.
            date (str): The target date for the calculation in 'YYYY-MM-DD' format.
        """
        if historical_data.empty:
            print(f"Warning: Received empty historical data for date {date}. Skipping.")
            return None

        target_date = pd.to_datetime(date)
        print(f"\n🚀 Running strategy '{self.strategy_name}' for date: {date}")

        # Standardize column names upon receiving data
        data = historical_data.copy()
        data.rename(columns={'Date': 'timestamp', 'Trading_Pair': 'symbol', '1d_return': 'funding_rate_diff'}, inplace=True)
        data['timestamp'] = pd.to_datetime(data['timestamp'])

        # 1. Qualify symbols based on data requirements
        all_symbols = data['symbol'].unique()
        qualified_symbols = []
        for symbol in all_symbols:
            symbol_data = data[data['symbol'] == symbol]
            if self._is_symbol_qualified(symbol_data, target_date):
                qualified_symbols.append(symbol)

        if not qualified_symbols:
            print(f"No qualified symbols found for {date} based on strategy requirements.")
            return None
        
        print(f"📊 Found {len(qualified_symbols)} qualified symbols.")

        # 2. Calculate factors for each qualified symbol
        all_factors = []
        for symbol in qualified_symbols:
            symbol_factors = {'symbol': symbol}
            symbol_data = data[data['symbol'] == symbol]

            for factor_name, factor_config in self.factors_config.items():
                lookback = factor_config['lookback']
                input_col = factor_config.get('input_col', 'funding_rate_diff')
                params = factor_config.get('params', {})
                
                # Filter data for the lookback period up to the target date
                lookback_data = symbol_data[symbol_data['timestamp'] <= target_date].tail(lookback)

                if len(lookback_data) == lookback:
                    series = lookback_data[input_col]
                    # Directly get the function from the imported module
                    factor_func = getattr(self.library, factor_config['func'])
                    factor_value = factor_func(series, **params)
                    symbol_factors[factor_name] = factor_value
                else:
                    symbol_factors[factor_name] = np.nan
            
            all_factors.append(symbol_factors)

        if not all_factors:
            print("⚠️ No factor data could be calculated.")
            return None

        results_df = pd.DataFrame(all_factors).set_index('symbol')

        # 3. Normalize factor scores
        results_df.replace([np.inf, -np.inf], np.nan, inplace=True)
        for factor_name in self.factors_config.keys():
            rank = results_df[factor_name].rank(method='dense', na_option='bottom')
            norm_rank = rank / rank.max()
            results_df[f"{factor_name}_norm"] = norm_rank

        # 4. Calculate final weighted score
        results_df['final_score'] = 0
        for factor_name, factor_config in self.factors_config.items():
            weight = factor_config.get('weight', 0)
            results_df['final_score'] += results_df[f"{factor_name}_norm"].fillna(0) * weight
        
        # 5. Final ranking
        results_df['final_rank'] = results_df['final_score'].rank(method='dense', ascending=False)
        
        print(f"✅ Finished calculation for {date}.")
        return results_df.sort_values('final_rank')

    def _filter_eligible_pairs(self) -> list:
        """
        執行資格審查，篩選出符合數據要求的交易對。
        """
        min_days = self.data_reqs.get('min_data_days', 0)
        skip_days = self.data_reqs.get('skip_first_n_days', 0)

        self.all_pairs = self.data['Trading_Pair'].unique()
        eligible_pairs = []

        if min_days == 0:
            return self.all_pairs.tolist()

        # 獲取每個交易對的上市日期（首次出現的日期）
        listing_dates = self.data.groupby('Trading_Pair')['Date'].min()

        for pair, listing_date in listing_dates.items():
            # 數據可用的總天數
            days_available = (self.target_date - listing_date).days + 1
            
            # 實際用於計算的起始日期
            start_date_for_calc = listing_date + pd.Timedelta(days=skip_days)

            # 檢查數據是否足夠
            if days_available >= min_days and self.target_date >= start_date_for_calc:
                eligible_pairs.append(pair)

        return eligible_pairs

    def _get_listing_dates(self):
        """
        (模擬功能) 獲取交易對的上市日期。
        在實際應用中，這裡應該讀取一個包含上市日期的檔案。
        目前，我們將使用數據中每個交易對第一次出現的日期作為代理。
        """
        listing_dates = self.data.groupby('Trading_Pair')['Date'].min()
        return listing_dates.to_dict()

    def _is_data_qualified(self, symbol, target_date):
        """
        Checks if data for a specific symbol is qualified for calculation.
        """
        # Implementation of _is_data_qualified method
        # This is a placeholder and should be implemented based on your specific requirements
        return True  # Placeholder return, actual implementation needed 