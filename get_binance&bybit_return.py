#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
合并币安和Bybit资金费用CSV工具
将两个交易所的资金费用数据合并为一个对比表格
"""

import pandas as pd
from datetime import datetime
import os
import glob


def find_csv_files(input_dir="csv/Return"):
    """
    在指定目录下查找币安和Bybit CSV文件

    Args:
        input_dir (str): 输入目录路径

    Returns:
        tuple: (binance_files, bybit_files)
    """
    print(f"正在 {input_dir} 目录中查找CSV文件...")

    # 检查输入目录是否存在
    if not os.path.exists(input_dir):
        print(f"❌ 输入目录不存在: {input_dir}")
        return [], []

    # 在指定目录中查找文件
    binance_pattern = os.path.join(input_dir, "binance_funding_fees_*.csv")
    bybit_pattern = os.path.join(input_dir, "bybit_funding_fees_*.csv")

    binance_files = glob.glob(binance_pattern)
    bybit_files = glob.glob(bybit_pattern)

    print("找到的CSV文件:")
    print(f"币安文件: {binance_files}")
    print(f"Bybit文件: {bybit_files}")

    return binance_files, bybit_files


def parse_date_from_filename(filename):
    """
    从文件名中解析日期范围
    """
    try:
        # 获取文件名（去掉路径）
        basename = os.path.basename(filename)

        # 提取文件名中的日期部分
        if "binance_funding_fees_" in basename:
            date_part = basename.replace("binance_funding_fees_", "").replace(".csv", "")
        elif "bybit_funding_fees_" in basename:
            date_part = basename.replace("bybit_funding_fees_", "").replace(".csv", "")
        else:
            return None

        # 分析日期格式 (如: 2025-01-20_to_2025-01-31)
        if "_to_" in date_part:
            start_date, end_date = date_part.split("_to_")
            return start_date, end_date
        else:
            return None
    except:
        return None


def load_and_process_binance_csv(filepath):
    """
    加载和处理币安CSV文件
    """
    print(f"正在加载币安文件: {filepath}")

    df = pd.read_csv(filepath)
    print(f"币安原始数据: {len(df)} 行")
    print(f"币安列名: {list(df.columns)}")

    # 重命名列以统一格式
    df = df.rename(columns={
        '时间(UTC)': 'Time',
        '时间': 'Time',  # 兼容舊格式
        '交易对': 'Symbol',
        '资金费用': 'Funding_Fee',
        '资产': 'Asset'
    })

    # 转换时间格式
    df['Time'] = pd.to_datetime(df['Time'])

    # 选择需要的列
    df = df[['Time', 'Symbol', 'Funding_Fee']].copy()
    df = df.rename(columns={'Funding_Fee': 'Binance_FF'})

    print(f"币安处理后数据: {len(df)} 行")
    return df


def load_and_process_bybit_csv(filepath):
    """
    加载和处理Bybit CSV文件
    """
    print(f"正在加载Bybit文件: {filepath}")

    df = pd.read_csv(filepath)
    print(f"Bybit原始数据: {len(df)} 行")
    print(f"Bybit列名: {list(df.columns)}")

    # 重命名列以统一格式
    df = df.rename(columns={
        '交易时间(UTC)': 'Time',
        '交易时间': 'Time',  # 兼容舊格式
        '交易对': 'Symbol',
        '资金费用': 'Funding_Fee',
        '方向': 'Direction',
        '数量': 'Quantity',
        '费率': 'Fee_Rate',
        '货币': 'Currency'
    })

    # 转换时间格式
    df['Time'] = pd.to_datetime(df['Time'])

    # 选择需要的列
    df = df[['Time', 'Symbol', 'Funding_Fee']].copy()
    df = df.rename(columns={'Funding_Fee': 'Bybit_FF'})

    print(f"Bybit处理后数据: {len(df)} 行")
    return df


def merge_funding_fees(binance_df, bybit_df):
    """
    合并两个数据框
    """
    print("\n开始合并数据...")

    # 创建时间和交易对的组合索引用于合并
    binance_df['Time_Symbol'] = binance_df['Time'].dt.strftime('%Y-%m-%d %H:%M:%S') + '_' + binance_df['Symbol']
    bybit_df['Time_Symbol'] = bybit_df['Time'].dt.strftime('%Y-%m-%d %H:%M:%S') + '_' + bybit_df['Symbol']

    # 获取所有唯一的时间和交易对组合
    all_time_symbols = set(binance_df['Time_Symbol'].tolist() + bybit_df['Time_Symbol'].tolist())

    print(f"总共有 {len(all_time_symbols)} 个唯一的时间-交易对组合")

    # 创建完整的数据框架
    merged_data = []

    for time_symbol in all_time_symbols:
        time_str, symbol = time_symbol.rsplit('_', 1)
        time_obj = pd.to_datetime(time_str)

        # 查找币安数据
        binance_row = binance_df[binance_df['Time_Symbol'] == time_symbol]
        binance_ff = binance_row['Binance_FF'].iloc[0] if len(binance_row) > 0 else 0

        # 查找Bybit数据
        bybit_row = bybit_df[bybit_df['Time_Symbol'] == time_symbol]
        bybit_ff = bybit_row['Bybit_FF'].iloc[0] if len(bybit_row) > 0 else 0

        # 计算净资金费用 (币安 + Bybit)
        net_ff = binance_ff + bybit_ff

        merged_data.append({
            'Time': time_obj,
            'Symbol': symbol,
            'Binance FF': binance_ff,
            'Bybit FF': bybit_ff,
            'Net FF': net_ff
        })

    # 转换为DataFrame并排序
    merged_df = pd.DataFrame(merged_data)
    merged_df = merged_df.sort_values(['Time', 'Symbol']).reset_index(drop=True)

    print(f"合并后数据: {len(merged_df)} 行")
    return merged_df


def ensure_output_directory(output_dir="csv/Return"):
    """
    确保输出目录存在，如果不存在则创建

    Args:
        output_dir (str): 输出目录路径

    Returns:
        str: 输出目录路径
    """
    try:
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            print(f"✓ 已创建输出目录: {output_dir}")
        else:
            print(f"✓ 输出目录已存在: {output_dir}")
        return output_dir
    except Exception as e:
        print(f"✗ 创建输出目录失败: {str(e)}")
        print("将使用当前目录作为输出路径")
        return "."


def generate_output_filename(start_date, end_date):
    """
    生成输出文件名
    """
    filename = f"Binance_&_Bybit_FF_return_{start_date}_to_{end_date}.csv"
    return filename


def save_merged_data(merged_df, start_date, end_date, output_dir="csv/Return"):
    """
    保存合并后的数据到CSV文件

    Args:
        merged_df (pandas.DataFrame): 合并后的数据框
        start_date (str): 开始日期
        end_date (str): 结束日期
        output_dir (str): 输出目录

    Returns:
        str: 保存的文件路径
    """
    # 确保输出目录存在
    output_dir = ensure_output_directory(output_dir)

    # 生成文件名
    filename = generate_output_filename(start_date, end_date)
    filepath = os.path.join(output_dir, filename)

    try:
        # 保存CSV文件
        merged_df.to_csv(filepath, index=False, encoding='utf-8-sig')
        print(f"✓ 合并数据已保存到: {filepath}")
        return filepath
    except Exception as e:
        print(f"✗ 保存文件失败: {str(e)}")
        return None


def main():
    """
    主函数
    """
    print("=" * 60)
    print("币安和Bybit资金费用合并工具 (UTC时区)")
    print("=" * 60)
    print("注意：所有时间数据均为UTC+0时区")

    try:
        # 查找CSV文件
        print("步骤1: 查找CSV文件")
        print("-" * 40)

        input_dir = "csv/Return"
        binance_files, bybit_files = find_csv_files(input_dir)

        if not binance_files:
            print(f"❌ 在 {input_dir} 目录下未找到币安CSV文件 (binance_funding_fees_*.csv)")
            print("请确保已运行币安资金费用查询工具并生成了CSV文件")
            return

        if not bybit_files:
            print(f"❌ 在 {input_dir} 目录下未找到Bybit CSV文件 (bybit_funding_fees_*.csv)")
            print("请确保已运行Bybit资金费用查询工具并生成了CSV文件")
            return

        # 如果有多个文件，让用户选择或使用最新的
        binance_file = binance_files[0] if len(binance_files) == 1 else max(binance_files, key=os.path.getctime)
        bybit_file = bybit_files[0] if len(bybit_files) == 1 else max(bybit_files, key=os.path.getctime)

        print(f"\n使用文件:")
        print(f"币安: {binance_file}")
        print(f"Bybit: {bybit_file}")

        # 解析日期范围
        binance_dates = parse_date_from_filename(binance_file)
        bybit_dates = parse_date_from_filename(bybit_file)

        # 加载和处理数据
        print(f"\n步骤2: 加载数据")
        print("-" * 40)

        binance_df = load_and_process_binance_csv(binance_file)
        bybit_df = load_and_process_bybit_csv(bybit_file)

        # 合并数据
        print(f"\n步骤3: 合并数据")
        print("-" * 40)

        merged_df = merge_funding_fees(binance_df, bybit_df)

        # 确定日期范围用于文件名
        if binance_dates:
            start_date, end_date = binance_dates
        elif bybit_dates:
            start_date, end_date = bybit_dates
        else:
            start_date = datetime.now().strftime('%Y-%m-%d')
            end_date = start_date

        # 保存结果
        print(f"\n步骤4: 保存结果")
        print("-" * 40)

        output_dir = "csv/Return"
        saved_file = save_merged_data(merged_df, start_date, end_date, output_dir)

        if saved_file:
            print(f"✅ 合并完成！文件已保存为: {saved_file}")
        else:
            print("❌ 文件保存失败")
            return

        # 显示统计信息
        print(f"\n合并统计:")
        print("-" * 40)
        print(f"- 总记录数: {len(merged_df)}")
        print(f"- 涉及交易对: {merged_df['Symbol'].nunique()}")
        print(f"- 时间范围: {merged_df['Time'].min()} 至 {merged_df['Time'].max()}")
        print(f"- 币安总资金费用: {merged_df['Binance FF'].sum():.6f}")
        print(f"- Bybit总资金费用: {merged_df['Bybit FF'].sum():.6f}")
        print(f"- 净资金费用 (币安+Bybit): {merged_df['Net FF'].sum():.6f}")

        # 显示前几行预览
        print(f"\n数据预览 (前10行):")
        print("-" * 60)
        print(merged_df.head(10).to_string(index=False))

        # 按交易对统计
        print(f"\n按交易对统计:")
        print("-" * 40)
        symbol_stats = merged_df.groupby('Symbol').agg({
            'Binance FF': 'sum',
            'Bybit FF': 'sum',
            'Net FF': 'sum'
        }).round(6)
        print(symbol_stats.to_string())

    except Exception as e:
        print(f"❌ 发生错误: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    print("请确保已安装依赖包:")
    print("pip install pandas")
    print()

    main()