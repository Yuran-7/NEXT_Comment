import pandas as pd
import numpy as np
import random

def reduce_cardinality(input_csv, output_csv, x):
    """
    降低第二列的基数
    input_csv: 输入 CSV 文件路径
    output_csv: 输出 CSV 文件路径
    x: 基数缩放因子，例如 x=10 表示只保留原来的1/10的唯一值数量
    """
    # 读取数据
    df = pd.read_csv(input_csv)

    if df.shape[1] < 2:
        raise ValueError("CSV 至少需要两列")

    col_name = df.columns[1]
    original_values = df[col_name].values

    unique_values = np.unique(original_values)
    n_unique = len(unique_values)
    target_unique = max(1, n_unique // x)

    print(f"原始唯一值数量: {n_unique}, 目标唯一值数量: {target_unique}")

    # 从原有唯一值中随机挑选 target_unique 个值
    chosen_values = np.random.choice(unique_values, size=target_unique, replace=False)

    # 为了让分布尽量均匀，把原始值的范围分桶映射到 chosen_values
    # 按原始值的排序，均匀地映射
    chosen_values_sorted = np.sort(chosen_values)
    min_val, max_val = np.min(original_values), np.max(original_values)
    bins = np.linspace(min_val, max_val, target_unique + 1)

    # 对原始值进行分桶，并替换为对应的 chosen_value
    indices = np.digitize(original_values, bins) - 1
    indices = np.clip(indices, 0, target_unique - 1)
    new_values = chosen_values_sorted[indices]

    # 更新数据并保存
    df[col_name] = new_values
    df.to_csv(output_csv, index=False)
    print(f"已生成文件：{output_csv}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="降低CSV第二列浮点数的基数")
    parser.add_argument("--input", required=True, help="输入CSV文件路径")
    parser.add_argument("--output", required=True, help="输出CSV文件路径")
    parser.add_argument("--x", type=int, required=True, help="基数缩放因子 (例如10表示1/10)")
    args = parser.parse_args()

    reduce_cardinality(args.input, args.output, args.x)

# python python/reduce_cardinality.py --input /NV1/ysh/dataset/buildings_10m.csv --output /NV1/ysh/dataset/buildings_10m_20.csv --x 20
