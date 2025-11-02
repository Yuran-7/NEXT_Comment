#!/usr/bin/env python3
"""
reduce_cardinality.py

将 CSV 第二列（index=1）浮点列的基数降低为原来的 1/x，并使新取值在原始[min, max]区间内随机分布。

用法示例：
python3 reduce_cardinality.py --input /path/buildings_10m.csv --output /path/buildings_10m_10.csv --x 10
"""

import argparse
import os
import sys
import numpy as np
import pandas as pd

def reduce_cardinality(input_csv, output_csv, x, seed=None, no_header=False):
    try:
        if seed is not None:
            np.random.seed(seed)

        if not os.path.isfile(input_csv):
            print(f"[ERROR] 输入文件不存在: {input_csv}", file=sys.stderr)
            return 2

        # 读取 CSV；如果没有表头，用 header=None
        header_arg = None if no_header else 0
        df = pd.read_csv(input_csv, header=header_arg, low_memory=False)

        if df.shape[1] < 2:
            print("[ERROR] CSV 至少需要两列。", file=sys.stderr)
            return 3

        # 取第二列（index=1）
        col_idx = 1
        col_name = df.columns[col_idx] if not no_header else col_idx
        col_series = df.iloc[:, col_idx].astype(float)  # 强制转浮点，便于unique/min/max

        unique_values = pd.unique(col_series)
        n_unique = len(unique_values)
        target_unique = max(1, n_unique // x)

        print(f"原始唯一值数量: {n_unique}, 目标唯一值数量: {target_unique}")

        min_val = float(col_series.min())
        max_val = float(col_series.max())
        if min_val == max_val:
            print("[WARN] 列的最小值等于最大值，无法随机化区间。输出原始文件的复制。")
            df.to_csv(output_csv, index=False, header=(not no_header))
            print(f"已生成文件（复制）: {output_csv}")
            return 0

        # 在 [min, max] 内生成 target_unique 个随机取值（均匀分布）
        chosen_values = np.random.uniform(min_val, max_val, size=target_unique)

        # 为每个原始唯一值随机分配一个 chosen_value（replace=True）
        # 这样确保最终实际唯一值数量接近 target_unique
        mapping_targets = np.random.choice(chosen_values, size=n_unique, replace=True)

        # 建立映射 dict（注意 unique_values 可能是 numpy array，使用 plain python float 作为键更稳）
        # 为避免浮点精度导致的键查找问题，用原始 unique values 的字符串形式作为键
        unique_keys = [float(v) for v in unique_values]
        mapping = {repr(k): float(v) for k, v in zip(unique_keys, mapping_targets)}

        # 应用映射：先把每个值转为 repr，然后 map
        col_as_repr = col_series.map(lambda v: repr(float(v)))
        new_col = col_as_repr.map(mapping)

        # 如果某些原始值没有在 mapping 中（理论上不应发生），就随机选择一个 chosen_value 填充
        missing_mask = new_col.isna()
        if missing_mask.any():
            print(f"[WARN] {missing_mask.sum()} 个值未命中映射，使用随机 chosen_value 填充。")
            new_col.loc[missing_mask] = np.random.choice(chosen_values, size=missing_mask.sum())

        # 将替换后的列写回 DataFrame（保持列名或位置）
        df.iloc[:, col_idx] = new_col.astype(float)

        # 确保输出目录存在
        out_dir = os.path.dirname(os.path.abspath(output_csv))
        if out_dir and not os.path.isdir(out_dir):
            os.makedirs(out_dir, exist_ok=True)

        # 写文件（如果无表头，header=False）
        df.to_csv(output_csv, index=False, header=(not no_header))

        final_unique = pd.unique(df.iloc[:, col_idx]).shape[0]
        print(f"实际唯一值数量: {final_unique}")
        print(f"已生成文件: {output_csv}")
        return 0

    except Exception as e:
        print("[EXCEPTION] 运行出错：", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1

def main():
    parser = argparse.ArgumentParser(description="降低CSV第二列浮点数的基数，并使新取值随机分布")
    parser.add_argument("--input", required=True, help="输入 CSV 文件路径")
    parser.add_argument("--output", required=True, help="输出 CSV 文件路径")
    parser.add_argument("--x", type=int, required=True, help="基数缩放因子 (例如 10 表示唯一值变为原来的 1/10)")
    parser.add_argument("--seed", type=int, default=None, help="随机种子（可选）")
    parser.add_argument("--no-header", action="store_true", help="如果 CSV 没有表头，使用此参数")
    args = parser.parse_args()

    rc = reduce_cardinality(args.input, args.output, args.x, seed=args.seed, no_header=args.no_header)
    sys.exit(rc)

if __name__ == "__main__":
    main()
# python python/reduce_cardinality.py --input /NV1/ysh/dataset/buildings_10m.csv --output /NV1/ysh/dataset/buildings_10m_50.csv --x 50