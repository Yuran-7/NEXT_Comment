import pandas as pd

# 读取 CSV 文件
df = pd.read_csv("/NV1/ysh/dataset/buildings_10m_50.csv")

# 确认列名存在
if "area" not in df.columns:
    raise ValueError("CSV 文件中没有名为 'area' 的列")

# 统计不同的浮点值数量
unique_count = df["area"].nunique()

print(f"area 列中共有 {unique_count} 个不同的值")

# python python/count_cardinality.py
