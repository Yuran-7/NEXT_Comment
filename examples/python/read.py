import pandas as pd

# === 参数配置 ===
csv_path = "/NV1/ysh/dataset/buildings_1m/buildings_1m.csv"  # 你的 CSV 文件路径
target_value = 107.261262
column_name = "area"  # 列名

# === 读取CSV文件 ===
df = pd.read_csv(csv_path)

# 检查是否包含area列
if column_name not in df.columns:
    raise ValueError(f"CSV中未找到列 '{column_name}'")

# === 找出最接近目标值的行 ===
df[column_name] = pd.to_numeric(df[column_name], errors="coerce")  # 确保为浮点型
df = df.dropna(subset=[column_name])  # 删除无效值

# 计算差值的绝对值，并找出最小差值的行
closest_index = (df[column_name] - target_value).abs().idxmin()
closest_row = df.loc[closest_index]

# === 输出结果 ===
print("最接近的行索引:", closest_index)
print("该行内容如下：")
print(closest_row)
