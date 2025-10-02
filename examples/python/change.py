import os

# 原始文件路径
input_file = "/NV1/ysh/dataset/buildings_1m/buildings_1D_query_0.01"

# 输出文件路径（把 0.01 改为 0.001）
output_file = input_file.replace("0.01", "0.001")

with open(input_file, "r") as fin, open(output_file, "w") as fout:
    for line in fin:
        parts = line.strip().split()
        if len(parts) == 2:
            try:
                first = float(parts[0])
                # 第二个数 = 第一个数 + 0.001
                second = first + 0.001
                fout.write(f"{first:.6f} {second:.6f}\n")
            except ValueError:
                # 遇到非浮点数，原样写入
                fout.write(line)
        else:
            # 不是两列，原样写入
            fout.write(line)

print(f"处理完成 ✅ 新文件保存为: {output_file}")
