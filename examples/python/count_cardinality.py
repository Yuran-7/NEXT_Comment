#!/usr/bin/env python3
import sys
from collections import Counter

if len(sys.argv) != 2:
    print(f"用法: {sys.argv[0]} <文件路径>")
    sys.exit(1)

filename = sys.argv[1]
counter = Counter()

# 统计第二列出现次数
with open(filename, 'r', encoding='utf-8') as f:
    for line in f:
        if not line.strip():
            continue
        parts = line.split()
        if len(parts) < 2:
            continue
        value = parts[1]
        counter[value] += 1

# 打印出现次数 > 1 的值
print("重复出现的第二列值及其次数：")
dup_count = 0
for val, cnt in counter.items():
    if cnt > 1:
        print(f"{val}\t{cnt}")
        dup_count += 1

# 打印统计信息
print(f"\n共有 {len(counter)} 个不同的第二列值。")
print(f"其中 {dup_count} 个值出现次数大于 1。")


# python python/count_cardinality.py /NV1/ysh/dataset/buildings_10m.csv
