#!/usr/bin/env python3
import random
import math
import argparse
from typing import Optional

# --------------------------
# 基准配置 (论文参考值)
# --------------------------
TOTAL_FULL = 115_000_000  # 论文使用的数据集规模
TARGET_SPATIAL = {0.001: 17, 0.01: 608, 0.1: 18753}
TARGET_NUMERIC = {0.001: 403, 0.01: 3929, 0.1: 40064}

# --------------------------
# 缩放目标值
# --------------------------
def scale_targets(n):
    scale = n / TOTAL_FULL
    spatial = {k: max(1, int(v * scale)) for k, v in TARGET_SPATIAL.items()}
    numeric = {k: max(1, int(v * scale)) for k, v in TARGET_NUMERIC.items()}
    return spatial, numeric

# --------------------------
# 随机生成 polygon
# --------------------------
def generate_polygon(lon, lat):
    # 随机生成矩形建筑
    dx = random.uniform(0.00005, 0.002)
    dy = random.uniform(0.00005, 0.002)
    minLon, maxLon = lon, lon + dx
    minLat, maxLat = lat, lat + dy
    polygon = (
        f"POLYGON (({minLon:.7f} {minLat:.7f}, "
        f"{maxLon:.7f} {minLat:.7f}, "
        f"{maxLon:.7f} {maxLat:.7f}, "
        f"{minLon:.7f} {maxLat:.7f}, "
        f"{minLon:.7f} {minLat:.7f}))"
    )
    # 返回 polygon 以及对应的最小/最大经纬度
    return polygon, minLon, minLat, maxLon, maxLat

# --------------------------
# 随机生成 tags (填充大小用)
# --------------------------
def generate_tags_fixed_length(target_len: int, add_some_random: bool = False) -> str:
    """
    生成指定字节长度（ASCII）的 tags 字符串。
    形式仍为: [key#value,...]，为保证精确长度，会包含一个 pad#xxxxx 标签。

    注意: 最小形式 "[pad#]" 长度为 6。若 target_len 小于 6，则强制为 6。
    """
    # 保证最小长度，可精确控制
    min_len = 6  # "[pad#]"
    L = max(min_len, int(target_len))

    # 先构造一些随机标签（可选），再用 pad# 填到指定长度
    inner = ""
    if add_some_random and L > 20:  # 仅在长度足够时增加一些随机性
        rand_tags = []
        # 先放 1~3 个随机标签
        for _ in range(random.randint(1, 3)):
            key = random.choice([
                "building", "addr:street", "addr:postcode", "name",
                "contact:phone", "contact:website", "material",
                "roof:shape", "height", "levels", "tourism",
                "office", "shop", "amenity", "note"
            ])
            value = ''.join(random.choices("abcdefghijklmnopqrstuvwxyz0123456789", k=random.randint(5, 15)))
            rand_tags.append(f"{key}#{value}")
        inner = ",".join(rand_tags)

    # 现在 inner 的长度为 len(inner)。最终 tags = "[" + inner + ("," if inner else "") + "pad#" + pad + "]"
    # 设 pad 的长度为 k，则总长:
    # 2 (外侧中括号) + len(inner) + (1 if inner else 0) + 4 ("pad#") + k == L
    overhead = 2 + len(inner) + (1 if inner else 0) + 4
    k = max(0, L - overhead)
    pad = "x" * k
    if inner:
        content = inner + "," + "pad#" + pad
    else:
        content = "pad#" + pad
    return "[" + content + "]"

def generate_tags_random():
    """保留原始的随机 tag 生成逻辑（不保证长度）。"""
    tags = []
    num_tags = random.randint(5, 15)  # 每个建筑 5~15 个标签
    for _ in range(num_tags):
        key = random.choice([
            "building", "addr:street", "addr:postcode", "name",
            "contact:phone", "contact:website", "material",
            "roof:shape", "height", "levels", "tourism",
            "office", "shop", "amenity", "note"
        ])
        value = ''.join(random.choices("abcdefghijklmnopqrstuvwxyz0123456789", k=random.randint(10, 50)))
        tags.append(f"{key}#{value}")
    return "[" + ",".join(tags) + "]"

def parse_size_to_bytes(text: Optional[str]) -> Optional[int]:
    """解析形如 '13GB', '13GiB', '13000000000' 的大小字符串为字节数。返回 None 表示不指定。"""
    if text is None:
        return None
    s = text.strip().replace(" ", "")
    if not s:
        return None
    u = s.upper()

    # 映射：十进制与二进制单位
    dec = {
        "KB": 10**3,
        "MB": 10**6,
        "GB": 10**9,
        "TB": 10**12,
    }
    bin_ = {
        "KIB": 1024**1,
        "MIB": 1024**2,
        "GIB": 1024**3,
        "TIB": 1024**4,
    }

    # 匹配带单位
    for unit, factor in {**dec, **bin_}.items():
        if u.endswith(unit):
            num = float(u[:-len(unit)])
            return int(num * factor)

    # 纯数字 => 字节
    try:
        return int(float(u))
    except ValueError:
        raise argparse.ArgumentTypeError(f"无法解析大小: {text}")

# --------------------------
# 主函数
# --------------------------
def generate_buildings(n: int, outfile: str, target_size_bytes: Optional[int] = None, precise: bool = True):
    spatial_target, numeric_target = scale_targets(n)
    print(f"[INFO] 数据规模: {n}")
    print(f"[INFO] 目标基数 (Spatial): {spatial_target}")
    print(f"[INFO] 目标基数 (Numeric): {numeric_target}")

    if target_size_bytes is not None and target_size_bytes > 0:
        avg = target_size_bytes / n
        print(f"[INFO] 目标文件大小: {target_size_bytes} 字节 (~{target_size_bytes/1e9:.3f} GB)")
        print(f"[INFO] 期望平均每行字节: {avg:.2f}")
    else:
        print(f"[INFO] 未设置目标文件大小，将使用随机 tags（大小不受控）")

    hotspots = 100  # 城市热点数量
    sigma = 0.05    # 每个热点的分布范围
    centers = [(random.uniform(-60, 60), random.uniform(-180, 180)) for _ in range(hotspots)]

    bytes_written = 0
    newline_len = 1  # 写入 "\n" 的字节数（UTF-8 下为 1）

    with open(outfile, "w", encoding="utf-8") as f:
        for i in range(1, n + 1):
            # 选择一个热点中心
            lat_c, lon_c = random.choice(centers)
            lat = random.gauss(lat_c, sigma)
            lon = random.gauss(lon_c, sigma)

            # 面积和周长 (log-normal)
            area = abs(random.lognormvariate(5, 0.6))
            perimeter = area ** 0.5 * 4  # 粗略假设矩形（未输出）

            # polygon 以及对应的 MBR（不再使用固定 0.001 的差值）
            polygon, minLon, minLat, maxLon, maxLat = generate_polygon(lon, lat)

            # 行前缀（不包含 tags 与换行）
            prefix = (f"{i}\t{area:.6f}\t{minLon:.7f}\t{minLat:.7f}\t"
                      f"{maxLon:.7f}\t{maxLat:.7f}\t{polygon}\t")
            base_len = len(prefix)

            if target_size_bytes and precise:
                # 自适应分配当前行应占字节数，保证最终总大小贴近目标
                remaining_lines = n - i + 1
                remaining_bytes = target_size_bytes - bytes_written
                desired_this_line = max(1, int(round(remaining_bytes / remaining_lines)))
                # 目标 tags 长度（ASCII 字节数）
                target_tags_len = max(6, desired_this_line - base_len - newline_len)
                tags = generate_tags_fixed_length(target_tags_len, add_some_random=False)
            elif target_size_bytes and not precise:
                # 只按平均分配一次，不实时纠偏
                desired_this_line = int(round(target_size_bytes / n))
                target_tags_len = max(6, desired_this_line - base_len - newline_len)
                tags = generate_tags_fixed_length(target_tags_len, add_some_random=True)
            else:
                tags = generate_tags_random()

            line = prefix + tags + "\n"

            # 写入并更新统计
            f.write(line)
            bytes_written += len(line.encode("utf-8"))  # 安全起见用字节数

            if i % 1_000_000 == 0:
                if target_size_bytes:
                    progress = bytes_written / target_size_bytes * 100
                    print(f"[INFO] 已生成 {i} 条记录，当前大小 {bytes_written} 字节（{progress:.2f}%）")
                else:
                    print(f"[INFO] 已生成 {i} 条记录")

    print(f"[INFO] 数据写入完成: {outfile}")
    if target_size_bytes:
        diff = bytes_written - target_size_bytes
        sign = "+" if diff >= 0 else "-"
        print(f"[INFO] 实际大小: {bytes_written} 字节，目标: {target_size_bytes} 字节，偏差: {sign}{abs(diff)} 字节 ({diff/target_size_bytes*100:.4f}%)")


# --------------------------
# CLI
# --------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="OSM Building Synthetic Dataset Generator")
    parser.add_argument("-n", "--num", type=int, required=True, help="生成的数据条数，例如 33000000")
    parser.add_argument("-o", "--out", type=str, default="osm_buildings.txt", help="输出文件名")
    parser.add_argument("--target-size", type=str, default=None, help="目标文件大小，例如 13GB / 13GiB / 13000000000")
    parser.add_argument("--no-precise", action="store_true", help="不进行逐行纠偏（更快但误差略大）")
    args = parser.parse_args()

    # 若未指定目标大小，且规模为 3300 万，默认按 13GB 目标
    target_size_str = args.target_size
    if target_size_str is None and args.num == 33_000_000:
        target_size_str = "13GB"

    target_bytes = parse_size_to_bytes(target_size_str)
    generate_buildings(args.num, args.out, target_size_bytes=target_bytes, precise=(not args.no_precise))