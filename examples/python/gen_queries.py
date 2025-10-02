#!/usr/bin/env python3
import argparse
import random
import sys
from typing import List, Tuple, Optional, Dict
import math

# --------------------------
# 参考基数（与数据生成脚本一致）
# --------------------------
TOTAL_FULL = 115_000_000  # 论文使用的数据集规模
TARGET_SPATIAL = {0.001: 17, 0.01: 608, 0.1: 18753}
TARGET_NUMERIC = {0.001: 403, 0.01: 3929, 0.1: 40064}


def scale_targets(n):
    scale = n / TOTAL_FULL
    spatial = {k: max(1, int(v * scale)) for k, v in TARGET_SPATIAL.items()}
    numeric = {k: max(1, int(v * scale)) for k, v in TARGET_NUMERIC.items()}
    return spatial, numeric


def parse_dataset(
    path: str,
    sample_size: int,
    seed: int,
    need_area: bool,
) -> Tuple[int, List[float], List[float], Optional[List[float]]]:
    """
    单次顺序扫描，计数总行数 n，做蓄水池采样得到经纬度样本（以及可选的 area 样本）。
    返回: (n, sample_lon, sample_lat, sample_area)
    """
    random.seed(seed)

    n = 0
    sample_lon: List[float] = []
    sample_lat: List[float] = []
    sample_area: Optional[List[float]] = [] if need_area else None

    # 蓄水池采样算法 R
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            n += 1
            if not line:
                continue
            parts = line.rstrip('\n').split('\t')
            if len(parts) < 6:
                continue
            try:
                area = float(parts[1])
                lon = float(parts[2])
                lat = float(parts[3])
            except ValueError:
                continue

            if len(sample_lon) < sample_size:
                sample_lon.append(lon)
                sample_lat.append(lat)
                if need_area and sample_area is not None:
                    sample_area.append(area)
            else:
                # 以概率 sample_size / n 替换已有样本中的随机一个
                j = random.randint(1, n)
                if j <= sample_size:
                    idx = random.randint(0, sample_size - 1)
                    sample_lon[idx] = lon
                    sample_lat[idx] = lat
                    if need_area and sample_area is not None:
                        sample_area[idx] = area

    return n, sample_lon, sample_lat, sample_area


def bisect_left(a: List[float], x: float) -> int:
    lo, hi = 0, len(a)
    while lo < hi:
        mid = (lo + hi) // 2
        if a[mid] < x:
            lo = mid + 1
        else:
            hi = mid
    return lo


def bisect_right(a: List[float], x: float) -> int:
    lo, hi = 0, len(a)
    while lo < hi:
        mid = (lo + hi) // 2
        if a[mid] <= x:
            lo = mid + 1
        else:
            hi = mid
    return lo


def gen_numeric_queries(
    n: int,
    sample_vals: List[float],
    window: float,
    target_count: int,
    qnum: int,
    tolerance: float,
    out_path: str,
) -> Tuple[int, int]:
    """
    从样本生成一维数值范围查询，列使用传入的 sample_vals（可来自 lon/lat/area）。
    目标是 scaled target_count，允许相对误差 tolerance。
    输出格式：
    NUMERIC\tmin\tmax\ttarget\testimated
    """
    mul = n / max(1, len(sample_vals))
    sample_vals_sorted = sorted(sample_vals)

    want = qnum
    got = 0
    attempts = 0
    max_attempts = qnum * 50

    with open(out_path, 'w', encoding='utf-8') as fout:
        fout.write(f"# MODE=NUMERIC\tWINDOW={window}\tTARGET={target_count}\tTOL={tolerance}\n")
        while got < want and attempts < max_attempts:
            attempts += 1
            center = random.choice(sample_vals_sorted)
            half = window / 2.0
            lo = center - half
            hi = center + half

            lidx = bisect_left(sample_vals_sorted, lo)
            ridx = bisect_right(sample_vals_sorted, hi)
            sample_count = max(0, ridx - lidx)
            est = int(round(sample_count * mul))

            if target_count <= 0:
                accept = sample_count >= 0  # 总能接收
            else:
                rel = abs(est - target_count) / max(1, target_count)
                accept = rel <= tolerance

            if accept:
                fout.write(f"NUMERIC\t{lo}\t{hi}\t{target_count}\t{est}\n")
                got += 1

    return got, attempts


def build_spatial_grid(sample_lon: List[float], sample_lat: List[float], cell: float) -> Dict[Tuple[int, int], int]:
    grid: Dict[Tuple[int, int], int] = {}
    inv = 1.0 / cell
    for x, y in zip(sample_lon, sample_lat):
        ix = math.floor(x * inv)
        iy = math.floor(y * inv)
        grid[(ix, iy)] = grid.get((ix, iy), 0) + 1
    return grid


def grid_sum(grid: Dict[Tuple[int, int], int], cell: float, xmin: float, ymin: float, xmax: float, ymax: float) -> int:
    inv = 1.0 / cell
    ix0 = math.floor(xmin * inv)
    iy0 = math.floor(ymin * inv)
    ix1 = math.floor(xmax * inv)
    iy1 = math.floor(ymax * inv)
    s = 0
    for ix in range(ix0, ix1 + 1):
        for iy in range(iy0, iy1 + 1):
            s += grid.get((ix, iy), 0)
    return s


def gen_spatial_queries(
    n: int,
    sample_lon: List[float],
    sample_lat: List[float],
    window: float,
    target_count: int,
    qnum: int,
    tolerance: float,
    out_path: str,
) -> Tuple[int, int]:
    """
    从样本生成二维经纬度矩形查询（以中心为基准，边长 = window）。
    通过网格计数近似估计命中数，缩放到全量，目标是 target_count。
    输出格式：
    SPATIAL\tminLon\tminLat\tmaxLon\tmaxLat\ttarget\testimated
    """
    mul = n / max(1, len(sample_lon))
    # 网格单元选为 window/4，使得每个查询覆盖约 4x4=16 个格子
    cell = window / 4.0
    grid = build_spatial_grid(sample_lon, sample_lat, cell)

    want = qnum
    got = 0
    attempts = 0
    max_attempts = qnum * 100

    with open(out_path, 'w', encoding='utf-8') as fout:
        fout.write(f"# MODE=SPATIAL\tWINDOW={window}\tTARGET={target_count}\tTOL={tolerance}\n")
        while got < want and attempts < max_attempts:
            attempts += 1
            idx = random.randrange(len(sample_lon))
            cx = sample_lon[idx]
            cy = sample_lat[idx]
            half = window / 2.0
            xmin = cx - half
            ymin = cy - half
            xmax = cx + half
            ymax = cy + half
            # 修正到合法范围
            xmin = max(-180.0, xmin)
            xmax = min(180.0, xmax)
            ymin = max(-90.0, ymin)
            ymax = min(90.0, ymax)
            if xmin >= xmax or ymin >= ymax:
                continue

            sample_count = grid_sum(grid, cell, xmin, ymin, xmax, ymax)
            est = int(round(sample_count * mul))

            if target_count <= 0:
                accept = sample_count >= 0
            else:
                rel = abs(est - target_count) / max(1, target_count)
                accept = rel <= tolerance

            if accept:
                fout.write(f"SPATIAL\t{xmin}\t{ymin}\t{xmax}\t{ymax}\t{target_count}\t{est}\n")
                got += 1

    return got, attempts


def main():
    p = argparse.ArgumentParser(description="Generate query workloads (numeric 1D or spatial 2D) with approximate target cardinalities.")
    p.add_argument('--dataset', '-d', type=str, required=True, help='输入数据集路径（由 gen_osm_buildings.py 生成的 TSV）')
    p.add_argument('--num', '-n', type=int, default=1000, help='生成查询数量，默认 1000')
    p.add_argument('--mode', '-m', choices=['numeric', 'spatial'], required=True, help='numeric 一维 或 spatial 二维(经纬度)')
    p.add_argument('--window', '-w', type=float, choices=[0.001, 0.01, 0.1], required=True, help='查询窗口：0.001 / 0.01 / 0.1')
    p.add_argument('--numeric-col', choices=['lon', 'lat', 'area'], default='lon', help='numeric 模式下使用的列，默认 lon')
    p.add_argument('--sample-size', type=int, default=1_000_000, help='采样规模，默认 100 万（足以估计基数）')
    p.add_argument('--tolerance', type=float, default=0.25, help='相对误差容忍度，默认 ±25%')
    p.add_argument('--seed', type=int, default=42, help='随机种子')
    p.add_argument('--out', '-o', type=str, default='queries.tsv', help='输出查询文件')

    args = p.parse_args()
    random.seed(args.seed)

    need_area = (args.mode == 'numeric' and args.numeric_col == 'area')
    n, sample_lon, sample_lat, sample_area = parse_dataset(
        args.dataset, args.sample_size, args.seed, need_area=need_area
    )
    spatial_target, numeric_target = scale_targets(n)

    print(f"[INFO] 数据集行数 n = {n}")
    print(f"[INFO] 样本规模 = {len(sample_lon)}")
    print(f"[INFO] 目标基数 (Spatial): {spatial_target}")
    print(f"[INFO] 目标基数 (Numeric): {numeric_target}")

    if args.mode == 'numeric':
        t = numeric_target[args.window]
        if args.numeric_col == 'lon':
            sample_vals = sample_lon
        elif args.numeric_col == 'lat':
            sample_vals = sample_lat
        else:
            assert sample_area is not None
            sample_vals = sample_area
        got, attempts = gen_numeric_queries(n, sample_vals, args.window, t, args.num, args.tolerance, args.out)
    else:
        t = spatial_target[args.window]
        got, attempts = gen_spatial_queries(n, sample_lon, sample_lat, args.window, t, args.num, args.tolerance, args.out)

    if got < args.num:
        print(f"[WARN] 仅生成 {got}/{args.num} 个满足条件的查询（尝试 {attempts} 次）。可增加 --sample-size 或放宽 --tolerance。")
    else:
        print(f"[INFO] 已生成 {got} 个查询，写入 {args.out}")


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
