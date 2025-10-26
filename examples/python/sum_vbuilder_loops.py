#!/usr/bin/env python3
import argparse
import re
from pathlib import Path

def parse_args():
    p = argparse.ArgumentParser(description="Sum VersionBuilder inner loop times from RocksDB LOG")
    p.add_argument("log", type=Path, help="Path to LOG file")
    p.add_argument("--verbose", "-v", action="store_true", help="Print per-line matches")
    return p.parse_args()

# Match patterns like:
# [VersionBuilder] Delete inner loops: 158310 us (deleted 4 files), Insert inner loops: 129435 us (added 4 files)
RE_DELETE = re.compile(r"Delete inner loops:\s*(\d+)\s*us")
RE_INSERT = re.compile(r"Insert inner loops:\s*(\d+)\s*us")


def humanize(us: int) -> str:
    ms = us / 1000.0
    s = us / 1_000_000.0
    return f"{us} us | {ms:.3f} ms | {s:.6f} s"


def main():
    args = parse_args()
    if not args.log.exists():
        raise SystemExit(f"File not found: {args.log}")

    total_delete = 0
    total_insert = 0
    count_delete = 0
    count_insert = 0

    with args.log.open("r", encoding="utf-8", errors="ignore") as f:
        for lineno, line in enumerate(f, 1):
            mdel = RE_DELETE.search(line)
            mins = RE_INSERT.search(line)
            if mdel:
                val = int(mdel.group(1))
                total_delete += val
                count_delete += 1
                if args.verbose:
                    print(f"L{lineno}: delete += {val}")
            if mins:
                val = int(mins.group(1))
                total_insert += val
                count_insert += 1
                if args.verbose:
                    print(f"L{lineno}: insert += {val}")

    combined = total_delete + total_insert

    print("VersionBuilder inner loops time totals:")
    print(f"  Delete inner loops:  {humanize(total_delete)} (matches: {count_delete})")
    print(f"  Insert inner loops:  {humanize(total_insert)} (matches: {count_insert})")
    print(f"  Combined total:      {humanize(combined)}")

if __name__ == "__main__":
    main()

# python python/sum_vbuilder_loops.py /NV1/ysh/NEXT/examples/testdb/LOG --verbose
