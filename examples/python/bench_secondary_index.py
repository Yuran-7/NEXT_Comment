#!/usr/bin/env python3
import argparse
import re
import shlex
import subprocess
import sys
import time
from pathlib import Path

DEFAULT_CMD = "./secondary_index_data_write_num /NV1/ysh/NEXT/examples/testdb 10000000 /NV1/ysh/dataset/buildings_10m.csv"
RE_EXEC = re.compile(r"Execution time:\s*([0-9.]+)\s*seconds", re.IGNORECASE)


def run_once(cmd: str, cwd: Path | None = None, timeout: int | None = None) -> tuple[float | None, float | None, str]:
    """
    Run one iteration.
    Returns: (wall_seconds, program_seconds, combined_output)
    - wall_seconds: measured by this script
    - program_seconds: parsed from program output if available, else None
    - combined_output: stdout+stderr text
    """
    t0 = time.monotonic_ns()
    try:
        proc = subprocess.run(
            cmd if isinstance(cmd, list) else shlex.split(cmd),
            cwd=str(cwd) if cwd else None,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=timeout,
            check=False,
            text=True,
        )
        out = proc.stdout or ""
    except subprocess.TimeoutExpired as e:
        return None, None, f"<TIMEOUT> {e}"
    except Exception as e:
        return None, None, f"<ERROR> {e}"
    t1 = time.monotonic_ns()
    wall = (t1 - t0) / 1_000_000_000.0

    m = RE_EXEC.search(out)
    prog = float(m.group(1)) if m else None
    return wall, prog, out


def main():
    ap = argparse.ArgumentParser(description="Run secondary_index_data_write_num N times and report average time")
    ap.add_argument("--cmd", default=DEFAULT_CMD, help="Command to execute each run")
    ap.add_argument("--runs", type=int, default=100, help="Number of iterations")
    ap.add_argument("--cwd", type=Path, default=Path("/NV1/ysh/NEXT/examples"), help="Working directory to execute in")
    ap.add_argument("--timeout", type=int, default=None, help="Per-run timeout in seconds (optional)")
    ap.add_argument("--log", type=Path, default=None, help="Optional path to write combined output log")
    ap.add_argument("--quiet", action="store_true", help="Reduce per-run printing")
    args = ap.parse_args()

    walls = []
    progs = []

    log_f = None
    if args.log:
        args.log.parent.mkdir(parents=True, exist_ok=True)
        log_f = args.log.open("w", encoding="utf-8")

    try:
        for i in range(1, args.runs + 1):
            wall, prog, out = run_once(args.cmd, cwd=args.cwd, timeout=args.timeout)
            if log_f is not None:
                print(f"===== Run {i} =====", file=log_f)
                log_f.write(out)
                if not out.endswith("\n"):
                    log_f.write("\n")
                log_f.flush()
            if wall is not None:
                walls.append(wall)
            if prog is not None:
                progs.append(prog)
            if not args.quiet:
                msg = f"Run {i:3d}: wall={wall:.3f}s"
                if prog is not None:
                    msg += f", exec={prog:.3f}s"
                print(msg)
    except KeyboardInterrupt:
        print("\nInterrupted by user; computing partial averages...")
    finally:
        if log_f is not None:
            log_f.close()

    def avg(arr):
        return sum(arr) / len(arr) if arr else float('nan')

    wall_avg = avg(walls)
    prog_avg = avg(progs)

    print("\nSummary:")
    print(f"  Runs attempted: {args.runs}")
    print(f"  Runs completed (wall): {len(walls)}")
    print(f"  Average wall time: {wall_avg:.3f} s" if walls else "  Average wall time: N/A")
    if progs:
        print(f"  Runs parsed (program 'Execution time'): {len(progs)}")
        print(f"  Average program execution time: {prog_avg:.3f} s")
    else:
        print("  Program 'Execution time' line not found; only wall time available")

if __name__ == "__main__":
    # Safety: warn when default command implies very heavy workload
    if len(sys.argv) == 1:
        print("Note: default command runs 10,000,000 records x 100 iterations — very heavy.\n"
              "Pass --runs and/or --cmd to adjust, e.g., --runs 3 or smaller dataset.")
    main()

# python python/bench_secondary_index.py --runs 3 --cmd "./secondary_index_data_write_num /NV1/ysh/NEXT/examples/testdb 10000000 /NV1/ysh/dataset/buildings_10m.csv"