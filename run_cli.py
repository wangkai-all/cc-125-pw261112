"""命令行入口：不启动 Web 服务也能在本地重跑完整实验。

示例：
  .venv/bin/python run_cli.py --rows 200000 --seed 20261114 --repeats 5
  .venv/bin/python run_cli.py --compressions snappy zstd gzip \
      --profiles defaults mixed --row-group-sizes 0 50000 --no-sorted
"""
from __future__ import annotations

import argparse
import json

from engine.datagen import DatasetSpec
from engine.queries import QueryConfig
from engine.runner import run_benchmark
from engine.schemes import COMPRESSIONS, PROFILES, default_plan


def main() -> None:
    ap = argparse.ArgumentParser(description="Parquet 压缩/编码对比实验（本地 CLI）")
    ap.add_argument("--rows", type=int, default=200_000)
    ap.add_argument("--seed", type=int, default=20261114)
    ap.add_argument("--compressions", nargs="+", default=["snappy", "zstd", "gzip"],
                    choices=COMPRESSIONS)
    ap.add_argument("--profiles", nargs="+", default=PROFILES, choices=PROFILES)
    ap.add_argument("--row-group-sizes", nargs="+", type=int, default=[0, 50_000])
    ap.add_argument("--no-sorted", action="store_true", help="不加入排序对照方案")
    ap.add_argument("--repeats", type=int, default=5, help="每条查询的重复执行次数")
    ap.add_argument("--out", default="artifacts")
    args = ap.parse_args()

    spec = DatasetSpec(rows=args.rows, seed=args.seed)
    schemes = default_plan(compressions=args.compressions,
                           profiles=args.profiles,
                           row_group_sizes=args.row_group_sizes,
                           add_sorted=not args.no_sorted)
    queries = [
        QueryConfig("point", "category", repeats=args.repeats),
        QueryConfig("range", "amount", repeats=args.repeats),
        QueryConfig("scan", "remark", repeats=args.repeats),
    ]
    manifest = run_benchmark(args.out, spec=spec, schemes=schemes,
                             query_configs=queries)
    ok = [s for s in manifest["schemes"] if s["status"] == "ok"]
    failed = [s for s in manifest["schemes"] if s["status"] != "ok"]
    print(f"\nrun_id: {manifest['run_id']}  通过 {len(ok)} / 失败 {len(failed)}")
    print(f"产物目录: {args.out}/{manifest['run_id']}/")
    for s in failed:
        print(f"  [失败] {s['name']}: {s['failure_reason']}")
    if ok:
        smallest = min(ok, key=lambda s: s["file_size_bytes"])
        fastest_write = min(ok, key=lambda s: s["write_ms"])
        print(f"  最小体积: {smallest['name']} ({smallest['file_size_bytes']} B)")
        print(f"  最快写入: {fastest_write['name']} ({fastest_write['write_ms']} ms)")
    print("\nmanifest: " +
          json.dumps({"run_id": manifest["run_id"],
                      "dataset_fingerprint": manifest["dataset"]["fingerprint"]},
                     ensure_ascii=False))


if __name__ == "__main__":
    main()
