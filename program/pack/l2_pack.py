#!/usr/bin/env python3
"""把 L2 原始数据按天打包（供 8001 API 断点续传下载到本地）。

为什么需要
  L2 数据（`/www/wwwroot/polymarket-l2/l2_data/<date>/*.jsonl.gz`，2111 文件 / 277MB）是
  **我们自己的 recorder 下载的**（`polymarket-l2/recorder_l2_dual.py`，43 上跑）✓。
  用户要求：**打包 + 放在 8001 上支持断点续传 + 下载到本地**（而不是全量 rsync 同步 ✗）。

做法
  · 每天一个 tar.gz：`l2_data/packs/l2_<date>.tar.gz` + 同名 `.sha256`
  · **幂等/增量**：已存在且比当天目录新 → 跳过（`--force` 可强制重打）
  · 默认只打「已结束的日期」+ 当天（当天会随数据增长，用 --today 重打）
  · 只依赖 tarfile/gzip（无需外部 tar，避免解析差异）

用法（43 上）
  python -B tools/l2_pack.py                # 打包所有缺的日期
  python -B tools/l2_pack.py --force        # 全部重打
  python -B tools/l2_pack.py --date 2026-09-10 --force   # 只重打某天
"""

from __future__ import annotations

import argparse
import hashlib
import os
import tarfile
import time

L2 = "/www/wwwroot/polymarket-l2/l2_data"
PACKS = os.path.join(L2, "packs")


def sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def pack(date: str, force: bool) -> None:
    src = os.path.join(L2, date)
    if not os.path.isdir(src):
        print(f"  跳过 {date}（目录不存在）")
        return
    out = os.path.join(PACKS, f"l2_{date}.tar.gz")
    if os.path.exists(out) and not force:
        newest = 0.0
        for root, _, files in os.walk(src):
            for fn in files:
                newest = max(newest, os.path.getmtime(os.path.join(root, fn)))
        if os.path.getmtime(out) >= newest:
            print(f"  跳过 {date}（包已是最新，{os.path.getsize(out) / 1e6:.1f}MB）")
            return
    n = sum(len(fs) for _, _, fs in os.walk(src))
    t0 = time.time()
    with tarfile.open(out, "w:gz", compresslevel=6) as tf:
        tf.add(src, arcname=date)  # 包内顶层 = 日期目录
    digest = sha256(out)
    with open(out + ".sha256", "w") as f:
        f.write(f"{digest}  l2_{date}.tar.gz\n")
    print(
        f"  ✓ {date}: {n} 文件 → {os.path.getsize(out) / 1e6:.1f}MB  "
        f"({time.time() - t0:.1f}s)  sha256={digest[:16]}…"
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default="")
    ap.add_argument("--force", action="store_true")
    a = ap.parse_args()
    os.makedirs(PACKS, exist_ok=True)
    dates = (
        [a.date]
        if a.date
        else sorted(
            d
            for d in os.listdir(L2)
            if os.path.isdir(os.path.join(L2, d)) and d[:4].isdigit()
        )
    )
    print(f"# L2 打包  {len(dates)} 个日期  → {PACKS}")
    for d in dates:
        pack(d, a.force)
    print("### 现有包:")
    tot = 0
    for fn in sorted(os.listdir(PACKS)):
        p = os.path.join(PACKS, fn)
        sz = os.path.getsize(p)
        tot += sz
        print(
            f"  {fn:<28} {sz / 1e6:8.1f}MB  {time.strftime('%m-%d %H:%M', time.localtime(os.path.getmtime(p)))}"
        )
    print(f"  合计 {tot / 1e6:.1f}MB")


if __name__ == "__main__":
    main()
