#!/usr/bin/env python3
"""l2_bar_coverage.py — 统计 recorder 每条线「每个 bar 实际覆盖了第几秒到第几秒」。

为什么需要它
-----------
2026-09-10 之前 recorder 有缺陷：**切 bar 后仍在旧连接上发订阅帧 → 不生效**
（连接活着、收 PONG，但新市场零消息），只能等服务器 ~2-4min 一次的例行断连才恢复
⇒ 每 bar 前 ~120s 无数据，只录到末 ~180s。09-10 16:51 用「订阅代 `sub_gen`」修复。

影响面很大：submin / Leader5m 的决策在 bar+10~60s，恰好落在那段空窗里 ⇒ 修前所有
基于 L2 的结论口径都不合用。所以「这批数据能用在 bar 的第几秒之后」必须可量化。

失效是**概率性**的（取决于服务器何时断连恰好落在切 bar 附近）：单个文件看着健康不能
说明没问题，必须看统计分布 —— 这也是本工具存在的理由。

两个坑（都踩过）
---------------
1. **文件名 ≠ 市场**：单个 `<slug>.jsonl.gz` 里可能混入相邻轮次/别的市场的行。
   因此一律按**每行自己的 `market` 字段**归因，取行数最多的那个市场当「本文件的主市场」，
   不看文件头几行（常是上一个市场的残留）。
2. **正在写入的文件**读出来像损坏（garbage / invalid block）→ 当天数据请等 bar 关闭后再测。

用法
----
    python tools/l2_bar_coverage.py --date 2026-09-10
    python tools/l2_bar_coverage.py --date 2026-09-10 --by-hour      # 逐小时（看修复的时刻台阶）
    python tools/l2_bar_coverage.py --date 2026-09-09 --date 2026-09-10 --coin btc --cycle 5m
    python tools/l2_bar_coverage.py --date 2026-09-10 --detail       # 逐 bar 明细

输出字段
-------
  bars     本组参与统计的文件数（无 book 行的文件单列）
  f_p50/p90   首个 book 帧相对 bar 起点的偏移（秒）—— **越小越好**；修复前中位数约 120s
  l_p50    最后一个 book 帧相对 bar 起点的偏移（秒）
  cov_p50  覆盖秒数 = l_p50 - f_p50
  ok%      首帧偏移 ≤ 10s 的比例（健康线应接近 100%）
"""

from __future__ import annotations

import argparse
import glob
import gzip
import json
import os
import re
import sys
from collections import defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_ROOT = os.path.join(os.path.dirname(HERE), "data")

# 文件名: <coin>-updown-<cycle>-<bar_epoch>.jsonl.gz
NAME_RE = re.compile(
    r"^(?P<coin>[a-z]+)-updown-(?P<cycle>\d+m)-(?P<epoch>\d+)\.jsonl\.gz$"
)
CYCLE_SECONDS = {"1m": 60, "5m": 300, "15m": 900, "1h": 3600}


def _pct(xs: list[float], q: float) -> float:
    """简单分位（最近秩法，够用且无依赖）。"""
    if not xs:
        return float("nan")
    s = sorted(xs)
    k = min(len(s) - 1, max(0, int(round(q * (len(s) - 1)))))
    return s[k]


def scan_file(path: str) -> dict | None:
    """扫一个 bar 文件 → 主市场首帧/末帧偏移。返回 None = 无可用 book 行。"""
    m = NAME_RE.match(os.path.basename(path))
    if not m:
        return None
    epoch = int(m.group("epoch"))
    cycle_s = CYCLE_SECONDS.get(m.group("cycle"), 300)

    # market -> [book 行数, 首帧 ms, 末帧 ms, 全部行数]
    stats: dict[str, list] = defaultdict(lambda: [0, None, None, 0])
    try:
        with gzip.open(path, "rt", encoding="utf-8", errors="replace") as fh:
            for line in fh:
                if not line or line[0] != "{":
                    continue
                try:
                    d = json.loads(line)
                except Exception:
                    continue
                mk = d.get("market") or "?"
                s = stats[mk]
                s[3] += 1
                if d.get("event_type") != "book":
                    continue
                ts = d.get("timestamp")
                if not ts:
                    continue
                ts = float(ts) / 1000.0
                s[0] += 1
                s[1] = ts if s[1] is None else min(s[1], ts)
                s[2] = ts if s[2] is None else max(s[2], ts)
    except (OSError, EOFError) as e:
        return {"error": f"{type(e).__name__}: {e}", "epoch": epoch, "cycle_s": cycle_s}

    books = {k: v for k, v in stats.items() if v[0] > 0}
    if not books:
        return {"empty": True, "epoch": epoch, "cycle_s": cycle_s}

    dom = max(books.items(), key=lambda kv: kv[1][0])
    mk, (nbook, t_first, t_last, nall) = dom
    return {
        "epoch": epoch,
        "cycle_s": cycle_s,
        "market": mk,
        "markets_seen": len(stats),
        "book_rows": nbook,
        "nall": nall,
        "first": t_first - epoch,
        "last": t_last - epoch,
    }


def group_and_report(files: list[str], by_hour: bool, detail: bool) -> None:
    rows = []
    bad = []
    empty = []
    for p in files:
        r = scan_file(p)
        if r is None:
            continue
        if "error" in r:
            bad.append((os.path.basename(p), r["error"]))
        elif r.get("empty"):
            empty.append(os.path.basename(p))
        else:
            r["name"] = os.path.basename(p)
            rows.append(r)

    if not rows:
        print("  没有可统计的 bar（文件都为空或读取失败）")
        return

    # 分组键: (日期目录, coin, cycle)
    by_name = {os.path.basename(p): p for p in files}
    groups: dict[tuple, list[dict]] = defaultdict(list)
    for r in rows:
        d = os.path.basename(os.path.dirname(by_name[r["name"]]))
        m = NAME_RE.match(r["name"])
        groups[(d, m.group("coin"), m.group("cycle"))].append(r)

    for (date, coin, cycle), rs in sorted(groups.items()):
        rs.sort(key=lambda r: r["epoch"])
        firsts = [r["first"] for r in rs]
        lasts = [r["last"] for r in rs]
        covs = [r["last"] - r["first"] for r in rs]
        ok = sum(1 for f in firsts if f <= 10.0) / len(firsts) * 100
        print(
            f"\n{date}  {coin} {cycle}  bars={len(rs)}  "
            f"(主市场 book 行中位数 {int(_pct([r['book_rows'] for r in rs], 0.5)):,})"
        )
        print(
            f"  {'时刻':<10} {'bars':>5} {'首帧p50':>8} {'p90':>7} {'末帧p50':>8} "
            f"{'覆盖p50':>8} {'≤10s':>7}"
        )

        def line(label: str, sub: list[dict]) -> None:
            if not sub:
                return
            f = [r["first"] for r in sub]
            l = [r["last"] for r in sub]
            print(
                f"  {label:<10} {len(sub):>5} {_pct(f, 0.5):>8.1f} {_pct(f, 0.9):>7.1f} "
                f"{_pct(l, 0.5):>8.1f} {_pct([b - a for a, b in zip(f, l)], 0.5):>8.1f} "
                f"{sum(1 for x in f if x <= 10.0) / len(f) * 100:>6.1f}%"
            )

        if by_hour:
            buckets: dict[int, list[dict]] = defaultdict(list)
            for r in rs:
                buckets[r["epoch"] // 3600 % 24].append(r)
            for h in sorted(buckets):
                line(f"{h:02d}:00Z", buckets[h])
        line("全部", rs)

        # 与 bar 长度的关系：首帧偏移 > 90s 的占比（修复前典型形态）
        late = [r for r in rs if r["first"] > 90.0]
        if late:
            print(
                f"  首帧 >90s 的 bar: {len(late)}/{len(rs)} = {len(late) / len(rs) * 100:.1f}%"
                f"  （最晚 {max(r['first'] for r in late):.1f}s）"
            )
        if detail:
            print("  逐 bar:")
            for r in rs:
                print(
                    f"    {r['name']:<44} first={r['first']:>7.1f}s last={r['last']:>7.1f}s "
                    f"cov={r['last'] - r['first']:>6.1f}s book={r['book_rows']:>7,} "
                    f"markets={r['markets_seen']}"
                )

    if empty:
        print(f"\n  无 book 行的文件 {len(empty)} 个（前 5）: {', '.join(empty[:5])}")
    if bad:
        print(f"\n  读取失败 {len(bad)} 个（正在写入的文件会这样，等 bar 关闭后再测）:")
        for n, e in bad[:5]:
            print(f"    {n}: {e}")


def main() -> None:
    ap = argparse.ArgumentParser(description="统计 L2 每 bar 的首帧/末帧偏移与覆盖率")
    ap.add_argument(
        "--dir", default=DEFAULT_ROOT, help=f"数据根目录（默认 {DEFAULT_ROOT}）"
    )
    ap.add_argument(
        "--date",
        action="append",
        required=True,
        help="日期目录名，如 2026-09-10；可重复传多次",
    )
    ap.add_argument("--coin", action="append", help="只测某个币（btc/eth），可重复")
    ap.add_argument("--cycle", action="append", help="只测某个周期（5m/15m），可重复")
    ap.add_argument("--by-hour", action="store_true", help="再按 UTC 小时分组打印")
    ap.add_argument("--detail", action="store_true", help="打印逐 bar 明细")
    a = ap.parse_args()

    files: list[str] = []
    for d in a.date:
        pat = os.path.join(a.dir, d, "*.jsonl.gz")
        found = sorted(glob.glob(pat))
        if not found:
            print(f"  ({d} 下没有 jsonl.gz)", file=sys.stderr)
        files.extend(found)
    if a.coin:
        files = [p for p in files if os.path.basename(p).split("-")[0] in set(a.coin)]
    if a.cycle:
        want = set(a.cycle)
        files = [p for p in files if any(f"-{c}-" in os.path.basename(p) for c in want)]

    print(f"数据根目录: {a.dir}")
    print(f"待统计文件: {len(files)} 个")
    if not files:
        return
    group_and_report(files, a.by_hour, a.detail)


if __name__ == "__main__":
    main()
