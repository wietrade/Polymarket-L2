#!/usr/bin/env python3
"""l2_depth_audit.py — 审计原始 L2 包里的「深度」到底有多全（全量扫描）。

为什么需要它
-----------
2026-09-14 用户问「原始数据有全量的深度数据吗」。此前只有一个副作用结论：
派生表 `quotes` **只留顶档**（`docs/派生表-口径.md` §4.5 写的是「本层不保留全档，
避免体积膨胀数倍」）。但「原始层到底有多全」从没被量化过 —— 而它决定了两件事：

1. 要不要补深度派生（若原始不完整，补了也没用）；
2. 纸面账本「顶档价成交」的假设是否成立（实测 $100 单 @0.50 有 62.5% 的帧顶档吃不下
   ⇒ 需要更深档位才能算真实成交均价）。

本工具只读原始 gz，不写任何东西（除了 `--out` 指定的 JSON）。

两个维度分开看（别混）
--------------------
- **档位维度**（本工具主产出）：单帧快照里有多少档、覆盖到什么价位、有没有截断。
- **时间维度**（本工具副产出）：帧间隔分布。⚠️ 采集端只保留 `book` 与 `last_trade_price`，
  **丢弃了 `price_change` / `tick_size_change`** ⇒ 帧与帧之间的档位级变化不可重建，
  这是数据边界（`event_type` 计数可自证：price_change 应为 0）。

判据（怎么算「全」）
------------------
- **不截断**：档数最大值应达 **99**（0.01~0.99 完整 tick 网格）。若 max 卡在 20/50，
  就是服务端或采集端截断；直方图在整值处堆叠是截断特征。
- **覆盖到底部**：`bids` 是否含 0.01、`asks` 是否含 0.99（两侧极端价有挂单时应有）。
- **步长**：正常 0.01；出现更细步长（0.001）说明该 bar 处于 tick_size 切换期。

用法
----
    python -B tools/l2_depth_audit.py                      # 全量
    python -B tools/l2_depth_audit.py --date 2026-09-13
    python -B tools/l2_depth_audit.py --jobs 6 --out /tmp/depth_audit.json

⚠️ 全量 4219 文件 / 716 MB(gz)，单进程约 10 分钟以上 ⇒ 默认并发 6。按
`/memories/repo/heavy-run-discipline.md`：先估体量、大输出落盘、长任务别挂到超时。
"""

from __future__ import annotations

import argparse
import glob
import gzip
import json
import os
import re
import sys
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_ROOT = os.path.join(os.path.dirname(HERE), "data")

NAME_RE = re.compile(
    r"^(?P<coin>[a-z]+)-updown-(?P<cycle>\d+m)-(?P<epoch>\d+)\.jsonl\.gz$"
)
# 轻量提取 event_type（比 json.loads 每行快得多；只对 book 行做完整解析）
EV_RE = re.compile(r'"event_type"\s*:\s*"([A-Za-z_]+)"')

# 帧间隔直方图桶（秒）：覆盖 p50~98ms、p90~706ms、长尾到 60s+
IV_EDGES = [0.0, 0.05, 0.1, 0.2, 0.5, 1.0, 2.0, 5.0, 10.0, 60.0, float("inf")]


def _iv_bucket(dt: float) -> str:
    for i in range(len(IV_EDGES) - 1):
        if IV_EDGES[i] <= dt < IV_EDGES[i + 1]:
            return f"{IV_EDGES[i]:g}~{IV_EDGES[i+1]:g}"
    return ">60"


def scan_file(path: str) -> dict:
    """扫单个 gz，返回可相加的聚合（Counter / 标量）。"""
    st = {
        "files": 1,
        "files_unreadable": 0,
        "lines": 0,
        "bad_json": 0,
        "ev": Counter(),
        "bid_n_hist": Counter(),
        "ask_n_hist": Counter(),
        "bid_min_px": Counter(),   # 买侧最低价（截断时这个会明显抬高）
        "ask_max_px": Counter(),   # 卖侧最高价
        "bid_has_001": 0,
        "ask_has_099": 0,
        "bid_side_empty": 0,
        "ask_side_empty": 0,
        "book_frames": 0,
        "nonstd_step_bid": 0,
        "nonstd_step_ask": 0,
        "gap_frames": 0,           # 一侧内部有 >1 tick 空洞的帧数
        "iv": Counter(),
        "ts_ms_min": None,
        "ts_ms_max": None,
    }
    last_ts: dict[str, int] = {}
    try:
        with gzip.open(path, "rt", encoding="utf-8") as fh:
            for line in fh:
                if '"book"' not in line:
                    m = EV_RE.search(line)
                    if m:
                        st["ev"][m.group(1)] += 1
                    st["lines"] += 1
                    continue
                try:
                    o = json.loads(line)
                except Exception:
                    st["bad_json"] += 1
                    st["lines"] += 1
                    continue
                st["lines"] += 1
                ev = o.get("event_type")
                st["ev"][ev or "?"] += 1
                if ev != "book":
                    continue
                st["book_frames"] += 1

                ts = o.get("timestamp")
                try:
                    ts = int(ts)
                except Exception:
                    ts = None
                if ts is not None:
                    if st["ts_ms_min"] is None or ts < st["ts_ms_min"]:
                        st["ts_ms_min"] = ts
                    if st["ts_ms_max"] is None or ts > st["ts_ms_max"]:
                        st["ts_ms_max"] = ts
                    aid = o.get("asset_id") or "?"
                    prev = last_ts.get(aid)
                    if prev is not None and ts >= prev:
                        st["iv"][_iv_bucket((ts - prev) / 1000.0)] += 1
                    last_ts[aid] = ts

                def _side(arr, is_bid):
                    px = []
                    for x in arr or []:
                        try:
                            px.append(round(float(x.get("price")), 4))
                        except Exception:
                            pass
                    return sorted(px) if is_bid else sorted(px, reverse=True)

                bids = _side(o.get("bids"), True)    # 升序
                asks = _side(o.get("asks"), False)   # 降序
                st["bid_n_hist"][len(bids)] += 1
                st["ask_n_hist"][len(asks)] += 1
                if not bids:
                    st["bid_side_empty"] += 1
                else:
                    st["bid_min_px"][bids[0]] += 1
                    if bids[-1] - bids[0] > 0:
                        steps = {round(bids[i + 1] - bids[i], 4) for i in range(len(bids) - 1)}
                        if steps - {0.01}:
                            st["nonstd_step_bid"] += 1
                        if any(s > 0.0101 for s in steps):
                            st["gap_frames"] += 1
                    if bids[0] <= 0.0101:
                        st["bid_has_001"] += 1
                if not asks:
                    st["ask_side_empty"] += 1
                else:
                    st["ask_max_px"][asks[0]] += 1
                    if asks[0] - asks[-1] > 0:
                        steps = {round(asks[i] - asks[i + 1], 4) for i in range(len(asks) - 1)}
                        if steps - {0.01}:
                            st["nonstd_step_ask"] += 1
                    if asks[0] >= 0.9899:
                        st["ask_has_099"] += 1
    except Exception:
        st["files_unreadable"] += 1
    return st


def merge(a: dict, b: dict) -> dict:
    out = {}
    for k, v in a.items():
        if isinstance(v, Counter):
            out[k] = v + b.get(k, Counter())
        elif k == "ts_ms_min":
            xs = [x for x in (a.get(k), b.get(k)) if x is not None]
            out[k] = min(xs) if xs else None
        elif k == "ts_ms_max":
            xs = [x for x in (a.get(k), b.get(k)) if x is not None]
            out[k] = max(xs) if xs else None
        else:
            out[k] = v + b.get(k, 0)
    return out


def pct(hist: Counter, p: float) -> float:
    tot = sum(hist.values())
    if not tot:
        return float("nan")
    acc = 0
    for k in sorted(hist):
        acc += hist[k]
        if acc >= tot * p:
            return float(k)
    return float(max(hist))


def report(agg: dict) -> str:
    L = []
    L.append(f"文件 {agg['files']}（读失败 {agg['files_unreadable']}）｜行 {agg['lines']:,}"
             f"｜book 帧 {agg['book_frames']:,}｜bad_json {agg['bad_json']}")
    L.append(f"event_type 计数: {dict(agg['ev'].most_common())}")
    L.append("")
    bh, ah = agg["bid_n_hist"], agg["ask_n_hist"]
    L.append("档位数分布（截断的话 max 会卡在 20/50 这类整值）")
    L.append(f"  买侧: p10={pct(bh,.10):.0f} p50={pct(bh,.50):.0f} p90={pct(bh,.90):.0f} "
             f"p99={pct(bh,.99):.0f} max={max(bh) if bh else 0}")
    L.append(f"  卖侧: p10={pct(ah,.10):.0f} p50={pct(ah,.50):.0f} p90={pct(ah,.90):.0f} "
             f"p99={pct(ah,.99):.0f} max={max(ah) if ah else 0}")
    top = sorted(bh.items(), key=lambda kv: -kv[1])[:6]
    L.append(f"  买侧最常见档数: {top}")
    tot = sum(bh.values()) or 1
    L.append(f"  档数 ≥50 的帧占比: {sum(v for k,v in bh.items() if k>=50)/tot*100:.1f}%"
             f"｜≥90: {sum(v for k,v in bh.items() if k>=90)/tot*100:.1f}%")
    L.append("")
    L.append("覆盖边界")
    L.append(f"  单侧空盘口: 买 {agg['bid_side_empty']:,} 帧 / 卖 {agg['ask_side_empty']:,} 帧")
    L.append(f"  买侧含 0.01: {agg['bid_has_001']:,} 帧｜卖侧含 0.99: {agg['ask_has_099']:,} 帧")
    bmin = agg["bid_min_px"].most_common(5)
    amax = agg["ask_max_px"].most_common(5)
    L.append(f"  买侧最低价 top: {bmin}")
    L.append(f"  卖侧最高价 top: {amax}")
    L.append(f"  非 0.01 步长帧: 买 {agg['nonstd_step_bid']:,} / 卖 {agg['nonstd_step_ask']:,}"
             f"｜档位间有空洞的帧: {agg['gap_frames']:,}")
    L.append("")
    iv = agg["iv"]
    tot_iv = sum(iv.values()) or 1
    L.append("帧间隔（同一 asset 相邻 book 帧）")
    for e in IV_EDGES[:-1]:
        k = f"{e:g}~{IV_EDGES[IV_EDGES.index(e)+1]:g}"
        v = iv.get(k, 0)
        L.append(f"  {k:>12}s : {v:>12,} ({v/tot_iv*100:5.1f}%)")
    L.append(f"  {'>60':>12}s : {iv.get('>60',0):>12,} ({iv.get('>60',0)/tot_iv*100:5.1f}%)")
    if agg.get("ts_ms_min"):
        L.append(f"  覆盖时间: {agg['ts_ms_min']} ~ {agg['ts_ms_max']} (ms epoch)")
    return "\n".join(L)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="审计原始 L2 包的深度完整性（只读全量扫描）")
    ap.add_argument("--root", default=DEFAULT_ROOT, help="原始数据根目录（默认 ../data）")
    ap.add_argument("--date", action="append", help="只扫指定 UTC 日期，可重复")
    ap.add_argument("--coin", choices=["btc", "eth"], help="只扫某币")
    ap.add_argument("--cycle", choices=["5m", "15m"], help="只扫某周期")
    ap.add_argument("--jobs", type=int, default=6, help="并发进程数（默认 6）")
    ap.add_argument("--out", default=None, help="聚合结果落盘 JSON 路径")
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args(argv)

    dates = args.date or sorted(
        os.path.basename(p) for p in glob.glob(os.path.join(args.root, "2026-*"))
        if os.path.isdir(p)
    )
    files = []
    for d in dates:
        for p in sorted(glob.glob(os.path.join(args.root, d, "*.jsonl.gz"))):
            m = NAME_RE.match(os.path.basename(p))
            if not m:
                continue
            if args.coin and m.group("coin") != args.coin:
                continue
            if args.cycle and m.group("cycle") != args.cycle:
                continue
            files.append(p)
    if not files:
        print("没有匹配的文件", file=sys.stderr)
        return 1
    if not args.quiet:
        print(f"扫描 {len(files)} 个文件，日期 {dates}，并发 {args.jobs}…", file=sys.stderr)

    agg = None
    done = 0
    with ProcessPoolExecutor(max_workers=max(1, args.jobs)) as ex:
        futs = [ex.submit(scan_file, p) for p in files]
        for fu in as_completed(futs):
            r = fu.result()
            agg = r if agg is None else merge(agg, r)
            done += 1
            if not args.quiet and done % 200 == 0:
                print(f"  …{done}/{len(files)}", file=sys.stderr)

    print(report(agg))
    if args.out:
        out = {"files": len(files), "dates": dates,
               "agg": {k: (dict(v) if isinstance(v, Counter) else v) for k, v in agg.items()}}
        with open(args.out, "w", encoding="utf-8") as fh:
            json.dump(out, fh, ensure_ascii=False, indent=1)
        if not args.quiet:
            print(f"\n聚合已落盘: {args.out}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
