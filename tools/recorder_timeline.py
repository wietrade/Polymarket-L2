#!/usr/bin/env python3
"""recorder_timeline.py — 把 recorder 日志里的换市场时序拆成「谁慢了」四个环节。

为什么需要它
-----------
2026-09-11 E9：修了 `RECV_TIMEOUT` 后，仍有约 19~27% 的 bar 首帧拖到 **bar+12.8s**
（且数值异常集中在 12.8/12.8/12.9s，像离散机制而非渐变延迟）。
Hub 8867 对同一个市场的首快照在 bar+2~5s 就到 ⇒ 市场数据那时已存在，迟到在我们这边。
但原日志**没有任何时间戳**，无法区分：(a) 我们订阅发晚了；(b) 订阅按时发了、服务端才回帧。

所以 2026-09-11 给 recorder 加了 UTC 时间戳与两条打点（`已发订阅` / `首帧(发订阅后 Xms)`），
本工具把日志读成一条**按 bar 的时序链**：

    bar 起点 ──► 检测到换 bar ──► gamma 取到 token ──► 发出订阅帧 ──► 收到首帧
                 (ticker 轮询)      (t_就绪)           (t_订阅)        (t_首帧)

四段耗时分别对应四个不同环节，谁慢一目了然：
    · 检测延迟  = t_检测 − bar 起点        → TICK_POLL_S / 事件循环忙
    · gamma     = t_就绪 − t_检测          → gamma REST（实测通常 <100ms）
    · 重建+握手 = t_订阅 − t_就绪          → ROTATE_WAIT_S(1s) + TLS 握手 + 等 sub_gen 检测
    · 服务端    = t_首帧 − t_订阅          → 服务端回第一帧（实测 230~250ms）

用法（在 43 上跑，日志在 /tmp）
    python tools/recorder_timeline.py                       # 最近 40 个 bar
    python tools/recorder_timeline.py --slow-only --slow-s 10
    python tools/recorder_timeline.py --log /tmp/recorder_l2_v3.log --lines 200000

⚠️ 日志里的时间是 **UTC**（`ts()` 用 gmtime），可直接与 slug 尾部的 bar 起点 epoch 对齐。
"""

from __future__ import annotations

import argparse
import collections
import re

# 例: "  [btc-5m] 22:24:59.123 bar→ btc-updown-5m-1789079100 (旧 ...)"
RE_BAR = re.compile(
    r"\[(?P<line>[a-z]+-\d+m)\]\s+(?P<t>\d\d:\d\d:\d\d\.\d\d\d)\s+bar→\s+(?P<slug>\S+)"
)
RE_TOK = re.compile(
    r"\[(?P<line>[a-z]+-\d+m)\]\s+(?P<t>\d\d:\d\d:\d\d\.\d\d\d)\s+新市场\s+(?P<slug>\S+)\s+token"
)
RE_SUB = re.compile(
    r"\[(?P<line>[a-z]+-\d+m)\]\[c(?P<conn>\d)\]\s+(?P<t>\d\d:\d\d:\d\d\.\d\d\d)\s+已发订阅\s+(?P<slug>\S+)"
)
RE_FIRST = re.compile(
    r"\[(?P<line>[a-z]+-\d+m)\]\[c(?P<conn>\d)\]\s+(?P<t>\d\d:\d\d:\d\d\.\d\d\d)\s+首帧\s+"
    r"(?P<slug>\S+)\s+\(发订阅后\s+(?P<ms>\d+)ms\)"
)


def secs(hhmmss: str) -> float:
    """'HH:MM:SS.mmm' → 当日秒数（跨零点会变成负数，够用；本工具只看相对差）。"""
    h, m, s = hhmmss.split(":")
    return int(h) * 3600 + int(m) * 60 + float(s)


def main() -> None:
    ap = argparse.ArgumentParser(description="拆 recorder 换市场时序")
    ap.add_argument("--log", default="/tmp/recorder_l2_v3.log")
    ap.add_argument("--bars", type=int, default=40, help="只报告最近 N 个 bar")
    ap.add_argument(
        "--slow-only", action="store_true", help="只列首帧偏移 > --slow-s 的"
    )
    ap.add_argument("--slow-s", type=float, default=10.0)
    a = ap.parse_args()

    ev = collections.defaultdict(lambda: collections.defaultdict(dict))
    with open(a.log, errors="replace") as fh:
        for line in fh:
            m = RE_BAR.search(line)
            if m:
                ev[(m["line"], m["slug"])]["t_detect"] = secs(m["t"])
                continue
            m = RE_TOK.search(line)
            if m:
                ev[(m["line"], m["slug"])]["t_tok"] = secs(m["t"])
                continue
            m = RE_SUB.search(line)
            if m:
                ev[(m["line"], m["slug"])][f"t_sub{m['conn']}"] = secs(m["t"])
                continue
            m = RE_FIRST.search(line)
            if m:
                ev[(m["line"], m["slug"])][f"t_first{m['conn']}"] = secs(m["t"])
                ev[(m["line"], m["slug"])][f"srv{m['conn']}"] = int(m["ms"])
                continue

    rows = []
    for (line, slug), d in ev.items():
        try:
            bar = int(slug.rsplit("-", 1)[-1])
        except ValueError:
            continue
        d["line"], d["slug"], d["bar"] = line, slug, bar
        d["bar_start_sod"] = bar % 86400  # 日志时间戳是 UTC ⇒ 用 UTC 的当日秒
        rows.append(d)
    rows.sort(key=lambda r: (r["bar"], r["line"]))
    rows = rows[-a.bars :]

    def off(r, key):
        v = r.get(key)
        return (v - r["bar_start_sod"]) if v is not None else None

    print(f"日志 {a.log}   解析出 {len(ev)} 条 (bar,线)  报最近 {len(rows)} 条")
    print(
        f"  {'线':<9}{'bar+检测':>9}{'+gamma':>8}{'+重建握手':>11}{'+服务端':>9}"
        f"{'=首帧':>8}   {'备注':<22}"
    )
    slow = 0
    for r in rows:
        firsts = [off(r, "t_first0"), off(r, "t_first1")]
        firsts = [x for x in firsts if x is not None]
        if not firsts:
            continue
        t_first = min(firsts)
        if a.slow_only and t_first <= a.slow_s:
            continue
        slow += 1 if t_first > a.slow_s else 0
        t_det, t_tok = off(r, "t_detect"), off(r, "t_tok")
        # 启动时 ticker 首次发现当前 bar 也会打 `bar→`（旧 None）⇒ 那不是"换 bar 事件"，
        # 检测偏移会落在 bar 中点，必须剔除（否则会看到 bar+391s 这种假慢档）。
        if t_det is None or t_det > 30:
            continue
        subs = [off(r, "t_sub0"), off(r, "t_sub1")]
        subs = [x for x in subs if x is not None]
        t_sub = min(subs) if subs else None
        g = (t_tok - t_det) if (t_det is not None and t_tok is not None) else None
        h = (t_sub - t_tok) if (t_tok is not None and t_sub is not None) else None
        s = (t_first - t_sub) if t_sub is not None else None
        flag = ""
        if t_first > a.slow_s:
            if t_det is not None and t_det > 5:
                flag = "← 检测就晚了"
            elif h is not None and h > 3:
                flag = "← 重建/握手慢"
            elif s is not None and s > 2:
                flag = "← 服务端回帧慢"
            else:
                flag = "← 待细分"
        print(
            f"  {r['line']:<9}{t_det if t_det is not None else -1:>9.1f}"
            f"{g if g is not None else -1:>8.2f}"
            f"{h if h is not None else -1:>11.2f}"
            f"{s if s is not None else -1:>9.2f}"
            f"{t_first:>8.1f}   {flag:<22}"
        )
    print("\n  若 --slow-only 为空，说明这段时间没有慢档；有的话看「备注」指向哪一段。")


if __name__ == "__main__":
    main()
