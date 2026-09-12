#!/usr/bin/env python3
"""侧别映射独立交叉验证：用**本地已有的官方结算结果**检验 up/down token 有没有配反。

为什么单独一个脚本（2026-09-12）：
  `l2_derive.py --stage verify` 只证明「parquet 与原始帧一致」，**证明不了 up/down 配对没错**
  （两边都用同一份 gamma 映射，错了会一起错）。本脚本换一个**独立的判据**：
  临结算时赢家价格收敛到 ~1、输家到 ~0 ⇒ 用 bar 末段的盘口反推赢家，与 gamma 的
  `outcomePrices`（已收进 `bar_tokens.parquet` 的 `winner`）比对。
  若 token 配反，一致率会掉到 ~50%；配对了则应接近 100%。

口径（写死在这里，避免以后记不清）：
  · bar 起点/周期：**从 `bar_tokens.slug` 取**（`...-5m-<epoch>` / `...-15m-<epoch>`），
    不按 quotes 分区路径推 —— 混市场残留行会落在**别的 bar 的文件**里，按路径推会算出错的窗口
    （试过这个错，制造了 6 条假不一致）
  · 时间窗：bar 结束前 **60s** 内、且 **≥2s** 内（避开结算瞬间）
  · 覆盖门槛：该 bar 在结束时点前 20s 内必须还有帧，否则算「末段无数据」跳过（数据缺口，不是映射问题）
  · 取值：该侧 `best_bid` 的**最后一次非空值**（末段单侧空盘口是常态：mid 为空占 99.4%）
  · 判据：`bid_up > bid_dn` ⇒ 预测 UP；某侧整个窗口都无 bid 记 -1（输家常见）
  · 只比 `winner` 非空（已结算、且 outcomes 为 ["Up","Down"]）的 bar

性质：**只读、离线可重跑**（不联网、不写任何文件）；一致率 < `--min-agree` 或样本不足则非 0 退出。

跑法：
  python -B tools/l2_settle_xcheck.py                 # 全部分区
  python -B tools/l2_settle_xcheck.py --days 2026-09-11
  python -B tools/l2_settle_xcheck.py --sample 300 --min-agree 0.99
"""

from __future__ import annotations

import argparse
import glob
import os
import random
import re

import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
DERIVED = os.path.join(ROOT, "data", "derived")
PAT = re.compile(r"day=([\d-]+)[\\/]coin=(\w+)[\\/]cycle=(\w+)")

WIN_MS = 60_000  # 末段窗口：结束前 60s
SKIP_MS = 2_000  # 距结束 2s 内不看（结算瞬间）
COVER_MS = 20_000  # 结束时点前 20s 内必须还有帧，否则算「末段无数据」跳过
SLUG_RE = re.compile(r"-updown-(5|15)m-(\d+)$")


def bar_window(slug):
    """从 slug 定 bar 起点(ms)与周期(ms)。

    为什么不按 quotes 分区路径推：混市场残留行会落在**别的 bar 的文件**里，
    按路径推会算出错的窗口（实测因此造出 6 条假不一致）。
    """
    m = SLUG_RE.search(str(slug))
    if not m:
        return None
    return int(m.group(2)) * 1000, int(m.group(1)) * 60_000


def last_bid(g, end_ms):
    s = g.dropna(subset=["best_bid"])
    s = s[(s["ts_ms"] <= end_ms - SKIP_MS) & (s["ts_ms"] >= end_ms - WIN_MS)]
    return None if s.empty else float(s["best_bid"].iloc[-1])


def main(argv=None):
    ap = argparse.ArgumentParser(description="用官方结算结果交叉验证 side 映射")
    ap.add_argument("--days", nargs="*", default=None, help="如 2026-09-11（默认全部）")
    ap.add_argument(
        "--sample", type=int, default=0, help="随机抽多少个 market（0=全部）"
    )
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument(
        "--min-agree", type=float, default=0.99, help="一致率下限（默认 0.99）"
    )
    ap.add_argument("--min-n", type=int, default=50, help="样本量下限（默认 50）")
    a = ap.parse_args(argv)

    tp = os.path.join(DERIVED, "bar_tokens.parquet")
    if not os.path.exists(tp):
        print("缺 %s（先跑 tools/l2_derive.py --stage tokens）" % tp)
        return 2
    tok = pd.read_parquet(tp)
    if "winner" not in tok.columns or tok["winner"].notna().sum() == 0:
        print("bar_tokens 里没有 winner 列（旧缓存？跑 --stage tokens 重抓一次）")
        return 2
    tok = tok[
        tok["winner"].notna() & tok["up_token"].notna() & tok["down_token"].notna()
    ]
    tmap = tok.set_index("market")[["winner", "up_token", "down_token", "slug"]]

    globs = []
    for d in a.days or ["2026-*"]:
        globs.append(
            os.path.join(
                DERIVED, "quotes", "day=%s" % d, "coin=*", "cycle=*", "part-0.parquet"
            )
        )
    parts = sorted({p for g in globs for p in glob.glob(g)})
    if not parts:
        print("没有匹配的 quotes 分区：%s" % globs)
        return 2

    frames = []
    for p in parts:
        m = PAT.search(p)
        q = pd.read_parquet(
            p, columns=["market", "asset_id", "ts_ms", "bar_ts", "best_bid"]
        )
        q = q[q["market"].isin(tmap.index)]
        if len(q):
            frames.append((m.group(1), q))
    if not frames:
        print("没有任何 quotes 行能对上 bar_tokens")
        return 2

    allm = sorted(set(pd.concat([f[1]["market"] for f in frames]).unique()))
    pick = set(allm)
    if a.sample and a.sample < len(allm):
        pick = set(random.Random(a.seed).sample(allm, a.sample))
    print(
        "分区 %d 个 | 可校验 market %d 个（本次取 %d）"
        % (len(frames), len(allm), len(pick))
    )

    rows = []
    skipped = {"no_window": 0, "no_cover": 0, "no_bid": 0}
    # 关键：必须**跨分区**按 market 汇总 —— 同一 market 的行会同时出现在自己的文件与相邻 bar 的文件里，
    # 按分区分别 groupby 会只拿到子集（实测因此把一条行情算成「末帧早于 bar 结束 131s」）
    qa = pd.concat([f for _d, f in frames], ignore_index=True)
    qa = qa[qa["market"].isin(pick)]
    for mk, g in qa.groupby("market"):
        if mk not in tmap.index:
            continue
        w = bar_window(tmap.loc[mk, "slug"])
        if w is None:
            skipped["no_window"] += 1
            continue
        bar_ms, cyc_ms = w
        end = bar_ms + cyc_ms
        # 末段覆盖门槛：bar 结束前 20s 内必须还有帧（否则是数据缺口，不是映射问题）
        if g["ts_ms"].max() < end - COVER_MS:
            skipped["no_cover"] += 1
            continue
        bu = last_bid(g[g["asset_id"] == tmap.loc[mk, "up_token"]], end)
        bd = last_bid(g[g["asset_id"] == tmap.loc[mk, "down_token"]], end)
        if bu is None and bd is None:
            skipped["no_bid"] += 1
            continue
        rows.append(
            {
                "day": pd.to_datetime(bar_ms, unit="ms", utc=True).strftime("%Y-%m-%d"),
                "market": mk,
                "winner": tmap.loc[mk, "winner"],
                "bid_up": bu,
                "bid_dn": bd,
            }
        )

    r = pd.DataFrame(rows)
    if r.empty:
        print("没有可比对的 bar")
        return 2
    r["pred"] = [
        "UP" if (x if pd.notna(x) else -1) > (y if pd.notna(y) else -1) else "DOWN"
        for x, y in zip(r["bid_up"], r["bid_dn"])
    ]
    agree = (r["pred"] == r["winner"]).mean()
    bad = r[r["pred"] != r["winner"]]

    print(
        "样本 bar: %d（跳过：末段无数据 %d / 无 bar 窗口 %d / 窗口内两侧都无 bid %d）"
        % (len(r), skipped["no_cover"], skipped["no_window"], skipped["no_bid"])
    )
    print("一致率: %.2f%%（官方 winner vs 末段 bid 判据）" % (agree * 100))
    for lab in ("UP", "DOWN"):
        s = r[r["winner"] == lab]
        if len(s):
            bu, bd = s["bid_up"], s["bid_dn"]
            print(
                "  官方 %-4s n=%3d | bid_up 非空 %3.0f%% p50=%s | bid_dn 非空 %3.0f%% p50=%s"
                % (
                    lab,
                    len(s),
                    bu.notna().mean() * 100,
                    "%.3f" % bu.median() if bu.notna().any() else "  n/a",
                    bd.notna().mean() * 100,
                    "%.3f" % bd.median() if bd.notna().any() else "  n/a",
                )
            )
    if len(bad):
        print("不一致 %d 条（最多列 10 条）：" % len(bad))
        print(bad.head(10).to_string())

    ok = len(r) >= a.min_n and agree >= a.min_agree
    print(
        "\n判定：%s（要求 n>=%d 且一致率>=%.2f%%）"
        % ("通过" if ok else "不通过", a.min_n, a.min_agree * 100)
    )
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
