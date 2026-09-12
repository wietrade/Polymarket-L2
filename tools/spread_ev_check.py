"""spread_ev_check —— **价差 → 结果** 的独立验证（只读派生层；updown-live 因子① 的外部复核）。

为什么有它（2026-09-12）：
  updown-live 在自己的**纸面账本**上发现因子①：`spread` 越宽，胜率与 EV/单越低
  （≤1 tick 胜率 73.1% / EV +4.46 ｜ ≥5 tick 胜率 50% / EV −29.26；见
  `updown-live/docs/因子挖掘与今日归因-20260912.md`）。那条证据是**我们的成交样本**（snapshot ask 口径）。
  本脚本用**完全独立的 L2 数据**（3355 个市场的原始盘口快照）复核同一件事：
  **"买领跑方、持到结算"**这一结构，在宽价差时是否确实更差。

口径（先读再信；派生层的硬规矩见 `docs/派生表-口径.md`）
  · **必须 `is_primary=1`** —— 原始文件里混着相邻轮次的残留市场，不过滤会张冠李戴。
  · 入场时点 = `bar_ts + --offset`（默认 60s）：取该 token **最接近但不超过**该时刻的快照。
  · **领跑方** = 该时刻 `mid` 较高的一侧（两侧 mid 都必须非空，否则剔除该市场）。
  · 成交价 = 领跑方当时的 `best_ask`（吃卖一）；结算 = `bar_tokens.winner` 对应 token。
    ROI = (1−ask)/ask（赢）/ −1（输）——**不含手续费**（口径纯净；要不要含费由使用方另算 0.07×(1−p)）。
  · 输出按 `spread` 分档：n、胜率、均价、ROI/单；并给出"spread 上限"式的逐档累积（≤T）。
  · 只读：不写文件；内存按一个分区文件界（逐日逐币逐周期）。
  · 自检：`--selftest`（合成夹具，手算核对；不碰真实数据）。

用法：
  python tools/spread_ev_check.py --derived data/derived --coins btc,eth --cycles 5m --offset 60
  python tools/spread_ev_check.py --selftest
"""

from __future__ import annotations

import argparse
import glob
import os
import sys

import polars as pl

BUCKETS = [(0.0, 0.011), (0.011, 0.021), (0.021, 0.031), (0.031, 0.051), (0.051, 9.9)]
CUM = [0.011, 0.021, 0.031, 0.051, 9.9]


def _norm() -> None:
    for s in (sys.stdout, sys.stderr):
        try:
            s.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass


def _one_partition(qpath: str, bt: pl.DataFrame, offset: int) -> pl.DataFrame:
    """一个 (day,coin,cycle) 分区 → 每市场一行：领跑方 spread/ask/mid + 是否赢。"""
    q = (
        pl.scan_parquet(qpath)
        .filter(pl.col("is_primary") == 1)               # 硬口径：滤掉相邻轮次残留
        .filter(pl.col("mid").is_not_null() & pl.col("spread").is_not_null())
        .select(["market", "asset_id", "ts_ms", "bar_ts", "best_ask", "mid", "spread", "n_bids", "n_asks"])
    )
    # 目标时刻 = bar_ts + offset（秒）⇒ 取"不超过该时刻"的最后一帧
    q = q.with_columns(((pl.col("bar_ts") + offset) * 1000).alias("t_target"))
    q = q.filter(pl.col("ts_ms") <= pl.col("t_target"))
    q = q.sort(["market", "asset_id", "ts_ms"]).group_by(["market", "asset_id"]).last()
    # 每个市场取两侧，mid 高者为领跑方
    w = (
        q.group_by("market")
        .agg([
            pl.col("asset_id").get(pl.col("mid").arg_max()).alias("lead_asset"),
            pl.col("mid").max().alias("lead_mid"),
            pl.col("mid").min().alias("other_mid"),
            pl.col("spread").get(pl.col("mid").arg_max()).alias("lead_spread"),
            pl.col("best_ask").get(pl.col("mid").arg_max()).alias("lead_ask"),
            pl.len().alias("n_sides"),
        ])
        .filter(pl.col("n_sides") == 2)
    )
    # ⚠️ w 是 LazyFrame、bt 是 DataFrame ⇒ 必须 .lazy() 才能 join（自检走的是 DataFrame 版
    #   同逻辑函数，没覆盖这条管线 ⇒ 首次真实运行才暴露；记在提交信息里）
    w = w.join(bt.lazy().select(["market", "up_token", "down_token", "winner"]), on="market", how="inner")
    # winner 口径：bar_tokens 的 winner 是 token id 还是 'Up'/'Down'？兼容两种
    won = (
        pl.when(pl.col("winner").cast(pl.Utf8) == pl.col("lead_asset"))
        .then(1)
        .otherwise(
            pl.when(pl.col("winner").cast(pl.Utf8).str.to_lowercase().is_in(["up", "1"]))
            .then((pl.col("lead_asset") == pl.col("up_token")).cast(pl.Int8))
            .otherwise((pl.col("lead_asset") == pl.col("down_token")).cast(pl.Int8))
        )
        .alias("won")
    )
    w = w.with_columns(won)
    w = w.with_columns(
        (
            pl.when(pl.col("won") == 1)
            .then((1.0 - pl.col("lead_ask")) / pl.col("lead_ask"))
            .otherwise(-1.0)
        ).alias("roi")
    )
    return w.collect()


def _report(df: pl.DataFrame, label: str) -> None:
    n = df.height
    if not n:
        print("== %s ==  无样本" % label)
        return
    print("== %s ==  样本 n=%d 市场" % (label, n))
    print("   %-14s %7s %8s %8s %10s %10s" % ("spread 区间", "n", "胜率%", "均价ask", "ROI/单", "ROI/单(含费)"))
    for lo, hi in BUCKETS:
        sub = df.filter((pl.col("lead_spread") >= lo) & (pl.col("lead_spread") < hi))
        if not sub.height:
            print("   %-14s %7d" % ("[%.3f,%.3f)" % (lo, hi), 0))
            continue
        w = sub["won"].sum()
        roi = sub["roi"].mean()
        roi_fee = (sub["roi"] - 0.07 * (1.0 - sub["lead_ask"]) / sub["lead_ask"]).mean()
        print("   %-14s %7d %7.1f%% %8.3f %+10.4f %+10.4f"
              % ("[%.3f,%.3f)" % (lo, hi), sub.height, w / sub.height * 100,
                 sub["lead_ask"].mean(), roi, roi_fee))
    print("   ── 累积（spread ≤ T）──")
    for T in CUM:
        sub = df.filter(pl.col("lead_spread") <= T)
        if not sub.height:
            continue
        w = sub["won"].sum()
        roi_fee = (sub["roi"] - 0.07 * (1.0 - sub["lead_ask"]) / sub["lead_ask"]).mean()
        print("   ≤%-13s %7d %7.1f%% %8.3f %+10.4f %+10.4f"
              % ("%.3f" % T, sub.height, w / sub.height * 100, sub["lead_ask"].mean(),
                 sub["roi"].mean(), roi_fee))
    print()


def _one_partition_q(q: pl.DataFrame, bt: pl.DataFrame, offset: int) -> pl.DataFrame:
    """与 `_one_partition` 同逻辑，但吃 DataFrame（供自检）—— 避免为自检造 parquet。"""
    q = q.filter((pl.col("is_primary") == 1) & pl.col("mid").is_not_null() & pl.col("spread").is_not_null())
    q = q.with_columns(((pl.col("bar_ts") + offset) * 1000).alias("t_target"))
    q = q.filter(pl.col("ts_ms") <= pl.col("t_target"))
    q = q.sort(["market", "asset_id", "ts_ms"]).group_by(["market", "asset_id"]).last()
    w = (q.group_by("market").agg([
            pl.col("asset_id").get(pl.col("mid").arg_max()).alias("lead_asset"),
            pl.col("spread").get(pl.col("mid").arg_max()).alias("lead_spread"),
            pl.col("best_ask").get(pl.col("mid").arg_max()).alias("lead_ask"),
            pl.len().alias("n_sides")]).filter(pl.col("n_sides") == 2))
    w = w.join(bt, on="market", how="inner")
    w = w.with_columns((pl.col("winner") == pl.col("lead_asset")).cast(pl.Int8).alias("won"))
    return w.with_columns(
        pl.when(pl.col("won") == 1).then((1.0 - pl.col("lead_ask")) / pl.col("lead_ask")).otherwise(-1.0).alias("roi"))


def main() -> int:
    _norm()
    ap = argparse.ArgumentParser(description="价差→结果 的独立验证（只读 L2 派生层）")
    ap.add_argument("--derived", default="data/derived")
    ap.add_argument("--coins", default="btc,eth")
    ap.add_argument("--cycles", default="5m")
    ap.add_argument("--offset", type=int, default=60, help="入场时刻 = bar_ts + offset 秒")
    ap.add_argument("--selftest", action="store_true")
    a = ap.parse_args()

    if a.selftest:
        bt = pl.DataFrame({"market": ["m1", "m2"], "up_token": ["U1", "U2"], "down_token": ["D1", "D2"],
                           "winner": ["U1", "D2"]})
        q = pl.DataFrame({
            "market": ["m1", "m1", "m2", "m2"], "asset_id": ["U1", "D1", "U2", "D2"],
            "ts_ms": [1_000_000] * 4, "bar_ts": [940] * 4, "best_ask": [0.60, 0.42, 0.70, 0.32],
            "mid": [0.595, 0.415, 0.695, 0.315], "spread": [0.01, 0.01, 0.04, 0.04],
            "n_bids": [50] * 4, "n_asks": [50] * 4, "is_primary": [1] * 4,
        })
        w = _one_partition_q(q, bt, 60).sort("market")
        ok, fails = 0, []

        def expect(label, got, want):
            nonlocal ok
            good = got == want
            ok += good
            print(f"  [{'OK ' if good else 'FAIL'}] {label}: {got!r}" + ("" if good else f"（期望 {want!r}）"))
            if not good:
                fails.append(label)

        print("== 自检（合成夹具，手算核对）==")
        expect("两市场都取到样本", w.height, 2)
        expect("m1 领跑方 = UP（mid 0.595 > 0.415）⇒ 赢 ⇒ ROI = (1-0.60)/0.60",
               round(w.filter(pl.col("market") == "m1")["roi"][0], 6), round(0.4 / 0.6, 6))
        expect("m2 领跑方 = UP（0.695 > 0.315）⇒ 输（winner=D2）⇒ ROI = -1",
               w.filter(pl.col("market") == "m2")["roi"][0], -1.0)
        expect("m1 spread 取领跑方的 0.01", w.filter(pl.col("market") == "m1")["lead_spread"][0], 0.01)
        print()
        if fails:
            print(f"✗ 失败 {len(fails)} 项：{fails}")
            return 1
        print(f"✓ 通过 ({ok}/{ok})")
        return 0

    bt = pl.read_parquet(os.path.join(a.derived, "bar_tokens.parquet"))
    for coin in [c.strip() for c in a.coins.split(",") if c.strip()]:
        for cycle in [c.strip() for c in a.cycles.split(",") if c.strip()]:
            parts = sorted(glob.glob(os.path.join(a.derived, "quotes", "day=*", "coin=" + coin, "cycle=" + cycle, "*.parquet")))
            if not parts:
                print("(无数据 %s %s)" % (coin, cycle))
                continue
            frames = []
            for p in parts:
                try:
                    frames.append(_one_partition(p, bt, a.offset))
                except Exception as e:  # 单分区失败不该毁全局（截断文件等）
                    print("  跳过 %s：%r" % (os.path.dirname(p).split("day=")[-1], e))
            if not frames:
                continue
            df = pl.concat(frames)
            _report(df, "%s-%s（offset=bar+%ds）" % (coin, cycle, a.offset))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
