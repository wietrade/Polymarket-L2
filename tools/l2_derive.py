#!/usr/bin/env python3
"""L2 派生层：把原始 WS 帧（data/<date>/*.jsonl.gz）规范成可直接查询的表。

为什么要这层（2026-09-12 实测，见 docs/派生表-口径.md）：
  原始 gz jsonl 是**唯一真相**，不动它；但它有四个坑，每个消费者都要重新踩：
    1. 文件名 != 市场：3411 个文件里 1081 个含 >2 个 asset_id（相邻轮次残留）
    2. 行内时间戳不单调：6003 行回退 => 不能拿「最后一行」当最新盘口
    3. 有截断文件（重打当天包时文件正在写）：gzip 直接 EOFError
    4. 只有 asset_id，没有 UP/DOWN 侧别；本地 markets.csv 对最新几天的 bar 覆盖不全
       （抽 40 个 L2 market id 命中 0/40）=> 必须单独向 gamma 取 outcomes

本工具只读原始数据（绝不写 data/<date>/），产物一律落在 <out>=data/derived/ 下：
  bar_tokens.parquet     L1 侧别映射：condition_id -> up_token/down_token（gamma outcomes 口径）
                         + 官方结算的 outcome_prices / winner / closed（本地就能知道 bar 赢家）
  bar_tokens_state.json  已解析集合（断点续跑；已解析的不再重复抓）
  quotes/day=.../coin=.../cycle=.../part-0.parquet   每 book 帧一行（顶档）
  trades/day=.../coin=.../cycle=.../part-0.parquet   每 last_trade_price 一行
  marks/day=.../coin=.../cycle=.../part-0.parquet    gap/mark 行（秒口径，注意与 ts_ms 不同）
  _manifest.json         计数 / 未知侧别 / 截断文件 / 耗时（可复核）

口径（重要，别混）：
  · ts_ms  = 帧内 `timestamp` 字段，ms epoch，**UTC**，不做时区转换
  · from_ts/to_ts/marks = **epoch 秒**（recorder 的 wall clock），不是 ms
  · best_bid / best_ask = 档位里的**最高买价 / 最低卖价**（用 `_best()` 按价格取，**与数组排序无关**）。
    ⚠️ 2026-09-12 修正：原按索引取 `bids[-1]`/`asks[0]`，因把「哪端是最优」判反而把 `best_ask` 取成最差卖价。
  · side: 1=UP / 0=DOWN / -1=未知（gamma 没查到，或 outcomes 不是 ["Up","Down"]）
  · is_primary: 1 = 该行 market 等于本文件的主市场；0 = 相邻轮次残留，其 slug/bar_ts 不代表该行
  · 归因一律用 market + asset_id，别用文件名

跑法：
  python tools/l2_derive.py                 # 全流程（tokens -> tables），断点续跑
  python tools/l2_derive.py --stage tokens  # 只抓侧别映射
  python tools/l2_derive.py --stage tables  # 只生成派生表（需 tokens 已就绪）
  python tools/l2_derive.py --stage verify  # 双侧校验：行数逐分区比对 + 抽样逐字段比对
  python tools/l2_derive.py --self-test     # 离线自检（造 fixtures，不联网不碰真数据）
"""

from __future__ import annotations

import argparse
import array
import glob
import gzip
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except Exception as e:  # pragma: no cover
    print("需要 pyarrow：%s" % e, file=sys.stderr)
    raise SystemExit(2)

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
DATA = os.path.join(ROOT, "data")
DEFAULT_OUT = os.path.join(DATA, "derived")

GAMMA = "https://gamma-api.polymarket.com/markets"
UA = "Mozilla/5.0 (l2_derive; contact: local)"
# gamma 必须带 closed=true 才能按 condition_id 查到已结束的 updown 市场（实测不加返回 []）
GAMMA_BATCH = 50

# L1 缓存版本：**改了 parse_market 的字段就 +1**（旧缓存字段集不同 ⇒ 自动重抓）
TOKENS_V = 2

FNAME_RE = re.compile(
    r"^(?P<coin>btc|eth)-updown-(?P<cycle>5m|15m)-(?P<bar>\d+)\.jsonl\.gz$"
)

SPECS = {
    "quotes": [
        ("day", pa.string()),
        ("ts_ms", pa.int64()),
        ("bar_ts", pa.int64()),
        ("market", pa.string()),
        ("asset_id", pa.string()),
        ("side", pa.int8()),
        ("best_bid", pa.float64()),
        ("best_bid_sz", pa.float64()),
        ("best_ask", pa.float64()),
        ("best_ask_sz", pa.float64()),
        ("mid", pa.float64()),
        ("spread", pa.float64()),
        ("n_bids", pa.int16()),
        ("n_asks", pa.int16()),
        ("book_hash", pa.string()),
        ("slug", pa.string()),
        ("is_primary", pa.int8()),
    ],
    "trades": [
        ("day", pa.string()),
        ("ts_ms", pa.int64()),
        ("bar_ts", pa.int64()),
        ("market", pa.string()),
        ("asset_id", pa.string()),
        ("side", pa.int8()),
        ("taker_side", pa.string()),
        ("price", pa.float64()),
        ("size", pa.float64()),
        ("fee_rate_bps", pa.int32()),
        ("tx_hash", pa.string()),
        ("slug", pa.string()),
        ("is_primary", pa.int8()),
    ],
    "marks": [
        ("day", pa.string()),
        ("from_ts", pa.float64()),
        ("to_ts", pa.float64()),
        ("lost_s", pa.float64()),
        ("slug", pa.string()),
        ("note", pa.string()),
        ("coin", pa.string()),
        ("cycle", pa.string()),
        ("bar_ts", pa.int64()),
    ],
}

# 分区内排序键：让「该 asset 的最后一行」真的等于「最新一帧」
SORT_KEYS = {
    "quotes": ("asset_id", "ts_ms"),
    "trades": ("asset_id", "ts_ms"),
    "marks": ("from_ts",),
}


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def log(msg: str) -> None:
    print("%s %s" % (utc_now(), msg), flush=True)


def fnum(x):
    """价格/数量字符串 -> float；空/异常 -> None（不用 0 兜底，避免把缺失当 0 价）。"""
    if x is None or x == "":
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None



def _best(levels, take_max: bool):
    """从 book 档位取**最优价**与其 size —— **与数组排序无关**。

    ⚠️ 2026-09-12 修正（真 bug）：原实现按索引取 `bids[-1]` / `asks[0]`，注释写「bids 升序、asks 降序，
    实测 0 例外」。**排序确实单调，但「哪一端是最优价」判反了** —— 原始帧实测（data/2026-09-12/*.jsonl.gz）：
        bids: 0.01 → 0.02 → … → 0.49 → **0.50**（升序，最优在**尾**）
        asks: 0.99 → 0.98 → … → 0.52 → **0.51**（降序，最优在**尾**）
    ⇒ `asks[0]` 取到 **0.99 = 最差卖价**，派生层的 `best_ask` / `mid` / `spread` **三列全错**
    （`best_bid` 恰好取对了 ⇒ 只坏一半、更难察觉）。教训：**索引口径只验证了单调性，没验证语义**。
    """
    best = None
    for x in levels or []:
        if not isinstance(x, dict):
            continue
        px = fnum(x.get("price"))
        if px is None:
            continue
        if best is None or (px > best[0] if take_max else px < best[0]):
            best = (px, fnum(x.get("size")))
    return best if best else (None, None)

def inum(x, default=None):
    if x is None or x == "":
        return default
    try:
        return int(float(x))
    except (TypeError, ValueError):
        return default


# --------------------------------------------------------------------------
# 读原始文件（容忍截断）
# --------------------------------------------------------------------------
def iter_frames(path: str):
    """流式产出 (dict_frame, None) 或 (None, 错误类型名)。

    截断文件（打包时正在写）在读到尾部时抛 EOFError：**已完整读出的行照常产出**，
    只在最后追加一条错误标记 —— 这正是我们要的「能读多少算多少，但必须记账」。
    """
    try:
        with gzip.open(path, "rt", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line), None
                except json.JSONDecodeError:
                    yield None, "bad_json"
    except (EOFError, gzip.BadGzipFile, OSError, UnicodeDecodeError) as e:
        yield None, type(e).__name__


def file_meta(files):
    """一趟扫描：每个文件的 {主市场, market 计数, 行数, 截断/坏行}，并顺带收出全部 condition_id。

    返回 (meta, ids)。ids 在这里一并收出来 —— 否则 tokens 阶段要再把 3411 个文件全解析一遍
    （实测重复一趟 ~2.5 分钟）。
    """
    meta = {}
    ids = set()
    t0 = time.time()
    for i, p in enumerate(files, 1):
        cnt = {}
        rows = bad = 0
        trunc = None
        for r, err in iter_frames(p):
            if err:
                if err == "bad_json":
                    bad += 1
                else:
                    trunc = err
                continue
            rows += 1
            m = r.get("market")
            if m:
                ids.add(m)
                cnt[m] = cnt.get(m, 0) + 1
        meta[p] = {
            "rows": rows,
            "bad_json": bad,
            "truncated": trunc,
            "primary": max(cnt, key=cnt.get) if cnt else None,
            "n_markets": len(cnt),
        }
        if i % 500 == 0:
            log("  扫描 %d/%d 文件（%.0fs）" % (i, len(files), time.time() - t0))
    return meta, ids


def resolve_paths(day_globs):
    """入参可以是目录、日目录、或 glob（如 data/2026-*）；统一展开成 *.jsonl.gz 列表。

    捷径：只写 `2026-09-12` 这种裸日期也认（自动按 DATA 拼路径）——踩过
    「--days 2026-09-12 报没有匹配文件」的坑。
    """
    files = []
    for g in day_globs:
        # 裸日期 / 裸月份（如 2026-09-12、2026-09）自动按 DATA 拼路径
        if re.fullmatch(r"\d{4}-\d{2}(-\d{2})?", g):
            g = os.path.join(DATA, g)
        for hit in glob.glob(g):
            if os.path.isdir(hit):
                files += glob.glob(os.path.join(hit, "*.jsonl.gz"))
            elif hit.endswith(".jsonl.gz"):
                files.append(hit)
    return sorted(set(os.path.abspath(p) for p in files))


# --------------------------------------------------------------------------
# L1：gamma 侧别映射
# --------------------------------------------------------------------------
def gamma_batch(ids, closed=True):
    q = "&".join("condition_ids=%s" % i for i in ids)
    url = "%s?limit=%d&%s%s" % (
        GAMMA,
        max(len(ids) * 2, 20),
        "closed=true&" if closed else "",
        q,
    )
    req = urllib.request.Request(
        url, headers={"User-Agent": UA, "Accept": "application/json"}
    )
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                return json.loads(r.read())
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, OSError):
            if attempt == 2:
                raise
            time.sleep(1.5 * (attempt + 1))
    return []


def parse_market(m):
    """gamma market -> 侧别记录。outcomes 是 JSON 字符串（实测 '["Up", "Down"]'）。"""
    out = m.get("outcomes")
    if isinstance(out, str):
        try:
            out = json.loads(out)
        except json.JSONDecodeError:
            out = None
    toks = m.get("clobTokenIds")
    if isinstance(toks, str):
        try:
            toks = json.loads(toks)
        except json.JSONDecodeError:
            toks = None
    up = dn = None
    kind = "?"
    if isinstance(out, list) and isinstance(toks, list) and len(out) == len(toks) == 2:
        lo = [str(o).strip().lower() for o in out]
        if lo == ["up", "down"]:
            up, dn, kind = toks[0], toks[1], "updown"
        elif lo == ["down", "up"]:
            up, dn, kind = toks[1], toks[0], "updown_rev"
        else:
            kind = "other:" + ",".join(lo)

    # 官方结算价（顺带收下来）：["1","0"] = 第一个 outcome 赢。
    # 用途：① 本地就知道 bar 的赢家；② **用「末段 up 侧价格应收敛到赢家」独立验证侧别映射**
    # （若 up/down 配错，末段价格与官方赢家会呈 ~50% 的一致率，一眼就能看出来）。
    op = m.get("outcomePrices")
    if isinstance(op, str):
        try:
            op = json.loads(op)
        except json.JSONDecodeError:
            op = None
    winner = None
    if isinstance(op, list) and len(op) == 2 and kind in ("updown", "updown_rev"):
        try:
            pf = [float(x) for x in op]
            if sorted(pf) == [0.0, 1.0]:
                win_idx = 0 if pf[0] == 1.0 else 1
                win_name = str(out[win_idx]).strip().lower()
                winner = "UP" if win_name == "up" else "DOWN"
        except (TypeError, ValueError):
            winner = None

    return {
        "slug": m.get("slug"),
        "market": m.get("conditionId"),
        "up_token": up,
        "down_token": dn,
        "outcome_kind": kind,
        "outcome_prices": json.dumps(op, ensure_ascii=False)
        if op is not None
        else None,
        "winner": winner,
        "closed": bool(m.get("closed")),
        "question": m.get("question"),
        "end_date": m.get("endDate"),
    }


def stage_tokens(files, ids, out, jobs, force):
    state_p = os.path.join(out, "bar_tokens_state.json")
    state = {"fetched_at": None, "resolved": {}}
    if os.path.exists(state_p):
        with open(state_p, encoding="utf-8") as f:
            state = json.load(f)
    resolved = state.get("resolved") or {}
    # 缓存版本号：字段集变了必须重抓，否则新列会静默留空（曾经踩过这类"以为有其实没有"）
    if state.get("v") != TOKENS_V:
        log("  L1 缓存版本 %s != %s ⇒ 全量重抓" % (state.get("v"), TOKENS_V))
        resolved = {}
    if force:
        resolved = {}

    ids = set(ids)
    todo = sorted(i for i in ids if i not in resolved)
    log(
        "L1 侧别映射：L2 里共 %d 个 condition_id，已解析 %d，待抓 %d"
        % (len(ids), len(ids) - len(todo), len(todo))
    )

    if todo:
        batches = [todo[i : i + GAMMA_BATCH] for i in range(0, len(todo), GAMMA_BATCH)]
        done = 0

        def work(b):
            try:
                return gamma_batch(b, closed=True)
            except Exception as e:
                return {"__error__": "%s: %s" % (type(e).__name__, e), "__ids__": b}

        with ThreadPoolExecutor(max_workers=jobs) as ex:
            for res in ex.map(work, batches):
                done += 1
                if isinstance(res, dict) and "__error__" in res:
                    log(
                        "  批次失败(%s)，%d 个 id 留待下次断点续跑"
                        % (res["__error__"], len(res["__ids__"]))
                    )
                else:
                    for m in res:
                        if m.get("conditionId"):
                            resolved[m["conditionId"]] = parse_market(m)
                if done % 20 == 0 or done == len(batches):
                    log(
                        "  gamma 批次 %d/%d（已解析 %d）"
                        % (done, len(batches), len(resolved))
                    )
                    state["resolved"] = resolved
                    state["fetched_at"] = utc_now()
                    state["v"] = TOKENS_V
                    with open(state_p, "w", encoding="utf-8") as f:
                        json.dump(state, f, ensure_ascii=False)

        # closed=true 只返回**已结束**市场 ⇒ 当前还在跑的 bar 会查不到，
        # 去掉 closed 再补一次（2026-09-12 实测：这一步是必须的，否则当天 bar 全部 side=-1）
        still = [i for i in todo if i not in resolved]
        if still:
            log("  补抓未结束市场 %d 个（不带 closed 重试）" % len(still))
            sb = [still[i : i + GAMMA_BATCH] for i in range(0, len(still), GAMMA_BATCH)]

            def work2(b):
                try:
                    return gamma_batch(b, closed=False)
                except Exception as e:
                    return {"__error__": "%s: %s" % (type(e).__name__, e), "__ids__": b}

            with ThreadPoolExecutor(max_workers=jobs) as ex:
                for res in ex.map(work2, sb):
                    if isinstance(res, dict) and "__error__" in res:
                        log("  补抓批次失败(%s)" % res["__error__"])
                        continue
                    for m in res:
                        if m.get("conditionId"):
                            resolved[m["conditionId"]] = parse_market(m)

    state["resolved"] = resolved
    state["fetched_at"] = state.get("fetched_at") or utc_now()
    state["v"] = TOKENS_V
    with open(state_p, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False)

    # 落 parquet（与 state 内容一致，便于 join）
    cols = [
        "market",
        "slug",
        "up_token",
        "down_token",
        "outcome_kind",
        "outcome_prices",
        "winner",
        "closed",
        "question",
        "end_date",
    ]
    rows = {c: [] for c in cols}
    for cid, v in sorted(resolved.items()):
        rows["market"].append(cid)
        for c in cols[1:]:
            rows[c].append(v.get(c))
    schema = pa.schema(
        [
            ("market", pa.string()),
            ("slug", pa.string()),
            ("up_token", pa.string()),
            ("down_token", pa.string()),
            ("outcome_kind", pa.string()),
            ("outcome_prices", pa.string()),
            ("winner", pa.string()),
            ("closed", pa.bool_()),
            ("question", pa.string()),
            ("end_date", pa.string()),
        ]
    )
    tbl = pa.table(
        {c: pa.array(rows[c], type=schema.field(c).type) for c in cols}, schema=schema
    )
    pq.write_table(tbl, os.path.join(out, "bar_tokens.parquet"), compression="zstd")

    kud = sum(1 for v in resolved.values() if v.get("outcome_kind") == "updown")
    missing = sorted(ids - set(resolved))
    log(
        "L1 完成：bar_tokens.parquet 共 %d 行（updown 侧别可用 %d；未查到 %d）"
        % (len(resolved), kud, len(missing))
    )
    return {
        "ids": len(ids),
        "resolved": len(resolved),
        "updown": kud,
        "missing": len(missing),
    }


def load_tokens(out):
    """{condition_id: {up, down}} -> (token->side 映射, 未知 id 集合)"""
    p = os.path.join(out, "bar_tokens.parquet")
    if not os.path.exists(p):
        return {}, set()
    t = pq.read_table(
        p, columns=["market", "up_token", "down_token", "outcome_kind"]
    ).to_pydict()
    tok2side, known = {}, set()
    for i, cid in enumerate(t["market"]):
        known.add(cid)
        if t["outcome_kind"][i] == "updown":
            if t["up_token"][i]:
                tok2side[t["up_token"][i]] = 1
            if t["down_token"][i]:
                tok2side[t["down_token"][i]] = 0
    return tok2side, known


# --------------------------------------------------------------------------
# L2：派生表
# --------------------------------------------------------------------------
class Buffers:
    """按 (day, coin, cycle) 收集列缓冲。

    整数列用 array.array（紧凑、无 None）；浮点列用 list（**允许 None**：
    空盘口 / 缺字段必须落成 parquet null，不能拿 0 兜底 —— 0 是合法价格）。
    字符串列用 list。每天每 coin-cycle 约 5 万行，内存无压力。
    """

    def __init__(self, kind):
        self.kind = kind
        self.spec = SPECS[kind]
        self.parts = {}

    @staticmethod
    def _new(t):
        if t == pa.int64():
            return array.array("q", [])
        if t == pa.int32():
            return array.array("l", [])
        if t == pa.int16():
            return array.array("h", [])
        if t == pa.int8():
            return array.array("b", [])
        return []

    def add(self, key, rec):
        buf = self.parts.get(key)
        if buf is None:
            buf = self.parts[key] = {n: self._new(t) for n, t in self.spec}
        for n, _t in self.spec:
            buf[n].append(rec[n])

    def flush(self, out, day, extra_sort=True):
        written = 0
        for (coin, cycle), buf in sorted(self.parts.items()):
            n = len(buf[self.spec[0][0]])
            if not n:
                continue
            idx = list(range(n))
            if extra_sort:
                # 排序口径：先按 asset_id 再按 ts_ms（ts_ms 同为 int）=>「该 asset 最后一行 = 最新盘口」
                keys = SORT_KEYS[self.kind]
                idx.sort(key=lambda i: tuple(buf[k][i] for k in keys))
            cols, types = {}, []
            for name, t in self.spec:
                a = buf[name]
                if isinstance(a, array.array):
                    cols[name] = array.array(a.typecode, (a[i] for i in idx))
                else:
                    cols[name] = [a[i] for i in idx]
                types.append((name, t))
            schema = pa.schema(types)
            tbl = pa.table(
                {c: pa.array(cols[c], type=schema.field(c).type) for c, _ in types},
                schema=schema,
            )
            d = os.path.join(
                out, self.kind, "day=%s" % day, "coin=%s" % coin, "cycle=%s" % cycle
            )
            os.makedirs(d, exist_ok=True)
            pq.write_table(tbl, os.path.join(d, "part-0.parquet"), compression="zstd")
            written += n
        self.parts.clear()
        return written


def stage_tables(files, meta, out, base=None, quiet=False):
    base = base or DATA
    tok2side, known = load_tokens(out)
    if not known:
        log(
            "警告：bar_tokens.parquet 不存在或为空 => 所有行 side=-1（先跑 --stage tokens）"
        )
    unknown_tokens = {}
    counts = {k: 0 for k in SPECS}
    unknown_side_rows = 0
    by_day = {}

    # 按天分组，逐天 flush（每天每 coin-cycle 约 5 万行，内存无压力）
    day_files = {}
    for p in files:
        rel = os.path.relpath(p, base)
        day = rel.split(os.sep)[0]
        day_files.setdefault(day, []).append(p)

    for day, ps in sorted(day_files.items()):
        bufs = {k: Buffers(k) for k in SPECS}
        for p in ps:
            m = FNAME_RE.match(os.path.basename(p))
            coin, cycle, bar_ts = (
                (m.group("coin"), m.group("cycle"), int(m.group("bar")))
                if m
                else ("other", "other", 0)
            )
            fm = meta.get(p, {})
            primary = fm.get("primary")
            for r, err in iter_frames(p):
                if not r or err:
                    continue
                et = r.get("event_type")
                mk = r.get("market")
                aid = r.get("asset_id")
                if et == "book":
                    bids = r.get("bids") or []
                    asks = r.get("asks") or []
                    bb, bsz = _best(bids, True)
                    ba, asz = _best(asks, False)
                    side = tok2side.get(aid, -1)
                    if side == -1:
                        unknown_tokens[aid] = unknown_tokens.get(aid, 0) + 1
                    rec = {
                        "day": day,
                        "ts_ms": inum(r.get("timestamp"), 0),
                        "bar_ts": bar_ts,
                        "market": mk,
                        "asset_id": aid,
                        "side": side,
                        "best_bid": bb,
                        "best_bid_sz": bsz,
                        "best_ask": ba,
                        "best_ask_sz": asz,
                        "mid": (bb + ba) / 2
                        if (bb is not None and ba is not None)
                        else None,
                        "spread": (ba - bb)
                        if (bb is not None and ba is not None)
                        else None,
                        "n_bids": len(bids),
                        "n_asks": len(asks),
                        "book_hash": r.get("hash"),
                        "slug": os.path.basename(p).replace(".jsonl.gz", ""),
                        "is_primary": 1 if (mk and mk == primary) else 0,
                    }
                    bufs["quotes"].add((coin, cycle), rec)
                    counts["quotes"] += 1
                    if side == -1:
                        unknown_side_rows += 1
                elif et == "last_trade_price":
                    side = tok2side.get(aid, -1)
                    if side == -1:
                        unknown_tokens[aid] = unknown_tokens.get(aid, 0) + 1
                    rec = {
                        "day": day,
                        "ts_ms": inum(r.get("timestamp"), 0),
                        "bar_ts": bar_ts,
                        "market": mk,
                        "asset_id": aid,
                        "side": side,
                        "taker_side": r.get("side"),
                        "price": fnum(r.get("price")),
                        "size": fnum(r.get("size")),
                        "fee_rate_bps": inum(r.get("fee_rate_bps"), 0),
                        "tx_hash": r.get("transaction_hash"),
                        "slug": os.path.basename(p).replace(".jsonl.gz", ""),
                        "is_primary": 1 if (mk and mk == primary) else 0,
                    }
                    bufs["trades"].add((coin, cycle), rec)
                    counts["trades"] += 1
                    if side == -1:
                        unknown_side_rows += 1
                elif et is None and r.get("mark") == "gap":
                    rec = {
                        "day": day,
                        "from_ts": float(r.get("from_ts") or 0.0),
                        "to_ts": float(r.get("to_ts") or 0.0),
                        "lost_s": float(r.get("lost_s") or 0.0),
                        "slug": r.get("slug"),
                        "note": r.get("note"),
                        "coin": coin,
                        "cycle": cycle,
                        "bar_ts": bar_ts,
                    }
                    bufs["marks"].add((coin, cycle), rec)
                    counts["marks"] += 1
        for k in SPECS:
            n = bufs[k].flush(out, day)
            by_day.setdefault(day, {})[k] = n
        if not quiet:
            log(
                "  派生表 %s：quotes/trades/marks = %s"
                % (day, [by_day[day][k] for k in ("quotes", "trades", "marks")])
            )

    top_unknown = sorted(unknown_tokens.items(), key=lambda kv: -kv[1])[:10]
    rep = {
        "counts": counts,
        "by_day": by_day,
        "unknown_side_rows": unknown_side_rows,
        "unknown_tokens": len(unknown_tokens),
        "unknown_tokens_top": [{"asset_id": a, "rows": n} for a, n in top_unknown],
    }
    log(
        "L2 完成：quotes=%d trades=%d marks=%d；side 未知行 %d（涉及 %d 个 asset_id）"
        % (
            counts["quotes"],
            counts["trades"],
            counts["marks"],
            unknown_side_rows,
            len(unknown_tokens),
        )
    )
    return rep


# --------------------------------------------------------------------------
# 校验
# --------------------------------------------------------------------------
def count_raw(files, meta):
    """从原始数据重算各表应有行数（唯一真相口径）。"""
    want = {k: 0 for k in SPECS}
    for p in files:
        for r, err in iter_frames(p):
            if not r or err:
                continue
            et = r.get("event_type")
            if et == "book":
                want["quotes"] += 1
            elif et == "last_trade_price":
                want["trades"] += 1
            elif et is None and r.get("mark") == "gap":
                want["marks"] += 1
    return want


def count_parquet(out):
    got = {}
    for k in SPECS:
        n = 0
        for p in glob.glob(os.path.join(out, k, "**", "*.parquet"), recursive=True):
            n += pq.ParquetFile(p).metadata.num_rows
        got[k] = n
    return got


def sample_compare(files, out, base=None, n_sample=8, seed=20260912):
    """抽样逐字段比对：随机文件里的 book/trade 行必须能在 parquet 里找到完全相同的值。"""
    import random

    base = base or DATA
    rnd = random.Random(seed)
    picks = rnd.sample(files, min(n_sample, len(files)))
    mismatches = []
    checked = 0
    for p in picks:
        day = os.path.relpath(p, base).split(os.sep)[0]
        m = FNAME_RE.match(os.path.basename(p))
        coin, cycle = (m.group("coin"), m.group("cycle")) if m else ("other", "other")
        q = os.path.join(
            out,
            "quotes",
            "day=%s" % day,
            "coin=%s" % coin,
            "cycle=%s" % cycle,
            "part-0.parquet",
        )
        t = os.path.join(
            out,
            "trades",
            "day=%s" % day,
            "coin=%s" % coin,
            "cycle=%s" % cycle,
            "part-0.parquet",
        )
        if not os.path.exists(q):
            mismatches.append("%s: 缺 quotes 分区" % os.path.relpath(q, out))
            continue
        qd = pq.read_table(q).to_pydict()
        td = (
            pq.read_table(t).to_pydict()
            if os.path.exists(t)
            else {c: [] for c, _ in SPECS["trades"]}
        )
        qidx = {}
        for i in range(len(qd["ts_ms"])):
            qidx[(qd["asset_id"][i], qd["ts_ms"][i], qd["book_hash"][i])] = i
        tidx = {}
        for i in range(len(td["ts_ms"])):
            tidx[(td["asset_id"][i], td["ts_ms"][i], td["tx_hash"][i])] = i
        rows = [(r, None) for r, e in iter_frames(p) if r]
        rnd.shuffle(rows)
        for r, _e in rows[:40]:
            et = r.get("event_type")
            aid = r.get("asset_id")
            if et == "book":
                k = (aid, inum(r.get("timestamp"), 0), r.get("hash"))
                if k not in qidx:
                    mismatches.append("%s: quote 缺 %s" % (os.path.basename(p), k[1]))
                    continue
                i = qidx[k]
                bids = r.get("bids") or []
                asks = r.get("asks") or []
                _bb, _ = _best(bids, True)
                _ba, _ = _best(asks, False)
                exp = (_bb, _ba, len(bids), len(asks))
                act = (
                    qd["best_bid"][i],
                    qd["best_ask"][i],
                    qd["n_bids"][i],
                    qd["n_asks"][i],
                )
                if exp != act:
                    mismatches.append(
                        "%s: quote 值不符 %s exp=%s act=%s"
                        % (os.path.basename(p), k[1], exp, act)
                    )
                checked += 1
            elif et == "last_trade_price":
                k = (aid, inum(r.get("timestamp"), 0), r.get("transaction_hash"))
                if k not in tidx:
                    mismatches.append("%s: trade 缺 %s" % (os.path.basename(p), k[1]))
                    continue
                i = tidx[k]
                exp = (fnum(r.get("price")), fnum(r.get("size")), r.get("side"))
                act = (td["price"][i], td["size"][i], td["taker_side"][i])
                if exp != act:
                    mismatches.append(
                        "%s: trade 值不符 %s exp=%s act=%s"
                        % (os.path.basename(p), k[1], exp, act)
                    )
                checked += 1
    return checked, mismatches


def stage_verify(files, meta, out, base=None):
    base = base or DATA
    log("校验①：原始行数 vs parquet 行数（逐表）")
    want = count_raw(files, meta)
    got = count_parquet(out)
    bad = []
    for k in SPECS:
        ok = want[k] == got[k]
        print(
            "  %-7s 原始 %8d | parquet %8d | %s"
            % (k, want[k], got[k], "一致" if ok else "不一致")
        )
        if not ok:
            bad.append(k)
    log("校验②：随机抽文件逐字段比对（quotes 顶档 / trades 价格数量）")
    checked, mism = sample_compare(files, out, base=base)
    print("  比对 %d 行，异常 %d 条" % (checked, len(mism)))
    for x in mism[:10]:
        print("   -", x)
    return {
        "row_counts": {"want": want, "got": got},
        "sample_checked": checked,
        "sample_mismatch": len(mism),
        "ok": (not bad and not mism),
    }


# --------------------------------------------------------------------------
# 离线自检
# --------------------------------------------------------------------------
def self_test():
    # Windows 控制台默认 GBK ⇒ 打印 −/⇒/→ 之类的字符会 UnicodeEncodeError 崩在自检中途
    # （2026-09-12 实测：一条含 U+2212 的断言标签直接把 self-test 打崩）。Linux 本就是 UTF-8，无副作用。
    for _s in (sys.stdout, sys.stderr):
        try:
            _s.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass
    import shutil
    import tempfile

    checks = []

    def check(name, cond, detail=""):
        checks.append((name, bool(cond), detail))

    tmp = tempfile.mkdtemp(prefix="l2derive_selftest_")
    try:
        day = "2026-01-02"
        dd = os.path.join(tmp, "data", day)
        os.makedirs(dd, exist_ok=True)
        up, dn, other = "111", "222", "999"
        good = os.path.join(dd, "btc-updown-5m-1789000000.jsonl.gz")
        rows = [
            # 主市场 up 侧 book：先给"量少"的，后给"顶档更好"的，验证排序后最后一行=最新
            {
                "market": "0xM1",
                "asset_id": up,
                "timestamp": "1789000005000",
                "hash": "h1",
                "event_type": "book",
                # ⚠️ 2026-09-12：改成**多档 + 真实帧排列** —— 买价升序、卖价降序（最优都在**最后**）。
                #   原夹具是单档书（bids[0.40]、asks[0.44]）⇒ 取头取尾都一样 ⇒ **永远抓不到
                #   「asks[0] 取了最差卖价」那个 bug**（自检把错误假设编了进去）。现在老代码会给 0.48 ⇒ 必红。
                "bids": [
                    {"price": "0.38", "size": "4"},
                    {"price": "0.42", "size": "10"},
                ],
                "asks": [
                    {"price": "0.48", "size": "3"},
                    {"price": "0.44", "size": "5"},
                ],
            },
            {
                "market": "0xM1",
                "asset_id": up,
                "timestamp": "1789000001000",
                "hash": "h0",  # 时间回退
                "event_type": "book",
                "bids": [
                    {"price": "0.30", "size": "2"},
                    {"price": "0.41", "size": "7"},
                ],
                "asks": [
                    {"price": "0.43", "size": "4"},
                    {"price": "0.50", "size": "9"},
                ],
            },
            # down 侧成交
            {
                "market": "0xM1",
                "asset_id": dn,
                "timestamp": "1789000006000",
                "event_type": "last_trade_price",
                "price": "0.57",
                "size": "12.5",
                "fee_rate_bps": "0",
                "side": "BUY",
                "transaction_hash": "0xtx1",
            },
            # 相邻轮次残留（非主市场）
            {
                "market": "0xM2",
                "asset_id": other,
                "timestamp": "1789000007000",
                "hash": "h2",
                "event_type": "book",
                "bids": [],
                "asks": [],
            },
            # gap mark（秒口径）
            {
                "mark": "gap",
                "slug": "btc-updown-5m-1789000000",
                "from_ts": 1789000010.5,
                "to_ts": 1789000013.5,
                "lost_s": 3.0,
                "note": "reconnected",
            },
        ]
        with gzip.open(good, "wt", encoding="utf-8") as f:
            for r in rows:
                f.write(json.dumps(r) + "\n")
        # 截断文件：合法 gz 再砍掉尾部
        trunc = os.path.join(dd, "eth-updown-5m-1789000300.jsonl.gz")
        with gzip.open(trunc, "wt", encoding="utf-8") as f:
            for i in range(50):
                f.write(
                    json.dumps(
                        {
                            "market": "0xM3",
                            "asset_id": "333",
                            "timestamp": str(1789000300000 + i),
                            "event_type": "book",
                            "bids": [{"price": "0.5", "size": "1"}],
                            "asks": [{"price": "0.6", "size": "1"}],
                            "hash": "t%d" % i,
                        }
                    )
                    + "\n"
                )
        raw = open(trunc, "rb").read()
        with open(trunc, "wb") as f:
            f.write(raw[: int(len(raw) * 0.7)])

        out = os.path.join(tmp, "derived")
        os.makedirs(out, exist_ok=True)
        # 造 L1 映射（离线，等价于 gamma 结果）
        schema = pa.schema(
            [
                ("market", pa.string()),
                ("slug", pa.string()),
                ("up_token", pa.string()),
                ("down_token", pa.string()),
                ("outcome_kind", pa.string()),
                ("question", pa.string()),
                ("end_date", pa.string()),
            ]
        )
        pq.write_table(
            pa.table(
                {
                    "market": pa.array(["0xM1", "0xM3"], pa.string()),
                    "slug": pa.array(
                        ["btc-updown-5m-1789000000", "eth-updown-5m-1789000300"],
                        pa.string(),
                    ),
                    "up_token": pa.array([up, "333"], pa.string()),
                    "down_token": pa.array([dn, "444"], pa.string()),
                    "outcome_kind": pa.array(["updown", "updown"], pa.string()),
                    "question": pa.array(["Q1", "Q2"], pa.string()),
                    "end_date": pa.array(["2026-01-02", "2026-01-02"], pa.string()),
                },
                schema=schema,
            ),
            os.path.join(out, "bar_tokens.parquet"),
        )

        files = resolve_paths([dd])
        meta, ids = file_meta(files)
        check("扫描到 2 个文件（good + 截断）", len(files) == 2, "got=%d" % len(files))
        check(
            "扫描顺带收出 condition_id 集合 = {0xM1,0xM2,0xM3}",
            ids == {"0xM1", "0xM2", "0xM3"},
            str(sorted(ids)),
        )
        m_good = meta[good]
        m_tr = meta[trunc]
        check(
            "主市场判定=0xM1（出现 3 次 vs 0xM2 一次）",
            m_good["primary"] == "0xM1",
            str(m_good["primary"]),
        )
        check(
            "good 文件 n_markets=2（含相邻轮次残留）",
            m_good["n_markets"] == 2,
            str(m_good["n_markets"]),
        )
        check(
            "截断文件被识别为 truncated 且非 None",
            m_tr["truncated"] is not None,
            str(m_tr["truncated"]),
        )
        check(
            "截断文件仍读出 >0 行（能读多少算多少）",
            m_tr["rows"] > 0,
            "rows=%d" % m_tr["rows"],
        )
        check(
            "截断文件行数 < 50（确实丢了尾部）",
            m_tr["rows"] < 50,
            "rows=%d" % m_tr["rows"],
        )

        rep = stage_tables(files, meta, out, base=os.path.join(tmp, "data"), quiet=True)
        check(
            "quotes 行数 = good 文件 3 帧 + 截断文件可读 %d 帧" % m_tr["rows"],
            rep["counts"]["quotes"] == 3 + m_tr["rows"],
            "got=%s want=%s" % (rep["counts"]["quotes"], 3 + m_tr["rows"]),
        )
        check(
            "trades 行数 = 1",
            rep["counts"]["trades"] == 1,
            str(rep["counts"]["trades"]),
        )
        check(
            "marks 行数 = 1", rep["counts"]["marks"] == 1, str(rep["counts"]["marks"])
        )
        check(
            "side 未知行 = 1（就是那条相邻轮次残留 0xM2/999）",
            rep["unknown_side_rows"] == 1,
            str(rep["unknown_side_rows"]),
        )

        q = pq.read_table(
            os.path.join(
                out, "quotes", "day=%s" % day, "coin=btc", "cycle=5m", "part-0.parquet"
            )
        ).to_pydict()
        # 排序后 asset=up 的行：ts 1000 在前、5000 在后 => 最后一行 = 最新
        ups = [i for i in range(len(q["ts_ms"])) if q["asset_id"][i] == up]
        check(
            "up 侧按 ts_ms 升序（时间回退被排序修掉）",
            [q["ts_ms"][i] for i in ups] == [1789000001000, 1789000005000],
            str([q["ts_ms"][i] for i in ups]),
        )
        i_latest = ups[-1]
        check(
            "最新一行 best_bid=0.42（= 买价里的最高价，与数组顺序无关）",
            q["best_bid"][i_latest] == 0.42,
            str(q["best_bid"][i_latest]),
        )
        check(
            "最新一行 best_ask=0.44（= 卖价里的最低价；老实现取 asks[0] 会给 0.48）",
            q["best_ask"][i_latest] == 0.44,
            str(q["best_ask"][i_latest]),
        )
        check(
            "最新一行 spread=0.02（0.44−0.42；不含手续费）",
            abs(q["spread"][i_latest] - 0.02) < 1e-12,
            str(q["spread"][i_latest]),
        )
        check(
            "最新一行 mid=0.43",
            abs(q["mid"][i_latest] - 0.43) < 1e-12,
            str(q["mid"][i_latest]),
        )
        i_early = ups[0]
        check(
            "较早一行 best_bid=0.41（bids 升序 => 末档才是最优）",
            q["best_bid"][i_early] == 0.41,
            str(q["best_bid"][i_early]),
        )
        check(
            "较早一行 n_bids=2 / n_asks=2",
            (q["n_bids"][i_early], q["n_asks"][i_early]) == (2, 2),
            str((q["n_bids"][i_early], q["n_asks"][i_early])),
        )
        check(
            "side 映射：up token -> 1",
            q["side"][i_latest] == 1,
            str(q["side"][i_latest]),
        )
        i_other = [i for i in range(len(q["ts_ms"])) if q["asset_id"][i] == other][0]
        check(
            "非主市场行 is_primary=0（归因不受文件名影响）",
            q["is_primary"][i_other] == 0,
            str(q["is_primary"][i_other]),
        )
        check(
            "非主市场行 side=-1（0xM2 未在映射表里）",
            q["side"][i_other] == -1,
            str(q["side"][i_other]),
        )
        check(
            "空盘口行 best_bid/best_ask 为 None（不拿 0 兜底）",
            q["best_bid"][i_other] is None and q["best_ask"][i_other] is None,
            str((q["best_bid"][i_other], q["best_ask"][i_other])),
        )
        check(
            "bar_ts 取自文件名 = 1789000000",
            q["bar_ts"][i_latest] == 1789000000,
            str(q["bar_ts"][i_latest]),
        )

        t = pq.read_table(
            os.path.join(
                out, "trades", "day=%s" % day, "coin=btc", "cycle=5m", "part-0.parquet"
            )
        ).to_pydict()
        check("成交 side 映射：down token -> 0", t["side"][0] == 0, str(t["side"][0]))
        check(
            "成交 price/size = 0.57 / 12.5",
            (t["price"][0], t["size"][0]) == (0.57, 12.5),
            str((t["price"][0], t["size"][0])),
        )

        mk = pq.read_table(
            os.path.join(
                out, "marks", "day=%s" % day, "coin=btc", "cycle=5m", "part-0.parquet"
            )
        ).to_pydict()
        check(
            "mark 用秒口径 from_ts=1789000010.5（不是 ms）",
            mk["from_ts"][0] == 1789000010.5,
            str(mk["from_ts"][0]),
        )
        check("mark lost_s=3.0", mk["lost_s"][0] == 3.0, str(mk["lost_s"][0]))

        v = stage_verify(files, meta, out, base=os.path.join(tmp, "data"))
        check(
            "校验：行数全表一致 + 抽样 0 异常",
            v["ok"] is True,
            "counts=%s mism=%d" % (v["row_counts"], v["sample_mismatch"]),
        )
        check(
            "校验抽样覆盖两文件全部可读行（good 3 book+1 trade，截断 %d）"
            % m_tr["rows"],
            v["sample_checked"] == 4 + m_tr["rows"],
            str(v["sample_checked"]),
        )

        # 断点续跑：再跑一次 stage_tables，结果必须一致（幂等）
        rep2 = stage_tables(
            files, meta, out, base=os.path.join(tmp, "data"), quiet=True
        )
        check(
            "幂等：重跑 counts 不变",
            rep2["counts"] == rep["counts"],
            "%s vs %s" % (rep2["counts"], rep["counts"]),
        )
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

    bad = [(n, d) for n, ok, d in checks if not ok]
    for n, ok, d in checks:
        print(
            "  [%s] %s%s"
            % ("PASS" if ok else "FAIL", n, ("  -> " + d) if (d and not ok) else "")
        )
    print("\n自检：PASS %d / FAIL %d" % (len(checks) - len(bad), len(bad)))
    return 1 if bad else 0


# --------------------------------------------------------------------------
def main(argv=None):
    ap = argparse.ArgumentParser(description="L2 派生层：侧别映射 + 顶档/成交/gap 表")
    ap.add_argument(
        "--stage", default="all", choices=["all", "tokens", "tables", "verify"]
    )
    ap.add_argument(
        "--days",
        nargs="*",
        default=None,
        help="日期目录或 glob（默认 data/2026-*；例 2026-09-12）",
    )
    ap.add_argument("--out", default=DEFAULT_OUT)
    ap.add_argument("--jobs", type=int, default=8, help="gamma 并发（默认 8）")
    ap.add_argument(
        "--force", action="store_true", help="tokens 阶段忽略已有缓存重新抓"
    )
    ap.add_argument(
        "--self-test", action="store_true", help="离线自检（不联网、不碰真数据）"
    )
    a = ap.parse_args(argv)

    if a.self_test:
        return self_test()

    out = os.path.abspath(a.out)
    os.makedirs(out, exist_ok=True)
    globs = a.days if a.days else [os.path.join(DATA, "2026-*")]
    files = resolve_paths(globs)
    if not files:
        log("没有匹配的原始文件：%s" % globs)
        return 2
    log(
        "原始文件 %d 个（%s）"
        % (
            len(files),
            ", ".join(os.path.basename(os.path.dirname(f)) for f in files[:1]),
        )
    )

    t0 = time.time()
    report = {"generated_at": utc_now(), "raw_files": len(files)}

    meta, ids = file_meta(files)
    trunc = [os.path.relpath(p, DATA) for p, m in meta.items() if m["truncated"]]
    badjson = sum(m["bad_json"] for m in meta.values())
    report["scan"] = {
        "rows": sum(m["rows"] for m in meta.values()),
        "truncated_files": trunc,
        "bad_json_lines": badjson,
        "mixed_market_files": sum(1 for m in meta.values() if m["n_markets"] > 1),
    }
    log(
        "扫描：行 %d；截断文件 %d 个；坏行 %d；一个文件里含多个市场的文件 %d 个"
        % (
            report["scan"]["rows"],
            len(trunc),
            badjson,
            report["scan"]["mixed_market_files"],
        )
    )
    if trunc:
        for x in trunc:
            print("   截断:", x)

    if a.stage in ("all", "tokens"):
        report["tokens"] = stage_tokens(files, ids, out, a.jobs, a.force)
    if a.stage in ("all", "tables"):
        report["tables"] = stage_tables(files, meta, out)
    if a.stage in ("all", "verify"):
        report["verify"] = stage_verify(files, meta, out)

    report["elapsed_s"] = round(time.time() - t0, 1)
    # 注：_manifest.json 描述的是**本次运行**的范围（stage/days），不是数据集的全部
    report["stage"] = a.stage
    report["days"] = sorted({os.path.relpath(p, DATA).split(os.sep)[0] for p in files})
    with open(os.path.join(out, "_manifest.json"), "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    log(
        "完成（%.0fs）-> %s"
        % (report["elapsed_s"], os.path.join(out, "_manifest.json"))
    )
    if a.stage in ("all", "verify") and not report.get("verify", {}).get("ok", True):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
