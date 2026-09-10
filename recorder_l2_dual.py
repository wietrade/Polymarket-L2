"""recorder_l2_dual.py — Polymarket updown L2 积累器 v3 (双连接热备 + 写盘解耦).

相对 v2 (recorder_l2.py) 的改动, 针对实测三个断线根因:
  1. slow consumer (1013): 原 route() 同步 gzip 写盘阻塞事件循环 → 洪峰拖慢 recv 被踢
     → 解耦: 连接只收帧入 asyncio.Queue, 独立 writer task 消费写盘 (事件循环不碰磁盘)
  2. ping timeout 误杀 (1011): 洪峰期服务器不立即回 pong, ping_timeout=8 太小误判死链
     → ping_interval=10 喂保活, ping_timeout=30 放宽; 真死链由 recv 无帧 >60s 判定
  3. 服务器周期性回收 → 无法阻止 → 双连接热备: 每线 2 条 WS 订同一 slug 独立重连,
     消息按 (event_type, asset_id, hash/transaction_hash) 去重后只写一份 → 单连接断不丢数据

架构 (每条市场线 btc/eth×5m/15m):
  · conn_loop x2   : 各持一条 WS, 收帧→去重→入队; 独立指数退避重连 (3s→60s)
  · writer_loop    : 消费队列写 gzip; 单文件模型(同线只开当前 slug 一个句柄, bar 切换
                     同步关旧开新, 丢弃切 bar 竞态旧 slug 尾消息 → 防 reopen/truncate 损坏; 管 gap 状态机
  · 切 bar 重建订阅 : 换市场时拉高订阅代 → 两条连接各自换**新连接**重建订阅
                     ← 实测在旧连接上追加订阅帧不生效(连接仍活但新市场零消息),
                       不重建则每 bar 前 ~120s 数据永久缺失
  · ticker_loop    : 每 5s 算 bar→取新市场 token (gamma, to_thread) → sub_ver++ 通知重订阅
  单条 WS 连接订阅的仍是当前 slug 的 up/down 两 token (实测 1 连接多 token 会被踢).

数据格式/存储/CLI 与 v2 完全兼容 (每 slug 一 gz, 行=原始消息 JSON + gap mark 行).

用法(43):  python -u recorder_l2_dual.py --dir /www/wwwroot/polymarket-l2/l2_data_dual
"""

from __future__ import annotations

import argparse
import asyncio
import gzip
import json
import os
import signal
import socket
import sys
import time
import urllib.request
from datetime import datetime, timezone

import websockets

UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
GUARD_PORT = 45614  # 单例防双开 (与 v2 不同端口, 可并行试跑)

MARKETS = [
    ("btc", 300, "5m", "btc-updown-5m"),
    ("eth", 300, "5m", "eth-updown-5m"),
    ("btc", 900, "15m", "btc-updown-15m"),
    ("eth", 900, "15m", "eth-updown-15m"),
]

GUARD_SOCK = None
# 连接判定阈值
PING_INTERVAL = 10.0  # 保活帧 (喂服务器 keepalive)
PING_TIMEOUT = 30.0  # 放宽: 防洪峰期 pong 延迟导致误杀
RECV_TIMEOUT = 15.0  # 单帧等待
DOWN_AFTER = 60.0  # 该时长无任何帧 → 视为该连接已 down
CLOSE_DELAY = 30.0  # bar 切换后旧文件延迟关闭窗口 (吸收残留尾消息)
DEDUP_WIN = 60.0  # 跨连接去重窗口秒
DEDUP_MAX = 40000  # 去重窗口最大 key 数
SILENT_ROTATE_AFTER = 45.0  # 市场活跃但该时长无写盘 → 判订阅失效, 强制重建
TICK_POLL_S = 1.0  # bar 切换检测间隔(原 5s -> 新 bar 头 3~11s 无数据)
ROTATE_WAIT_S = 1.0  # 重建订阅的重连等待(原 2s)
FLUSH_EVERY_S = 2.0  # 磁盘可见性: 每 2s flush 一次
#   ⚠ 原来只每 1000 行 flush → 磁盘上可见数据可能落后 ~60s
#   → 任何“实时对比 L2”的测量(如 hub vs L2 报价滞后)都会失准

# ⚠ 为什么换市场必须换新连接(2026-09-10 查到)
#   recorder 切 bar 后仍在旧连接上发 {"assets_ids": 新 token} 订阅帧, 实测 **不生效**:
#   连接继续收 PONG(活)但新市场零消息 → 只能等服务器 ~2-4min 一次的例行断连重连才恢复。
#   后果: 2026-09-07/08/09 全部 5m/15m 文件, 每个 bar 只有后 ~180s 有数据。


def now_utc():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def singleton_acquire():
    global GUARD_SOCK
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("127.0.0.1", GUARD_PORT))
        s.listen(1)
        GUARD_SOCK = s
    except OSError:
        print("🔒 已有 v3 recorder 在跑, 退出", flush=True)
        sys.exit(1)
    print(f"🔒 L2 recorder v3 单例锁取得 ({GUARD_PORT})", flush=True)


def gamma_tokens(slug):
    """slug -> (up_token, down_token); 异常则 None. (阻塞, 调用方放线程)"""
    try:
        req = urllib.request.Request(
            "https://gamma-api.polymarket.com/markets/slug/" + slug, headers=UA
        )
        d = json.loads(urllib.request.urlopen(req, timeout=10).read())
        if isinstance(d, list) and d:
            d = d[0]
        tk = d.get("clobTokenIds")
        tk = json.loads(tk) if isinstance(tk, str) else tk
        if not tk or len(tk) < 2:
            return None
        return tk[0], tk[1]
    except Exception:
        return None


class LineDualRecorder:
    """一条市场线(如 btc-5m): 双 WS 连接热备 + 独立 writer/ticker."""

    def __init__(self, coin, cycle, label, prefix, data_dir):
        self.coin = coin
        self.cycle = cycle
        self.label = label
        self.prefix = prefix
        self.data_dir = data_dir
        # 市场状态 (ticker 更新, 连接读)
        self.slug = None
        self.up = self.dn = None
        self.sub_ver = 0
        # 连接状态
        self.alive = [0.0, 0.0]  # 最近帧时刻 (0=从未)
        self.disc = [0, 0]
        self.msg = [0, 0]
        self.dup = [0, 0]
        self.q = asyncio.Queue(maxsize=50000)
        # 统计 / gap
        self.n_gap = 0
        self.n_err = 0
        self.gap_from = None
        self.gap_slug = None
        # 单文件模型: 同线同时只维护当前活跃 slug 一个 gz 句柄 (v2 式, 杜绝 reopen/truncate 交错写损坏)
        self.active_f = None  # 当前活跃 gzip 句柄
        self.active_slug = None  # active_f 对应的 slug
        self.seen: dict = {}  # 去重 key -> ts
        # ⚠ bar 切换必须在**新连接**上重建订阅: 实测在旧连接上追加订阅帧不生效
        #   (连接仍活着收 PONG, 但新市场零消息) → 要等服务器 ~2-4min 例行断开才恢复
        #   → 实测后果: 每个 bar 前 ~120s 数据永久缺失(2026-09-08/09 全量文件均如此)
        #   实现: 订阅代 sub_gen —— 换市场/静默重建时 +1; 连接侧 my_gen 落后即断连重连
        #   (用代而非计时器: 旧连接若在 0.5s 内醒来会发无效订阅帧并清掉计时器 → 竞态漏修)
        self.sub_gen = 0
        self.n_rotate = [0, 0]
        self.last_write_ts = 0.0  # 最近成功写盘时刻(静默看门狗)
        self.n_since_flush = 0  # 自上次 flush 的写入条数
        self.last_flush_ts = 0.0  # 最近 flush 时刻(时间驱动刷盘)

    # ---------- 文件 ----------
    def _path(self, slug):
        bar = int(slug.rsplit("-", 1)[1])
        d = datetime.fromtimestamp(bar, tz=timezone.utc).strftime("%Y-%m-%d")
        dd = os.path.join(self.data_dir, d)
        os.makedirs(dd, exist_ok=True)
        return os.path.join(dd, slug + ".jsonl.gz")

    # ---- 单文件写盘 (v2 式): 只维护 active_slug 一个 gz, bar 切换同步关旧开新 ----
    def _open_active(self):
        """懒开当前 active_slug 文件; 返回句柄(未就绪则 None)."""
        if self.active_f is None and self.active_slug:
            self.active_f = gzip.open(
                self._path(self.active_slug), "wt", encoding="utf-8"
            )
        return self.active_f

    def _close_active(self):
        if self.active_f is not None:
            try:
                self.active_f.close()
            except Exception:
                pass
            self.active_f = None

    def _write(self, m) -> bool:
        """写一条消息到当前活跃文件; 文件未就绪返回 False.

        磁盘可见性: 每 1000 行 或 每 FLUSH_EVERY_S 秒 flush 一次。
        只按行数 flush 时, 磁盘上可见数据可能落后 ~60s → 实时对比会失准(2026-09-10 实测)。
        """
        f = self._open_active()
        if f is None:
            return False
        f.write(json.dumps(m, ensure_ascii=False) + "\n")
        self.last_write_ts = time.time()
        self.n_since_flush += 1
        if (
            self.n_since_flush >= 1000
            or self.last_write_ts - self.last_flush_ts >= FLUSH_EVERY_S
        ):
            try:
                f.flush()
            except Exception:
                pass
            self.n_since_flush = 0
            self.last_flush_ts = self.last_write_ts
        return True

    def _write_mark(self, row):
        """向当前活跃文件写一行 (mark/gap)."""
        f = self._open_active()
        if f is None:
            return
        f.write(json.dumps(row, ensure_ascii=False) + "\n")
        f.flush()

    # ---------- 连接 (x2, idx=0/1) ----------
    async def conn_loop(self, idx):
        attempt = 0
        while True:
            try:
                async with websockets.connect(
                    WS_URL,
                    ping_interval=PING_INTERVAL,
                    ping_timeout=PING_TIMEOUT,
                    open_timeout=20,
                ) as ws:
                    attempt = 0
                    self.alive[idx] = time.time()  # 连接建立即算活
                    my_ver = -1
                    my_slug = None
                    my_gen = self.sub_gen  # 本连接对应的订阅代(换市场/静默重建会 +1)
                    last_any = time.time()
                    while True:
                        # 换市场 或 静默看门狗 → 必须换新连接重建订阅(旧连接追加订阅帧不生效)
                        if my_gen < self.sub_gen:
                            raise ConnectionError("bar_rotate")
                        # 订阅/重订阅: state 换市场后 sub_ver++ → 重发
                        if self.up and self.dn and my_ver != self.sub_ver:
                            sub = {"assets_ids": [self.up, self.dn], "type": "market"}
                            await ws.send(json.dumps(sub))
                            my_ver = self.sub_ver
                            my_slug = self.slug
                            self.alive[idx] = time.time()
                        try:
                            raw = await asyncio.wait_for(
                                ws.recv(), timeout=RECV_TIMEOUT
                            )
                        except asyncio.TimeoutError:
                            # 真死链判定: 长时间无任何帧 → 主动断重连
                            if time.time() - last_any > DOWN_AFTER:
                                raise ConnectionError("无数据 >60s, 判定死链")
                            continue
                        except websockets.ConnectionClosed:
                            raise
                        last_any = time.time()
                        self.alive[idx] = last_any
                        if raw == "PONG":
                            continue
                        try:
                            parsed = json.loads(raw)
                        except Exception:
                            continue
                        msgs = parsed if isinstance(parsed, list) else [parsed]
                        for m in msgs:
                            if not isinstance(m, dict):
                                continue
                            et = m.get("event_type", "")
                            if et not in ("book", "last_trade_price"):
                                continue
                            if my_slug is None:
                                continue
                            self.msg[idx] += 1
                            # 连接侧去重窗口 (滤同源重复, 减队列压力)
                            # 唯一键: book→hash(盘口快照哈希), last_trade→transaction_hash(链上tx)
                            #  (不能用 txhash: last_trade 实际字段是 transaction_hash, 曾漏去重双写)
                            aid = m.get("asset_id", "")
                            h = (
                                m.get("hash")
                                or m.get("transaction_hash")
                                or m.get("txhash")
                                or ""
                            )
                            key = (et, aid, h)
                            if key[2]:
                                now = time.time()
                                if (
                                    key in self.seen
                                    and now - self.seen[key] < DEDUP_WIN
                                ):
                                    self.dup[idx] += 1
                                    continue
                                self.seen[key] = now
                                if len(self.seen) > DEDUP_MAX:
                                    # 空间保护: 丢一半最旧
                                    drop = sorted(self.seen, key=self.seen.get)[
                                        : DEDUP_MAX // 2
                                    ]
                                    for k in drop:
                                        del self.seen[k]
                            if self.q.full():
                                self.n_err += 1  # 极端洪峰兜底: 丢队列最不致命
                                continue
                            await self.q.put((m, my_slug))
            except asyncio.CancelledError:
                raise
            except Exception as e:
                if "bar_rotate" in str(e):
                    self.n_rotate[idx] += 1
                    print(
                        f"  [{self.coin}-{self.label}][c{idx}] 重建订阅(换新连接), "
                        f"{ROTATE_WAIT_S:.0f}s 后重连",
                        flush=True,
                    )
                    wait = ROTATE_WAIT_S
                else:
                    self.disc[idx] += 1
                    attempt += 1
                    wait = min(3 * attempt, 60)
                    print(
                        f"  [{self.coin}-{self.label}][c{idx}] WS 断开({type(e).__name__}) "
                        f"第{self.disc[idx]}次, {wait}s 后重连",
                        flush=True,
                    )
                try:
                    await asyncio.sleep(wait)
                except asyncio.CancelledError:
                    raise

    # ---------- bar 切换 (每 5s) ----------
    async def ticker_loop(self):
        while True:
            try:
                now = time.time()
                bar = int(now // self.cycle) * self.cycle
                slug = f"{self.prefix}-{bar}"
                if slug != self.slug:
                    old = self.slug
                    self.slug = slug
                    self.up = self.dn = None  # 停旧订阅, 等新 token
                    print(
                        f"  [{self.coin}-{self.label}] bar→ {slug} (旧 {old})",
                        flush=True,
                    )
                    tok = await asyncio.to_thread(gamma_tokens, slug)
                    if tok:
                        self.up, self.dn = tok
                        self.sub_ver += 1  # 通知两连接重订阅
                        self.sub_gen += 1  # 换市场 → 必须换新连接重建订阅
                        print(
                            f"  [{self.coin}-{self.label}] 新市场 {slug} token 就绪",
                            flush=True,
                        )
                    else:
                        self.slug = None  # 下轮(5s)重试
                        print(
                            f"  [{self.coin}-{self.label}] {slug} 取 token 失败, 稍后重试",
                            flush=True,
                        )
            except asyncio.CancelledError:
                raise
            except Exception as e:
                print(
                    f"  [{self.coin}-{self.label}] ticker err {type(e).__name__}",
                    flush=True,
                )
            await asyncio.sleep(TICK_POLL_S)

    # ---------- 写盘 + gap (单 task) ----------
    async def writer_loop(self):
        last_hk = 0.0
        wcnt = 0
        while True:
            try:
                item = await asyncio.wait_for(self.q.get(), timeout=5)
            except asyncio.TimeoutError:
                item = None
            except asyncio.CancelledError:
                raise
            now = time.time()
            # bar 切换: self.slug(ticker 维护) 变化 → 同步关旧文件切新 slug
            if self.slug != self.active_slug:
                self._close_active()
                self.active_slug = self.slug
                # 切换瞬间挂着的未闭合 gap 随旧文件一并收尾(不再补 mark)
                self.gap_from = None
                self.gap_slug = None
            if item is not None:
                m, slug = item
                # 只写当前活跃 slug; 切 bar 竞态的旧 slug 尾消息丢弃
                #  (不写旧文件 → 杜绝 reopen/truncate, 保证单句柄顺序完整)
                if slug != self.active_slug:
                    continue
                try:
                    if self._write(m):
                        wcnt += 1
                        if wcnt % 1000 == 0 and self.active_f is not None:
                            self.active_f.flush()
                except Exception as e:
                    self.n_err += 1
                    if self.n_err <= 3 or self.n_err % 50 == 0:
                        print(
                            f"  [{self.coin}-{self.label}] ⚠ 写盘失败 x{self.n_err} "
                            f"({type(e).__name__})",
                            flush=True,
                        )
            # 每 5s: 去重窗口瘦身 + gap 状态机
            if now - last_hk >= 5:
                last_hk = now
                self._housekeep(now)
                self._gap_tick(now)

    def _housekeep(self, now):
        # 去重窗口瘦身: 无条件清超期 key + 超上限丢一半
        for k in list(self.seen):
            if now - self.seen[k] > DEDUP_WIN * 4:
                del self.seen[k]
        if len(self.seen) > DEDUP_MAX:
            drop = sorted(self.seen, key=self.seen.get)[: DEDUP_MAX // 2]
            for k in drop:
                del self.seen[k]

    def _conn_ok(self, now):
        return any(now - a < DOWN_AFTER for a in self.alive)

    def _close_gap(self, now, resolved=True):
        row = {
            "mark": "gap",
            "slug": self.gap_slug or self.active_slug or self.slug,
            "from_ts": round(self.gap_from or now, 2),
            "to_ts": round(now, 2),
            "lost_s": round(now - (self.gap_from or now), 1),
            "note": "reconnected" if resolved else "unresolved",
        }
        self._write_mark(row)
        self.n_gap += 1
        self.gap_from = None
        self.gap_slug = None
        if resolved:
            print(
                f"  [{self.coin}-{self.label}] gap {row['lost_s']}s 已标记(reconnected)",
                flush=True,
            )

    def _gap_tick(self, now):
        """双连接都 down 且市场订阅中 → 计 gap; 任一恢复 → 闭合."""
        in_market = self.up is not None
        ok = self._conn_ok(now)
        if in_market and not ok:
            if self.gap_from is None:
                self.gap_from = now
                self.gap_slug = self.slug
        elif self.gap_from is not None and ok:
            self._close_gap(now, resolved=True)
        # 静默看门狗: 市场订阅中却长时间无写盘 → 订阅失效, 强制换新连接(兜底)
        if (
            in_market
            and self.last_write_ts
            and now - self.last_write_ts > SILENT_ROTATE_AFTER
        ):
            self.last_write_ts = now  # 防每 5s 重复触发
            self.sub_gen += 1
            print(
                f"  [{self.coin}-{self.label}] ⚠ 静默 {SILENT_ROTATE_AFTER:.0f}s 无数据 "
                f"→ 强制重建订阅",
                flush=True,
            )


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", default="l2_data")
    a = ap.parse_args()
    singleton_acquire()
    os.makedirs(a.dir, exist_ok=True)
    print(
        f"L2 recorder v3 (双连接热备) 启动: btc/eth 5m+15m updown | "
        f"输出 {os.path.abspath(a.dir)}",
        flush=True,
    )
    recs = [LineDualRecorder(c, cy, lb, pre, a.dir) for c, cy, lb, pre in MARKETS]
    tasks = []
    for r in recs:
        tasks += [r.conn_loop(0), r.conn_loop(1), r.writer_loop(), r.ticker_loop()]

    async def hb():
        t0 = time.time()
        while True:
            await asyncio.sleep(30)
            parts = []
            for r in recs:
                gap_s = ""
                if r.gap_from is not None:
                    gap_s = f"(gap挂{time.time() - r.gap_from:.0f}s)"
                parts.append(
                    f"{r.coin}-{r.label}:{sum(r.msg)}条"
                    f"丢{sum(r.dup)}断{r.disc}重建{sum(r.n_rotate)}"
                    f"活{sum(1 for a in r.alive if time.time() - a < 60)}/2"
                    f"gap{r.n_gap}{gap_s}"
                )
            print(f"  [hb {time.time() - t0:.0f}s] " + " | ".join(parts), flush=True)

    try:
        await asyncio.gather(*tasks, hb())
    finally:
        for r in recs:
            r._close_active()
        print("退出: 已关闭所有窗口文件", flush=True)


def _term_handler(sig, frame):
    raise KeyboardInterrupt


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, _term_handler)
    signal.signal(signal.SIGINT, _term_handler)
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("停止.", flush=True)
