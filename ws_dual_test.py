"""ws_dual_test.py — 一次性实验: 同 IP 双 WS 连接订阅同一 slug, 验证:
1) 服务器是否允许同 IP 多连接 (有无限制/互踢)
2) 两条流收到的 book/last_trade 是否能交叉补缺 (去重可行性)
用法(43): /www/wwwroot/polymarket/venv/bin/python -u ws_dual_test.py [秒数]
"""

from __future__ import annotations

import asyncio
import json
import sys
import time
import urllib.request

import websockets

UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
RUN_S = int(sys.argv[1]) if len(sys.argv) > 1 else 150


def gamma_tokens(slug):
    req = urllib.request.Request(
        "https://gamma-api.polymarket.com/markets/slug/" + slug, headers=UA
    )
    d = json.loads(urllib.request.urlopen(req, timeout=10).read())
    if isinstance(d, list) and d:
        d = d[0]
    tk = d.get("clobTokenIds")
    tk = json.loads(tk) if isinstance(tk, str) else tk
    if not tk or len(tk) < 2:
        return ()
    return tuple(tk[:2])


async def one(wsid, toks, stats, seen):
    try:
        async with websockets.connect(
            WS_URL, ping_interval=10, ping_timeout=8, open_timeout=20
        ) as ws:
            await ws.send(json.dumps({"assets_ids": list(toks), "type": "market"}))
            t0 = time.time()
            stats["open_at"] = t0
            while time.time() - t0 < RUN_S:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=15)
                except asyncio.TimeoutError:
                    stats["timeout"] = stats.get("timeout", 0) + 1
                    continue
                try:
                    parsed = json.loads(raw)
                except Exception:
                    stats["nonjson"] = stats.get("nonjson", 0) + 1
                    continue
                msgs = parsed if isinstance(parsed, list) else [parsed]
                for m in msgs:
                    et = m.get("event_type", "")
                    aid = m.get("asset_id", "")
                    if et in ("book", "last_trade_price") and aid in toks:
                        stats["msg"] = stats.get("msg", 0) + 1
                        h = m.get("hash") or m.get("txhash")
                        seen.add((et, aid, h))
    except Exception as e:
        stats["disc"] = stats.get("disc", 0) + 1
        print(f"ws{wsid} 断开 {type(e).__name__}: {e}", flush=True)
        # 断后不重连, 记录即可(实验目的是看连接寿命)
        stats["closed_at"] = time.time()


async def main():
    bar = int(time.time() // 300) * 300
    slug = f"btc-updown-5m-{bar}"
    toks = gamma_tokens(slug)
    if not toks:
        print(f"取 token 失败 slug={slug}", flush=True)
        return
    print(f"slug={slug} tokens={list(toks)} 双连接对跑 {RUN_S}s", flush=True)
    sA, sB = {}, {}
    vA, vB = set(), set()
    await asyncio.gather(one("A", toks, sA, vA), one("B", toks, sB, vB))

    def life(s):
        if "open_at" in s and "closed_at" in s:
            return f"活 {s['closed_at'] - s['open_at']:.0f}s 后断开"
        return "全程未断"

    print("\n=== 结果 ===", flush=True)
    print(
        f"A: msg={sA.get('msg', 0)} 唯一key={len(vA)} 断开={sA.get('disc', 0)} {life(sA)}",
        flush=True,
    )
    print(
        f"B: msg={sB.get('msg', 0)} 唯一key={len(vB)} 断开={sB.get('disc', 0)} {life(sB)}",
        flush=True,
    )
    inter = len(vA & vB)
    union = len(vA | vB)
    print(f"并集={union} 交集={inter} A∩B覆盖={inter / max(union, 1):.1%}", flush=True)
    print(f"A独有={len(vA - vB)} B独有={len(vB - vA)}", flush=True)


asyncio.run(main())
