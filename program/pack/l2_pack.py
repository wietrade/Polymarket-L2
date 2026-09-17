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

小时包（2026-09-17 加，用户要求「按小时补拉，缺几个小时就补拉几个小时的」）
  日包最小粒度是 1 天（≈215MB）⇒ 只缺 2 小时也得搬一整天。小时包粒度 1 小时（≈12MB），
  当天正在长的那一小时会在下一窗口被重打（同名覆盖，本地按 sha256 发现变化后重拉）。
  `python -B tools/l2_pack.py --hourly --hours 72`  ⇒ 最近 72 小时各一个 `l2_<date>T<HH>.tar.gz`
  两套包**并存**：日包继续负责历史全量补拉，小时包负责窗口内增量。
  包内顶层仍是 `<date>/`（与日包一致）⇒ 本地解压是**合并**到同一棵树，不会分叉。
  小时口径 = **UTC**（与 `l2_data/<date>/` 日目录同口径）；小时取自文件名里的 bar epoch，
  不取 mtime —— bar 的 epoch 是口径字段，mtime 只是写盘时间。
"""

from __future__ import annotations

import argparse
import hashlib
import os
import re
import tarfile
import time

L2 = "/www/wwwroot/polymarket-l2/l2_data"
PACKS = os.path.join(L2, "packs")


def set_root(root: str) -> None:
    """把数据根改成 root（默认是 43 上的 l2_data）。

    存在的理由 = **可测**：打包逻辑必须能在本机临时目录上离线跑通再上生产机，
    否则每次改动都等于拿采集机当试验场（这台机在跑唯一一份 L2 采集）。
    """
    global L2, PACKS
    L2 = root
    PACKS = os.path.join(root, "packs")


# 实测（2026-09-17）：43 上 7347 个数据文件名 **100% 符合**本模式 ⇒ 解析失败就是真异常，
# 必须计数报出来，不能静默丢（丢一个文件 = 本地永远缺那根 bar，且不报错）。
BAR_RE = re.compile(r"^(?:btc|eth)-updown-(?:5m|15m)-(\d+)\.jsonl\.gz$")


def sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _newest_mtime(paths: list[str]) -> float:
    newest = 0.0
    for p in paths:
        try:
            newest = max(newest, os.path.getmtime(p))
        except OSError:
            pass
    return newest


def write_pack(
    out: str, names: list[str], force: bool, label: str, quiet_skip: bool = False
) -> str:
    """把 <L2>/<name> 逐个打进 out（包内路径 = names，顶层仍是 <date>/）。

    幂等：**只要有任何源文件比包新就重打**（同名覆盖）。这一条同时解决两件事：
      ① 当天/当前小时的包还在长 ⇒ 下次重打；
      ② 被中断的包（只打了一半）mtime 会是新的、不会被误当“已最新”而卡住。

    返回 'empty' / 'skip' / 'made'。**逐条打印交给调用方**：日包就十几天，逐条打很清楚；
    小时包每次跑 72 条、且母本里的小时包不清理、随历史无限增长
    —— 逐条打会变成跑一年每次几千行（本仓最怕的“日志只增不删”形态）。
    """
    if not names:
        return "empty"
    srcs = [os.path.join(L2, n) for n in names]
    if os.path.exists(out) and not force:
        if os.path.getmtime(out) >= _newest_mtime(srcs):
            if not quiet_skip:
                print(f"  跳过 {label}（包已是最新，{os.path.getsize(out) / 1e6:.1f}MB）")
            return "skip"
    t0 = time.time()
    with tarfile.open(out, "w:gz", compresslevel=6) as tf:
        for p, n in zip(srcs, names):
            if os.path.exists(p):
                tf.add(p, arcname=n)
    digest = sha256(out)
    with open(out + ".sha256", "w") as f:
        # 第三个字段 = 包内文件数。给本地补拉器做「解压后文件数对不对」的对账，
        # 否则“解压一半”这种状态只能靠人眼看（2026-09-17 踩到）。旧文件只有两字段 ⇒ 兼容。
        f.write(f"{digest}  {os.path.basename(out)}  {len(names)}\n")
    print(
        f"  ✓ {label}: {len(names)} 文件 → {os.path.getsize(out) / 1e6:.1f}MB  "
        f"({time.time() - t0:.1f}s)  sha256={digest[:16]}…"
    )
    return "made"


def hour_of(fname: str) -> tuple[str, int] | None:
    """文件名 → (UTC 小时键 `YYYY-MM-DDTHH`, bar epoch)。不符合命名模式则 None。"""
    m = BAR_RE.match(fname)
    if not m:
        return None
    ts = int(m.group(1))
    return time.strftime("%Y-%m-%dT%H", time.gmtime(ts)), ts


def pack_hours(hours: int, force: bool, only_date: str = "") -> None:
    """按小时打包（只打窗口内的）。hours<=0 表示全部历史。"""
    cutoff = 0.0 if hours <= 0 else time.time() - hours * 3600
    groups: dict[str, list[str]] = {}
    unknown: list[str] = []
    stray: list[str] = []
    for d in sorted(
        x
        for x in os.listdir(L2)
        if os.path.isdir(os.path.join(L2, x)) and x[:4].isdigit()
    ):
        if only_date and d != only_date:
            continue
        for fn in sorted(os.listdir(os.path.join(L2, d))):
            if not fn.endswith(".jsonl.gz"):
                continue
            hk = hour_of(fn)
            if hk is None:
                unknown.append(f"{d}/{fn}")
                continue
            hkey, ts = hk
            if hkey[:10] != d:
                # 文件名里的 bar 日期与所在日目录不符 ⇒ 不猜是哪边的错，单独报出来
                stray.append(f"{d}/{fn}←{hkey}")
                continue
            if ts < cutoff:
                continue
            groups.setdefault(hkey, []).append(f"{d}/{fn}")
    print(
        f"# 小时包：窗口 最近 {hours} 小时（UTC）⇒ {len(groups)} 个小时有数据"
        if hours > 0
        else f"# 小时包：全历史 ⇒ {len(groups)} 个小时有数据"
    )
    made = 0
    kept = 0
    for hkey in sorted(groups):
        out = os.path.join(PACKS, f"l2_{hkey}.tar.gz")
        st = write_pack(out, groups[hkey], force, hkey, quiet_skip=True)
        made += 1 if st == "made" else 0
        kept += 1 if st == "skip" else 0
    print(f"# 小时包：本次重打 {made} 个 / 已最新 {kept} 个（已最新的不逐条打，防日志膨胀）")
    if unknown:
        print(f"  ！{len(unknown)} 个文件名不符合命名模式，未入任何包：{unknown[:3]}")
    if stray:
        print(f"  ！{len(stray)} 个文件的 bar 日期与日目录不符，未入包：{stray[:3]}")


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
        f.write(f"{digest}  l2_{date}.tar.gz  {n}\n")
    print(
        f"  ✓ {date}: {n} 文件 → {os.path.getsize(out) / 1e6:.1f}MB  "
        f"({time.time() - t0:.1f}s)  sha256={digest[:16]}…"
    )


def selftest() -> int:
    """离线自检（临时目录，不碰 /www/wwwroot）：命名即断言、失败退出码非 0。

    时间夹具一律**相对 now** 生成（除了一个手算过的字面量），否则自检会在某一天撞车：
    第一版把"当前小时"写成"旧的固定日期"，当天跑就会与历史小时同名 ⇒ 期望值随日期而变。
    """
    import contextlib
    import gzip
    import io
    import shutil
    import tempfile

    fails: list[str] = []

    def chk(name: str, cond: bool) -> None:
        print(("  OK   " if cond else "  FAIL ") + name)
        if not cond:
            fails.append(name)

    def hk(ts: int) -> str:
        return time.strftime("%Y-%m-%dT%H", time.gmtime(ts))

    def mkfile(root: str, ts: int, coin: str = "btc", cycle: str = "5m") -> str:
        d = time.strftime("%Y-%m-%d", time.gmtime(ts))
        os.makedirs(os.path.join(root, d), exist_ok=True)
        p = os.path.join(root, d, f"{coin}-updown-{cycle}-{ts}.jsonl.gz")
        with gzip.open(p, "wb") as f:
            f.write(b"")
        return p

    print("[1] hour_of 解析")
    # 1789529400 = 2026-09-16 03:30:00Z（手算并实测过；写成字面量才真的在验时区口径）
    chk(
        "bar epoch -> UTC 小时键（1789529400 -> 2026-09-16T03）",
        hour_of("btc-updown-5m-1789529400.jsonl.gz") == ("2026-09-16T03", 1789529400),
    )
    chk(
        "15m 同样能解",
        hour_of("eth-updown-15m-1789529400.jsonl.gz") == ("2026-09-16T03", 1789529400),
    )
    chk("不规范名 -> None（不猜）", hour_of("weird-name.jsonl.gz") is None)
    chk("非本币种 -> None", hour_of("sol-updown-5m-1789529400.jsonl.gz") is None)
    chk("缺 .gz -> None", hour_of("btc-updown-5m-1789529400.jsonl") is None)

    tmp = tempfile.mkdtemp(prefix="l2pack-st") + "/l2_data"
    try:
        set_root(tmp)
        os.makedirs(PACKS, exist_ok=True)
        now = int(time.time())
        cur = now - (now % 3600)  # 当前 UTC 小时起点
        old = cur - 3 * 86400  # 3 天前的同一小时（与"当前小时"必不同名）
        a1 = mkfile(tmp, old)  # 旧小时 第 1 个文件
        a2 = mkfile(tmp, old + 60, coin="eth")  # 旧小时 第 2 个文件（同小时）
        b1 = mkfile(tmp, old + 3600)  # 旧小时 + 1
        r1 = mkfile(tmp, cur)  # 当前小时（窗口测试用）
        d_old = time.strftime("%Y-%m-%d", time.gmtime(old))
        d_cur = time.strftime("%Y-%m-%d", time.gmtime(cur))
        with gzip.open(os.path.join(tmp, d_old, "weird-name.jsonl.gz"), "wb") as f:
            f.write(b"")
        # bar 日期与所在日目录不符：放在 d_old 里、名字却是 10 天后
        with gzip.open(
            os.path.join(tmp, d_old, "btc-updown-5m-%d.jsonl.gz" % (old + 10 * 86400)),
            "wb",
        ) as f:
            f.write(b"")

        P_OLD = "l2_%s.tar.gz" % hk(old)
        P_OLD1 = "l2_%s.tar.gz" % hk(old + 3600)
        P_CUR = "l2_%s.tar.gz" % hk(cur)

        print("[2] 分组与包内容")
        pack_hours(0, force=True)
        packs = sorted(
            x for x in os.listdir(PACKS) if x.endswith(".tar.gz") and "T" in x
        )
        chk(
            "同小时两文件合成一个包、共 3 个小时包",
            packs == sorted([P_OLD, P_OLD1, P_CUR]),
        )
        with tarfile.open(os.path.join(PACKS, P_OLD), "r:gz") as tf:
            names = sorted(tf.getnames())
        chk(
            "包内顶层是 <date>/（能与日包合并解压到同一棵树）",
            names
            == [
                "%s/btc-updown-5m-%d.jsonl.gz" % (d_old, old),
                "%s/eth-updown-5m-%d.jsonl.gz" % (d_old, old + 60),
            ],
        )
        chk(
            ".sha256 副文件生成且与包内容一致",
            open(os.path.join(PACKS, P_OLD + ".sha256")).read().split()[0]
            == sha256(os.path.join(PACKS, P_OLD)),
        )
        chk(
            "不规范名/日期不符的文件都没进包",
            all("weird" not in n and str(old + 10 * 86400) not in n for n in names),
        )
        chk(
            "该小时目录里确实存在这两个异常文件（断言不是在真空里过）",
            os.path.exists(os.path.join(tmp, d_old, "weird-name.jsonl.gz"))
            and os.path.exists(
                os.path.join(
                    tmp, d_old, "btc-updown-5m-%d.jsonl.gz" % (old + 10 * 86400)
                )
            ),
        )

        print("[3] 幂等与重打")
        m0 = os.path.getmtime(os.path.join(PACKS, P_OLD))
        m0b = os.path.getmtime(os.path.join(PACKS, P_OLD1))
        pack_hours(0, force=False)
        chk(
            "源未变 -> 不重打（mtime 不动）",
            os.path.getmtime(os.path.join(PACKS, P_OLD)) == m0,
        )
        os.utime(a1, (time.time() + 5, time.time() + 5))
        pack_hours(0, force=False)
        chk("源变新 -> 该小时重打", os.path.getmtime(os.path.join(PACKS, P_OLD)) != m0)
        chk(
            "其他小时不受牵连（mtime 逐字不变）",
            os.path.getmtime(os.path.join(PACKS, P_OLD1)) == m0b,
        )
        chk(
            "重打后包内容仍正确（不会只剩被 touch 的那个文件）",
            len(tarfile.open(os.path.join(PACKS, P_OLD), "r:gz").getnames()) == 2,
        )

        print("[4] 窗口过滤")
        for fn in os.listdir(PACKS):
            os.remove(os.path.join(PACKS, fn))
        pack_hours(1, force=True)
        left = sorted(x for x in os.listdir(PACKS) if x.endswith(".tar.gz"))
        chk("hours=1 -> 只打当前小时（3 天前的历史小时一个都不打）", left == [P_CUR])
        pack_hours(0, force=True)
        chk(
            "hours=0 -> 全历史都打",
            sorted(x for x in os.listdir(PACKS) if x.endswith(".tar.gz"))
            == sorted([P_OLD, P_OLD1, P_CUR]),
        )
        chk(
            "当前小时的文件确实在盘上（窗口测试不是在真空里过）",
            os.path.exists(r1)
            and os.path.basename(r1)
            in tarfile.open(os.path.join(PACKS, P_CUR), "r:gz").getnames()[0],
        )

        print("[5] 日志量（防膨胀）")
        # 母本里的小时包不清理、随历史天增长：已最新的必须**不逐条打**，
        # 否则跑一年就是每次几千行（cron 日志/邮件会被冲垮）。
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            pack_hours(0, force=False)  # 全部已最新
        out = buf.getvalue()
        chk("已最新的小时包不逐条打印（只一行汇总）", "跳过 2026" not in out and "已最新" in out)
        buf2 = io.StringIO()
        with contextlib.redirect_stdout(buf2):
            pack_hours(0, force=True)  # 强制重打
        out2 = buf2.getvalue()
        chk("真正重打时才逐条打（不会把该看的也隐掉）", out2.count("  ✓ ") == 3)
    finally:
        shutil.rmtree(os.path.dirname(tmp), ignore_errors=True)

    print()
    if fails:
        print("自检失败 %d 项：%s" % (len(fails), "; ".join(fails)))
        return 1
    print("自检全部通过")
    return 0


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default="")
    ap.add_argument("--force", action="store_true")
    ap.add_argument(
        "--root", default=L2, help="数据根（默认 43 的 l2_data；测试可指向临时目录）"
    )
    ap.add_argument(
        "--selftest", action="store_true", help="离线自检（临时目录，不碰生产数据）"
    )
    ap.add_argument(
        "--hourly",
        action="store_true",
        help="打小时包 l2_<date>T<HH>.tar.gz（供「缺几小时补几小时」）",
    )
    ap.add_argument(
        "--hours",
        type=int,
        default=72,
        help="小时包只打最近 N 小时（0=全部历史；默认 72）",
    )
    a = ap.parse_args()
    if a.selftest:
        raise SystemExit(selftest())
    set_root(a.root)
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
    if a.hourly:
        pack_hours(a.hours, a.force, only_date=a.date)
    print("### 现有包:")
    tot = 0
    nh = 0
    nd = 0
    hbytes = 0
    for fn in sorted(os.listdir(PACKS)):
        if fn.endswith(".sha256"):
            continue
        p = os.path.join(PACKS, fn)
        sz = os.path.getsize(p)
        tot += sz
        if fn.startswith("l2_2") and "T" in fn:
            nh += 1
            hbytes += sz
            continue  # 小时包只汇总：母本不清理，逐条列会随历史无限增长
        nd += 1 if fn.startswith("l2_2") else 0
        print(
            f"  {fn:<28} {sz / 1e6:8.1f}MB  {time.strftime('%m-%d %H:%M', time.localtime(os.path.getmtime(p)))}"
        )
    if nh:
        print(f"  [小时包 {nh} 个，共 {hbytes / 1e6:.1f}MB，不逐条列出]")
    print(f"  合计 {tot / 1e6:.1f}MB（日包 {nd} 个 / 小时包 {nh} 个；手工包不计数）")


if __name__ == "__main__":
    main()
