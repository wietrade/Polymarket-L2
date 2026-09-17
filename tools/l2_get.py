#!/usr/bin/env python3
"""l2_get.py —— 本地 L2 数据补拉器（只下缺的 / 断点续传 / 校验后才解压）。

背景（用户 2026-09-10 要求）：
  L2 原始数据在 43 上按天打包成 `l2_<date>.tar.gz`（+ `.sha256`）放在 8001 的 `/_dl/`，
  支持 HTTP Range 断点续传。此前本地没有任何包、也没有清单，只能靠人工对账。

关键约束（为什么必须比 sha256，而不能只看"文件在不在"）：
  **同一天的包会随数据增长被同名重打**（例如 09-10 的包在 19:32 生成、只覆盖到 19:30，
  之后的完整版会把它同名覆盖）⇒ 只判存在会永久漏掉后半天的数据 ⇒ 一律按 sha256 判定。
  ⚠️ **别再假设"每天自动重打"**：2026-09-12 复核发现 43 的 crontab 里已没有那条定时任务
  （现在靠手工跑 `l2_pack_publish.sh`）⇒ 当天包何时更新取决于人，**按 sha256 反复对账、并按需重跑**。

**解压一律是「合并」**（2026-09-12 修）：早期版本在「同名目录已存在」时直接当作"已解压"返回，
  于是重下后的新增文件永远不落地（实测：09-10 少 144 个、09-08 少 91 个，且不报错）。
  tar 解压本身幂等 ⇒ 直接覆盖式合并：既补新文件、也刷新被追加写过的老文件。

用法：
  python tools/l2_get.py --list                    # 只列清单与本地状态，不下载
  python tools/l2_get.py                           # 补拉所有**已完成**的包并解压
  python tools/l2_get.py --jobs 24                 # 加大并发（实测这条路由单连 接被限速）
  python tools/l2_get.py --include-today           # 连还在长的包也拉（当前小时/当天，慎用）
  python tools/l2_get.py --hours 24                # 只看最近 24 小时的包
  python tools/l2_get.py --mode day --days 3       # 只拉日包（历史全量补拉）
  python tools/l2_get.py --no-extract              # 只下压缩包，不解压
  python tools/l2_get.py --dir I:/plot/l2/data     # 指定数据根目录（默认 <项目 根>/data）

粒度：谁在清单里就拉谁（2026-09-17 改成按小时）
  服务端现在同时发布 **小时包** `l2_<date>T<HH>.tar.gz`（≈12MB）与 **日包**
  `l2_<date>.tar.gz`（≈215MB）。默认 `--mode auto`：清单里**有小时包就用小时包**。
  ⇒ 只缺 3 小时就只下 3 个小时包，不再为了 3 小时搬一整天。
  小时口径 = **UTC**（与 `data/<date>/` 日目录同口径）；包内顶层仍是 `<date>/`
  ⇒ 解压是**合并**到同一棵树，两套包不会分叉。
  判「要不要拉」一律按 **sha256**：同名包会被重打（当前小时/当天还在长），
  只看“文件在不在”会永久漏掉后半段（2026-09-12 实测：09-10 少 144 个文件）。

目录布局（数据与代码分离，数据不入口 git 仓库）：
  <root>/packs/l2_<date>.tar.gz        下载的包（+ .sha256）
  <root>/<date>/...                    解压后的 jsonl.gz（tar 内顶层就是 <date>/）
  <root>/manifest.json                 服务端清单副本（便于离线对账）
"""

from __future__ import annotations

import argparse
import calendar
import datetime as dt
import hashlib
import json
import re
import shutil
import subprocess
import tarfile
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

DEFAULT_BASE = "http://43.165.167.132:8001"
UA = {"User-Agent": "l2_get/1.0", "Accept": "*/*"}
CHUNK = 1 << 20


def log(msg: str) -> None:
    print(msg, flush=True)


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for blk in iter(lambda: f.read(CHUNK), b""):
            h.update(blk)
    return h.hexdigest()


def fetch_manifest(base: str, timeout: float = 30.0) -> dict:
    req = urllib.request.Request(f"{base}/dl/manifest.json", headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


def remote_size(base: str, name: str, timeout: float = 20.0) -> int | None:
    """拿远端文件大小（用 Range 取 0-0，读 Content-Range 的总长）。"""
    req = urllib.request.Request(
        f"{base}/dl/{name}", headers={**UA, "Range": "bytes=0-0"}
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            cr = r.headers.get("Content-Range", "")
            if "/" in cr:
                return int(cr.rsplit("/", 1)[1])
            return int(r.headers.get("Content-Length") or 0) or None
    except Exception:
        return None


def _get_range(
    base: str, name: str, s: int, e: int, dest: Path, tries: int = 3
) -> bool:
    """下载一个字节区间 → dest（已存在且长度正确则视为已完成）。带重试。"""
    want = e - s + 1
    if dest.exists() and dest.stat().st_size == want:
        return True
    req = urllib.request.Request(
        f"{base}/dl/{name}", headers={**UA, "Range": f"bytes={s}-{e}"}
    )
    for k in range(tries):
        try:
            with urllib.request.urlopen(req, timeout=120) as r, dest.open("wb") as f:
                while True:
                    blk = r.read(CHUNK)
                    if not blk:
                        break
                    f.write(blk)
            if dest.stat().st_size == want:
                return True
        except Exception:
            time.sleep(1.0 + k)
    return False


def _download_single(base: str, name: str, dest: Path) -> bool:
    """单连接下载（curl -C - 续传）。留给 --jobs 1 或服务端不支持 Range 的兜底。"""
    part = dest.with_suffix(dest.suffix + ".part")
    if part.exists():
        shutil.move(str(part), str(dest))  # 上次的临时文件当作续传起点
    curl = shutil.which("curl")
    if not curl:
        log("    ！找不到 curl（Windows 10+ 自带 curl.exe）")
        return False
    cmd = [curl, "-sS", "-L", "--fail", "-C", "-", "-o", str(part), f"{base}/dl/{name}"]
    rc = subprocess.call(cmd)
    if rc != 0 or not part.exists():
        log(f"    ！单连接下载失败（curl rc={rc}）")
        return False
    shutil.move(str(part), str(dest))
    return True


def download(
    base: str, name: str, dest: Path, jobs: int = 12, chunk_mb: int = 4
) -> bool:
    """分片**并发**下载 → dest（已存在则不重复拉）。

    2026-09-10 实测：43 本机从 8001 下载 **323 MB/s**、服务器到国际 **1.09 MB/s**，
    而本机到 Cloudflare 也有 **482 KB/s**，但「43↔本机」**单连接只有 9~30 KB/s**
    （8 并发→93 KB/s、24 并发→173 KB/s）⇒ 用并发分片绕开这条路由的单连接限速。
    分片落在 `.parts/<包名>/<序号>`，长度对得上即视为已完成 ⇒ 中断后重跑自动续传。
    """
    stale = dest.with_suffix(dest.suffix + ".part")
    if stale.exists():
        stale.unlink(missing_ok=True)  # 旧的单连接临时文件（已无用）
    total = remote_size(base, name)
    if not total:
        log("    ！取不到远端大小，退回单连接下载")
        return _download_single(base, name, dest)
    if jobs <= 1:
        return _download_single(base, name, dest)
    parts_dir = dest.parent / ".parts" / name
    parts_dir.mkdir(parents=True, exist_ok=True)
    step = max(1, chunk_mb) * 1048576
    spans = [(s, min(s + step - 1, total - 1)) for s in range(0, total, step)]
    todo = [
        (i, s, e)
        for i, (s, e) in enumerate(spans)
        if not (
            (parts_dir / f"{i:05d}").exists()
            and (parts_dir / f"{i:05d}").stat().st_size == e - s + 1
        )
    ]
    if len(todo) < len(spans):
        done_mb = (len(spans) - len(todo)) * step / 1048576
        log(
            f"    续传：已完成 {len(spans) - len(todo)}/{len(spans)} 片（约 {done_mb:.1f} MB），"
            f"并发 {jobs}"
        )
    else:
        log(f"    {len(spans)} 片 × {chunk_mb} MB，并发 {jobs}")
    ok = 0
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=max(1, jobs)) as ex:
        futs = {
            ex.submit(_get_range, base, name, s, e, parts_dir / f"{i:05d}"): (
                i,
                e - s + 1,
            )
            for i, s, e in todo
        }
        for fu in as_completed(futs):
            i, ln = futs[fu]
            if fu.result():
                ok += 1
                if ok % 10 == 0 or ok == len(todo):
                    sp = ok * ln / max(0.001, time.time() - t0) / 1024
                    log(f"    {ok}/{len(todo)} 片  {sp:.0f} KB/s")
    if ok != len(todo):
        log(
            f"    ！{len(todo) - ok}/{len(todo)} 片失败（已完成的分片保留，重跑会续传）"
        )
        return False
    tmp = dest.with_suffix(dest.suffix + ".merging")
    with tmp.open("wb") as out:
        for i in range(len(spans)):
            with (parts_dir / f"{i:05d}").open("rb") as f:
                shutil.copyfileobj(f, out, CHUNK)
    shutil.move(str(tmp), str(dest))
    shutil.rmtree(parts_dir, ignore_errors=True)
    return True


def extract(pkg: Path, root: Path) -> bool:
    """把包**合并**解压到 <root>/（tar 内顶层就是 <date>/）。

    为什么必须合并，而不能「同名目录已存在就跳过」（2026-09-12 修复）：
      同一天的包会随数据增长被**同名重打**（当天包最后一根 bar 还在写、前一天会被补包）。
      sha256 变了会重新下载，但若解压时因「目录已存在」直接返回，
      **新增的那部分文件永远不会落地、而且完全不报错**。
      实测代价：09-10 因此少 144 个文件、09-08 少 91 个（都只能人工发现）。
      tar 解压本身是幂等的覆盖写 ⇒ 直接合并解压：既补新文件、也刷新被追加写过的老文件。
    """
    try:
        with tarfile.open(pkg, "r:gz") as tf:
            members = tf.getmembers()  # 读一遍成员表（缓存，后面 extractall 不再重读）
            if not members:
                return False
            new = sum(1 for m in members if m.isfile() and not (root / m.name).exists())
            nfile = sum(1 for m in members if m.isfile())
            old = nfile - new
            try:
                # filter="data"：只解普通文件/目录，挡绝对路径与符号链接
                # （Python 3.12+ 支持；3.14 起为默认，不传会伐 DeprecationWarning）
                tf.extractall(root, filter="data")
            except TypeError:  # 老版本无 filter 参数
                tf.extractall(root)
            log(f"    合并解压 {nfile} 个文件（新增 {new} / 覆盖刷新 {old}）")
        return True
    except Exception as e:  # 损坏的包不该静默
        log(f"    ！解压失败 {pkg.name}: {type(e).__name__} {e}")
        return False


def hour_of_file(fname: str) -> str:
    """文件名 → UTC 小时键（与 43 上 `l2_pack.py` 的 `hour_of` 同一口径）。"""
    m = re.match(r"^(?:btc|eth)-updown-(?:5m|15m)-(\d+)\.jsonl\.gz$", fname)
    if not m:
        return ""
    return time.strftime("%Y-%m-%dT%H", time.gmtime(int(m.group(1))))


def parse_pack(name: str) -> dict | None:
    """包名 → 描述（`hour` / `day`）。认不出来返回 None（**报出来，不静默丢**）。

    小时口径 = UTC，与 `data/<date>/` 日目录同口径。包内顶层始终是 `<date>/`，
    所以两套包解压到同一棵树是**合并**、不会分叉。
    """
    mh = re.match(r"^l2_(\d{4}-\d{2}-\d{2})T(\d{2})\.tar\.gz$", name)
    if mh:
        day, hh = mh.group(1), mh.group(2)
        try:
            # 用 calendar.timegm（UTC 语义），不用 time.mktime（本地时区，会差 8 小时）
            start = calendar.timegm(time.strptime(day + hh, "%Y-%m-%d%H"))
        except ValueError:
            start = 0
        return {"kind": "hour", "day": day, "hkey": day + "T" + hh, "start": start}
    md = re.match(r"^l2_(\d{4}-\d{2}-\d{2})\.tar\.gz$", name)
    if md:
        return {"kind": "day", "day": md.group(1), "hkey": "", "start": 0}
    return None


def local_files_for(root: Path, info: dict) -> int:
    """本地已落地的、属于该包的文件数（用于与清单里的 `nfiles` 对账）。

    为什么要有这个数：解压是**合并**的、失败只打一行日志 ⇒
    “包里该有 12 个文件、本地只落了 9 个”这种半截状态必须能自己发现
    （2026-09-17 就是被这种半截状态骗过：本地 09-16 的原始文件被采集器截断）。
    """
    d = root / info["day"]
    if not d.is_dir():
        return 0
    if info["kind"] == "day":
        return sum(1 for p in d.iterdir() if p.is_file())
    return sum(
        1 for p in d.iterdir() if p.is_file() and hour_of_file(p.name) == info["hkey"]
    )


def select_packs(
    parsed: list[dict],
    mode: str,
    hours: int,
    days: int,
    include_unfinished: bool,
    now: int,
    today: str,
) -> tuple[list[dict], list[str], list[str]]:
    """从清单里选出**这次要考虑的包**（纯函数，不碰盘 ⇒ 可离线自检）。

    三道筛：① 粒度（mode）② 窗口（hours/days）③ 服务端说“还没长完”的跳过。
    返回 (候选, 跳过的未完成包名, 因窗口被排除的包名)——排除项也要能看到，
    否则“为什么没拉这个小时”只能猜。
    """
    nh = [x for x in parsed if x["kind"] == "hour"]
    nd = [x for x in parsed if x["kind"] == "day"]
    if mode == "auto":
        mode = "hour" if nh else "day"
    pool = {"hour": nh, "day": nd, "all": parsed}[mode]
    cut_day = (
        (dt.date.fromisoformat(today) - dt.timedelta(days=days - 1)).isoformat()
        if days
        else ""
    )
    cands, unfinished, windowed = [], [], []
    for x in pool:
        name = str(x["entry"].get("name") or "")
        if x["kind"] == "hour":
            if hours and x["start"] and x["start"] < now - hours * 3600:
                windowed.append(name)
                continue
        elif cut_day and x["day"] < cut_day:
            windowed.append(name)
            continue
        # `final` 由服务端清单给；**老清单没这个字段时退回到旧规则**
        # （日包的当天包仍然跳过），否则会㿝成“当天包每次重拉 215MB”。
        fin = x["entry"].get("final")
        if fin is None:
            fin = not (x["kind"] == "day" and x["day"] == today)
        if not fin and not include_unfinished:
            unfinished.append(name)
            continue
        cands.append(x)
    return cands, unfinished, windowed


def selftest() -> int:
    """离线自检（不联网、不碰生产数据）：命名即断言、失败退出码非 0。"""
    import tempfile

    fails: list[str] = []

    def chk(name: str, cond: bool) -> None:
        print(("  OK   " if cond else "  FAIL ") + name)
        if not cond:
            fails.append(name)

    print("[1] 包名解析")
    h = parse_pack("l2_2026-09-16T04.tar.gz")
    chk(
        "小时包解析出 kind/day/hkey",
        h is not None and h["kind"] == "hour" and h["hkey"] == "2026-09-16T04",
    )
    # 1789529400 = 2026-09-16 03:30Z（手算+实测过）；小时起点 = 1789527600
    chk(
        "小时起点是 UTC 口径（1789527600），不是本地时区的差 8 小时",
        parse_pack("l2_2026-09-16T03.tar.gz")["start"] == 1789527600,
    )
    d = parse_pack("l2_2026-09-16.tar.gz")
    chk(
        "日包解析出 kind=day",
        d is not None and d["kind"] == "day" and d["day"] == "2026-09-16",
    )
    chk(
        "手工包/怪名 -> None（不静默当包处理）",
        parse_pack("l2_full.tar.gz") is None
        and parse_pack("l2_2026-09-16T4.tar.gz") is None,
    )

    print("[2] 文件名 -> 小时键")
    chk("5m 文件", hour_of_file("btc-updown-5m-1789529400.jsonl.gz") == "2026-09-16T03")
    chk(
        "15m 文件同一口径",
        hour_of_file("eth-updown-15m-1789529400.jsonl.gz") == "2026-09-16T03",
    )
    chk("不规范名 -> 空串（不计入任何小时）", hour_of_file("weird.jsonl.gz") == "")

    print("[3] 粒度/窗口/未完成 三道筛")

    def ent(name, final=True, sha="a"):
        return {
            "name": name,
            "sha256": sha,
            "final": final,
            "size": 1000,
            "kind": "hour" if "T" in name else "day",
        }

    now = 1789529400  # 2026-09-16T03:30Z
    parsed = [
        dict(parse_pack(n), entry=ent(n))
        for n in (
            "l2_2026-09-16T03.tar.gz",
            "l2_2026-09-16T04.tar.gz",
            "l2_2026-09-15T23.tar.gz",
            "l2_2026-09-16.tar.gz",
            "l2_2026-09-15.tar.gz",
        )
    ]
    c, u, w = select_packs(parsed, "auto", 0, 0, False, now, "2026-09-16")
    chk(
        "auto + 清单里有小时包 -> 只考虑小时包（3 个），日包不捎带",
        len(c) == 3 and all(x["kind"] == "hour" for x in c),
    )
    c, u, w = select_packs(parsed, "auto", 2, 0, False, now, "2026-09-16")
    chk(
        "--hours 2 -> 只留最近 2 小时（04 与 03），23 那个被窗口排除且被记名",
        sorted(x["hkey"] for x in c) == ["2026-09-16T03", "2026-09-16T04"]
        and w == ["l2_2026-09-15T23.tar.gz"],
    )
    c, u, w = select_packs(parsed, "day", 0, 0, False, now, "2026-09-16")
    chk(
        "mode=day -> 只看日包（2 个）",
        len(c) == 2 and all(x["kind"] == "day" for x in c),
    )
    parsed2 = [
        dict(
            parse_pack("l2_2026-09-16T04.tar.gz"),
            entry=ent("l2_2026-09-16T04.tar.gz", final=False),
        ),
        dict(
            parse_pack("l2_2026-09-16T03.tar.gz"), entry=ent("l2_2026-09-16T03.tar.gz")
        ),
    ]
    c, u, w = select_packs(parsed2, "hour", 0, 0, False, now, "2026-09-16")
    chk(
        "未标 final 的（当前小时）默认跳过，且被记名",
        [x["hkey"] for x in c] == ["2026-09-16T03"]
        and u == ["l2_2026-09-16T04.tar.gz"],
    )
    c, u, w = select_packs(parsed2, "hour", 0, 0, True, now, "2026-09-16")
    chk("--include-today -> 未完成的也纳入", len(c) == 2 and u == [])
    c, u, w = select_packs(
        [x for x in parsed if x["kind"] == "day"],
        "auto",
        0,
        0,
        False,
        now,
        "2026-09-16",
    )
    chk("清单里没有小时包 -> auto 回退到日包（老行为不变）", len(c) == 2)
    c, u, w = select_packs([], "auto", 0, 0, False, now, "2026-09-16")
    chk(
        "空清单 -> 不报错、返回空（调用方会打印“待补 0”）",
        c == [] and u == [] and w == [],
    )

    print("[4] 本地文件数对账")
    tmp = Path(tempfile.mkdtemp(prefix="l2get-st"))
    try:
        os_d = tmp / "2026-09-16"
        os_d.mkdir(parents=True)
        for n in (
            "btc-updown-5m-1789529400.jsonl.gz",
            "eth-updown-5m-1789529460.jsonl.gz",
            "btc-updown-5m-1789531200.jsonl.gz",
        ):
            (os_d / n).write_bytes(b"")
        chk(
            "小时包：只数属于该小时的文件（03 点 2 个、04 点 1 个）",
            local_files_for(
                tmp, {"kind": "hour", "day": "2026-09-16", "hkey": "2026-09-16T03"}
            )
            == 2
            and local_files_for(
                tmp, {"kind": "hour", "day": "2026-09-16", "hkey": "2026-09-16T04"}
            )
            == 1,
        )
        chk(
            "日包：数该日目录全部文件",
            local_files_for(tmp, {"kind": "day", "day": "2026-09-16", "hkey": ""}) == 3,
        )
        chk(
            "目录不存在 -> 0（不抛异常）",
            local_files_for(tmp, {"kind": "hour", "day": "2000-01-01", "hkey": "x"})
            == 0,
        )
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

    print()
    if fails:
        print("自检失败 %d 项：%s" % (len(fails), "; ".join(fails)))
        return 1
    print("自检全部通过")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="L2 包补拉 + 解压（按小时为主）")
    ap.add_argument("--base", default=DEFAULT_BASE, help="服务基址（默认 43:8001）")
    ap.add_argument(
        "--dir", default=None, help="数据根目录（默认 <workspace>/l2_data）"
    )
    ap.add_argument(
        "--days", type=int, default=0, help="只处理最近 N 天的**日包**（0=全部）"
    )
    ap.add_argument(
        "--hours",
        type=int,
        default=0,
        help="只处理最近 N 小时的**小时包**（0=清单里有的都算）",
    )
    ap.add_argument(
        "--mode",
        choices=("auto", "hour", "day", "all"),
        default="auto",
        help="auto=清单里有小时包就用小时包，否则用日包",
    )
    ap.add_argument(
        "--include-today", action="store_true", help="连还在长的包也拉（当前小时/当天）"
    )
    ap.add_argument("--no-extract", action="store_true", help="只下载，不解压")
    ap.add_argument("--list", action="store_true", help="只列状态，不下载")
    ap.add_argument("--force", action="store_true", help="即使 sha256 一致也重下")
    ap.add_argument(
        "--jobs",
        type=int,
        default=12,
        help="并发分片数（默认 12；实测这条路由单连接仅 9~30KB/s，并发能绕开）",
    )
    ap.add_argument("--chunk-mb", type=int, default=4, help="分片大小 MB（默认 4）")
    ap.add_argument("--selftest", action="store_true", help="离线自检（不联网）")
    a = ap.parse_args()
    if a.selftest:
        return selftest()

    # 本例默认数据根 = <项目根>/data（本文件位于 <项目根>/tools/ 下）
    root = Path(a.dir) if a.dir else Path(__file__).resolve().parents[1] / "data"
    packs = root / "packs"
    packs.mkdir(parents=True, exist_ok=True)
    log(f"数据根目录: {root}")

    try:
        man = fetch_manifest(a.base)
    except (urllib.error.URLError, OSError, ValueError) as e:
        log(f"！取清单失败（{a.base}/dl/manifest.json）: {type(e).__name__} {e}")
        return 2
    files = man.get("files") or []
    gen = man.get("generated") or 0
    # 本地留一份清单副本（便于离线对账/回溯；模块 docstring 承诺的 <root>/manifest.json）
    try:
        (root / "manifest.json").write_text(
            json.dumps(man, ensure_ascii=False, indent=1), encoding="utf-8"
        )
    except OSError as e:
        log(f"  ！写本地 manifest 失败: {type(e).__name__} {e}")
    log(
        f"远端清单生成于 {dt.datetime.fromtimestamp(gen):%Y-%m-%d %H:%M:%S}，共 {len(files)} 个包"
    )

    parsed, unrecognized = [], []
    for f in files:
        name = str(f.get("name") or "")
        info = parse_pack(name)
        if info is None:
            unrecognized.append(name)
            continue
        info["entry"] = f
        parsed.append(info)
    nh = [x for x in parsed if x["kind"] == "hour"]
    nd = [x for x in parsed if x["kind"] == "day"]
    eff_mode = a.mode if a.mode != "auto" else ("hour" if nh else "day")
    log(f"清单：小时包 {len(nh)} / 日包 {len(nd)}")
    if unrecognized:
        log(
            f"  ！{len(unrecognized)} 个包名认不出来（既非日包也非小时包），未处理：{unrecognized[:3]}"
        )

    now = int(time.time())
    today = dt.date.today().isoformat()
    want, unfinished, windowed = select_packs(
        parsed, a.mode, a.hours, a.days, a.include_today, now, today
    )
    log(f"模式 {eff_mode}  ⇒ 本次考虑 {len(want)} 个（窗口外排除 {len(windowed)}）")
    skipped = len(unfinished)
    for nm in unfinished:
        log(f"  跳过（还在长，未标 final）: {nm}")

    todo, uptodate = [], 0
    for x in want:
        name = str(x["entry"].get("name") or "")
        dest = packs / name
        wantsha = str(x["entry"].get("sha256") or "")
        if dest.exists() and not a.force:
            if wantsha and sha256_of(dest) == wantsha:
                uptodate += 1
                if not a.no_extract and not a.list:
                    # 合并解压（不再假设「目录在 = 已解压」）：
                    # ① 当前小时/当天的包会被同名重打 ⇒ 目录在也可能缺新文件；
                    # ② 本地目录可能被人工动过（或被采集器中断写坏）⇒ 按包内容补齐。
                    # 幂等且只几秒，比静默漏数据便宜得多（2026-09-12 修）。
                    # `--list` 是「只列状态」⇒ 不写盘。
                    extract(dest, root)
                continue
            log(f"  需重下（sha 不一致）: {name}")
        todo.append(x)

    log(f"状态：已最新 {uptodate} 个 | 待补 {len(todo)} 个 | 跳过未完成 {skipped} 个")
    if a.list or not todo:
        for x in todo:
            nm = str(x["entry"].get("name"))
            log(f"  - {nm}  {(x['entry'].get('size') or 0) / 1048576:.1f} MB")
        if a.list:
            miss = [str(x["entry"].get("name")) for x in todo]
            if miss:
                log(
                    f"缺 {len(miss)} 个（{eff_mode} 粒度）："
                    + ", ".join(miss[:8])
                    + (" …" if len(miss) > 8 else "")
                )
        return 0

    ok = 0
    warn = 0
    for x in todo:
        name = str(x["entry"]["name"])
        log(f"→ {name}（{(x['entry'].get('size') or 0) / 1048576:.1f} MB）")
        if not download(a.base, name, packs / name, jobs=a.jobs, chunk_mb=a.chunk_mb):
            continue
        dest = packs / name
        got = sha256_of(dest)
        want_sha = str(x["entry"].get("sha256") or "")
        if want_sha and got != want_sha:
            log(
                f"    ！sha256 不符（期望 {want_sha[:12]}… 实得 {got[:12]}…）⇒ 删除重来更安全，已保留文件供排查"
            )
            continue
        (packs / (name + ".sha256")).write_text(f"{got}  {name}\n", encoding="utf-8")
        ok += 1
        log("    sha256 校验通过")
        if not a.no_extract:
            t0 = time.time()
            if extract(dest, root):
                log(f"    已解压到 {root}（{time.time() - t0:.1f}s）")
                nf = x["entry"].get("nfiles")
                if isinstance(nf, int) and nf > 0:
                    lf = local_files_for(root, x)
                    if lf != nf:
                        warn += 1
                        log(
                            f"    ！本地该粒度文件数 {lf} ≠ 包内 {nf}"
                            f"（缺 = 没解压全；多 = 本地有已删文件）"
                        )
    log(
        f"完成：成功 {ok}/{len(todo)}" + (f"｜文件数对账告警 {warn} 个" if warn else "")
    )
    return 0 if ok == len(todo) else 1


if __name__ == "__main__":
    raise SystemExit(main())
