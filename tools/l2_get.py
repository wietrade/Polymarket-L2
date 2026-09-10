#!/usr/bin/env python3
"""l2_get.py —— 本地 L2 数据补拉器（只下缺的 / 断点续传 / 校验后才解压）。

背景（用户 2026-09-10 要求）：
  L2 原始数据在 43 上按天打包成 `l2_<date>.tar.gz`（+ `.sha256`）放在 8001 的 `/_dl/`，
  支持 HTTP Range 断点续传。此前本地没有任何包、也没有清单，只能靠人工对账。

关键约束（为什么必须比 sha256，而不能只看"文件在不在"）：
  **同一天的包会随数据增长被同名重打**（例如 09-10 的包在 19:32 生成、只覆盖到 19:30，
  凌晨 03:30 会被完整版覆盖）⇒ 只判存在会永久漏掉后半天的数据 ⇒ 一律按 sha256 判定。

用法：
  python tools/l2_get.py --list                    # 只列清单与本地状态，不下载
  python tools/l2_get.py                           # 补拉所有**已完成**的日包并解压
  python tools/l2_get.py --jobs 24                 # 加大并发（实测这条路由单连接被限速）
  python tools/l2_get.py --include-today           # 连当天的包也拉（当天还会变，慎用）
  python tools/l2_get.py --days 3                  # 只关心最近 3 天
  python tools/l2_get.py --no-extract              # 只下压缩包，不解压
  python tools/l2_get.py --dir I:/plot/l2/data     # 指定数据根目录（默认 <项目根>/data）

目录布局（数据与代码分离，数据不入口 git 仓库）：
  <root>/packs/l2_<date>.tar.gz        下载的包（+ .sha256）
  <root>/<date>/...                    解压后的 jsonl.gz（tar 内顶层就是 <date>/）
  <root>/manifest.json                 服务端清单副本（便于离线对账）
"""

from __future__ import annotations

import argparse
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
    """解压到 <root>/（tar 内顶层就是 <date>/）。已存在同名目录则跳过。"""
    try:
        with tarfile.open(pkg, "r:gz") as tf:
            names = tf.getnames()
        if not names:
            return False
        tip = names[0].split("/")[0]
        if (root / tip).exists():
            return True  # 视为已解压
        with tarfile.open(pkg, "r:gz") as tf:
            try:
                # filter="data"：只解普通文件/目录，挡绝对路径与符号链接
                # （Python 3.12+ 支持；3.14 起为默认，不传会伐 DeprecationWarning）
                tf.extractall(root, filter="data")
            except TypeError:  # 老版本无 filter 参数
                tf.extractall(root)
        return True
    except Exception as e:  # 损坏的包不该静默
        log(f"    ！解压失败 {pkg.name}: {type(e).__name__} {e}")
        return False


def main() -> int:
    ap = argparse.ArgumentParser(description="L2 日包补拉 + 解压")
    ap.add_argument("--base", default=DEFAULT_BASE, help="服务基址（默认 43:8001）")
    ap.add_argument(
        "--dir", default=None, help="数据根目录（默认 <workspace>/l2_data）"
    )
    ap.add_argument("--days", type=int, default=0, help="只处理最近 N 天（0=全部）")
    ap.add_argument(
        "--include-today", action="store_true", help="连当天的包也拉（当天会变）"
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
    a = ap.parse_args()

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
    log(
        f"远端清单生成于 {dt.datetime.fromtimestamp(gen):%Y-%m-%d %H:%M:%S}，共 {len(files)} 个包"
    )

    DAY_RE = re.compile(r"^l2_(\d{4}-\d{2}-\d{2})\.tar\.gz$")
    today = dt.date.today().isoformat()
    todo, uptodate, skipped = [], 0, 0
    for f in files:
        name = str(f.get("name") or "")
        m = DAY_RE.match(name)
        if not m:
            continue  # l2_full.tar.gz 等手工包不自动拉（要的话手动 curl）
        day = m.group(1)
        if (
            a.days
            and day < (dt.date.today() - dt.timedelta(days=a.days - 1)).isoformat()
        ):
            continue
        if day == today and not a.include_today:
            log(f"  跳过（当天包还会增长）: {name}")
            skipped += 1
            continue
        dest = packs / name
        want = str(f.get("sha256") or "")
        if dest.exists() and not a.force:
            got = sha256_of(dest)
            if want and got == want:
                uptodate += 1
                if not a.no_extract and not (root / day).exists():
                    extract(dest, root)
                continue
            log(f"  需重下（sha 不一致）: {name}")
        todo.append((f, dest, want))

    log(f"状态：已最新 {uptodate} 个 | 待补 {len(todo)} 个 | 跳过当天 {skipped} 个")
    if a.list or not todo:
        for f, dest, want in todo:
            log(f"  - {f['name']}  {f['size'] / 1048576:.1f} MB")
        return 0

    ok = 0
    for f, dest, want in todo:
        name = f["name"]
        log(f"→ {name}（{(f.get('size') or 0) / 1048576:.1f} MB）")
        if not download(a.base, name, dest, jobs=a.jobs, chunk_mb=a.chunk_mb):
            continue
        got = sha256_of(dest)
        if want and got != want:
            log(
                f"    ！sha256 不符（期望 {want[:12]}… 实得 {got[:12]}…）⇒ 删除重来更安全，已保留文件供排查"
            )
            continue
        (packs / (name + ".sha256")).write_text(f"{got}  {name}\n", encoding="utf-8")
        ok += 1
        log("    sha256 校验通过")
        if not a.no_extract:
            t0 = time.time()
            if extract(dest, root):
                log(f"    已解压到 {root}（{time.time() - t0:.1f}s）")
    log(f"完成：成功 {ok}/{len(todo)}")
    return 0 if ok == len(todo) else 1


if __name__ == "__main__":
    raise SystemExit(main())
