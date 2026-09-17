#!/usr/bin/env python3
"""l2_filesync.py —— L2 原始数据「按文件比对、缺谁拉谁」（只读远端 / 只写本地 data/）

## 它做什么（用户 2026-09-17 提的做法，比走"打包"更直也更准）

    ① ssh 列远端清单：`find <root> -name '*.jsonl.gz' -printf '%s %p\n'`（7357 个文件，实测 10.5s）
    ② 与本地逐文件比 **大小** ⇒ 分出 缺 / 大小不符 / 本地多出
    ③ 只把「缺 + 不符」的那些文件并发拉回来
    ④ 拉完复比一次，确认差异归零（**不靠"跑完了"当结论**）

## 为什么不用「日包 / 小时包」

包机制存在的原因是历史约束：8001 的 `/dl/<文件名>` **只服务扁平文件名、不能列目录**
⇒ 本地没法枚举服务器，只能靠"服务端打包 + 清单"间接知道缺什么。于是有了两级绕路：
日包（215MB/天）→ 小时包（6MB/小时）。但 ssh 本来就能直接列目录，所以：

| | 走包 | 按文件 |
|:--|:--|:--|
| 本次真实差异（09-16/09-17） | 231.4 MB（日包） | **172.2 MB** |
| 只补缺的部分 | 包是整块粒度，包内所有文件都重下 | **只下缺的那几个** |
| **本地文件被截断能否发现** | **发现不了**（包只管"本地有没有这个包"） | **能**（大小不符，实测抓到 3 个） |
| 续传 | 需专门的分片机制 | 失败的下次重比一次，单文件 ≤1MB |
| 服务端成本 | 每次要 gzip 打包（占采集机 CPU） | 一条 `find` |

⇒ 结论：**补拉默认走本工具**；包机制（`l2_get.py` + `l2_pack*.py`）保留给
"本地没有 ssh 通路、只能走 8001 下载"的场景，以及历史留档，不再是主路径。

## 口径与已知边界

- 判据是 **大小**，不是 sha256：远端文件一直在长（当前 bar 的 gz 开着写），
  对"边长边比"的文件算 sha256 只会折磨采集机。大小相等即视为一致；
  真要字节级校验用 `--verify-sha`（**慢**：要两边各算一遍全量 1.4GB）。
- 本地多出的文件**不删**（只报告）：数据只增不删是红线，删要人工确认。
- 只拉 `*.jsonl.gz`（L2 原始数据的唯一形态）。`derived/` 是本地派生物，不同步。
- 并发是 ssh/scp 连接级的（实测这条路由单连接被限速，多连接才有用）。

## 用法

    python -B tools/l2_filesync.py --list              # 只比不拉（看差哪些）
    python -B tools/l2_filesync.py                     # 缺谁拉谁（默认 jobs=24）
    python -B tools/l2_filesync.py --days 2            # 只看最近 2 天（少扫一点）
    python -B tools/l2_filesync.py --selftest          # 离线自检
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import hashlib
import os
import re
import subprocess
import time

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_ROOT = os.path.join(os.path.dirname(HERE), "data")
DEFAULT_KEY = "I:/1H/43.165.167.132_id_ed25519"
DEFAULT_HOST = "root@43.165.167.132"
DEFAULT_REMOTE = "/www/wwwroot/polymarket-l2/l2_data"
SSH_OPTS = ["-o", "StrictHostKeyChecking=no", "-o", "ConnectTimeout=20"]
NAME_RE = re.compile(r"^[^/]+\.jsonl\.gz$")


def log(msg: str) -> None:
    print(msg, flush=True)


def remote_list(
    key: str, host: str, remote_root: str, timeout: int = 180
) -> dict[str, int]:
    """远端文件清单 {相对路径: 字节数}。路径相对 remote_root（形如 `2026-09-16/xxx.jsonl.gz`）。"""
    cmd = [
        "ssh",
        "-i",
        key,
        *SSH_OPTS,
        host,
        "cd %s && find . -name '*.jsonl.gz' -printf '%%s %%p\\n'" % remote_root,
    ]
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if p.returncode != 0:
        raise RuntimeError(
            "ssh 列清单失败 rc=%d: %s" % (p.returncode, p.stderr.strip()[:300])
        )
    out: dict[str, int] = {}
    for line in p.stdout.splitlines():
        parts = line.split(None, 1)
        if len(parts) != 2 or not parts[0].isdigit():
            continue
        rel = parts[1].strip().lstrip("./")
        if NAME_RE.match(os.path.basename(rel)):
            out[rel] = int(parts[0])
    return out


def local_list(root: str) -> dict[str, int]:
    """现场扫盘 {相对路径: 字节数}（只扫 `<date>/` 这种日目录）。"""
    out: dict[str, int] = {}
    if not os.path.isdir(root):
        return out
    for day in sorted(os.listdir(root)):
        d = os.path.join(root, day)
        if not os.path.isdir(d) or not day[:4].isdigit():
            continue
        for fn in os.listdir(d):
            if fn.endswith(".jsonl.gz"):
                out["%s/%s" % (day, fn)] = os.path.getsize(os.path.join(d, fn))
    return out


# --------------------------------------------------------------------------- #
# 清单文件：**与服务器列表同格式**（`<字节数> <相对路径>`，一行一个，`#` 开头是注释）
# ⇒ "本地清单 vs 服务器清单"可以直接 diff / comm，人也看得懂、可留档。
# --------------------------------------------------------------------------- #
def read_manifest(path: str) -> dict[str, int]:
    """读清单文件（容错：坏行跳过；`#` 注释跳过）。不存在则返回空 dict。"""
    out: dict[str, int] = {}
    if not os.path.exists(path):
        return out
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.rstrip("\n")
            if not line or line.startswith("#"):
                continue
            p = line.split(None, 1)
            if len(p) != 2 or not p[0].isdigit():
                continue
            out[p[1].strip().lstrip("./")] = int(p[0])
    return out


def write_manifest(path: str, files: dict[str, int], header: str = "") -> None:
    """写清单文件（原子替换：先写 .tmp 再 os.replace，避免中途被杀留下半截清单）。"""
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8", newline="\n") as fh:
        if header:
            fh.write(header if header.endswith("\n") else header + "\n")
        fh.writelines("%d %s\n" % (files[rel], rel) for rel in sorted(files))
    os.replace(tmp, path)


def reconcile(
    manifest: dict[str, int], root: str
) -> tuple[dict[str, int], list[str], list[str]]:
    """用**现场盘上的真实文件**校正清单，返回 (校正后的清单, 盘上已没了, 大小变了)。

    为什么不能盲信清单：文件可能被截断/被删（2026-09-17 就撞上本地文件被采集器截断）。
    清单是**索引**，盘是**真相** —— 索引与真相不符时必须报出来，而不是拿索引当结论。
    同时把"盘上有但清单没记"的并入清单（首次运行 / 别人手工拷进来的文件）。
    """
    fixed: dict[str, int] = {}
    gone: list[str] = []
    changed: list[str] = []
    seen: set[str] = set()
    for rel, sz in manifest.items():
        p = os.path.join(root, rel)
        if not os.path.exists(p):
            gone.append(rel)
            continue
        real = os.path.getsize(p)
        if real != sz:
            changed.append(rel)
        fixed[rel] = real
        seen.add(rel)
    for rel, sz in local_list(root).items():
        if rel not in seen:
            fixed[rel] = sz  # 盘上有、清单没记 ⇒ 补进清单
    return fixed, gone, changed


def fmt_listing(files: dict[str, int]) -> str:
    return "\n".join("%d %s" % (files[r], r) for r in sorted(files))


def diff(
    remote: dict[str, int],
    local: dict[str, int],
    days: int = 0,
    now: float | None = None,
) -> tuple[list[str], list[tuple[str, int, int]], list[str]]:
    """分三类：(缺失, 大小不符[(rel,本地,远端)], 本地多出)。纯函数 ⇒ 可离线自检。

    days>0 时只保留最近 N 天（按目录名 `YYYY-MM-DD` 的 UTC 日期判），避免每次都扫全量。
    """
    now = time.time() if now is None else now
    cut = ""
    if days > 0:
        cut = time.strftime("%Y-%m-%d", time.gmtime(now - days * 86400))
    miss, short = [], []
    for rel, sz in sorted(remote.items()):
        day = rel.split("/", 1)[0]
        if cut and day < cut:
            continue
        got = local.get(rel)
        if got is None:
            miss.append(rel)
        elif got != sz:
            short.append((rel, got, sz))
        # 本地与被截断的旧文件都可能更大/更小 → 只要不等就重拉（不猜哪种情况）
    extra = [k for k in local if k not in remote]
    if cut:
        extra = [k for k in extra if k.split("/", 1)[0] >= cut]
    return miss, short, extra


def pull_one(
    key: str, host: str, remote_root: str, rel: str, root: str, tries: int = 3
) -> bool:
    """拉单个文件。scp 会整文件覆盖写 ⇒ 天然幂等；失败重试。"""
    dst = os.path.join(root, rel)
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    for k in range(tries):
        p = subprocess.run(
            [
                "scp",
                "-q",
                "-O",
                "-i",
                key,
                *SSH_OPTS,
                "%s:%s/%s" % (host, remote_root, rel),
                dst,
            ],
            capture_output=True,
            text=True,
            timeout=300,
        )
        if p.returncode == 0 and os.path.exists(dst):
            return True
        time.sleep(0.5 * (k + 1))
    return False


def sha256_of(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for blk in iter(lambda: f.read(1 << 20), b""):
            h.update(blk)
    return h.hexdigest()


def selftest() -> int:
    """离线自检（不联网、不碰生产数据）：命名即断言、失败退出码非 0。"""
    import tempfile

    fails: list[str] = []

    def chk(name: str, cond: bool) -> None:
        print(("  OK   " if cond else "  FAIL ") + name)
        if not cond:
            fails.append(name)

    now = 1789529400.0  # 2026-09-16T03:30Z
    R = {
        "2026-09-16/a.jsonl.gz": 100,
        "2026-09-16/b.jsonl.gz": 200,
        "2026-09-17/c.jsonl.gz": 300,
    }
    print("[1] 比对三分类")
    L = {"2026-09-16/a.jsonl.gz": 100, "2026-09-16/b.jsonl.gz": 150}
    m, s, e = diff(R, L, 0, now)
    chk(
        "本地有且大小相同 -> 不动",
        "2026-09-16/a.jsonl.gz" not in m
        and all(x[0] != "2026-09-16/a.jsonl.gz" for x in s),
    )
    chk("远端有本地无 -> 缺失", m == ["2026-09-17/c.jsonl.gz"])
    chk(
        "大小不符 -> 单独归类且带上两个大小（本地被截断就是这种）",
        s == [("2026-09-16/b.jsonl.gz", 150, 200)],
    )
    chk("本地多出 -> 报告但不删", e == [])

    print("[2] 本地多出 / 截断方向都能认出")
    L2 = dict(L, **{"2026-09-15/old.jsonl.gz": 10, "2026-09-16/b.jsonl.gz": 999})
    m2, s2, e2 = diff(R, L2, 0, now)
    chk(
        "本地更大的文件同样判为不符（不假设只有变小）",
        ("2026-09-16/b.jsonl.gz", 999, 200) in s2,
    )
    chk("远端已删的本地文件被列为多出", e2 == ["2026-09-15/old.jsonl.gz"])

    print("[3] 窗口过滤（保留日期 >= now-N天 的那一天）")
    # now = 2026-09-16T03:30Z ⇒ days=2 的截止日 = 09-14；days=30 的截止日 = 08-17
    R3 = dict(R, **{"2026-09-01/z.jsonl.gz": 50})
    L3 = dict(L2, **{"2026-09-01/keep.jsonl.gz": 7})
    m0, s0, e0 = diff(R3, L3, 0, now)
    chk(
        "days=0：老日期也参与（缺 09-01/z、多出 09-01/keep）",
        "2026-09-01/z.jsonl.gz" in m0 and "2026-09-01/keep.jsonl.gz" in e0,
    )
    m3, s3, e3 = diff(R3, L3, days=2, now=now)
    chk(
        "days=2：09-01 两边都被窗口排除（既不拉也不报多）",
        "2026-09-01/z.jsonl.gz" not in m3 and "2026-09-01/keep.jsonl.gz" not in e3,
    )
    m4, s4, e4 = diff(R3, L3, days=30, now=now)
    chk(
        "days=30：09-01 又回到窗口内（窗口是日期阈值，不是只看最近 N 个）",
        "2026-09-01/z.jsonl.gz" in m4,
    )

    print("[4] 本地清单与真文件")
    tmp = tempfile.mkdtemp(prefix="l2fs-st")
    try:
        os.makedirs(os.path.join(tmp, "2026-09-16"))
        with open(os.path.join(tmp, "2026-09-16", "x.jsonl.gz"), "wb") as f:
            f.write(b"12345")
        with open(os.path.join(tmp, "2026-09-16", "notes.txt"), "w") as f:
            f.write("x")
        os.makedirs(os.path.join(tmp, "derived"))
        with open(os.path.join(tmp, "derived", "y.jsonl.gz"), "wb") as f:
            f.write(b"1")
        got = local_list(tmp)
        chk(
            "只收 *.jsonl.gz、只认日目录（derived/ 与非 gz 不收）",
            got == {"2026-09-16/x.jsonl.gz": 5},
        )
        m5, s5, e5 = diff(got, got, 0, now)
        chk("自己和自己比 -> 零差异（对称性）", m5 == [] and s5 == [] and e5 == [])

        print("[5] 清单文件：往返 / 与服务器列表同格式 / 盘与清单不符时以盘为准")
        mp = os.path.join(tmp, "filesync_local.txt")
        write_manifest(mp, {"2026-09-16/x.jsonl.gz": 5}, "# hdr\n# 第二行注释")
        back = read_manifest(mp)
        chk("写出再读回内容一致（注释行被跳过）", back == {"2026-09-16/x.jsonl.gz": 5})
        chk(
            "清单与服务器列表同格式 ⇒ 可直接 diff",
            fmt_listing(back).strip() == "5 2026-09-16/x.jsonl.gz",
        )
        chk("原子写：不留 .tmp 残件", not os.path.exists(mp + ".tmp"))
        with open(os.path.join(tmp, "2026-09-16", "x.jsonl.gz"), "wb") as f:
            f.write(b"12")  # 模拟被截断
        fixed, chg_gone, chg = reconcile(back, tmp)
        chk(
            "盘上被截断 -> 以盘为准修正，并记入 changed",
            fixed["2026-09-16/x.jsonl.gz"] == 2 and chg == ["2026-09-16/x.jsonl.gz"],
        )
        fixed2, g2, _ = reconcile({"2026-09-16/nope.jsonl.gz": 9}, tmp)
        chk(
            "清单有、盘上没了 -> 记入 gone 且不留在校正结果里",
            g2 == ["2026-09-16/nope.jsonl.gz"]
            and "2026-09-16/nope.jsonl.gz" not in fixed2,
        )
        chk(
            "盘上有、清单没记 -> 补进清单（首次运行/手工拷入）",
            "2026-09-16/x.jsonl.gz" in fixed2,
        )
        badp = os.path.join(tmp, "bad.txt")
        with open(badp, "w", encoding="utf-8", newline="\n") as f:
            f.write("# 注释\n不是数字 路径\n7 2026-09-16/ok.jsonl.gz\n\n")
        gotb = read_manifest(badp)
        chk(
            "坏行/空行/注释都不会让读清单崩，且好行照收",
            gotb == {"2026-09-16/ok.jsonl.gz": 7},
        )
    finally:
        import shutil

        shutil.rmtree(tmp, ignore_errors=True)

    print()
    if fails:
        log("自检失败 %d 项：%s" % (len(fails), "; ".join(fails)))
        return 1
    log("自检全部通过")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="L2 按文件比对补拉（缺谁拉谁）")
    ap.add_argument("--selftest", action="store_true", help="离线自检（不联网）")
    ap.add_argument(
        "--root", default=DEFAULT_ROOT, help="本地数据根（默认 <项目>/data）"
    )
    ap.add_argument("--remote-root", default=DEFAULT_REMOTE, help="远端数据根")
    ap.add_argument("--key", default=os.environ.get("L2_SSH_KEY", DEFAULT_KEY))
    ap.add_argument("--host", default=os.environ.get("L2_SSH_HOST", DEFAULT_HOST))
    ap.add_argument("--days", type=int, default=0, help="只比最近 N 天（0=全部）")
    ap.add_argument("--jobs", type=int, default=24, help="并发 scp 连接数（默认 24）")
    ap.add_argument("--list", action="store_true", help="只比不拉")
    ap.add_argument(
        "--verify-sha",
        action="store_true",
        help="对已一致的文件再做 sha256 对账（慢：两边各算一遍全量）",
    )
    a = ap.parse_args()
    if a.selftest:
        return selftest()

    t0 = time.time()
    try:
        rem = remote_list(a.key, a.host, a.remote_root)
    except Exception as ex:
        log("！%s" % ex)
        return 2
    log(
        "远端清单：%d 个文件 / %.1f MB（%.1fs）"
        % (len(rem), sum(rem.values()) / 1e6, time.time() - t0)
    )
    remote_txt = os.path.join(a.root, "filesync_remote.txt")
    write_manifest(
        remote_txt,
        rem,
        "# L2 远端文件清单（服务器侧）｜%s｜源 %s:%s｜%d 个文件"
        % (time.strftime("%F %T"), a.host, a.remote_root, len(rem)),
    )
    log("  服务器清单快照 -> %s" % remote_txt)

    local_txt = os.path.join(a.root, "filesync_local.txt")
    had = read_manifest(local_txt)
    loc, gone, changed = reconcile(had, a.root)
    log("本地已下载清单：盘上 %d 个文件（清单文件原有 %d 条）" % (len(loc), len(had)))
    if gone:
        log("  清单里有、盘上已没了: %d 个（会被重新下发）" % len(gone))
    if changed:
        log(
            "  清单与盘上大小不符  : %d 个（本地被截断就是这样）: %s"
            % (len(changed), ", ".join(changed[:3]))
        )

    def lhdr(note: str) -> str:
        return (
            "# L2 本地已下载清单（本地侧，与 filesync_remote.txt 同格式 ⇒ 可直接 diff）"
            "｜更新 %s｜%d 个文件｜%s" % (time.strftime("%F %T"), len(loc), note)
        )

    miss, short, extra = diff(rem, loc, a.days)
    todo = list(miss) + [x[0] for x in short]
    need_mb = sum(rem[k] for k in todo) / 1e6
    log("")
    log(
        "  本地缺        : %4d 个  %.1f MB"
        % (len(miss), sum(rem[k] for k in miss) / 1e6)
    )
    log(
        "  大小不符      : %4d 个  %.1f MB（本地被截断就会出现在这里）"
        % (len(short), sum(rem[rel] for rel, _, _ in short) / 1e6)
    )
    log("  本地多出(不删): %4d 个" % len(extra))
    log("  ⇒ 需下载 %d 个 / %.1f MB" % (len(todo), need_mb))
    if a.days:
        log("  （窗口 --days %d；窗口外的差异未统计）" % a.days)
    if extra[:5]:
        log("  多出示例: %s" % ", ".join(extra[:5]))
    if a.list or not todo:
        # 差异清零（或只列状态）时也把清单落盘：清单就是"已下载完成"的记录
        write_manifest(local_txt, loc, lhdr("差异 %d 个" % len(todo)))
        log("  本地清单 -> %s" % local_txt)
        return 0

    log("\n并发 %d 拉取…" % a.jobs)
    t1 = time.time()
    done = 0
    fail: list[str] = []
    with cf.ThreadPoolExecutor(max_workers=max(1, a.jobs)) as ex:
        futs = {
            ex.submit(pull_one, a.key, a.host, a.remote_root, rel, a.root): rel
            for rel in todo
        }
        for fu in cf.as_completed(futs):
            rel = futs[fu]
            if fu.result():
                done += 1
                loc[rel] = rem[rel]  # 成功即记账
                # 定期落盘 ⇒ 中途被杀/断网也留下**真实**进度，下次只补剩下的
                if done % 25 == 0:
                    write_manifest(
                        local_txt, loc, lhdr("拉取中 %d/%d" % (done, len(todo)))
                    )
            else:
                fail.append(rel)
            if done % 50 == 0 and done:
                sp = (
                    sum(rem[k] for k in todo[:done])
                    / max(0.001, time.time() - t1)
                    / 1e6
                )
                log("    %d/%d  %.2f MB/s" % (done, len(todo), sp))
    log("下载完成：成功 %d / %d（%.1fs）" % (done, len(todo), time.time() - t1))
    if fail:
        log(
            "  ！失败 %d 个（重跑本工具即可重比再补，单文件 ≤1MB）: %s"
            % (len(fail), ", ".join(fail[:5]))
        )

    # 复比：**用盘上的真实现场**校正清单后再比，不靠"跑完了"当结论
    loc2, gone2, changed2 = reconcile(loc, a.root)
    m2, s2, e2 = diff(rem, loc2, a.days)
    log("复比：缺 %d / 不符 %d（应为 0 / 0）" % (len(m2), len(s2)))
    write_manifest(local_txt, loc2, lhdr("复比：缺 %d / 不符 %d" % (len(m2), len(s2))))
    log("  本地清单 -> %s" % local_txt)
    if m2 or s2:
        log(
            "  ！仍有差异，重跑一次（已下载的不再重下）: %s"
            % (", ".join((m2 + [x[0] for x in s2])[:5]))
        )
        return 1

    if a.verify_sha:
        log("sha256 全量对账（慢）…")
        bad = 0
        for rel in sorted(rem):
            if not os.path.exists(os.path.join(a.root, rel)):
                continue
            p = subprocess.run(
                [
                    "ssh",
                    "-i",
                    a.key,
                    *SSH_OPTS,
                    a.host,
                    "sha256sum %s/%s" % (a.remote_root, rel),
                ],
                capture_output=True,
                text=True,
                timeout=300,
            )
            want = p.stdout.split()[0] if p.returncode == 0 and p.stdout.split() else ""
            got = sha256_of(os.path.join(a.root, rel))
            if want and got != want:
                bad += 1
                log("    ！%s 不一致" % rel)
        log("sha256 对账：不一致 %d 个" % bad)
        if bad:
            return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
