#!/bin/bash
# l2_pack_publish.sh —— 每天把 L2 数据打包并发布到 8001/8002 的可下载目录（断点续传）
#
# 背景（用户 2026-09-10 要求）
#   L2 数据 = 我们自己的 recorder（polymarket-l2/recorder_l2_dual.py）在 43 上下载的原始数据。
#   用户要「打包 → 放在 8001 → 可断点续传 → 下载到本地」，而不是全量 rsync 同步 ✗。
#   机制：`pmframe/api.py` 的 `/dl/<文件名>` 端点已支持 **HTTP Range（206）** ✓，
#         它只服务 `<--www>/_dl/` 下的**扁平文件名**（不带子目录）⇒ 包必须直接放 _dl/ ✓。
#
# 由 crontab 每日 03:30 调用（当天结束后的凌晨打昨天的包，保证完整）。
# 也可手工跑：bash tools/l2_pack_publish.sh
#
# 2026-09-11 变更（原因：_dl 涨到 566M，且手工删掉的旧包会自己回来）
#   原来是「母本全量复制」：cp -f "$PACKS"/l2_*.tar.gz* "$DL"/
#   ⇒ 母本里所有历史日包都进 _dl；而下面的清理只删 7 天以上的
#     ⇒ 4 天前的包手工删掉后，次日 03:30 又被复制回来（实测：删 4 个老日包释放 351M，被 cron 抵消）。
#   现改为**只发布最近 PUBLISH_DAYS 天的按天包**（按 mtime；当天包会同名重打 ⇒ mtime 即最新），
#   手工包（l2_full.tar.gz 等）不受天数限制、始终发布。
#   母本不受影响：/www/wwwroot/polymarket-l2/l2_data/packs 始终保留全部历史包，
#   需要旧包时从母本复制回 _dl 即可。
#   想留更长的「补拉窗口」就把 PUBLISH_DAYS 调大（1 天 ≈ 215M 常驻；3 天 ≈ 300M+）。
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
TS=/www/wwwroot/ts_test
PY=/www/wwwroot/polymarket/venv/bin/python
PACKS=/www/wwwroot/polymarket-l2/l2_data/packs
DL=$TS/www/_dl
PUBLISH_DAYS=1    # 只把母本里最近 N 天的按天包复制到 _dl（1 = 昨天完整包 + 当天增量包）
KEEP_DAYS=7       # 保底清理：_dl 里超过 KEEP_DAYS 的按天包删掉（必须 >= PUBLISH_DAYS）
# 小时包（2026-09-17 加，用户要求「按小时补拉，缺几个小时就补拉几个小时的」）
#   日包最小粒度 1 天（≈215MB）⇒ 只缺 2 小时也得搬一整天；小时包粒度 1 小时（≈12MB）。
#   两套包**并存**：日包负责历史全量，小时包负责窗口内增量。
#   口径 = UTC（与 l2_data/<date>/ 日目录同口径）。
PUBLISH_HOURS=72  # 小时包发布窗口（小时）。本地能补多远就取决于它
KEEP_HOURS=168    # _dl 里小时包保留 7 天

# ⚠️ 日包/小时包的 glob 必须互相排除：`l2_2*.tar.gz*` 会把小时包（l2_2…T…tar.gz）一起匹配，
#    不排除就会把小时包也按「天」的粒度去发布/清理（-mtime 粒度是天，装不下 72 小时）。
DAY_GLOB='l2_2*.tar.gz*'
HOUR_GLOB='l2_2*T*.tar.gz*'

echo "===== $(date '+%F %T') L2 pack+publish ====="
"$PY" -B "$HERE/l2_pack.py"          # 幂等：已最新则跳过；当天包会随数据增长自 动重打
"$PY" -B "$HERE/l2_pack.py" --hourly --hours "$PUBLISH_HOURS"   # 小时包（当前小时会重打）
mkdir -p "$DL"
# 只复制最近 PUBLISH_DAYS 天的按天包（含各自 .sha256）
find "$PACKS" -maxdepth 1 -name "$DAY_GLOB" ! -name 'l2_2*T*' -mtime -$PUBLISH_DAYS -exec cp -f {} "$DL"/ \;
# 小时包：-mmin（-mtime 的粒度是「天」，装不下 72 小时）；-u 避免每次都全量重拷
find "$PACKS" -maxdepth 1 -name "$HOUR_GLOB" -mmin -$((PUBLISH_HOURS * 60)) -exec cp -f -u {} "$DL"/ \;
# 手工包（l2_full.tar.gz 等）始终发布
find "$PACKS" -maxdepth 1 -name 'l2_full.tar.gz*' -exec cp -f {} "$DL"/ \;
# 清理：只保留最近 KEEP_DAYS 天的**按天**包（l2_full.tar.gz 等手工包不动）
find "$DL" -maxdepth 1 -name "$DAY_GLOB" ! -name 'l2_2*T*' -mtime +$KEEP_DAYS -print -delete
find "$DL" -maxdepth 1 -name "$HOUR_GLOB" -mmin +$((KEEP_HOURS * 60)) -print -delete

# 生成清单（2026-09-10 加）：本地补拉工具据此判断"缺哪个/哪个 sha 变了"。
# 必要：同一天的包会随数据增长被**同名重打** ⇒ 只看文件存在会永久漏掉后半天的数据。
"$PY" -B - <<'PY'
import calendar
import hashlib
import json
import pathlib
import re
import time

DL = pathlib.Path("/www/wwwroot/ts_test/www/_dl")
HOUR_RE = re.compile(r"^l2_(\d{4}-\d{2}-\d{2})T(\d{2})\.tar\.gz$")
DAY_RE = re.compile(r"^l2_\d{4}-\d{2}-\d{2}\.tar\.gz$")
now = int(time.time())
items = []
for p in sorted(DL.glob("l2_*.tar.gz")):
    side = p.with_suffix(p.suffix + ".sha256")
    nfiles = None
    if side.exists():
        parts = side.read_text().split()
        sha = parts[0].strip()
        # 第三个字段 = 包内文件数（l2_pack.py 写）；缺就留 None（老包没有）
        if len(parts) > 2 and parts[2].isdigit():
            nfiles = int(parts[2])
    else:
        sha = hashlib.sha256(p.read_bytes()).hexdigest()
    st = p.stat()
    mh = HOUR_RE.match(p.name)
    if mh:
        kind = "hour"
        start = calendar.timegm(
            time.strptime(mh.group(1) + mh.group(2), "%Y-%m-%d%H")
        )
        # final = 这一小时已经走完（之后不会再有新帧写进来）⇒ 本地可据此判断
        # “要不要现在拉”。当前的 15m bar 会跨到下一小时，故按整小时判定。
        final = now >= start + 3600
    elif DAY_RE.match(p.name):
        kind = "day"
        final = p.name != "l2_%s.tar.gz" % time.strftime("%Y-%m-%d", time.gmtime())
    else:
        kind = "manual"
        final = True  # l2_full.tar.gz 等手工包
    items.append(
        {
            "name": p.name,
            "size": st.st_size,
            "mtime": int(st.st_mtime),
            "sha256": sha,
            "kind": kind,
            "final": bool(final),
            "nfiles": nfiles,
        }
    )
(DL / "manifest.json").write_text(
    json.dumps({"generated": now, "files": items}, indent=1),
    encoding="utf-8",
)
nh = sum(1 for i in items if i["kind"] == "hour")
print(f"--- manifest.json: {len(items)} 个包（小时包 {nh} / 其余 {len(items) - nh}）")
PY

echo "--- 可下载清单:"
ls -lh "$DL" | awk '{print "   ", $9, $5}'
echo
