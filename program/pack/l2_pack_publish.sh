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
set -u
V3=/www/wwwroot/ts_test/pmframe-v3
PY=/www/wwwroot/polymarket/venv/bin/python
PACKS=/www/wwwroot/polymarket-l2/l2_data/packs
DL=$V3/www/_dl
KEEP_DAYS=7

echo "===== $(date '+%F %T') L2 pack+publish ====="
"$PY" -B "$V3/tools/l2_pack.py"          # 幂等：已最新则跳过；当天包会随数据增长自动重打
mkdir -p "$DL"
cp -f "$PACKS"/l2_*.tar.gz "$PACKS"/l2_*.tar.gz.sha256 "$DL"/ 2>/dev/null
# 清理：只保留最近 KEEP_DAYS 天的**按天**包（l2_full.tar.gz 等手工包不动）
find "$DL" -maxdepth 1 -name 'l2_2*.tar.gz*' -mtime +$KEEP_DAYS -print -delete

# 生成清单（2026-09-10 加）：本地补拉工具据此判断"缺哪个/哪个 sha 变了"。
# 必要：同一天的包会随数据增长被**同名重打** ⇒ 只看文件存在会永久漏掉后半天的数据。
"$PY" -B - <<'PY'
import hashlib
import json
import pathlib
import time

DL = pathlib.Path("/www/wwwroot/ts_test/pmframe-v3/www/_dl")
items = []
for p in sorted(DL.glob("l2_*.tar.gz")):
    side = p.with_suffix(p.suffix + ".sha256")
    if side.exists():
        sha = side.read_text().split()[0].strip()
    else:
        sha = hashlib.sha256(p.read_bytes()).hexdigest()
    st = p.stat()
    items.append(
        {
            "name": p.name,
            "size": st.st_size,
            "mtime": int(st.st_mtime),
            "sha256": sha,
            "final": True,  # 由本地工具按日期再判（当天的包会继续长）
        }
    )
(DL / "manifest.json").write_text(
    json.dumps({"generated": int(time.time()), "files": items}, indent=1),
    encoding="utf-8",
)
print(f"--- manifest.json: {len(items)} 个包")
PY

echo "--- 可下载清单:"
ls -lh "$DL" | awk '{print "   ", $9, $5}'
echo
