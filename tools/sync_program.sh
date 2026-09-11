#!/bin/bash
# sync_program.sh —— 把 43 上**正在运行**的 L2 采集程序快照到本地 program/，并写校验表。
#
# 为什么要有这个脚本（用户 2026-09-11 要求「程序同步下来」）：
#   43 上的 recorder 是唯一在跑的真身（可能被热修过），本地必须能一键取回**运行版**，
#   并留下 sha256 对账表 —— 否则「本地代码 = 线上代码」只能靠记忆，无法验证。
#
# 说明：program/ 是**只读快照**（含 VERSIONS.txt 校验表）。
#   要改程序请改 git 仓库 i:\plot\polymarket-l2\（GitHub wietrade/Polymarket-L2），
#   改完用 srv_sync 的流程推上 43；本脚本只负责「取回并核对」。
#
# 用法: bash sync_program.sh [--check]
#   --check  只比对本地与远端 sha256，不下载（用于日常核对是否漂移）
set -u
KEY=I:/1H/43.165.167.132_id_ed25519
HOST=root@43.165.167.132
# -n：ssh 不得读 stdin —— 否则它会吐掉 while read 循环的剩余输入（本脚本踩过：
#    每轮只取回 1 个文件，其余静默丢失）
SSH=(ssh -n -i "$KEY" -o StrictHostKeyChecking=no -o ConnectTimeout=20 "$HOST")
DEST="$(cd "$(dirname "$0")/.." && pwd)/program"
SRC_L2=/www/wwwroot/polymarket-l2
# 2026-09-11 修正：打包/发布脚本已归属本项目自身 tools/（原先借放 ts_test 的代码树，
#   该树重排后路径失效 ⇒ 由 L2 项目接管，职责闭环）。
SRC_TOOLS=/www/wwwroot/polymarket-l2/tools
# 采集程序（43 的运行副本）+ 打包/发布脚本
FILES_L2="recorder_l2_dual.py watchdog_l2.sh"
FILES_TOOLS="l2_pack.py l2_pack_publish.sh"
CHECK=0
[ "${1:-}" = "--check" ] && CHECK=1

mkdir -p "$DEST/pack"
echo "===== $(date '+%F %T') L2 program sync ====="

remote_hashes=$("${SSH[@]}" "cd $SRC_L2 && sha256sum $FILES_L2; cd $SRC_TOOLS && sha256sum $FILES_TOOLS" 2>/dev/null)
if [ -z "$remote_hashes" ]; then
  echo "！取远端 sha256 失败（SSH/网络？）" >&2
  exit 2
fi

drift=0
MANIFEST="$DEST/VERSIONS.txt.tmp"
{
  echo "# L2 program 快照校验表（由 sync_program.sh 生成）"
  echo "# 生成时间: $(date '+%F %T %z')"
  echo "# 对应远端: $HOST:$SRC_L2 与 $HOST:$SRC_TOOLS"
  echo "# 校验: cd program && sha256sum -c VERSIONS.txt"
} > "$MANIFEST"
while read -r sha name; do
  [ -z "${name:-}" ] && continue
  case "$name" in
    l2_pack*) rel="pack/$name" ; src="$SRC_TOOLS/$name" ;;
    *)        rel="$name"      ; src="$SRC_L2/$name"    ;;
  esac
  local_path="$DEST/$rel"
  echo "$sha  $rel" >> "$MANIFEST"
  local_sha=""
  [ -f "$local_path" ] && local_sha=$(sha256sum "$local_path" | awk '{print $1}')
  if [ "$local_sha" = "$sha" ]; then
    echo "  ✓ 一致 $rel"
    continue
  fi
  drift=1
  if [ "$CHECK" = "1" ]; then
    echo "  ✗ 漂移 $rel（本地 ${local_sha:0:12}… vs 远端 ${sha:0:12}…）"
    continue
  fi
  if "${SSH[@]}" "cat $src" > "$local_path" 2>/dev/null; then
    got=$(sha256sum "$local_path" | awk '{print $1}')
    if [ "$got" = "$sha" ]; then
      echo "  ↓ 已取回 $rel"
    else
      echo "  ！取回后校验失败 $rel（$got vs $sha）" >&2
    fi
  else
    echo "  ！取回失败 $rel" >&2
  fi
done <<< "$remote_hashes"

if [ "$CHECK" = "0" ]; then
  mv -f "$MANIFEST" "$DEST/VERSIONS.txt"
  echo "--- 校验表已写: $DEST/VERSIONS.txt"
else
  rm -f "$MANIFEST"
fi
[ "$drift" = "1" ] && [ "$CHECK" = "1" ] && exit 1
exit 0
