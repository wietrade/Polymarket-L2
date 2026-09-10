#!/bin/bash
# L2 recorder v3 进程守护 (recorder_l2_dual.py, 双连接热备) — cron 每 1 分钟调用.
# 若 v3 进程消失 且 单例端口(45614)已释放 → 自动重启, 防"静默长期断数据".
# v2(recorder_l2.py 单连接) 已于 2026-09-08 退役: 不再守护 (曾致 v2/v3 双写同目录损坏数据).
# 不 pkill / 不强杀; 仅当确认无活实例才启动(单例锁天然防双开).
cd /www/wwwroot/polymarket-l2 || exit 1

# 1) v3 进程还在 → 无事 (精确匹配 python 真身, 防 bash 包装误判)
if pgrep -f "/www/wwwroot/polymarket/venv/bin/python -u recorder_l2_dual.py" >/dev/null 2>&1; then
    exit 0
fi

# 2) 单例端口 45614 仍被占(可能有实例在启动窗口期) → 无事
if /www/wwwroot/polymarket/venv/bin/python -c \
    "import socket;s=socket.socket();s.settimeout(2);s.connect(('127.0.0.1',45614));s.close()" 2>/dev/null; then
    exit 0
fi

# 3) 确认无活实例 → 重启(记录一次)
echo "$(date -u +%F_%T) v3 DOWN detected → restarting" >> /www/wwwroot/polymarket-l2/watchdog.log
nohup /www/wwwroot/polymarket/venv/bin/python -u recorder_l2_dual.py \
    --dir /www/wwwroot/polymarket-l2/l2_data \
    >> /tmp/recorder_l2_v3.log 2>&1 &
echo "$(date -u +%F_%T) v3 restart issued (pid $!)" >> /www/wwwroot/polymarket-l2/watchdog.log
