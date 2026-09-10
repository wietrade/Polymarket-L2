# Polymarket L2 数据积累器 (recorder_l2_dual)

Polymarket BTC/ETH updown 5m & 15m 市场的 **L2 盘口 + 真实成交** 数据积累器。
双连接热备方案，供研究重建盘口/成交/滑点使用。

**本仓库同时是采集器源码库与本地数据工作区** —— 2026-09-11 由原来的 `polymarket-l2`（源码）
与 `l2`（数据/工具）两个目录合并而成，代码与数据靠**子目录**分开，不再靠两个仓库分开。

- **43 服务器（运行）**: `/www/wwwroot/polymarket-l2/`
- **本地唯一目录**: `i:\plot\polymarket-l2\`（GitHub `wietrade/Polymarket-L2`）
- 数据红线提醒: `data/` 下采集的 L2 原始数据 **勿删**（如要清理先与负责人确认）

> 我只关心数据与下载工具 → 直接看 **[`docs/数据与工具.md`](docs/数据与工具.md)**
> 我要改采集器 → 看本文 §5 部署与运维

---

## 0. 仓库布局

```
polymarket-l2/                     本地唯一目录 = GitHub wietrade/Polymarket-L2
├── recorder_l2_dual.py    ★改     采集器主程序（v3 双连接热备，43 上跑的就是它）
├── watchdog_l2.sh         ★改     cron 每分钟守护采集器
├── dedup_check.py  gz_diag.py  ws_dual_test.py
│                                  采集器配套验证工具
├── tools/                        本地工具（下载 / 对账）
│   ├── l2_get.py                 从 43 补拉日包 → sha256 校验 → 解压
│   └── sync_program.sh           取回 43 运行版 sha256，核对是否漂移
├── program/                      43 运行版**只读快照** + VERSIONS.txt 校验表
├── data/                         数据（2284 文件 / 663MB；已 gitignore，勿手改）
├── logs/                         本地产物（已 gitignore）
└── docs/数据与工具.md             数据链路、下载命令、已知坑
```

改代码只改标了 ★ 的文件。`program/` 里那份 `recorder_l2_dual.py` 是从 43 取回的**运行版快照**，
别在那里改 —— 一是改了会被下次 `sync_program.sh` 覆盖，二是它与根目录源码的差异正是"线上是否漂移"的证据。

---

## 1. 为什么是双连接热备（背景）

Polymarket `wss://ws-subscriptions-clob.polymarket.com/ws/market` **服务器会周期性主动关连接**（约每 30s 停回 PONG → 客户端 2.4min 判一次 stale/断），这是端点固有行为，官方 SDK 也一样。
单连接方案（旧 recorder_l2.py）会周期性断流造成数据缺口，且旧版同步写盘阻塞 recv 会被服务器以 `1013 slow consumer` 踢。

**对策 = 每市场线开 2 条独立 WS 订同一市场，消息去重后写一份**：单连接断时另一条无缝补上，缺口归零。

> ⚠️ 关键认知：服务器的限制维度是 **每连接订阅的 token 数**（单连接订 4 市场 8 token 会被 ~30s 踢），**不是每 IP 连接数**。本程序每连接只订 2 token（up/down），符合服务器容忍范围。

## 2. 架构

```
4 条线: btc/eth × 5m(300s)/15m(900s) updown 市场
每条线 (LineDualRecorder):
  conn_loop ×2  : 各持 1 条 WS 订当前 slug 的 up/down 2 token
                  收帧 → 去重(共享 60s 窗) → 入 asyncio.Queue
                  断线独立指数退避重连 (3s→60s)
  ticker_loop   : 每 5s 算当前 bar → bar 变了经 gamma REST 取新市场 token → 通知重订阅
  writer_loop   : 唯一写盘者。从队列消费 → 写当前活跃 slug 的 gzip 文件
                  单文件模型: 同线同时只开当前 slug 一个 gz 句柄
                  bar 切换同步关旧开新; 切 bar 竞态的旧 slug 尾消息丢弃
                  gap 状态机: 双连接都 down 才计缺口
```

- **写盘解耦**: 连接只收帧入队，事件循环不碰磁盘 → 不会被 slow consumer 踢
- **单文件模型**: 防 `gzip.open(wt)` 重开导致 truncate/多句柄交错写损坏（历史教训）
- **去重**: key=`(event_type, asset_id, hash[book] / transaction_hash[last_trade])`，60s 窗口
  > ⚠️ last_trade 的唯一字段是 **`transaction_hash`**，不是 `hash`/`txhash`（曾漏去重致每笔成交双写）
- **gap 语义**: 双连接都 down 且市场订阅中 → 记 `gap_from`；任一恢复 → 写 gap mark 行。单连接断不记 gap

## 3. 运行参数（代码内常量）

| 常量 | 值 | 含义 |
|:--|:--|:--|
| `PING_INTERVAL` | 10s | websockets 协议级保活帧（喂服务器 keepalive）|
| `PING_TIMEOUT` | 30s | 放宽防洪峰期 pong 延迟误杀 |
| `RECV_TIMEOUT` | 15s | 单帧等待 |
| `DOWN_AFTER` | 60s | 无任何帧超此时长 → 判定连接 down |
| `DEDUP_WIN` | 60s | 去重窗口 |
| `DEDUP_MAX` | 40000 | 去重表上限 |
| `GUARD_PORT` | 45614 | 单例锁（防双开）|

## 4. 文件清单

| 文件 | 作用 |
|:--|:--|
| `recorder_l2_dual.py` | 主程序（v3 双连接热备，当前运行版）|
| `watchdog_l2.sh` | cron 守护脚本（每分钟；v3 挂了且单例锁释放则自动拉起）|
| `dedup_check.py` | 去重验证工具（统计某 bar 的 book/last_trade 重复数）|
| `gz_diag.py` | gz 结构诊断（成员数/损坏定位）|
| `ws_dual_test.py` | 双连接可行性/补缺测试脚本 |
| `README.md` | 本文档 |

## 5. 部署与运维（43）

### 启动
```bash
cd /www/wwwroot/polymarket-l2 && setsid nohup /www/wwwroot/polymarket/venv/bin/python \
  -u recorder_l2_dual.py --dir /www/wwwroot/polymarket-l2/l2_data \
  > /tmp/recorder_l2_v3.log 2>&1 < /dev/null &
```
日志: `/tmp/recorder_l2_v3.log`（心跳每 30s 打一行）。

### 守护
cron: `* * * * * bash /www/wwwroot/polymarket-l2/watchdog_l2.sh`（已存在）
watchdog 只守护 **v3**（旧 v2 已退役，勿改回守护 recorder_l2.py —— 曾致 v2/v3 双写同目录损坏数据）。

### 重启
```bash
# 找 PID 并停
ps aux | grep recorder_l2_dual | grep -v grep   # 取 python 真身 PID
kill <PID>          # 优雅退出(关文件); 若卡住 kill -9
# 等 watchdog 自动拉起(≤1min) 或手动按上面启动命令
```

### 升级流程（本地改版 → 部署）
1. 本地改 `i:\plot\polymarket-l2\recorder_l2_dual.py`
2. `scp ... recorder_l2_dual.py root@43:/www/wwwroot/polymarket-l2/`
3. 43 上 kill 进程 → watchdog 自动拉起新版（或手动启动）
4. 等一个完整 bar 关闭后跑 `dedup_check.py` 验证

### 日常健康检查
```bash
tail -5 /tmp/recorder_l2_v3.log   # 看 hb: 各线 "活2/2 gap0" 为健康; "活 x/2" 有连接在退避
```

## 6. 数据格式

- 路径: `l2_data/<UTC日期>/<coin>-updown-<cycle>-<bar_epoch>.jsonl.gz`
  （例 `l2_data/2026-09-07/btc-updown-5m-1788801300.jsonl.gz`）
- 每行 = 一条原始 WS 消息 JSON，事件类型:
  - **book**（全档盘口快照: 每档 price/size + hash 盘口哈希 + timestamp ms）
  - **last_trade_price**（真实成交: price/size/side/transaction_hash/fee_rate_bps + timestamp ms）
  - **gap mark**（`{"mark":"gap", from_ts, to_ts, lost_s, note}`）双连接断流缺口标记，供分析剔除
- book 为**状态快照**：断流恢复后首帧即当前全量盘口，可据此排序重建；乱序可用 timestamp(ms) 校正

## 7. 已知限制与结论

- 服务器周期断是常态，双连接热备已把缺口降到 ~0（实测单连接断被另一条补，无 gap mark）
- **禁止**再引入第二个写进程到同一 `l2_data`（曾因 watchdog 复活 v2 双写导致所有 gz 损坏 —— 排查法: `ps aux | grep recorder_l2` 是否多进程）
- 读**正在写入**的 gz 会误报损坏（garbage/invalid block），须等 bar 关闭后再验证
- 本程序已退役 v2 单连接方案（`recorder_l2.py`），本地留档 `recorder_l2_v2_original.py` 备查

## 8. 验证工具用法

```bash
# 去重验证: 改脚本内文件路径为某已关闭 bar, 期望 book/last_trade 重复=0
/www/wwwroot/polymarket/venv/bin/python dedup_check.py

# gz 结构诊断
/www/wwwroot/polymarket/venv/bin/python gz_diag.py <file.gz>
```

## 9. 相关文档

- **数据链路 / 下载工具 / 已知坑**：[`docs/数据与工具.md`](docs/数据与工具.md)
  （本地取数、`l2_get.py` 用法、sha256 对账、`program/` 快照与漂移核对）
- 线上快照校验表：`program/VERSIONS.txt`（`cd program && sha256sum -c VERSIONS.txt`）
- 待办与结论真源：`i:\plot\updown-live\docs\TODO-待办清单.md`
- 记忆：`/memories/repo/recorder-l2-ws.md`
