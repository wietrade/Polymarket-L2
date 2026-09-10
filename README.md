# L2 数据与工具（Polymarket updown 5m / 15m）

本目录是**本地 L2 研究工作区**：把 43 上自研 recorder 采集的原始数据按天取回本地，
配套下载/校验工具，以及"线上正在跑的那份程序"的只读快照。

> 数据红线：`data/` 下的原始数据**勿删**。确需清理请先与负责人确认。
> 代码与数据分开：数据在 `data/`，工具在 `tools/`，线上程序快照在 `program/`。

---

## 1. 目录布局

```
l2/
  README.md              本文件
  data/                  数据（唯一实体，勿手改）
    packs/               下载的压缩包 l2_<date>.tar.gz（+ .sha256），以及分片续传用的 .parts/
    <date>/              解压结果：<date>/*.jsonl.gz（tar 内顶层就是日期目录）
    manifest.json        服务端清单副本（可选，便于离线对账）
  tools/                 本地工具
    l2_get.py            从 43 补拉日包：清单对账 → 并发分片下载 → sha256 校验 → 解压
    sync_program.sh      从 43 取回**运行版**程序快照 + 写校验表
  program/               线上程序快照（只读；要改代码请改 git 仓库，见 §5）
    recorder_l2_dual.py  采集器（4 线 × 双连接热备）
    watchdog_l2.sh       cron 每分钟守护采集器
    pack/                打包/发布脚本（在 43 上属于 pmframe-v3/tools）
      l2_pack.py
      l2_pack_publish.sh
    VERSIONS.txt         sha256 校验表（与 43 对账用）
```

## 2. 数据来源与发布链路

```
43 采集：polymarket-l2/recorder_l2_dual.py
        4 条线 = btc/eth × 5m/15m；每条线 2 条独立 WS 订同一市场（双连接热备，
        单条断线由另一条补，gap 归零）→ 去重 → 单文件模型写 gzip
        ↓ 落盘 /www/wwwroot/polymarket-l2/l2_data/<date>/*.jsonl.gz
每天 03:30：cron 跑 tools/l2_pack_publish.sh
        l2_pack.py  按天打包（幂等；**当天的包会随数据增长被同名重打**）
        发布到 8001 的 www/_dl/（支持 HTTP Range 断点续传）
        同时生成 _dl/manifest.json（文件名 + 大小 + mtime + sha256）
        ↓
本地：python tools/l2_get.py     （按清单只补缺的/变化的，校验通过才解压）
```

数据内容：每个 `<date>/<coin>-updown-<cycle>-<epoch>.jsonl.gz` 是该 bar 的
`book`（L2 盘口增量）与 `last_trade`（真实成交）事件流。

## 3. 常用命令

```bash
# 看清单与本地状态（不下载）
python tools/l2_get.py --list

# 补拉所有"已完成"的日包并解压（默认数据根 = ./data）
python tools/l2_get.py

# 并发数（默认 12）：本机到 43 单连接被限速，加大并发能明显提速
python tools/l2_get.py --jobs 24

# 只关心最近 3 天 / 只下不解压
python tools/l2_get.py --days 3
python tools/l2_get.py --no-extract

# 连当天的包也拉（当天还会变，慎用）
python tools/l2_get.py --include-today

# 手工下载单个包（curl -C - 续传）
curl -C - -O http://43.165.167.132:8001/dl/l2_2026-09-08.tar.gz
sha256sum -c l2_2026-09-08.tar.gz.sha256

# 核对本地程序快照是否与 43 运行版一致
bash tools/sync_program.sh --check
```

## 4. 校验与对账

- **下载后必须看 sha256**：`l2_get.py` 会自动比对，不一致就不解压。
- **不要用"文件存在"判断已下载**：同一天的包会被**同名重打**（例如当天的包先只覆盖到
  某个时刻，凌晨 03:30 变成完整版）。只判存在会永久漏掉后半天的数据。
- 程序快照对账：`program/VERSIONS.txt` 记录每个文件的 sha256，
  在 `program/` 下执行 `sha256sum -c VERSIONS.txt` 即可验证。

## 5. 程序（要改代码看这里）

- `program/` 只是**只读快照**，由 `tools/sync_program.sh` 从 43 取回，用于"本地代码 = 线上代码"的可验证性。
- **可编辑源码**：
  - 采集器：`i:\plot\polymarket-l2\`（GitHub `wietrade/Polymarket-L2`）→ 改动经该仓库推上 43。
  - 打包/发布脚本：`i:\plot\updown-live\pmframe-v3\tools\l2_pack*.py|sh`（随 updown-live 仓库版本化，用 `tools/srv_sync.sh` 部署）。

## 6. 已知坑（都是踩过的，研究时注意）

| 坑 | 说明 |
|:---|:---|
| **单连接被限速** | 43 本机从 8001 下载 323 MB/s、服务器到国际 1.09 MB/s、本机到 Cloudflare 482 KB/s，但**43↔本机单连接只有 9~30 KB/s**；实测 8 并发 93 KB/s、24 并发 173 KB/s ⇒ 用 `--jobs` 绕开 |
| **同名重打** | 见 §4；必须比 sha256 |
| **文件名 ≠ 市场** | 单个 `<slug>.jsonl.gz` 里可能混入相邻轮次/别的市场的行 ⇒ 一律**按每行的 `asset_id`/`market` 字段归因**，别看文件名 |
| **历史数据的时间覆盖** | 2026-09-10 之前，采集器在每 bar 前 ~120s 是瞎的（切 bar 后旧连接上发订阅帧不生效）；已于 09-10 用「订阅代 `sub_gen`」修复 ⇒ **09-10 之前的数据只应信任每 bar 末 ~180s**，涉及 bar 前段的研究口径需重跑 |
| **ssh 会吞 stdin** | 脚本里循环 `while read ... done <<< "$list"` 时，`ssh` 必须加 `-n`，否则它吃掉剩余输入、每轮只处理一个文件（`sync_program.sh` 踩过） |
| **watchdog 双写** | 历史上 watchdog 误重启旧版采集器，导致两个进程写同一批 gz ⇒ 文件交错损坏。现在 watchdog 只认 `recorder_l2_dual.py` 单例（锁口 45614） |

## 7. 相关文档

- 采集器设计/背景：`i:\plot\polymarket-l2\README.md`
- 待办与结论真源：`i:\plot\updown-live\docs\TODO-待办清单.md`（T18 = L2 打包下载，T28 = 实盘请求量优化）
- 记忆：`/memories/repo/recorder-l2-ws.md`（双连接热备与断流真相）
