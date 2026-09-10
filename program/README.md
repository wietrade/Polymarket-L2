# program/ —— 线上运行版程序快照（只读）

本目录由 `../tools/sync_program.sh` 从 43 **取回**，用来回答一个具体问题：
**「我本地看到的代码，是否就是线上正在跑的那份？」**（靠 `VERSIONS.txt` 的 sha256 对账）

## 不要在这里改代码

| 快照文件 | 可编辑源头 | 怎么部署上去 |
|:---|:---|:---|
| `recorder_l2_dual.py`、`watchdog_l2.sh` | `i:\plot\polymarket-l2\`（GitHub `wietrade/Polymarket-L2`） | 推该仓库的 main，再同步到 43 |
| `pack/l2_pack.py`、`pack/l2_pack_publish.sh` | `i:\plot\updown-live\pmframe-v3\tools\` | `bash updown-live/tools/srv_sync.sh push` |

改完源头、部署完 43 之后，跑一次 `sync_program.sh` 更新本目录与校验表，
这样"快照 = 线上"始终成立。

## 核对是否漂移

```bash
bash ../tools/sync_program.sh --check     # 只比对 sha256，不下载；有漂移退出码 1
bash ../tools/sync_program.sh             # 有差异则取回并重写 VERSIONS.txt
cd program && sha256sum -c VERSIONS.txt   # 校验本目录快照自身是否完整
```

最近一次核对（2026-09-11）：`recorder_l2_dual.py` / `watchdog_l2.sh` / `pack/l2_pack.py` /
`pack/l2_pack_publish.sh` **4/4 与 43 运行版一致**。

## 为什么要有这份快照

采集器曾经出现"线上在跑、本地没有、仓库里也没有"的状态（2026-09-10 的 `sub_gen` 热修），
一旦本地工作区丢失，修复就没了。从那天起：
- 线上程序一律**先在仓库提交**，再部署；
- 本目录提供可验证的"线上版本"副本，便于随时比对与回滚。
