# gflow-cli Plus（二次开发分支说明）

Plus 是本 fork 在官方 gflow-cli 之上加的多账号能力，代码全部在 `src/gflow_cli/plus/`，
测试在 `tests/plus/`。对官方文件只改了几处接入点（`cli.py` 注册 `gflow plus`，
`mcp/server.py` 注册 `gflow_plus_*` 工具，以及对应的 MCP 对等测试），方便跟进上游。

## 解决什么问题

| 问题 | Plus 的做法 |
|---|---|
| MCP 调用超过 50 秒被客户端掐断 | 提交工具 1 秒内返回 `job_id` / `batch_id`，生成在后台跑；`gflow_plus_wait_jobs` 每次最多等 35 秒 |
| 多个账号要手动分活 | 每个已登录账号一条车道：同账号排队，不同账号并发；视频优先给余额最高的空闲账号，图片按最久未用轮换 |
| 同账号并发撞锁、窗口抢焦点 | 车道锁 + 浏览器启动错开 2.5 秒；同账号连续提交之间随机间隔（`GFLOW_CLI_JITTER_RANGE`，默认 0.5–1.5 秒） |
| 任务栏不停闪自动化 Chrome 的图标 | Windows 上把屏幕外的 gflow Chrome 窗口从任务栏隐藏（页面照常渲染，不是无头模式） |
| 返工时重复生成、重复扣费 | `out_file` 已存在且非空就跳过（Flow 把 `.png` 改存成 `.jpg` 时也认） |
| 进程重启后任务卡在"运行中" | 新进程自动接手：没开始的、跑到一半的图片重新跑；跑到一半的视频不重跑，标为 `indeterminate`（可能已扣费，需先去 Flow 核对） |

## 扣费安全规则

- 只有**确定没扣费**（`charged == "no"`）的失败才会换账号重试：限流、余额不足、账号被占用、提交前的界面或网络问题。
- 进入提交阶段后的失败（`charged` 为 `maybe` / `yes`）一律停手，不换号重跑，结果里 `may_have_spent: true`。
- Flow 提示"检测到异常活动"（`PUBLIC_ERROR_UNUSUAL_ACTIVITY`）时，该账号冷却 10 分钟（2026-11-24 实测约 10 分钟解除），这次提交不重跑。
- 刷新余额时跳过正在出图的账号，读余额期间占住该账号车道，避免和任务抢浏览器。

## MCP 工具

| 工具 | 用途 | 花积分 |
|---|---|---|
| `gflow_plus_health` | 各账号健康、余额、冷却、推荐车道；`refresh=false` 只读缓存 | 否 |
| `gflow_plus_pool_status` | 缓存状态 + 活跃任务数，不联网 | 否 |
| `gflow_plus_submit_image` | 提交一张图（有 `ref_paths` 为 i2i，否则 t2i），默认 `9:16`、`nano2` | 图片 0 积分 |
| `gflow_plus_submit_video` | 提交一条视频（t2v / i2v / r2v，按输入自动判断），默认 `9:16`、`omni-1.2-flash`；`max_credits` 设上限 | 是 |
| `gflow_plus_submit_upscale` | 非阻塞提交图片（2k/4k）或视频（1080p/720p/270p/4k）放大，自动路由到归属账号并复用热浏览器 | 视频 4k 是 |
| `gflow_plus_submit_extend` | 非阻塞提交单段视频续拍，自动路由到归属账号，绝不跨账号重试 | 是 |
| `gflow_plus_submit_batch` | 一次提交多条图片/视频/放大/续拍，支持 `job:<label>` 批内依赖串联和显式 `depends_on` | 视内容 |
| `gflow_plus_wait_jobs` | 等任务完成，最多 35 秒一次；返回状态计数和每条的结果（`output_paths`、`media_id`、账号、`duration`）或错误。视频的 `duration` 读自生成的 MP4 文件本身，是实际秒数 | 否 |
| `gflow_plus_job_status` | 单个任务、整批或最近任务的交付记录（批量/列表默认精简回包，`verbose=true` 返回完整尝试日志） | 否 |
| `gflow_plus_cancel_job` | 取消还在排队的任务；已经在跑的停不下来 | 否 |
| `gflow_plus_retry_job` | 重跑单个失败/已取消任务或整批失败项（`may_have_spent=true` 或 `indeterminate` 默认拒跑，需显式 `force_spent=true`） | 是 |
| `gflow_plus_profile_control` | 清除冷却、无头续期登录态、临时禁用/恢复账号、关闭空闲热浏览器 | 否 |
| `gflow_plus_list_media` | 跨所有账号检索本地 `gflow.db` 里的图片和视频（返回 `media_id`、归属账号、项目 ID 和本地路径） | 否 |
| `gflow_plus_hide_taskbar` | 立即隐藏一次任务栏里的 gflow Chrome | 否 |

推荐给 AI 的用法：`submit_batch` 一次交完 → 反复 `wait_jobs(batch_id=...)` 直到 `done` 为 `true`
→ 按结果里的 `output_paths` 交付。

**保存位置限制**：和官方生成工具一样（GHSA-hqfj-4jww-76f2），MCP 传入的 `out_file` / `out_dir`
必须落在 gflow 的输出目录 `GFLOW_CLI_OUTPUT_DIR` 里面，可以写绝对路径，也可以写相对它的路径；
越界的直接拒绝（批量里有一条越界，整批都不提交）。要直接存进项目目录，就把
`GFLOW_CLI_OUTPUT_DIR` 设成项目根目录。命令行 `gflow plus run-batch` 不受此限制。

## 命令行

```bash
gflow plus health [--no-refresh] [--renew] [--json]
gflow plus pool status [--json]
gflow plus pool pick --kind video|image [--json]
gflow plus pool jobs [--batch-id ID] [--status STATUS] [--limit N] [--json]
gflow plus pool retry [--job-id ID] [--batch-id ID] [--force-spent] [--profile NAME] [--json]  # 会花积分
gflow plus pool profile-control clear_cooldown|renew|exclude|include|close_browser [--profile NAME] [--json]
gflow plus media [--kind all|image|video] [--profile NAME] [--query TEXT] [--limit N] [--json]
gflow plus upscale MEDIA_ID [--media-kind image|video] [--scale SCALE] [--json]  # 视频 4k 会花积分
gflow plus extend MEDIA_ID PROMPT [--aspect 9:16|16:9] [--max-credits N] [--json]  # 会花积分
gflow plus run-batch manifest.json [--no-skip-if-exists] [--json]   # 会花积分
gflow plus hide-taskbar [--json]
```

`manifest.json` 是一个列表，或 `{"items": [...]}`，每项格式同 `gflow_plus_submit_batch`。

## 设置（环境变量）

| 变量 | 默认 | 说明 |
|---|---|---|
| `GFLOW_PLUS_PROFILES` | 空（全部） | 只调度这些账号，逗号分隔 |
| `GFLOW_PLUS_EXCLUDED_PROFILES` | 空 | 不参与调度的账号 |
| `GFLOW_PLUS_MIN_VIDEO_CREDITS` | `20` | 余额低于此值的账号不接视频 |
| `GFLOW_PLUS_CREDIT_CACHE_TTL_S` | `300` | 余额缓存秒数；提交视频前过期会自动重读 |
| `GFLOW_PLUS_STAGGER_SECONDS` | `2.5` | 不同账号启动浏览器的最小间隔 |
| `GFLOW_PLUS_RATE_LIMIT_COOLDOWN_S` | `180` | 限流 / 余额不足后的冷却 |
| `GFLOW_PLUS_AUTH_ERROR_COOLDOWN_S` | `1800` | 登录失效后的冷却 |
| `GFLOW_PLUS_TRANSIENT_COOLDOWN_S` | `30` | 提交前界面 / 网络问题后的冷却 |
| `GFLOW_PLUS_WAF_COOLDOWN_S` | `600` | "检测到异常活动"后的冷却 |
| `GFLOW_PLUS_MAX_FAILOVER_ATTEMPTS` | `3` | 一个任务最多换几个账号 |
| `GFLOW_PLUS_DEFAULT_WAIT_TIMEOUT_S` | `25` | `wait_jobs` 默认等待秒数（上限 35） |
| `GFLOW_PLUS_WARM_BROWSER_TTL_S` | `120` | 单账号空闲热浏览器保活秒数（`0` 关闭复用） |
| `GFLOW_PLUS_HIDE_SYNC_TOOLS` | `0` | 设为 `1` 隐藏原版阻塞式生成/放大 MCP 工具，避免 AI 误调超时 |
| `GFLOW_PLUS_STATE_DB_PATH` | `<GFLOW_CLI_HOME>/plus_state.db` | Plus 自己的状态库，不动官方 `gflow.db` |
| `GFLOW_PLUS_HIDE_TASKBAR` | `1` | `0` 关闭任务栏隐藏 |

## 状态文件

- `<GFLOW_CLI_HOME>/plus_state.db`：任务记录和各账号余额、冷却。
- `<GFLOW_CLI_HOME>/plus_owners/*.lock`：每个运行中的 gflow 进程持有一把内核文件锁，用来判断任务的所属进程是否还活着；进程退出后由下一个进程清理。

## 跟进上游

- `develop` 只跟踪 `upstream/develop`，不放自己的改动；`plus/main` 是 Plus 的开发分支。
- 新能力只加在 `src/gflow_cli/plus/` 和 `tests/plus/`；对官方文件的修复单独提交，便于回推 PR。
