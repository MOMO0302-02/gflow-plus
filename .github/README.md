# gflow-plus：Google Flow 多账号版 MCP 服务器与命令行

> gflow-plus 是从开源项目 [ffroliva/gflow-cli](https://github.com/ffroliva/gflow-cli) 分出来、独立维护的版本（基于官方 v0.106.0）。在官方功能的基础上，增加了**多个 Google 账号自动调度**：自动挑选空闲且积分足够的账号，同一账号同一时间只跑一个任务，遇到不扣费的报错会换账号重试。提交任务后立即返回，不会卡住 AI 客户端。

本项目与 Google 无关。它在真实的 Chrome 窗口里操作你自己已登录的 Flow，生成内容消耗的是你自己账号的 Flow 积分，并受 Google 服务条款约束。使用前请阅读 [免责声明](../DISCLAIMER.md) 和 [账号安全说明](../ACCOUNT_SAFETY.md)（英文）。

## 和官方版的区别

| | 官方 gflow-cli | gflow-plus |
|---|---|---|
| 安装来源 | PyPI 上的 `gflow-cli` | 本仓库的 GitHub 版本（不在 PyPI 上） |
| 账号 | 一次只用一个账号 | 多个已登录账号自动轮换 |
| 提交任务 | 等生成完才返回，长任务可能碰到 MCP 超时 | 立即返回 `job_id`，再分次等待结果 |
| 失败处理 | 报错后需要手动重试 | 不扣费的失败自动换账号；限流、风控、登录过期的账号自动冷却 |
| 官方功能 | 全部 | 保留到 v0.106.0 为止的全部功能，之后独立维护，不再同步官方 |

## plus 新增的 MCP 工具

| 工具 | 作用 | 花积分 |
|---|---|---|
| `gflow_plus_health` | 查所有账号的健康状态、积分、冷却时间、热浏览器和推荐账号 | 否 |
| `gflow_plus_pool_status` | 读缓存的账号状态和任务数，不开浏览器、不联网 | 否 |
| `gflow_plus_submit_image` | 提交出图任务（文生图 / 图生图），立即返回 `job_id` | 是 |
| `gflow_plus_submit_video` | 提交视频任务（文生视频 / 图生视频 / 参考图生视频），立即返回 `job_id`，可用 `max_credits` 设积分上限 | 是 |
| `gflow_plus_submit_upscale` | 非阻塞提交图片（2K/4K）或视频（1080p/720p/270p/4K）放大任务，自动路由到素材归属账号 | 视频 4K 是 |
| `gflow_plus_submit_extend` | 非阻塞提交单段视频续拍任务，自动路由到归属账号且绝不跨号重试 | 是 |
| `gflow_plus_submit_batch` | 一次提交多条图片/视频/放大/续拍任务，支持 `job:<label>` 依赖串联（如出图→自动 2K 放大或出图→自动首帧生视频） | 是 |
| `gflow_plus_job_status` | 查单个任务、整批任务或最近任务的状态和交付记录（列表默认精简回包省 Token，可设 `verbose=true` 看完整尝试日志） | 否 |
| `gflow_plus_wait_jobs` | 等待任务完成，每次最多等 35 秒左右，没完成就再调一次 | 否 |
| `gflow_plus_cancel_job` | 取消还在排队、没开始跑的任务 | 否 |
| `gflow_plus_retry_job` | 一键重跑失败/已取消的单个任务或整批失败项（已扣费或不确定状态的任务默认拒跑，防止双扣） | 是 |
| `gflow_plus_profile_control` | 运行时运维：清除冷却、无头续期登录态、临时禁用/恢复某账号、关闭空闲热浏览器 | 否 |
| `gflow_plus_list_media` | 跨所有账号检索本地已生成的图片/视频（返回 `media_id`、归属账号、项目 ID 和本地路径） | 否 |
| `gflow_plus_hide_taskbar` | 在 Windows 上立即隐藏后台 Chrome 窗口的任务栏图标 | 否 |

官方原有的工具（`gflow_generate_image`、`gflow_generate_video`、`gflow_get_credits` 等）全部保留，用法不变；如果希望彻底避免 AI 误调同步阻塞工具，可在环境变量中设置 `GFLOW_PLUS_HIDE_SYNC_TOOLS=1`。

## 安装

需要先装好 [uv](https://docs.astral.sh/uv/) 和 Google Chrome 正式版。以下以 Windows 为例，目录可以换成你自己的：

```powershell
uv venv D:\Infra\apps\gflow-cli\.venv-plus --python 3.12
uv pip install --python D:\Infra\apps\gflow-cli\.venv-plus\Scripts\python.exe "gflow-plus @ git+https://github.com/MOMO0302-02/gflow-plus@v1.0.0"
```

把 `v1.0.0` 换成 [版本列表](https://github.com/MOMO0302-02/gflow-plus/releases) 里的最新版本号。也可以不建环境，直接用 uvx 运行：

```powershell
uvx --from git+https://github.com/MOMO0302-02/gflow-plus@v1.0.0 gflow --version
```

> 不要用 `pip install gflow-cli` 安装或升级：PyPI 上的 `gflow-cli` 是原项目，会把本版本覆盖掉。本版本也没有 `gflow update` 命令。

## 首次设置

先设置环境变量，再装浏览器、登录账号：

```powershell
$env:GFLOW_CLI_HOME = "D:\Infra\apps\gflow-cli\home"                # 账号和数据目录
$env:PLAYWRIGHT_BROWSERS_PATH = "D:\Infra\apps\gflow-cli\browsers"  # 浏览器目录
python -m playwright install chromium                               # 用上面环境里的 python

gflow auth login --browser chrome --profile <账号名>   # 每个账号执行一次，在弹出的 Chrome 里手动登录
gflow auth use <账号名>                                # 设定默认账号
gflow plus health                                      # 检查所有账号（不花积分）
```

如果 Google 提示“检测到异常活动”，不要反复重试，等 10 分钟左右再登录。

## 接入 MCP 客户端

在 Claude Desktop、Claude Code、Cursor、Codex、magpie 等客户端里添加一个 stdio 服务器：

```json
{
  "gflow-plus": {
    "command": "D:\\Infra\\apps\\gflow-cli\\.venv-plus\\Scripts\\gflow.exe",
    "args": ["mcp", "run"],
    "env": {
      "GFLOW_CLI_HOME": "D:\\Infra\\apps\\gflow-cli\\home",
      "GFLOW_CLI_OUTPUT_DIR": "D:\\你的输出目录",
      "PLAYWRIGHT_BROWSERS_PATH": "D:\\Infra\\apps\\gflow-cli\\browsers"
    }
  }
}
```

生成的文件只能保存在 `GFLOW_CLI_OUTPUT_DIR` 目录里。

**推荐用法：** 用 `gflow_plus_submit_*` 提交任务，拿到 `job_id` 后用 `gflow_plus_wait_jobs` 等结果。花积分之前先确认模型、数量和预计积分，视频任务建议设置 `max_credits`。

## 命令行

| 命令 | 作用 |
|---|---|
| `gflow plus health` | 查所有账号的健康状态、积分、冷却时间和推荐账号 |
| `gflow plus pool status` | 查看缓存的账号状态和最近任务（不开浏览器、不联网） |
| `gflow plus pool pick` | 看当前出图 / 出视频会选哪个账号 |
| `gflow plus pool jobs` | 列出后台任务和交付记录 |
| `gflow plus pool retry` | 重跑失败或已取消的任务（默认防双扣） |
| `gflow plus pool profile-control` | 清冷却、无头续期、临时禁用/启用账号、关闭热浏览器 |
| `gflow plus media` | 跨账号检索本地已生成的图片与视频 |
| `gflow plus upscale` | 通过归属账号的热浏览器通道放大图片或视频 |
| `gflow plus extend` | 通过归属账号的热浏览器通道续拍一段视频 |
| `gflow plus run-batch` | 按 JSON 清单把一批（可带 `job:<label>` 依赖）任务分给健康账号执行 |
| `gflow plus hide-taskbar` | 隐藏后台 Chrome 窗口的任务栏图标（仅 Windows） |

官方原有的命令（`gflow image`、`gflow video`、`gflow credits` 等）全部可用，见 [官方使用文档](../docs/USAGE.md)（英文）。

## 调度设置

都是可选的环境变量，不设就用默认值：

| 环境变量 | 默认值 | 说明 |
|---|---|---|
| `GFLOW_PLUS_PROFILES` | 空（全部已登录账号） | 只让这些账号参与调度，逗号分隔 |
| `GFLOW_PLUS_EXCLUDED_PROFILES` | 空 | 不参与自动调度的账号，逗号分隔 |
| `GFLOW_PLUS_MIN_VIDEO_CREDITS` | `20` | 积分低于这个数的账号不接视频任务 |
| `GFLOW_PLUS_CREDIT_CACHE_TTL_S` | `300` | 积分余额缓存多少秒 |
| `GFLOW_PLUS_STAGGER_SECONDS` | `2.5` | 不同账号启动浏览器的最小间隔（秒） |
| `GFLOW_PLUS_RATE_LIMIT_COOLDOWN_S` | `180` | 遇到限流（HTTP 429）后冷却多少秒 |
| `GFLOW_PLUS_WAF_COOLDOWN_S` | `600` | 遇到“异常活动”风控拒绝后冷却多少秒 |
| `GFLOW_PLUS_AUTH_ERROR_COOLDOWN_S` | `1800` | 登录过期后冷却多少秒 |
| `GFLOW_PLUS_TRANSIENT_COOLDOWN_S` | `30` | 提交前的临时网络 / 页面错误冷却多少秒 |
| `GFLOW_PLUS_MAX_FAILOVER_ATTEMPTS` | `3` | 不扣费的失败最多换几个账号重试 |
| `GFLOW_PLUS_DEFAULT_WAIT_TIMEOUT_S` | `25` | `gflow_plus_wait_jobs` 默认每次等多少秒 |
| `GFLOW_PLUS_WARM_BROWSER_TTL_S` | `120` | 每个账号空闲 Chrome 热复用的保活秒数（设 `0` 关闭复用） |
| `GFLOW_PLUS_HIDE_SYNC_TOOLS` | `0` | 设为 `1` 时在 MCP 工具列表里隐藏原版阻塞式生图/生视频/放大工具，防止 AI 误调超时 |
| `GFLOW_PLUS_HIDE_TASKBAR` | `1` | 是否隐藏后台 Chrome 的任务栏图标（Windows） |

## 版本与分支

- `plus/main`：主分支，仓库首页显示的就是它。
- 每个版本都会打标签并发布在 [版本列表](https://github.com/MOMO0302-02/gflow-plus/releases) 里。

升级时重新执行安装命令，换成新的版本号，在 `uv pip install` 后面加上 `--reinstall-package gflow-plus`。升级前先确认没有任务在跑（`gflow plus pool status` 显示 0 个进行中的任务）。

## 许可

沿用原项目的 [MIT 许可证](../LICENSE)，原始代码版权归原作者 Flavio Oliva (ffroliva) 所有。原项目的英文文档见 [原版 README](../README.md) 和 [docs 目录](../docs/INDEX.md)，内容以 v0.106.0 为准，可能与本项目后续的改动不一致。
