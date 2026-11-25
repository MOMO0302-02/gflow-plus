# AGENTS.md — gflow-plus

> 给 AI 编程助手的总入口，先读这个文件。Claude Code 读 [CLAUDE.md](CLAUDE.md)，它会导入本文件。

## 项目是什么

- **gflow-plus**：从开源项目 [ffroliva/gflow-cli](https://github.com/ffroliva/gflow-cli) v0.106.0
  分出来、**独立维护**的版本，仓库 [MOMO0302-02/gflow-plus](https://github.com/MOMO0302-02/gflow-plus)。
  **不再合并原项目的更新**，所有改动都在本仓库完成。
- 功能：Google Flow 的命令行 + MCP 服务器（Veo / Omni 视频，Nano Banana / Imagen 图片），
  驱动的是用户自己已登录的真实 Chrome。在原项目基础上增加了多账号调度（`src/gflow_cli/plus/`，
  `gflow plus …` 命令，`gflow_plus_*` MCP 工具）。
- 技术栈：Python 3.11+ · `uv` · `hatchling` · Playwright Chromium · `pyright` strict · `ruff` · `pytest`。
  `playwright` 和 `mcp` 在 `pyproject.toml` 里有版本上限，升级要单独验证，不能顺手改。
- 名字：发行包叫 `gflow-plus`，命令仍是 `gflow`（另有别名 `gflow-plus`、`flow`），Python 包仍是
  `gflow_cli`（改它牵涉全部代码，不改）。
- 用户面向的中文介绍在 [.github/README.md](.github/README.md)（GitHub 首页显示它）；根目录
  `README.md` 和 `docs/` 是原项目的英文文档，内容以 v0.106.0 为准，改到相关功能时顺手更正。

## 分支与仓库

- 唯一主分支 `plus/main`，GitHub 默认分支也是它。没有 develop / main 双分支，没有 upstream 远程。
- 小改动可以直接在 `plus/main` 提交；大改动开 `feature/…`、`bugfix/…` 分支，做完合回 `plus/main`。
- 提交信息用 Conventional Commits（`feat:`、`fix:`、`docs:`、`chore:` …），不加 AI 署名。
- 推送、打标签、发 Release、改 GitHub 仓库设置都属于外部操作，先取得用户确认。

## 本机生产环境（改动前必须知道）

| 项目 | 位置 |
|---|---|
| 生产环境（magpie 里的 `gflow-plus` 在本机用它） | `D:\Infra\apps\gflow-cli\.venv-plus` |
| 账号与数据 `GFLOW_CLI_HOME` | `D:\Infra\apps\gflow-cli\home`（3 个已登录账号） |
| 浏览器 `PLAYWRIGHT_BROWSERS_PATH` | `D:\Infra\apps\gflow-cli\browsers` |
| 输出目录 `GFLOW_CLI_OUTPUT_DIR` | `D:\cnshopper` |

- 开发用本仓库自己的 `.venv`（`uv sync --extra dev`），**不要在开发过程中动生产环境**。
- magpie 里的 `gflow-plus`：同步给其他电脑的是 `uvx --from git+https://github.com/MOMO0302-02/gflow-plus@vX.Y.Z gflow mcp run`；
  本机用 `magpie library mcp here` 覆盖成 `.venv-plus\Scripts\gflow.exe mcp run`。
- 重装生产环境前先让用户在 magpie 里关掉 `gflow-plus`：运行中的 `gflow.exe` 会被 Windows 锁住。
  重装命令必须带 `--no-cache`（uv 按文件名缓存，同版本号会装回旧包）。完整流程见
  [`skills/release/SKILL.md`](skills/release/SKILL.md)。
- **永远不要**运行 `pip install gflow-cli` 或从 PyPI 升级：PyPI 上的 `gflow-cli` 是原项目，会覆盖本版本。
  本版本已删除 `gflow update` 命令和新版本提示。

## 技能（skills/）怎么用

`skills/<name>/SKILL.md` 是纯 Markdown 的工作流程，任何 AI 工具都能读；`.claude/commands/gflow/*.md`
只是 Claude Code 的入口。下表的情况出现时，先读对应技能再动手。

| 你要做的事 | 先读 |
|---|---|
| 断言 Flow 的某个功能坏了、没有、做不到 | [`spike`](skills/spike/SKILL.md)：先到真实页面取证 |
| 解释一个现象或修一个原因还没查清的 bug | [`issue-resolve`](skills/issue-resolve/SKILL.md) 的 Bug Lane |
| 改传输层、登录、选择器或数据结构 | [`predict`](skills/predict/SKILL.md) |
| 做新功能 | [`scenario`](skills/scenario/SKILL.md) → [`plan`](skills/plan/SKILL.md) |
| 接着上次的工作 | [`status`](skills/status/SKILL.md) |
| 提交任何改动 | [`check`](skills/check/SKILL.md) |
| 说某个生成功能已经能用 | [`live-verify`](skills/live-verify/SKILL.md) |
| 碰登录或 reCAPTCHA | [`known-issues`](skills/known-issues/SKILL.md) |
| 写只能在浏览器里发生的场景测试 | [docs/E2E_TESTING.md](docs/E2E_TESTING.md) |
| 测试或 CI 太慢 | [`optimize`](skills/optimize/SKILL.md) |
| 发布新版本 | [`release`](skills/release/SKILL.md) |
| 帮用户跑 `gflow` 命令、做多镜头视频 | [`gflow-cli`](skills/gflow-cli/SKILL.md)、[`video-production`](skills/video-production/SKILL.md) |

**技能里继承自原项目的流程不再适用**：凡是提到往 `develop` 提 PR、回合并 `main`、SonarCloud、
发布到 PyPI 或 MCP Registry、原项目 issue 编号的步骤，一律跳过或按本文件替换（分支用 `plus/main`，
发布按 `release` 技能）。`sonar`、`pr-council-review`、`llm-council`、`doc-review`、`changelog`
保留作参考，需要时再用。

## 提交前检查

按顺序运行，全部通过才能提交（推荐直接按 [`check`](skills/check/SKILL.md) 技能做，它还包含
CLI 和 MCP 两边一致性的人工核对）：

```bash
export PYTHONUTF8=1          # Windows PowerShell: $env:PYTHONUTF8 = "1"
uv run python scripts/ci/check_repo_hygiene.py
uv run python scripts/ci/check_size.py
uv run python scripts/ci/check_doc_links.py
uv run python scripts/ci/check_website_docs_pii.py
uv run python scripts/ci/generate_website_docs.py --check
uv run python scripts/ci/check_council_memory.py
uv run python scripts/ci/generate_plugin_skills.py --check
uv run ruff check src tests
uv run ruff format --check src tests
uv run pyright src
uv run python -m pytest -q --cov=gflow_cli
```

- 这份清单和 `.github/workflows/ci.yml` 保持一致；改其中一个就同步改另一个。
- `ruff format` 只对 `src tests` 运行，不要格式化 `scripts/`。
- 本地跑全部测试用 `-n 8 --dist worksteal -m "not live and not e2e and not smoke"`（约 1 分钟）；
  Windows 上直接用 `.venv\Scripts\python.exe -m pytest`。
- `website/docs/` 是 `docs/` 生成的副本，`plugins/gflow/skills/` 是 `skills/` 的副本：改源文件后
  运行对应的生成脚本，不要手改副本。
- Documentation is a first-class deliverable. 改了行为、配置或流程，就同步改相关文档，或说明为什么不用改。

## 测试

- 先写测试再改代码（TDD），整体覆盖率不低于 80%。
- **E2E is the decisive layer, and it is required for every behavior change that touches a Flow surface.**
  离线测试只能证明我们的代码按我们以为的方式运行，证明不了 Flow 还和抓包时一样。改到 Flow 相关
  功能，就运行覆盖它的 `tests/e2e/` 测试；没有就补一个。`/gflow:live-verify` does **not** satisfy
  this: it drives CLI commands by hand into a gitignored note and never runs `pytest -m e2e`.
- 只有拿不到的外部条件（积分用完、账号不可用、没有的硬件）才能作为没跑的理由，并要写明。
  “单元测试覆盖了”“只是薄薄一层”不算理由。
- e2e 需要 `GFLOW_CLI_E2E_PROFILE` 和 `-m e2e`，默认不会运行。按花费分子标记：`e2e_auth`、
  `e2e_image`、`e2e_data`、`e2e_media`、`e2e_telemetry` 等不花积分；`e2e_video` 会花 Veo 积分，
  要 `GFLOW_CLI_E2E_RUN_VIDEO=1` 才运行。**任何花积分的运行都先问用户。**
- 贴测试输出前，去掉账号、token、cookie。

## 代码规范

- 处处写类型标注，`src/gflow_cli` 用 `pyright` strict。
- 只用 `structlog` 记日志，`src/` 里不要 `print()` 或 `import logging`。
- Errors as RFC 9457 Problem Details with stable per-class exit codes (3–45 by default;
  `GFLOW_CLI_EXIT_CODES=2` opts in to the stable core, see `docs/ERRORS.md`; e.g. 11 is
  `ConfigurationError`, 37 `InsufficientCreditsError`, 39 `FlowAccessUnavailableError`,
  45 `CreditCapExceededError`)。完整对照表在 `src/gflow_cli/errors.py::EXIT_CODE_MAP`；
  `gflow doctor` 的 33 表示“发现问题”，不是错误。
- 行宽 100，`ruff` 管格式和 import 排序。
- 最少代码：不写没人用的抽象、开关和兜底；能用标准库就不自己造。
- **CLI 和 MCP 两边对称**：给 CLI 命令加参数，就同步改对应的 MCP 工具；做不到的要在
  `tests/mcp/test_cli_parity.py` 里写明原因。MCP 是另一个入口，要单独验证。
- **选择器不依赖界面文字**：`src/gflow_cli/api/transports/` 里的 DOM 选择器只用结构特征
  （链接、图标字形、ARIA role、层级关系），不用翻译后的按钮文字。仅有的例外是在两种以上语言里
  实测不变的格式标记（如放大菜单里的 `2K`、`1080p`，时长单选里的数字），没匹配上要报错退出，不能点错。
- **只说观察到的事实**：可以说 Flow 这次给的是哪个域名，不要因为账号在 flow.google.com 上就断言
  “某功能只有 labs 有”。我们驱动不了的功能，写“还没移植到新版界面”。`migrated_route()` 这类按实际
  域名分支的代码保留。

## 抓取真实页面（spike）

- 进程内抓取：`scripts/dev/spike_*.py`；人工操作 + HAR：`scripts/dev/har-spike/`。先用前者，
  够不到再用后者。流程见 [`spike`](skills/spike/SKILL.md)。
- 抓到的 HAR、DOM 带 token 和 cookie，只放在已忽略的 `scripts/dev/_spike_out/` 和
  `scripts/dev/har-spike/artifacts/`，绝不提交；结论写进 `docs/superpowers/spikes/`。
- 看到 `ProfileLockedError` 说明账号正被别的进程用着，等它或换账号，不要杀 Chrome 进程。

## 发布

按 [`release`](skills/release/SKILL.md) 技能：改七个版本位置 → 提交推送 → 打 `vX.Y.Z` 标签 →
`gh release create` → 重装本机生产环境 → 更新 magpie 同步的标签。只在 GitHub 发布。

## 去哪里找

- 架构：[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)
- 引用素材（`@名字`、`--reference-entity`、`--ref`）：[docs/REFERENCE_STRATEGIES.md](docs/REFERENCE_STRATEGIES.md)
- 全部文档索引：[docs/INDEX.md](docs/INDEX.md)
- 已知问题（碰登录、reCAPTCHA 前必读）：[KNOWN_ISSUES.md](KNOWN_ISSUES.md)
- 发布摘要：[RELEASE.md](RELEASE.md)
