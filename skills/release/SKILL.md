---
name: release
version: "2.0"
description: >
  发布 gflow-plus 新版本：改版本号、写 CHANGELOG、打 vX.Y.Z 标签、建 GitHub Release、
  重装本机生产环境并更新 magpie 同步的版本。
---

# `/gflow:release` — 发布新版本

gflow-plus 只在 GitHub 上发布，**不发 PyPI、不登记 MCP Registry**。主分支是 `plus/main`，
没有 develop/main 双分支，也没有 release 分支和回合并。

## 输入

- 新版本号 `X.Y.Z`（语义化版本：修 bug 加第三位，加功能加第二位，不兼容改动加第一位）。
  没给就根据 CHANGELOG 的 `[Unreleased]` 内容建议一个，先问用户。

## 步骤

**1. 看要发布什么。** `git log --oneline v<上个版本>..HEAD`，对照 `CHANGELOG.md` 的 `[Unreleased]`。

**2. 确认工作区干净、在 `plus/main` 上、和 origin 同步。** `git status -sb` 不能有未提交改动；
`git fetch origin` 后 `plus/main` 不能落后。

**3. 跑完整检查。** 按 `/gflow:check` 执行，全部通过才继续。改过 Flow 相关功能的，
还要跑对应的 e2e（见 AGENTS.md「测试」）。

**4. 写 CHANGELOG。** 把 `[Unreleased]` 改成 `## [X.Y.Z] - YYYY-MM-DD`，上面留一个新的空
`[Unreleased]`。

**5. 确认没有任务在跑。** `gflow plus pool status` 显示进行中的任务为 0，后面重装才安全。

**6. Bump the shared release version — SEVEN sites, and they must all agree.**

`check_repo_hygiene._check_version_agreement` 和下面列出的测试会检查这些位置版本一致；
`tests/test_release_version_sites.py` 直接解析这张表，增删版本位置时同步改表。

| # | Site | Gate |
|---|---|---|
| 1 | `pyproject.toml` `[project].version` | `check_repo_hygiene._check_version_agreement` |
| 2 | `src/gflow_cli/__init__.py` `__version__` | same |
| 3 | `.codex-plugin/plugin.json` `"version"` | same |
| 4 | `uv.lock` (the `gflow-plus` package block) | same — 改完运行 `uv lock` 重新生成 |
| 5 | `plugins/gflow/.claude-plugin/plugin.json` `"version"` | `tests/test_plugin_manifests.py` |
| 6 | `docker/Dockerfile` `ARG GFLOW_VERSION` | `tests/test_dockerfile_version_pin.py` |
| 7 | `lhm.plugin.json` `"version"` | `tests/test_release_version_sites.py` |

<!-- end-version-sites -->

改完运行 `uv lock`、`uv sync --extra dev`，再跑一次
`uv run python scripts/ci/check_repo_hygiene.py` 和
`.venv\Scripts\python.exe -m pytest -q tests/test_release_version_sites.py`。

**7. 提交并推送。**

```powershell
git add -A
git commit -m "chore(release): vX.Y.Z"
git push origin plus/main
```

**8. 打标签并建 GitHub Release。** 标签用 `vX.Y.Z`。仓库里已经没有会被标签触发的发布任务，
推标签只会产生一个 Release。

```powershell
git tag -a vX.Y.Z -m "gflow-plus X.Y.Z"
git push origin vX.Y.Z
gh release create vX.Y.Z --repo MOMO0302-02/gflow-plus --title "gflow-plus X.Y.Z" --notes-file <本版 CHANGELOG 摘录>
```

**9. 重装本机生产环境。** 先请用户在 magpie 资源库里关掉 `gflow-plus` 开关（运行中的
`gflow.exe` 会被 Windows 锁住），确认进程都退出后：

```powershell
uv pip install --python D:\Infra\apps\gflow-cli\.venv-plus\Scripts\python.exe --reinstall-package gflow-plus --no-cache D:\Workspace\gflow-cli
```

`--no-cache` 不能省：uv 按文件名缓存构建结果，同版本号重建时会装回旧包。
装完验收：`gflow --version` 显示新版本，`gflow plus health` 三个账号都正常，再请用户打开开关，
并通过 magpie 调一次 `gflow-plus__gflow_plus_pool_status`。

**10. 更新 magpie 同步给其他电脑的版本。** 同步命令里写死了标签，要换成新标签。先备份
`C:\Users\MOMO\.config\magpie\library.json`，然后分三条命令执行，每条单独运行：

```powershell
magpie library mcp rm gflow-plus
magpie library mcp add gflow-plus agents=claude,codex,opencode "env=GFLOW_CLI_HOME=D:\Infra\apps\gflow-cli\home" "env=GFLOW_CLI_OUTPUT_DIR=D:\cnshopper" "env=PLAYWRIGHT_BROWSERS_PATH=D:\Infra\apps\gflow-cli\browsers" timeout=300 -- uvx --from git+https://github.com/MOMO0302-02/gflow-plus@vX.Y.Z gflow mcp run
magpie library mcp here gflow-plus -- D:\Infra\apps\gflow-cli\.venv-plus\Scripts\gflow.exe mcp run
```

读回 `library.json` 里的 `gflow-plus` 条目确认：`command` 是本机 `.venv-plus\Scripts\gflow.exe`，
`agents` 是 claude、codex、opencode 三个，`shared.args` 里是新标签。

- 同名 `add` 不会覆盖已有条目，所以要先 `rm`。
- `here` 紧跟在 `add` 后面执行时曾经没生效，也没有任何输出；单独再执行一次才写进去。
- 在 magpie 界面里把开关关掉再打开，曾经丢掉 `shared` 部分和 claude、codex 两个 agent（实测
  2026-11-25）。重装期间关过开关的，打开后一定按上面重新写一遍并读回确认。

**11. 汇报。** 版本号、Release 链接、本机验收结果、magpie 同步的标签，以及没做成的步骤。

## 注意

- 不要发 PyPI，不要运行 `pip install gflow-cli`：PyPI 上的 `gflow-cli` 是原项目，会覆盖 plus 版。
- 标签一旦推送就不要改；发错了就发一个新的修订版本。
