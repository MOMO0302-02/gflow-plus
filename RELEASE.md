# 发布流程

gflow-plus 只在 GitHub 上发布（`vX.Y.Z` 标签 + GitHub Release），不发 PyPI、不登记 MCP Registry。
完整步骤在 [`skills/release/SKILL.md`](skills/release/SKILL.md)，这里是摘要：

1. `plus/main` 干净、和 origin 同步，`/gflow:check` 全部通过。
2. CHANGELOG 的 `[Unreleased]` 改成 `[X.Y.Z] - 日期`。
3. 七个版本位置一起改：`pyproject.toml`、`src/gflow_cli/__init__.py`、`.codex-plugin/plugin.json`、
   `uv.lock`（运行 `uv lock`）、`plugins/gflow/.claude-plugin/plugin.json`、`docker/Dockerfile`、
   `lhm.plugin.json`。
4. 提交、推送，打 `vX.Y.Z` 标签，`gh release create`。
5. 关掉 magpie 里的 `gflow-plus`，用 `--reinstall-package gflow-plus --no-cache` 重装
   `D:\Infra\apps\gflow-cli\.venv-plus`，验收后再打开。
6. 把 magpie 同步给其他电脑的 `uvx --from git+…@vX.Y.Z` 换成新标签。

## Prerelease Versus Full Release

需要先试用的版本，用 `vX.Y.ZrcN` 标签，并在 `gh release create` 时加 `--prerelease`。
预发布不更新 magpie 同步的版本，只在本机试用；确认没问题后再发正式的 `vX.Y.Z`。
