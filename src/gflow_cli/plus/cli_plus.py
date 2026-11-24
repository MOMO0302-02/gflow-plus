"""CLI commands for gflow-cli Plus (``gflow plus ...``)."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any, cast

import click
from rich.console import Console
from rich.table import Table

from gflow_cli import json_output
from gflow_cli._cli_helpers import spends_credits
from gflow_cli.plus.pool.manager import get_pool_manager
from gflow_cli.plus.pool.models import JobKind, JobStatus
from gflow_cli.plus.win_stealth import hide_offscreen_chrome_windows

console = Console()


@click.group(name="plus")
def plus() -> None:
    """Multi-account scheduling, non-blocking batches, and Windows stealth tools."""


@spends_credits(False)
@plus.command("health")
@click.option(
    "--refresh/--no-refresh",
    default=True,
    show_default=True,
    help="Refresh balances via wire read before reporting.",
)
@click.option(
    "--renew",
    is_flag=True,
    default=False,
    help="Headlessly renew expired sessions before checking credits.",
)
@click.option("--json", "as_json", is_flag=True, default=False, help="Output JSON.")
def plus_health(refresh: bool, renew: bool, as_json: bool) -> None:
    """Inspect all saved accounts' health, credits, cooldowns, and recommended lanes."""
    manager = get_pool_manager()
    if refresh:
        report = asyncio.run(manager.scheduler.refresh_credits(force=True, renew=renew))
    else:
        report = manager.scheduler.health_report()

    if as_json:
        click.echo(json_output.dumps(report))
        return

    table = Table(title="gflow plus — Account Pool Health")
    table.add_column("Profile", style="cyan")
    table.add_column("Healthy")
    table.add_column("Credits", justify="right")
    table.add_column("Busy")
    table.add_column("Cooldown")
    table.add_column("Last Error")
    for item in report["profiles"]:
        cd = f"{item['cooldown_remaining_s']}s" if item["cooldown_remaining_s"] > 0 else "-"
        table.add_row(
            str(item["profile"]),
            "[green]yes[/green]" if item["healthy"] else "[red]no[/red]",
            str(item["credits"]) if item["credits"] is not None else "?",
            "yes" if item["busy"] else "no",
            cd,
            str(item["last_error"] or "-"),
        )
    console.print(table)
    console.print(
        f"Total credits: [bold]{report['total_credits']}[/bold] | "
        f"Recommended image: [cyan]{report['recommended_image_profile']}[/cyan] | "
        f"Recommended video: [cyan]{report['recommended_video_profile']}[/cyan]"
    )


@plus.group("pool")
def pool_group() -> None:
    """Inspect multi-account pool lanes and async background jobs."""


@spends_credits(False)
@pool_group.command("status")
@click.option("--json", "as_json", is_flag=True, default=False, help="Output JSON.")
def pool_status(as_json: bool) -> None:
    """Show cached lane health and recent job summary (no browser/network call)."""
    manager = get_pool_manager()
    report = manager.scheduler.health_report()
    recent = manager.list_jobs(limit=20)
    payload: dict[str, Any] = {
        **report,
        "recent_jobs_count": len(recent),
        "active_jobs": sum(1 for j in recent if j.status in ("queued", "running")),
    }
    if as_json:
        click.echo(json_output.dumps(payload))
        return
    console.print(
        f"Healthy lanes: {payload['healthy_profiles']}/{payload['total_profiles']} | "
        f"Active jobs: {payload['active_jobs']} | "
        f"Total credits (cached): {payload['total_credits']}"
    )


@spends_credits(False)
@pool_group.command("pick")
@click.option(
    "--kind",
    type=click.Choice(["image", "video"]),
    default="video",
    show_default=True,
    help="Task kind to pick the best lane for.",
)
@click.option("--json", "as_json", is_flag=True, default=False, help="Output JSON.")
def pool_pick(kind: JobKind, as_json: bool) -> None:
    """Select the currently recommended profile for an image or video job."""
    manager = get_pool_manager()
    picked = manager.scheduler.pick_profile(kind)
    data = {"kind": kind, "profile": picked}
    if as_json:
        click.echo(json_output.dumps(data))
        return
    console.print(picked or "[red]No healthy profile available[/red]")


@spends_credits(False)
@pool_group.command("jobs")
@click.option("--batch-id", default=None, help="Filter by batch_id.")
@click.option(
    "--status",
    "job_status",
    type=click.Choice(["queued", "running", "succeeded", "failed", "indeterminate", "cancelled"]),
    default=None,
    help="Filter by job status.",
)
@click.option("--limit", default=20, show_default=True, type=int, help="Max rows to return.")
@click.option("--json", "as_json", is_flag=True, default=False, help="Output JSON.")
def pool_jobs(
    batch_id: str | None,
    job_status: JobStatus | None,
    limit: int,
    as_json: bool,
) -> None:
    """List async pool jobs and their delivery ledger."""
    manager = get_pool_manager()
    jobs = manager.list_jobs(batch_id=batch_id, status=job_status, limit=limit)
    rows = [j.to_public_dict() for j in jobs]
    if as_json:
        click.echo(json_output.dumps({"jobs": rows, "count": len(rows)}))
        return
    table = Table(title="gflow plus — Pool Jobs")
    table.add_column("Job ID", style="cyan")
    table.add_column("Label")
    table.add_column("Type")
    table.add_column("Profile")
    table.add_column("Status")
    table.add_column("Output / Error")
    for row in rows:
        res: dict[str, Any] = cast("dict[str, Any]", row["result"]) if row["result"] else {}
        err: dict[str, Any] = cast("dict[str, Any]", row["error"]) if row["error"] else {}
        paths = cast("list[str]", res.get("output_paths", []))
        detail = ", ".join(paths) or str(err.get("detail") or "-")
        table.add_row(
            str(row["job_id"]),
            str(row["label"] or "-"),
            str(row["task_type"]),
            str(row["profile"] or "-"),
            str(row["status"]),
            detail[:80],
        )
    console.print(table)


def _load_manifest_items(manifest: Path) -> list[dict[str, Any]]:
    raw = json.loads(manifest.read_text(encoding="utf-8"))
    if isinstance(raw, dict):
        mapping = cast("dict[str, object]", raw)
        if "items" in mapping and isinstance(mapping["items"], list):
            return cast("list[dict[str, Any]]", mapping["items"])
    if isinstance(raw, list):
        return cast("list[dict[str, Any]]", raw)
    raise click.BadParameter("Manifest must be a JSON list or an object containing 'items'.")


@spends_credits(True)
@plus.command("run-batch")
@click.argument("manifest", type=click.Path(exists=True, dir_okay=False, path_type=Path))
@click.option(
    "--skip-if-exists/--no-skip-if-exists",
    default=True,
    show_default=True,
    help="Skip items whose output_file already exists and is non-empty.",
)
@click.option("--json", "as_json", is_flag=True, default=False, help="Output JSON.")
def plus_run_batch(manifest: Path, skip_if_exists: bool, as_json: bool) -> None:
    """Run a JSON batch manifest across all healthy profiles in parallel."""
    items = _load_manifest_items(manifest)

    async def _run() -> dict[str, Any]:
        manager = get_pool_manager()
        submitted = await manager.submit_batch(items, skip_if_exists=skip_if_exists)
        batch_id = str(submitted["batch_id"])
        while True:
            state = await manager.wait_jobs(batch_id=batch_id, timeout_s=25.0)
            if state["done"]:
                return manager.batch_summary(batch_id)

    summary = asyncio.run(_run())
    if as_json:
        click.echo(json_output.dumps(summary))
        return
    counts = summary["counts"]
    console.print(
        f"Batch [cyan]{summary['batch_id']}[/cyan] done: "
        f"succeeded={counts['succeeded']} failed={counts['failed']} "
        f"indeterminate={counts['indeterminate']}"
    )


@spends_credits(False)
@plus.command("hide-taskbar")
@click.option("--json", "as_json", is_flag=True, default=False, help="Output JSON.")
def plus_hide_taskbar(as_json: bool) -> None:
    """Hide Windows taskbar entries for off-screen gflow Chrome windows."""
    count = hide_offscreen_chrome_windows()
    if as_json:
        click.echo(json_output.dumps({"hidden_windows": count}))
        return
    console.print(f"Hidden off-screen Chrome taskbar windows: {count}")
