from __future__ import annotations

from pathlib import Path
from typing import Optional, Sequence

import orjson
import typer
from rich.console import Console
from rich.table import Table

from .api import validate
from .drift import DriftSnapshot, compare_snapshots, snapshot as create_snapshot
from .exporters import (
    from_contract_to_pydantic,
    from_contract_to_sql,
    from_contract_to_typeddict,
    to_dbt_tests,
    to_gx_suite,
)
from .inference import infer_contract
from .linting import suggest_improvements
from .report import ValidationReport
from .schema import Contract, load_contract, save_contract
from .utils import read_dataframe
from .versioning import compare_contracts, is_breaking_change


app = typer.Typer(help="df-contracts command line interface")
console = Console()


class CLIState:
    def __init__(self) -> None:
        self.contract: Contract | None = None
        self.inference_suggestions: list[str] = []


def get_state(ctx: typer.Context) -> CLIState:
    if ctx.obj is None:
        ctx.obj = CLIState()
    return ctx.obj


@app.callback()
def main(ctx: typer.Context) -> None:
    get_state(ctx)


@app.command()
def init(
    path: Path,
    name: str = typer.Option("dataset", help="Contract name"),
    version: str = typer.Option("0.1.0", help="Contract version"),
    sample: Optional[float] = typer.Option(None, help="Sample fraction"),
    show_suggestions: bool = typer.Option(True, help="Display inferred suggestions"),
) -> None:
    df = read_dataframe(path)
    result = infer_contract(df, name=name, version=version)
    state = get_state(typer.get_current_context())
    state.contract = result.contract
    state.inference_suggestions = [suggestion.message for suggestion in result.suggestions]
    typer.echo(result.contract.to_json())
    if show_suggestions and result.suggestions:
        console.rule("Suggestions")
        for suggestion in result.suggestions:
            console.print(f"- [bold]{suggestion.column}[/bold]: {suggestion.message}")


@app.command()
def save(
    out: Path = typer.Option(..., exists=False, help="Output contract file"),
    contract_path: Optional[Path] = typer.Option(None, help="Existing contract to save"),
) -> None:
    state = get_state(typer.get_current_context())
    contract = state.contract if contract_path is None else load_contract(contract_path)
    if contract is None:
        raise typer.BadParameter("No contract available. Use init or provide --contract.")
    save_contract(contract, out)
    console.print(f"Saved contract to [green]{out}[/green]")


@app.command()
def check(
    path: Path,
    contract: Path = typer.Option(..., help="Contract file"),
    profile: str = typer.Option("prod", help="Profile name"),
    report: Optional[Path] = typer.Option(None, help="JSON report path"),
    html: Optional[Path] = typer.Option(None, help="HTML report output"),
    junit: Optional[Path] = typer.Option(None, help="JUnit XML output"),
    pr_md: Optional[Path] = typer.Option(None, help="GitHub PR markdown output"),
    sample: Optional[float] = typer.Option(None, help="Sample fraction"),
    by: Optional[str] = typer.Option(None, help="Comma-separated stratification columns"),
    max_examples: int = typer.Option(20, help="Maximum examples in report"),
    with_snapshot: bool = typer.Option(False, help="Embed drift snapshot"),
) -> None:
    df = read_dataframe(path)
    by_cols: Sequence[str] | None = by.split(",") if by else None
    contract_obj = load_contract(contract)
    validation = validate(
        df,
        contract_obj,
        profile=profile,
        sample=sample,
        by=by_cols,
        max_examples=max_examples,
        with_snapshot=with_snapshot,
    )
    _render_report(validation)
    if report:
        report.write_text(validation.to_json())
    if html:
        html.write_text(validation.to_html(df=df))
    if junit:
        junit.write_text(validation.to_junit())
    if pr_md:
        pr_md.write_text(validation.format_for_github_pr())
    raise typer.Exit(code=0 if validation.ok else 1)


@app.command("diff-contracts")
def diff_contracts(
    old: Path,
    new: Path,
    json: Optional[Path] = typer.Option(None, help="Write diff JSON to path"),
    fail_on_breaking: bool = typer.Option(False, help="Exit with error on breaking changes"),
) -> None:
    old_contract = load_contract(old)
    new_contract = load_contract(new)
    diff = compare_contracts(old_contract, new_contract)
    console.print_json(data=diff)
    if json:
        json.write_text(orjson.dumps(diff, option=orjson.OPT_INDENT_2).decode())
    if fail_on_breaking and is_breaking_change(diff):
        raise typer.Exit(code=1)


@app.command()
def lint(
    path: Path,
    contract: Path = typer.Option(..., help="Contract file"),
    apply_suggestions: bool = typer.Option(False, help="Apply suggestions"),
    out: Optional[Path] = typer.Option(None, help="Output path for updated contract"),
) -> None:
    df = read_dataframe(path)
    contract_obj = load_contract(contract)
    report = suggest_improvements(contract_obj, df)
    if report.is_clean():
        console.print("[green]No lint suggestions[/green]")
    else:
        table = Table(title="Lint suggestions")
        table.add_column("Severity")
        table.add_column("Column")
        table.add_column("Message")
        table.add_column("Diff")
        for suggestion in report.suggestions:
            table.add_row(suggestion.severity, suggestion.location, suggestion.message, suggestion.diff)
        console.print(table)
    if apply_suggestions and report.suggestions:
        updated = report.apply(contract_obj)
        destination = out or contract
        save_contract(updated, destination)
        console.print(f"[green]Updated contract written to {destination}[/green]")


@app.command()
def snapshot(
    path: Path,
    out: Path = typer.Option(..., help="Snapshot JSON path"),
    sample: Optional[float] = typer.Option(None, help="Sample fraction"),
) -> None:
    df = read_dataframe(path, sample=sample)
    snap = create_snapshot(df)
    out.write_text(snap.to_json())
    console.print(f"Snapshot saved to [green]{out}[/green]")


@app.command()
def drift(
    path: Path,
    ref: Path = typer.Option(..., help="Reference snapshot JSON"),
    report: Optional[Path] = typer.Option(None, help="Write drift report JSON"),
    html: Optional[Path] = typer.Option(None, help="Write drift HTML report"),
) -> None:
    df = read_dataframe(path)
    cur_snapshot = create_snapshot(df)
    ref_data = orjson.loads(ref.read_text())
    ref_snapshot = DriftSnapshot.from_dict(ref_data)
    drift_report = compare_snapshots(ref_snapshot, cur_snapshot)
    console.print_json(data=drift_report.as_dict())
    if report:
        report.write_text(drift_report.to_json())
    if html:
        html.write_text(drift_report.to_html())


@app.command("export-types")
def export_types(
    contract: Path,
    kind: str = typer.Option("typedict", help="typedict or pydantic"),
    out: Path = typer.Option(..., help="Output file"),
) -> None:
    contract_obj = load_contract(contract)
    if kind == "typedict":
        code = from_contract_to_typeddict(contract_obj)
    elif kind == "pydantic":
        code = from_contract_to_pydantic(contract_obj)
    else:
        raise typer.BadParameter("kind must be 'typedict' or 'pydantic'")
    out.write_text(code)
    console.print(f"[green]Exported {kind} definitions to {out}[/green]")


@app.command()
def sql(
    contract: Path,
    dialect: str = typer.Option("postgres", help="SQL dialect"),
    out: Path = typer.Option(..., help="Output SQL file"),
) -> None:
    contract_obj = load_contract(contract)
    sql_text = from_contract_to_sql(contract_obj, dialect=dialect)
    out.write_text(sql_text)
    console.print(f"[green]SQL DDL written to {out}[/green]")


@app.command("export-dbt")
def export_dbt(
    contract: Path,
    table: str = typer.Option(..., help="dbt model name"),
    out: Path = typer.Option(..., help="Output YAML"),
) -> None:
    contract_obj = load_contract(contract)
    yaml = to_dbt_tests(contract_obj, table)
    out.write_text(yaml)
    console.print(f"[green]dbt tests written to {out}[/green]")


@app.command("export-gx")
def export_gx(
    contract: Path,
    out: Path = typer.Option(..., help="Output JSON"),
) -> None:
    contract_obj = load_contract(contract)
    suite = to_gx_suite(contract_obj)
    out.write_text(orjson.dumps(suite, option=orjson.OPT_INDENT_2).decode())
    console.print(f"[green]Great Expectations suite written to {out}[/green]")


def _render_report(report: ValidationReport) -> None:
    report.to_rich_console(console=console)


if __name__ == "__main__":  # pragma: no cover
    app()
