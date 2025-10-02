from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from .api import compare, is_breaking_change, validate
from .inference import infer_contract
from .report import ValidationReport
from .schema import Contract, load_contract, save_contract
from .utils import read_dataframe

app = typer.Typer(help="DataFrame contract utilities")


class CLIState:
    def __init__(self) -> None:
        self.contract: Contract | None = None
        self.source: Path | None = None


@app.callback()
def main(ctx: typer.Context) -> None:
    if ctx.obj is None:
        ctx.obj = CLIState()


@app.command()
def init(
    path: Path,
    name: str = typer.Option("dataset", help="Contract name"),
    version: str = typer.Option("0.1.0", help="Contract version"),
    sample: Optional[float] = typer.Option(None, help="Sample fraction"),
) -> None:
    df = read_dataframe(path, sample=sample)
    contract = infer_contract(df, name=name, version=version)
    ctx: CLIState = typer.get_current_context().obj
    ctx.contract = contract
    ctx.source = path
    typer.echo(contract.to_json())


@app.command()
def save(
    out: Path = typer.Option(..., exists=False, help="Output contract file"),
    format: Optional[str] = typer.Option(None, "--format", "-f", help="Force format json|toml"),
    contract_path: Optional[Path] = typer.Option(None, help="Contract to load instead of last inferred"),
) -> None:
    ctx: CLIState = typer.get_current_context().obj
    contract = ctx.contract if contract_path is None else load_contract(contract_path)
    if contract is None:
        raise typer.BadParameter("No contract available. Run init or provide --contract.")
    if format:
        fmt = format.lower()
        if fmt == "json":
            out.write_text(contract.to_json())
            return
        if fmt == "toml":
            out.write_text(contract.to_toml())
            return
        raise typer.BadParameter("format must be 'json' or 'toml'")
    save_contract(contract, out)


@app.command()
def check(
    path: Path,
    contract: Path = typer.Option(..., help="Contract file"),
    profile: str = typer.Option("prod", help="Profile name"),
    report: Optional[Path] = typer.Option(None, help="Optional JSON report output"),
    sample: Optional[float] = typer.Option(None, help="Sample fraction"),
    max_examples: int = typer.Option(20, help="Maximum examples in the report"),
) -> None:
    df = read_dataframe(path, sample=sample)
    contract_obj = load_contract(contract)
    validation = validate(df, contract_obj, profile=profile, max_examples=max_examples)
    _render_report(validation)
    if report:
        report.write_text(validation.to_json())
    raise typer.Exit(code=0 if validation.ok else 1)


@app.command()
def diff(old: Path, new: Path) -> None:
    old_contract = load_contract(old)
    new_contract = load_contract(new)
    diff_result = compare(old_contract, new_contract)
    console = Console()
    console.print_json(data=diff_result)
    if is_breaking_change(diff_result):
        console.print("[bold red]Breaking changes detected[/bold red]")


@app.command()
def stats(path: Path, sample: Optional[float] = typer.Option(None, help="Sample fraction")) -> None:
    df = read_dataframe(path, sample=sample)
    table = Table(title="DataFrame Stats")
    table.add_column("Column")
    table.add_column("Dtype")
    table.add_column("Null %", justify="right")
    table.add_column("Distinct", justify="right")
    for column in df.columns:
        series = df[column]
        null_ratio = series.isna().mean() * 100
        distinct = series.nunique(dropna=True)
        table.add_row(column, str(series.dtype), f"{null_ratio:.2f}", str(distinct))
    Console().print(table)


def _render_report(report: ValidationReport) -> None:
    console = Console()
    report.to_rich_console(console=console)


if __name__ == "__main__":  # pragma: no cover
    app()
