from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import orjson
from rich.console import Console
from rich.table import Table

from .types import ValidationStats, ViolationDict


@dataclass(slots=True)
class ValidationReport:
    ok: bool
    stats: ValidationStats
    violations: list[ViolationDict]
    schema_diffs: list[str]

    def as_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "stats": dict(self.stats),
            "violations": [dict(v) for v in self.violations],
            "schema_diffs": list(self.schema_diffs),
        }

    def to_json(self) -> str:
        return orjson.dumps(self.as_dict(), option=orjson.OPT_INDENT_2).decode()

    def to_rich_console(self, console: Console | None = None) -> None:
        console = console or Console()
        header = "VALIDATION PASSED" if self.ok else "VALIDATION FAILED"
        console.rule(header)
        console.print(f"Rows: {self.stats['rows']}  Columns: {self.stats['cols']}")
        if self.schema_diffs:
            console.print("[bold red]Schema differences:[/bold red]")
            for diff in self.schema_diffs:
                console.print(f"- {diff}")
        if not self.violations:
            console.print("No violations detected.")
            return
        table = Table(title="Violations")
        table.add_column("ID")
        table.add_column("Level")
        table.add_column("Kind")
        table.add_column("Columns")
        table.add_column("Summary")
        table.add_column("Count", justify="right")
        for violation in self.violations:
            table.add_row(
                violation["id"],
                violation["level"],
                violation["kind"],
                ", ".join(violation["columns"]),
                violation["summary"],
                str(violation["count"]),
            )
        console.print(table)
