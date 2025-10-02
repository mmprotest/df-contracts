from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

import orjson
from jinja2 import Environment, select_autoescape
from rich.console import Console
from rich.table import Table

from .types import ValidationStats, ViolationDict
from .drift import DriftSnapshot

_jinja_env = Environment(autoescape=select_autoescape(enabled_extensions=("html",)))


@dataclass(slots=True)
class ValidationReport:
    ok: bool
    stats: ValidationStats
    violations: list[ViolationDict]
    schema_diffs: list[str]
    profile: str = "prod"
    snapshot: DriftSnapshot | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "stats": dict(self.stats),
            "violations": [dict(v) for v in self.violations],
            "schema_diffs": list(self.schema_diffs),
            "profile": self.profile,
            "snapshot": self.snapshot.as_dict() if self.snapshot else None,
        }

    def to_json(self) -> str:
        return orjson.dumps(self.as_dict(), option=orjson.OPT_INDENT_2).decode()

    def to_html(self, df: Any | None = None, *, max_examples: int = 50) -> str:
        template = _jinja_env.from_string(_HTML_TEMPLATE)
        rows = [
            {
                "id": v["id"],
                "level": v["level"],
                "kind": v["kind"],
                "columns": ", ".join(v["columns"]),
                "summary": v["summary"],
                "count": v["count"],
                "examples": v["examples"][:max_examples],
            }
            for v in self.violations
        ]
        html = template.render(
            ok=self.ok,
            rows=rows,
            stats=self.stats,
            schema_diffs=self.schema_diffs,
            profile=self.profile,
            generated_at=datetime.utcnow().isoformat() + "Z",
        )
        return html

    def _repr_html_(self) -> str:
        return self.to_html(max_examples=20)

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

    def to_junit(self) -> str:
        cases = []
        for violation in self.violations:
            testcase = {
                "name": violation["id"],
                "classname": violation["kind"],
                "level": violation["level"],
                "summary": violation["summary"],
                "count": violation["count"],
            }
            cases.append(testcase)
        failures = sum(1 for case in cases if case["level"] == "ERROR")
        skipped = sum(1 for case in cases if case["level"] == "WARN")
        total = len(cases)
        xml_lines = [
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>",
            f"<testsuite name=\"df-contracts\" tests=\"{total}\" failures=\"{failures}\" skipped=\"{skipped}\" timestamp=\"{datetime.utcnow().isoformat()}Z\">",
        ]
        for case in cases:
            xml_lines.append(
                f"  <testcase classname=\"{case['classname']}\" name=\"{case['name']}\">"
            )
            if case["level"] == "ERROR":
                xml_lines.append(
                    f"    <failure message=\"{case['summary']}\">Count: {case['count']}</failure>"
                )
            elif case["level"] == "WARN":
                xml_lines.append(
                    f"    <skipped message=\"{case['summary']}\" />"
                )
            xml_lines.append("  </testcase>")
        xml_lines.append("</testsuite>")
        return "\n".join(xml_lines)

    def format_for_github_pr(self) -> str:
        if not self.violations and not self.schema_diffs:
            return "✅ Validation succeeded with no findings."
        lines = ["## df-contracts validation report", ""]
        if self.schema_diffs:
            lines.append("### Schema differences")
            for diff in self.schema_diffs:
                lines.append(f"- {diff}")
            lines.append("")
        if self.violations:
            lines.append("### Violations")
            lines.append("| ID | Level | Kind | Columns | Summary | Count |")
            lines.append("| --- | --- | --- | --- | --- | --- |")
            for v in self.violations:
                lines.append(
                    f"| {v['id']} | {v['level']} | {v['kind']} | {', '.join(v['columns'])} | {v['summary']} | {v['count']} |"
                )
        return "\n".join(lines)


_HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
  <meta charset=\"utf-8\">
  <title>df-contracts validation</title>
  <style>
    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: 2rem; }
    table { border-collapse: collapse; width: 100%; margin-top: 1rem; }
    th, td { border: 1px solid #ddd; padding: 8px; }
    th { background: #f2f2f2; cursor: pointer; }
    .ok { color: #2e8540; }
    .fail { color: #b10e1e; }
    .level-ERROR { background: #ffe6e6; }
    .level-WARN { background: #fff8e6; }
  </style>
</head>
<body>
  <h1>df-contracts validation</h1>
  <p>Status: <strong class="{{ 'ok' if ok else 'fail' }}">{{ 'PASSED' if ok else 'FAILED' }}</strong></p>
  <p>Rows: {{ stats['rows'] }} &middot; Columns: {{ stats['cols'] }} &middot; Profile: {{ profile }} &middot; Generated: {{ generated_at }}</p>
  {% if schema_diffs %}
  <h2>Schema differences</h2>
  <ul>
    {% for diff in schema_diffs %}
    <li>{{ diff }}</li>
    {% endfor %}
  </ul>
  {% endif %}
  {% if rows %}
  <h2>Violations</h2>
  <table>
    <thead>
      <tr>
        <th>ID</th>
        <th>Level</th>
        <th>Kind</th>
        <th>Columns</th>
        <th>Summary</th>
        <th>Count</th>
      </tr>
    </thead>
    <tbody>
      {% for row in rows %}
      <tr class="level-{{ row['level'] }}">
        <td>{{ row['id'] }}</td>
        <td>{{ row['level'] }}</td>
        <td>{{ row['kind'] }}</td>
        <td>{{ row['columns'] }}</td>
        <td>{{ row['summary'] }}</td>
        <td>{{ row['count'] }}</td>
      </tr>
      {% endfor %}
    </tbody>
  </table>
  {% else %}
  <p>No violations detected.</p>
  {% endif %}
</body>
</html>
"""
