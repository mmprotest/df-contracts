from __future__ import annotations

from df_contracts.report import ValidationReport


def test_report_serialisation() -> None:
    report = ValidationReport(
        ok=False,
        stats={"rows": 5, "cols": 3, "duration_ms": 10},
        violations=[
            {
                "id": "column.amount.min",
                "level": "ERROR",
                "kind": "column",
                "columns": ["amount"],
                "summary": "Values violate min 0",
                "count": 2,
                "examples": [{"amount": -1}],
            }
        ],
        schema_diffs=["missing column foo"],
        profile="prod",
    )
    html = report.to_html()
    assert "df-contracts validation" in html
    junit = report.to_junit()
    assert "testsuite" in junit
    markdown = report.format_for_github_pr()
    assert "Violations" in markdown
