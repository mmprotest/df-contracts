from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable

import orjson
import pandas as pd
import pytest

from .api import validate
from .report import ValidationReport
from .schema import Contract, load_contract
from .utils import ensure_pandas


class PluginState:
    def __init__(
        self,
        *,
        json_path: Path | None,
        junit_path: Path | None,
        html_path: Path | None,
    ) -> None:
        self.json_path = json_path
        self.junit_path = junit_path
        self.html_path = html_path
        self.reports: list[tuple[str, ValidationReport]] = []

    def record(self, name: str, report: ValidationReport) -> None:
        self.reports.append((name, report))

    def write_all(self) -> None:
        if not self.reports:
            return
        if self.json_path:
            payload = {"runs": [{"name": name, "report": report.as_dict()} for name, report in self.reports]}
            self.json_path.write_text(orjson.dumps(payload, option=orjson.OPT_INDENT_2).decode())
        if self.junit_path:
            suite = _aggregate_junit(self.reports)
            self.junit_path.write_text(suite)
        if self.html_path:
            html = _aggregate_html(self.reports)
            self.html_path.write_text(html)


class ContractsHelper:
    def __init__(self, state: PluginState) -> None:
        self._state = state

    def load(self, contract_or_path: Contract | str | Path) -> Contract:
        if isinstance(contract_or_path, Contract):
            return contract_or_path
        return load_contract(contract_or_path)

    def must_match(
        self,
        contract_or_path: Contract | str | Path,
        df: pd.DataFrame | Iterable[dict[str, Any]],
        *,
        profile: str = "prod",
        sample: float | None = None,
        by: Iterable[str] | None = None,
        max_examples: int = 20,
        with_snapshot: bool = False,
    ) -> ValidationReport:
        contract = self.load(contract_or_path)
        if isinstance(df, pd.DataFrame):
            frame = df
        else:
            frame = pd.DataFrame(list(df))
        frame = ensure_pandas(frame)
        report = validate(
            frame,
            contract,
            profile=profile,
            sample=sample,
            by=list(by) if by else None,
            max_examples=max_examples,
            with_snapshot=with_snapshot,
        )
        self._state.record(contract.name, report)
        if not report.ok:
            raise AssertionError(
                "DataFrame did not satisfy contract:\n" + report.to_json()
            )
        return report

    def write_report(self, path: Path) -> None:
        self._state.json_path = path
        self._state.write_all()


@pytest.fixture
def df_contracts(request: pytest.FixtureRequest) -> ContractsHelper:
    state: PluginState = request.config._dfc_state  # type: ignore[attr-defined]
    return ContractsHelper(state)


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption("--dfc-report", action="store", default=None, help="Aggregate JSON report path")
    parser.addoption("--dfc-junit", action="store", default=None, help="JUnit XML output path")
    parser.addoption("--dfc-html", action="store", default=None, help="HTML output path")


def pytest_configure(config: pytest.Config) -> None:
    report_opt = config.getoption("--dfc-report")
    junit_opt = config.getoption("--dfc-junit")
    html_opt = config.getoption("--dfc-html")
    json_path = Path(report_opt) if report_opt else None
    junit_path = Path(junit_opt) if junit_opt else None
    html_path = Path(html_opt) if html_opt else None
    config._dfc_state = PluginState(json_path=json_path, junit_path=junit_path, html_path=html_path)  # type: ignore[attr-defined]


def pytest_unconfigure(config: pytest.Config) -> None:
    state: PluginState | None = getattr(config, "_dfc_state", None)
    if state is not None:
        state.write_all()


def _aggregate_junit(reports: list[tuple[str, ValidationReport]]) -> str:
    cases = []
    for name, report in reports:
        for violation in report.violations:
            cases.append((name, violation))
    failures = sum(1 for _, violation in cases if violation["level"] == "ERROR")
    skipped = sum(1 for _, violation in cases if violation["level"] == "WARN")
    total = len(cases)
    lines = [
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>",
        f"<testsuite name=\"df-contracts\" tests=\"{total}\" failures=\"{failures}\" skipped=\"{skipped}\">",
    ]
    for name, violation in cases:
        lines.append(f"  <testcase classname=\"{name}\" name=\"{violation['id']}\">")
        if violation["level"] == "ERROR":
            lines.append(
                f"    <failure message=\"{violation['summary']}\">Count: {violation['count']}</failure>"
            )
        elif violation["level"] == "WARN":
            lines.append(f"    <skipped message=\"{violation['summary']}\" />")
        lines.append("  </testcase>")
    lines.append("</testsuite>")
    return "\n".join(lines)


def _aggregate_html(reports: list[tuple[str, ValidationReport]]) -> str:
    sections = []
    for name, report in reports:
        sections.append(f"<section><h2>{name}</h2>{report.to_html()}</section>")
    body = "".join(sections)
    return (
        "<!DOCTYPE html><html><head><meta charset='utf-8'><title>df-contracts pytest reports" "</title></head><body>"
        "<h1>df-contracts pytest summary</h1>" + body + "</body></html>"
    )
