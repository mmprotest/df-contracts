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
    def __init__(self, report_path: Path | None) -> None:
        self.report_path = report_path
        self.runs: list[dict[str, Any]] = []

    def record(self, name: str, report: ValidationReport) -> None:
        self.runs.append({"name": name, "report": report.as_dict()})

    def write(self, path: Path | None = None) -> None:
        target = path or self.report_path
        if not target or not self.runs:
            return
        payload = {"runs": self.runs}
        target.write_text(orjson.dumps(payload, option=orjson.OPT_INDENT_2).decode())


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
        max_examples: int = 20,
    ) -> ValidationReport:
        contract = self.load(contract_or_path)
        if isinstance(df, pd.DataFrame):
            frame = df
        else:
            frame = pd.DataFrame(list(df))
        frame = ensure_pandas(frame)
        report = validate(frame, contract, profile=profile, max_examples=max_examples)
        self._state.record(contract.name, report)
        if not report.ok:
            raise AssertionError(
                "DataFrame did not satisfy contract:\n" + report.to_json()
            )
        return report

    def write_report(self, path: Path) -> None:
        self._state.write(path)


@pytest.fixture
def df_contracts(request: pytest.FixtureRequest) -> ContractsHelper:
    state: PluginState = request.config._dfc_state  # type: ignore[attr-defined]
    return ContractsHelper(state)


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption("--dfc-report", action="store", default=None, help="Aggregate report path")


def pytest_configure(config: pytest.Config) -> None:
    report_opt = config.getoption("--dfc-report")
    report_path = Path(report_opt) if report_opt else None
    config._dfc_state = PluginState(report_path)  # type: ignore[attr-defined]


def pytest_unconfigure(config: pytest.Config) -> None:
    state: PluginState | None = getattr(config, "_dfc_state", None)
    if state is not None:
        state.write()
