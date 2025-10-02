from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Iterable, Literal

import pandas as pd

from .inference import infer_contract
from .schema import ColumnSpec, Contract
from .utils import ensure_pandas

Severity = Literal["INFO", "WARN"]


@dataclass(slots=True)
class LintSuggestion:
    severity: Severity
    message: str
    location: str
    diff: str
    apply_func: Callable[[Contract], Contract] | None = None

    def apply(self, contract: Contract) -> Contract:
        if not self.apply_func:
            return contract
        return self.apply_func(contract)


@dataclass(slots=True)
class LintReport:
    suggestions: list[LintSuggestion]

    def is_clean(self) -> bool:
        return not self.suggestions

    def apply(self, contract: Contract, *, bump: bool = True) -> Contract:
        updated = contract.model_copy(deep=True)
        for suggestion in self.suggestions:
            updated = suggestion.apply(updated)
        if bump:
            updated.version = bump_version(updated.version)
        return updated


def suggest_improvements(contract: Contract, df: pd.DataFrame | Iterable[dict[str, object]]) -> LintReport:
    frame = ensure_pandas(df)
    inference = infer_contract(frame, name=contract.name, version=contract.version)
    suggestions: list[LintSuggestion] = []
    contract_map = {col.name: col for col in contract.columns}
    for suggestion in inference.suggestions:
        if suggestion.column not in contract_map:
            continue
        column = contract_map[suggestion.column]
        if suggestion.kind in {"non_negative", "positive"} and column.min is None:
            min_value = 0.0 if suggestion.kind == "non_negative" else max(0.0, float(suggestion.details.get("min", 0.0)))
            suggestions.append(
                _build_min_suggestion(column.name, min_value, severity="WARN")
            )
        if suggestion.kind == "enum" and not column.enum:
            values = suggestion.details.get("values", [])
            suggestions.append(
                _build_enum_suggestion(column.name, [str(v) for v in values])
            )
    suggestions.extend(_lint_contract_rules(contract, frame))
    return LintReport(suggestions=suggestions)


def _lint_contract_rules(contract: Contract, df: pd.DataFrame) -> list[LintSuggestion]:
    results: list[LintSuggestion] = []
    for column in contract.columns:
        if column.nullable is True:
            results.append(
                LintSuggestion(
                    severity="WARN",
                    message=f"Column '{column.name}' allows any nulls; consider explicit ratio.",
                    location=column.name,
                    diff="Set nullable to a float ratio",
                )
            )
        if column.dtype.startswith("datetime64") and column.tz is None:
            results.append(
                LintSuggestion(
                    severity="INFO",
                    message=f"Datetime column '{column.name}' has no timezone; default to UTC.",
                    location=column.name,
                    diff="Set tz to 'UTC'",
                    apply_func=lambda contract, col=column.name: _set_column_attr(contract, col, tz="UTC"),
                )
            )
        if column.dtype.startswith("float") or column.dtype.startswith("int"):
            if "amount" in column.name.lower() and (column.min is None or float(column.min) < 0):
                results.append(
                    _build_min_suggestion(column.name, 0.0, severity="WARN")
                )
    return results


def _build_min_suggestion(column: str, value: float, *, severity: Severity) -> LintSuggestion:
    return LintSuggestion(
        severity=severity,
        message=f"Column '{column}' appears to be non-negative; set min >= {value}.",
        location=column,
        diff=f"Set min to {value}",
        apply_func=lambda contract, col=column, val=value: _set_column_attr(contract, col, min=val),
    )


def _build_enum_suggestion(column: str, values: list[str]) -> LintSuggestion:
    return LintSuggestion(
        severity="INFO",
        message=f"Column '{column}' has low cardinality; define enum of {values}.",
        location=column,
        diff=f"Set enum to {values}",
        apply_func=lambda contract, col=column, vals=values: _set_column_attr(contract, col, enum=vals),
    )


def _set_column_attr(contract: Contract, column: str, **changes: object) -> Contract:
    updated = contract.model_copy(deep=True)
    for idx, col in enumerate(updated.columns):
        if col.name == column:
            data = col.model_dump()
            data.update(changes)
            updated.columns[idx] = ColumnSpec.model_validate(data)
            break
    return updated


def bump_version(version: str) -> str:
    try:
        major, minor, patch = [int(part) for part in version.split(".")]
    except ValueError:  # pragma: no cover - defensive
        return version
    minor += 1
    patch = 0
    return f"{major}.{minor}.{patch}"
