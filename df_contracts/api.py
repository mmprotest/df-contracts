from __future__ import annotations

import time
from typing import Any, List, Sequence

import pandas as pd
import regex
from pandas.api.types import is_datetime64_any_dtype, is_numeric_dtype

from . import versioning
from .drift import DriftSnapshot, snapshot as drift_snapshot
from .checks import TABLE_CHECKS
from .errors import RuleExecutionError
from .report import ValidationReport
from .schema import ColumnProfileOverride, ColumnSpec, Contract, ProfileOverrides, RuleSpec
from .types import ValidationStats, ViolationDict
from .utils import ensure_pandas, head_records, is_dtype_compatible, normalize_dtype


def validate(
    df: pd.DataFrame,
    contract: Contract,
    *,
    profile: str = "prod",
    sample: float | None = None,
    by: Sequence[str] | None = None,
    max_examples: int = 20,
    with_snapshot: bool = False,
) -> ValidationReport:
    frame = ensure_pandas(df).copy()
    if sample:
        if not 0 < sample <= 1:
            raise ValueError("sample must be in (0, 1]")
        frame = _sample(frame, sample, by)
    profile_overrides = contract.profiles.get(profile) if contract.profiles else None
    if profile_overrides and profile_overrides.default_max_examples:
        max_examples = profile_overrides.default_max_examples
    start = time.perf_counter()
    stats: ValidationStats = {
        "rows": int(frame.shape[0]),
        "cols": int(frame.shape[1]),
        "duration_ms": 0,
    }
    violations: List[ViolationDict] = []
    schema_diffs: list[str] = []
    ok = True
    available_columns = set(frame.columns)
    for column in contract.columns:
        if column.name not in frame.columns:
            schema_diffs.append(f"missing column {column.name}")
            violations.append(
                _violation(
                    violation_id=f"column.{column.name}.missing",
                    level="ERROR",
                    kind="schema",
                    columns=[column.name],
                    summary=f"Column {column.name} missing",
                    count=frame.shape[0],
                    examples=[],
                )
            )
            ok = False
            continue
        series = frame[column.name]
        available_columns.discard(column.name)
        actual_dtype = normalize_dtype(series.dtype)
        if not is_dtype_compatible(actual_dtype, column.dtype):
            schema_diffs.append(
                f"dtype mismatch for {column.name}: expected {column.dtype} got {actual_dtype}"
            )
            violations.append(
                _violation(
                    violation_id=f"column.{column.name}.dtype",
                    level="ERROR",
                    kind="schema",
                    columns=[column.name],
                    summary=f"Expected dtype {column.dtype} got {actual_dtype}",
                    count=frame.shape[0],
                    examples=[],
                )
            )
            ok = False
        null_ratio = float(series.isna().mean())
        overrides = _column_overrides(column, profile_overrides)
        allowed_nullable = overrides.nullable if overrides and overrides.nullable is not None else column.nullable
        allowed_max_null = overrides.max_null_ratio if overrides and overrides.max_null_ratio is not None else None
        if allowed_max_null is not None:
            allowed_nullable = allowed_max_null
        if allowed_nullable is False and null_ratio > 0:
            violations.append(
                _violation(
                    violation_id=f"column.{column.name}.nulls",
                    level="ERROR",
                    kind="column",
                    columns=[column.name],
                    summary=f"Null ratio {null_ratio:.3f} exceeds allowed 0",
                    count=int(series.isna().sum()),
                    examples=_series_examples(series.isna(), series, max_examples),
                )
            )
            ok = False
        elif isinstance(allowed_nullable, float) and null_ratio > allowed_nullable:
            violations.append(
                _violation(
                    violation_id=f"column.{column.name}.nulls",
                    level="ERROR",
                    kind="column",
                    columns=[column.name],
                    summary=f"Null ratio {null_ratio:.3f} exceeds allowed {column.nullable}",
                    count=int(series.isna().sum()),
                    examples=_series_examples(series.isna(), series, max_examples),
                )
            )
            ok = False
        allow_unknown = (
            overrides.allow_unknown if overrides and overrides.allow_unknown is not None else column.allow_unknown
        )
        if column.enum and not allow_unknown:
            allowed = set(column.enum)
            bad_values = series.dropna().astype(str)
            mask = ~bad_values.isin(allowed)
            if mask.any():
                examples = bad_values[mask].head(max_examples).to_frame(name=column.name)
                violations.append(
                    _violation(
                        violation_id=f"column.{column.name}.enum",
                        level="ERROR",
                        kind="column",
                        columns=[column.name],
                        summary=f"Found unexpected values {sorted(bad_values[mask].unique())[:5]}",
                        count=int(mask.sum()),
                        examples=head_records(examples, limit=max_examples),
                    )
                )
                ok = False
        if column.regex:
            pattern = regex.compile(column.regex)
            str_values = series.dropna().astype(str)
            mask = ~str_values.apply(pattern.fullmatch)
            if mask.any():
                examples = str_values[mask].head(max_examples).to_frame(name=column.name)
                violations.append(
                    _violation(
                        violation_id=f"column.{column.name}.regex",
                        level="ERROR",
                        kind="column",
                        columns=[column.name],
                        summary="Values do not match required pattern",
                        count=int(mask.sum()),
                        examples=head_records(examples, limit=max_examples),
                    )
                )
                ok = False
        if column.min is not None or column.max is not None:
            if is_numeric_dtype(series) or _coerce_numeric(series):
                numeric_series = pd.to_numeric(series, errors="coerce")
                if column.min is not None:
                    min_value = float(column.min)
                    mask = numeric_series < min_value
                    ok = _check_bounds(
                        ok,
                        mask,
                        column,
                        "min",
                        min_value,
                        numeric_series,
                        violations,
                        max_examples,
                    )
                if column.max is not None:
                    max_value = float(column.max)
                    mask = numeric_series > max_value
                    ok = _check_bounds(
                        ok,
                        mask,
                        column,
                        "max",
                        max_value,
                        numeric_series,
                        violations,
                        max_examples,
                    )
            elif _coerce_datetime(series):
                dt_series = pd.to_datetime(series, errors="coerce")
                if column.min is not None:
                    min_value = pd.to_datetime(column.min)
                    mask = dt_series < min_value
                    ok = _check_bounds(
                        ok,
                        mask,
                        column,
                        "min",
                        min_value,
                        dt_series,
                        violations,
                        max_examples,
                    )
                if column.max is not None:
                    max_value = pd.to_datetime(column.max)
                    mask = dt_series > max_value
                    ok = _check_bounds(
                        ok,
                        mask,
                        column,
                        "max",
                        max_value,
                        dt_series,
                        violations,
                        max_examples,
                    )
        if column.min_length is not None or column.max_length is not None:
            lengths = series.dropna().astype(str).str.len()
            if column.min_length is not None:
                mask = lengths < column.min_length
                if mask.any():
                    examples = series.dropna().astype(str)[mask].head(max_examples).to_frame(name=column.name)
                    violations.append(
                        _violation(
                            violation_id=f"column.{column.name}.min_length",
                            level="ERROR",
                            kind="column",
                            columns=[column.name],
                            summary=f"Length shorter than {column.min_length}",
                            count=int(mask.sum()),
                            examples=head_records(examples, limit=max_examples),
                        )
                    )
                    ok = False
            if column.max_length is not None:
                mask = lengths > column.max_length
                if mask.any():
                    examples = series.dropna().astype(str)[mask].head(max_examples).to_frame(name=column.name)
                    violations.append(
                        _violation(
                            violation_id=f"column.{column.name}.max_length",
                            level="ERROR",
                            kind="column",
                            columns=[column.name],
                            summary=f"Length exceeds {column.max_length}",
                            count=int(mask.sum()),
                            examples=head_records(examples, limit=max_examples),
                        )
                    )
                    ok = False
        if column.unique is True:
            mask = series.duplicated(keep=False)
            if mask.any():
                duplicates = frame.loc[mask, [column.name]]
                violations.append(
                    _violation(
                        violation_id=f"column.{column.name}.unique",
                        level="ERROR",
                        kind="column",
                        columns=[column.name],
                        summary="Duplicate values found",
                        count=int(mask.sum()),
                        examples=head_records(duplicates, limit=max_examples),
                    )
                )
                ok = False
    if not contract.allow_extra_columns and available_columns:
        for extra in sorted(available_columns):
            schema_diffs.append(f"unexpected column {extra}")
            violations.append(
                _violation(
                    violation_id=f"column.{extra}.unexpected",
                    level="WARN",
                    kind="schema",
                    columns=[extra],
                    summary="Unexpected column present",
                    count=frame.shape[0],
                    examples=[],
                )
            )
    for key in contract.unique_keys:
        mask = frame.duplicated(subset=key, keep=False)
        if mask.any():
            duplicates = frame.loc[mask, key]
            violations.append(
                _violation(
                    violation_id=f"contract.unique.{'+'.join(key)}",
                    level="ERROR",
                    kind="table",
                    columns=list(key),
                    summary="Composite key is not unique",
                    count=int(mask.sum()),
                    examples=head_records(duplicates, limit=max_examples),
                )
            )
            ok = False
    for rule in contract.rules:
        if rule.kind == "row" and rule.expr:
            ok = _apply_row_rule(rule, frame, violations, ok, max_examples)
        elif rule.kind == "table" and rule.fn_name:
            ok = _apply_table_rule(rule, frame, violations, ok, max_examples)
    stats["duration_ms"] = int((time.perf_counter() - start) * 1000)
    embedded_snapshot: DriftSnapshot | None = drift_snapshot(frame) if with_snapshot else None
    return ValidationReport(
        ok=ok,
        stats=stats,
        violations=violations,
        schema_diffs=schema_diffs,
        profile=profile,
        snapshot=embedded_snapshot,
    )


def _violation(
    *,
    violation_id: str,
    level: str,
    kind: str,
    columns: list[str],
    summary: str,
    count: int,
    examples: list[dict[str, Any]],
) -> ViolationDict:
    return {
        "id": violation_id,
        "level": level,
        "kind": kind,
        "columns": columns,
        "summary": summary,
        "count": count,
        "examples": examples,
    }


def _series_examples(mask: pd.Series, series: pd.Series, limit: int) -> list[dict[str, Any]]:
    failing = series[mask]
    return head_records(failing.to_frame(series.name or "value"), limit=limit)


def _coerce_numeric(series: pd.Series) -> bool:
    if is_numeric_dtype(series):
        return True
    coerced = pd.to_numeric(series, errors="coerce")
    return coerced.notna().any()


def _coerce_datetime(series: pd.Series) -> bool:
    if is_datetime64_any_dtype(series):
        return True
    coerced = pd.to_datetime(series, errors="coerce")
    return coerced.notna().any()


def _check_bounds(
    ok: bool,
    mask: pd.Series,
    column: ColumnSpec,
    bound: str,
    value: Any,
    series: pd.Series,
    violations: list[ViolationDict],
    max_examples: int,
) -> bool:
    if mask.any():
        examples = series[mask].head(max_examples).to_frame(name=column.name)
        violations.append(
            _violation(
                violation_id=f"column.{column.name}.{bound}",
                level="ERROR",
                kind="column",
                columns=[column.name],
                summary=f"Values violate {bound} {value}",
                count=int(mask.sum()),
                examples=head_records(examples, limit=max_examples),
            )
        )
        return False
    return ok


def _sample(frame: pd.DataFrame, frac: float, by: Sequence[str] | None) -> pd.DataFrame:
    if not by:
        return frame.sample(frac=frac, random_state=0)
    missing = [col for col in by if col not in frame.columns]
    if missing:
        raise ValueError(f"Sampling columns missing from frame: {missing}")
    parts = []
    for _, group in frame.groupby(list(by)):
        if group.empty:
            continue
        take = max(1, int(round(len(group) * frac)))
        parts.append(group.sample(n=min(take, len(group)), random_state=0))
    if not parts:
        return frame.head(0)
    return pd.concat(parts).sort_index()


def _column_overrides(
    column: ColumnSpec,
    profile: ProfileOverrides | None,
) -> ColumnProfileOverride | None:
    if profile and column.name in profile.columns:
        return profile.columns[column.name]
    return None


def _apply_row_rule(
    rule: RuleSpec,
    frame: pd.DataFrame,
    violations: list[ViolationDict],
    ok: bool,
    max_examples: int,
) -> bool:
    try:
        result = frame.eval(rule.expr, engine="python")  # type: ignore[arg-type]
    except Exception as exc:  # pragma: no cover - defensive
        raise RuleExecutionError(str(exc)) from exc
    if result.dtype != bool:
        result = result.astype(bool)
    mask = ~result.fillna(False)
    if mask.any():
        failing = frame.loc[mask]
        violations.append(
            _violation(
                violation_id=f"rule.{rule.id}",
                level=rule.level,
                kind="row",
                columns=list(failing.columns),
                summary=rule.message,
                count=int(mask.sum()),
                examples=head_records(failing, limit=max_examples),
            )
        )
        if rule.level == "ERROR":
            ok = False
    return ok


def _apply_table_rule(
    rule: RuleSpec,
    frame: pd.DataFrame,
    violations: list[ViolationDict],
    ok: bool,
    max_examples: int,
) -> bool:
    fn = TABLE_CHECKS.get(rule.fn_name or "")
    if not fn:
        raise RuleExecutionError(f"Unknown table rule: {rule.fn_name}")
    failing = fn(frame, rule.params)
    if not failing.empty:
        violations.append(
            _violation(
                violation_id=f"rule.{rule.id}",
                level=rule.level,
                kind="table",
                columns=list(failing.columns),
                summary=rule.message,
                count=int(failing.shape[0]),
                examples=head_records(failing, limit=max_examples),
            )
        )
        if rule.level == "ERROR":
            ok = False
    return ok


def compare(old: Contract, new: Contract) -> dict[str, Any]:
    return versioning.compare_contracts(old, new)


def is_breaking_change(diff: dict[str, Any]) -> bool:
    return versioning.is_breaking_change(diff)
