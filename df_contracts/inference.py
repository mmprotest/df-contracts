from __future__ import annotations

from dataclasses import dataclass
from typing import List, Sequence

import pandas as pd
from pandas.api.types import is_datetime64_any_dtype, is_numeric_dtype

from .schema import ColumnSpec, Contract
from .utils import ensure_pandas, normalize_dtype


def _infer_datetime(series: pd.Series) -> bool:
    if is_datetime64_any_dtype(series):
        return True
    converted = pd.to_datetime(series, errors="coerce")
    success_ratio = converted.notna().mean()
    if success_ratio >= 0.95:
        series.update(converted)
        return True
    return False


@dataclass(slots=True)
class InferenceSuggestion:
    column: str
    kind: str
    message: str
    details: dict[str, object]


@dataclass(slots=True)
class InferenceResult:
    contract: Contract
    suggestions: list[InferenceSuggestion]


def infer_contract(
    df: pd.DataFrame,
    name: str,
    version: str = "0.1.0",
    *,
    enum_max_cardinality: int = 50,
    enum_min_freq_ratio: float = 0.95,
    nullable_threshold: float = 0.0,
) -> InferenceResult:
    frame = ensure_pandas(df)
    columns: List[ColumnSpec] = []
    suggestions: list[InferenceSuggestion] = []
    for column in frame.columns:
        series = frame[column]
        series_no_na = series.dropna()
        dtype = normalize_dtype(series.dtype)
        if dtype == "object" and _infer_datetime(series):
            dtype = normalize_dtype(series.dtype)
        null_ratio = float(series.isna().mean())
        if null_ratio == 0:
            nullable: bool | float = False
        elif null_ratio <= nullable_threshold:
            nullable = False
        else:
            nullable = round(null_ratio, 4)
        spec = ColumnSpec(name=column, dtype=dtype, nullable=nullable)
        if is_numeric_dtype(series):
            if not series_no_na.empty:
                spec.min = series_no_na.min()
                spec.max = series_no_na.max()
        elif dtype.startswith("datetime64"):
            if not series_no_na.empty:
                spec.min = series_no_na.min().isoformat()
                spec.max = series_no_na.max().isoformat()
        if not series_no_na.empty and series_no_na.nunique() <= enum_max_cardinality:
            coverage = float(series_no_na.value_counts(normalize=True).sum())
            if coverage >= enum_min_freq_ratio:
                spec.enum = sorted(series_no_na.astype(str).unique().tolist())
        if series_no_na.is_unique and series.is_unique:
            spec.unique = True
        columns.append(spec)
        suggestions.extend(_derive_suggestions(column, series))
    suggestions.extend(pairwise_suggestions(list(frame.columns), frame))
    unique_keys: list[list[str]] = []
    contract = Contract(name=name, version=version, columns=columns, unique_keys=unique_keys)
    return InferenceResult(contract=contract, suggestions=suggestions)


def _derive_suggestions(column: str, series: pd.Series) -> list[InferenceSuggestion]:
    if series.empty:
        return []
    suggestions: list[InferenceSuggestion] = []
    cleaned = series.dropna()
    if cleaned.empty:
        return suggestions
    if is_numeric_dtype(cleaned):
        min_value = cleaned.min()
        if pd.notna(min_value) and min_value >= 0:
            kind = "non_negative" if min_value == 0 else "positive"
            suggestions.append(
                InferenceSuggestion(
                    column=column,
                    kind=kind,
                    message=f"Column '{column}' appears {kind.replace('_', ' ')} (min={min_value}).",
                    details={"min": float(min_value)},
                )
            )
    if cleaned.nunique() <= 20:
        counts = cleaned.value_counts(normalize=True)
        coverage = float(counts.sum())
        if coverage >= 0.9:
            suggestions.append(
                InferenceSuggestion(
                    column=column,
                    kind="enum",
                    message=f"Column '{column}' has low cardinality; consider enum constraint.",
                    details={"values": sorted(map(str, cleaned.unique()))},
                )
            )
    if is_datetime64_any_dtype(cleaned):
        if cleaned.is_monotonic_increasing:
            suggestions.append(
                InferenceSuggestion(
                    column=column,
                    kind="monotonic_increasing",
                    message=f"Datetime column '{column}' is monotonic increasing.",
                    details={"direction": "increasing"},
                )
            )
    return suggestions


def pairwise_suggestions(columns: Sequence[str], frame: pd.DataFrame) -> list[InferenceSuggestion]:
    suggestions: list[InferenceSuggestion] = []
    lower = {name.lower(): name for name in columns}
    for start_name, end_name in _common_pairs(lower):
        start_series = frame[lower[start_name]]
        end_series = frame[lower[end_name]]
        if start_series.isna().all() or end_series.isna().all():
            continue
        diffs = end_series - start_series
        numeric = pd.to_numeric(diffs, errors="coerce")
        if numeric.notna().mean() >= 0.9 and (numeric >= 0).all():
            suggestions.append(
                InferenceSuggestion(
                    column=f"{lower[start_name]}->{lower[end_name]}",
                    kind="range_pair",
                    message=f"'{lower[start_name]}' is never greater than '{lower[end_name]}'.",
                    details={"start": lower[start_name], "end": lower[end_name]},
                )
            )
    return suggestions


def _common_pairs(names: dict[str, str]) -> list[tuple[str, str]]:
    pairs = [("start", "end"), ("from", "to"), ("begin", "finish")]
    results: list[tuple[str, str]] = []
    for start, end in pairs:
        if start in names and end in names:
            results.append((start, end))
    return results
