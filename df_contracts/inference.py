from __future__ import annotations

from typing import List

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


def infer_contract(
    df: pd.DataFrame,
    name: str,
    version: str = "0.1.0",
    *,
    enum_max_cardinality: int = 50,
    enum_min_freq_ratio: float = 0.95,
    nullable_threshold: float = 0.0,
) -> Contract:
    frame = ensure_pandas(df)
    columns: List[ColumnSpec] = []
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
    unique_keys: list[list[str]] = []
    return Contract(name=name, version=version, columns=columns, unique_keys=unique_keys)
