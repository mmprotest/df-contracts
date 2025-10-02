from __future__ import annotations

from pathlib import Path
from typing import Any
import warnings

import pandas as pd
from pandas.api.types import pandas_dtype

try:  # pragma: no cover - optional dependency
    import polars as pl
except Exception:  # pragma: no cover
    pl = None  # type: ignore[assignment]


def ensure_pandas(df: Any) -> pd.DataFrame:
    """Return a pandas DataFrame, converting from polars when necessary."""

    if isinstance(df, pd.DataFrame):
        return df
    if pl is not None and isinstance(df, pl.DataFrame):  # pragma: no cover - optional
        warnings.warn("Converting polars.DataFrame to pandas.DataFrame", stacklevel=2)
        return df.to_pandas()
    raise TypeError("df_contracts.validate expects a pandas or polars DataFrame")


def normalize_dtype(dtype: Any) -> str:
    """Normalise dtype representation to a stable pandas string."""

    try:
        pd_dtype = pandas_dtype(dtype)
    except TypeError:
        return str(dtype)
    return str(pd_dtype)


def is_dtype_compatible(actual: str, expected: str) -> bool:
    actual_norm = normalize_dtype(actual).lower()
    expected_norm = normalize_dtype(expected).lower()
    if actual_norm == expected_norm:
        return True
    numeric_aliases = {
        "int64": {"int64", "int32", "int16", "int8", "int"},
        "float64": {"float64", "float32", "float", "double"},
        "bool": {"bool", "boolean"},
        "boolean": {"bool", "boolean"},
        "string": {"string", "object"},
    }
    for aliases in numeric_aliases.values():
        if expected_norm in aliases and actual_norm in aliases:
            return True
    if expected_norm.startswith("datetime64") and actual_norm.startswith("datetime64"):
        return True
    return False


def read_dataframe(path: str | Path, *, sample: float | None = None) -> pd.DataFrame:
    path = Path(path)
    suffix = path.suffix.lower()
    if suffix in {".csv", ".tsv", ".txt"}:
        df = pd.read_csv(path)
    elif suffix in {".parquet"}:
        df = pd.read_parquet(path)
    else:
        raise ValueError(f"Unsupported file extension: {path.suffix}")
    if sample:
        if not 0 < sample <= 1:
            raise ValueError("sample must be in (0, 1]")
        df = df.sample(frac=sample, random_state=0)
    return df


def head_records(frame: pd.DataFrame, *, limit: int) -> list[dict[str, Any]]:
    limited = frame.head(limit)
    return [row._asdict() for row in limited.itertuples(index=False, name="Row")]


__all__ = [
    "ensure_pandas",
    "normalize_dtype",
    "is_dtype_compatible",
    "read_dataframe",
    "head_records",
]
