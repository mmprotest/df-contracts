from __future__ import annotations

from typing import Callable, Dict, List

import pandas as pd


TableCheck = Callable[[pd.DataFrame, dict[str, object]], pd.DataFrame]


def start_le_end(frame: pd.DataFrame, params: dict[str, object]) -> pd.DataFrame:
    start_col = str(params["start"])
    end_col = str(params["end"])
    mask = frame[start_col] > frame[end_col]
    return frame.loc[mask, [start_col, end_col]]


def non_decreasing_by_key(frame: pd.DataFrame, params: dict[str, object]) -> pd.DataFrame:
    column = str(params["col"])
    by_cols = [str(col) for col in params.get("by", [])]
    if not by_cols:
        diffs = frame[column].diff()
        mask = diffs < 0
        return frame.loc[mask, [column]]
    violations: List[pd.DataFrame] = []
    for _, group in frame.groupby(by_cols, dropna=False):
        diffs = group[column].diff()
        mask = diffs < 0
        if mask.any():
            violations.append(group.loc[mask, by_cols + [column]])
    if not violations:
        return frame.iloc[0:0]
    return pd.concat(violations, axis=0)


def within_tolerance(frame: pd.DataFrame, params: dict[str, object]) -> pd.DataFrame:
    lhs = str(params["lhs"])
    rhs = str(params["rhs"])
    tol = float(params.get("tol", 0.0))
    diff = (frame[lhs] - frame[rhs]).abs()
    mask = diff > tol
    return frame.loc[mask, [lhs, rhs]]


def functional_dependency(frame: pd.DataFrame, params: dict[str, object]) -> pd.DataFrame:
    lhs = [str(col) for col in params.get("lhs", [])]
    rhs = [str(col) for col in params.get("rhs", [])]
    if not lhs or not rhs:
        return frame.iloc[0:0]
    dupes = frame.groupby(lhs, dropna=False)[rhs].nunique(dropna=False)
    mask = (dupes > 1).any(axis=1)
    if not mask.any():
        return frame.iloc[0:0]
    violating_keys = dupes[mask].index
    merged = frame.set_index(lhs, drop=False)
    return merged.loc[violating_keys].reset_index(drop=True)[lhs + rhs]


TABLE_CHECKS: Dict[str, TableCheck] = {
    "start_le_end": start_le_end,
    "non_decreasing_by_key": non_decreasing_by_key,
    "within_tolerance": within_tolerance,
    "functional_dependency": functional_dependency,
}
