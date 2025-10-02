from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from df_contracts import RuleSpec, infer_contract


def build_contract(df: pd.DataFrame):
    contract = infer_contract(df, name="orders").contract
    columns = {col.name: col for col in contract.columns}
    columns["amount"].min = 0
    columns["id"].unique = True
    contract.rules.append(
        RuleSpec(
            id="amount_positive",
            level="ERROR",
            kind="row",
            expr="amount >= 0",
            message="amount must be non-negative",
        )
    )
    return contract


def test_plugin_must_match(sample_df: pd.DataFrame, tmp_path: Path, df_contracts) -> None:
    contract = build_contract(sample_df)
    df_contracts.must_match(contract, sample_df)
    broken = sample_df.copy()
    broken.loc[0, "amount"] = -1
    with pytest.raises(AssertionError):
        df_contracts.must_match(contract, broken)
    report_path = tmp_path / "agg.json"
    df_contracts.write_report(report_path)
    payload = json.loads(report_path.read_text())
    assert payload["runs"]
