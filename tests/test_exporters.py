from __future__ import annotations

import pandas as pd

from df_contracts import infer_contract
from df_contracts.exporters import to_dbt_tests, to_gx_suite


def build_contract(df: pd.DataFrame):
    contract = infer_contract(df, name="orders").contract
    columns = {col.name: col for col in contract.columns}
    columns["id"].unique = True
    columns["category"].nullable = False
    columns["category"].enum = ["A", "B"]
    columns["amount"].min = 0
    return contract


def test_dbt_export(sample_df: pd.DataFrame) -> None:
    contract = build_contract(sample_df)
    yaml = to_dbt_tests(contract, "orders")
    assert "models:" in yaml
    assert "accepted_values" in yaml


def test_gx_export(sample_df: pd.DataFrame) -> None:
    contract = build_contract(sample_df)
    suite = to_gx_suite(contract)
    expectation_types = {exp["expectation_type"] for exp in suite["expectations"]}
    assert "expect_column_values_to_not_be_null" in expectation_types
    assert "expect_column_values_to_be_in_set" in expectation_types
