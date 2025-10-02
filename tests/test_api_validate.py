from __future__ import annotations

import pandas as pd

from df_contracts import RuleSpec, infer_contract, validate


def _augment_contract(contract):
    columns = {col.name: col for col in contract.columns}
    columns["amount"].min = 0
    columns["email"].regex = r"^[^@]+@[^@]+\\.[^@]+$"
    columns["id"].unique = True
    columns["category"].allow_unknown = False
    columns["ratio"].max = 1
    return contract


def test_validate_pass(sample_df):
    contract = infer_contract(sample_df, name="orders")
    contract = _augment_contract(contract)
    contract.rules.append(
        RuleSpec(
            id="amount_positive",
            level="ERROR",
            kind="row",
            expr="amount >= 0",
            message="amount must be non-negative",
        )
    )
    contract.rules.append(
        RuleSpec(
            id="start_before_end",
            level="ERROR",
            kind="table",
            fn_name="start_le_end",
            params={"start": "start_date", "end": "end_date"},
            message="start must be <= end",
        )
    )
    report = validate(sample_df, contract)
    assert report.ok
    assert report.violations == []


def test_validate_failures(sample_df):
    contract = infer_contract(sample_df, name="orders")
    contract = _augment_contract(contract)
    contract.rules.append(
        RuleSpec(
            id="amount_positive",
            level="ERROR",
            kind="row",
            expr="amount >= 0",
            message="amount must be non-negative",
        )
    )
    contract.rules.append(
        RuleSpec(
            id="start_before_end",
            level="ERROR",
            kind="table",
            fn_name="start_le_end",
            params={"start": "start_date", "end": "end_date"},
            message="start must be <= end",
        )
    )
    broken = sample_df.copy()
    broken.loc[0, "category"] = "C"
    broken.loc[1, "email"] = "not-an-email"
    broken.loc[2, "amount"] = -10
    broken.loc[3, "id"] = 1
    broken.loc[4, "start_date"] = pd.Timestamp("2024-01-11")
    report = validate(broken, contract)
    assert not report.ok
    violation_ids = {violation["id"] for violation in report.violations}
    assert "column.category.enum" in violation_ids
    assert "column.email.regex" in violation_ids
    assert "column.amount.min" in violation_ids
    assert "column.id.unique" in violation_ids
    assert "rule.start_before_end" in violation_ids
