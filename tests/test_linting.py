from __future__ import annotations

import pandas as pd

from df_contracts import infer_contract
from df_contracts.linting import suggest_improvements


def test_lint_suggestions_apply(sample_df: pd.DataFrame) -> None:
    contract = infer_contract(sample_df, name="orders").contract
    columns = {col.name: col for col in contract.columns}
    columns["amount"].min = None
    report = suggest_improvements(contract, sample_df)
    assert report.suggestions
    updated = report.apply(contract)
    assert updated.version != contract.version
    updated_columns = {col.name: col for col in updated.columns}
    assert updated_columns["amount"].min is not None
