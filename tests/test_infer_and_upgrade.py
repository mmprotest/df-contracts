from __future__ import annotations

from df_contracts import compare, infer_contract, is_breaking_change


def test_infer_and_breaking_change(sample_df):
    old = infer_contract(sample_df, name="orders", version="0.1.0").contract
    new = infer_contract(sample_df, name="orders", version="0.2.0").contract
    columns = {col.name: col for col in new.columns}
    columns["amount"].dtype = "int64"
    diff = compare(old, new)
    assert is_breaking_change(diff)
    assert any("dtype" in change for change in diff["breaking"])
