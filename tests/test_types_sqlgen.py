from __future__ import annotations

import pandas as pd

from df_contracts import infer_contract
from df_contracts.exporters import from_contract_to_pydantic, from_contract_to_sql, from_contract_to_typeddict


def test_types_generation(sample_df: pd.DataFrame) -> None:
    contract = infer_contract(sample_df, name="orders").contract
    typed_code = from_contract_to_typeddict(contract)
    pydantic_code = from_contract_to_pydantic(contract)
    namespace: dict[str, object] = {}
    exec(typed_code, namespace)
    exec(pydantic_code, namespace)
    assert "OrdersRow" in namespace
    sql = from_contract_to_sql(contract, dialect="sqlite")
    assert sql.startswith("CREATE TABLE")
