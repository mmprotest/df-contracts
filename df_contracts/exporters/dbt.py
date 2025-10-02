from __future__ import annotations

from ..schema import Contract


def to_dbt_tests(contract: Contract, table_name: str) -> str:
    lines = ["version: 2", "", "models:", f"  - name: {table_name}", "    columns:"]
    for column in contract.columns:
        lines.append(f"      - name: {column.name}")
        tests: list[str | dict[str, object]] = []
        if column.nullable is False:
            tests.append("not_null")
        if column.unique is True:
            tests.append("unique")
        if column.enum:
            tests.append({"accepted_values": {"values": column.enum}})
        if not tests:
            continue
        lines.append("        tests:")
        for test in tests:
            if isinstance(test, str):
                lines.append(f"          - {test}")
            else:
                values = ", ".join(test["accepted_values"]["values"])
                lines.append(f"          - accepted_values: {{ values: [{values}] }}")
    return "\n".join(lines) + "\n"
