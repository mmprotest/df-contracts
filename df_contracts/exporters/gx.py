from __future__ import annotations

from ..schema import Contract


def to_gx_suite(contract: Contract) -> dict[str, object]:
    expectations: list[dict[str, object]] = []
    for column in contract.columns:
        if column.nullable is False:
            expectations.append(
                {
                    "expectation_type": "expect_column_values_to_not_be_null",
                    "kwargs": {"column": column.name},
                }
            )
        if column.unique is True:
            expectations.append(
                {
                    "expectation_type": "expect_column_values_to_be_unique",
                    "kwargs": {"column": column.name},
                }
            )
        if column.enum:
            expectations.append(
                {
                    "expectation_type": "expect_column_values_to_be_in_set",
                    "kwargs": {"column": column.name, "value_set": column.enum},
                }
            )
        if column.min is not None:
            expectations.append(
                {
                    "expectation_type": "expect_column_min_to_be_between",
                    "kwargs": {"column": column.name, "min_value": column.min},
                }
            )
        if column.max is not None:
            expectations.append(
                {
                    "expectation_type": "expect_column_max_to_be_between",
                    "kwargs": {"column": column.name, "max_value": column.max},
                }
            )
    return {
        "expectation_suite_name": f"{contract.name}_suite",
        "expectations": expectations,
        "meta": {"contract_version": contract.version},
    }
