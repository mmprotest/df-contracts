from __future__ import annotations

from ..schema import Contract


def from_contract_to_sql(contract: Contract, dialect: str = "postgres") -> str:
    dialect = dialect.lower()
    type_mapping = _TYPE_MAPPING.get(dialect)
    if not type_mapping:
        raise ValueError(f"Unsupported SQL dialect: {dialect}")
    lines = [f"CREATE TABLE {contract.name} ("]
    column_lines: list[str] = []
    for column in contract.columns:
        sql_type = type_mapping.get(column.dtype.lower(), type_mapping.get("default", "TEXT"))
        nullable = " NOT NULL" if column.nullable is False else ""
        constraints = []
        if column.enum:
            allowed = ", ".join(f"'{value}'" for value in column.enum)
            constraints.append(f"CHECK ({column.name} IN ({allowed}))")
        if column.min is not None:
            constraints.append(f"CHECK ({column.name} >= {column.min})")
        if column.max is not None:
            constraints.append(f"CHECK ({column.name} <= {column.max})")
        constraint_sql = f" {' '.join(constraints)}" if constraints else ""
        column_lines.append(f"  {column.name} {sql_type}{nullable}{constraint_sql}")
    lines.append(",\n".join(column_lines))
    lines.append(")")
    return "\n".join(lines) + "\n"


_TYPE_MAPPING = {
    "postgres": {
        "int64": "BIGINT",
        "int32": "INTEGER",
        "float64": "DOUBLE PRECISION",
        "float32": "REAL",
        "bool": "BOOLEAN",
        "boolean": "BOOLEAN",
        "datetime64[ns]": "TIMESTAMP",
        "object": "TEXT",
        "string": "TEXT",
        "default": "TEXT",
    },
    "sqlite": {
        "int64": "INTEGER",
        "int32": "INTEGER",
        "float64": "REAL",
        "float32": "REAL",
        "bool": "INTEGER",
        "boolean": "INTEGER",
        "datetime64[ns]": "TEXT",
        "object": "TEXT",
        "string": "TEXT",
        "default": "TEXT",
    },
    "bigquery": {
        "int64": "INT64",
        "int32": "INT64",
        "float64": "FLOAT64",
        "float32": "FLOAT64",
        "bool": "BOOL",
        "boolean": "BOOL",
        "datetime64[ns]": "TIMESTAMP",
        "object": "STRING",
        "string": "STRING",
        "default": "STRING",
    },
}
