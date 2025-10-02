from __future__ import annotations

from typing import Dict

from ..schema import ColumnSpec, Contract


def from_contract_to_typeddict(contract: Contract) -> str:
    imports = ["from typing import Optional, TypedDict"]
    needs_datetime = any(python_type(col) == "datetime" for col in contract.columns)
    if needs_datetime:
        imports.append("from datetime import datetime")
    lines = imports + ["", f"class {camel_case(contract.name)}Row(TypedDict):"]
    for column in contract.columns:
        annotation = python_type(column)
        if column.nullable is not False:
            annotation = f"Optional[{annotation}]"
        lines.append(f"    \"{column.name}\": {annotation}")
    return "\n".join(lines) + "\n"


def from_contract_to_pydantic(contract: Contract) -> str:
    lines = ["from __future__ import annotations", ""]
    imports = ["from typing import Optional", "from pydantic import BaseModel"]
    if any(python_type(col) == "datetime" for col in contract.columns):
        imports.append("from datetime import datetime")
    lines.extend(imports)
    lines.append("")
    lines.append(f"class {camel_case(contract.name)}Row(BaseModel):")
    for column in contract.columns:
        annotation = python_type(column)
        if column.nullable is not False:
            annotation = f"Optional[{annotation}]"
        default = " = None" if column.nullable is not False else ""
        lines.append(f"    {safe_identifier(column.name)}: {annotation}{default}")
    return "\n".join(lines) + "\n"


def python_type(column: ColumnSpec) -> str:
    dtype = column.dtype.lower()
    mapping: Dict[str, str] = {
        "int64": "int",
        "int32": "int",
        "float64": "float",
        "float32": "float",
        "bool": "bool",
        "boolean": "bool",
        "datetime64[ns]": "datetime",
        "datetime64[ns, tz]": "datetime",
        "object": "str",
        "string": "str",
    }
    if dtype.startswith("datetime64"):
        return "datetime"
    return mapping.get(dtype, "str")


def camel_case(name: str) -> str:
    parts = [segment.capitalize() for segment in name.replace("_", " ").split() if segment]
    return "".join(parts) or "Contract"


def safe_identifier(name: str) -> str:
    cleaned = name.replace("-", "_")
    if cleaned and cleaned[0].isdigit():
        cleaned = f"col_{cleaned}"
    return cleaned
