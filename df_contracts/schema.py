from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

import orjson
from pydantic import BaseModel, ConfigDict, Field
from tomli import loads as load_toml
from tomli_w import dumps as dump_toml

from .errors import ContractIOError


class ColumnSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    dtype: str
    nullable: bool | float = False
    unique: bool | list[str] | None = None
    min: Optional[str | float | int] = None
    max: Optional[str | float | int] = None
    enum: Optional[list[str]] = None
    allow_unknown: bool = False
    regex: Optional[str] = None
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    tz: Optional[str] = None
    description: Optional[str] = None
    unit: Optional[str] = None
    profiles: Dict[str, "ColumnProfileOverride"] = Field(default_factory=dict)


class RuleSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    level: str = Field(pattern=r"^(ERROR|WARN)$")
    kind: str = Field(pattern=r"^(row|table)$")
    expr: Optional[str] = None
    fn_name: Optional[str] = None
    params: Dict[str, Any] = Field(default_factory=dict)
    message: str


class ColumnProfileOverride(BaseModel):
    model_config = ConfigDict(extra="allow")

    nullable: bool | float | None = None
    allow_unknown: bool | None = None
    enum: Optional[list[str]] = None
    max_null_ratio: Optional[float] = None


class ProfileConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    max_null_ratio_multiplier: Optional[float] = None
    max_examples: Optional[int] = None


class ProfileOverrides(BaseModel):
    model_config = ConfigDict(extra="allow")

    columns: Dict[str, ColumnProfileOverride] = Field(default_factory=dict)
    default_max_examples: Optional[int] = None


class Contract(BaseModel):
    model_config = ConfigDict(extra="allow")

    name: str
    version: str
    description: Optional[str] = None
    columns: List[ColumnSpec]
    rules: List[RuleSpec] = Field(default_factory=list)
    profile_defaults: Optional[Dict[str, ProfileConfig]] = None
    profiles: Dict[str, "ProfileOverrides"] = Field(default_factory=dict)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    unique_keys: List[List[str]] = Field(default_factory=list)
    allow_extra_columns: bool = True

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump()

    def to_json(self) -> str:
        return orjson.dumps(self.to_dict(), option=orjson.OPT_INDENT_2).decode()

    def to_toml(self) -> str:
        return dump_toml(self.to_dict())

    def column_map(self) -> dict[str, ColumnSpec]:
        return {col.name: col for col in self.columns}


def load_contract(path: str | Path) -> Contract:
    file_path = Path(path)
    try:
        text = file_path.read_text()
    except OSError as exc:  # pragma: no cover - IO errors
        raise ContractIOError(str(exc)) from exc
    if file_path.suffix.lower() == ".json":
        data = orjson.loads(text)
    elif file_path.suffix.lower() == ".toml":
        data = load_toml(text)
    else:  # pragma: no cover - invalid extensions
        raise ContractIOError(f"Unsupported contract extension: {file_path.suffix}")
    return Contract.model_validate(data)


def save_contract(contract: Contract, path: str | Path) -> None:
    file_path = Path(path)
    if file_path.suffix.lower() == ".json":
        payload = contract.to_json()
    elif file_path.suffix.lower() == ".toml":
        payload = contract.to_toml()
    else:  # pragma: no cover - invalid extension
        raise ContractIOError(f"Unsupported contract extension: {file_path.suffix}")
    file_path.write_text(payload)
