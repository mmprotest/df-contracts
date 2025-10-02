from __future__ import annotations

from typing import Any, Literal, TypedDict


class ValidationStats(TypedDict):
    rows: int
    cols: int
    duration_ms: int


class ViolationDict(TypedDict):
    id: str
    level: Literal["ERROR", "WARN"]
    kind: Literal["column", "row", "table", "schema"]
    columns: list[str]
    summary: str
    count: int
    examples: list[dict[str, Any]]
