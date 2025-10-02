from __future__ import annotations

from typing import Any, Dict, List

from .schema import Contract
from .utils import normalize_dtype


def _describe(message: str, *, column: str | None = None) -> str:
    return f"{column}: {message}" if column else message


def compare_contracts(old: Contract, new: Contract) -> dict[str, Any]:
    breaking: List[str] = []
    non_breaking: List[str] = []
    changed_columns: Dict[str, dict[str, Any]] = {}
    old_cols = {col.name: col for col in old.columns}
    new_cols = {col.name: col for col in new.columns}
    for name, old_col in old_cols.items():
        if name not in new_cols:
            breaking.append(_describe("column removed", column=name))
            continue
        new_col = new_cols[name]
        col_changes: dict[str, Any] = {}
        if normalize_dtype(old_col.dtype) != normalize_dtype(new_col.dtype):
            col_changes["dtype"] = {"from": old_col.dtype, "to": new_col.dtype}
            if _is_dtype_narrowing(old_col.dtype, new_col.dtype):
                breaking.append(_describe("dtype narrowed", column=name))
            else:
                non_breaking.append(_describe("dtype widened", column=name))
        old_nullable = old_col.nullable
        new_nullable = new_col.nullable
        if old_nullable != new_nullable:
            col_changes["nullable"] = {"from": old_nullable, "to": new_nullable}
            if _nullable_stricter(old_nullable, new_nullable):
                breaking.append(_describe("nullability tightened", column=name))
            else:
                non_breaking.append(_describe("nullability relaxed", column=name))
        if old_col.enum and new_col.enum:
            old_enum = set(old_col.enum)
            new_enum = set(new_col.enum)
            removed = sorted(old_enum - new_enum)
            added = sorted(new_enum - old_enum)
            if removed:
                breaking.append(_describe(f"enum removed values {removed}", column=name))
            if added:
                non_breaking.append(_describe(f"enum added values {added}", column=name))
            if removed or added:
                col_changes["enum"] = {"removed": removed, "added": added}
        if col_changes:
            changed_columns[name] = col_changes
    for name in new_cols.keys() - old_cols.keys():
        non_breaking.append(_describe("column added", column=name))
    rule_changes: dict[str, Any] = {}
    old_rules = {rule.id: rule for rule in old.rules}
    new_rules = {rule.id: rule for rule in new.rules}
    for rule_id, old_rule in old_rules.items():
        if rule_id not in new_rules:
            breaking.append(f"rule {rule_id} removed")
            continue
        new_rule = new_rules[rule_id]
        if old_rule.level != new_rule.level or old_rule.kind != new_rule.kind or old_rule.message != new_rule.message:
            rule_changes[rule_id] = {
                "from": old_rule.model_dump(),
                "to": new_rule.model_dump(),
            }
            if new_rule.level == "ERROR" and old_rule.level == "WARN":
                breaking.append(f"rule {rule_id} escalated to ERROR")
    for rule_id in new_rules.keys() - old_rules.keys():
        new_rule = new_rules[rule_id]
        impact = "breaking" if new_rule.level == "ERROR" else "non-breaking"
        (breaking if impact == "breaking" else non_breaking).append(f"rule {rule_id} added")
    return {
        "breaking": breaking,
        "non_breaking": non_breaking,
        "changed_columns": changed_columns,
        "changed_rules": rule_changes,
    }


def _is_dtype_narrowing(old: str, new: str) -> bool:
    old_norm = normalize_dtype(old)
    new_norm = normalize_dtype(new)
    if old_norm == new_norm:
        return False
    numeric_order = ["int8", "int16", "int32", "int64", "float32", "float64"]
    if old_norm in numeric_order and new_norm in numeric_order:
        return numeric_order.index(new_norm) < numeric_order.index(old_norm)
    if old_norm.startswith("float") and new_norm.startswith("int"):
        return True
    if old_norm.startswith("datetime64") and not new_norm.startswith("datetime64"):
        return True
    return True


def _nullable_stricter(old: bool | float, new: bool | float) -> bool:
    def to_ratio(value: bool | float) -> float:
        if isinstance(value, bool):
            return 1.0 if value else 0.0
        return float(value)

    return to_ratio(new) < to_ratio(old)


def is_breaking_change(diff: dict[str, Any]) -> bool:
    return bool(diff.get("breaking"))
