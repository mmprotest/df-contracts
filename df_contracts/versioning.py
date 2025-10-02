from __future__ import annotations

from typing import Any, Dict, List

from .schema import Contract
from .utils import normalize_dtype


def _describe(message: str, *, column: str | None = None) -> str:
    return f"{column}: {message}" if column else message


def compare_contracts(old: Contract, new: Contract) -> dict[str, Any]:
    diff: dict[str, Any] = {
        "columns_added": [],
        "columns_removed": [],
        "columns_renamed": [],
        "dtype_changes": [],
        "nullability_changes": [],
        "enum_changes": [],
        "rule_changes": [],
        "breaking": [],
        "non_breaking": [],
    }
    old_cols = {col.name: col for col in old.columns}
    new_cols = {col.name: col for col in new.columns}
    for name in old_cols.keys() - new_cols.keys():
        diff["columns_removed"].append(name)
        diff["breaking"].append(_describe("column removed", column=name))
    for name in new_cols.keys() - old_cols.keys():
        diff["columns_added"].append(name)
        diff["non_breaking"].append(_describe("column added", column=name))
    for name in sorted(set(old_cols) & set(new_cols)):
        old_col = old_cols[name]
        new_col = new_cols[name]
        if normalize_dtype(old_col.dtype) != normalize_dtype(new_col.dtype):
            change = "narrow" if _is_dtype_narrowing(old_col.dtype, new_col.dtype) else "widen"
            diff["dtype_changes"].append({"column": name, "from": old_col.dtype, "to": new_col.dtype, "change": change})
            bucket = diff["breaking"] if change == "narrow" else diff["non_breaking"]
            bucket.append(_describe(f"dtype {change}ed", column=name))
        if old_col.nullable != new_col.nullable:
            kind = "tightened" if _nullable_stricter(old_col.nullable, new_col.nullable) else "relaxed"
            diff["nullability_changes"].append({"column": name, "from": old_col.nullable, "to": new_col.nullable, "change": kind})
            bucket = diff["breaking"] if kind == "tightened" else diff["non_breaking"]
            bucket.append(_describe(f"nullability {kind}", column=name))
        if old_col.enum or new_col.enum:
            old_enum = set(old_col.enum or [])
            new_enum = set(new_col.enum or [])
            removed = sorted(old_enum - new_enum)
            added = sorted(new_enum - old_enum)
            if removed or added:
                diff["enum_changes"].append({"column": name, "removed": removed, "added": added})
            if removed:
                diff["breaking"].append(_describe(f"enum removed {removed}", column=name))
            if added:
                diff["non_breaking"].append(_describe(f"enum added {added}", column=name))
    old_rules = {rule.id: rule for rule in old.rules}
    new_rules = {rule.id: rule for rule in new.rules}
    for rule_id in old_rules.keys() - new_rules.keys():
        diff["rule_changes"].append({"id": rule_id, "change": "removed"})
        diff["breaking"].append(f"rule {rule_id} removed")
    for rule_id in new_rules.keys() - old_rules.keys():
        diff["rule_changes"].append({"id": rule_id, "change": "added", "level": new_rules[rule_id].level})
        bucket = diff["breaking"] if new_rules[rule_id].level == "ERROR" else diff["non_breaking"]
        bucket.append(f"rule {rule_id} added")
    for rule_id in sorted(set(old_rules) & set(new_rules)):
        old_rule = old_rules[rule_id]
        new_rule = new_rules[rule_id]
        if old_rule.level != new_rule.level or old_rule.kind != new_rule.kind or old_rule.message != new_rule.message:
            diff["rule_changes"].append({"id": rule_id, "change": "modified", "from": old_rule.model_dump(), "to": new_rule.model_dump()})
            if new_rule.level == "ERROR" and old_rule.level == "WARN":
                diff["breaking"].append(f"rule {rule_id} escalated to ERROR")
            else:
                diff["non_breaking"].append(f"rule {rule_id} changed")
    return diff


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
