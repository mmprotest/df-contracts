from __future__ import annotations

from .api import compare, is_breaking_change, validate
from .drift import DriftReport, DriftSnapshot, compare_snapshots, snapshot
from .inference import InferenceResult, infer_contract
from .linting import LintReport, LintSuggestion, suggest_improvements
from .report import ValidationReport
from .schema import ColumnSpec, Contract, RuleSpec, load_contract, save_contract
from .versioning import compare_contracts

__all__ = [
    "ColumnSpec",
    "Contract",
    "RuleSpec",
    "ValidationReport",
    "DriftSnapshot",
    "DriftReport",
    "compare",
    "compare_contracts",
    "infer_contract",
    "InferenceResult",
    "is_breaking_change",
    "snapshot",
    "compare_snapshots",
    "LintReport",
    "LintSuggestion",
    "suggest_improvements",
    "load_contract",
    "save_contract",
    "validate",
]
