from __future__ import annotations


class DFContractsError(Exception):
    """Base exception for df-contracts."""


class ContractIOError(DFContractsError):
    """Raised when contract files cannot be loaded or saved."""


class SchemaValidationError(DFContractsError):
    """Raised when a DataFrame violates the declared schema."""


class RuleExecutionError(DFContractsError):
    """Raised when a rule cannot be executed."""
