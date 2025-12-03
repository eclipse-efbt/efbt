"""
Output Layer Mapping Workflow - Phase-based Transaction Handling

This package provides a modular, phase-based approach to generating output layer structures
with savepoint-based transaction management and FK validation between phases.

Main Components:
- PhaseExecutor: Orchestrates phase execution with savepoint handling
- TransactionValidator: FK validation utilities with PRAGMA checks
- Phases 1-5: Individual phase implementations for structure generation
"""

from .phase_executor import PhaseExecutor
from .transaction_validator import (
    validate_fks_for_phase,
    run_pragma_foreign_key_check,
    validate_orm_foreign_keys
)

__all__ = [
    'PhaseExecutor',
    'validate_fks_for_phase',
    'run_pragma_foreign_key_check',
    'validate_orm_foreign_keys',
]
