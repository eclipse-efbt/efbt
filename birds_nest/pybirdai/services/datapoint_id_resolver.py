"""
Resolve report TABLE_CELL rows to generated report cell datapoint IDs.
"""

from functools import lru_cache
from importlib import import_module
import logging
from typing import Iterable, Optional


logger = logging.getLogger(__name__)

REPORT_CELLS_MODULE = "pybirdai.process_steps.filter_code.report_cells"


def build_datapoint_id(cell, table=None, validate_class: bool = False) -> Optional[str]:
    """
    Return the datapoint ID accepted by ExecuteDataPoint for a TABLE_CELL.

    Newer generated COREP cells are named from the report template and cell ID,
    while older reference cells use a normalized table/code/version combination.
    Try the generated names in a stable order and optionally require that the
    corresponding Cell_* class exists in report_cells.
    """
    candidates = list(build_datapoint_id_candidates(cell, table=table))
    if not validate_class:
        return candidates[0] if candidates else None

    for candidate in candidates:
        if datapoint_class_exists(candidate):
            return candidate

    logger.warning(
        "No generated Cell_* class found for cell %s from candidates %s",
        getattr(cell, "cell_id", None),
        candidates,
    )
    return None


def build_datapoint_id_candidates(cell, table=None) -> Iterable[str]:
    """Yield possible ExecuteDataPoint IDs for a TABLE_CELL."""
    if cell is None:
        return

    table = table or getattr(cell, "table_id", None)
    table_id = _field_value(table, "table_id")
    cell_id = _field_value(cell, "cell_id")
    combination_id = _field_value(cell, "table_cell_combination_id")

    candidates = []

    qualified_cell_id = _build_template_cell_id(table_id, cell_id)
    legacy_id = _build_legacy_reference_id(cell, table)
    qualified_combination_id = _build_template_cell_id(table_id, combination_id)

    if _prefers_cell_id_datapoint(table_id, cell_id):
        candidates.extend([
            qualified_cell_id,
            legacy_id,
            combination_id,
            qualified_combination_id,
        ])
    else:
        candidates.extend([
            legacy_id,
            qualified_cell_id,
            combination_id,
            qualified_combination_id,
        ])

    yield from _dedupe(candidate for candidate in candidates if candidate)


def datapoint_class_exists(datapoint_id: str) -> bool:
    """Return whether report_cells contains Cell_<datapoint_id>."""
    if not datapoint_id:
        return False

    return f"Cell_{datapoint_id}" in _report_cell_class_names()


@lru_cache(maxsize=1)
def _report_cell_class_names() -> frozenset[str]:
    report_cells = import_module(REPORT_CELLS_MODULE)
    return frozenset(name for name in dir(report_cells) if name.startswith("Cell_"))


def _prefers_cell_id_datapoint(table_id: Optional[str], cell_id: Optional[str]) -> bool:
    if not table_id or not cell_id:
        return False

    table_id_upper = table_id.upper()
    return (
        table_id_upper.startswith("EBA_COREP_")
        or "_COREP_" in table_id_upper
    )


def _build_template_cell_id(table_id: Optional[str], cell_id: Optional[str]) -> Optional[str]:
    if not table_id or not cell_id:
        return None

    prefix = f"{table_id}_"
    if cell_id.startswith(prefix):
        return cell_id

    return f"{table_id}_{cell_id}"


def _build_legacy_reference_id(cell, table=None) -> Optional[str]:
    table = table or getattr(cell, "table_id", None)
    combination_id = _field_value(cell, "table_cell_combination_id")
    if not combination_id or not table:
        return None

    table_code = _field_value(table, "code")
    table_version = _field_value(table, "version")
    if not table_code or not table_version:
        return None

    combination_number = _extract_combination_number(combination_id)
    normalized_code = table_code.replace(".", "_")
    normalized_version = table_version.replace(".", "_").replace(" ", "_")
    return f"{normalized_code}_{normalized_version}_{combination_number}_REF"


def _extract_combination_number(combination_id: str) -> str:
    if combination_id.endswith("_REF"):
        combination_id = combination_id[:-4]

    if combination_id.startswith("EBA_"):
        remainder = combination_id[4:]
        if "_" not in remainder or remainder.split("_", 1)[0].isdigit():
            return remainder.split("_", 1)[0]

    numeric_parts = [part for part in combination_id.split("_") if part.isdigit()]
    if numeric_parts:
        return numeric_parts[-1]

    return combination_id


def _field_value(obj, field_name: str) -> Optional[str]:
    value = getattr(obj, field_name, None) if obj is not None else None
    if value is None:
        return None

    return str(value)


def _dedupe(values: Iterable[str]) -> Iterable[str]:
    seen = set()
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        yield value
