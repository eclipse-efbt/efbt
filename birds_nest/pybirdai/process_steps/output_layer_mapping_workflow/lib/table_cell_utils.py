# coding=UTF-8
# Copyright (c) 2025 Arfa Digital Consulting
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDX-License-Identifier: EPL-2.0
#
# Contributors:
#    Benjamin Arfa - initial API and implementation
#

"""
Utilities for handling TABLE_CELL lookups for deduplicated tables.

After Z-axis table deduplication, TABLE_CELL records remain linked to the original
table via table_id FK. This module provides utilities to find cells through the
CELL_POSITION traversal path:

    TABLE -> AXIS -> AXIS_ORDINATE -> CELL_POSITION -> TABLE_CELL

This allows deduplicated tables to show their associated cells correctly.
"""

import re
import logging
from typing import Optional, List

logger = logging.getLogger(__name__)


def get_table_cells_via_cell_position(table):
    """
    Get TABLE_CELL records for a table by traversing the CELL_POSITION path.

    For deduplicated tables (Z-axis variants), cells are shared but positions
    are duplicated. This function finds cells via:
        TABLE -> AXIS -> AXIS_ORDINATE -> CELL_POSITION -> TABLE_CELL

    For Z-variant tables, this function automatically queries using the base table
    to find shared cells.

    Args:
        table: TABLE model instance

    Returns:
        QuerySet of unique TABLE_CELL objects
    """
    from pybirdai.models.bird_meta_data_model import (
        TABLE, AXIS, AXIS_ORDINATE, CELL_POSITION, TABLE_CELL
    )
    from .table_utils import get_base_table_id

    # For Z-variant tables, use base table for queries
    base_table_id = get_base_table_id(table.table_id)

    if base_table_id != table.table_id:
        # This is a Z-variant - query using base table
        logger.debug(f"Z-variant table '{table.table_id}' detected, using base table '{base_table_id}' for cell queries")
        base_table = TABLE.objects.filter(table_id=base_table_id).first()

        if base_table:
            query_table = base_table
            logger.debug(f"Using base table '{base_table_id}' for cell position traversal")
        else:
            logger.warning(f"Base table '{base_table_id}' not found, falling back to '{table.table_id}'")
            query_table = table
    else:
        query_table = table

    # Get all axes for the query table
    axes = AXIS.objects.filter(table_id=query_table)

    if not axes.exists():
        return TABLE_CELL.objects.none()

    # Get all ordinates for these axes
    axis_ids = axes.values_list('axis_id', flat=True)
    ordinate_ids = AXIS_ORDINATE.objects.filter(
        axis_id__in=axis_ids
    ).values_list('axis_ordinate_id', flat=True)

    if not ordinate_ids:
        return TABLE_CELL.objects.none()

    # Get unique cell IDs from CELL_POSITION
    cell_ids = CELL_POSITION.objects.filter(
        axis_ordinate_id__in=ordinate_ids
    ).values_list('cell_id', flat=True).distinct()

    # Return unique TABLE_CELL records
    return TABLE_CELL.objects.filter(cell_id__in=cell_ids)


def is_deduplicated_table(table_id: str) -> bool:
    """
    Check if a table is a Z-axis deduplicated variant.

    Deduplicated tables have pattern: {original_id}_{member_id}
    Common patterns include _EBA_, _qx, _cu, _ga suffixes.

    Args:
        table_id: The table's ID string

    Returns:
        True if this appears to be a deduplicated table
    """
    if not table_id:
        return False

    # Common Z-axis member patterns from DPM integration
    # These patterns indicate the table has been deduplicated with a Z-axis member
    z_axis_patterns = [
        r'_EBA_[a-zA-Z]{2}_',  # e.g., _EBA_qx, _EBA_cu
        r'_EBA_q[A-Z]',        # e.g., _EBA_qEC
        r'_[a-z]{2}\d+$',      # e.g., _qx50
    ]

    for pattern in z_axis_patterns:
        if re.search(pattern, table_id):
            return True

    return False


def get_original_table_id(deduplicated_table_id: str) -> str:
    """
    Extract the original table ID from a deduplicated table ID.

    DEPRECATED: Use get_base_table_id() from table_utils instead.
    This function is kept for backward compatibility but delegates to the
    primary implementation in table_utils.

    Deduplicated tables follow the pattern: {original_id}_{member_id}
    Example: 'C_07.00.a_EBA_qEC_EBA_qx50' -> 'C_07.00.a'

    Args:
        deduplicated_table_id: The deduplicated table's ID

    Returns:
        The original table ID (base ID before deduplication)
    """
    from .table_utils import get_base_table_id
    return get_base_table_id(deduplicated_table_id)


def extract_z_axis_suffix(table_id: str) -> str:
    """
    Extract the Z-axis member suffix from a deduplicated table ID.

    This returns everything after the base table code, which includes
    the Z-axis dimension and member codes.

    Examples:
        'EBA_COREP_C_07_00_a_4_0_EBA_qEC_EBA_qx2029' -> '_EBA_qEC_EBA_qx2029'
        'C_07.00.a_EBA_qEC_EBA_qx50' -> '_EBA_qEC_EBA_qx50'
        'C_07.00.a' -> '' (no Z-axis suffix)

    Args:
        table_id: The table's ID string

    Returns:
        The Z-axis suffix (starting with _EBA_) or empty string if not deduplicated
    """
    if not table_id or not is_deduplicated_table(table_id):
        return ''

    # Find the first _EBA_ occurrence and return everything from that point
    match = re.search(r'(_EBA_.+)$', table_id)
    if match:
        return match.group(1)

    return ''


def extract_base_table_code(table_id: str, table_code: str) -> str:
    """
    Extract the base table code without Z-axis suffix.

    This handles the case where table_code may already contain the Z-axis suffix
    (e.g., from TABLE.code field). It returns the base code with the suffix removed.

    Examples:
        table_id='EBA_COREP_C_07_00_a_4_0_EBA_qEC_EBA_qx1', table_code='C_07.00.a_EBA_qEC_EBA_qx1'
            -> 'C_07.00.a'
        table_id='EBA_COREP_C_07_00_a_4_0', table_code='C_07.00.a'
            -> 'C_07.00.a' (no suffix to remove)
        table_id='EBA_COREP_C_07_00_a_4_0_EBA_qEC_EBA_qx1', table_code='C 07.00.a_EBA_qEC_EBA_qx1'
            -> 'C 07.00.a'

    Args:
        table_id: The table's ID string (used to extract the Z-axis suffix pattern)
        table_code: The table code that may or may not contain the suffix

    Returns:
        The base table code without the Z-axis suffix
    """
    if not table_code:
        return table_code

    z_suffix = extract_z_axis_suffix(table_id)
    if not z_suffix:
        return table_code

    # The suffix from table_id starts with underscore: _EBA_qEC_EBA_qx1
    # In table_code it might be: _EBA_qEC_EBA_qx1 or EBA_qEC_EBA_qx1
    suffix_without_leading_underscore = z_suffix.lstrip('_')

    # Check if table_code ends with the suffix (with or without leading underscore)
    if table_code.endswith(z_suffix):
        return table_code[:-len(z_suffix)]
    elif table_code.endswith('_' + suffix_without_leading_underscore):
        return table_code[:-(len(suffix_without_leading_underscore) + 1)]
    elif table_code.endswith(suffix_without_leading_underscore):
        # Find where the suffix starts (might be preceded by underscore)
        idx = table_code.rfind(suffix_without_leading_underscore)
        if idx > 0:
            base = table_code[:idx]
            # Remove trailing underscore if present
            return base.rstrip('_')

    return table_code


def get_z_axis_sibling_tables(table_id: str):
    """
    Find all Z-axis sibling tables that share the same original base.

    Args:
        table_id: The current table's ID (can be original or deduplicated)

    Returns:
        QuerySet of TABLE objects that are Z-axis variants of the same base,
        excluding the current table
    """
    from pybirdai.models.bird_meta_data_model import TABLE

    if not table_id:
        return TABLE.objects.none()

    # Get the base table ID
    base_id = get_original_table_id(table_id)

    # Find all tables that start with this base ID followed by _EBA_
    siblings = TABLE.objects.filter(
        table_id__startswith=f"{base_id}_EBA_"
    ).exclude(table_id=table_id)

    return siblings


def get_all_z_axis_variants(table_id: str):
    """
    Get all Z-axis variants including the current table.

    Args:
        table_id: The current table's ID

    Returns:
        QuerySet of TABLE objects including current and all siblings
    """
    from pybirdai.models.bird_meta_data_model import TABLE

    if not table_id:
        return TABLE.objects.none()

    base_id = get_original_table_id(table_id)

    # Get all variants including current table
    return TABLE.objects.filter(
        table_id__startswith=f"{base_id}_EBA_"
    )


def get_table_cell_count(table) -> int:
    """
    Get the cell count for a table, handling deduplicated tables correctly.

    Args:
        table: TABLE model instance

    Returns:
        Number of cells associated with this table
    """
    cells = get_table_cells_via_cell_position(table)
    return cells.count()


def extract_z_axis_member_from_table_id(table_id: str) -> Optional[str]:
    """
    Extract the Z-axis member ID from a deduplicated table ID.

    Deduplicated tables follow the pattern: {original_table_id}_{z_axis_member_id}
    The Z-axis member ID typically contains '_EBA_' followed by the member code.

    Examples:
        'F_01_00_EBA_EC_EBA_qx50' -> 'EBA_qx50'
        'C_07.00.a_EBA_qEC_EBA_qx51' -> 'EBA_qx51'
        'EBA_COREP_C_07_00_a_4_0_EBA_qEC_EBA_qx50_Z' -> 'EBA_qx50'

    Args:
        table_id: The deduplicated table's ID string

    Returns:
        Z-axis member ID or None if not a deduplicated table
    """
    if not table_id:
        return None

    # Pattern 1: Extract last _EBA_ segment (most common)
    # Matches: anything + _EBA_ + member_code (ends before optional _Z suffix)
    match = re.search(r'_EBA_([a-zA-Z]{2}\d+)(?:_Z)?$', table_id)
    if match:
        return f"EBA_{match.group(1)}"  # Returns 'EBA_qx50'

    # Pattern 2: Extract full suffix after base table name
    # Matches: base_table + _EBA_qEC_EBA_qx50 format
    match = re.search(r'(_EBA_q[A-Z]{1,2}_EBA_[a-zA-Z]{2}\d+)', table_id)
    if match:
        # Extract just the member part (EBA_qx50 from _EBA_qEC_EBA_qx50)
        suffix = match.group(1)
        member_match = re.search(r'_EBA_([a-zA-Z]{2}\d+)$', suffix)
        if member_match:
            return f"EBA_{member_match.group(1)}"

    return None


def resolve_full_member_id(member_suffix: str, domain_id: str) -> str:
    """
    Resolve the full member_id from an extracted Z-axis member suffix and domain.

    The extracted Z-axis member from table IDs is in format 'EBA_qx50',
    but the actual MEMBER.member_id in the database includes the domain prefix:
    'EBA_EC_EBA_qx50'.

    This function reconstructs the full member_id for database lookups.

    Examples:
        resolve_full_member_id('EBA_qx50', 'EBA_EC') -> 'EBA_EC_EBA_qx50'
        resolve_full_member_id('EBA_qx51', 'EBA_EC') -> 'EBA_EC_EBA_qx51'

    Args:
        member_suffix: Extracted member suffix (e.g., 'EBA_qx50')
        domain_id: Domain ID for the Z-axis variable (e.g., 'EBA_EC')

    Returns:
        Full member_id for database lookup
    """
    if not member_suffix or not domain_id:
        return member_suffix

    # If member_suffix already includes the domain, return as-is
    if member_suffix.startswith(f"{domain_id}_"):
        return member_suffix

    # Reconstruct full member_id: domain_id + member_suffix
    # Example: 'EBA_EC' + '_EBA_qx50' = 'EBA_EC_EBA_qx50'
    return f"{domain_id}_{member_suffix}"


# ========== Phase 5 Support Functions ==========

def get_cells_for_table(table, table_id: str):
    """
    Get TABLE_CELL records for a table, handling Z-variants.

    For Z-variant tables, queries using the base table to find shared cells.
    Uses the CELL_POSITION traversal path:
        TABLE -> AXIS -> AXIS_ORDINATE -> CELL_POSITION -> TABLE_CELL

    Args:
        table: TABLE model instance
        table_id: The table ID string

    Returns:
        QuerySet of TABLE_CELL objects
    """
    from pybirdai.models.bird_meta_data_model import (
        TABLE, AXIS, AXIS_ORDINATE, CELL_POSITION, TABLE_CELL
    )
    from .table_utils import get_base_table_id

    # For Z-variant tables, use base table for queries
    base_table_id = get_base_table_id(table_id)

    if base_table_id != table_id:
        # This is a Z-variant table - query using base table
        logger.info(f"Detected Z-variant table '{table_id}', using base table '{base_table_id}' for cell queries")
        base_table = TABLE.objects.filter(table_id=base_table_id).first()

        if base_table:
            query_table = base_table
            logger.info(f"Successfully found base table '{base_table_id}' in database")
        else:
            # Base table doesn't exist - try with current table (fallback)
            logger.warning(f"Base table '{base_table_id}' not found, falling back to '{table_id}'")
            query_table = table
    else:
        # Not a Z-variant table - use table directly
        logger.info(f"Using table '{table_id}' directly (not a Z-variant)")
        query_table = table

    # Get cells via CELL_POSITION traversal
    table_axes = AXIS.objects.filter(table_id=query_table)
    table_ordinates = AXIS_ORDINATE.objects.filter(axis_id__in=table_axes)
    all_cell_positions = CELL_POSITION.objects.filter(axis_ordinate_id__in=table_ordinates)
    cell_ids = all_cell_positions.values_list('cell_id', flat=True).distinct()
    cells = TABLE_CELL.objects.filter(cell_id__in=cell_ids)

    logger.info(f"Found {cells.count()} cells via CELL_POSITION traversal "
                f"(query_table={query_table.table_id}, original_table={table_id})")

    return cells


def filter_cells_by_ordinates(cells, selected_ordinates: List[str]):
    """
    Filter cells to those matching selected ordinates.

    Uses AND logic across axes: cells must have positions matching
    selected ordinates in EACH axis.

    Args:
        cells: QuerySet of TABLE_CELL objects
        selected_ordinates: List of selected axis_ordinate_ids

    Returns:
        Filtered QuerySet of TABLE_CELL objects
    """
    from pybirdai.models.bird_meta_data_model import AXIS_ORDINATE, CELL_POSITION
    from collections import defaultdict

    if not selected_ordinates:
        return cells

    # Group selected ordinates by their axis
    ordinates_by_axis = defaultdict(list)
    selected_ordinate_objs = AXIS_ORDINATE.objects.filter(
        axis_ordinate_id__in=selected_ordinates
    ).select_related('axis_id')

    for ordinate in selected_ordinate_objs:
        if ordinate.axis_id:
            ordinates_by_axis[ordinate.axis_id_id].append(ordinate.axis_ordinate_id)

    logger.info(f"Selected ordinates grouped by {len(ordinates_by_axis)} axes")

    # For each axis, find cells with positions in that axis's selected ordinates
    # Then intersect across all axes
    filtered_cell_sets = []
    for axis_id, axis_ordinates in ordinates_by_axis.items():
        cells_for_axis = set(CELL_POSITION.objects.filter(
            axis_ordinate_id__in=axis_ordinates,
            cell_id__in=cells
        ).values_list('cell_id', flat=True).distinct())
        filtered_cell_sets.append(cells_for_axis)
        logger.debug(f"Axis {axis_id}: {len(axis_ordinates)} ordinates -> {len(cells_for_axis)} cells")

    # Intersect all sets - cells must match in ALL axes
    if filtered_cell_sets:
        final_cell_ids = filtered_cell_sets[0]
        for cell_set in filtered_cell_sets[1:]:
            final_cell_ids = final_cell_ids.intersection(cell_set)
        cells = cells.filter(cell_id__in=final_cell_ids)
        logger.info(f"After intersection: {len(final_cell_ids)} cells")

    return cells
