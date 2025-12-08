"""
Table utility functions for output layer mapping workflow.

This module provides helper functions for working with table IDs, particularly
for handling Z-variant (deduplicated) tables.
"""

import logging
import re

logger = logging.getLogger(__name__)


def get_base_table_id(table_id):
    """
    Extract base table ID from Z-variant table ID.

    Z-variant tables are created during DPM table deduplication when a table has
    a Z-axis (3rd dimension). The Z-variant table ID is formed by appending the
    member ID to the base table ID:

        base_table_id + '_' + member_id

    Examples:
        'EBA_COREP_C_07_00_a_4_0_EBA_qEC_EBA_qx0' → 'EBA_COREP_C_07_00_a_4_0'
        'EBA_FINREP_F_01_01_4_0_EBA_qEC_EBA_qx50' → 'EBA_FINREP_F_01_01_4_0'
        'AE_REF_F_32_01_REF_AE 3_2' → 'AE_REF_F_32_01_REF_AE 3_2' (no Z-axis, unchanged)

    The member ID suffix typically follows the pattern:
        _EBA_q<DOMAIN>_EBA_q<CODE>

    For example:
        - _EBA_qEC_EBA_qx0 (Exposure Class domain, member x0)
        - _EBA_qEC_EBA_qx50 (Exposure Class domain, member x50)

    Args:
        table_id (str): The table ID (may be base or Z-variant)

    Returns:
        str: The base table ID with Z-variant suffix removed

    Note:
        If the table_id doesn't match the Z-variant pattern, it's returned unchanged.
        This allows the function to be safely called on any table ID.
    """
    if not table_id:
        return table_id

    # Pattern for Z-variant suffix: _EBA_q<domain>_EBA_q<code>
    # Examples: _EBA_qEC_EBA_qx0, _EBA_qEC_EBA_qx50, _EBA_qEC_EBA_qx2026
    z_variant_pattern = r'_EBA_q[A-Z]+_EBA_q[a-z0-9]+$'

    match = re.search(z_variant_pattern, table_id)
    if match:
        # Extract base table by removing the Z-variant suffix
        base_table_id = table_id[:match.start()]
        logger.debug(f"Extracted base table '{base_table_id}' from Z-variant '{table_id}'")
        return base_table_id

    # No Z-variant pattern found, return as-is
    logger.debug(f"Table '{table_id}' is not a Z-variant (or pattern not recognized)")
    return table_id


def is_z_variant_table(table_id):
    """
    Check if a table ID represents a Z-variant (deduplicated) table.

    Args:
        table_id (str): The table ID to check

    Returns:
        bool: True if table_id matches Z-variant pattern, False otherwise
    """
    if not table_id:
        return False

    z_variant_pattern = r'_EBA_q[A-Z]+_EBA_q[a-z0-9]+$'
    return bool(re.search(z_variant_pattern, table_id))


def get_z_variant_member_id(table_id):
    """
    Extract the Z-axis member ID from a Z-variant table ID.

    Args:
        table_id (str): The Z-variant table ID

    Returns:
        str: The member ID (e.g., 'EBA_qEC_EBA_qx0'), or None if not a Z-variant

    Examples:
        'EBA_COREP_C_07_00_a_4_0_EBA_qEC_EBA_qx0' → 'EBA_qEC_EBA_qx0'
        'AE_REF_F_32_01_REF_AE 3_2' → None
    """
    if not table_id:
        return None

    z_variant_pattern = r'_(EBA_q[A-Z]+_EBA_q[a-z0-9]+)$'
    match = re.search(z_variant_pattern, table_id)

    if match:
        member_id = match.group(1)
        logger.debug(f"Extracted Z-axis member '{member_id}' from table '{table_id}'")
        return member_id

    return None
