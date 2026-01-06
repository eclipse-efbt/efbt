"""
Table utility functions for output layer mapping workflow.

This module provides helper functions for working with table IDs, particularly
for handling Z-variant (deduplicated) tables.

Includes dictionary-based caching for performance optimization.
"""

import logging
import re
from typing import Optional

logger = logging.getLogger(__name__)

# Module-level cache for base table ID lookups
_base_table_id_cache: dict = {}


def clear_table_utils_cache():
    """Clear all caches in this module. Call between workflow runs."""
    global _base_table_id_cache
    _base_table_id_cache.clear()
    logger.debug("table_utils cache cleared")


def get_base_table_id(table_id: str, strip_trailing_z: bool = True) -> str:
    """
    Extract base table ID from Z-variant table ID.

    Z-variant tables are created during DPM table deduplication when a table has
    a Z-axis (3rd dimension). The Z-variant table ID is formed by appending the
    member ID to the base table ID using double underscore as delimiter:

        base_table_id + '__' + member_id

    Examples:
        'EBA_COREP_C_07_00_a_4_0__EBA_qEC_EBA_qx0' → 'EBA_COREP_C_07_00_a_4_0'
        'EBA_FINREP_F_01_01_4_0__EBA_qEC_EBA_qx50' → 'EBA_FINREP_F_01_01_4_0'
        'AE_REF_F_32_01_REF_AE 3_2' → 'AE_REF_F_32_01_REF_AE 3_2' (no Z-axis, unchanged)
        'F01_01_3_0_Z' → 'F01_01_3_0' (if strip_trailing_z=True)

    The double underscore '__' clearly separates the base table ID from the
    Z-axis member ID, making parsing simple and unambiguous.

    Args:
        table_id: The table ID (may be base or Z-variant)
        strip_trailing_z: Also remove trailing '_Z' suffix (default True)

    Returns:
        The base table ID with Z-variant suffix removed

    Note:
        If the table_id doesn't contain '__', it's returned unchanged.
        This allows the function to be safely called on any table ID.
        Results are cached for performance.
    """
    if not table_id:
        return table_id

    # Check cache first
    cache_key = (table_id, strip_trailing_z)
    if cache_key in _base_table_id_cache:
        return _base_table_id_cache[cache_key]

    result = table_id

    # Primary pattern: Split on '__' delimiter (used by DPM deduplication)
    # Example: 'EBA_COREP_C_07_00_a_4_0__EBA_qEC_EBA_qx0' → 'EBA_COREP_C_07_00_a_4_0'
    if '__' in result:
        result = result.split('__')[0]
        logger.debug(f"Extracted base table '{result}' from Z-variant '{table_id}' (split on '__')")
    else:
        # Fallback: Legacy pattern with single underscore (for backward compatibility)
        # Pattern: _EBA_q<domain>_EBA_q<code>
        z_variant_pattern = r'_EBA_q[A-Z]+_EBA_q[a-z0-9]+$'
        match = re.search(z_variant_pattern, result)
        if match:
            result = result[:match.start()]
            logger.debug(f"Extracted base table '{result}' from Z-variant '{table_id}' (legacy pattern)")

    # Strip trailing _Z suffix (Z-axis indicator)
    if strip_trailing_z and result.endswith('_Z'):
        result = result[:-2]
        logger.debug(f"Stripped trailing _Z from '{table_id}' to '{result}'")

    # Cache the result
    _base_table_id_cache[cache_key] = result

    if result == table_id:
        logger.debug(f"Table '{table_id}' is not a Z-variant (or pattern not recognized)")

    return result


def is_z_variant_table(table_id):
    """
    Check if a table ID represents a Z-variant (deduplicated) table.

    Z-variant tables use '__' as delimiter between base table ID and member ID.

    Args:
        table_id (str): The table ID to check

    Returns:
        bool: True if table_id contains '__' delimiter, False otherwise
    """
    if not table_id:
        return False

    # Primary check: '__' delimiter (used by DPM deduplication)
    if '__' in table_id:
        return True

    # Fallback: Legacy pattern for backward compatibility
    z_variant_pattern = r'_EBA_q[A-Z]+_EBA_q[a-z0-9]+$'
    return bool(re.search(z_variant_pattern, table_id))


def get_z_variant_member_id(table_id):
    """
    Extract the Z-axis member ID from a Z-variant table ID.

    Z-variant tables use '__' as delimiter, making extraction simple:
        base_table_id + '__' + member_id

    Args:
        table_id (str): The Z-variant table ID

    Returns:
        str: The member ID (e.g., 'EBA_qEC_EBA_qx0'), or None if not a Z-variant

    Examples:
        'EBA_COREP_C_07_00_a_4_0__EBA_qEC_EBA_qx0' → 'EBA_qEC_EBA_qx0'
        'AE_REF_F_32_01_REF_AE 3_2' → None
    """
    if not table_id:
        return None

    # Primary pattern: Split on '__' delimiter
    if '__' in table_id:
        member_id = table_id.split('__')[1]
        logger.debug(f"Extracted Z-axis member '{member_id}' from table '{table_id}' (split on '__')")
        return member_id

    # Fallback: Legacy pattern for backward compatibility
    z_variant_pattern = r'_(EBA_q[A-Z]+_EBA_q[a-z0-9]+)$'
    match = re.search(z_variant_pattern, table_id)

    if match:
        member_id = match.group(1)
        logger.debug(f"Extracted Z-axis member '{member_id}' from table '{table_id}' (legacy pattern)")
        return member_id

    return None
