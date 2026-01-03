"""
Cache utilities for output layer mapping workflow.

Provides a centralized way to clear all module-level caches between workflow runs.
This is important for ensuring fresh data when processing multiple tables
or running workflows repeatedly.
"""

import logging

logger = logging.getLogger(__name__)


def clear_all_caches():
    """
    Clear all caches across all lib modules.

    Should be called at the start of each workflow run to ensure fresh data.
    Also useful for testing and memory management.

    Clears caches in:
    - table_utils (Z-axis extraction cache)
    - transaction_validator (existence check caches)
    - subdomain_manager (subdomain and member caches)
    - combination_creator (variable/member caches)
    - cube_structure_generator (structure/cube caches)
    - mapping_lookup_builder (mapping lookup caches)
    """
    from .table_utils import clear_table_utils_cache
    from .transaction_validator import clear_validation_cache
    from .subdomain_manager import clear_subdomain_cache
    from .combination_creator import clear_combination_cache
    from .cube_structure_generator import clear_cube_structure_cache
    from .mapping_lookup_builder import clear_mapping_lookup_cache

    clear_table_utils_cache()
    clear_validation_cache()
    clear_subdomain_cache()
    clear_combination_cache()
    clear_cube_structure_cache()
    clear_mapping_lookup_cache()

    logger.info("All lib module caches cleared")


def get_cache_stats() -> dict:
    """
    Get statistics about current cache sizes.

    Returns:
        Dict with cache names and their sizes
    """
    from . import table_utils
    from . import transaction_validator
    from . import subdomain_manager
    from . import combination_creator
    from . import cube_structure_generator
    from . import mapping_lookup_builder

    return {
        'table_utils._base_table_id_cache': len(table_utils._base_table_id_cache),
        'transaction_validator._variable_cache': len(transaction_validator._variable_cache),
        'transaction_validator._member_cache': len(transaction_validator._member_cache),
        'transaction_validator._domain_cache': len(transaction_validator._domain_cache),
        'transaction_validator._subdomain_cache': len(transaction_validator._subdomain_cache),
        'subdomain_manager._subdomain_cache': len(subdomain_manager._subdomain_cache),
        'subdomain_manager._single_member_cache': len(subdomain_manager._single_member_cache),
        'combination_creator._variable_cache': len(combination_creator._variable_cache),
        'combination_creator._member_cache': len(combination_creator._member_cache),
        'cube_structure_generator._subdomain_cache': len(cube_structure_generator._subdomain_cache),
        'cube_structure_generator._cube_structure_cache': len(cube_structure_generator._cube_structure_cache),
        'mapping_lookup_builder._variable_mapping_cache': len(mapping_lookup_builder._variable_mapping_cache),
        'mapping_lookup_builder._member_mapping_cache': len(mapping_lookup_builder._member_mapping_cache),
    }


# Query optimization hints for common operations
QUERY_OPTIMIZATION_HINTS = """
Query Optimization Tips for Output Layer Mapping Workflow:

1. VARIABLE lookups:
   - Use select_related('domain_id', 'subdomain_id') when fetching with FK data
   - Use only('variable_id', 'name', 'code') for existence checks

2. MEMBER lookups:
   - Use select_related('domain_id', 'maintenance_agency_id') for full data
   - Batch with in_bulk() when looking up multiple members

3. CUBE_STRUCTURE_ITEM queries:
   - Use select_related('variable_id', 'subdomain_id') for item details
   - Order by 'order' for consistent processing

4. TABLE_CELL queries:
   - Use prefetch_related('ordinate_items') for cell processing
   - Filter by table_id first to limit result set

5. MAPPING_DEFINITION queries:
   - Use select_related('variable_mapping_id', 'member_mapping_id')
   - Prefetch MEMBER_MAPPING_ITEM for mapping processing

6. General tips:
   - Use .exists() instead of .count() > 0 for existence checks
   - Use .first() with .filter() instead of .get() with try/except
   - Cache frequently accessed objects at module level
   - Clear caches between workflow runs with clear_all_caches()
"""


def print_query_optimization_hints():
    """Print query optimization hints to the logger."""
    logger.info(QUERY_OPTIMIZATION_HINTS)
