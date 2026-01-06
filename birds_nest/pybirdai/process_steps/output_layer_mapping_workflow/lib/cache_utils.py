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
