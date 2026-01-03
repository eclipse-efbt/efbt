"""
Library functions for output layer mapping workflow.

This module contains reusable utility functions and classes for:
- Cache management (cache_utils)
- Debug tracking (debug_tracker)
- Entity managers (agency, framework) - entity_managers
- Naming conventions (naming_utils)
- Table utilities for Z-variant handling (table_utils, table_cell_utils)
- Domain and member management (domain_manager)
- Subdomain management (subdomain_manager)
- Cube structure generation (cube_structure_generator)
- Combination creation (combination_creator)
- Mapping creation (mapping_creators)
- Mapping lookup building (mapping_lookup_builder)
- Reference table generation (reference_table_generator)
- FK validation (transaction_validator)
- Float subdomain utilities (float_subdomain_utils)
"""

# Cache utilities - master cache management
from .cache_utils import clear_all_caches, get_cache_stats

# Debug tracker
from .debug_tracker import (
    track_object,
    track_objects,
    track_message,
    get_tracked_count,
    initialize_debug_data,
    summarize_debug_data
)

# Entity managers
from .entity_managers import AgencyManager, FrameworkManager

# Naming utilities
from .naming_utils import NamingUtils

# Table utilities
from .table_utils import (
    get_base_table_id,
    is_z_variant_table,
    get_z_variant_member_id,
    clear_table_utils_cache
)

# Table cell utilities
from .table_cell_utils import (
    get_table_cells_via_cell_position,
    is_deduplicated_table,
    get_original_table_id,
    extract_z_axis_suffix,
    extract_base_table_code,
    get_z_axis_sibling_tables,
    get_all_z_axis_variants,
    get_table_cell_count,
    extract_z_axis_member_from_table_id,
    resolve_full_member_id,
    get_cells_for_table,
    filter_cells_by_ordinates
)

# Domain manager
from .domain_manager import DomainManager

# Subdomain manager
from .subdomain_manager import (
    SubdomainManager,
    clear_subdomain_cache
)

# Cube structure generator
from .cube_structure_generator import (
    CubeStructureGenerator,
    clear_cube_structure_cache
)

# Combination creator
from .combination_creator import (
    CombinationCreator,
    clear_combination_cache
)

# Mapping creators
from .mapping_creators import (
    VariableMappingCreator,
    MemberMappingCreator,
    MappingDefinitionCreator,
    OrdinateLinkCreator,
    MappingBatchCreator
)

# Mapping lookup builder
from .mapping_lookup_builder import (
    MappingLookupBuilder,
    build_mapping_lookups,
    clear_mapping_lookup_cache
)

# Reference table generator
from .reference_table_generator import generate_reference_table_artifacts

# Transaction validator
from .transaction_validator import (
    # Cached existence checks
    variable_exists,
    get_variable,
    member_exists,
    get_member,
    domain_exists,
    subdomain_exists,
    framework_exists,
    agency_exists,
    # Batch validation
    validate_variables_exist,
    validate_members_exist,
    # Target variable validation
    validate_target_variables,
    # FK validation
    run_pragma_foreign_key_check,
    validate_orm_foreign_keys,
    validate_fks_for_phase,
    # Cache management
    clear_validation_cache
)

# Float subdomain utilities
from .float_subdomain_utils import (
    ensure_float_subdomain_for_mtrc,
    check_float_subdomain_status
)

__all__ = [
    # Cache management
    'clear_all_caches',
    'get_cache_stats',
    # Debug tracker
    'track_object',
    'track_objects',
    'track_message',
    'get_tracked_count',
    'initialize_debug_data',
    'summarize_debug_data',
    # Entity managers
    'AgencyManager',
    'FrameworkManager',
    # Naming
    'NamingUtils',
    # Table utils
    'get_base_table_id',
    'is_z_variant_table',
    'get_z_variant_member_id',
    'clear_table_utils_cache',
    # Table cell utils
    'get_table_cells_via_cell_position',
    'is_deduplicated_table',
    'get_original_table_id',
    'extract_z_axis_suffix',
    'extract_base_table_code',
    'get_z_axis_sibling_tables',
    'get_all_z_axis_variants',
    'get_table_cell_count',
    'extract_z_axis_member_from_table_id',
    'resolve_full_member_id',
    'get_cells_for_table',
    'filter_cells_by_ordinates',
    # Domain manager
    'DomainManager',
    # Subdomain manager
    'SubdomainManager',
    'clear_subdomain_cache',
    # Cube structure generator
    'CubeStructureGenerator',
    'clear_cube_structure_cache',
    # Combination creator
    'CombinationCreator',
    'clear_combination_cache',
    # Mapping creators
    'VariableMappingCreator',
    'MemberMappingCreator',
    'MappingDefinitionCreator',
    'OrdinateLinkCreator',
    'MappingBatchCreator',
    # Mapping lookup builder
    'MappingLookupBuilder',
    'build_mapping_lookups',
    'clear_mapping_lookup_cache',
    # Reference table generator
    'generate_reference_table_artifacts',
    # Transaction validator - cached checks
    'variable_exists',
    'get_variable',
    'member_exists',
    'get_member',
    'domain_exists',
    'subdomain_exists',
    'framework_exists',
    'agency_exists',
    # Transaction validator - batch
    'validate_variables_exist',
    'validate_members_exist',
    # Transaction validator - target variables
    'validate_target_variables',
    # Transaction validator - FK
    'run_pragma_foreign_key_check',
    'validate_orm_foreign_keys',
    'validate_fks_for_phase',
    'clear_validation_cache',
    # Float subdomain
    'ensure_float_subdomain_for_mtrc',
    'check_float_subdomain_status',
]
