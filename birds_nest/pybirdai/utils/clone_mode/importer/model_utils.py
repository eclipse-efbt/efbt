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
"""
Model and import order utilities for data import.
"""
import inspect
import logging
import re
from typing import Dict, List, Set, Type

from django.db import models, connection

logger = logging.getLogger(__name__)


def build_model_map() -> tuple:
    """
    Build mapping of table names to model classes.

    Returns:
        Tuple of (model_map dict, allowed_table_names set)
    """
    from pybirdai.models import bird_meta_data_model

    model_map = {}
    allowed_table_names = set()

    logger.debug("Building model map from bird_meta_data_model")
    model_count = 0

    for name, obj in inspect.getmembers(bird_meta_data_model):
        if inspect.isclass(obj) and issubclass(obj, models.Model) and obj != models.Model:
            model_map[obj._meta.db_table] = obj
            # Add table name to allowed list for SQL injection protection
            allowed_table_names.add(obj._meta.db_table)
            model_count += 1
            logger.debug(f"Added model {name} -> table {obj._meta.db_table}")

    logger.debug(f"Built model map with {model_count} models")
    return model_map, allowed_table_names


def get_import_order() -> List[str]:
    """
    Define the order in which tables should be imported to respect foreign key dependencies.

    Returns:
        List of table names in dependency order
    """
    return [
        'pybirdai_maintenance_agency',  # No dependencies
        'pybirdai_facet_collection',    # No dependencies
        'pybirdai_domain',              # Depends on maintenance_agency
        'pybirdai_framework',           # Depends on maintenance_agency
        'pybirdai_variable',            # Depends on maintenance_agency, domain
        'pybirdai_member',              # Depends on maintenance_agency, domain
        'pybirdai_subdomain',           # Depends on domain
        'pybirdai_framework_subdomain', # Depends on framework, subdomain
        'pybirdai_subdomain_enumeration', # Depends on member, subdomain
        'pybirdai_variable_set',        # Depends on maintenance_agency
        'pybirdai_variable_set_enumeration', # Depends on variable_set, variable, subdomain
        'pybirdai_member_hierarchy',    # Depends on maintenance_agency, domain
        'pybirdai_framework_hierarchy', # Depends on framework, member_hierarchy
        'pybirdai_member_hierarchy_node', # Depends on member_hierarchy, member
        'pybirdai_cube_structure',      # Depends on maintenance_agency
        'pybirdai_cube_structure_item', # Depends on cube_structure, variable, subdomain, variable_set, member
        'pybirdai_cube',                # Depends on maintenance_agency, framework, cube_structure
        'pybirdai_combination',         # Depends on maintenance_agency
        'pybirdai_combination_item',    # Depends on combination, variable, subdomain, variable_set, member
        'pybirdai_cube_to_combination', # Depends on cube, combination
        'pybirdai_cube_link',           # Depends on maintenance_agency, cube
        'pybirdai_cube_structure_item_link', # Depends on cube_link, cube_structure_item
        'pybirdai_member_link',         # Depends on cube_structure_item_link, member
        'pybirdai_table',               # Depends on maintenance_agency
        'pybirdai_framework_table',     # Depends on framework, table
        'pybirdai_axis',                # Depends on table
        'pybirdai_axis_ordinate',       # Depends on axis
        'pybirdai_ordinate_item',       # Depends on axis_ordinate, variable, member, member_hierarchy
        'pybirdai_table_cell',          # Depends on table
        'pybirdai_cell_position',       # Depends on table_cell, axis_ordinate
        'pybirdai_member_mapping',      # Depends on maintenance_agency
        'pybirdai_member_mapping_item', # Depends on member_mapping, variable, member
        'pybirdai_variable_mapping',    # Depends on maintenance_agency
        'pybirdai_variable_mapping_item', # Depends on variable_mapping, variable
        'pybirdai_mapping_definition',  # Depends on maintenance_agency, member_mapping, variable_mapping
        'pybirdai_mapping_to_cube',     # Depends on mapping_definition
        'pybirdai_mapping_ordinate_link', # Depends on mapping_definition, axis_ordinate
    ]


def get_table_name_from_csv_filename(filename: str) -> str:
    """
    Convert CSV filename back to table name.

    Args:
        filename: CSV filename (e.g., 'bird_domain.csv')

    Returns:
        Django table name (e.g., 'pybirdai_domain')
    """
    base_name = filename.replace('.csv', '')
    if base_name.startswith('bird_'):
        table_name = f"pybirdai_{base_name.replace('bird_', '')}"
    elif base_name.startswith('auth_') or base_name.startswith('django_'):
        table_name = base_name
    else:
        table_name = f"pybirdai_{base_name}"

    logger.debug(f"Converted filename '{filename}' to table name '{table_name}'")
    return table_name


def calculate_optimal_batch_size(model_class, base_batch_size: int = 250) -> int:
    """
    Calculate optimal batch size based on model field count and database constraints.

    Args:
        model_class: Django model class
        base_batch_size: Base batch size to start with

    Returns:
        Optimal batch size for bulk operations
    """
    field_count = len(model_class._meta.fields)

    # SQLite has a limit of 999 variables per statement
    # Leave some margin for safety
    max_variables = 900 if connection.vendor == 'sqlite' else 10000

    # Calculate max records per batch
    max_records_per_batch = max_variables // max(field_count, 1)

    # Use the smaller of base batch size or calculated maximum
    optimal_batch_size = min(base_batch_size, max_records_per_batch)

    # Ensure minimum batch size of 10
    optimal_batch_size = max(10, optimal_batch_size)

    logger.debug(f"Model {model_class.__name__}: {field_count} fields, "
                f"optimal batch size: {optimal_batch_size} (max variables: {max_variables})")

    return optimal_batch_size


def is_high_volume_table(table_name: str, row_count: int) -> bool:
    """
    Determine if a table should use bulk SQLite import based on volume and table characteristics.

    Args:
        table_name: Database table name
        row_count: Number of rows to import

    Returns:
        True if bulk import should be used
    """
    # Known high-volume tables that benefit from bulk import
    high_volume_tables = {
        'pybirdai_cell_position',
        'pybirdai_table_cell',
        'pybirdai_axis_ordinate',
        'pybirdai_ordinate_item'
    }

    # Use bulk import for known high-volume tables or tables with >50,000 rows
    return table_name in high_volume_tables or row_count > 50000


def is_safe_table_name(table_name: str, allowed_table_names: Set[str]) -> bool:
    """
    Validate table name against whitelist and pattern to prevent SQL injection.

    Args:
        table_name: Table name to validate
        allowed_table_names: Set of allowed table names

    Returns:
        True if table name is safe
    """
    if not table_name:
        return False
    # Check if table name matches expected pattern (letters, digits, underscores only)
    if not re.match(r'^[a-zA-Z][a-zA-Z0-9_]*$', table_name):
        return False
    # Check against whitelist of allowed table names
    return table_name in allowed_table_names


def should_import_table(table_name: str, framework_ids: List[str] = None) -> bool:
    """
    Check if a table should be imported based on framework filter.

    Args:
        table_name: The database table name (e.g., 'pybirdai_domain')
        framework_ids: Optional list of framework IDs to filter by

    Returns:
        True if the table should be imported, False otherwise
    """
    # If no framework filter, import all tables
    if not framework_ids:
        return True

    # Use FrameworkSelectionService to check if table should be included
    try:
        from pybirdai.services.framework_selection import FrameworkSelectionService
        for fid in framework_ids:
            if FrameworkSelectionService.should_include_table(fid, table_name):
                return True
        logger.debug(f"Skipping table {table_name} - not in whitelist for frameworks {framework_ids}")
        return False
    except ImportError:
        logger.warning("FrameworkSelectionService not available, importing all tables")
        return True
