# coding=UTF-8
# Copyright (c) 2025 Bird Software Solutions Ltd
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDX-License-Identifier: EPL-2.0
#
# Contributors:
#    Neil Mackenzie - initial API and implementation
#    Benjamin Arfa - improvements
#

"""
Import functions for SDD model data from CSV files.

This module provides functions for importing various SDD model entities
from CSV files, organized into logical groups:
- Basic entities (agencies, frameworks, domains, members, variables)
- Hierarchies (member hierarchies and their nodes)
- Report templates (tables, axes, cells, positions)
- Mappings (variable/member mappings and their relationships)
"""

# Orchestrator functions (main entry points)
from .import_report_templates_from_sdd import import_report_templates_from_sdd
from .import_semantic_integrations_from_sdd import import_semantic_integrations_from_sdd
from .import_hierarchies_from_sdd import import_hierarchies_from_sdd

# Basic import functions
from .import_maintenance_agencies import import_maintenance_agencies
from .import_frameworks import import_frameworks
from .import_domains import import_domains
from .import_members import import_members
from .import_variables import import_variables

# Hierarchy import functions
from .import_member_hierarchies import import_member_hierarchies
from .import_parent_members_with_children import import_parent_members_with_children
from .import_member_hierarchy_nodes import import_member_hierarchy_nodes

# Report template import functions
from .import_report_tables import import_report_tables
from .import_axis import import_axis
from .import_axis_ordinates import import_axis_ordinates
from .import_table_cells import import_table_cells
from .import_table_cells_csv_copy import import_table_cells_csv_copy
from .import_ordinate_items import import_ordinate_items
from .import_ordinate_items_csv_copy import import_ordinate_items_csv_copy
from .import_cell_positions import import_cell_positions
from .import_cell_positions_csv_copy import import_cell_positions_csv_copy

# Mapping import functions
from .import_variable_mappings import import_variable_mappings
from .import_variable_mapping_items import import_variable_mapping_items
from .import_member_mappings import import_member_mappings
from .import_member_mapping_items import import_member_mapping_items
from .import_mapping_definitions import import_mapping_definitions
from .import_mapping_to_cubes import import_mapping_to_cubes

# Utility functions
from .utilities import replace_dots, delete_hierarchy_warnings_files, delete_mapping_warnings_files

# Lookup functions
from .lookups import (
    find_member_mapping_with_id,
    find_member_with_id,
    find_member_hierarchy_with_id,
    find_variable_with_id,
    find_maintenance_agency_with_id,
    find_domain_with_id,
    find_table_with_id,
    find_axis_with_id,
    find_table_cell_with_id,
    find_axis_ordinate_with_id,
    find_variable_mapping_with_id,
    find_mapping_definition_with_id,
    find_member_with_id_for_hierarchy
)

# Warning writer functions
from .warning_writers import (
    save_missing_domains_to_csv,
    save_missing_members_to_csv,
    save_missing_variables_to_csv,
    save_missing_children_to_csv,
    save_missing_hierarchies_to_csv,
    save_missing_mapping_variables_to_csv,
    save_missing_mapping_members_to_csv,
    create_mappings_warnings_summary
)

# Database helper functions
from .database_helpers import (
    get_primary_key_column,
    backup_table_data,
    is_duplicate_content,
    restore_backed_up_data_bulk,
    bulk_insert_sqlite,
    bulk_insert_postgresql,
    bulk_insert_generic,
    cleanup_backup_table
)

# CSV copy importer
from .csv_copy_importer import create_instances_from_csv_copy


__all__ = [
    # Orchestrators
    'import_report_templates_from_sdd',
    'import_semantic_integrations_from_sdd',
    'import_hierarchies_from_sdd',

    # Basic imports
    'import_maintenance_agencies',
    'import_frameworks',
    'import_domains',
    'import_members',
    'import_variables',

    # Hierarchy imports
    'import_member_hierarchies',
    'import_parent_members_with_children',
    'import_member_hierarchy_nodes',

    # Report template imports
    'import_report_tables',
    'import_axis',
    'import_axis_ordinates',
    'import_table_cells',
    'import_table_cells_csv_copy',
    'import_ordinate_items',
    'import_ordinate_items_csv_copy',
    'import_cell_positions',
    'import_cell_positions_csv_copy',

    # Mapping imports
    'import_variable_mappings',
    'import_variable_mapping_items',
    'import_member_mappings',
    'import_member_mapping_items',
    'import_mapping_definitions',
    'import_mapping_to_cubes',
]
