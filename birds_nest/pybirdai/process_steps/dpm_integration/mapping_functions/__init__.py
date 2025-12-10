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
DPM Integration Mapping Functions Module

This module provides functions for mapping DPM (Data Point Model) data structures
from source formats to target SDD (Structured Data Definition) formats.

The module is organized into:
- utils: Common utility functions for data processing
- Individual mapping modules: One per data domain (frameworks, domains, members, etc.)

All functions are exposed at the module level for backward compatibility.
"""

# Import all utility functions
from pybirdai.process_steps.dpm_integration.mapping_functions.utils import (
    pascal_to_upper_snake,
    clean_spaces_df,
    normalize_id_map,
    # Common mapping utilities
    apply_cascade_filter,
    convert_to_bool,
    map_column,
    select_final_columns,
)

# Import all mapping functions
from .frameworks import map_frameworks
from .domains import map_domains
from .members import map_members, ensure_x0_members
from .dimensions import map_dimensions
from .metrics import map_metrics
from .tables import (
    map_tables,
    load_table_to_framework_mapping,
    load_taxonomy_version_to_table_mapping
)
from .axis import map_axis
from .axis_ordinate import map_axis_ordinate
from .table_cell import map_table_cell
from .cell_position import map_cell_position
from .datapoint_version import map_datapoint_version
from .context_definition import map_context_definition
from .hierarchy import map_hierarchy, create_default_hierarchies
from .hierarchy_node import map_hierarchy_node, create_default_hierarchy_nodes, create_all_default_hierarchy_nodes
from .ordinate_categorisation import (
    map_ordinate_categorisation,
    traceback_restrictions,
    update_ordinate_items_with_default_hierarchies
)
from .table_duplication import process_all_tables

# Define what gets exported when using "from mapping_functions import *"
__all__ = [
    # Utility functions (modern pandas-based)
    'pascal_to_upper_snake',
    'clean_spaces_df',
    'normalize_id_map',
    # Common mapping utilities
    'apply_cascade_filter',
    'convert_to_bool',
    'map_column',
    'select_final_columns',

    # Mapping functions
    'map_frameworks',
    'map_domains',
    'map_members',
    'map_dimensions',
    'map_metrics',
    'map_tables',
    'map_axis',
    'map_axis_ordinate',
    'map_table_cell',
    'map_cell_position',
    'map_datapoint_version',
    'map_context_definition',
    'map_hierarchy',
    'map_hierarchy_node',
    'map_ordinate_categorisation',
    'ensure_x0_members',
    'create_default_hierarchies',
    'create_default_hierarchy_nodes',
    'create_all_default_hierarchy_nodes',
    'update_ordinate_items_with_default_hierarchies',

    # Helper functions
    'load_table_to_framework_mapping',
    'load_taxonomy_version_to_table_mapping',
    'traceback_restrictions',
    'process_all_tables'
]
