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
Table Deduplication Utilities Submodule

Memory-efficient utilities for duplicating table subgraphs based on Z-axis members.
Each utility is in a separate file for better code organization.
"""

from .identify_z_axis_tables import identify_z_axis_tables
from .extract_required_metadata import extract_required_metadata
from .duplicate_table_with_member import duplicate_table_with_member
from .duplicate_axes_with_member import duplicate_axes_with_member
from .duplicate_ordinates_with_member import duplicate_ordinates_with_member
from .duplicate_cells_with_member import duplicate_cells_with_member
from .duplicate_cell_positions_with_member import duplicate_cell_positions_with_member
from .duplicate_ordinate_cats_with_member import duplicate_ordinate_cats_with_member
from .load_config import load_z_axis_tables_from_config
from .get_axis_members import get_axis_domain_members, get_hierarchy_members
from .precompute_members import precompute_axis_members_mapping, precompute_duplication_info_from_ordinate_items
from .write_csvs import write_all_csvs_to_directory
from .should_skip_duplication import should_skip_duplication
from .bulk_duplication import (
    build_table_subgraph_cache,
    bulk_duplicate_table_for_members,
    bulk_duplicate_axes_for_members,
    bulk_duplicate_ordinates_for_members,
    bulk_duplicate_cells_for_members,
    bulk_duplicate_cell_positions_for_members,
    bulk_duplicate_ordinate_cats_for_members,
    duplicate_table_subgraph_bulk
)

__all__ = [
    'identify_z_axis_tables',
    'extract_required_metadata',
    'duplicate_table_with_member',
    'duplicate_axes_with_member',
    'duplicate_ordinates_with_member',
    'duplicate_cells_with_member',
    'duplicate_cell_positions_with_member',
    'duplicate_ordinate_cats_with_member',
    'load_z_axis_tables_from_config',
    'get_axis_domain_members',
    'get_hierarchy_members',
    'precompute_axis_members_mapping',
    'precompute_duplication_info_from_ordinate_items',
    'write_all_csvs_to_directory',
    'should_skip_duplication',
    # Bulk duplication (optimized)
    'build_table_subgraph_cache',
    'bulk_duplicate_table_for_members',
    'bulk_duplicate_axes_for_members',
    'bulk_duplicate_ordinates_for_members',
    'bulk_duplicate_cells_for_members',
    'bulk_duplicate_cell_positions_for_members',
    'bulk_duplicate_ordinate_cats_for_members',
    'duplicate_table_subgraph_bulk',
]
