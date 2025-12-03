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
Bulk duplication utilities for efficient Z-axis table deduplication.

Instead of duplicating one member at a time, these functions duplicate
all members at once using vectorized operations for 3-5x speedup.
"""

import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)


def build_table_subgraph_cache(z_axis_tables, axes_df, ordinates_df, cells_df,
                                cell_positions_df, ordinate_cat_df):
    """
    Pre-cache all subgraph data for Z-axis tables to avoid repeated filtering.

    Args:
        z_axis_tables: DataFrame with TABLE_ID and Z_AXIS_ID columns
        axes_df, ordinates_df, cells_df, cell_positions_df, ordinate_cat_df: Source DataFrames

    Returns:
        dict: {table_id: {
            'table_row': Series,
            'axes': DataFrame,
            'ordinates': DataFrame,
            'cells': DataFrame,
            'cell_positions': DataFrame,
            'ordinate_cats': DataFrame,
            'axis_ids': list,
            'ordinate_ids': list,
            'cell_ids': list
        }}
    """
    cache = {}
    table_ids = z_axis_tables['TABLE_ID'].unique()

    # Pre-filter all DataFrames by table_id sets for efficiency
    z_table_set = set(table_ids)

    # Filter axes once
    all_z_axes = axes_df[axes_df['TABLE_ID'].isin(z_table_set)]
    all_z_axis_ids = set(all_z_axes['AXIS_ID'].unique())

    # Filter ordinates once
    all_z_ordinates = ordinates_df[ordinates_df['AXIS_ID'].isin(all_z_axis_ids)]
    all_z_ordinate_ids = set(all_z_ordinates['AXIS_ORDINATE_ID'].unique())

    # Filter cells once
    all_z_cells = cells_df[cells_df['TABLE_ID'].isin(z_table_set)]
    all_z_cell_ids = set(all_z_cells['CELL_ID'].unique())

    # Filter cell_positions once
    all_z_positions = cell_positions_df[cell_positions_df['CELL_ID'].isin(all_z_cell_ids)]

    # Filter ordinate_cats once
    all_z_ordinate_cats = ordinate_cat_df[ordinate_cat_df['AXIS_ORDINATE_ID'].isin(all_z_ordinate_ids)]

    # Now build per-table cache from pre-filtered data
    for _, table_row in z_axis_tables.iterrows():
        table_id = str(table_row['TABLE_ID'])

        # Get table's axes
        table_axes = all_z_axes[all_z_axes['TABLE_ID'] == table_id]
        axis_ids = table_axes['AXIS_ID'].unique().tolist()

        # Get table's ordinates
        table_ordinates = all_z_ordinates[all_z_ordinates['AXIS_ID'].isin(axis_ids)]
        ordinate_ids = table_ordinates['AXIS_ORDINATE_ID'].unique().tolist()

        # Get table's cells
        table_cells = all_z_cells[all_z_cells['TABLE_ID'] == table_id]
        cell_ids = table_cells['CELL_ID'].unique().tolist()

        # Get table's cell_positions (drop ID - SQLite will auto-generate to avoid duplicates)
        table_positions = all_z_positions[all_z_positions['CELL_ID'].isin(cell_ids)]
        if 'ID' in table_positions.columns:
            table_positions = table_positions.drop(columns=['ID'])

        # Get table's ordinate_cats
        table_ordinate_cats = all_z_ordinate_cats[all_z_ordinate_cats['AXIS_ORDINATE_ID'].isin(ordinate_ids)]

        cache[table_id] = {
            'table_row': table_row,
            'axes': table_axes.copy(),
            'ordinates': table_ordinates.copy(),
            'cells': table_cells.copy(),
            'cell_positions': table_positions.copy(),
            'ordinate_cats': table_ordinate_cats.copy(),
            'axis_ids': axis_ids,
            'ordinate_ids': ordinate_ids,
            'cell_ids': cell_ids
        }

    return cache


def bulk_duplicate_table_for_members(table_row, members_df):
    """
    Duplicate a table row for ALL members at once.

    Args:
        table_row: Series with table data
        members_df: DataFrame with MEMBER_ID and NAME columns

    Returns:
        DataFrame with duplicated table rows
    """
    n_members = len(members_df)
    if n_members == 0:
        return pd.DataFrame()

    # Create base DataFrame by repeating table_row
    table_dict = table_row.to_dict()
    base_df = pd.DataFrame([table_dict] * n_members)

    # Get member info as arrays
    member_ids = members_df['MEMBER_ID'].astype(str).str.replace(' ', '_').str.replace('.', '_').values
    member_names = members_df['NAME'].astype(str).values

    # Vectorized ID/CODE/NAME updates
    original_table_id = str(table_dict.get('TABLE_ID', ''))
    base_df['TABLE_ID'] = original_table_id + '_' + pd.Series(member_ids)

    if 'CODE' in base_df.columns:
        original_code = str(table_dict.get('CODE', ''))
        base_df['CODE'] = original_code + '_' + pd.Series(member_ids)

    if 'NAME' in base_df.columns:
        original_name = str(table_dict.get('NAME', ''))
        base_df['NAME'] = original_name + ' - Z axis : ' + pd.Series(member_names)

    return base_df


def bulk_duplicate_axes_for_members(table_axes, table_id, members_df):
    """
    Duplicate axes for ALL members at once using vectorized operations.

    Option A: Replace TABLE_ID prefix in AXIS_ID to propagate the TABLE_ID change.
    Example: TABLE_ID T → T_M means AXIS_ID T_X → T_M_X (not T_X_M)

    Args:
        table_axes: DataFrame of axes for a single table
        table_id: Original table ID
        members_df: DataFrame with MEMBER_ID and NAME columns

    Returns:
        tuple: (all_new_axes_df, axis_id_mappings)
            - all_new_axes_df: DataFrame with all duplicated axes
            - axis_id_mappings: dict {member_id: {old_axis_id: new_axis_id}}
    """
    if table_axes.empty or members_df.empty:
        return pd.DataFrame(), {}

    n_axes = len(table_axes)
    n_members = len(members_df)

    # Get member info as arrays
    member_ids = members_df['MEMBER_ID'].astype(str).str.replace(' ', '_').str.replace('.', '_').values
    member_names = members_df['NAME'].astype(str).values

    # Store original axis IDs and table_id
    original_axis_ids = table_axes['AXIS_ID'].astype(str).values
    original_table_id = str(table_id)

    # Create all duplicates at once: repeat axes n_members times
    all_axes = pd.concat([table_axes] * n_members, ignore_index=True)

    # Build new IDs by replacing TABLE_ID prefix
    new_axis_id_list = []
    new_table_id_list = []
    axis_id_mappings = {}

    for i, member_id in enumerate(member_ids):
        new_table_id = f"{original_table_id}_{member_id}"
        member_axis_mappings = {}

        for j, orig_axis_id in enumerate(original_axis_ids):
            # Replace TABLE_ID prefix with new TABLE_ID
            new_axis_id = orig_axis_id.replace(original_table_id, new_table_id, 1)
            new_axis_id_list.append(new_axis_id)
            new_table_id_list.append(new_table_id)
            member_axis_mappings[orig_axis_id] = new_axis_id

        axis_id_mappings[member_id] = member_axis_mappings

    all_axes['AXIS_ID'] = new_axis_id_list
    all_axes['TABLE_ID'] = new_table_id_list

    # Update CODE by replacing TABLE_ID prefix
    if 'CODE' in all_axes.columns:
        original_codes = table_axes['CODE'].astype(str).values
        new_code_list = []
        for i, member_id in enumerate(member_ids):
            new_table_id = f"{original_table_id}_{member_id}"
            for orig_code in original_codes:
                new_code = orig_code.replace(original_table_id, new_table_id, 1)
                new_code_list.append(new_code)
        all_axes['CODE'] = new_code_list

    # Update NAME with member info
    if 'NAME' in all_axes.columns:
        member_name_suffixes = np.repeat(member_names, n_axes)
        all_axes['NAME'] = all_axes['NAME'].astype(str) + ' - Z axis : ' + member_name_suffixes

    return all_axes, axis_id_mappings


def bulk_duplicate_ordinates_for_members(table_ordinates, axis_id_mappings, members_df):
    """
    Duplicate ordinates for ALL members at once using vectorized operations.

    Option A: Replace AXIS_ID prefix in ORDINATE_ID to propagate the ID change.
    Example: AXIS_ID T_X → T_M_X means ORDINATE_ID T_X_O → T_M_X_O (not T_X_O_M)

    Args:
        table_ordinates: DataFrame of ordinates for a single table
        axis_id_mappings: dict {member_id: {old_axis_id: new_axis_id}}
        members_df: DataFrame with MEMBER_ID and NAME columns

    Returns:
        tuple: (all_new_ordinates_df, ordinate_id_mappings)
    """
    if table_ordinates.empty or members_df.empty:
        return pd.DataFrame(), {}

    n_ordinates = len(table_ordinates)
    n_members = len(members_df)

    # Get member info
    member_ids = members_df['MEMBER_ID'].astype(str).str.replace(' ', '_').str.replace('.', '_').values
    member_names = members_df['NAME'].astype(str).values

    # Store original IDs
    original_ordinate_ids = table_ordinates['AXIS_ORDINATE_ID'].astype(str).values
    original_axis_ids = table_ordinates['AXIS_ID'].astype(str).values

    # Create all duplicates at once
    all_ordinates = pd.concat([table_ordinates] * n_members, ignore_index=True)

    # Build new IDs by replacing AXIS_ID prefix
    new_ordinate_id_list = []
    new_axis_id_list = []
    ordinate_id_mappings = {}

    for i, member_id in enumerate(member_ids):
        axis_mapping = axis_id_mappings[member_id]
        member_ordinate_mappings = {}

        for j in range(n_ordinates):
            orig_ordinate_id = original_ordinate_ids[j]
            orig_axis_id = original_axis_ids[j]
            new_axis_id = axis_mapping.get(orig_axis_id, orig_axis_id)

            # Replace AXIS_ID prefix with new AXIS_ID in ORDINATE_ID
            new_ordinate_id = orig_ordinate_id.replace(orig_axis_id, new_axis_id, 1)
            new_ordinate_id_list.append(new_ordinate_id)
            new_axis_id_list.append(new_axis_id)
            member_ordinate_mappings[orig_ordinate_id] = new_ordinate_id

        ordinate_id_mappings[member_id] = member_ordinate_mappings

    all_ordinates['AXIS_ORDINATE_ID'] = new_ordinate_id_list
    all_ordinates['AXIS_ID'] = new_axis_id_list

    # Update CODE by replacing AXIS_ID prefix
    if 'CODE' in all_ordinates.columns:
        original_codes = table_ordinates['CODE'].astype(str).values
        new_code_list = []
        for i, member_id in enumerate(member_ids):
            axis_mapping = axis_id_mappings[member_id]
            for j in range(n_ordinates):
                orig_code = original_codes[j]
                orig_axis_id = original_axis_ids[j]
                new_axis_id = axis_mapping.get(orig_axis_id, orig_axis_id)
                new_code = orig_code.replace(orig_axis_id, new_axis_id, 1)
                new_code_list.append(new_code)
        all_ordinates['CODE'] = new_code_list

    # Update NAME with member info
    if 'NAME' in all_ordinates.columns:
        member_name_suffixes = np.repeat(member_names, n_ordinates)
        all_ordinates['NAME'] = all_ordinates['NAME'].astype(str) + ' - Z axis : ' + member_name_suffixes

    # Update PARENT_AXIS_ORDINATE_ID using ordinate_id_mappings
    if 'PARENT_AXIS_ORDINATE_ID' in all_ordinates.columns:
        parent_updates = []
        for i, member_id in enumerate(member_ids):
            mapping = ordinate_id_mappings[member_id]
            for j in range(n_ordinates):
                parent_id = str(table_ordinates.iloc[j]['PARENT_AXIS_ORDINATE_ID'])
                parent_updates.append(mapping.get(parent_id, parent_id))
        all_ordinates['PARENT_AXIS_ORDINATE_ID'] = parent_updates

    # Update PATH if present
    if 'PATH' in all_ordinates.columns:
        path_updates = []
        original_paths = table_ordinates['PATH'].values
        for i, member_id in enumerate(member_ids):
            mapping = ordinate_id_mappings[member_id]
            for j in range(n_ordinates):
                path = original_paths[j]
                if pd.isna(path):
                    path_updates.append(path)
                else:
                    path_parts = str(path).split('.')
                    new_path_parts = [mapping.get(part, part) for part in path_parts]
                    path_updates.append('.'.join(new_path_parts))
        all_ordinates['PATH'] = path_updates

    return all_ordinates, ordinate_id_mappings


def bulk_duplicate_cells_for_members(table_cells, table_id, members_df):
    """
    Duplicate cells for ALL members at once using vectorized operations.

    Args:
        table_cells: DataFrame of cells for a single table
        table_id: Original table ID
        members_df: DataFrame with MEMBER_ID and NAME columns

    Returns:
        tuple: (all_new_cells_df, cell_id_mappings)
    """
    if table_cells.empty or members_df.empty:
        return pd.DataFrame(), {}

    n_cells = len(table_cells)
    n_members = len(members_df)

    # Get member info
    member_ids = members_df['MEMBER_ID'].astype(str).str.replace(' ', '_').str.replace('.', '_').values
    member_names = members_df['NAME'].astype(str).values

    # Store original IDs
    original_cell_ids = table_cells['CELL_ID'].astype(str).values

    # Create all duplicates at once
    all_cells = pd.concat([table_cells] * n_members, ignore_index=True)

    # Create suffix arrays
    member_suffixes = np.repeat(member_ids, n_cells)
    member_name_suffixes = np.repeat(member_names, n_cells)

    # Vectorized updates
    all_cells['CELL_ID'] = all_cells['CELL_ID'].astype(str) + '_' + member_suffixes
    all_cells['TABLE_ID'] = str(table_id) + '_' + member_suffixes

    if 'CODE' in all_cells.columns:
        all_cells['CODE'] = all_cells['CODE'].astype(str) + '_' + member_suffixes

    if 'NAME' in all_cells.columns:
        all_cells['NAME'] = all_cells['NAME'].astype(str) + ' - Z axis : ' + member_name_suffixes

    # Build cell_id_mappings per member
    cell_id_mappings = {}
    for i, member_id in enumerate(member_ids):
        new_cell_ids = [f"{cid}_{member_id}" for cid in original_cell_ids]
        cell_id_mappings[member_id] = dict(zip(original_cell_ids, new_cell_ids))

    return all_cells, cell_id_mappings


def bulk_duplicate_cell_positions_for_members(table_positions, ordinate_id_mappings, cell_id_mappings, members_df):
    """
    Duplicate cell positions for ALL members at once using vectorized operations.

    NOTE: Both AXIS_ORDINATE_ID and CELL_ID are updated to reference duplicated ordinates and cells.
    ID is dropped to let SQLite auto-generate new unique IDs.

    Args:
        table_positions: DataFrame of cell positions for a single table
        ordinate_id_mappings: dict {member_id: {old_ordinate_id: new_ordinate_id}}
        cell_id_mappings: dict {member_id: {old_cell_id: new_cell_id}}
        members_df: DataFrame with MEMBER_ID column

    Returns:
        DataFrame with all duplicated cell positions (without ID column)
    """
    if table_positions.empty or members_df.empty:
        return pd.DataFrame()

    # Drop ID column first - SQLite will auto-generate new unique IDs
    if 'ID' in table_positions.columns:
        table_positions = table_positions.drop(columns=['ID'])

    n_positions = len(table_positions)
    n_members = len(members_df)

    # Get member info
    member_ids = members_df['MEMBER_ID'].astype(str).str.replace(' ', '_').str.replace('.', '_').values

    # Store original IDs
    original_ordinate_ids = table_positions['AXIS_ORDINATE_ID'].astype(str).values
    original_cell_ids = table_positions['CELL_ID'].astype(str).values

    # Create all duplicates at once
    all_positions = pd.concat([table_positions] * n_members, ignore_index=True)

    # Build new IDs using mappings
    new_ordinate_ids = []
    new_cell_ids = []
    for i, member_id in enumerate(member_ids):
        ordinate_mapping = ordinate_id_mappings[member_id]
        cell_mapping = cell_id_mappings[member_id]
        for j in range(n_positions):
            new_ordinate_ids.append(ordinate_mapping.get(original_ordinate_ids[j], original_ordinate_ids[j]))
            new_cell_ids.append(cell_mapping.get(original_cell_ids[j], original_cell_ids[j]))

    # Update both AXIS_ORDINATE_ID and CELL_ID to reference duplicated entities
    all_positions['AXIS_ORDINATE_ID'] = new_ordinate_ids
    all_positions['CELL_ID'] = new_cell_ids

    return all_positions


def bulk_duplicate_ordinate_cats_for_members(table_ordinate_cats, ordinate_id_mappings, members_df):
    """
    Duplicate ordinate categorisations for ALL members at once using vectorized operations.

    NOTE: Hierarchy fields are "exploded" - cleared to create direct variable-member pairs.
    Only AXIS_ORDINATE_ID, VARIABLE_ID, and MEMBER_ID are kept.
    MEMBER_HIERARCHY_ID, STARTING_MEMBER_ID, IS_STARTING_MEMBER_INCLUDED are cleared.

    Args:
        table_ordinate_cats: DataFrame of ordinate categorisations for a single table
        ordinate_id_mappings: dict {member_id: {old_ordinate_id: new_ordinate_id}}
        members_df: DataFrame with MEMBER_ID column

    Returns:
        DataFrame with all duplicated ordinate categorisations (hierarchy exploded)
    """
    if table_ordinate_cats.empty or members_df.empty:
        return pd.DataFrame()

    n_cats = len(table_ordinate_cats)
    n_members = len(members_df)

    # Get member info
    member_ids = members_df['MEMBER_ID'].astype(str).str.replace(' ', '_').str.replace('.', '_').values

    # Store original IDs
    original_ordinate_ids = table_ordinate_cats['AXIS_ORDINATE_ID'].astype(str).values

    # Create all duplicates at once
    all_cats = pd.concat([table_ordinate_cats] * n_members, ignore_index=True)

    # Build new IDs using mappings
    new_ordinate_ids = []
    for i, member_id in enumerate(member_ids):
        ordinate_mapping = ordinate_id_mappings[member_id]
        for j in range(n_cats):
            new_ordinate_ids.append(ordinate_mapping.get(original_ordinate_ids[j], original_ordinate_ids[j]))

    all_cats['AXIS_ORDINATE_ID'] = new_ordinate_ids

    # EXPLODE: Clear hierarchy fields to create direct variable-member pairs
    if 'MEMBER_HIERARCHY_ID' in all_cats.columns:
        all_cats['MEMBER_HIERARCHY_ID'] = None
    if 'STARTING_MEMBER_ID' in all_cats.columns:
        all_cats['STARTING_MEMBER_ID'] = None
    if 'IS_STARTING_MEMBER_INCLUDED' in all_cats.columns:
        all_cats['IS_STARTING_MEMBER_INCLUDED'] = False

    return all_cats


def duplicate_table_subgraph_bulk(table_cache_entry, members_df, table_id):
    """
    Duplicate an entire table subgraph for ALL members at once.

    This is the main entry point for bulk duplication - it replaces the
    per-member loop with a single set of vectorized operations.

    NOTE: Cells (TABLE_CELL) ARE duplicated along with all other entities.
    Cell positions reference the duplicated cells with updated CELL_ID and AXIS_ORDINATE_ID.

    Args:
        table_cache_entry: Dict with pre-cached table data
        members_df: DataFrame with MEMBER_ID and NAME columns
        table_id: Original table ID

    Returns:
        dict: {
            'tables': DataFrame,
            'axes': DataFrame,
            'ordinates': DataFrame,
            'cells': DataFrame,
            'cell_positions': DataFrame,
            'ordinate_cats': DataFrame
        }
    """
    if members_df.empty:
        return {
            'tables': pd.DataFrame(),
            'axes': pd.DataFrame(),
            'ordinates': pd.DataFrame(),
            'cells': pd.DataFrame(),
            'cell_positions': pd.DataFrame(),
            'ordinate_cats': pd.DataFrame()
        }

    table_row = table_cache_entry['table_row']
    table_axes = table_cache_entry['axes']
    table_ordinates = table_cache_entry['ordinates']
    table_cells = table_cache_entry['cells']
    table_positions = table_cache_entry['cell_positions']
    table_ordinate_cats = table_cache_entry['ordinate_cats']

    # Bulk duplicate tables
    new_tables = bulk_duplicate_table_for_members(table_row, members_df)

    # Bulk duplicate axes
    new_axes, axis_id_mappings = bulk_duplicate_axes_for_members(table_axes, table_id, members_df)

    # Bulk duplicate ordinates
    new_ordinates, ordinate_id_mappings = bulk_duplicate_ordinates_for_members(
        table_ordinates, axis_id_mappings, members_df
    )

    # Bulk duplicate cells
    new_cells, cell_id_mappings = bulk_duplicate_cells_for_members(
        table_cells, table_id, members_df
    )

    # Bulk duplicate cell positions (update both AXIS_ORDINATE_ID and CELL_ID)
    new_positions = bulk_duplicate_cell_positions_for_members(
        table_positions, ordinate_id_mappings, cell_id_mappings, members_df
    )

    # Bulk duplicate ordinate categorisations
    new_ordinate_cats = bulk_duplicate_ordinate_cats_for_members(
        table_ordinate_cats, ordinate_id_mappings, members_df
    )

    return {
        'tables': new_tables,
        'axes': new_axes,
        'ordinates': new_ordinates,
        'cells': new_cells,
        'cell_positions': new_positions,
        'ordinate_cats': new_ordinate_cats
    }
