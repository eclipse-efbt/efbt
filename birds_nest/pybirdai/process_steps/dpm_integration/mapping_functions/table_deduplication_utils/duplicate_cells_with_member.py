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

"""Duplicate cells by appending Z-axis member to IDs."""

import pandas as pd


def duplicate_cells_with_member(cells_df, table_id, new_table_id, member_id, member_name):
    """
    Duplicate cells by appending the Z-axis member ID to IDs and CODE,
    and updating the NAME with human-readable member information.

    Args:
        cells_df: DataFrame of all cells
        table_id: Original table ID
        new_table_id: New table ID with member ID appended
        member_id: The Z-axis member ID (e.g., "EBA_CU_USD")
        member_name: The Z-axis member name for display (e.g., "United States Dollar")

    Returns:
        tuple: (new_cells_df, cell_id_mapping)
    """
    # Filter cells for this table
    table_cells = cells_df[cells_df['TABLE_ID'] == table_id].copy()

    if table_cells.empty:
        return pd.DataFrame(), {}

    # Store original IDs for mapping (vectorized)
    original_cell_ids = table_cells['CELL_ID'].astype(str).tolist()

    # Update CELL_ID (vectorized)
    table_cells['CELL_ID'] = table_cells['CELL_ID'].astype(str) + f"_{member_id}"

    # Update TABLE_ID (vectorized)
    table_cells['TABLE_ID'] = new_table_id

    # Update CODE field (vectorized)
    if 'CODE' in table_cells.columns:
        table_cells['CODE'] = table_cells['CODE'].astype(str) + f"_{member_id}"

    # Update NAME field (vectorized)
    if 'NAME' in table_cells.columns:
        table_cells['NAME'] = table_cells['NAME'].astype(str) + f" - Z axis : {member_name}"

    # Create ID mapping dictionary
    new_cell_ids = table_cells['CELL_ID'].tolist()
    cell_id_mapping = dict(zip(original_cell_ids, new_cell_ids))

    return table_cells, cell_id_mapping
