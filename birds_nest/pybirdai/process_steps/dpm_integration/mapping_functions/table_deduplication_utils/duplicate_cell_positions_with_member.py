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

"""Duplicate cell positions using cell and ordinate ID mappings."""

import pandas as pd


def duplicate_cell_positions_with_member(positions_df, original_cell_ids, ordinate_id_mapping, cell_id_mapping=None):
    """
    Duplicate cell positions by updating ordinate and cell references.

    NOTE: Both AXIS_ORDINATE_ID and CELL_ID are updated to reference duplicated entities.
    ID is dropped to let SQLite auto-generate new unique IDs.

    Args:
        positions_df: DataFrame of all cell positions
        original_cell_ids: List of original cell IDs
        ordinate_id_mapping: Dict mapping old ordinate IDs to new ordinate IDs
        cell_id_mapping: Optional dict mapping old cell IDs to new cell IDs (if cells are duplicated)

    Returns:
        DataFrame of duplicated cell positions (without ID column)
    """
    # Filter positions for these cells
    table_positions = positions_df[positions_df['CELL_ID'].isin(original_cell_ids)].copy()

    if table_positions.empty:
        return pd.DataFrame()

    # Drop ID column - SQLite will auto-generate new unique IDs
    if 'ID' in table_positions.columns:
        table_positions = table_positions.drop(columns=['ID'])

    # Update CELL_ID if mapping provided (cells are duplicated)
    if cell_id_mapping:
        table_positions['CELL_ID'] = (
            table_positions['CELL_ID']
            .astype(str)
            .replace(cell_id_mapping)
        )

    # Update AXIS_ORDINATE_ID using vectorized replace (faster than lambda)
    table_positions['AXIS_ORDINATE_ID'] = (
        table_positions['AXIS_ORDINATE_ID']
        .astype(str)
        .replace(ordinate_id_mapping)
    )

    return table_positions
