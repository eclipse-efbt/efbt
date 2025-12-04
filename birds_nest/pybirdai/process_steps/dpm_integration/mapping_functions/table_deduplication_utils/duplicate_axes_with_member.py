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

"""Duplicate axes by replacing TABLE_ID prefix in IDs (Option A)."""

import pandas as pd


def duplicate_axes_with_member(axes_df, table_id, new_table_id, member_id, member_name):
    """
    Duplicate axes for a table by replacing the TABLE_ID prefix in IDs and CODE.

    NAME field is kept unchanged - only Table level includes member info
    in its name for cleaner display.

    Option A: Replace TABLE_ID prefix in AXIS_ID to propagate the TABLE_ID change.
    Example: TABLE_ID T → T_M means AXIS_ID T_X → T_M_X (not T_X_M)

    Args:
        axes_df: DataFrame of all axes
        table_id: Original table ID
        new_table_id: New table ID with member ID appended
        member_id: The Z-axis member ID (e.g., "EBA_CU_USD")
        member_name: The Z-axis member name (kept for signature compatibility)

    Returns:
        tuple: (new_axes_df, axis_id_mapping)
            - new_axes_df: DataFrame of duplicated axes
            - axis_id_mapping: dict mapping old AXIS_ID -> new AXIS_ID
    """
    # Filter axes for this table
    table_axes = axes_df[axes_df['TABLE_ID'] == table_id].copy()

    if table_axes.empty:
        return pd.DataFrame(), {}

    # Store original IDs for mapping
    original_axis_ids = table_axes['AXIS_ID'].astype(str).tolist()
    original_table_id = str(table_id)

    # Update AXIS_ID by replacing TABLE_ID prefix with new TABLE_ID
    table_axes['AXIS_ID'] = table_axes['AXIS_ID'].astype(str).apply(
        lambda x: x.replace(original_table_id, new_table_id, 1)
    )

    # Update TABLE_ID
    table_axes['TABLE_ID'] = new_table_id

    # Update CODE by replacing TABLE_ID prefix
    if 'CODE' in table_axes.columns:
        table_axes['CODE'] = table_axes['CODE'].astype(str).apply(
            lambda x: x.replace(original_table_id, new_table_id, 1)
        )

    # Create ID mapping dictionary
    new_axis_ids = table_axes['AXIS_ID'].tolist()
    axis_id_mapping = dict(zip(original_axis_ids, new_axis_ids))

    return table_axes, axis_id_mapping
