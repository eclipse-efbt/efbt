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

"""Duplicate ordinates by replacing AXIS_ID prefix in IDs (Option A)."""

import pandas as pd


def duplicate_ordinates_with_member(ordinates_df, original_axis_ids, axis_id_mapping, member_id, member_name):
    """
    Duplicate ordinates by replacing the AXIS_ID prefix in IDs and CODE,
    and updating the NAME with human-readable member information.

    Option A: Replace AXIS_ID prefix in ORDINATE_ID to propagate the ID change.
    Example: AXIS_ID T_X → T_M_X means ORDINATE_ID T_X_O → T_M_X_O (not T_X_O_M)

    Args:
        ordinates_df: DataFrame of all ordinates
        original_axis_ids: List of original axis IDs
        axis_id_mapping: Dict mapping old axis IDs to new axis IDs
        member_id: The Z-axis member ID (e.g., "EBA_CU_USD")
        member_name: The Z-axis member name for display (e.g., "United States Dollar")

    Returns:
        tuple: (new_ordinates_df, ordinate_id_mapping)
    """
    # Filter ordinates for these axes
    table_ordinates = ordinates_df[ordinates_df['AXIS_ID'].isin(original_axis_ids)].copy()

    if table_ordinates.empty:
        return pd.DataFrame(), {}

    # Store original IDs for mapping
    original_ordinate_ids = table_ordinates['AXIS_ORDINATE_ID'].astype(str).tolist()
    original_ordinate_axis_ids = table_ordinates['AXIS_ID'].astype(str).tolist()

    # Update AXIS_ORDINATE_ID by replacing old AXIS_ID prefix with new AXIS_ID
    new_ordinate_ids = []
    for orig_ordinate_id, orig_axis_id in zip(original_ordinate_ids, original_ordinate_axis_ids):
        new_axis_id = axis_id_mapping.get(orig_axis_id, orig_axis_id)
        new_ordinate_id = orig_ordinate_id.replace(orig_axis_id, new_axis_id, 1)
        new_ordinate_ids.append(new_ordinate_id)
    table_ordinates['AXIS_ORDINATE_ID'] = new_ordinate_ids

    # Update AXIS_ID using mapping
    table_ordinates['AXIS_ID'] = table_ordinates['AXIS_ID'].astype(str).map(axis_id_mapping)

    # Update CODE by replacing old AXIS_ID prefix with new AXIS_ID
    if 'CODE' in table_ordinates.columns:
        original_codes = table_ordinates['CODE'].astype(str).tolist()
        new_codes = []
        for orig_code, orig_axis_id in zip(original_codes, original_ordinate_axis_ids):
            new_axis_id = axis_id_mapping.get(orig_axis_id, orig_axis_id)
            new_code = orig_code.replace(orig_axis_id, new_axis_id, 1)
            new_codes.append(new_code)
        table_ordinates['CODE'] = new_codes

    # Update NAME field
    if 'NAME' in table_ordinates.columns:
        table_ordinates['NAME'] = table_ordinates['NAME'].astype(str) + f" - Z axis : {member_name}"

    # Create ID mapping dictionary
    ordinate_id_mapping = dict(zip(original_ordinate_ids, new_ordinate_ids))

    # Update PARENT_AXIS_ORDINATE_ID using ordinate_id_mapping
    if 'PARENT_AXIS_ORDINATE_ID' in table_ordinates.columns:
        table_ordinates['PARENT_AXIS_ORDINATE_ID'] = (
            table_ordinates['PARENT_AXIS_ORDINATE_ID']
            .astype(str)
            .replace(ordinate_id_mapping)
        )

    # Update PATH using mapping
    if 'PATH' in table_ordinates.columns:
        def update_path(path):
            if pd.isna(path):
                return path
            path_parts = str(path).split('.')
            new_path_parts = [ordinate_id_mapping.get(part, part) for part in path_parts]
            return '.'.join(new_path_parts)

        table_ordinates['PATH'] = table_ordinates['PATH'].apply(update_path)

    return table_ordinates, ordinate_id_mapping
