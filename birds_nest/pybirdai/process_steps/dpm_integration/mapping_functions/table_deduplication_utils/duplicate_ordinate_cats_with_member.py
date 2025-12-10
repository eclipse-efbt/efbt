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

"""Duplicate ordinate categorisations using ordinate ID mapping."""

import pandas as pd


def duplicate_ordinate_cats_with_member(cats_df, original_ordinate_ids, ordinate_id_mapping):
    """
    Duplicate ordinate categorisations using ordinate ID mapping.

    NOTE: Hierarchy fields are "exploded" - cleared to create direct variable-member pairs.
    Only AXIS_ORDINATE_ID, VARIABLE_ID, and MEMBER_ID are kept.
    MEMBER_HIERARCHY_ID, STARTING_MEMBER_ID, IS_STARTING_MEMBER_INCLUDED are cleared.

    Args:
        cats_df: DataFrame of all ordinate categorisations
        original_ordinate_ids: List of original ordinate IDs
        ordinate_id_mapping: Dict mapping old ordinate IDs to new ordinate IDs

    Returns:
        DataFrame of duplicated ordinate categorisations (hierarchy exploded)
    """
    # Filter categorisations for these ordinates
    table_cats = cats_df[cats_df['AXIS_ORDINATE_ID'].isin(original_ordinate_ids)].copy()

    if table_cats.empty:
        return pd.DataFrame()

    # Update AXIS_ORDINATE_ID using vectorized replace (much faster than iterrows)
    table_cats['AXIS_ORDINATE_ID'] = (
        table_cats['AXIS_ORDINATE_ID']
        .astype(str)
        .replace(ordinate_id_mapping)
    )

    # EXPLODE: Clear hierarchy fields to create direct variable-member pairs
    if 'MEMBER_HIERARCHY_ID' in table_cats.columns:
        table_cats['MEMBER_HIERARCHY_ID'] = None
    if 'STARTING_MEMBER_ID' in table_cats.columns:
        table_cats['STARTING_MEMBER_ID'] = None
    if 'IS_STARTING_MEMBER_INCLUDED' in table_cats.columns:
        table_cats['IS_STARTING_MEMBER_INCLUDED'] = False

    return table_cats
