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

"""Extract metadata required for a table's subgraph."""


def extract_required_metadata(table_id, axis_ids, ordinates_df, ordinate_cat_df):
    """
    Extract metadata (variables, members, hierarchies) required for a table.

    Args:
        table_id: The table ID
        axis_ids: List of axis IDs for this table
        ordinates_df: DataFrame of axis ordinates
        ordinate_cat_df: DataFrame of ordinate categorisations

    Returns:
        dict with keys: 'variables', 'members', 'hierarchies'
    """
    # Get ordinates for this table's axes
    table_ordinates = ordinates_df[ordinates_df['AXIS_ID'].isin(axis_ids)]
    ordinate_ids = table_ordinates['AXIS_ORDINATE_ID'].unique()

    # Get categorisations for these ordinates
    table_cats = ordinate_cat_df[ordinate_cat_df['AXIS_ORDINATE_ID'].isin(ordinate_ids)]

    metadata = {
        'variables': table_cats['VARIABLE_ID'].dropna().unique().tolist() if 'VARIABLE_ID' in table_cats.columns else [],
        'members': table_cats['MEMBER_ID'].dropna().unique().tolist() if 'MEMBER_ID' in table_cats.columns else [],
        'hierarchies': table_cats['MEMBER_HIERARCHY_ID'].dropna().unique().tolist() if 'MEMBER_HIERARCHY_ID' in table_cats.columns else []
    }

    return metadata
