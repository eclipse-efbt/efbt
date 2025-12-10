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

"""Get all members from the domain referenced by ordinates of a specific axis."""

import pandas as pd
import logging

logger = logging.getLogger(__name__)


def get_axis_domain_members(axis_id, ordinates_df, ordinate_cat_df, dimensions_df, members_df):
    """
    Get all members from the domain referenced by ordinates of a specific axis.
    Optimized with vectorized operations.

    Args:
        axis_id: ID of the axis
        ordinates_df: DataFrame containing axis ordinates
        ordinate_cat_df: DataFrame containing ordinate categorisation
        dimensions_df: DataFrame containing dimensions (variables)
        members_df: DataFrame containing members

    Returns:
        DataFrame with members (CODE, MEMBER_ID, NAME) or empty DataFrame
    """
    # Vectorized filtering for ordinates
    axis_ordinates = ordinates_df[ordinates_df['AXIS_ID'] == axis_id]

    if axis_ordinates.empty:
        logger.warning(f"No ordinates found for axis {axis_id}")
        return pd.DataFrame()

    # Get ordinate categorisations using vectorized isin
    ordinate_ids = axis_ordinates['AXIS_ORDINATE_ID'].values
    categorisations = ordinate_cat_df[ordinate_cat_df['AXIS_ORDINATE_ID'].isin(ordinate_ids)]

    if categorisations.empty:
        logger.warning(f"No categorisations found for axis {axis_id} ordinates")
        return pd.DataFrame()

    # Get unique variable IDs using numpy unique (faster than pandas)
    variable_ids = categorisations['VARIABLE_ID'].dropna().unique()

    if len(variable_ids) == 0:
        logger.warning(f"No variables found in categorisations for axis {axis_id}")
        return pd.DataFrame()

    # Vectorized filtering for dimensions
    axis_dimensions = dimensions_df[dimensions_df['VARIABLE_ID'].isin(variable_ids)]

    if axis_dimensions.empty:
        logger.warning(f"No dimensions found for variables")
        return pd.DataFrame()

    # Get domain IDs
    domain_ids = axis_dimensions['DOMAIN_ID'].dropna().unique()

    if len(domain_ids) == 0:
        logger.warning(f"No domains found for axis {axis_id}")
        return pd.DataFrame()

    # Vectorized filtering for members
    domain_members = members_df[members_df['DOMAIN_ID'].isin(domain_ids)]

    if domain_members.empty:
        logger.warning(f"No members found for domains")
        return pd.DataFrame()

    # Return deduplicated members
    result = domain_members[['MEMBER_ID', 'CODE', 'NAME']].drop_duplicates()

    logger.debug(f"Found {len(result)} members for axis {axis_id}")

    return result


def get_hierarchy_members(hierarchy_id, hierarchy_nodes_df, members_df):
    """
    Get members for duplication from a hierarchy (children of root, level > 0).

    Args:
        hierarchy_id: ID of the member hierarchy
        hierarchy_nodes_df: DataFrame containing hierarchy nodes
        members_df: DataFrame containing members

    Returns:
        DataFrame with members (MEMBER_ID, CODE, NAME) excluding the root _x0 member
    """
    if hierarchy_id is None or pd.isna(hierarchy_id) or str(hierarchy_id).strip() in ['', 'nan', 'None']:
        logger.warning(f"Invalid hierarchy_id: {hierarchy_id}")
        return pd.DataFrame()

    # Get hierarchy nodes for this hierarchy (level > 0 to exclude root)
    hierarchy_nodes = hierarchy_nodes_df[
        (hierarchy_nodes_df['MEMBER_HIERARCHY_ID'] == str(hierarchy_id)) &
        (hierarchy_nodes_df['LEVEL'].astype(int) > 0)
    ]

    if hierarchy_nodes.empty:
        logger.warning(f"No child nodes found for hierarchy {hierarchy_id}")
        return pd.DataFrame()

    # Get member IDs from hierarchy nodes
    member_ids = hierarchy_nodes['MEMBER_ID'].unique()

    # Get member details
    members = members_df[members_df['MEMBER_ID'].isin(member_ids)]

    if members.empty:
        logger.warning(f"No members found for hierarchy {hierarchy_id}")
        return pd.DataFrame()

    result = members[['MEMBER_ID', 'CODE', 'NAME']].drop_duplicates()

    logger.debug(f"Found {len(result)} members for hierarchy {hierarchy_id}")

    return result
