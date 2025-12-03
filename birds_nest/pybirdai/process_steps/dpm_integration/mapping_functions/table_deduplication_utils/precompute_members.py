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

"""Pre-compute mapping of axis_id to members for all Z-axes."""

import logging
import pandas as pd
from .get_axis_members import get_axis_domain_members, get_hierarchy_members

logger = logging.getLogger(__name__)


def precompute_axis_members_mapping(z_axis_tables, ordinates_df, ordinate_cat_df,
                                   dimensions_df, members_df):
    """
    Pre-compute mapping of axis_id to members for all Z-axes.
    This optimization avoids redundant calls to get_axis_domain_members for shared axes.

    Args:
        z_axis_tables: DataFrame with tables that have Z-axis (including Z_AXIS_ID column)
        ordinates_df: DataFrame containing axis ordinates
        ordinate_cat_df: DataFrame containing ordinate categorisation
        dimensions_df: DataFrame containing dimensions (variables)
        members_df: DataFrame containing members

    Returns:
        Dictionary mapping {axis_id: members_dataframe}
    """
    axis_members_map = {}
    unique_z_axes = z_axis_tables['Z_AXIS_ID'].unique()

    logger.info(f"Pre-computing members for {len(unique_z_axes)} unique Z-axes")

    for z_axis_id in unique_z_axes:
        members = get_axis_domain_members(
            z_axis_id, ordinates_df, ordinate_cat_df, dimensions_df, members_df
        )
        if not members.empty:
            axis_members_map[z_axis_id] = members
            logger.debug(f"Cached {len(members)} members for axis {z_axis_id}")
        else:
            logger.warning(f"No members found for axis {z_axis_id}")

    logger.info(f"Pre-computed mapping complete: {len(axis_members_map)} axes with members")
    return axis_members_map


def precompute_duplication_info_from_ordinate_items(
    z_axis_tables, ordinates_df, ordinate_items_df, hierarchy_nodes_df, members_df
):
    """
    Pre-compute duplication info for Z-axis tables using ordinate items and hierarchies.

    This is more efficient than getting all domain members because it uses the
    MEMBER_HIERARCHY_ID from ordinate items to get only the relevant members.

    Args:
        z_axis_tables: DataFrame with tables that have Z-axis (including Z_AXIS_ID column)
        ordinates_df: DataFrame containing axis ordinates
        ordinate_items_df: DataFrame containing ordinate items (with MEMBER_HIERARCHY_ID)
        hierarchy_nodes_df: DataFrame containing hierarchy nodes
        members_df: DataFrame containing members

    Returns:
        Dictionary mapping {z_axis_id: members_dataframe}
    """
    axis_members_map = {}
    unique_z_axes = z_axis_tables['Z_AXIS_ID'].unique()

    logger.info(f"Pre-computing members for {len(unique_z_axes)} unique Z-axes using ordinate items")

    # Cache hierarchy members to avoid redundant lookups
    hierarchy_members_cache = {}

    for z_axis_id in unique_z_axes:
        # Get ordinates for this Z-axis
        axis_ordinates = ordinates_df[ordinates_df['AXIS_ID'] == z_axis_id]

        if axis_ordinates.empty:
            logger.warning(f"No ordinates found for Z-axis {z_axis_id}")
            continue

        # Get ordinate IDs
        ordinate_ids = axis_ordinates['AXIS_ORDINATE_ID'].values

        # Get ordinate items for these ordinates
        axis_ordinate_items = ordinate_items_df[
            ordinate_items_df['AXIS_ORDINATE_ID'].isin(ordinate_ids)
        ]

        if axis_ordinate_items.empty:
            logger.warning(f"No ordinate items found for Z-axis {z_axis_id}")
            continue

        # Get unique hierarchy IDs from ordinate items
        hierarchy_ids = axis_ordinate_items['MEMBER_HIERARCHY_ID'].dropna().unique()
        hierarchy_ids = [h for h in hierarchy_ids if str(h).strip() not in ['', 'nan', 'None']]

        if not hierarchy_ids:
            logger.warning(f"No valid MEMBER_HIERARCHY_ID found for Z-axis {z_axis_id}")
            continue

        # Get members from all hierarchies (usually just one per axis)
        all_members = []
        for hierarchy_id in hierarchy_ids:
            hierarchy_id_str = str(hierarchy_id)

            # Use cache if available
            if hierarchy_id_str not in hierarchy_members_cache:
                hierarchy_members_cache[hierarchy_id_str] = get_hierarchy_members(
                    hierarchy_id_str, hierarchy_nodes_df, members_df
                )

            members = hierarchy_members_cache[hierarchy_id_str]
            if not members.empty:
                all_members.append(members)

        if all_members:
            combined_members = pd.concat(all_members, ignore_index=True).drop_duplicates()
            axis_members_map[z_axis_id] = combined_members
            logger.debug(f"Cached {len(combined_members)} members for Z-axis {z_axis_id} from hierarchies")
        else:
            logger.warning(f"No members found for Z-axis {z_axis_id}")

    logger.info(f"Pre-computed mapping complete: {len(axis_members_map)} Z-axes with members")
    return axis_members_map
