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

"""Identify tables that have a Z-axis."""

import logging

logger = logging.getLogger(__name__)


def identify_z_axis_tables(tables_df, axes_df):
    """
    Identify tables that have a Z-axis.

    Args:
        tables_df: DataFrame of tables
        axes_df: DataFrame of axes

    Returns:
        DataFrame of Z-axis tables with columns: TABLE_ID, Z_AXIS_ID
    """
    # Find all Z-axes
    z_axes = axes_df[axes_df['ORIENTATION'] == 'Z'].copy()

    if z_axes.empty:
        import pandas as pd
        return pd.DataFrame(columns=['TABLE_ID', 'Z_AXIS_ID'])

    # Get unique table IDs that have Z-axes
    z_axis_tables = z_axes[['TABLE_ID', 'AXIS_ID']].rename(columns={'AXIS_ID': 'Z_AXIS_ID'})

    # Merge with tables to get full table information
    result = tables_df.merge(z_axis_tables, on='TABLE_ID', how='inner')

    logger.info(f"Identified {len(result)} tables with Z-axis")
    return result
