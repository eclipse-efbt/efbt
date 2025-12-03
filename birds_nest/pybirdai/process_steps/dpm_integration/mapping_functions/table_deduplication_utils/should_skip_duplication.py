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

"""Determine if table duplication should be skipped based on Z-axis domain."""

import logging

logger = logging.getLogger(__name__)

# Domains that should skip duplication due to too many members
SKIP_DOMAINS = {'EBA_GA', 'EBA_CU'}  # Geographical Area, Currency


def should_skip_duplication(z_axis_id, ordinates_df, ordinate_cat_df, dimensions_df):
    """
    Determine if table duplication should be skipped based on Z-axis domain.

    Skip duplication for large domains (too many members):
    - EBA_GA (Geographical Area) - hundreds of countries/regions
    - EBA_CU (Currency) - hundreds of currencies

    Args:
        z_axis_id: ID of the Z-axis
        ordinates_df: DataFrame containing axis ordinates
        ordinate_cat_df: DataFrame containing ordinate categorisation
        dimensions_df: DataFrame containing dimensions (variables)

    Returns:
        tuple: (should_skip: bool, reason: str or None)
               - should_skip: True if duplication should be skipped, False otherwise
               - reason: Domain ID if skipped, None otherwise
    """
    try:
        # Get ordinates for the Z-axis
        axis_ordinates = ordinates_df[ordinates_df['AXIS_ID'] == z_axis_id]

        if axis_ordinates.empty:
            logger.debug(f"No ordinates found for Z-axis {z_axis_id}")
            return (False, None)

        # Get ordinate categorisations
        ordinate_ids = axis_ordinates['AXIS_ORDINATE_ID'].values
        categorisations = ordinate_cat_df[ordinate_cat_df['AXIS_ORDINATE_ID'].isin(ordinate_ids)]

        if categorisations.empty:
            logger.debug(f"No categorisations found for Z-axis {z_axis_id}")
            return (False, None)

        # Get unique variable IDs
        variable_ids = categorisations['VARIABLE_ID'].dropna().unique()

        if len(variable_ids) == 0:
            logger.debug(f"No variables found for Z-axis {z_axis_id}")
            return (False, None)

        # Get dimensions and their domain IDs
        axis_dimensions = dimensions_df[dimensions_df['VARIABLE_ID'].isin(variable_ids)]

        if axis_dimensions.empty:
            logger.debug(f"No dimensions found for Z-axis {z_axis_id}")
            return (False, None)

        # Get domain IDs
        domain_ids = axis_dimensions['DOMAIN_ID'].dropna().unique()

        # Check if any domain is in the skip list
        for domain_id in domain_ids:
            if str(domain_id) in SKIP_DOMAINS:
                logger.info(f"Z-axis {z_axis_id} has domain {domain_id} - skipping duplication")
                return (True, str(domain_id))

        return (False, None)

    except Exception as e:
        logger.warning(f"Error checking if duplication should be skipped for Z-axis {z_axis_id}: {e}")
        # On error, default to not skipping
        return (False, None)
