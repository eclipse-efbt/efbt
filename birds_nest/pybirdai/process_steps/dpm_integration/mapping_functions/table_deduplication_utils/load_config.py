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

"""Load Z-axis configuration from JSON file."""

import os
import json
import pandas as pd
import logging

logger = logging.getLogger(__name__)


def load_z_axis_tables_from_config(tables_df, config_path=None):
    """
    Load Z-axis table information from configuration file.

    Args:
        tables_df: DataFrame of all tables
        config_path: Path to z_axis_config.json (defaults to results/dpm_z_axis_configuration/z_axis_config.json)

    Returns:
        DataFrame with Z-axis tables (TABLE_ID, Z_AXIS_ID columns + all table columns), or None if config not found
    """
    # Default config path
    if config_path is None:
        config_path = "results/dpm_z_axis_configuration/z_axis_config.json"

    if not os.path.exists(config_path):
        logger.debug(f"Z-axis config not found at {config_path}. Will use axis scanning fallback.")
        return None

    try:
        with open(config_path, 'r') as f:
            config = json.load(f)

        # Extract table IDs and Z-axis IDs
        z_axis_data = [
            {
                'TABLE_ID': item['table_id'],
                'Z_AXIS_ID': item['z_axis_id']
            }
            for item in config['z_axis_tables']
        ]

        if not z_axis_data:
            logger.warning("Z-axis config file is empty.")
            return pd.DataFrame(columns=['TABLE_ID', 'Z_AXIS_ID'])

        # Create DataFrame
        z_axis_df = pd.DataFrame(z_axis_data)

        # Merge with full table information
        result = tables_df.merge(z_axis_df, on='TABLE_ID', how='inner')

        logger.info(f"Loaded {len(result)} Z-axis tables from config file: {config_path}")
        return result

    except Exception as e:
        logger.warning(f"Failed to load Z-axis config: {e}. Will use axis scanning fallback.")
        return None
