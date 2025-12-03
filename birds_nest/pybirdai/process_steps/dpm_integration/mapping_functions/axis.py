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

import os
import pandas as pd
import logging
from pybirdai.process_steps.dpm_integration.mapping_functions.utils import (
    pascal_to_upper_snake, clean_spaces_df, apply_cascade_filter, convert_to_bool
)

logger = logging.getLogger(__name__)


def map_axis(path=os.path.join("target", "Axis.csv"), table_map: dict = {},
             save_z_axis_config: bool = False, output_directory: str = None):
    """
    Map axis from Axis.csv to the target format.

    Args:
        path: Path to Axis.csv
        table_map: Mapping of table VIDs to table IDs
        save_z_axis_config: If True, save Z-axis configuration to JSON file
        output_directory: Directory to save config (defaults to results/dpm_z_axis_configuration)

    Returns:
        Tuple of (df, id_mapping)
    """
    # Map numeric to alphabetic (for ORIENTATION field)
    orientation_map = {"1": "X", "2": "Y", "3": "Z", "0": "0", "X": "X", "Y": "Y", "Z": "Z"}
    # Map alphabetic to numeric (for ORDER field)
    order_map = {"X": "1", "Y": "2", "Z": "3", "0": "0"}

    df = pd.read_csv(path, dtype=str)

    # Transform column names to UPPER_SNAKE_CASE
    df.columns = [pascal_to_upper_snake(col) for col in df.columns]

    # Filter axes: only keep axes where TABLE_VID exists in table_map (cascade filter)
    df = apply_cascade_filter(df, 'TABLE_VID', table_map)

    # Map table IDs and create new axis IDs
    df['TABLE_ID'] = df['TABLE_VID'].astype(str).map(table_map).fillna(df['TABLE_VID']).astype(str)
    df['ORIENTATION'] = df['AXIS_ORIENTATION'].astype(str).map(orientation_map).fillna('')
    df['NEW_AXIS_ID'] = df['TABLE_ID'].astype(str) + "_" + df['ORIENTATION']

    # Create ID mapping
    id_mapping = dict(zip(df['AXIS_ID'].astype(str), df['NEW_AXIS_ID'].astype(str)))

    # Rename columns
    df.drop(axis=1,labels='AXIS_ID',inplace=True)
    df = df.rename(columns={
        "NEW_AXIS_ID": "AXIS_ID",
        "AXIS_LABEL": "NAME",
    })

    # Generate CODE from AXIS_ID
    def extract_code(axis_id):
        parts = str(axis_id).rsplit("_", 4)
        if len(parts) >= 4:
            return "_".join(parts[-4:-2] + [parts[-1]])
        return str(axis_id)

    df['CODE'] = df['AXIS_ID'].apply(extract_code)

    # Add ORDER field (convert alphabetic orientation to numeric order)
    df['ORDER'] = df['ORIENTATION'].astype(str).map(order_map).fillna('')

    # Add new fields
    df['MAINTENANCE_AGENCY_ID'] = "EBA"
    df['DESCRIPTION'] = ""

    # Convert IS_OPEN_AXIS to bool
    df = convert_to_bool(df, 'IS_OPEN_AXIS', default=False)

    df = df[[
        "AXIS_ID", "CODE", "ORIENTATION", "ORDER", "NAME", "DESCRIPTION", "TABLE_ID", "IS_OPEN_AXIS"
    ]]

    df = clean_spaces_df(df)

    # Save Z-axis configuration if requested
    if save_z_axis_config:
        _save_z_axis_configuration(df, output_directory)

    return df, id_mapping


def _save_z_axis_configuration(axes_df, output_directory=None):
    """
    Save Z-axis configuration to JSON file for reuse during table duplication.

    Args:
        axes_df: DataFrame of mapped axes
        output_directory: Directory to save config (defaults to results/dpm_z_axis_configuration)
    """
    import json
    from datetime import datetime

    # Default output directory
    if output_directory is None:
        output_directory = "results/dpm_z_axis_configuration"

    # Create directory if it doesn't exist
    os.makedirs(output_directory, exist_ok=True)

    # Identify Z-axes
    z_axes = axes_df[axes_df['ORIENTATION'] == 'Z'].copy()

    if z_axes.empty:
        logger.info("No Z-axes found. Skipping Z-axis config generation.")
        return

    # Build configuration
    z_axis_config = {
        "metadata": {
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "total_z_axis_tables": len(z_axes['TABLE_ID'].unique()),
        },
        "z_axis_tables": [
            {
                "table_id": str(row['TABLE_ID']),
                "z_axis_id": str(row['AXIS_ID'])
            }
            for _, row in z_axes.iterrows()
        ]
    }

    # Save to file
    config_path = os.path.join(output_directory, "z_axis_config.json")
    with open(config_path, 'w') as f:
        json.dump(z_axis_config, f, indent=2)

    logger.info(f"Saved Z-axis configuration to {config_path} "
                f"({len(z_axis_config['z_axis_tables'])} Z-axis tables)")

