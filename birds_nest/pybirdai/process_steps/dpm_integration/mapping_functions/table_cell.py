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
from pybirdai.process_steps.dpm_integration.mapping_functions.utils import pascal_to_upper_snake


def map_table_cell(path=os.path.join("target", "TableCell.csv"), table_map: dict = {}, dp_map: dict = {}):
    """Map table cells from TableCell.csv to the target format"""
    df = pd.read_csv(path, dtype=str)

    # Transform column names to UPPER_SNAKE_CASE
    df.columns = [pascal_to_upper_snake(col) for col in df.columns]

    df['MAINTENANCE_AGENCY_ID'] = "EBA"

    # Create new cell IDs (convert to float→int→string then prepend "EBA_")
    df['NEW_CELL_ID'] = "EBA_" + pd.to_numeric(df['CELL_ID'], errors='coerce').fillna(0).astype(int).astype(str)

    # Map table IDs
    df['TABLE_VID'] = df['TABLE_VID'].astype(str).map(table_map).fillna(df['TABLE_VID'])

    # Convert IS_SHADED to bool
    if 'IS_SHADED' in df.columns:
        df['IS_SHADED'] = df['IS_SHADED'].astype(str).str.lower().isin(['true', '1', 'yes'])
    else:
        df['IS_SHADED'] = False

    # Handle DATA_POINT_VID
    if not dp_map:
        df['DATA_POINT_VID'] = ""
    else:
        if 'DATA_POINT_VID' in df.columns:
            # Clean and map data point IDs
            df['DATA_POINT_VID'] = (
                df['DATA_POINT_VID']
                .astype(str)
                .str.replace('.0', '', regex=False)
                .str.replace('nan', '', regex=False)
                .map(dp_map)
                .fillna(df['DATA_POINT_VID'])
            )
        else:
            df['DATA_POINT_VID'] = ""

    # Create ID mapping
    id_mapping = dict(zip(df['CELL_ID'].astype(str), df['NEW_CELL_ID'].astype(str)))

    # Rename columns
    df.drop(axis=1,labels='CELL_ID',inplace=True)
    df = df.rename(columns={
        "NEW_CELL_ID": "CELL_ID",
        "TABLE_VID": "TABLE_ID",
        "DATA_POINT_VID": "TABLE_CELL_COMBINATION_ID"
    })

    # Add fields
    df['SYSTEM_DATA_CODE'] = ""
    df['NAME'] = df['CELL_ID']

    # Select final columns
    df = df[[
        "CELL_ID", "IS_SHADED", "TABLE_CELL_COMBINATION_ID", "SYSTEM_DATA_CODE", "NAME", "TABLE_ID"
    ]]

    return df, id_mapping
