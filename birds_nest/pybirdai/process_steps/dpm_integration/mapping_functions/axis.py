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
from pybirdai.process_steps.dpm_integration.mapping_functions.utils import (
    pascal_to_upper_snake, clean_spaces_df
)


def map_axis(path=os.path.join("target", "Axis.csv"), table_map: dict = {}):
    """Map axis from Axis.csv to the target format"""
    orientation_id_map = {"X": "1", "Y": "2", "Z": "3", "0": "0"}
    df = pd.read_csv(path, dtype=str)

    # Transform column names to UPPER_SNAKE_CASE
    df.columns = [pascal_to_upper_snake(col) for col in df.columns]

    # Map table IDs and create new axis IDs
    df['TABLE_ID'] = df['TABLE_VID'].astype(str).map(table_map).fillna(df['TABLE_VID'])
    df['ORIENTATION'] = df['AXIS_ORIENTATION'].astype(str).map(orientation_id_map).fillna('')
    df['NEW_AXIS_ID'] = df['TABLE_ID'] + "_" + df['ORIENTATION']

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

    # Add ORDER field
    df['ORDER'] = df['ORIENTATION'].astype(str).map(orientation_id_map).fillna('')

    # Add new fields
    df['MAINTENANCE_AGENCY_ID'] = "EBA"
    df['DESCRIPTION'] = ""

    # Convert IS_OPEN_AXIS to bool
    if 'IS_OPEN_AXIS' in df.columns:
        df['IS_OPEN_AXIS'] = (
            df['IS_OPEN_AXIS']
            .astype(str)
            .str.lower()
            .isin(['true', '1', 'yes'])
        )
    else:
        df['IS_OPEN_AXIS'] = False

    df = df[[
        "AXIS_ID", "CODE", "ORIENTATION", "ORDER", "NAME", "DESCRIPTION", "TABLE_ID", "IS_OPEN_AXIS"
    ]]

    df = clean_spaces_df(df)

    return df, id_mapping
