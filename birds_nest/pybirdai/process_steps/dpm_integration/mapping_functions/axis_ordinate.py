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


def map_axis_ordinate(path=os.path.join("target", "AxisOrdinate.csv"), axis_map: dict = {}):
    """Map axis ordinates from AxisOrdinate.csv to the target format"""
    df = pd.read_csv(path, dtype=str)

    # Transform column names to UPPER_SNAKE_CASE
    df.columns = [pascal_to_upper_snake(col) for col in df.columns]

    df['MAINTENANCE_AGENCY_ID'] = "EBA"

    # Map axis IDs and create new ordinate IDs
    df['AXIS_ID'] = df['AXIS_ID'].astype(str).map(axis_map).fillna(df['AXIS_ID'])
    df['NEW_ORDINATE_ID'] = df['AXIS_ID'] + '_' + df['ORDINATE_CODE'].astype(str).str.strip()

    # Create ID mapping
    id_mapping = dict(zip(df['ORDINATE_ID'].astype(str), df['NEW_ORDINATE_ID'].astype(str)))

    # Rename columns
    df.drop(axis=1,labels='ORDINATE_ID',inplace=True)
    df = df.rename(columns={
        "NEW_ORDINATE_ID": "AXIS_ORDINATE_ID",
        "ORDINATE_CODE": "CODE",
        "PARENT_ORDINATE_ID": "PARENT_AXIS_ORDINATE_ID",
        "ORDINATE_LABEL": "NAME"
    })

    # Map parent IDs
    df['PARENT_AXIS_ORDINATE_ID'] = df['PARENT_AXIS_ORDINATE_ID'].astype(str).map(id_mapping).fillna(df['PARENT_AXIS_ORDINATE_ID'])

    df['DESCRIPTION'] = ""

    # Map hierarchical path components
    def map_path(path_str):
        if pd.isna(path_str) or path_str == '':
            return ''
        parts = str(path_str).split('.')
        return '.'.join([id_mapping.get(part, part) if part else part for part in parts])

    df['PATH'] = df['PATH'].apply(map_path)

    # Select final columns
    df = df[[
        "AXIS_ORDINATE_ID", "IS_ABSTRACT_HEADER", "CODE", "ORDER", "LEVEL",
        "PATH", "AXIS_ID", "PARENT_AXIS_ORDINATE_ID", "NAME", "DESCRIPTION"
    ]]

    # Clean text fields
    df = clean_spaces_df(df)

    return df, id_mapping
