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
import numpy as np
from pybirdai.process_steps.dpm_integration.mapping_functions.utils import (
    pascal_to_upper_snake, clean_spaces_df
)


def map_datapoint_version(path=os.path.join("target", "DataPointVersion.csv"), context_map: dict = {}, context_data=None, dimension_map: dict = {}, member_map: dict = {}):
    """Map datapoint versions from DataPointVersion.csv to the target format"""
    df = pd.read_csv(path, dtype=str)

    # Transform column names to UPPER_SNAKE_CASE
    df.columns = [pascal_to_upper_snake(col) for col in df.columns]

    df['MAINTENANCE_AGENCY_ID'] = "EBA"

    # Generate new data point IDs
    df['COMBINATION_ID'] = "EBA_" + pd.to_numeric(df['DATA_POINT_VID'], errors='coerce').fillna(0).astype(int).astype(str)

    # Create ID mapping
    id_mapping = dict(zip(df['DATA_POINT_VID'].astype(str), df['COMBINATION_ID'].astype(str)))

    # Merge with context data if provided
    if context_data is not None and len(context_data) > 0:
        # Convert context_data to DataFrame if it's a numpy array
        if isinstance(context_data, np.ndarray):
            context_df = pd.DataFrame(context_data)
        else:
            context_df = context_data

        dpv_subset = df[['COMBINATION_ID', 'CONTEXT_ID']].copy()
        dp_items_df = dpv_subset.merge(context_df, on='CONTEXT_ID', how='left')
        dp_items_df = dp_items_df.drop(columns=['CONTEXT_ID'])

        # Rename columns
        dp_items_df = dp_items_df.rename(columns={
            "DIMENSION_ID": "VARIABLE_ID"
        })

        dp_items_df['VARIABLE_SET'] = ""
        dp_items_df['SUBDOMAIN_ID'] = ""

        dp_items_df = dp_items_df[[
            "COMBINATION_ID", "VARIABLE_ID", "MEMBER_ID", "VARIABLE_SET", "SUBDOMAIN_ID"
        ]]

        dp_items = dp_items_df
    else:
        dp_items = np.array([])

    # Compute code function for NAME field
    def compute_code(string, member_map):
        if pd.isna(string) or string == '':
            return ''

        string = str(string)
        new_key = ""
        key_value = dict()
        is_new_key = True
        is_new_value = False
        previous_char = ""

        for char in string:
            if previous_char.isnumeric() and char.isalpha():
                new_key = ""
                is_new_key = True
                is_new_value = False

            if char.isnumeric() and previous_char.isalpha():
                is_new_value = True
                is_new_key = False

            if is_new_key:
                new_key += char

            if is_new_value:
                if "EBA_" + new_key not in key_value:
                    key_value["EBA_" + new_key] = ""
                key_value["EBA_" + new_key] += char

            previous_char = char

        return "|".join(f"{key}({member_map.get(int(float(value)), value)})" for key, value in key_value.items())

    # Rename columns
    df = df.rename(columns={
        "DATA_POINT_VID": "CODE",
        "FROM_DATE": "VALID_FROM",
        "TO_DATE": "VALID_TO",
        "CATEGORISATION_KEY": "NAME"
    })

    # Update NAME field with compute_code
    df['NAME'] = df['NAME'].apply(lambda x: compute_code(x, member_map))

    df['VERSION'] = ""

    # Select final columns
    df = df[[
        "COMBINATION_ID", "CODE", "NAME", "MAINTENANCE_AGENCY_ID", "VERSION", "VALID_FROM", "VALID_TO"
    ]]

    # Clean text fields
    df = clean_spaces_df(df)

    return (df, dp_items), id_mapping
