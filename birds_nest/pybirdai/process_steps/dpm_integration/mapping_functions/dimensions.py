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


def map_dimensions(path=None, domain_id_map: dict = {}, base_path="target"):
    """Map dimensions from Dimension.csv to the target format

    Args:
        path: Path to Dimension.csv (deprecated, use base_path instead)
        domain_id_map: Dictionary mapping domain IDs
        base_path: Base directory containing CSV files (default: "target")
    """
    if path is None:
        path = os.path.join(base_path, "Dimension.csv")
    df = pd.read_csv(path, dtype=str)

    # Transform column names to UPPER_SNAKE_CASE
    df.columns = [pascal_to_upper_snake(col) for col in df.columns]

    # Create new IDs and map domain IDs
    df['VARIABLE_ID'] = "EBA_" + df['DIMENSION_CODE'].astype(str)
    df['DOMAIN_ID'] = df['DOMAIN_ID'].astype(str).map(domain_id_map).fillna(df['DOMAIN_ID'])

    # Create ID mapping
    id_mapping = dict(zip(df['DIMENSION_ID'].astype(str), df['VARIABLE_ID'].astype(str)))

    # Rename columns
    df = df.rename(columns={
        "DIMENSION_CODE": "CODE",
        "DIMENSION_LABEL": "NAME",
        "DIMENSION_DESCRIPTION": "DESCRIPTION"
    })

    # Remove _SOS_Duplicate_.* patterns from NAME and DESCRIPTION
    df['NAME'] = df['NAME'].str.replace(r'_SOS_Duplicate_.*', '', regex=True)
    df['DESCRIPTION'] = df['DESCRIPTION'].str.replace(r'_SOS_Duplicate_.*', '', regex=True)

    # Add new fields
    df['MAINTENANCE_AGENCY_ID'] = "EBA"
    df['PRIMARY_CONCEPT'] = ""
    df['IS_DECOMPOSED'] = False

    # Select final columns
    df = df[[
        "MAINTENANCE_AGENCY_ID", "VARIABLE_ID", "CODE", "NAME", "DOMAIN_ID",
        "DESCRIPTION", "PRIMARY_CONCEPT"
    ]]

    df = clean_spaces_df(df)

    return df, id_mapping
