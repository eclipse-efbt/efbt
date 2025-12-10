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


def map_domains(path=None, base_path="target"):
    """Map domains from Domain.csv to the target format

    Args:
        path: Path to Domain.csv (deprecated, use base_path instead)
        base_path: Base directory containing CSV files (default: "target")
    """
    if path is None:
        path = os.path.join(base_path, "Domain.csv")
    df = pd.read_csv(path, dtype=str)

    # Transform column names to UPPER_SNAKE_CASE
    df.columns = [pascal_to_upper_snake(col) for col in df.columns]

    # Create new domain ID
    df['NEW_DOMAIN_ID'] = "EBA_" + df['DOMAIN_CODE'].astype(str)

    # Create ID mapping
    id_mapping = dict(zip(df['DOMAIN_ID'].astype(str), df['NEW_DOMAIN_ID'].astype(str)))

    # Rename columns
    df.drop(axis=1, columns=['DOMAIN_ID'], inplace=True)
    df = df.rename(columns={
        "NEW_DOMAIN_ID": "DOMAIN_ID",
        "DOMAIN_CODE": "CODE",
        "DOMAIN_LABEL": "NAME",
        "DOMAIN_DESCRIPTION": "DESCRIPTION",
        "DATA_TYPE_ID": "DATA_TYPE",
    })

    # Add new fields
    df['MAINTENANCE_AGENCY_ID'] = "EBA"
    df['FACET_ID'] = False
    df['IS_REFERENCE'] = False
    df['IS_ENUMERATED'] = False

    df = clean_spaces_df(df)

    df = df[[
        "MAINTENANCE_AGENCY_ID", "DOMAIN_ID", "NAME", "IS_ENUMERATED",
        "DESCRIPTION", "DATA_TYPE", "CODE", "FACET_ID", "IS_REFERENCE"
    ]]

    return df, id_mapping
