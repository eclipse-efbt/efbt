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
    pascal_to_upper_snake, clean_spaces_df, normalize_id_map
)


def map_members(path=os.path.join("target", "Member.csv"), domain_id_map: dict = {}):
    """Map members from Member.csv to the target format"""
    df = pd.read_csv(path, dtype=str)

    # Transform column names to UPPER_SNAKE_CASE
    df.columns = [pascal_to_upper_snake(col) for col in df.columns]

    # Normalize domain ID map for .0 variants
    domain_map_norm = normalize_id_map(domain_id_map)

    # Map DOMAIN_ID
    df['DOMAIN_ID'] = df['DOMAIN_ID'].str.strip()
    df['MAPPED_DOMAIN_ID'] = df['DOMAIN_ID'].map(domain_map_norm).fillna(df['DOMAIN_ID'])

    # Create NEW_MEMBER_ID
    df['NEW_MEMBER_ID'] = df['MAPPED_DOMAIN_ID'] + "_EBA_" + df['MEMBER_CODE']

    # Create ID mapping
    id_mapping = dict(zip(df['MEMBER_ID'].astype(str), df['NEW_MEMBER_ID'].astype(str)))
    df.drop(axis=1, columns=['MEMBER_ID'], inplace=True)
    # Rename and select columns
    df = df.rename(columns={
        "NEW_MEMBER_ID": "MEMBER_ID",
        "MAPPED_DOMAIN_ID": "DOMAIN_ID",
        "MEMBER_CODE": "CODE",
        "MEMBER_LABEL": "NAME",
        "MEMBER_DESCRIPTION": "DESCRIPTION",
    })

    df['MAINTENANCE_AGENCY_ID'] = "EBA"

    df = df[["MAINTENANCE_AGENCY_ID", "MEMBER_ID", "CODE", "NAME", "DOMAIN_ID", "DESCRIPTION"]]
    df = clean_spaces_df(df)

    return df, id_mapping
