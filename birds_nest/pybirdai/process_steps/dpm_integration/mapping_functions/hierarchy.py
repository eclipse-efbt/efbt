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


def map_hierarchy(path=os.path.join("target", "Hierarchy.csv"), domain_id_map: dict = {}):
    """Map hierarchies from Hierarchy.csv to the target format"""
    df = pd.read_csv(path, dtype=str)

    # Transform column names to UPPER_SNAKE_CASE
    df.columns = [pascal_to_upper_snake(col) for col in df.columns]

    # Create new hierarchy ID
    df['NEW_HIERARCHY_ID'] = "EBA_" + df['HIERARCHY_CODE'].astype(str)

    # Map DOMAIN_ID
    df['DOMAIN_ID'] = df['DOMAIN_ID'].astype(str).map(domain_id_map).fillna(df['DOMAIN_ID'])

    # Generate ID mapping
    id_mapping = dict(zip(df['HIERARCHY_ID'].astype(str), df['NEW_HIERARCHY_ID'].astype(str)))

    # Rename columns
    df.drop(axis=1,labels='HIERARCHY_ID',inplace=True)
    df = df.rename(columns={
        "NEW_HIERARCHY_ID": "MEMBER_HIERARCHY_ID",
        "HIERARCHY_CODE": "CODE",
        "HIERARCHY_LABEL": "NAME",
        "HIERARCHY_DESCRIPTION": "DESCRIPTION"
    })

    # Add new fields
    df['MAINTENANCE_AGENCY_ID'] = "EBA"
    df['IS_MAIN_HIERARCHY'] = False

    df = df[[
        "MAINTENANCE_AGENCY_ID", "MEMBER_HIERARCHY_ID", "CODE", "DOMAIN_ID",
        "NAME", "DESCRIPTION", "IS_MAIN_HIERARCHY"
    ]]

    df = clean_spaces_df(df)

    return df, id_mapping
