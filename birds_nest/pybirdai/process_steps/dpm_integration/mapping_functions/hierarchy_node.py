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
    pascal_to_upper_snake, normalize_id_map
)


def map_hierarchy_node(path=os.path.join("target", "HierarchyNode.csv"), hierarchy_map: dict = {}, member_map: dict = {}):
    """Map hierarchy nodes from HierarchyNode.csv to the target format"""
    df = pd.read_csv(path, dtype=str)

    # Handle NaN/empty values
    df['ParentMemberID'] = df['ParentMemberID'].fillna("0").replace('', "0")
    df['MemberID'] = df['MemberID'].fillna("0").replace('', "0")

    # Transform column names to UPPER_SNAKE_CASE
    df.columns = [pascal_to_upper_snake(col) for col in df.columns]

    # Normalize maps for .0 variants
    member_map_norm = normalize_id_map(member_map)
    hierarchy_map_norm = normalize_id_map(hierarchy_map)

    # Convert to int then string for mapping (handles floats like "123.0")
    df['MEMBER_ID'] = pd.to_numeric(df['MEMBER_ID'], errors='coerce').fillna(0).astype(int).astype(str)
    df['PARENT_MEMBER_ID'] = pd.to_numeric(df['PARENT_MEMBER_ID'], errors='coerce').fillna(0).astype(int).astype(str)

    # Vectorized ID mapping
    df['MEMBER_ID'] = df['MEMBER_ID'].map(member_map_norm).fillna(df['MEMBER_ID'])
    df['PARENT_MEMBER_ID'] = df['PARENT_MEMBER_ID'].map(member_map_norm).fillna(df['PARENT_MEMBER_ID'])
    df['HIERARCHY_ID'] = df['HIERARCHY_ID'].astype(str).map(hierarchy_map_norm).fillna(df['HIERARCHY_ID'])
    df['PARENT_HIERARCHY_ID'] = df['PARENT_HIERARCHY_ID'].astype(str).map(hierarchy_map_norm).fillna(df['PARENT_HIERARCHY_ID'])

    # Rename fields
    df = df.rename(columns={
        "HIERARCHY_ID": "MEMBER_HIERARCHY_ID",
        "COMPARISON_OPERATOR": "COMPARATOR",
        "UNARY_OPERATOR": "OPERATOR"
    })

    # Add new fields
    df['MAINTENANCE_AGENCY_ID'] = "EBA"
    df['VALID_FROM'] = "1900-01-01"
    df['VALID_TO'] = "9999-12-31"

    # Clean COMPARATOR and OPERATOR
    df['COMPARATOR'] = df['COMPARATOR'].astype(str).str.strip()
    df['OPERATOR'] = df['OPERATOR'].astype(str).str.strip()

    # Set default COMPARATOR where both are empty
    empty_mask = (
        (df['COMPARATOR'].isin(['', 'nan'])) &
        (df['OPERATOR'].isin(['', 'nan']))
    )
    df.loc[empty_mask, 'COMPARATOR'] = ">="

    # Select final columns
    df = df[[
        "MEMBER_HIERARCHY_ID", "MEMBER_ID", "LEVEL", "PARENT_MEMBER_ID",
        "COMPARATOR", "OPERATOR", "VALID_FROM", "VALID_TO"
    ]]

    return df, {}
