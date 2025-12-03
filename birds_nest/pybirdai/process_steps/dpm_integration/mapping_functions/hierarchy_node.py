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

    # Fix AT→ATY domain mismatch for metric members
    def fix_aty_domain_mismatch(member_id):
        """Fix AT→ATY domain mismatch for metric members (mi*, si*, ei* codes)"""
        if pd.isna(member_id) or member_id == '':
            return member_id

        # Replace EBA_AT_EBA_ with EBA_ATY_EBA_ for metric codes
        if 'EBA_AT_EBA_' in str(member_id):
            parts = str(member_id).split('_EBA_')
            if len(parts) == 2:
                member_code = parts[1]
                # Check if it's a metric code (mi*, si*, ei*)
                if member_code.startswith(('mi', 'si', 'ei')):
                    return str(member_id).replace('EBA_AT_EBA_', 'EBA_ATY_EBA_')
        return member_id

    # Apply fix to member ID columns
    df['MEMBER_ID'] = df['MEMBER_ID'].apply(fix_aty_domain_mismatch)
    df['PARENT_MEMBER_ID'] = df['PARENT_MEMBER_ID'].apply(fix_aty_domain_mismatch)

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


def create_default_hierarchy_nodes(hierarchy_id: str, domain_id: str, members_df: pd.DataFrame) -> pd.DataFrame:
    """
    Create hierarchy nodes for a default hierarchy.
    The _x0 member is the root (level 0), and all other domain members are children (level 1).

    Args:
        hierarchy_id: The hierarchy ID (e.g., "EBA_CU_DEFAULT")
        domain_id: The domain ID (e.g., "EBA_CU")
        members_df: DataFrame of all members

    Returns:
        DataFrame of hierarchy nodes
    """
    # Get all members for this domain
    domain_members = members_df[members_df['DOMAIN_ID'] == domain_id]['MEMBER_ID'].tolist()

    x0_member_id = f"{domain_id}_x0"
    nodes = []

    # Create root node (_x0 at level 0)
    nodes.append({
        "MEMBER_HIERARCHY_ID": hierarchy_id,
        "MEMBER_ID": x0_member_id,
        "LEVEL": 0,
        "PARENT_MEMBER_ID": "",
        "COMPARATOR": ">=",
        "OPERATOR": "",
        "VALID_FROM": "1900-01-01",
        "VALID_TO": "9999-12-31"
    })

    # Create child nodes (all other members at level 1, parented to _x0)
    for member_id in domain_members:
        if member_id != x0_member_id:
            nodes.append({
                "MEMBER_HIERARCHY_ID": hierarchy_id,
                "MEMBER_ID": member_id,
                "LEVEL": 1,
                "PARENT_MEMBER_ID": x0_member_id,
                "COMPARATOR": ">=",
                "OPERATOR": "",
                "VALID_FROM": "1900-01-01",
                "VALID_TO": "9999-12-31"
            })

    return pd.DataFrame(nodes)


def create_all_default_hierarchy_nodes(domain_to_hierarchy_map: dict, members_df: pd.DataFrame) -> pd.DataFrame:
    """
    Create hierarchy nodes for all default hierarchies.

    Args:
        domain_to_hierarchy_map: Dict mapping domain_id to hierarchy_id
        members_df: DataFrame of all members

    Returns:
        DataFrame of all hierarchy nodes for default hierarchies
    """
    all_nodes = []

    for domain_id, hierarchy_id in domain_to_hierarchy_map.items():
        nodes_df = create_default_hierarchy_nodes(hierarchy_id, domain_id, members_df)
        all_nodes.append(nodes_df)

    if all_nodes:
        return pd.concat(all_nodes, ignore_index=True)

    return pd.DataFrame(columns=[
        "MEMBER_HIERARCHY_ID", "MEMBER_ID", "LEVEL", "PARENT_MEMBER_ID",
        "COMPARATOR", "OPERATOR", "VALID_FROM", "VALID_TO"
    ])
