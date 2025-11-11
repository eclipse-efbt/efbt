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
    df['ORIGINAL_DOMAIN_ID'] = df['DOMAIN_ID'].copy()
    df['MAPPED_DOMAIN_ID'] = df['DOMAIN_ID'].map(domain_map_norm).fillna(df['DOMAIN_ID'])

    # Handle missing domains by parsing MEMBER_XBRL_CODE
    # If domain is not found in mapping or is empty, extract from XBRL code
    def extract_domain_and_member_from_xbrl(row):
        """Extract domain and member info from MEMBER_XBRL_CODE when domain is missing"""
        domain_missing = (pd.isna(row['MAPPED_DOMAIN_ID']) or
                         row['MAPPED_DOMAIN_ID'] == '' or
                         (pd.notna(row['ORIGINAL_DOMAIN_ID']) and
                          str(row['ORIGINAL_DOMAIN_ID']).strip() not in domain_map_norm))

        if domain_missing and pd.notna(row['MEMBER_XBRL_CODE']) and ':' in str(row['MEMBER_XBRL_CODE']):
            # Split MEMBER_XBRL_CODE at ':' (e.g., "eba_AP:x1" -> ["eba_AP", "x1"])
            xbrl_parts = str(row['MEMBER_XBRL_CODE']).split(':', 1)
            domain_code = xbrl_parts[0]  # Left side: domain (e.g., "eba_AP")
            member_code = xbrl_parts[1]  # Right side: member (e.g., "x1")
            member_id = f"{domain_code}_{member_code}"  # Format: DomainID_MemberCode
            return pd.Series({'MAPPED_DOMAIN_ID': domain_code, 'NEW_MEMBER_ID': member_id, 'FROM_XBRL': True})
        else:
            # Use existing mapped domain and standard member ID format
            member_id = f"{row['MAPPED_DOMAIN_ID']}_EBA_{row['MEMBER_CODE']}"
            return pd.Series({'MAPPED_DOMAIN_ID': row['MAPPED_DOMAIN_ID'], 'NEW_MEMBER_ID': member_id, 'FROM_XBRL': False})

    # Apply the extraction logic
    xbrl_results = df.apply(extract_domain_and_member_from_xbrl, axis=1)
    df['MAPPED_DOMAIN_ID'] = xbrl_results['MAPPED_DOMAIN_ID']
    df['NEW_MEMBER_ID'] = xbrl_results['NEW_MEMBER_ID']

    # Create ID mapping
    id_mapping = dict(zip(df['MEMBER_ID'].astype(str), df['NEW_MEMBER_ID'].astype(str)))

    # Drop temporary columns before renaming
    columns_to_drop = ['MEMBER_ID', 'ORIGINAL_DOMAIN_ID', 'DOMAIN_ID']
    df.drop(axis=1, columns=[col for col in columns_to_drop if col in df.columns], inplace=True)

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
