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
    pascal_to_upper_snake, normalize_id_map
)


def traceback_restrictions(path=os.path.join("target", "OpenMemberRestriction.csv")):
    """Load and process open member restrictions"""
    df = pd.read_csv(path, dtype=str)

    # Rename columns with "Restriction" prefix (except RestrictionID)
    rename_dict = {col: "Restriction" + col for col in df.columns if col != "RestrictionID"}
    df = df.rename(columns=rename_dict)

    return df


def map_ordinate_categorisation(path=os.path.join("target", "OrdinateCategorisation.csv"), member_map: dict = {}, dimension_map: dict = {}, ordinate_map: dict = {}, hierarchy_map: dict = {}, start_index_after_last: bool = False):
    """Map ordinate categorisation from OrdinateCategorisation.csv to the target format"""
    df = pd.read_csv(path, dtype=str)

    # Merge with restrictions
    restrictions_df = traceback_restrictions()
    if not restrictions_df.empty:
        df = df.merge(restrictions_df, on="RestrictionID", how="left")

    # Transform column names to UPPER_SNAKE_CASE
    df.columns = [pascal_to_upper_snake(col) for col in df.columns]
    df['MAINTENANCE_AGENCY_ID'] = "EBA"

    # Normalize ID maps for .0 variants
    member_map_norm = normalize_id_map(member_map)
    hierarchy_map_norm = normalize_id_map(hierarchy_map)

    # Vectorized ID mapping (all 5 ID fields)
    df['MEMBER_ID'] = df['MEMBER_ID'].astype(str).map(member_map_norm).fillna(df['MEMBER_ID'])
    df['DIMENSION_ID'] = df['DIMENSION_ID'].astype(str).map(dimension_map).fillna(df['DIMENSION_ID'])
    df['ORDINATE_ID'] = df['ORDINATE_ID'].astype(str).map(ordinate_map).fillna(df['ORDINATE_ID'])

    if 'RESTRICTION_HIERARCHY_ID' in df.columns:
        df['RESTRICTION_HIERARCHY_ID'] = df['RESTRICTION_HIERARCHY_ID'].astype(str).map(hierarchy_map_norm).fillna(df['RESTRICTION_HIERARCHY_ID'])

    if 'RESTRICTION_MEMBER_ID' in df.columns:
        df['RESTRICTION_MEMBER_ID'] = df['RESTRICTION_MEMBER_ID'].astype(str).map(member_map_norm).fillna(df['RESTRICTION_MEMBER_ID'])

    # Rename fields
    df = df.rename(columns={
        "ORDINATE_ID": "AXIS_ORDINATE_ID",
        "DIMENSION_ID": "VARIABLE_ID",
        "RESTRICTION_HIERARCHY_ID": "MEMBER_HIERARCHY_ID",
        "RESTRICTION_MEMBER_ID": "STARTING_MEMBER_ID",
        "RESTRICTION_MEMBER_INCLUDED": "IS_STARTING_MEMBER_INCLUDED"
    })

    # Convert IS_STARTING_MEMBER_INCLUDED to bool
    if 'IS_STARTING_MEMBER_INCLUDED' in df.columns:
        df['IS_STARTING_MEMBER_INCLUDED'] = (
            df['IS_STARTING_MEMBER_INCLUDED']
            .astype(str)
            .str.lower()
            .isin(['true', '1', 'yes'])
        )
    else:
        df['IS_STARTING_MEMBER_INCLUDED'] = False

    df['MEMBER_HIERARCHY_VALID_FROM'] = ""

    # Set IS_STARTING_MEMBER_INCLUDED to False where STARTING_MEMBER_ID is empty
    if 'STARTING_MEMBER_ID' in df.columns:
        empty_mask = df['STARTING_MEMBER_ID'].isin(['', 'nan']) | df['STARTING_MEMBER_ID'].isna()
        df.loc[empty_mask, 'IS_STARTING_MEMBER_INCLUDED'] = False

    # Handle ID generation
    if start_index_after_last and 'ID' in df.columns and not df.empty:
        valid_ids = pd.to_numeric(df['ID'], errors='coerce').dropna()
        max_id = int(valid_ids.max()) if not valid_ids.empty else 0
        start_idx = max_id + 1
        df['ID'] = range(start_idx, start_idx + len(df))
    else:
        df['ID'] = range(len(df))

    # Select final columns
    df = df[[
        "ID",
        "MEMBER_HIERARCHY_VALID_FROM",
        "IS_STARTING_MEMBER_INCLUDED",
        "AXIS_ORDINATE_ID",
        "VARIABLE_ID",
        "MEMBER_ID",
        "MEMBER_HIERARCHY_ID",
        "STARTING_MEMBER_ID"
    ]]

    return df, {}
