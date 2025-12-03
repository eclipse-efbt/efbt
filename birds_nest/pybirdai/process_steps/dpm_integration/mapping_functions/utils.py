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

import pandas as pd
import re


def pascal_to_upper_snake(name):
    """Convert PascalCase to UPPER_SNAKE_CASE"""
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).upper()


def clean_spaces_df(df):
    """Clean spaces and carriage returns from string columns in DataFrame"""
    if df.empty:
        return df

    df = df.copy()

    # Get list of object/string columns
    str_cols = df.select_dtypes(include=['object']).columns.tolist()

    # Apply string cleaning to each column
    for col in str_cols:
        try:
            df[col] = (
                df[col]
                .fillna('')
                .astype(str)
                .str.replace("\n", " ", regex=False)
                .str.replace("\r", " ", regex=False)
                .str.replace('"', "'", regex=False)
                .str.replace("  ", " ", regex=False)
                .str.strip()
            )
        except Exception as e:
            # If cleaning fails for a column, skip it
            pass

    return df


def normalize_id_map(id_map):
    """
    Normalize ID mapping dict to handle .0 variants.
    Creates entries for both 'key' and 'key.0' pointing to same value.
    """
    normalized = {}
    for key, value in id_map.items():
        key_str = str(key)
        normalized[key_str] = value
        # Add .0 variant if key looks numeric and doesn't already have .0
        if '.' not in key_str and key_str.replace('-', '').isdigit():
            normalized[key_str + '.0'] = value
        # Also add variant without .0 if key ends with .0
        if key_str.endswith('.0'):
            normalized[key_str[:-2]] = value
    return normalized


# ============================================================================
# COMMON MAPPING UTILITIES
# Reusable functions for common patterns in mapping files
# ============================================================================

def apply_cascade_filter(df, column, id_map):
    """
    Filter DataFrame to only keep rows where column value exists in id_map keys.

    This is the "cascade filter" pattern - ensuring child entities only include
    those whose parent entity was imported.

    Args:
        df: DataFrame to filter
        column: Column name to filter on
        id_map: Dict mapping source IDs to target IDs (only keys are used)

    Returns:
        Filtered DataFrame

    Example:
        # Only keep axes where TABLE_VID exists in table_map
        df = apply_cascade_filter(df, 'TABLE_VID', table_map)
    """
    if not id_map:
        return df
    return df[df[column].astype(str).isin(id_map.keys())]


def convert_to_bool(df, column, default=False):
    """
    Convert a column to boolean, handling string representations.

    Recognizes 'true', '1', 'yes' (case-insensitive) as True, everything else as False.

    Args:
        df: DataFrame (modified in place)
        column: Column name to convert
        default: Default value if column doesn't exist

    Returns:
        DataFrame with converted column

    Example:
        df = convert_to_bool(df, 'IS_SHADED', default=False)
    """
    if column not in df.columns:
        df[column] = default
    else:
        df[column] = (
            df[column]
            .astype(str)
            .str.lower()
            .isin(['true', '1', 'yes'])
        )
    return df


def map_column(df, column, id_map, keep_original=True):
    """
    Map values in a column using an ID mapping dict.

    Args:
        df: DataFrame (modified in place)
        column: Column name to map
        id_map: Dict mapping source values to target values
        keep_original: If True, keep original value when no mapping found

    Returns:
        DataFrame with mapped column

    Example:
        df = map_column(df, 'TABLE_VID', table_map)
    """
    if not id_map:
        return df

    if keep_original:
        df[column] = df[column].astype(str).map(id_map).fillna(df[column])
    else:
        df[column] = df[column].astype(str).map(id_map)

    return df


def select_final_columns(df, columns):
    """
    Select and reorder columns for final output.

    Args:
        df: DataFrame
        columns: List of column names to select (in order)

    Returns:
        DataFrame with only the specified columns
    """
    return df[columns]