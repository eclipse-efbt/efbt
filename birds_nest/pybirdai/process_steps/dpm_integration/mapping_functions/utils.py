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
import numpy as np
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
# COMPATIBILITY WRAPPERS FOR UNMIGRATED FILES
# These functions provide backwards compatibility for mapping files that
# haven't been migrated to pandas yet. They wrap pandas functionality.
# ============================================================================

def read_csv_to_dict(path, dtype=None):
    """DEPRECATED: Use pd.read_csv() directly. Compatibility wrapper."""
    df = pd.read_csv(path, dtype=str)
    return df.to_dict('records')


def dict_list_to_structured_array(data, columns=None, force_str_columns=None):
    """DEPRECATED: Use pd.DataFrame() directly. Compatibility wrapper."""
    if not data:
        return np.array([])

    df = pd.DataFrame(data)
    if columns:
        df = df[columns]

    return df.to_records(index=False)


def add_field(arr, field_name, values, dtype='U100'):
    """DEPRECATED: Use df[field_name] = values. Compatibility wrapper."""
    df = pd.DataFrame(arr)
    df[field_name] = values
    return df.to_records(index=False)


def rename_fields(arr, rename_dict):
    """DEPRECATED: Use df.rename(columns=...). Compatibility wrapper."""
    if len(arr) == 0:
        return arr
    df = pd.DataFrame(arr)
    df = df.rename(columns=rename_dict)
    return df.to_records(index=False)


def drop_fields(arr, fields_to_drop):
    """DEPRECATED: Use df.drop(columns=...). Compatibility wrapper."""
    if len(arr) == 0:
        return arr
    df = pd.DataFrame(arr)
    if isinstance(fields_to_drop, str):
        fields_to_drop = [fields_to_drop]
    df = df.drop(columns=fields_to_drop, errors='ignore')
    return df.to_records(index=False)


def select_fields(arr, fields):
    """DEPRECATED: Use df[fields]. Compatibility wrapper."""
    if len(arr) == 0:
        return arr
    df = pd.DataFrame(arr)
    return df[fields].to_records(index=False)


def clean_spaces(arr):
    """DEPRECATED: Use clean_spaces_df(). Compatibility wrapper."""
    if len(arr) == 0:
        return arr
    df = pd.DataFrame(arr)
    df = clean_spaces_df(df)
    return df.to_records(index=False)


def merge_arrays(left, right, left_on, right_on=None, how='inner', force_str_columns=None):
    """DEPRECATED: Use df.merge(). Compatibility wrapper."""
    if right_on is None:
        right_on = left_on
    if len(left) == 0 or len(right) == 0:
        return np.array([])

    df_left = pd.DataFrame(left)
    df_right = pd.DataFrame(right)
    result = df_left.merge(df_right, left_on=left_on, right_on=right_on, how=how)
    return result.to_records(index=False)


def array_to_dict(arr, key_field, value_field):
    """DEPRECATED: Use dict(zip()). Compatibility wrapper."""
    df = pd.DataFrame(arr)
    return dict(zip(df[key_field].astype(str), df[value_field].astype(str)))