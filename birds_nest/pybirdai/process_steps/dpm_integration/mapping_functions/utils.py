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

import numpy as np
import csv
import re
from collections import defaultdict


def pascal_to_upper_snake(name):
    """Convert PascalCase to UPPER_SNAKE_CASE"""
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).upper()


def read_csv_to_dict(path, dtype=None):
    """Read CSV file and return as list of dictionaries"""
    data = []
    with open(path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            data.append(row)
    return data


def dict_list_to_structured_array(data, columns=None, force_str_columns=None):
    """Convert list of dictionaries to numpy structured array"""
    if not data:
        return np.array([])

    if columns is None:
        columns = list(data[0].keys())

    if force_str_columns is None:
        force_str_columns = set()

    # Create dtype for structured array
    dtype_list = []
    for col in columns:
        # Force certain columns to be strings
        if col in force_str_columns:
            max_len = max(len(str(row.get(col, ''))) for row in data)
            dtype_list.append((col, f'U{max(max_len + 50, 100)}'))  # Extra space for transformations
        else:
            # Check if column contains numeric data
            sample_val = str(data[0].get(col, '')).strip()
            if sample_val and sample_val.replace('.', '').replace('-', '').isdigit():
                dtype_list.append((col, 'i8'))
            else:
                # Find max string length for this column
                max_len = max(len(str(row.get(col, ''))) for row in data)
                dtype_list.append((col, f'U{max(max_len, 1)}'))

    # Create structured array
    arr = np.zeros(len(data), dtype=dtype_list)
    for i, row in enumerate(data):
        for col in columns:
            val = row.get(col, '')
            if arr.dtype[col].kind in ['f', 'i']:
                try:
                    arr[i][col] = int(float(val)) if val else 0
                except:
                    arr[i][col] = 0
            else:
                arr[i][col] = str(val)

    return arr


def rename_fields(arr, rename_dict):
    """Rename fields in structured array"""
    if len(arr) == 0:
        return arr

    # Handle empty array
    if arr.size == 0:
        return arr

    old_dtype = arr.dtype
    new_dtype = []

    # Build new dtype safely
    for name in old_dtype.names:
        dtype_info = old_dtype.fields[name]
        new_name = rename_dict.get(name, name)
        new_dtype.append((new_name, dtype_info[0]))

    new_arr = np.zeros(arr.shape, dtype=new_dtype)
    for old_name in old_dtype.names:
        new_name = rename_dict.get(old_name, old_name)
        if new_name in new_arr.dtype.names:
            new_arr[new_name] = arr[old_name]

    return new_arr


def drop_fields(arr, fields_to_drop):
    """Drop fields from structured array"""
    if len(arr) == 0:
        return arr

    if isinstance(fields_to_drop, str):
        fields_to_drop = [fields_to_drop]

    keep_fields = [f for f in arr.dtype.names if f not in fields_to_drop]
    return arr[keep_fields]


def select_fields(arr, fields):
    """Select specific fields from structured array"""
    if len(arr) == 0:
        return arr
    return arr[fields]


def add_field(arr, field_name, values, dtype='U100'):
    """Add a new field to structured array"""
    if len(arr) == 0:
        new_dtype = [(field_name, dtype)]
        return np.array(values if isinstance(values, list) else [values], dtype=new_dtype)

    old_dtype = arr.dtype.descr
    new_dtype = old_dtype + [(field_name, dtype)]

    new_arr = np.zeros(arr.shape, dtype=new_dtype)

    for field in arr.dtype.names:
        new_arr[field] = arr[field]

    if isinstance(values, (list, np.ndarray)):
        new_arr[field_name] = values
    else:
        new_arr[field_name][:] = values

    return new_arr


def merge_arrays(left, right, left_on, right_on=None, how='inner', force_str_columns=None):
    """Merge two structured arrays"""
    if right_on is None:
        right_on = left_on

    if len(left) == 0 or len(right) == 0:
        return np.array([])

    # Create mapping from right array
    right_dict = {}
    for row in right:
        key = str(row[right_on])
        if key not in right_dict:
            right_dict[key] = []
        right_dict[key].append(row)

    # Merge
    result = []
    for left_row in left:
        key = str(left_row[left_on])
        if key in right_dict:
            for right_row in right_dict[key]:
                # Combine fields
                merged_row = {}
                for field in left.dtype.names:
                    merged_row[field] = left_row[field]
                for field in right.dtype.names:
                    if field != right_on and field not in merged_row:  # Avoid duplicate fields
                        merged_row[field] = right_row[field]
                result.append(merged_row)
        elif how == 'left':
            merged_row = {}
            for field in left.dtype.names:
                merged_row[field] = left_row[field]
            for field in right.dtype.names:
                if field != right_on and field not in merged_row:
                    merged_row[field] = ''
            result.append(merged_row)

    if not result:
        return np.array([])

    return dict_list_to_structured_array(result, force_str_columns=force_str_columns)


def array_to_dict(arr, key_field, value_field):
    """Convert structured array to dictionary"""
    result = {}
    for row in arr:
        result[str(row[key_field])] = str(row[value_field])
    return result


def clean_spaces(arr):
    """Clean spaces and carriage returns from string fields"""
    if len(arr) == 0:
        return arr

    new_arr = arr.copy()
    for field in arr.dtype.names:
        if arr.dtype[field].kind == 'U':  # Unicode string
            for i in range(len(new_arr)):
                val = str(new_arr[i][field])
                # Replace all types of line breaks and carriage returns with spaces
                val = val.replace("\n", " ").replace("\r", " ").replace("\r\n", " ")
                # Replace quotes and clean up multiple spaces
                val = val.replace('"', "'").replace("  ", " ").strip()
                new_arr[i][field] = val
    return new_arr