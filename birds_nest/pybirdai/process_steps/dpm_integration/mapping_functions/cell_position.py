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
import numpy as np
from pybirdai.process_steps.dpm_integration.mapping_functions.utils import (
    read_csv_to_dict, dict_list_to_structured_array, add_field, drop_fields,
    rename_fields, pascal_to_upper_snake, select_fields
)


def map_cell_position(path=os.path.join("target", "CellPosition.csv"), cell_map: dict = {}, ordinate_map: dict = {}, start_index_after_last: bool = False):
    """Map cell positions from CellPosition.csv to the target format"""
    data_list = read_csv_to_dict(path)
    # Force ID fields to be strings since they will be mapped to string values
    data = dict_list_to_structured_array(data_list, force_str_columns={'CellID', 'OrdinateID'})

    column_mapping = {col: pascal_to_upper_snake(col) for col in data.dtype.names}
    data = rename_fields(data, column_mapping)

    # Optimized: Update CELL_ID and ORDINATE_ID using vectorized operations
    # Create vectorized mapping functions
    def map_cell_id(cell_id):
        return cell_map.get(str(cell_id), str(cell_id))

    def map_ordinate_id(ordinate_id):
        return ordinate_map.get(str(ordinate_id), str(ordinate_id))

    # Vectorize the mapping functions for efficient application
    vec_map_cell = np.vectorize(map_cell_id)
    vec_map_ordinate = np.vectorize(map_ordinate_id)

    # Apply mappings to entire columns at once
    data["CELL_ID"] = vec_map_cell(data["CELL_ID"])
    data["ORDINATE_ID"] = vec_map_ordinate(data["ORDINATE_ID"])

    # Optimized: Generate IDs using numpy array operations
    if start_index_after_last and "ID" in data.dtype.names and len(data) > 0:
        # Vectorized max_id calculation
        id_column = data["ID"]
        # Filter out NaN values and convert to int
        valid_ids = id_column[np.char.str_len(id_column.astype(str)) > 0]
        valid_ids = valid_ids[valid_ids.astype(str) != 'nan']
        max_id = int(float(np.max(valid_ids))) if len(valid_ids) > 0 else 0
        start_idx = max_id + 1 if max_id else 0
        # Generate IDs using numpy arange (much faster than list(range()))
        data["ID"] = np.arange(start_idx, start_idx + len(data), dtype='i8')
    else:
        if "ID" in data.dtype.names:
            data = drop_fields(data, "ID")
        # Generate IDs using numpy arange
        ids = np.arange(len(data), dtype='i8')
        data = add_field(data, "ID", ids, dtype='i8')

    data = rename_fields(data, {"ORDINATE_ID": "AXIS_ORDINATE_ID"})

    # Reorder columns to: ID, CELL_ID, AXIS_ORDINATE_ID
    desired_column_order = ["ID", "CELL_ID", "AXIS_ORDINATE_ID"]

    # Only include columns that actually exist in the data
    available_columns = [col for col in desired_column_order if col in data.dtype.names]

    # Add any remaining columns that aren't in the desired order
    remaining_columns = [col for col in data.dtype.names if col not in available_columns]
    final_column_order = available_columns + remaining_columns

    # Reorder the data
    if len(data) > 0:
        data = select_fields(data, final_column_order)

    return data, {}
