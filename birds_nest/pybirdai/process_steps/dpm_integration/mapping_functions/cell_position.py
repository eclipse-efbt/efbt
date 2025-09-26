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
from .utils import (
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

    # Update CELL_ID
    cell_ids = []
    for row in data:
        cell_ids.append(cell_map.get(str(row["CELL_ID"]), str(row["CELL_ID"])))

    for i, row in enumerate(data):
        data[i]["CELL_ID"] = cell_ids[i]

    # Update ORDINATE_ID
    ordinate_ids = []
    for row in data:
        ordinate_ids.append(ordinate_map.get(str(row["ORDINATE_ID"]), str(row["ORDINATE_ID"])))

    for i, row in enumerate(data):
        data[i]["ORDINATE_ID"] = ordinate_ids[i]

    if start_index_after_last and "ID" in data.dtype.names and len(data) > 0:
        max_id = max(int(float(row["ID"])) for row in data if str(row["ID"]) != 'nan')
        start_idx = max_id + 1 if max_id else 0
        ids = list(range(start_idx, start_idx + len(data)))
        for i, row in enumerate(data):
            data[i]["ID"] = ids[i]
    else:
        if "ID" in data.dtype.names:
            data = drop_fields(data, "ID")
        ids = list(range(len(data)))
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
