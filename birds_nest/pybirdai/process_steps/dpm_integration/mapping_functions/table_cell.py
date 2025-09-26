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
    select_fields, rename_fields, pascal_to_upper_snake
)


def map_table_cell(path=os.path.join("target", "TableCell.csv"), table_map: dict = {}, dp_map: dict = {}):
    """Map table cells from TableCell.csv to the target format"""
    data = read_csv_to_dict(path)
    # Force ID fields to be strings since they will be mapped to string values
    cells = dict_list_to_structured_array(data, force_str_columns={'CellID', 'TableVID', 'DataPointVID'})

    column_mapping = {col: pascal_to_upper_snake(col) for col in cells.dtype.names}
    cells = rename_fields(cells, column_mapping)
    cells = add_field(cells, "MAINTENANCE_AGENCY_ID", "EBA")

    new_cell_ids = []
    for row in cells:
        new_cell_ids.append("EBA_" + str(int(float(str(row["CELL_ID"])))))

    cells = add_field(cells, "NEW_CELL_ID", new_cell_ids)

    # Update TABLE_VID
    table_vids = []
    for row in cells:
        table_vids.append(table_map.get(str(row["TABLE_VID"]), str(row["TABLE_VID"])))

    for i, row in enumerate(cells):
        cells[i]["TABLE_VID"] = table_vids[i]

    # Convert IS_SHADED to bool
    is_shaded = []
    for row in cells:
        if "IS_SHADED" in cells.dtype.names:
            val = str(row["IS_SHADED"])
        else:
            val = "False"
        is_shaded.append(val.lower() in ['true', '1', 'yes'])

    cells = add_field(cells, "IS_SHADED_BOOL", is_shaded, dtype='bool')
    if "IS_SHADED" in cells.dtype.names:
        cells = drop_fields(cells, "IS_SHADED")
    cells = rename_fields(cells, {"IS_SHADED_BOOL": "IS_SHADED"})

    # Handle DATA_POINT_VID
    if not dp_map:
        cells = add_field(cells, "DATA_POINT_VID_NEW", "")
    else:
        dp_vids = []
        for row in cells:
            if "DATA_POINT_VID" in cells.dtype.names:
                val = str(row["DATA_POINT_VID"])
            else:
                val = ""
            val = val.replace(".0", "").replace("nan", "")
            dp_vids.append(dp_map.get(val, val))
        cells = add_field(cells, "DATA_POINT_VID_NEW", dp_vids)

    if "DATA_POINT_VID" in cells.dtype.names:
        cells = drop_fields(cells, "DATA_POINT_VID")
    cells = rename_fields(cells, {"DATA_POINT_VID_NEW": "DATA_POINT_VID"})

    id_mapping = {}
    for row in cells:
        id_mapping[str(row["CELL_ID"])] = str(row["NEW_CELL_ID"])

    cells = drop_fields(cells, "CELL_ID")

    cells = rename_fields(cells, {
        "NEW_CELL_ID": "CELL_ID",
        "TABLE_VID": "TABLE_ID",
        "DATA_POINT_VID": "TABLE_CELL_COMBINATION_ID"
    })

    cells = add_field(cells, "SYSTEM_DATA_CODE", "")

    names = []
    for row in cells:
        names.append(str(row["CELL_ID"]))
    cells = add_field(cells, "NAME", names)

    cells = select_fields(cells, [
        "CELL_ID", "IS_SHADED", "TABLE_CELL_COMBINATION_ID", "SYSTEM_DATA_CODE", "NAME", "TABLE_ID"
    ])

    return cells, id_mapping