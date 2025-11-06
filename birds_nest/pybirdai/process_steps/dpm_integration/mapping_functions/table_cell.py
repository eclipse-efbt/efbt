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

    # Optimized: Vectorized string conversion (replaces catastrophic loop)
    # Old: "EBA_" + str(int(float(str(row["CELL_ID"]))))
    # Convert entire column to float then int then string, prepend "EBA_"
    cell_ids_float = cells["CELL_ID"].astype(float).astype(int).astype(str)
    new_cell_ids = np.char.add("EBA_", cell_ids_float)

    cells = add_field(cells, "NEW_CELL_ID", new_cell_ids)

    # Optimized: Update TABLE_VID using vectorized mapping
    def map_table_vid(table_vid):
        return table_map.get(str(table_vid), str(table_vid))

    vec_map_table = np.vectorize(map_table_vid)
    cells["TABLE_VID"] = vec_map_table(cells["TABLE_VID"])

    # Optimized: Convert IS_SHADED to bool using vectorized operations
    if "IS_SHADED" in cells.dtype.names:
        # Vectorize the boolean conversion
        is_shaded_str = np.char.lower(cells["IS_SHADED"].astype(str))
        is_shaded = np.isin(is_shaded_str, ['true', '1', 'yes'])
    else:
        # Default to False for all rows
        is_shaded = np.zeros(len(cells), dtype=bool)

    cells = add_field(cells, "IS_SHADED_BOOL", is_shaded, dtype='bool')
    if "IS_SHADED" in cells.dtype.names:
        cells = drop_fields(cells, "IS_SHADED")
    cells = rename_fields(cells, {"IS_SHADED_BOOL": "IS_SHADED"})

    # Optimized: Handle DATA_POINT_VID using vectorized operations
    if not dp_map:
        cells = add_field(cells, "DATA_POINT_VID_NEW", "")
    else:
        if "DATA_POINT_VID" in cells.dtype.names:
            # Vectorize string cleaning and mapping
            dp_str = cells["DATA_POINT_VID"].astype(str)
            # Replace .0 and nan
            dp_str = np.char.replace(dp_str, '.0', '')
            dp_str = np.char.replace(dp_str, 'nan', '')

            # Vectorize dictionary lookup
            def map_dp_vid(val):
                return dp_map.get(val, val)

            vec_map_dp = np.vectorize(map_dp_vid)
            dp_vids = vec_map_dp(dp_str)
        else:
            # Default to empty strings for all rows
            dp_vids = np.full(len(cells), "", dtype=object)

        cells = add_field(cells, "DATA_POINT_VID_NEW", dp_vids)

    if "DATA_POINT_VID" in cells.dtype.names:
        cells = drop_fields(cells, "DATA_POINT_VID")
    cells = rename_fields(cells, {"DATA_POINT_VID_NEW": "DATA_POINT_VID"})

    # Optimized: Build id_mapping dict using vectorized array operations
    old_ids = cells["CELL_ID"].astype(str)
    new_ids = cells["NEW_CELL_ID"].astype(str)
    id_mapping = dict(zip(old_ids, new_ids))

    cells = drop_fields(cells, "CELL_ID")

    cells = rename_fields(cells, {
        "NEW_CELL_ID": "CELL_ID",
        "TABLE_VID": "TABLE_ID",
        "DATA_POINT_VID": "TABLE_CELL_COMBINATION_ID"
    })

    cells = add_field(cells, "SYSTEM_DATA_CODE", "")

    # Optimized: Build names array using vectorized conversion
    names = cells["CELL_ID"].astype(str)
    cells = add_field(cells, "NAME", names)

    cells = select_fields(cells, [
        "CELL_ID", "IS_SHADED", "TABLE_CELL_COMBINATION_ID", "SYSTEM_DATA_CODE", "NAME", "TABLE_ID"
    ])

    return cells, id_mapping
