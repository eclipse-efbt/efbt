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
    select_fields, rename_fields, pascal_to_upper_snake, clean_spaces
)


def map_axis(path=os.path.join("target", "Axis.csv"), table_map: dict = {}):
    """Map axis from Axis.csv to the target format"""
    orientation_id_map = {"X": "1", "Y": "2", "Z": "3", "0": "0"}
    data = read_csv_to_dict(path)
    # Force ID fields to be strings since they will be mapped to string values
    axes = dict_list_to_structured_array(data, force_str_columns={'AxisID', 'TableVID'})

    column_mapping = {col: pascal_to_upper_snake(col) for col in axes.dtype.names}
    axes = rename_fields(axes, column_mapping)
    axes = add_field(axes, "MAINTENANCE_AGENCY_ID", "EBA")

    new_axis_ids = []
    for row in axes:
        table_id = table_map.get(str(row["TABLE_VID"]), str(row["TABLE_VID"]))
        orientation = orientation_id_map.get(str(row["AXIS_ORIENTATION"]), "")
        new_axis_ids.append(f"{table_id}_{orientation}")

    axes = add_field(axes, "NEW_AXIS_ID", new_axis_ids)

    id_mapping = {}
    for row in axes:
        id_mapping[str(row["AXIS_ID"])] = str(row["NEW_AXIS_ID"])

    axes = drop_fields(axes, "AXIS_ID")

    axes = rename_fields(axes, {
        "NEW_AXIS_ID": "AXIS_ID",
        "AXIS_LABEL": "NAME",
        "AXIS_ORIENTATION": "ORIENTATION"
    })

    # Add CODE field
    codes = []
    for row in axes:
        parts = str(row["AXIS_ID"]).rsplit("_", 4)
        if len(parts) >= 4:
            code = "_".join(parts[-4:-2] + [parts[-1]])
        else:
            code = str(row["AXIS_ID"])
        codes.append(code)

    axes = add_field(axes, "CODE", codes)

    # Add TABLE_ID field
    table_ids = []
    for row in axes:
        table_ids.append(table_map.get(str(row["TABLE_VID"]), str(row["TABLE_VID"])))

    axes = add_field(axes, "TABLE_ID", table_ids)

    # Add ORDER field
    orders = []
    for row in axes:
        orders.append(orientation_id_map.get(str(row["ORIENTATION"]), ""))

    axes = add_field(axes, "ORDER", orders)

    axes = add_field(axes, "DESCRIPTION", "")

    # Convert IS_OPEN_AXIS to bool
    is_open = []
    for row in axes:
        if "IS_OPEN_AXIS" in axes.dtype.names:
            val = str(row["IS_OPEN_AXIS"])
        else:
            val = "False"
        is_open.append(val.lower() in ['true', '1', 'yes'])

    axes = add_field(axes, "IS_OPEN_AXIS_BOOL", is_open, dtype='bool')
    if "IS_OPEN_AXIS" in axes.dtype.names:
        axes = drop_fields(axes, "IS_OPEN_AXIS")
    axes = rename_fields(axes, {"IS_OPEN_AXIS_BOOL": "IS_OPEN_AXIS"})

    axes = select_fields(axes, [
        "AXIS_ID", "CODE", "ORIENTATION", "ORDER", "NAME", "DESCRIPTION", "TABLE_ID", "IS_OPEN_AXIS"
    ])

    # Clean text fields
    axes = clean_spaces(axes)

    return axes, id_mapping