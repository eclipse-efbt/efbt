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
from collections import defaultdict
from .utils import (
    read_csv_to_dict, dict_list_to_structured_array, add_field, drop_fields,
    select_fields, rename_fields, pascal_to_upper_snake, clean_spaces
)


def map_axis_ordinate(path=os.path.join("target", "AxisOrdinate.csv"), axis_map: dict = {}):
    """Map axis ordinates from AxisOrdinate.csv to the target format"""
    types = defaultdict(lambda: str, OrdinateID="int", OrdinateCode="str", AxisID="int")
    data = read_csv_to_dict(path)
    # Force ID and PATH fields to be strings since they will be mapped to string values
    ordinates = dict_list_to_structured_array(data, force_str_columns={'OrdinateID', 'AxisID', 'ParentOrdinateID', 'Path'})

    column_mapping = {col: pascal_to_upper_snake(col) for col in ordinates.dtype.names}
    ordinates = rename_fields(ordinates, column_mapping)
    ordinates = add_field(ordinates, "MAINTENANCE_AGENCY_ID", "EBA")

    new_ordinate_ids = []
    for row in ordinates:
        axis_id = axis_map.get(str(row["AXIS_ID"]), str(row["AXIS_ID"]))
        code = str(row["ORDINATE_CODE"]).strip()
        new_ordinate_ids.append(f"{axis_id}_{code}")

    ordinates = add_field(ordinates, "NEW_ORDINATE_ID", new_ordinate_ids)

    id_mapping = {}
    for row in ordinates:
        id_mapping[str(row["ORDINATE_ID"])] = str(row["NEW_ORDINATE_ID"])

    ordinates = drop_fields(ordinates, "ORDINATE_ID")

    ordinates = rename_fields(ordinates, {
        "NEW_ORDINATE_ID": "AXIS_ORDINATE_ID",
        "ORDINATE_CODE": "CODE",
        "PARENT_ORDINATE_ID": "PARENT_AXIS_ORDINATE_ID",
        "ORDINATE_LABEL": "NAME"
    })

    # Update PARENT_AXIS_ORDINATE_ID
    parent_ids = []
    for row in ordinates:
        parent_ids.append(id_mapping.get(str(row["PARENT_AXIS_ORDINATE_ID"]), str(row["PARENT_AXIS_ORDINATE_ID"])))

    for i, row in enumerate(ordinates):
        ordinates[i]["PARENT_AXIS_ORDINATE_ID"] = parent_ids[i]

    ordinates = add_field(ordinates, "DESCRIPTION", "")

    # Update AXIS_ID
    axis_ids = []
    for row in ordinates:
        axis_ids.append(axis_map.get(str(row["AXIS_ID"]), str(row["AXIS_ID"])))

    for i, row in enumerate(ordinates):
        ordinates[i]["AXIS_ID"] = axis_ids[i]

    # Update PATH
    paths = []
    for row in ordinates:
        path = str(row["PATH"])
        parts = path.split(".")
        new_parts = []
        for part in parts:
            if part:
                try:
                    new_parts.append(id_mapping.get(part, part))
                except:
                    new_parts.append(part)
            else:
                new_parts.append(part)
        paths.append(".".join(new_parts))

    for i, row in enumerate(ordinates):
        ordinates[i]["PATH"] = paths[i]

    ordinates = select_fields(ordinates, [
        "AXIS_ORDINATE_ID", "IS_ABSTRACT_HEADER", "CODE", "ORDER", "LEVEL", "PATH", "AXIS_ID", "PARENT_AXIS_ORDINATE_ID", "NAME", "DESCRIPTION"
    ])

    # Clean text fields
    ordinates = clean_spaces(ordinates)

    return ordinates, id_mapping