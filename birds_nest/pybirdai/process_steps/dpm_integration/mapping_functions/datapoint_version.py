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
from collections import defaultdict
from .utils import (
    read_csv_to_dict, dict_list_to_structured_array, add_field, drop_fields,
    select_fields, rename_fields, pascal_to_upper_snake, merge_arrays, clean_spaces
)


def map_datapoint_version(path=os.path.join("target", "DataPointVersion.csv"), context_map: dict = {}, context_data=None, dimension_map: dict = {}, member_map: dict = {}):
    """Map datapoint versions from DataPointVersion.csv to the target format"""
    types = defaultdict(lambda: str, ContextID="str")
    data_list = read_csv_to_dict(path)
    # Force ID fields to be strings since they will be mapped to string values
    dpv = dict_list_to_structured_array(data_list, force_str_columns={'DataPointVID', 'ContextID'})

    column_mapping = {col: pascal_to_upper_snake(col) for col in dpv.dtype.names}
    dpv = rename_fields(dpv, column_mapping)
    dpv = add_field(dpv, "MAINTENANCE_AGENCY_ID", "EBA")

    new_dpv_ids = []
    for row in dpv:
        new_dpv_ids.append("EBA_" + str(int(float(str(row["DATA_POINT_VID"])))))

    dpv = add_field(dpv, "NEW_DATA_POINT_VID", new_dpv_ids)

    if context_data is not None and len(context_data) > 0:
        dpv_subset = select_fields(dpv, ["NEW_DATA_POINT_VID", "CONTEXT_ID"])
        dp_items = merge_arrays(dpv_subset, context_data, "CONTEXT_ID")
        dp_items = drop_fields(dp_items, "CONTEXT_ID")
    else:
        dp_items = np.array([])

    id_mapping = {}
    for row in dpv:
        id_mapping[str(row["DATA_POINT_VID"])] = str(row["NEW_DATA_POINT_VID"])

    def is_number(char):
        if char in "0123456789":
            return True
        return False

    def compute_code(string):
        new_key = ""
        value = ""
        previous_char = ""
        key_value = dict()
        is_new_key = True
        is_new_value = False
        for idx, char in enumerate(string):
            if previous_char.isnumeric() and char.isalpha():
                new_key = ""
                is_new_key = True
                is_new_value = False

            if char.isnumeric() and previous_char.isalpha():
                is_new_value = True
                is_new_key = False

            if is_new_key:
                new_key += char

            if is_new_value:
                if "EBA_" + new_key not in key_value:
                    key_value["EBA_" + new_key] = ""
                key_value["EBA_" + new_key] += char

            previous_char = char

        return "|".join(f"{key}({member_map.get(int(float(value)), value)})" for key, value in key_value.items())

    dpv = rename_fields(dpv, {
        "NEW_DATA_POINT_VID": "COMBINATION_ID",
        "DATA_POINT_VID": "CODE",
        "FROM_DATE": "VALID_FROM",
        "TO_DATE": "VALID_TO",
        "CATEGORISATION_KEY": "NAME"
    })

    # Update NAME field
    names = []
    for row in dpv:
        names.append(compute_code(str(row["NAME"])))

    for i, row in enumerate(dpv):
        dpv[i]["NAME"] = names[i]

    dpv = add_field(dpv, "VERSION", "")

    if len(dp_items) > 0:
        dp_items = rename_fields(dp_items, {
            "NEW_DATA_POINT_VID": "COMBINATION_ID",
            "DIMENSION_ID": "VARIABLE_ID",
            "MEMBER_ID": "MEMBER_ID"
        })

        dp_items = add_field(dp_items, "VARIABLE_SET", "")
        dp_items = add_field(dp_items, "SUBDOMAIN_ID", "")

        dp_items = select_fields(dp_items, [
            "COMBINATION_ID", "VARIABLE_ID", "MEMBER_ID", "VARIABLE_SET", "SUBDOMAIN_ID"
        ])

    dpv = select_fields(dpv, [
        "COMBINATION_ID", "CODE", "NAME", "MAINTENANCE_AGENCY_ID", "VERSION", "VALID_FROM", "VALID_TO"
    ])

    # Clean text fields
    dpv = clean_spaces(dpv)

    return (dpv, dp_items), id_mapping