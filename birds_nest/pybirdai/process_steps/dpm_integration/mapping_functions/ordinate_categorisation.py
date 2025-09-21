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
from .utils import (
    read_csv_to_dict, dict_list_to_structured_array, add_field, drop_fields,
    select_fields, rename_fields, pascal_to_upper_snake, merge_arrays
)


def traceback_restrictions(path=os.path.join("target", "OpenMemberRestriction.csv")):
    """Load and process open member restrictions"""
    data_list = read_csv_to_dict(path)
    # Force ID fields to be strings since they will be mapped to string values
    restriction_df = dict_list_to_structured_array(data_list, force_str_columns={'HierarchyID', 'MemberID'})

    # Rename columns with "Restriction" prefix
    rename_dict = {}
    for col in restriction_df.dtype.names:
        if col != "RestrictionID":
            rename_dict[col] = "Restriction" + col

    restriction_df = rename_fields(restriction_df, rename_dict)

    return restriction_df


def map_ordinate_categorisation(path=os.path.join("target", "OrdinateCategorisation.csv"), member_map: dict = {}, dimension_map: dict = {}, ordinate_map: dict = {}, hierarchy_map: dict = {}, start_index_after_last: bool = False):
    """Map ordinate categorisation from OrdinateCategorisation.csv to the target format"""
    data_list = read_csv_to_dict(path)
    # Force ID fields to be strings since they will be mapped to string values
    data = dict_list_to_structured_array(data_list, force_str_columns={'MemberID', 'DimensionID', 'OrdinateID'})

    restrictions_arr = traceback_restrictions()

    if len(restrictions_arr) > 0:
        data = merge_arrays(data, restrictions_arr, "RestrictionID", how="left", force_str_columns={'MemberID', 'DimensionID', 'OrdinateID', 'HierarchyID'})

    column_mapping = {col: pascal_to_upper_snake(col) for col in data.dtype.names}
    data = rename_fields(data, column_mapping)
    data = add_field(data, "MAINTENANCE_AGENCY_ID", "EBA")

    # Update MEMBER_ID
    member_ids = []
    for row in data:
        mem_id = str(row["MEMBER_ID"])
        # Try both with and without .0 suffix for numeric member IDs
        mapped_id = member_map.get(mem_id, None)
        if mapped_id is None:
            mapped_id = member_map.get(mem_id + ".0", mem_id)
        member_ids.append(mapped_id)

    for i, row in enumerate(data):
        data[i]["MEMBER_ID"] = member_ids[i]

    # Update DIMENSION_ID
    dimension_ids = []
    for row in data:
        dimension_ids.append(dimension_map.get(str(row["DIMENSION_ID"]), str(row["DIMENSION_ID"])))

    for i, row in enumerate(data):
        data[i]["DIMENSION_ID"] = dimension_ids[i]

    # Update ORDINATE_ID
    ordinate_ids = []
    for row in data:
        ordinate_ids.append(ordinate_map.get(str(row["ORDINATE_ID"]), str(row["ORDINATE_ID"])))

    for i, row in enumerate(data):
        data[i]["ORDINATE_ID"] = ordinate_ids[i]

    # Update RESTRICTION_HIERARCHY_ID
    if "RESTRICTION_HIERARCHY_ID" in data.dtype.names:
        restriction_hierarchy_ids = []
        for row in data:
            restriction_hierarchy_ids.append(hierarchy_map.get(str(row["RESTRICTION_HIERARCHY_ID"]), str(row["RESTRICTION_HIERARCHY_ID"])))

        for i, row in enumerate(data):
            data[i]["RESTRICTION_HIERARCHY_ID"] = restriction_hierarchy_ids[i]

    # Update RESTRICTION_MEMBER_ID
    if "RESTRICTION_MEMBER_ID" in data.dtype.names:
        restriction_member_ids = []
        for row in data:
            restriction_member_ids.append(member_map.get(str(row["RESTRICTION_MEMBER_ID"]), str(row["RESTRICTION_MEMBER_ID"])))

        for i, row in enumerate(data):
            data[i]["RESTRICTION_MEMBER_ID"] = restriction_member_ids[i]

    data = rename_fields(data, {
        "ORDINATE_ID": "AXIS_ORDINATE_ID",
        "DIMENSION_ID": "VARIABLE_ID",
        "MEMBER_ID": "MEMBER_ID",
        "RESTRICTION_HIERARCHY_ID": "MEMBER_HIERARCHY_ID",
        "RESTRICTION_MEMBER_ID": "STARTING_MEMBER_ID",
        "RESTRICTION_MEMBER_INCLUDED": "IS_STARTING_MEMBER_INCLUDED"
    })

    # Convert IS_STARTING_MEMBER_INCLUDED to bool
    if "IS_STARTING_MEMBER_INCLUDED" in data.dtype.names:
        is_included = []
        for row in data:
            val = str(row["IS_STARTING_MEMBER_INCLUDED"])
            is_included.append(val.lower() in ['true', '1', 'yes'])

        data = add_field(data, "IS_STARTING_MEMBER_INCLUDED_BOOL", is_included, dtype='bool')
        if "IS_STARTING_MEMBER_INCLUDED" in data.dtype.names:
            data = drop_fields(data, "IS_STARTING_MEMBER_INCLUDED")
        data = rename_fields(data, {"IS_STARTING_MEMBER_INCLUDED_BOOL": "IS_STARTING_MEMBER_INCLUDED"})

    data = add_field(data, "MEMBER_HIERARCHY_VALID_FROM", "")

    # Set IS_STARTING_MEMBER_INCLUDED to False where STARTING_MEMBER_ID is empty
    if "STARTING_MEMBER_ID" in data.dtype.names and "IS_STARTING_MEMBER_INCLUDED" in data.dtype.names:
        for i, row in enumerate(data):
            if str(row["STARTING_MEMBER_ID"]) == '' or str(row["STARTING_MEMBER_ID"]) == 'nan':
                data[i]["IS_STARTING_MEMBER_INCLUDED"] = False

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

    data = select_fields(data, [
        "ID",
        "MEMBER_HIERARCHY_VALID_FROM",
        "IS_STARTING_MEMBER_INCLUDED",
        "AXIS_ORDINATE_ID",
        "VARIABLE_ID",
        "MEMBER_ID",
        "MEMBER_HIERARCHY_ID",
        "STARTING_MEMBER_ID"
    ])

    return data, {}