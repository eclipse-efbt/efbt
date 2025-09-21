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
    read_csv_to_dict, dict_list_to_structured_array, add_field,
    select_fields, rename_fields, pascal_to_upper_snake
)


def map_hierarchy_node(path=os.path.join("target", "HierarchyNode.csv"), hierarchy_map: dict = {}, member_map: dict = {}):
    """Map hierarchy nodes from HierarchyNode.csv to the target format"""
    data_list = read_csv_to_dict(path)

    # Handle NaN values
    for row in data_list:
        if not row.get("ParentMemberID") or row["ParentMemberID"] == '':
            row["ParentMemberID"] = "0"
        if not row.get("MemberID") or row["MemberID"] == '':
            row["MemberID"] = "0"

    # Force ID fields to be strings since they will be mapped to string values
    data = dict_list_to_structured_array(data_list, force_str_columns={'HierarchyID', 'MemberID', 'ParentHierarchyID', 'ParentMemberID'})

    column_mapping = {col: pascal_to_upper_snake(col) for col in data.dtype.names}
    data = rename_fields(data, column_mapping)

    # Update MEMBER_ID
    member_ids = []
    for row in data:
        mem_id = int(float(str(row["MEMBER_ID"])))
        # Try both with and without .0 suffix for numeric member IDs
        mapped_id = member_map.get(str(mem_id), None)
        if mapped_id is None:
            mapped_id = member_map.get(str(mem_id) + ".0", str(mem_id))
        member_ids.append(mapped_id)

    for i, row in enumerate(data):
        data[i]["MEMBER_ID"] = member_ids[i]

    # Update PARENT_MEMBER_ID
    parent_member_ids = []
    for row in data:
        parent_id = int(float(str(row["PARENT_MEMBER_ID"])))
        # Try both with and without .0 suffix for numeric member IDs
        mapped_id = member_map.get(str(parent_id), None)
        if mapped_id is None:
            mapped_id = member_map.get(str(parent_id) + ".0", str(parent_id))
        parent_member_ids.append(mapped_id)

    for i, row in enumerate(data):
        data[i]["PARENT_MEMBER_ID"] = parent_member_ids[i]

    # Update PARENT_HIERARCHY_ID
    parent_hierarchy_ids = []
    for row in data:
        parent_hierarchy_ids.append(hierarchy_map.get(str(row["PARENT_HIERARCHY_ID"]), str(row["PARENT_HIERARCHY_ID"])))

    for i, row in enumerate(data):
        data[i]["PARENT_HIERARCHY_ID"] = parent_hierarchy_ids[i]

    # Update HIERARCHY_ID
    hierarchy_ids = []
    for row in data:
        hierarchy_ids.append(hierarchy_map.get(str(row["HIERARCHY_ID"]), str(row["HIERARCHY_ID"])))

    for i, row in enumerate(data):
        data[i]["HIERARCHY_ID"] = hierarchy_ids[i]

    data = add_field(data, "MAINTENANCE_AGENCY_ID", "EBA")

    data = rename_fields(data, {
        "HIERARCHY_ID": "MEMBER_HIERARCHY_ID",
        "COMPARISON_OPERATOR": "COMPARATOR",
        "UNARY_OPERATOR": "OPERATOR"
    })

    data = add_field(data, "VALID_FROM", "1900-01-01")
    data = add_field(data, "VALID_TO", "9999-12-31")

    # Strip spaces from COMPARATOR and OPERATOR
    for i, row in enumerate(data):
        data[i]["COMPARATOR"] = str(row["COMPARATOR"]).strip()
        data[i]["OPERATOR"] = str(row["OPERATOR"]).strip()

    # Set default COMPARATOR
    for i, row in enumerate(data):
        if (str(row["COMPARATOR"]) == '' or str(row["COMPARATOR"]) == 'nan') and \
           (str(row["OPERATOR"]) == '' or str(row["OPERATOR"]) == 'nan'):
            data[i]["COMPARATOR"] = ">="

    data = select_fields(data, [
        "MEMBER_HIERARCHY_ID", "MEMBER_ID", "LEVEL", "PARENT_MEMBER_ID", "COMPARATOR", "OPERATOR", "VALID_FROM", "VALID_TO"
    ])

    return data, {}