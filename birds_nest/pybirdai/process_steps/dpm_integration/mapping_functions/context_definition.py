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
    read_csv_to_dict, dict_list_to_structured_array, add_field,
    rename_fields, pascal_to_upper_snake
)


def map_context_definition(path=os.path.join("target", "ContextDefinition.csv"), dimension_map: dict = {}, member_map: dict = {}):
    """Map context definitions from ContextDefinition.csv to the target format"""
    types = defaultdict(lambda: str, ContextID="str")
    data_list = read_csv_to_dict(path)
    # Force ID fields to be strings since they will be mapped to string values
    data = dict_list_to_structured_array(data_list, force_str_columns={'DimensionID', 'MemberID'})

    column_mapping = {col: pascal_to_upper_snake(col) for col in data.dtype.names}
    data = rename_fields(data, column_mapping)
    data = add_field(data, "MAINTENANCE_AGENCY_ID", "EBA")

    # Update DIMENSION_ID
    dimension_ids = []
    for row in data:
        dim_id = int(float(str(row["DIMENSION_ID"])))
        dimension_ids.append(dimension_map.get(str(dim_id), str(dim_id)))

    for i, row in enumerate(data):
        data[i]["DIMENSION_ID"] = dimension_ids[i]

    # Update MEMBER_ID
    member_ids = []
    for row in data:
        mem_id = int(float(str(row["MEMBER_ID"])))
        member_ids.append(member_map.get(str(mem_id), str(mem_id)))

    for i, row in enumerate(data):
        data[i]["MEMBER_ID"] = member_ids[i]

    return data, {}