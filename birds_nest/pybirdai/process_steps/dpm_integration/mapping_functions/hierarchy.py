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


def map_hierarchy(path=os.path.join("target", "Hierarchy.csv"), domain_id_map: dict = {}):
    """Map hierarchies from Hierarchy.csv to the target format"""
    data_list = read_csv_to_dict(path)
    # Force DomainID to be string since it will be mapped to string values
    hierarchies = dict_list_to_structured_array(data_list, force_str_columns={'DomainID'})

    column_mapping = {col: pascal_to_upper_snake(col) for col in hierarchies.dtype.names}
    hierarchies = rename_fields(hierarchies, column_mapping)
    hierarchies = add_field(hierarchies, "MAINTENANCE_AGENCY_ID", "EBA")

    new_hierarchy_ids = []
    for row in hierarchies:
        new_hierarchy_ids.append("EBA_" + str(row["HIERARCHY_CODE"]))

    hierarchies = add_field(hierarchies, "NEW_HIERARCHY_ID", new_hierarchy_ids)

    # Update DOMAIN_ID
    domain_ids = []
    for row in hierarchies:
        domain_ids.append(domain_id_map.get(str(row["DOMAIN_ID"]), str(row["DOMAIN_ID"])))

    for i, row in enumerate(hierarchies):
        hierarchies[i]["DOMAIN_ID"] = domain_ids[i]

    # Generate id mapping
    id_mapping = {}
    for row in hierarchies:
        id_mapping[str(row["HIERARCHY_ID"])] = str(row["NEW_HIERARCHY_ID"])

    hierarchies = rename_fields(hierarchies, {
        "NEW_HIERARCHY_ID": "MEMBER_HIERARCHY_ID",
        "HIERARCHY_CODE": "CODE",
        "HIERARCHY_LABEL": "NAME",
        "HIERARCHY_DESCRIPTION": "DESCRIPTION"
    })

    hierarchies = add_field(hierarchies, "IS_MAIN_HIERARCHY", False, dtype='bool')

    hierarchies = select_fields(hierarchies, [
        "MAINTENANCE_AGENCY_ID", "MEMBER_HIERARCHY_ID", "CODE", "DOMAIN_ID", "NAME", "DESCRIPTION", "IS_MAIN_HIERARCHY"
    ])

    # Clean text fields
    hierarchies = clean_spaces(hierarchies)

    return hierarchies, id_mapping