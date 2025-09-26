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
    select_fields, rename_fields, pascal_to_upper_snake, clean_spaces
)


def map_members(path=os.path.join("target", "Member.csv"), domain_id_map: dict = {}):
    """Map members from Member.csv to the target format"""
    data = read_csv_to_dict(path)
    # Force DomainID to be string since it will be mapped to string values
    members = dict_list_to_structured_array(data, force_str_columns={'DomainID'})

    # Transform column names to UPPER_SNAKE_CASE
    column_mapping = {col: pascal_to_upper_snake(col) for col in members.dtype.names}
    members = rename_fields(members, column_mapping)

    # Set maintenance agency ID and create new member ID
    members = add_field(members, "MAINTENANCE_AGENCY_ID", "EBA")

    new_member_ids = []
    new_domain_ids = []
    for row in members:
        original_domain_id = str(row["DOMAIN_ID"]).strip()
        if original_domain_id and original_domain_id != "":
            # Try both with and without .0 suffix for numeric domain IDs
            domain_id = domain_id_map.get(original_domain_id, None)
            if domain_id is None:
                domain_id = domain_id_map.get(original_domain_id + ".0", original_domain_id)
            new_member_ids.append(domain_id + "_EBA_" + str(row["MEMBER_CODE"]))
            new_domain_ids.append(domain_id)
        else:
            # Handle empty domain ID - use the member code directly
            new_member_ids.append("EBA_" + str(row["MEMBER_CODE"]))
            new_domain_ids.append("")

    members = add_field(members, "NEW_MEMBER_ID", new_member_ids)

    # Update DOMAIN_ID
    for i, row in enumerate(members):
        members[i]["DOMAIN_ID"] = new_domain_ids[i]

    # Create ID mapping
    id_mapping = {}
    for row in members:
        id_mapping[str(row["MEMBER_ID"])] = str(row["NEW_MEMBER_ID"])

    members = drop_fields(members, "MEMBER_ID")
    members = rename_fields(members, {
        "NEW_MEMBER_ID": "MEMBER_ID",
        "MEMBER_CODE": "CODE",
        "MEMBER_LABEL": "NAME",
        "MEMBER_DESCRIPTION": "DESCRIPTION",
    })

    # Filter out rows with empty MEMBER_ID
    mask = np.array([row["MEMBER_ID"] != '' and row["MEMBER_ID"] != 'nan' for row in members])
    members = members[mask]

    members = select_fields(members, [
        "MAINTENANCE_AGENCY_ID", "MEMBER_ID", "CODE", "NAME", "DOMAIN_ID", "DESCRIPTION"
    ])

    members = clean_spaces(members)

    return members, id_mapping