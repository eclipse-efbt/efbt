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


def map_domains(path=os.path.join("target", "Domain.csv")):
    """Map domains from Domain.csv to the target format"""
    data = read_csv_to_dict(path)
    domains = dict_list_to_structured_array(data)

    # Transform column names to UPPER_SNAKE_CASE
    column_mapping = {col: pascal_to_upper_snake(col) for col in domains.dtype.names}
    domains = rename_fields(domains, column_mapping)

    # Set maintenance agency ID and create new domain ID
    domains = add_field(domains, "MAINTENANCE_AGENCY_ID", "EBA")

    new_domain_ids = []
    for row in domains:
        new_domain_ids.append("EBA_" + str(row["DOMAIN_CODE"]))
    domains = add_field(domains, "NEW_DOMAIN_ID", new_domain_ids)

    # Create ID mapping
    id_mapping = {}
    for row in domains:
        id_mapping[str(row["DOMAIN_ID"])] = str(row["NEW_DOMAIN_ID"])

    domains = drop_fields(domains, "DOMAIN_ID")

    domains = rename_fields(domains, {
        "NEW_DOMAIN_ID": "DOMAIN_ID",
        "DOMAIN_CODE": "CODE",
        "DOMAIN_LABEL": "NAME",
        "DOMAIN_DESCRIPTION": "DESCRIPTION",
        "DATA_TYPE_ID": "DATA_TYPE",
    })

    domains = clean_spaces(domains)

    domains = add_field(domains, "FACET_ID", False, dtype='bool')
    domains = add_field(domains, "IS_REFERENCE", False, dtype='bool')
    domains = add_field(domains, "IS_ENUMERATED", False, dtype='bool')

    domains = select_fields(domains, [
        "MAINTENANCE_AGENCY_ID", "DOMAIN_ID", "NAME", "IS_ENUMERATED", "DESCRIPTION", "DATA_TYPE", "CODE", "FACET_ID", "IS_REFERENCE"
    ])

    return domains, id_mapping