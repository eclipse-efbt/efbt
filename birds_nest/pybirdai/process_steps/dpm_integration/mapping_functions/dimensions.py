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


def map_dimensions(path=os.path.join("target", "Dimension.csv"), domain_id_map: dict = {}):
    """Map dimensions from Dimension.csv to the target format"""
    data = read_csv_to_dict(path)
    # Force DomainID to be string since it will be mapped to string values
    dimensions = dict_list_to_structured_array(data, force_str_columns={'DomainID'})

    # Transform column names to UPPER_SNAKE_CASE
    column_mapping = {col: pascal_to_upper_snake(col) for col in dimensions.dtype.names}
    dimensions = rename_fields(dimensions, column_mapping)

    # Set maintenance agency ID and create new dimension ID
    dimensions = add_field(dimensions, "MAINTENANCE_AGENCY_ID", "EBA")

    new_dimension_ids = []
    new_domain_ids = []
    for row in dimensions:
        new_dimension_ids.append("EBA_" + str(row["DIMENSION_CODE"]))
        new_domain_ids.append(domain_id_map.get(str(row["DOMAIN_ID"]), str(row["DOMAIN_ID"])))

    dimensions = add_field(dimensions, "NEW_DIMENSION_ID", new_dimension_ids)

    # Update DOMAIN_ID
    for i, row in enumerate(dimensions):
        dimensions[i]["DOMAIN_ID"] = new_domain_ids[i]

    # Create ID mapping
    id_mapping = {}
    for row in dimensions:
        id_mapping[str(row["DIMENSION_ID"])] = str(row["NEW_DIMENSION_ID"])

    dimensions = rename_fields(dimensions, {
        "MAINTENANCE_AGENCY_ID": "MAINTENANCE_AGENCY_ID",
        "NEW_DIMENSION_ID": "VARIABLE_ID",
        "DIMENSION_CODE": "CODE",
        "DIMENSION_LABEL": "NAME",
        "DIMENSION_DESCRIPTION": "DESCRIPTION"
    })

    dimensions = add_field(dimensions, "PRIMARY_CONCEPT", "")
    dimensions = add_field(dimensions, "IS_DECOMPOSED", False, dtype='bool')

    # Convert IS_IMPLIED_IF_NOT_EXPLICITLY_MODELLED to bool
    is_implied = []
    for row in dimensions:
        if "IS_IMPLIED_IF_NOT_EXPLICITLY_MODELLED" in dimensions.dtype.names:
            val = str(row["IS_IMPLIED_IF_NOT_EXPLICITLY_MODELLED"])
        else:
            val = "False"
        is_implied.append(val.lower() in ['true', '1', 'yes'])

    dimensions = add_field(dimensions, "IS_IMPLIED_BOOL", is_implied, dtype='bool')
    dimensions = drop_fields(dimensions, "IS_IMPLIED_IF_NOT_EXPLICITLY_MODELLED")
    dimensions = rename_fields(dimensions, {"IS_IMPLIED_BOOL": "IS_IMPLIED_IF_NOT_EXPLICITLY_MODELLED"})

    dimensions = select_fields(dimensions, [
        "MAINTENANCE_AGENCY_ID", "VARIABLE_ID", "CODE", "NAME", "DOMAIN_ID", "DESCRIPTION", "PRIMARY_CONCEPT", "IS_DECOMPOSED", "IS_IMPLIED_IF_NOT_EXPLICITLY_MODELLED"
    ])

    dimensions = clean_spaces(dimensions)

    return dimensions, id_mapping