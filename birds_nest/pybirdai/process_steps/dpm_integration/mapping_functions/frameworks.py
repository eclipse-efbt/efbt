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
    select_fields, clean_spaces
)


def map_frameworks(path=os.path.join("target", "ReportingFramework.csv")):
    """Map frameworks from ReportingFramework.csv to the target format"""
    data = read_csv_to_dict(path)
    frameworks = dict_list_to_structured_array(data)

    framework_columns = [
        "MAINTENANCE_AGENCY_ID", "FRAMEWORK_ID", "NAME", "CODE", "DESCRIPTION",
        "FRAMEWORK_TYPE", "REPORTING_POPULATION", "OTHER_LINKS", "ORDER", "FRAMEWORK_STATUS"
    ]

    # Add new fields
    frameworks = add_field(frameworks, "MAINTENANCE_AGENCY_ID", "EBA")

    # Create FRAMEWORK_ID
    framework_ids = []
    codes = []
    names = []
    for row in frameworks:
        framework_ids.append("EBA_" + str(row["FrameworkCode"]))
        codes.append(str(row["FrameworkCode"]))
        names.append(str(row["FrameworkLabel"]))

    frameworks = add_field(frameworks, "FRAMEWORK_ID", framework_ids)
    frameworks = add_field(frameworks, "CODE", codes)
    frameworks = add_field(frameworks, "NAME", names)
    frameworks = add_field(frameworks, "FRAMEWORK_STATUS", "PUBLISHED")

    # Add empty fields
    for col in framework_columns:
        if col not in frameworks.dtype.names:
            frameworks = add_field(frameworks, col, "")

    # Create mapping before dropping
    framework_id_mapping = {}
    for row in frameworks:
        framework_id_mapping[str(row["FrameworkID"])] = str(row["FRAMEWORK_ID"])

    # Drop unwanted columns
    frameworks = drop_fields(frameworks, ["ConceptID", "FrameworkCode", "FrameworkLabel", "FrameworkID"])

    # Select only required columns
    frameworks = select_fields(frameworks, framework_columns)

    # Clean text fields
    frameworks = clean_spaces(frameworks)

    return frameworks, framework_id_mapping