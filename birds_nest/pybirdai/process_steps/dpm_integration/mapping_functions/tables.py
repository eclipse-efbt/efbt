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
    select_fields, rename_fields, pascal_to_upper_snake, merge_arrays,
    array_to_dict, clean_spaces
)


def load_template_to_framework_mapping(base_path="target"):
    """Load mapping from templates to frameworks"""
    template_group_data = read_csv_to_dict(os.path.join(base_path, "TemplateGroup.csv"))
    template_group = dict_list_to_structured_array(template_group_data, ["FrameworkID", "TemplateGroupID"])

    template_group_template_data = read_csv_to_dict(os.path.join(base_path, "TemplateGroupTemplate.csv"))
    template_group_template = dict_list_to_structured_array(template_group_template_data, ["TemplateID", "TemplateGroupID"])

    merged = merge_arrays(template_group, template_group_template, "TemplateGroupID")
    merged = select_fields(merged, ["TemplateID", "FrameworkID"])

    column_mapping = {col: pascal_to_upper_snake(col) for col in merged.dtype.names}
    merged = rename_fields(merged, column_mapping)

    result = array_to_dict(merged, "TEMPLATE_ID", "FRAMEWORK_ID")
    return result


def load_taxonomy_version_to_table_mapping(base_path="target"):
    """Load mapping from taxonomy versions to tables"""
    taxonomy_to_table_data = read_csv_to_dict(os.path.join(base_path, "TaxonomyTableVersion.csv"))
    taxonomy_to_table_version = dict_list_to_structured_array(taxonomy_to_table_data, ["TaxonomyID", "TableVID"])

    taxonomy_to_package_data = read_csv_to_dict(os.path.join(base_path, "Taxonomy.csv"))
    taxonomy_to_package_version = dict_list_to_structured_array(taxonomy_to_package_data, ["TaxonomyID", "DpmPackageCode"])

    merged = merge_arrays(taxonomy_to_table_version, taxonomy_to_package_version, "TaxonomyID")
    merged = select_fields(merged, ["TableVID", "DpmPackageCode"])

    column_mapping = {col: pascal_to_upper_snake(col) for col in merged.dtype.names}
    merged = rename_fields(merged, column_mapping)

    result = array_to_dict(merged, "TABLE_VID", "DPM_PACKAGE_CODE")
    return result


def map_tables(path=os.path.join("target", "Table.csv"), framework_id_map: dict = {}):
    """Map tables from Table.csv to the target format"""
    data = read_csv_to_dict(path)
    # Remove ConceptID during reading
    for row in data:
        if "ConceptID" in row:
            del row["ConceptID"]
    # Force ID fields to be strings since they will be mapped to string values
    tables = dict_list_to_structured_array(data, force_str_columns={'TemplateID'})

    # Get directory and create proper path for TableVersion.csv
    path_dir = os.path.dirname(path)
    tables_versions_path = os.path.join(path_dir, "TableVersion.csv")
    tables_versions_data = read_csv_to_dict(tables_versions_path)
    for row in tables_versions_data:
        if "ConceptID" in row:
            del row["ConceptID"]
    # Force ID fields to be strings since they will be mapped to string values
    tables_versions = dict_list_to_structured_array(tables_versions_data, force_str_columns={'TableVID'})

    tables = merge_arrays(tables, tables_versions, "TableID", force_str_columns={'TemplateID', 'TableVID'})
    template_to_framework_mapping = load_template_to_framework_mapping()
    table_to_taxonomy_mapping = load_taxonomy_version_to_table_mapping()

    # Transform column names to UPPER_SNAKE_CASE
    column_mapping = {col: pascal_to_upper_snake(col) for col in tables.dtype.names}
    tables = rename_fields(tables, column_mapping)

    # Set maintenance agency ID and create new table ID
    tables = add_field(tables, "MAINTENANCE_AGENCY_ID", "EBA")

    new_table_ids = []
    for row in tables:
        template_id = str(row["TEMPLATE_ID"])

        # Try both with and without .0 suffix for numeric template IDs
        original_framework_id = template_to_framework_mapping.get(template_id, None)
        if original_framework_id is None:
            original_framework_id = template_to_framework_mapping.get(template_id + ".0", template_id)

        # Try both with and without .0 suffix for framework mapping
        framework_id = framework_id_map.get(original_framework_id, None)
        if framework_id is None:
            framework_id = framework_id_map.get(original_framework_id + ".0", original_framework_id)

        # Extract framework code from mapped framework ID (e.g., "EBA_AE" -> "AE")
        framework_code = framework_id.replace("EBA_", "") if framework_id.startswith("EBA_") else framework_id

        # Get table code and clean it
        table_code = str(row["ORIGINAL_TABLE_CODE"]).replace(" ", "_")

        # Get version from taxonomy mapping
        table_vid = str(row["TABLE_VID"])
        # Try both with and without .0 suffix for taxonomy mapping
        version = table_to_taxonomy_mapping.get(table_vid, None)
        if version is None:
            version = table_to_taxonomy_mapping.get(table_vid + ".0", "")
        else:
            version = str(version)

        # Create new ID format: EBA_{Framework_Code}_EBA_{Template_Code}_{Framework_Code}_{Version}
        new_id = f"EBA_{framework_code}_EBA_{table_code}_{framework_code}_{version}".replace(".", "_")
        new_table_ids.append(new_id)

    tables = add_field(tables, "NEW_TABLE_ID", new_table_ids)

    # Create ID mapping
    id_mapping = {}
    for row in tables:
        id_mapping[str(row["TABLE_VID"])] = str(row["NEW_TABLE_ID"])

    tables = drop_fields(tables, "TABLE_ID")

    tables = rename_fields(tables, {
        "NEW_TABLE_ID": "TABLE_ID",
        "ORIGINAL_TABLE_LABEL": "DESCRIPTION",
        "FROM_DATE": "VALID_FROM",
        "TO_DATE": "VALID_TO"
    })

    # Add NAME and CODE fields
    names = []
    codes = []
    versions = []
    for row in tables:
        names.append(str(row["ORIGINAL_TABLE_CODE"]))
        codes.append(str(row["ORIGINAL_TABLE_CODE"]).replace(" ", "_"))
        template_id = str(row["TEMPLATE_ID"])

        # Apply same float/integer key handling as above
        original_framework_id = template_to_framework_mapping.get(template_id, None)
        if original_framework_id is None:
            original_framework_id = template_to_framework_mapping.get(template_id + ".0", template_id)

        framework_id = framework_id_map.get(original_framework_id, None)
        if framework_id is None:
            framework_id = framework_id_map.get(original_framework_id + ".0", original_framework_id)

        framework_code = framework_id.replace("EBA_", "") if framework_id.startswith("EBA_") else framework_id
        table_vid = str(row["TABLE_VID"])

        # Apply same float/integer key handling for taxonomy mapping
        taxonomy = table_to_taxonomy_mapping.get(table_vid, None)
        if taxonomy is None:
            taxonomy = table_to_taxonomy_mapping.get(table_vid + ".0", "")
        else:
            taxonomy = str(taxonomy)

        version = f"{framework_code}_{taxonomy}".replace(".", "_")
        versions.append(version)

    tables = add_field(tables, "NAME", names)
    tables = add_field(tables, "CODE", codes)
    tables = add_field(tables, "VERSION", versions)

    tables = select_fields(tables, [
        "TABLE_ID", "NAME", "CODE", "DESCRIPTION", "MAINTENANCE_AGENCY_ID", "VERSION", "VALID_FROM", "VALID_TO"
    ])

    # Clean text fields
    tables = clean_spaces(tables)

    return tables, id_mapping