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
import pandas as pd
from pybirdai.process_steps.dpm_integration.mapping_functions.utils import (
    pascal_to_upper_snake, clean_spaces_df, normalize_id_map
)


def load_template_to_framework_mapping(base_path="target"):
    """Load mapping from templates to frameworks"""
    template_group = pd.read_csv(os.path.join(base_path, "TemplateGroup.csv"), dtype=str)
    template_group_template = pd.read_csv(os.path.join(base_path, "TemplateGroupTemplate.csv"), dtype=str)

    # Transform column names to UPPER_SNAKE_CASE
    template_group.columns = [pascal_to_upper_snake(col) for col in template_group.columns]
    template_group_template.columns = [pascal_to_upper_snake(col) for col in template_group_template.columns]

    # Merge on TEMPLATE_GROUP_ID
    merged = template_group_template.merge(template_group, on="TEMPLATE_GROUP_ID", how="left")

    # Create mapping dictionary with .0 suffix handling
    result = dict(zip(merged['TEMPLATE_ID'].astype(str), merged['FRAMEWORK_ID'].astype(str)))
    return normalize_id_map(result)


def load_taxonomy_version_to_table_mapping(base_path="target"):
    """Load mapping from taxonomy versions to tables"""
    taxonomy_to_table_version = pd.read_csv(os.path.join(base_path, "TaxonomyTableVersion.csv"), dtype=str)
    taxonomy_to_package_version = pd.read_csv(os.path.join(base_path, "Taxonomy.csv"), dtype=str)

    # Transform column names to UPPER_SNAKE_CASE
    taxonomy_to_table_version.columns = [pascal_to_upper_snake(col) for col in taxonomy_to_table_version.columns]
    taxonomy_to_package_version.columns = [pascal_to_upper_snake(col) for col in taxonomy_to_package_version.columns]

    # Merge on TAXONOMY_ID
    merged = taxonomy_to_table_version.merge(taxonomy_to_package_version, on="TAXONOMY_ID", how="left")

    # Create mapping dictionary with .0 suffix handling
    result = dict(zip(merged['TABLE_VID'].astype(str), merged['DPM_PACKAGE_CODE'].astype(str)))
    return normalize_id_map(result)


def map_tables(path=os.path.join("target", "Table.csv"), framework_id_map: dict = {}):
    """Map tables from Table.csv to the target format"""
    # Read tables and table versions
    df = pd.read_csv(path, dtype=str)
    if 'ConceptID' in df.columns:
        df = df.drop(columns=['ConceptID'])

    path_dir = os.path.dirname(path)
    tables_versions_path = os.path.join(path_dir, "TableVersion.csv")
    df_versions = pd.read_csv(tables_versions_path, dtype=str)
    if 'ConceptID' in df_versions.columns:
        df_versions = df_versions.drop(columns=['ConceptID'])

    # Merge tables with versions
    df = df.merge(df_versions, on="TableID", how="left")

    # Load mappings
    template_to_framework_mapping = load_template_to_framework_mapping()
    table_to_taxonomy_mapping = load_taxonomy_version_to_table_mapping()

    # Transform column names to UPPER_SNAKE_CASE
    df.columns = [pascal_to_upper_snake(col) for col in df.columns]

    # Set maintenance agency ID
    df['MAINTENANCE_AGENCY_ID'] = "EBA"

    # Normalize framework_id_map for .0 suffix handling
    framework_id_map_norm = normalize_id_map(framework_id_map)

    # Vectorized ID generation function
    def generate_table_id(row):
        template_id = str(row["TEMPLATE_ID"])
        original_framework_id = template_to_framework_mapping.get(template_id, template_id)
        framework_id = framework_id_map_norm.get(original_framework_id, original_framework_id)
        framework_code = framework_id.replace("EBA_", "") if framework_id.startswith("EBA_") else framework_id
        table_code = str(row["ORIGINAL_TABLE_CODE"]).replace(" ", "_")
        table_vid = str(row["TABLE_VID"])
        version = table_to_taxonomy_mapping.get(table_vid, "")
        new_id = f"EBA_{framework_code}_EBA_{table_code}_{framework_code}_{version}".replace(".", "_")
        return new_id

    df['TABLE_ID'] = df.apply(generate_table_id, axis=1)

    # Create ID mapping
    id_mapping = dict(zip(df['TABLE_VID'].astype(str), df['TABLE_ID'].astype(str)))

    # Generate NAME, CODE, and VERSION fields
    def generate_version(row):
        template_id = str(row["TEMPLATE_ID"])
        original_framework_id = template_to_framework_mapping.get(template_id, template_id)
        framework_id = framework_id_map_norm.get(original_framework_id, original_framework_id)
        framework_code = framework_id.replace("EBA_", "") if framework_id.startswith("EBA_") else framework_id
        table_vid = str(row["TABLE_VID"])
        taxonomy = table_to_taxonomy_mapping.get(table_vid, "")
        version = f"{framework_code}_{taxonomy}".replace(".", "_")
        return version

    df['NAME'] = df['ORIGINAL_TABLE_CODE'].astype(str)
    df['CODE'] = df['ORIGINAL_TABLE_CODE'].astype(str).str.replace(" ", "_")
    df['VERSION'] = df.apply(generate_version, axis=1)

    # Rename columns
    df = df.rename(columns={
        "ORIGINAL_TABLE_LABEL": "DESCRIPTION",
        "FROM_DATE": "VALID_FROM",
        "TO_DATE": "VALID_TO"
    })

    # Select final columns
    df = df[[
        "TABLE_ID", "NAME", "CODE", "DESCRIPTION", "MAINTENANCE_AGENCY_ID", "VERSION", "VALID_FROM", "VALID_TO"
    ]]

    # Clean text fields
    df = clean_spaces_df(df)

    return df, id_mapping
