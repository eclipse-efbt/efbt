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


def load_table_to_framework_mapping(base_path="target"):
    """
    Load mapping from tables to frameworks using the correct chain:
    ReportingFramework → Taxonomy → TaxonomyTableVersion → Table

    Returns:
        dict: Mapping of TABLE_VID → FRAMEWORK_CODE (e.g., "12345" → "COREP")
    """
    # Load ReportingFramework.csv to get FrameworkID → FrameworkCode
    reporting_framework = pd.read_csv(os.path.join(base_path, "ReportingFramework.csv"), dtype=str)
    framework_id_to_code = dict(zip(
        reporting_framework['FrameworkID'].astype(str),
        reporting_framework['FrameworkCode'].astype(str)
    ))
    framework_id_to_code = normalize_id_map(framework_id_to_code)

    # Load Taxonomy.csv to get TAXONOMY_ID → FRAMEWORK_ID
    taxonomy = pd.read_csv(os.path.join(base_path, "Taxonomy.csv"), dtype=str)
    taxonomy.columns = [pascal_to_upper_snake(col) for col in taxonomy.columns]
    taxonomy_to_framework = dict(zip(
        taxonomy['TAXONOMY_ID'].astype(str),
        taxonomy['FRAMEWORK_ID'].astype(str)
    ))
    taxonomy_to_framework = normalize_id_map(taxonomy_to_framework)

    # Load TaxonomyTableVersion.csv to get TABLE_VID → TAXONOMY_ID
    taxonomy_table_version = pd.read_csv(os.path.join(base_path, "TaxonomyTableVersion.csv"), dtype=str)
    taxonomy_table_version.columns = [pascal_to_upper_snake(col) for col in taxonomy_table_version.columns]
    table_to_taxonomy = dict(zip(
        taxonomy_table_version['TABLE_VID'].astype(str),
        taxonomy_table_version['TAXONOMY_ID'].astype(str)
    ))
    table_to_taxonomy = normalize_id_map(table_to_taxonomy)

    # Build final mapping: TABLE_VID → FRAMEWORK_CODE
    table_to_framework = {}
    for table_vid, taxonomy_id in table_to_taxonomy.items():
        framework_id = taxonomy_to_framework.get(taxonomy_id)
        if framework_id:
            framework_code = framework_id_to_code.get(framework_id)
            if framework_code:
                table_to_framework[table_vid] = framework_code

    return table_to_framework


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


def map_tables(path=os.path.join("target", "Table.csv"), framework_id_map: dict = {}, frameworks=None, generate_framework_table=True):
    """
    Map tables from Table.csv to the target format.

    Args:
        path: Path to Table.csv
        framework_id_map: Dictionary mapping framework IDs
        frameworks: List of framework codes to filter (e.g., ['FINREP', 'COREP']).
                   If None, all frameworks are imported.
        generate_framework_table: If True, also generate framework_table junction data

    Returns:
        If generate_framework_table is True: (tables_df, id_mapping, framework_table_df)
        Otherwise: (tables_df, id_mapping)
    """
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

    # Load mappings using correct chain: ReportingFramework → Taxonomy → TaxonomyTableVersion → Table
    table_to_framework_mapping = load_table_to_framework_mapping()
    table_to_taxonomy_mapping = load_taxonomy_version_to_table_mapping()

    # Filter by frameworks if specified (before column transformation)
    if frameworks:
        # Find all TABLE_VIDs that belong to the specified frameworks
        valid_table_vids = [
            table_vid for table_vid, framework_code in table_to_framework_mapping.items()
            if framework_code in frameworks
        ]
        # Use original PascalCase column name 'TableVID' from CSV
        df = df[df['TableVID'].astype(str).isin(valid_table_vids)]

    # Transform column names to UPPER_SNAKE_CASE
    df.columns = [pascal_to_upper_snake(col) for col in df.columns]

    # Set maintenance agency ID
    df['MAINTENANCE_AGENCY_ID'] = "EBA"

    # Vectorized ID generation function (includes framework for uniqueness)
    def generate_table_id(row):
        table_vid = str(row["TABLE_VID"])
        # Get framework code from the correct mapping chain
        framework_code = table_to_framework_mapping.get(table_vid, "UNKNOWN")
        table_code = str(row["ORIGINAL_TABLE_CODE"]).replace(" ", "_")
        version = table_to_taxonomy_mapping.get(table_vid, "")
        new_id = f"EBA_{framework_code}_{table_code}_{version}".replace(".", "_")
        return new_id

    df['TABLE_ID'] = df.apply(generate_table_id, axis=1)

    # Generate NAME, CODE, and VERSION fields
    def generate_version(row):
        table_vid = str(row["TABLE_VID"])
        taxonomy = table_to_taxonomy_mapping.get(table_vid, "")
        version = taxonomy.replace(".", "_") if taxonomy else ""
        return version

    df['NAME'] = df['ORIGINAL_TABLE_CODE'].astype(str)
    df['CODE'] = df['ORIGINAL_TABLE_CODE'].astype(str).str.replace(" ", "_")
    df['VERSION'] = df.apply(generate_version, axis=1)

    # Filter by version: only keep tables with VERSION starting with "4_" (semantic versioning)
    if frameworks:
        df = df[df['VERSION'].astype(str).str.startswith('4_')]

    # Create ID mapping after all filtering is complete
    id_mapping = dict(zip(df['TABLE_VID'].astype(str), df['TABLE_ID'].astype(str)))

    # Rename columns
    df = df.rename(columns={
        "ORIGINAL_TABLE_LABEL": "DESCRIPTION",
        "FROM_DATE": "VALID_FROM",
        "TO_DATE": "VALID_TO"
    })

    # Select final columns (TABLE_VID excluded - Phase B will use JSON mapping instead)
    df = df[[
        "TABLE_ID", "NAME", "CODE", "DESCRIPTION", "MAINTENANCE_AGENCY_ID", "VERSION", "VALID_FROM", "VALID_TO"
    ]]

    # Clean text fields
    df = clean_spaces_df(df)

    if generate_framework_table:
        # Generate framework_table junction data
        framework_table_data = []
        for table_vid, table_id in id_mapping.items():
            framework_code = table_to_framework_mapping.get(table_vid)
            if framework_code:
                # Use the mapped framework ID (e.g., "EBA_FINREP")
                framework_id = framework_id_map.get(framework_code, f"EBA_{framework_code}")
                framework_table_data.append({
                    "FRAMEWORK_ID": framework_id,
                    "TABLE_ID": table_id
                })

        framework_table_df = pd.DataFrame(framework_table_data)
        return df, id_mapping, framework_table_df

    return df, id_mapping
