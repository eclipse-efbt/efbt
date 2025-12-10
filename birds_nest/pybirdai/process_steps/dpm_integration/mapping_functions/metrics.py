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
    pascal_to_upper_snake, clean_spaces_df
)


def _create_data_type_domains():
    """
    Create domain records for the 6 data types used by metrics.

    These domains (Boolean, Date, Float, Integer, String, FRQNCY) are not present
    in the source DPM data, so we create them here to support metrics mapping.

    Returns:
        tuple: (domains_array, domain_map_dict)
    """
    # Define the 6 data type domains with their properties
    # DATA_TYPE codes: 1=Integer, 2=String, 3=Float, 4=Date, 5=Boolean, 6=FRQNCY
    # Note: CODE already includes EBA_ prefix to match configuration file
    data_type_domains = [
        {"CODE": "EBA_Boolean", "NAME": "Boolean", "DATA_TYPE": 5, "DESCRIPTION": "Boolean data type"},
        {"CODE": "EBA_Date", "NAME": "Date", "DATA_TYPE": 4, "DESCRIPTION": "Date data type"},
        {"CODE": "EBA_Float", "NAME": "Float", "DATA_TYPE": 3, "DESCRIPTION": "Float data type"},
        {"CODE": "EBA_Integer", "NAME": "Integer", "DATA_TYPE": 1, "DESCRIPTION": "Integer data type"},
        {"CODE": "EBA_String", "NAME": "String", "DATA_TYPE": 2, "DESCRIPTION": "String data type"},
        {"CODE": "EBA_FRQNCY", "NAME": "FRQNCY", "DATA_TYPE": 2, "DESCRIPTION": "Frequency data type"},
    ]

    # Create DataFrame directly
    df = pd.DataFrame(data_type_domains)

    # Add required fields
    df['MAINTENANCE_AGENCY_ID'] = "EBA"
    df['DOMAIN_ID'] = df['CODE']  # Domain IDs are the same as CODE
    df['IS_ENUMERATED'] = False
    df['FACET_ID'] = False
    df['IS_REFERENCE'] = False

    # Select and order fields to match domain schema
    df = df[[
        "MAINTENANCE_AGENCY_ID",
        "DOMAIN_ID",
        "NAME",
        "IS_ENUMERATED",
        "DESCRIPTION",
        "DATA_TYPE",
        "CODE",
        "FACET_ID",
        "IS_REFERENCE"
    ]]

    # Create mapping dict (code -> domain_id)
    domain_map_dict = dict(zip(df['CODE'].astype(str), df['DOMAIN_ID'].astype(str)))

    return df, domain_map_dict


def map_metrics(
    config_path=os.path.join("resources", "dpm_metrics_configuration", "configuration_dpm_measure_domain.csv"),
    domain_map: dict = {}
):
    """
    Map metrics from configuration CSV to variable records.

    This creates variable records for metrics based on the configuration file.
    Metrics are mapped from EBA_ATY domain members to proper variable records
    with appropriate data type domains (Boolean, Date, Float, Integer, String, FRQNCY).

    Also creates the 6 data type domain records required by the metrics.

    Args:
        config_path: Path to configuration CSV file
        domain_map: Mapping of original domain IDs/names to transformed IDs (not used, we create our own)

    Returns:
        tuple: (metrics_array, metrics_map, data_type_domains_array)
            - metrics_array: Variable records for metrics
            - metrics_map: Mapping of codes to variable IDs
            - data_type_domains_array: Domain records for Boolean, Date, Float, Integer, String, FRQNCY
    """
    # Create data type domains first
    data_type_domains, data_type_domain_map = _create_data_type_domains()

    # Read configuration CSV directly to DataFrame
    df = pd.read_csv(config_path, dtype=str)

    # Transform column names to UPPER_SNAKE_CASE
    df.columns = [pascal_to_upper_snake(col) for col in df.columns]

    # Map domain IDs using the data_type_domain_map
    # Configuration file already has EBA_ prefix, so direct lookup
    df['DOMAIN_ID'] = df['DOMAIN_ID'].astype(str).map(data_type_domain_map).fillna(df['DOMAIN_ID'])

    # Create ID mapping for downstream use (code -> variable_id)
    id_mapping = dict(zip(df['VARIABLE_CODE'].astype(str), df['VARIABLE_ID'].astype(str)))

    # Rename fields to match variable schema
    df = df.rename(columns={
        "VARIABLE_CODE": "CODE",
        "VARIABLE_NAME": "NAME"
    })

    # Add fields
    df['MAINTENANCE_AGENCY_ID'] = "EBA"
    df['DESCRIPTION'] = df['NAME']  # Use NAME as description
    df['PRIMARY_CONCEPT'] = ""
    df['IS_DECOMPOSED'] = False
    df['IS_IMPLIED_IF_NOT_EXPLICITLY_MODELLED'] = False

    # Select and order final fields
    df = df[[
        "MAINTENANCE_AGENCY_ID",
        "VARIABLE_ID",
        "CODE",
        "NAME",
        "DOMAIN_ID",
        "DESCRIPTION",
        "PRIMARY_CONCEPT",
        "IS_DECOMPOSED",
        "IS_IMPLIED_IF_NOT_EXPLICITLY_MODELLED"
    ]]

    # Clean spaces and special characters
    df = clean_spaces_df(df)

    return df, id_mapping, data_type_domains
