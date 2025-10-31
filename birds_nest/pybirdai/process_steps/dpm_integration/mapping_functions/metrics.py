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
from pybirdai.process_steps.dpm_integration.mapping_functions.utils import (
    read_csv_to_dict, dict_list_to_structured_array, add_field,
    select_fields, rename_fields, pascal_to_upper_snake, clean_spaces
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

    # Create numpy structured array matching domain schema
    domains = dict_list_to_structured_array(data_type_domains)

    # Add required fields
    domains = add_field(domains, "MAINTENANCE_AGENCY_ID", "EBA")

    # Domain IDs are the same as CODE (already have EBA_ prefix)
    domain_ids = [str(row['CODE']) for row in domains]
    domains = add_field(domains, "DOMAIN_ID", domain_ids)

    # Add boolean fields
    domains = add_field(domains, "IS_ENUMERATED", False, dtype='bool')
    domains = add_field(domains, "FACET_ID", False, dtype='bool')
    domains = add_field(domains, "IS_REFERENCE", False, dtype='bool')

    # Select and order fields to match domain schema
    domains = select_fields(domains, [
        "MAINTENANCE_AGENCY_ID",
        "DOMAIN_ID",
        "NAME",
        "IS_ENUMERATED",
        "DESCRIPTION",
        "DATA_TYPE",
        "CODE",
        "FACET_ID",
        "IS_REFERENCE"
    ])

    # Create mapping dict (code -> domain_id)
    domain_map_dict = {}
    for row in domains:
        code = str(row["CODE"])
        domain_id = str(row["DOMAIN_ID"])
        domain_map_dict[code] = domain_id

    return domains, domain_map_dict


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

    # Read configuration CSV
    data = read_csv_to_dict(config_path)

    # Force domain_id to be string for mapping
    metrics = dict_list_to_structured_array(data, force_str_columns={'domain_id', 'subdomain_id'})

    # Transform column names to UPPER_SNAKE_CASE
    column_mapping = {col: pascal_to_upper_snake(col) for col in metrics.dtype.names}
    metrics = rename_fields(metrics, column_mapping)

    # Set maintenance agency ID
    metrics = add_field(metrics, "MAINTENANCE_AGENCY_ID", "EBA")

    # Map domain IDs using the data_type_domain_map we created
    # Configuration file already has EBA_ prefix, so direct lookup
    mapped_domain_ids = []
    for row in metrics:
        domain_name = str(row["DOMAIN_ID"])
        # Direct lookup - domain names in config already have EBA_ prefix
        mapped_domain = data_type_domain_map.get(domain_name, domain_name)
        mapped_domain_ids.append(mapped_domain)

    # Update DOMAIN_ID field with mapped values
    for i, row in enumerate(metrics):
        metrics[i]["DOMAIN_ID"] = mapped_domain_ids[i]

    # Create ID mapping for downstream use (code -> variable_id)
    id_mapping = {}
    for row in metrics:
        id_mapping[str(row["VARIABLE_CODE"])] = str(row["VARIABLE_ID"])

    # Rename fields to match variable schema
    metrics = rename_fields(metrics, {
        "MAINTENANCE_AGENCY_ID": "MAINTENANCE_AGENCY_ID",
        "VARIABLE_ID": "VARIABLE_ID",
        "VARIABLE_CODE": "CODE",
        "VARIABLE_NAME": "NAME"
    })

    # Add DESCRIPTION field (use NAME as description)
    descriptions = []
    for row in metrics:
        descriptions.append(str(row["NAME"]))
    metrics = add_field(metrics, "DESCRIPTION", descriptions)

    # Add required fields for variable schema
    metrics = add_field(metrics, "PRIMARY_CONCEPT", "")
    metrics = add_field(metrics, "IS_DECOMPOSED", False, dtype='bool')
    metrics = add_field(metrics, "IS_IMPLIED_IF_NOT_EXPLICITLY_MODELLED", False, dtype='bool')

    # Select and order final fields to match dimensions output
    metrics = select_fields(metrics, [
        "MAINTENANCE_AGENCY_ID",
        "VARIABLE_ID",
        "CODE",
        "NAME",
        "DOMAIN_ID",
        "DESCRIPTION",
        "PRIMARY_CONCEPT",
        "IS_DECOMPOSED",
        "IS_IMPLIED_IF_NOT_EXPLICITLY_MODELLED"
    ])

    # Clean spaces and special characters
    metrics = clean_spaces(metrics)

    return metrics, id_mapping, data_type_domains
