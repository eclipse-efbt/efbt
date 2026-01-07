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
"""
Column index mappings for CSV data import.

Provides mapping between CSV column indices and Django model field names.
"""
import logging

from pybirdai.utils.clone_mode.clone_mode_column_index import ColumnIndexes

logger = logging.getLogger(__name__)


def build_column_mappings() -> dict:
    """
    Build column index mappings for each model type.

    Returns:
        dict: Mapping of table names to {column_index: field_name} dictionaries
    """
    col_idx = ColumnIndexes()
    column_mappings = {}

    # Maintenance Agency mappings
    column_mappings['pybirdai_maintenance_agency'] = {
        col_idx.maintenance_agency_id: 'maintenance_agency_id',
        col_idx.maintenance_agency_code: 'code',
        col_idx.maintenance_agency_name: 'name',
        col_idx.maintenance_agency_description: 'description'
    }

    # Framework mappings
    column_mappings['pybirdai_framework'] = {
        col_idx.framework_maintenance_agency_id: 'maintenance_agency_id',
        col_idx.framework_id: 'framework_id',
        col_idx.framework_name: 'name',
        col_idx.framework_code: 'code',
        col_idx.framework_description: 'description',
        col_idx.framework_type: 'framework_type',
        col_idx.framework_reporting_population: 'reporting_population',
        col_idx.framework_other_links: 'other_links',
        col_idx.framework_order: 'order',
        col_idx.framework_status: 'status'
    }

    # Domain mappings
    column_mappings['pybirdai_domain'] = {
        col_idx.domain_maintenance_agency: 'maintenance_agency_id',
        col_idx.domain_domain_true_id: 'domain_id',
        col_idx.domain_domain_name_index: 'name',
        col_idx.domain_domain_is_enumerated: 'is_enumerated',
        col_idx.domain_domain_description: 'description',
        col_idx.domain_domain_data_type: 'data_type',
        col_idx.domain_code: 'code',
        col_idx.domain_facet_id: 'facet_id',
        col_idx.domain_domain_is_reference: 'is_reference'
    }

    # Variable mappings
    column_mappings['pybirdai_variable'] = {
        col_idx.variable_maintenance_agency: 'maintenance_agency_id',
        col_idx.variable_variable_true_id: 'variable_id',
        col_idx.variable_variable_name_index: 'name',
        col_idx.variable_code_index: 'code',
        col_idx.variable_domain_index: 'domain_id',
        col_idx.variable_variable_description: 'description',
        col_idx.variable_primary_concept: 'primary_concept',
        col_idx.variable_is_decomposed: 'is_decomposed'
    }

    # Member mappings
    column_mappings['pybirdai_member'] = {
        col_idx.member_maintenance_agency: 'maintenance_agency_id',
        col_idx.member_member_id_index: 'member_id',
        col_idx.member_member_code_index: 'code',
        col_idx.member_member_name_index: 'name',
        col_idx.member_domain_id_index: 'domain_id',
        col_idx.member_member_descriptions: 'description'
    }

    # Variable Set mappings
    column_mappings['pybirdai_variable_set'] = {
        col_idx.variable_set_maintenance_agency_id: 'maintenance_agency_id',
        col_idx.variable_set_variable_set_id: 'variable_set_id',
        col_idx.variable_set_name: 'name',
        col_idx.variable_set_code: 'code',
        col_idx.variable_set_description: 'description'
    }

    # Variable Set Enumeration mappings
    column_mappings['pybirdai_variable_set_enumeration'] = {
        col_idx.variable_set_enumeration_valid_set: 'variable_set_id',
        col_idx.variable_set_enumeration_variable_id: 'variable_id',
        col_idx.variable_set_enumeration_valid_from: 'valid_from',
        col_idx.variable_set_enumeration_valid_to: 'valid_to',
        col_idx.variable_set_enumeration_subdomain_id: 'subdomain_id',
        col_idx.variable_set_enumeration_is_flow: 'is_flow',
        col_idx.variable_set_enumeration_order: 'order'
    }

    # Subdomain mappings
    column_mappings['pybirdai_subdomain'] = {
        col_idx.subdomain_maintenance_agency_id: 'maintenance_agency_id',
        col_idx.subdomain_subdomain_id_index: 'subdomain_id',
        col_idx.subdomain_subdomain_name: 'name',
        col_idx.subdomain_domain_id_index: 'domain_id',
        col_idx.subdomain_is_listed: 'is_listed',
        col_idx.subdomain_subdomain_code: 'code',
        col_idx.subdomain_facet_id: 'facet_id',
        col_idx.subdomain_subdomain_description: 'description',
        col_idx.subdomain_is_natural: 'is_natural'
    }

    # Subdomain Enumeration mappings
    column_mappings['pybirdai_subdomain_enumeration'] = {
        col_idx.subdomain_enumeration_member_id_index: 'member_id',
        col_idx.subdomain_enumeration_subdomain_id_index: 'subdomain_id',
        col_idx.subdomain_enumeration_valid_from: 'valid_from',
        col_idx.subdomain_enumeration_valid_to_index: 'valid_to',
        col_idx.subdomain_enumeration_order: 'order'
    }

    # Member Hierarchy mappings
    column_mappings['pybirdai_member_hierarchy'] = {
        col_idx.member_hierarchy_maintenance_agency: 'maintenance_agency_id',
        col_idx.member_hierarchy_id: 'member_hierarchy_id',
        col_idx.member_hierarchy_code: 'code',
        col_idx.member_hierarchy_domain_id: 'domain_id',
        col_idx.member_hierarchy_name: 'name',
        col_idx.member_hierarchy_description: 'description',
        col_idx.member_hierarchy_is_main_hierarchy: 'is_main_hierarchy'
    }

    # Member Hierarchy Node mappings
    column_mappings['pybirdai_member_hierarchy_node'] = {
        col_idx.member_hierarchy_node_hierarchy_id: 'member_hierarchy_id',
        col_idx.member_hierarchy_node_member_id: 'member_id',
        col_idx.member_hierarchy_node_level: 'level',
        col_idx.member_hierarchy_node_parent_member_id: 'parent_member_id',
        col_idx.member_hierarchy_node_comparator: 'comparator',
        col_idx.member_hierarchy_node_operator: 'operator',
        col_idx.member_hierarchy_node_valid_from: 'valid_from',
        col_idx.member_hierarchy_node_valid_to: 'valid_to'
    }

    # Table mappings
    column_mappings['pybirdai_table'] = {
        col_idx.table_table_id: 'table_id',
        col_idx.table_table_name: 'name',
        col_idx.table_code: 'code',
        col_idx.table_description: 'description',
        col_idx.table_maintenance_agency_id: 'maintenance_agency_id',
        col_idx.table_version: 'version',
        col_idx.table_valid_from: 'valid_from',
        col_idx.table_valid_to: 'valid_to'
    }

    # Axis mappings
    column_mappings['pybirdai_axis'] = {
        col_idx.axis_id: 'axis_id',
        col_idx.axis_code: 'code',
        col_idx.axis_orientation: 'orientation',
        col_idx.axis_order: 'order',
        col_idx.axis_name: 'name',
        col_idx.axis_description: 'description',
        col_idx.axis_table_id: 'table_id',
        col_idx.axis_is_open_axis: 'is_open_axis'
    }

    # Axis Ordinate mappings
    column_mappings['pybirdai_axis_ordinate'] = {
        col_idx.axis_ordinate_axis_ordinate_id: 'axis_ordinate_id',
        col_idx.axis_ordinate_is_abstract_header: 'is_abstract_header',
        col_idx.axis_ordinate_code: 'code',
        col_idx.axis_ordinate_order: 'order',
        col_idx.axis_ordinate_level: 'level',
        col_idx.axis_ordinate_path: 'path',
        col_idx.axis_ordinate_axis_id: 'axis_id',
        col_idx.axis_ordinate_parent_axis_ordinate_id: 'parent_axis_ordinate_id',
        col_idx.axis_ordinate_name: 'name',
        col_idx.axis_ordinate_description: 'description'
    }

    # Ordinate Item mappings
    column_mappings['pybirdai_ordinate_item'] = {
        col_idx.ordinate_item_axis_ordinate_id: 'axis_ordinate_id',
        col_idx.ordinate_item_variable_id: 'variable_id',
        col_idx.ordinate_item_member_id: 'member_id',
        col_idx.ordinate_item_member_hierarchy_id: 'member_hierarchy_id',
        col_idx.ordinate_item_member_hierarchy_valid_from: 'member_hierarchy_valid_from',
        col_idx.ordinate_item_starting_member_id: 'starting_member_id',
        col_idx.ordinate_item_is_starting_member_included: 'is_starting_member_included'
    }

    # Table Cell mappings
    column_mappings['pybirdai_table_cell'] = {
        col_idx.table_cell_cell_id: 'cell_id',
        col_idx.table_cell_is_shaded: 'is_shaded',
        col_idx.table_cell_combination_id: 'table_cell_combination_id',
        col_idx.table_cell_table_id: 'table_id',
        col_idx.table_cell_system_data_code: 'system_data_code',
        col_idx.table_cell_name: 'name'
    }

    # Cell Position mappings
    column_mappings['pybirdai_cell_position'] = {
        col_idx.cell_positions_cell_id: 'cell_id',
        col_idx.cell_positions_axis_ordinate_id: 'axis_ordinate_id'
    }

    # Member Mapping mappings
    column_mappings['pybirdai_member_mapping'] = {
        col_idx.member_mapping_maintenance_agency_id: 'maintenance_agency_id',
        col_idx.member_mapping_member_mapping_id: 'member_mapping_id',
        col_idx.member_mapping_name: 'name',
        col_idx.member_mapping_code: 'code'
    }

    # Member Mapping Item mappings
    column_mappings['pybirdai_member_mapping_item'] = {
        col_idx.member_mapping_item_member_mapping_id: 'member_mapping_id',
        col_idx.member_mapping_row: 'member_mapping_row',
        col_idx.member_mapping_variable_id: 'variable_id',
        col_idx.member_mapping_is_source: 'is_source',
        col_idx.member_mapping_member_id: 'member_id',
        col_idx.member_mapping_item_valid_from: 'valid_from',
        col_idx.member_mapping_item_valid_to: 'valid_to',
        col_idx.member_mapping_item_member_hierarchy: 'member_hierarchy'
    }

    # Combination mappings
    column_mappings['pybirdai_combination'] = {
        col_idx.combination_combination_id: 'combination_id',
        col_idx.combination_combination_code: 'code',
        col_idx.combination_combination_name: 'name',
        col_idx.combination_maintenance_agency: 'maintenance_agency_id',
        col_idx.combination_version: 'version',
        col_idx.combination_valid_from: 'valid_from',
        col_idx.combination_combination_valid_to: 'valid_to',
        col_idx.combination_metric: 'metric'
    }

    # Combination Item mappings
    column_mappings['pybirdai_combination_item'] = {
        col_idx.combination_item_combination_id: 'combination_id',
        col_idx.combination_item_variable_id: 'variable_id',
        col_idx.combination_item_subdomain_id: 'subdomain_id',
        col_idx.combination_variable_set: 'variable_set_id',
        col_idx.combination_member_id: 'member_id',
        col_idx.combination_item_member_hierarchy: 'member_hierarchy'
    }

    # Mapping Definition mappings
    column_mappings['pybirdai_mapping_definition'] = {
        col_idx.mapping_definition_maintenance_agency_id: 'maintenance_agency_id',
        col_idx.mapping_definition_mapping_id: 'mapping_id',
        col_idx.mapping_definition_name: 'name',
        col_idx.mapping_definition_mapping_type: 'mapping_type',
        col_idx.mapping_definition_code: 'code',
        col_idx.mapping_definition_algorithm: 'algorithm',
        col_idx.mapping_definition_member_mapping_id: 'member_mapping_id',
        col_idx.mapping_definition_variable_mapping_id: 'variable_mapping_id'
    }

    # Mapping To Cube mappings
    column_mappings['pybirdai_mapping_to_cube'] = {
        col_idx.mapping_to_cube_cube_mapping_id: 'cube_mapping_id',
        col_idx.mapping_to_cube_mapping_id: 'mapping_id',
        col_idx.mapping_to_cube_valid_from: 'valid_from',
        col_idx.mapping_to_cube_valid_to: 'valid_to'
    }

    # Variable Mapping mappings
    column_mappings['pybirdai_variable_mapping'] = {
        col_idx.variable_mapping_variable_mapping_id: 'variable_mapping_id',
        col_idx.variable_mapping_maintenance_agency_id: 'maintenance_agency_id',
        col_idx.variable_mapping_code: 'code',
        col_idx.variable_mapping_name: 'name'
    }

    # Variable Mapping Item mappings
    column_mappings['pybirdai_variable_mapping_item'] = {
        col_idx.variable_mapping_item_variable_mapping_id: 'variable_mapping_id',
        col_idx.variable_mapping_item_variable_id: 'variable_id',
        col_idx.variable_mapping_item_is_source: 'is_source',
        col_idx.variable_mapping_item_valid_from: 'valid_from',
        col_idx.variable_mapping_item_valid_to: 'valid_to'
    }

    # Cube Structure mappings
    column_mappings['pybirdai_cube_structure'] = {
        col_idx.cube_structure_maintenance_agency: 'maintenance_agency_id',
        col_idx.cube_structure_id_index: 'cube_structure_id',
        col_idx.cube_structure_name_index: 'name',
        col_idx.cube_structure_code_index: 'code',
        col_idx.cube_structure_description_index: 'description',
        col_idx.cube_structure_valid_from: 'valid_from',
        col_idx.cube_structure_valid_to_index: 'valid_to',
        col_idx.cube_structure_version: 'version'
    }

    # Cube Structure Item mappings
    column_mappings['pybirdai_cube_structure_item'] = {
        col_idx.cube_structure_item_cube_structure_id: 'cube_structure_id',
        col_idx.cube_structure_item_variable_index: 'cube_variable_code',
        col_idx.cube_structure_item_variable_id: 'variable_id',
        col_idx.cube_structure_item_role_index: 'role',
        col_idx.cube_structure_item_order: 'order',
        col_idx.cube_structure_item_subdomain_index: 'subdomain_id',
        col_idx.cube_structure_item_variable_set: 'variable_set_id',
        col_idx.cube_structure_item_specific_member: 'member_id',
        col_idx.cube_structure_item_dimension_type: 'dimension_type',
        col_idx.cube_structure_item_attribute_associated_variable: 'attribute_associated_variable',
        col_idx.cube_structure_item_is_flow: 'is_flow',
        col_idx.cube_structure_item_is_mandatory: 'is_mandatory',
        col_idx.cube_structure_item_description: 'description',
        col_idx.cube_structure_item_is_implemented: 'is_implemented',
        col_idx.cube_structure_item_is_identifier: 'is_identifier'
    }

    # Cube mappings
    column_mappings['pybirdai_cube'] = {
        col_idx.cube_maintenance_agency_id: 'maintenance_agency_id',
        col_idx.cube_object_id_index: 'cube_id',
        col_idx.cube_class_name_index: 'name',
        col_idx.cube_class_code_index: 'code',
        col_idx.cube_framework_index: 'framework_id',
        col_idx.cube_cube_structure_id_index: 'cube_structure_id',
        col_idx.cube_cube_type_index: 'cube_type',
        col_idx.cube_is_allowed: 'is_allowed',
        col_idx.cube_valid_from: 'valid_from',
        col_idx.cube_valid_to_index: 'valid_to',
        col_idx.cube_version: 'version',
        col_idx.cube_description: 'description',
        col_idx.cube_published: 'published',
        col_idx.cube_dataset_url: 'dataset_url',
        col_idx.cube_filters: 'filters',
        col_idx.cube_di_export: 'di_export'
    }

    # Cube to Combination mappings
    column_mappings['pybirdai_cube_to_combination'] = {
        col_idx.cube_to_combination_cube_id: 'cube_id',
        col_idx.cube_to_combination_combination_id: 'combination_id'
    }

    # Cube Link mappings
    column_mappings['pybirdai_cube_link'] = {
        col_idx.cube_link_maintenance_agency_id: 'maintenance_agency_id',
        col_idx.cube_link_id: 'cube_link_id',
        col_idx.cube_link_code: 'code',
        col_idx.cube_link_name: 'name',
        col_idx.cube_link_description: 'description',
        col_idx.cube_link_valid_from: 'valid_from',
        col_idx.cube_link_valid_to: 'valid_to',
        col_idx.cube_link_version: 'version',
        col_idx.cube_link_order_relevance: 'order_relevance',
        col_idx.cube_link_primary_cube_id: 'primary_cube_id',
        col_idx.cube_link_foreign_cube_id: 'foreign_cube_id',
        col_idx.cube_link_type: 'link_type',
        col_idx.cube_link_join_identifier: 'join_identifier'
    }

    # Cube Structure Item Link mappings
    column_mappings['pybirdai_cube_structure_item_link'] = {
        col_idx.cube_structure_item_link_id: 'cube_structure_item_link_id',
        col_idx.cube_structure_item_link_cube_link_id: 'cube_link_id',
        col_idx.cube_structure_item_link_foreign_cube_variable_code: 'foreign_cube_variable_code',
        col_idx.cube_structure_item_link_primary_cube_variable_code: 'primary_cube_variable_code'
    }

    # Member Link mappings
    column_mappings['pybirdai_member_link'] = {
        col_idx.member_link_cube_structure_item_link_id: 'cube_structure_item_link_id',
        col_idx.member_link_primary_member_id: 'primary_member_id',
        col_idx.member_link_foreign_member_id: 'foreign_member_id',
        col_idx.member_link_is_linked: 'is_linked',
        col_idx.member_link_valid_from: 'valid_from',
        col_idx.member_link_valid_to: 'valid_to'
    }

    # Facet Collection mappings
    column_mappings['pybirdai_facet_collection'] = {
        col_idx.facet_collection_code: 'code',
        col_idx.facet_collection_facet_id: 'facet_id',
        col_idx.facet_collection_facet_value_type: 'facet_value_type',
        col_idx.facet_collection_maintenance_agency_id: 'maintenance_agency_id',
        col_idx.facet_collection_name: 'name'
    }

    # Framework Table mappings (from bird_meta_data_model_extension)
    column_mappings['pybirdai_framework_table'] = {
        col_idx.framework_table_framework_id: 'framework_id',
        col_idx.framework_table_table_id: 'table_id'
    }

    # Framework Subdomain mappings (from bird_meta_data_model_extension)
    column_mappings['pybirdai_framework_subdomain'] = {
        col_idx.framework_subdomain_framework_id: 'framework_id',
        col_idx.framework_subdomain_subdomain_id: 'subdomain_id'
    }

    # Framework Hierarchy mappings (from bird_meta_data_model_extension)
    column_mappings['pybirdai_framework_hierarchy'] = {
        col_idx.framework_hierarchy_framework_id: 'framework_id',
        col_idx.framework_hierarchy_member_hierarchy_id: 'member_hierarchy_id'
    }

    # Mapping Ordinate Link mappings
    column_mappings['pybirdai_mapping_ordinate_link'] = {
        col_idx.mapping_ordinate_link_mapping_id: 'mapping_definition_id',
        col_idx.mapping_ordinate_link_axis_ordinate_id: 'axis_ordinate_id',
        col_idx.mapping_ordinate_link_created_at: 'created_at'
    }

    logger.debug(f"Built column mappings for {len(column_mappings)} model types")
    return column_mappings
