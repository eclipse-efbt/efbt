# coding=UTF-8
# Copyright (c) 2024 Bird Software Solutions Ltd
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDX-License-Identifier: EPL-2.0
#
# Contributors:
#    Neil Mackenzie - initial API and implementation

class ColumnIndexes(object):

    maintenance_agency_id = 0
    maintenance_agency_code = 1
    maintenance_agency_name = 2
    maintenance_agency_description = 3

    framework_maintenance_agency_id = 0
    framework_id = 1
    framework_name = 2
    framework_code = 3
    framework_description = 4
    framework_type = 5
    framework_reporting_population = 6
    framework_other_links = 7
    framework_order = 8
    framework_status = 9

    cube_maintenance_agency_id = 0
    cube_object_id_index = 1
    cube_class_name_index = 2
    cube_class_code_index = 3
    cube_framework_index = 4
    cube_cube_structure_id_index = 5
    cube_cube_type_index = 6
    cube_is_allowed = 7
    cube_valid_from = 8
    cube_valid_to_index = 9
    cube_version = 10
    cube_description = 11
    cube_published = 12
    cube_dataset_url = 13
    cube_filters = 14
    cube_di_export = 15

    variable_set_enumeration_valid_set = 0
    variable_set_enumeration_variable_id = 1
    variable_set_enumeration_valid_from = 2
    variable_set_enumeration_valid_to = 3
    variable_set_enumeration_subdomain_id = 4
    variable_set_enumeration_is_flow = 5
    variable_set_enumeration_order = 6

    variable_set_maintenance_agency_id = 0
    variable_set_variable_set_id = 1
    variable_set_name = 2
    variable_set_code = 3
    variable_set_description = 4

    variable_maintenance_agency = 0
    variable_variable_true_id = 1
    variable_code_index = 2
    variable_variable_name_index = 3
    variable_domain_index = 4
    variable_variable_description = 5
    variable_primary_concept = 6
    variable_is_decomposed = 7

    domain_maintenance_agency = 0
    domain_domain_true_id = 1
    domain_domain_name_index = 2
    domain_domain_is_enumerated = 3
    domain_domain_description = 4
    domain_domain_data_type = 5
    domain_code = 6
    domain_facet_id = 7
    domain_domain_is_reference = 8


    member_maintenance_agency = 0
    member_member_id_index = 1
    member_member_code_index = 2
    member_member_name_index = 3
    member_domain_id_index = 4
    member_member_descriptions = 5



    subdomain_maintenance_agency_id = 0
    subdomain_subdomain_id_index = 1
    subdomain_subdomain_name = 2
    subdomain_domain_id_index = 3
    subdomain_is_listed = 4
    subdomain_subdomain_code = 5
    subdomain_facet_id = 6
    subdomain_subdomain_description = 7
    subdomain_is_natural = 8

    subdomain_enumeration_member_id_index = 0
    subdomain_enumeration_subdomain_id_index = 1
    subdomain_enumeration_valid_from = 2
    subdomain_enumeration_valid_to_index = 3
    subdomain_enumeration_order = 4


    cube_structure_maintenance_agency = 0
    cube_structure_id_index = 1
    cube_structure_name_index = 2
    cube_structure_code_index = 3
    cube_structure_description_index = 4
    cube_structure_valid_from = 5
    cube_structure_valid_to_index = 6
    cube_structure_version = 7

    cube_structure_item_cube_structure_id = 0
    cube_structure_item_variable_index = 1
    cube_structure_item_variable_id = 2
    cube_structure_item_role_index = 3
    cube_structure_item_order = 4
    cube_structure_item_subdomain_index = 5
    cube_structure_item_variable_set = 6
    cube_structure_item_specific_member = 7
    cube_structure_item_dimension_type = 8
    cube_structure_item_attribute_associated_variable = 9
    cube_structure_item_is_flow = 10
    cube_structure_item_is_mandatory = 11
    cube_structure_item_description = 12
    cube_structure_item_is_implemented = 13
    cube_structure_item_is_identifier = 14

    combination_combination_id = 0
    combination_combination_code = 1
    combination_combination_name = 2
    combination_maintenance_agency = 3
    combination_version = 4
    combination_valid_from = 5
    combination_combination_valid_to = 6
    combination_metric = 7


    combination_item_combination_id = 0
    combination_item_variable_id = 1
    combination_item_subdomain_id = 2
    combination_variable_set = 3
    combination_member_id = 4
    combination_item_member_hierarchy = 5

    member_mapping_maintenance_agency_id = 0
    member_mapping_member_mapping_id = 1
    member_mapping_name = 2
    member_mapping_code = 3

    member_mapping_item_member_mapping_id = 0
    member_mapping_row = 1
    member_mapping_variable_id = 2
    member_mapping_is_source = 3
    member_mapping_member_id = 4
    member_mapping_item_valid_from = 5
    member_mapping_item_valid_to = 6
    member_mapping_item_member_hierarchy = 7

    table_table_id = 0
    table_table_name = 1
    table_code = 2
    table_description = 3
    table_maintenance_agency_id = 4
    table_version = 5
    table_valid_from = 6
    table_valid_to = 7

    table_cell_cell_id = 0
    table_cell_is_shaded = 1
    table_cell_combination_id = 2
    table_cell_table_id = 3
    table_cell_system_data_code = 4
    table_cell_name = 5

    axis_id = 0
    axis_code = 1
    axis_orientation = 2
    axis_order = 3
    axis_name = 4
    axis_description = 5
    axis_table_id = 6
    axis_is_open_axis = 7

    axis_ordinate_axis_ordinate_id = 0
    axis_ordinate_is_abstract_header = 1
    axis_ordinate_code = 2
    axis_ordinate_order = 3
    axis_ordinate_level = 4
    axis_ordinate_path = 5
    axis_ordinate_axis_id = 6
    axis_ordinate_parent_axis_ordinate_id = 7
    axis_ordinate_name = 8
    axis_ordinate_description = 9

    ordinate_item_axis_ordinate_id = 0
    ordinate_item_variable_id = 1
    ordinate_item_member_id = 2
    ordinate_item_member_hierarchy_id = 3
    ordinate_item_member_hierarchy_valid_from = 4
    ordinate_item_starting_member_id = 5
    ordinate_item_is_starting_member_included = 6


    cell_positions_cell_id = 0
    cell_positions_axis_ordinate_id = 1

    member_hierarchy_maintenance_agency = 0
    member_hierarchy_id = 1
    member_hierarchy_code = 2
    member_hierarchy_domain_id = 3
    member_hierarchy_name = 4
    member_hierarchy_description = 5
    member_hierarchy_is_main_hierarchy = 6

    member_hierarchy_node_hierarchy_id = 0
    member_hierarchy_node_member_id = 1
    member_hierarchy_node_level = 2
    member_hierarchy_node_parent_member_id = 3
    member_hierarchy_node_comparator = 4
    member_hierarchy_node_operator = 5
    member_hierarchy_node_valid_from = 6
    member_hierarchy_node_valid_to = 7

    cube_link_maintenance_agency_id = 0
    cube_link_id = 1
    cube_link_code = 2
    cube_link_name = 3
    cube_link_description = 4
    cube_link_valid_from = 5
    cube_link_valid_to = 6
    cube_link_version = 7
    cube_link_order_relevance = 8
    cube_link_primary_cube_id = 9
    cube_link_foreign_cube_id = 10
    cube_link_type = 11
    cube_link_join_identifier = 12

    cube_structure_item_link_id = 0
    cube_structure_item_link_cube_link_id = 1
    cube_structure_item_link_foreign_cube_variable_code = 2
    cube_structure_item_link_primary_cube_variable_code = 3

    member_link_cube_structure_item_link_id = 0
    member_link_primary_member_id = 1
    member_link_foreign_member_id = 2
    member_link_is_linked = 3
    member_link_valid_from = 4
    member_link_valid_to = 5

    mapping_definition_maintenance_agency_id = 0
    mapping_definition_mapping_id = 1
    mapping_definition_name = 2
    mapping_definition_mapping_type = 3
    mapping_definition_code = 4
    mapping_definition_algorithm = 5
    mapping_definition_member_mapping_id = 6
    mapping_definition_variable_mapping_id = 7

    mapping_to_cube_cube_mapping_id = 0
    mapping_to_cube_mapping_id = 1
    mapping_to_cube_valid_from = 2
    mapping_to_cube_valid_to = 3

    variable_mapping_variable_mapping_id = 0
    variable_mapping_maintenance_agency_id = 1
    variable_mapping_code = 2
    variable_mapping_name = 3

    variable_mapping_item_variable_mapping_id = 0
    variable_mapping_item_variable_id = 1
    variable_mapping_item_is_source = 2
    variable_mapping_item_valid_from = 3
    variable_mapping_item_valid_to = 4

    cube_to_combination_cube_id = 0
    cube_to_combination_combination_id = 1

    facet_collection_code = 0
    facet_collection_facet_id = 1
    facet_collection_facet_value_type = 2
    facet_collection_maintenance_agency_id = 3
    facet_collection_name = 4
