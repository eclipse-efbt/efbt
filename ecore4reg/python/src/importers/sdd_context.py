# coding=UTF-8#
# Copyright (c) 2020 Bird Software Solutions Ltd
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDX-License-Identifier: EPL-2.0
#
# Contributors:
#    Neil Mackenzie - initial API and implementation
#

from pyecore.ecore import EPackage
from sdd_model import *


class SDDContext(object):
    '''
    Documentation for Context
    '''
    # variables to configure the behaviour


    # the directory where we get our input files
    file_directory = ""
    # the directory where we save our outputs.
    output_directory = ""

    # The domains
    domains = DomainModule(name = 'domainsModule')
    #The variables
    variables = VariableModule(name = 'variablesModule')
    #The members
    members = MemberModule(name = 'memberModule')
    #The subdomains
    subdomains = SubDomainModule(name = 'subdomainsModule')
    #The subdomain enumerations
    combinations = CombinationModule(name = 'combinationsModule')
    #The cubesModule
    cubes = CubeModule(name = 'cubesModule')
    #The cubeStructuresModule
    cube_structures = CubeModule(name = 'cubeStructuresModule')
    #The cubeStructureItemsModule
    cube_structure_items= CubeModule(name = 'cubeStructureItemsModule')
    #The cubeMappingModule
    cube_mappings = CubeMappingModule(name = 'cubeMappingModule')
    #The mappingDefinitionModule
    mapping_definitions = MappingDefinitionModule(name = 'mappingDefinitionModule')
    #The memberMappingModule
    member_mappings = MemberMappingModule(name = 'memberMappingModule')
    #The variableMappingModule
    variable_mappings = VariableMappingModule(name = 'variableMappingModule')
    #The variableMappingModule
    report_tables = ReportTableModule(name = 'reportTableModule')
    #The TableCellModule
    table_cells = TableCellModule(name = 'tableCellModule')
    #The AxisModule
    exes = AxisModule (name = 'axisModule')
    #the AxisOrdinateModule
    axis_ordinates = AxisOrdinateModule (name = 'axisOrdinateModule')
    # the CellPositionModule
    cell_positions = CellPositionModule(name = 'cellPositionModule')
    #The core sdd model
    core_sdd_model = SMCubesCoreModel(name = 'smcubesCoreModel')
    #The extra sdd model
    extra_sdd_model = SmcubesExtraModel(name = 'SmcubesExtraModel')

    variableToDomainMap = {}
    variableToLongNamesMap = {}
    # For the reference output layers we record a map between domains
    # and domain names
    domain_to_domain_name_map = {}
    # For the reference output layers we record a map between members ids
    # andtheir containing domains
    memberIDToDomainMap = {}
    # For the reference output layers we record a map between members ids
    # and their names
    memberIDToMemberNameMap = {}
    # For the reference output layers we record a map between members ids
    # and their codes
    memberIDToMemberCodeMap = {}

    subDomainToMemberListMap = {}
    subDomainIDToDomainID = {}

    variableSetToVariableMap = {}

    cube_class_name_index = 3
    cube_object_id_index = 1
    cube_cube_type_index = 6
    cube_valid_to_index = 9
    cube_framework_index = 4
    
    variable_set_valid_to = 3
    variable_set_variable_id = 1
    variable_set_valid_set = 0
    
    variable_variable_name_index = 1
    variable_long_name_index = 3
    variable_domain_index = 4
    variable_code_index = 2
    variable_variable_description = 5
    variable_variable_true_id = 1
    
    domain_domain_id_index = 6
    domain_domain_name_index = 2
    domain_domain_data_type = 5
    domain_domain_description = 4
    domain_domain_true_id = 1
    domain_domain_is_enumerated = 3
    domain_domain_is_reference = 8
    
    member_member_id_index = 1
    member_member_code_index = 2
    member_member_name_index = 3
    member_domain_id_index = 4
    
    subdomain_domain_id_index = 3
    subdomain_subdomain_id_index = 1
    subdomain_subdomain_code = 0
    subdomain_subdomain_description = 1
    subdomain_subdomain_name = 7

    subdomain_enumeration_member_id_index = 0
    subdomain_enumeration_subdomain_id_index = 1
    subdomain_enumeration_valid_to_index = 3
    
    cube_structure_item_variable_index = 1
    cube_structure_item_class_id_index = 0
    cube_structure_item_subdomain_index = 5
    cube_structure_item_specific_member = 7
    cube_structure_item_attribute_name = 2
    cube_structure_item_variable_set = 6
    member_member_descriptions = 5
    
    combination_combination_code = 1
    combination_combination_id = 0
    combination_combination_name = 2
    
    combination_item_combination_id = 0
    combination_item_variable_id = 1
    combination_member_id = 4
    
    member_mapping_id = 0
    member_mapping_row = 1
    member_mapping_variable_id = 2
    member_mapping_is_source = 3
    member_mapping_member_id = 4
    
    table_table_id = 0
    table_table_name = 1 
    table_code = 2 
    table_description = 3
    table_maintenance_ageny = 4
    table_version = 5
    table_valid_from = 6
    table_valid_to = 7 
    
    table_cell_cell_id = 0
    table_cell_is_shaded = 1
    table_cell_combination_id = 2
    table_cell_table_id = 3
    table_cell_system_data_code = 4
    
    axis_id = 0
    axis_code = 1
    axis_orientation = 2
    axis_order = 3
    axis_name = 4
    axis_description = 5
    axis_table_id =  6
    axis_is_open_axis = 7
    
    axis_ordinate_id = 0
    axis_ordinate_is_abstract_header = 1
    axis_ordinate_code = 2
    axis_ordinate_order = 3
    axis_ordinate_level = 4
    axis_ordinate_path = 5
    axis_ordinate_axis_id = 6
    axis_ordinate_parent_axis_ordinate_id = 7
    axis_ordinate_name = 8
    axis_ordinate_description = 9
    
    cell_positions_cell_id = 0
    cell_positions_axis_ordinate_id = 1
   

