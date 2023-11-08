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
from regdna import ELDataType, ELPackage, ModuleList, GenerationRulesModule


class Context(object):
    '''
    Documentation for Context
    '''
    # variables to configure the behaviour
    use_codes = True
    use_subdomains = False
    use_subdomains_in_il = True
    load_eil_from_website = False
    use_variable_long_name = True
    persist_to_regdna = True
    inScopeFileDirectory = ""
    add_pks_to_input_layer_from_website = True 
    reporting_framework = "FINREP"
    reporting_framework_version = ["3","3.0-Ind"]
    match_domains_in_generation_file = False
    input_layer_name = "Input Layer 6.2"
    # the directory where we get our input files
    file_directory = ""
    # the directory where we save our outputs.
    output_directory = ""

    # we create the main 5 'primitive' data types
    e_string = ELDataType()
    e_string.name = "String"

    e_double = ELDataType()
    e_double.name = "double"

    e_int = ELDataType()
    e_int.name = "int"

    e_date = ELDataType()
    e_date.name = "Date"

    e_boolean = ELDataType()
    e_boolean.name = "boolean"

    # create the moduleList to hold all the modules
    module_list = ModuleList()
    
    main_catogory_to_name_map = {}
    report_to_main_catogory_map = {}
    tables_for_main_catagory_map = {}
    table_and_part_tuple_map = {}
    table_parts_to_linked_tables_map = {}
    table_parts_to_to_filter_map = {}
    #table_parts_to_to_filter_items_map = {}
    table_parts_to_main_catagory_map = {}
    main_catagories_in_scope = []

    # create  regdna  packages
    types_package = ELPackage(name='types')
    il_domains_package = ELPackage(
        name='il_domains',
        nsURI='http://www.eclipse.org/bird/il_domains',
        nsPrefix='il_domains')
    sdd_domains_package = ELPackage(
        name='sdd_domains',
        nsURI='http://www.eclipse.org/bird/sdd_domains',
        nsPrefix='sdd_domains')
    input_tables_package = ELPackage(
        name='input_tables',
        nsURI='http://www.eclipse.org/bird/input_tables',
        nsPrefix='input_tables')
    output_tables_package = ELPackage(
        name='output_tables',
        nsURI='http://www.eclipse.org/bird/output_tables',
        nsPrefix='output_tables')
    
    

    module_list = ModuleList()
    
    # classesMap keeps a reference between ldm ID's for classes and
    # the class instance
    classes_map = {}

    # classesMap keeps a reference between ldm ID's for classes and
    # the list of these calsses that we call tables
    table_map = {}
    # A map between the LDM names for primitive types types, and
    # our standard primitive types such as EString
    datatype_map = {}
    # For the reference output layers we record a map between variables
    # and domains
    variable_to_domain_map = {}
    variable_to_long_names_map = {}
    variable_to_primary_concept_map = {}
    # For the reference output layers we record a map between domains
    # and domain names
    domain_to_domain_name_map = {}
    # For the reference output layers we record a map between members ids
    # andtheir containing domains
    member_id_to_domain_map = {}
    # For the reference output layers we record a map between members ids
    # and their names
    member_id_to_member_name_map = {}
    # For the reference output layers we record a map between members ids
    # and their codes
    member_id_to_member_code_map = {}

    fk_to_mandatory_map = {}
    fk_to_column_map = {}
    # we record a list of missing domains
    missing_domains = []
    # enumMap keeps a reference between ldm ID's for domains and
    # the enum instance
    enum_map = {}
    enum_id_enum_name_map = {}
    
    arc_to_source_map = {}
    arc_name_to_arc_class_map = {}
    
    arc_target_to_arc_map = {}

    logical_attribute_to_relational_name = {}
    subdomain_to_member_list_map = {}
    subdomain_id_to_domain_id = {}

    import_logic_strings = []

    variable_set_to_variable_map = {}
    
    cube_class_name_index = 2
    cubeClassCodeIndex = 3
    cube_object_id_index = 1
    cube_cube_type_index = 6
    cube_valid_to_index = 9
    cube_framework_index = 4
    
    variable_set_enumeration_valid_to = 3
    variable_set_enumeration_variable_id = 1
    variable_set_enumeration_valid_set = 0
    variable_set_enumeration_subdomain_id = 4
    
    variable_set_variable_set_id = 1
    
    variable_variable_name_index = 1
    variable_long_name_index = 3
    variable_domain_index = 4
    variable_code_index = 2
    variable_variable_description = 5
    variable_variable_true_id = 1
    variable_primary_concept = 6
    
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
    cube_structure_item_role_index = 3
    member_member_descriptions = 5
    
    combination_combination_code = 1
    combination_combination_id = 0
    combination_combination_name = 2
    
    
    combination_item_combination_id = 0
    combination_item_variable_id = 1
    combination_variable_set=3
    combination_member_id = 4
    
    

    def __init__(self):

        self.types_package.eClassifiers.append(self.e_string)
        self.types_package.eClassifiers.append(self.e_double)
        self.types_package.eClassifiers.append(self.e_int)
        self.module_list.modules.append(self.types_package)
        self.module_list.modules.append(self.il_domains_package)
        self.module_list.modules.append(self.sdd_domains_package)
        self.module_list.modules.append(self.input_tables_package)
        self.module_list.modules.append(self.output_tables_package)
        self.module_list.modules.append(self.generation_rules_module)

