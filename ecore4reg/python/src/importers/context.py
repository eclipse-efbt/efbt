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
from ecore4reg import ELDataType, ELPackage, ModuleList, GenerationRulesModule, VTLModule


class Context(object):
    '''
    Documentation for Context
    '''
    # variables to configure the behaviour
    use_codes = True
    input_from_website = False
    persist_vtl_comments = True
    use_subdomains_in_rol = False
    use_subdomains_in_il = True
    load_eil_from_website = False
    use_variable_long_name = True
    persist_to_ecore4reg = True
    inScopeFileDirectory = ""
    add_pks_to_input_layer_from_website = True 
    reporting_framework = "FINREP"
    reporting_framework_version = ["3","3.0-Ind"]
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
    table_parts_to_to_filter_items_map = {}

    # create  Ecore4Reg  packages
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

    # create  Ecore packages
    il_domains_ecore_package = EPackage(
        name='il_domains',
        nsURI='http://www.eclipse.org/bird/il_domains',
        nsPrefix='il_domains')
    sdd_domains_ecore_package = EPackage(
        name='sdd_domains',
        nsURI='http://www.eclipse.org/bird/sdd_domains',
        nsPrefix='sdd_domains')
    input_tables_ecore_package = EPackage(
        name='input_tables',
        nsURI='http://www.eclipse.org/bird/input_tables',
        nsPrefix='input_tables')
    output_tables_ecore_package = EPackage(
        name='output_tables',
        nsURI='http://www.eclipse.org/bird/output_tables',
        nsPrefix='output_tables')

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
    
    foreign_key_tuple = []

    generation_rules_module = GenerationRulesModule(name='generations')

    vtl_module = VTLModule(name="vtl")
    module_list = ModuleList()

    subdomain_to_member_list_map = {}
    subdomain_id_to_domain_id = {}

    importLogicStrings = []

    variable_set_to_variable_map = {}
    
    cubeClassNameIndex = 8
    cubeClassCodeIndex = 0
    cubeObjectIDIndex = 1
    cubeCubeTypeIndex = 3
    cubeValidToIndex = 11
    cubeFrameworkIndex = 5
    cubeRoleIndex = 9
    
    variable_set_valid_to = 4
    variable_set_variable_id = 5
    variable_set_valid_set = 6
    
    variableVariableNameIndex = 6
    variableLongNameIndex = 4
    variableDomainIndex = 2
    variableCodeIndex = 0
    variable_variable_description = 1
    variable_variable_true_id = 6
    
    domainDomainIDIndex = 0
    domainDomainNameIndex = 8
    domain_domain_data_type = 1
    domain_domain_description = 2
    domain_domain_true_id = 3
    domain_domain_is_enumerated = 5
    domain_domain_is_reference = 6
    
    memberMemberIDIndex = 4
    memberMemberCodeIndex = 0
    memberMemberNameIndex = 5
    memberDomainIDIndex = 2
    member_member_descriptions = 1
    
    subdomainDomainIDIndex = 2
    subDomainSubDomainIDIndex = 8
    subdomain_subdomain_code = 0
    subdomain_subdomain_description = 1
    subdomain_subdomain_name = 7
    
    subdomain_enumerationMemberIDIndex = 0
    subdomain_enumerationSubdomainIDIndex = 2
    subdomain_enumerationValidToIndex = 4
    
    cube_structure_itemVariableIndex = 2
    cube_structure_itemClassIDIndex = 1
    cube_structure_itemSubdomainIndex = 10
    cube_structure_itemSpecificMember = 7
    cube_structure_itemAttributeName = 11
    cube_structure_itemVariableSet = 12
    cube_structure_item_role_index = 9
    
    combination_combination_code = 1
    combination_combination_id = 0
    combination_combination_name = 2
    
    combination_item_combination_id = 0
    combination_item_variable_id = 1
    combination_member_id = 4
    
    

    def set_up_csv_indexes(self):
        if self.input_from_website:
            self.cubeClassNameIndex = 2
            self.cubeClassCodeIndex = 3
            self.cubeObjectIDIndex = 1
            self.cubeCubeTypeIndex = 6
            self.cubeValidToIndex = 9
            self.cubeFrameworkIndex = 4
            
            self.variable_set_valid_to = 3
            self.variable_set_variable_id = 1
            self.variable_set_valid_set = 0
            
            self.variableVariableNameIndex = 1
            self.variableLongNameIndex = 3
            self.variableDomainIndex = 4
            self.variableCodeIndex = 2
            self.variable_variable_description = 5
            self.variable_variable_true_id = 1
            
            self.domainDomainIDIndex = 6
            self.domainDomainNameIndex = 2
            self.domain_domain_data_type = 5
            self.domain_domain_description = 4
            self.domain_domain_true_id = 1
            self.domain_domain_is_enumerated = 3
            self.domain_domain_is_reference = 8
            
            self.memberMemberIDIndex = 1
            self.memberMemberCodeIndex = 2
            self.memberMemberNameIndex = 3
            self.memberDomainIDIndex = 4
            
            self.subdomainDomainIDIndex = 3
            self.subDomainSubDomainIDIndex = 1
            self.subdomain_subdomain_code = 0
            self.subdomain_subdomain_description = 1
            self.subdomain_subdomain_name = 7
    
            self.subdomain_enumerationMemberIDIndex = 0
            self.subdomain_enumerationSubdomainIDIndex = 1
            self.subdomain_enumerationValidToIndex = 3
            
            self.cube_structure_itemVariableIndex = 1
            self.cube_structure_itemClassIDIndex = 0
            self.cube_structure_itemSubdomainIndex = 5
            self.cube_structure_itemSpecificMember = 7
            self.cube_structure_itemAttributeName = 2
            self.cube_structure_itemVariableSet = 6
            self.cube_structure_item_role_index = 3
            self.member_member_descriptions = 5
            
            self.combination_combination_code = 1
            self.combination_combination_id = 0
            self.combination_combination_name = 2
            
            self.combination_item_combination_id = 0
            self.combination_item_variable_id = 1
            self.combination_member_id = 4
        else:
            self.cubeClassNameIndex = 8
            self.cubeClassCodeIndex = 0
            self.cubeObjectIDIndex = 1
            self.cubeCubeTypeIndex = 3
            self.cubeValidToIndex = 11
            self.cubeFrameworkIndex = 5
            
            self.variable_set_valid_to = 4
            self.variable_set_variable_id = 5
            self.variable_set_valid_set = 6
            
            self.variableVariableNameIndex = 6
            self.variableLongNameIndex = 4
            self.variableDomainIndex = 2
            self.variableCodeIndex = 0
            self.variable_variable_description = 1
            self.variable_variable_true_id = 6
            
            self.domainDomainIDIndex = 0
            self.domainDomainNameIndex = 8
            self.domain_domain_data_type = 1
            self.domain_domain_description = 2
            self.domain_domain_true_id = 3
            self.domain_domain_is_enumerated = 5
            self.domain_domain_is_reference = 6
            
            self.memberMemberIDIndex = 4
            self.memberMemberCodeIndex = 0
            self.memberMemberNameIndex = 5
            self.memberDomainIDIndex = 2
            self.member_member_descriptions = 1
            
            self.subdomainDomainIDIndex = 2
            self.subDomainSubDomainIDIndex = 8
            self.subdomain_subdomain_code = 0
            self.subdomain_subdomain_description = 1
            self.subdomain_subdomain_name = 7
    
            self.subdomain_enumerationMemberIDIndex = 0
            self.subdomain_enumerationSubdomainIDIndex = 2
            self.subdomain_enumerationValidToIndex = 4
            
            self.cube_structure_itemVariableIndex = 2
            self.cube_structure_itemClassIDIndex = 1
            self.cube_structure_itemSubdomainIndex = 10
            self.cube_structure_itemSpecificMember = 7
            self.cube_structure_itemAttributeName = 11
            self.cube_structure_itemVariableSet = 12
            self.cube_structure_item_role_index = 9
            
            self.combination_combination_code = 1
            self.combination_combination_id = 0
            self.combination_combination_name = 2
            
            self.combination_item_combination_id = 0
            self.combination_item_variable_id = 1
            self.combination_member_id = 4
            
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
        self.module_list.modules.append(self.vtl_module)
        
        
