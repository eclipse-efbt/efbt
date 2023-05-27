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
    combinationsModule = CombinationModule(name = 'combinationsModule')
    #The cubesModule
    cubesModule = CubeModule(name = 'cubesModule')
    #The cubeStructuresModule
    cubeStructuresModule = CubeModule(name = 'cubeStructuresModule')
    #The cubeStructureItemsModule
    cubeStructureItemsModule = CubeModule(name = 'cubeStructureItemsModule')
    #The cubeMappingModule
    cubeMappingModule = CubeMappingModule(name = 'cubeMappingModule')
    #The mappingDefinitionModule
    mappingDefinitionModule = MappingDefinitionModule(name = 'mappingDefinitionModule')
    #The memberMappingModule
    memberMappingModule = MemberMappingModule(name = 'memberMappingModule')
    #The variableMappingModule
    variableMappingModule = VariableMappingModule(name = 'variableMappingModule')
    #The birdModel
    birdModel = SMCubesCoreModel(name = 'SMCubesCoreModel')

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

    cubeClassNameIndex = 0
    cubeObjectIDIndex = 1
    cubeCubeTypeIndex = 3
    cubeValidToIndex = 11
    cubeFrameworkIndex = 5
    
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
    
    combination_combination_code = 1
    combination_combination_id = 0
    combination_combination_name = 2
    
    combination_item_combination_id = 0
    combination_item_variable_id = 1
    combination_member_id = 4

    def set_up_csv_indexes(self):
        if self.input_from_website:
            self.cubeClassNameIndex = 3
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
            self.member_member_descriptions = 5
            
            self.combination_combination_code = 1
            self.combination_combination_id = 0
            self.combination_combination_name = 2
            
            self.combination_item_combination_id = 0
            self.combination_item_variable_id = 1
            self.combination_member_id = 4
        else:
            self.cubeClassNameIndex = 0
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
            
            self.combination_combination_code = 1
            self.combination_combination_id = 0
            self.combination_combination_name = 2
            
            self.combination_item_combination_id = 0
            self.combination_item_variable_id = 1
            self.combination_member_id = 4
           
