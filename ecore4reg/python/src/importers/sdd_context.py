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
    combinationsModules = []
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


