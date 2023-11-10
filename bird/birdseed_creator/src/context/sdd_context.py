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

    use_codes = True
    
    # the directory where we get our input files
    file_directory = ""
    # the directory where we save our outputs.
    output_directory = ""

    # The domains
    domains = DomainModule(name = 'domainsModule')
    #The variables
    variables = VariableModule(name = 'variablesModule')
    #The variable sets
    variable_sets = VariableSetModule(name = 'variablesModule')
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
    axes = AxisModule (name = 'axisModule')
    #the AxisOrdinateModule
    axis_ordinates = AxisOrdinateModule (name = 'axisOrdinateModule')
    # the CellPositionModule
    cell_positions = CellPositionModule(name = 'cellPositionModule')
    #The core sdd model
    core_sdd_model = SMCubesCoreModel(name = 'smcubesCoreModel')
    #The extra sdd model
    extra_sdd_model = SMCubesExtraModel(name = 'smcubesExtraModel')
    
    sdd_model = SMCubesModel(name = 'smcubesModel') 

    combinations_dictionary = {}
    member_dictionary = {}
    variable_dictionary= {}
    axis_ordinate_dictionary= {}
    table_cell_dictionary= {}
    member_mapping_dictionary = {}
    cell_positions_dictionary = {}
    variable_set_enumeration_dictionary = {}
    variable_to_domain_map = {}
    variable_set_to_variable_map = {}
    # For the reference output layers we record a map between domains
    # and domain names
    domain_to_domain_name_map = {}


    
    def __init__(self):

        self.core_sdd_model.variableModules.append(self.variables)
        self.core_sdd_model.variableSetModules.append(self.variable_sets)
        self.core_sdd_model.domainModules.append(self.domains)
        self.core_sdd_model.memberModules.append(self.members)
        self.core_sdd_model.subDomainModules.append(self.subdomains)
        self.extra_sdd_model.combinationModules.append(self.combinations)
        self.extra_sdd_model.memberMappingModules.append(self.member_mappings)
        self.extra_sdd_model.axisModules.append(self.axes)
        self.extra_sdd_model.axisOrdinateModules.append(self.axis_ordinates)
        self.extra_sdd_model.cellPositionModules.append(self.cell_positions)
        #self.extra_sdd_model.ordinateItemsModules.append(self.ordinate_items)
        self.extra_sdd_model.reportTableModules.append(self.report_tables)
        self.extra_sdd_model.tableCellModules.append(self.table_cells)
        self.sdd_model.coreModel=self.core_sdd_model
        self.sdd_model.extraModel=self.extra_sdd_model
