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
from ecore4reg import ELDataType, ELPackage, ModuleList, ViewModule, VTLModule, WorkflowModule


class Context(object):
    '''
    Documentation for Context
    '''
    # variables to configure the behaviour

    persist_vtl_comments = True
    use_subdomains = False
    use_variable_long_name = True
    persist_to_ecore4reg = True
    inScopeFileDirectory = ""
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

    # create  Ecore4Reg  packages
    types_package = ELPackage(name='types')
    input_layer_enums_package = ELPackage(
        name='input_layer_enums',
        nsURI='http://www.eclipse.org/bird/input_layer_enums',
        nsPrefix='input_layer_enums')
    output_layer_enums_package = ELPackage(
        name='output_layer_enums',
        nsURI='http://www.eclipse.org/bird/output_layer_enums',
        nsPrefix='output_layer_enums')
    input_layer_entities_ackage = ELPackage(
        name='input_layer_entities',
        nsURI='http://www.eclipse.org/bird/input_layer_entities',
        nsPrefix='input_layer_entities')
    output_layer_entities_package = ELPackage(
        name='output_layer_entities',
        nsURI='http://www.eclipse.org/bird/output_layer_entities',
        nsPrefix='output_layer_entities')

    # create  Ecore packages
    input_layer_enums_ecore_package = EPackage(
        name='input_layer_enums',
        nsURI='http://www.eclipse.org/bird/input_layer_enums',
        nsPrefix='input_layer_enums')
    output_layer_enums_ecore_package = EPackage(
        name='output_layer_enums',
        nsURI='http://www.eclipse.org/bird/output_layer_enums',
        nsPrefix='output_layer_enums')
    input_layer_entities_ecore_package = EPackage(
        name='input_layer_entities',
        nsURI='http://www.eclipse.org/bird/input_layer_entities',
        nsPrefix='input_layer_entities')
    output_layer_entities_ecore_package = EPackage(
        name='output_layer_entities',
        nsURI='http://www.eclipse.org/bird/output_layer_entities',
        nsPrefix='output_layer_entities')

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
    domainToDomainNameMap = {}
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

    view_module = ViewModule(name='generations')

    workflow_module = WorkflowModule(name='finrepWorkflow')

    vtl_module = VTLModule(name="vtl")
    module_list = ModuleList()

    sub_domain_to_member_list_map = {}
    sub_domain_id_to_domain_id = {}

    importLogicStrings = []

    variable_set_to_variable_map = {}

    def __init__(self):

        self.types_package.eClassifiers.append(self.e_string)
        self.types_package.eClassifiers.append(self.e_double)
        self.types_package.eClassifiers.append(self.e_int)
        self.module_list.modules.append(self.types_package)
        self.module_list.modules.append(self.input_layer_enums_package)
        self.module_list.modules.append(self.output_layer_enums_package)
        self.module_list.modules.append(self.input_layer_entities_ackage)
        self.module_list.modules.append(self.output_layer_entities_package)
        self.module_list.modules.append(self.view_module)
        self.module_list.modules.append(self.workflow_module)
        self.module_list.modules.append(self.vtl_module)
