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


from regdna import  ELPackage, ModuleList, GenerationRulesModule, ReportModule, ELAnnotationDirective

from context.ecore_lite_types import EcoreLiteTypes


class Context(object):
    '''
    Documentation for Context
    '''
    # variables to configure the behaviour
    
    use_codes = True
    load_ldm = False
    load_eil_from_website = False
    use_variable_long_name = True
    persist_to_regdna = True
    inScopeFileDirectory = ""
    add_pks_to_input_layer_from_website = True 
    match_domains_in_generation_file = False
    input_layer_name = "Input Layer 6.3"
    # the directory where we get our input files
    file_directory = ""
    # the directory where we save our outputs.
    output_directory = ""
    use_sub_domains_in_input_layer = True

    types = EcoreLiteTypes()
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
    table_part_varaible_transformation_map = {}

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
    
    generation_rules_module = GenerationRulesModule(name='generations')
    reports_module = ReportModule(name='report')
    reports_dictionary = {}

    module_list = ModuleList()
    # classesMap keeps a reference between ldm ID's for classes and
    # the class instance
    classes_map = {}
    # classesMap keeps a reference between ldm ID's for classes and
    # the list of these classes that we call tables
    table_map = {}
    # A map between the LDM names for primitive types types, and
    # our standard primitive types such as EString
    datatype_map = {}
    # For the reference output layers we record a map between domains
    # and domain names
    domain_to_domain_name_map = {}
    
    enum_literals_map = {}
    
    # we record a list of missing domains
    missing_domains = []
    # enumMap keeps a reference between ldm ID's for domains and
    # the enum instance
    enum_map = {}
    enum_id_enum_name_map = {}
    import_logic_strings = []

    def __init__(self):

        key_annotation_directive = ELAnnotationDirective(name='key', sourceURI='key')
        dependency_annotation_directive = ELAnnotationDirective(name='dep', sourceURI='dep')
        self.input_tables_package.annotationDirectives.append(key_annotation_directive)
        self.input_tables_package.annotationDirectives.append(dependency_annotation_directive)
        types = EcoreLiteTypes()
        self.types_package.eClassifiers.append(types.e_string)
        self.types_package.eClassifiers.append(types.e_double)
        self.types_package.eClassifiers.append(types.e_int)
        self.module_list.modules.append(self.types_package)
        self.module_list.modules.append(self.il_domains_package)
        self.module_list.modules.append(self.sdd_domains_package)
        self.module_list.modules.append(self.input_tables_package)
        self.module_list.modules.append(self.output_tables_package)
        self.module_list.modules.append(self.generation_rules_module)
        self.module_list.modules.append(self.reports_module)
        

