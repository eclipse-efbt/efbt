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
    
    input_layer_name_has_EIL_postfix = False
    use_codes = True
    load_ldm = False
    use_variable_long_name = True
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
    
    main_catogory_to_name_map_finrep = {}
    main_catogory_to_name_map_ae = {}
    typ_instrmnt_to_name_map = {}
    typ_instrmnt_to_name_map = {}
    
    report_to_main_catogory_map = {}
    report_to_typ_instrmnt_map = {}
    tables_for_main_catagory_map_finrep = {}
    tables_for_main_catagory_map_ae = {}
    entities_for_typ_instrmnt_map = {}
    ldm_entity_to_first_linked_entities_map = {}
    table_and_part_tuple_map_finrep = {}
    table_and_part_tuple_map_ae = {}
    table_parts_to_linked_tables_map_finrep = {}
    table_parts_to_linked_tables_map_ae = {}
    table_parts_to_to_filter_map_finrep = {}
    table_parts_to_to_filter_map_ae = {}
    #table_parts_to_to_filter_items_map = {}
    table_parts_to_main_catagory_map_finrep = {}
    table_parts_to_main_catagory_map_ae = {}
    table_parts_to_typ_instrmnt_map = {}
    ldm_entity_to_linked_tables_map = {}
    main_catagories_in_scope_finrep = []
    main_catagories_in_scope_ae = []
    typ_instrmnt_in_scope = []
    table_part_varaible_transformation_map = {}

    # create  regdna  packages
    types_package = ELPackage(name='types')
    ldm_domains_package = ELPackage(
        name='ldm_domains',
        nsURI='http://www.eclipse.org/bird/ldm_domains',
        nsPrefix='ldm_domains')
    il_domains_package = ELPackage(
        name='il_domains',
        nsURI='http://www.eclipse.org/bird/il_domains',
        nsPrefix='il_domains')
    finrep_domains_package = ELPackage(
        name='finrep_domains',
        nsURI='http://www.eclipse.org/bird/finrep_domains',
        nsPrefix='finrep_domains')
    ae_domains_package = ELPackage(
        name='ae_domains',
        nsURI='http://www.eclipse.org/bird/ae_domains',
        nsPrefix='ae_domains')
    input_tables_package = ELPackage(
        name='input_tables',
        nsURI='http://www.eclipse.org/bird/input_tables',
        nsPrefix='input_tables')
    ldm_entities_package = ELPackage(
        name='ldm_entities',
        nsURI='http://www.eclipse.org/bird/ldm_entities',
        nsPrefix='ldm_entities')
    finrep_output_tables_package = ELPackage(
        name='finrep_output_tables',
        nsURI='http://www.eclipse.org/bird/finrep_output_tables',
        nsPrefix='finrep_output_tables')
    ae_output_tables_package = ELPackage(
        name='ae_output_tables',
        nsURI='http://www.eclipse.org/bird/ae_output_tables',
        nsPrefix='ae_output_tables')

    
    
    finrep_generation_rules_module = GenerationRulesModule(name='finrep_generation_rules')
    ae_generation_rules_module = GenerationRulesModule(name='ae_generation_rules')
    finrep_on_sdd_reports_module = ReportModule(name='finrep_report_sdd')
    ae_on_sdd_reports_module = ReportModule(name='ae_report_sdd')
    finrep_on_il_reports_module = ReportModule(name='finrep_report_il')
    ae_on_il_reports_module = ReportModule(name='ae_report_il')
    finrep_on_ldm_reports_module = ReportModule(name='finrep_report_ldm')
    ae_on_ldm_reports_module = ReportModule(name='ae_report_ldm')
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
    
    arc_to_source_map = {}
    arc_name_to_arc_class_map = {}
    
    arc_target_to_arc_map = {}

    def __init__(self):

        il_key_annotation_directive = ELAnnotationDirective(name='key', sourceURI='key')
        il_dependency_annotation_directive = ELAnnotationDirective(name='dep', sourceURI='dep')
        il_entity_hierarchy_annotation_directive = ELAnnotationDirective(name='entity_hierarchy', sourceURI='entity_hierarchy')
        il_relationship_type_annotation_directive = ELAnnotationDirective(name='relationship_type', sourceURI='relationship_type')
        ldm_key_annotation_directive = ELAnnotationDirective(name='key', sourceURI='key')
        ldm_dependency_annotation_directive = ELAnnotationDirective(name='dep', sourceURI='dep')
        ldm_entity_hierarchy_annotation_directive = ELAnnotationDirective(name='entity_hierarchy', sourceURI='entity_hierarchy')
        ldm_relationship_type_annotation_directive = ELAnnotationDirective(name='relationship_type', sourceURI='relationship_type')
        long_name_directive_input_tables = ELAnnotationDirective(name='long_name', sourceURI='long_name')
        long_name_directive_ldm_entities = ELAnnotationDirective(name='long_name', sourceURI='long_name')
        long_name_directive_finrep_output_tables_sdd = ELAnnotationDirective(name='long_name', sourceURI='long_name')
        long_name_directive_ae_output_tables_sdd = ELAnnotationDirective(name='long_name', sourceURI='long_name')
        self.input_tables_package.annotationDirectives.append(il_key_annotation_directive)
        self.input_tables_package.annotationDirectives.append(il_dependency_annotation_directive)
        self.input_tables_package.annotationDirectives.append(il_entity_hierarchy_annotation_directive)
        self.input_tables_package.annotationDirectives.append(il_relationship_type_annotation_directive)
        self.input_tables_package.annotationDirectives.append(long_name_directive_input_tables)
        self.ldm_entities_package.annotationDirectives.append(ldm_key_annotation_directive)
        self.ldm_entities_package.annotationDirectives.append(ldm_dependency_annotation_directive)
        self.ldm_entities_package.annotationDirectives.append(ldm_entity_hierarchy_annotation_directive)
        self.ldm_entities_package.annotationDirectives.append(ldm_relationship_type_annotation_directive)
        self.ldm_entities_package.annotationDirectives.append(long_name_directive_ldm_entities)
        self.finrep_output_tables_package.annotationDirectives.append(long_name_directive_finrep_output_tables_sdd)
        self.ae_output_tables_package.annotationDirectives.append(long_name_directive_ae_output_tables_sdd)
        types = EcoreLiteTypes()
        self.types_package.eClassifiers.append(types.e_string)
        self.types_package.eClassifiers.append(types.e_double)
        self.types_package.eClassifiers.append(types.e_int)
        self.module_list.modules.append(self.types_package)
        self.module_list.modules.append(self.ldm_domains_package)
        self.module_list.modules.append(self.il_domains_package)
        self.module_list.modules.append(self.finrep_domains_package)
        self.module_list.modules.append(self.ae_domains_package)
        self.module_list.modules.append(self.input_tables_package)
        self.module_list.modules.append(self.ldm_entities_package)
        self.module_list.modules.append(self.finrep_output_tables_package)
        self.module_list.modules.append(self.ae_output_tables_package)
        self.module_list.modules.append(self.finrep_generation_rules_module)
        self.module_list.modules.append(self.ae_generation_rules_module)
        self.module_list.modules.append(self.finrep_on_sdd_reports_module)
        self.module_list.modules.append(self.ae_on_sdd_reports_module)


        
 
        

