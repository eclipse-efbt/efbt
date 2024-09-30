# coding=UTF-8#
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
#


from pybirdai.regdna import  ELPackage, ModuleList, GenerationRulesModule, ReportModule, ELAnnotationDirective
from pybirdai.context.ecore_lite_types import EcoreLiteTypes

class Context(object):
    '''
    Documentation for Context
    '''
    # variables to configure the behaviour
    
    ldm_or_il = 'il'

    enrich_ldm_relationships = False
    use_codes = True

    # the directory where we get our input files
    file_directory = ""
    # the directory where we save our outputs.
    output_directory = ""

    types = EcoreLiteTypes()
    # create the moduleList to hold all the modules
    module_list = ModuleList()

    # create  regdna  packages
    types_package = ELPackage(name='types')
    ldm_domains_package = ELPackage(
        name='ldm_domains',
        nsURI='http://www.eclipse.org/bird/ldm_domains',
        nsPrefix='ldm_domains')
   
    ldm_entities_package = ELPackage(
        name='ldm_entities',
        nsURI='http://www.eclipse.org/bird/ldm_entities',
        nsPrefix='ldm_entities')
    
    il_domains_package = ELPackage(
        name='il_domains',
        nsURI='http://www.eclipse.org/bird/il_domains',
        nsPrefix='ldm_domains')
   
    il_tables_package = ELPackage(
        name='il_entities',
        nsURI='http://www.eclipse.org/bird/il_entities',
        nsPrefix='il_entities')
    

    
    reports_dictionary = {}

    enum_literals_map = {}

    module_list = ModuleList()
    # classesMap keeps a reference between ldm ID's for classes and
    # the class instance
    classes_map = {}

    table_map = {}

    fk_to_mandatory_map = {}

    fk_to_column_map = {}

    # A map between the ELDM names for primitive types types, and
    # our standard primitive types such as EString
    datatype_map = {}

    main_category_to_name_map_finrep = {}
    main_category_to_name_map_ae = {}

    table_parts_to_main_category_map_finrep = {}
    table_parts_to_main_category_map_ae = {}
    
    tables_for_main_category_map_finrep = {}
    tables_for_main_category_map_ae = {}

    table_parts_to_linked_tables_map_finrep = {}
    table_parts_to_linked_tables_map_ae = {}

    table_parts_to_to_filter_map_finrep = {}
    table_parts_to_to_filter_map_ae = {}

    table_and_part_tuple_map_finrep = {}
    table_and_part_tuple_map_ae = {}

    ldm_entity_to_linked_tables_map = {}
    report_to_main_category_map = {}
    enum_map = {}
    
    arc_to_source_map = {}
    arc_name_to_arc_class_map = {}
    
    arc_target_to_arc_map = {}

    enums_used = []

    main_categories_in_scope_finrep = []
    main_categories_in_scope_ae = []

    load_sdd_from_website =False

    save_derived_sdd_items = True

    def __init__(self):

        ldm_key_annotation_directive = ELAnnotationDirective(name='key', sourceURI='key')
        ldm_dependency_annotation_directive = ELAnnotationDirective(name='dep', sourceURI='dep')
        ldm_entity_hierarchy_annotation_directive = ELAnnotationDirective(name='entity_hierarchy', sourceURI='entity_hierarchy')
        ldm_relationship_type_annotation_directive = ELAnnotationDirective(name='relationship_type', sourceURI='relationship_type')
        code_annotation_directive = ELAnnotationDirective(name='code', sourceURI='code')
        long_name_directive_ldm_entities = ELAnnotationDirective(name='long_name', sourceURI='long_name')

        il_key_annotation_directive = ELAnnotationDirective(name='key', sourceURI='key')
        il_dependency_annotation_directive = ELAnnotationDirective(name='dep', sourceURI='dep')
        il_entity_hierarchy_annotation_directive = ELAnnotationDirective(name='entity_hierarchy', sourceURI='entity_hierarchy')
        il_relationship_type_annotation_directive = ELAnnotationDirective(name='relationship_type', sourceURI='relationship_type')
        il_code_annotation_directive = ELAnnotationDirective(name='code', sourceURI='code')
        long_name_directive_il_entities = ELAnnotationDirective(name='long_name', sourceURI='long_name')

       
        self.ldm_entities_package.annotationDirectives.append(ldm_key_annotation_directive)
        self.ldm_entities_package.annotationDirectives.append(ldm_dependency_annotation_directive)
        self.ldm_entities_package.annotationDirectives.append(ldm_entity_hierarchy_annotation_directive)
        self.ldm_entities_package.annotationDirectives.append(ldm_relationship_type_annotation_directive)
        self.ldm_entities_package.annotationDirectives.append(long_name_directive_ldm_entities)
        self.ldm_entities_package.annotationDirectives.append(code_annotation_directive)

        self.il_tables_package.annotationDirectives.append(il_key_annotation_directive)
        self.il_tables_package.annotationDirectives.append(il_dependency_annotation_directive)
        self.il_tables_package.annotationDirectives.append(il_entity_hierarchy_annotation_directive)
        self.il_tables_package.annotationDirectives.append(il_relationship_type_annotation_directive)
        self.il_tables_package.annotationDirectives.append(long_name_directive_il_entities)
        self.il_tables_package.annotationDirectives.append(il_code_annotation_directive)

        types = EcoreLiteTypes()
        self.types_package.eClassifiers.append(types.e_string)
        self.types_package.eClassifiers.append(types.e_double)
        self.types_package.eClassifiers.append(types.e_int)
        self.module_list.modules.append(self.types_package)
        self.module_list.modules.append(self.ldm_domains_package)
        self.module_list.modules.append(self.ldm_entities_package)

        




        
 
        

