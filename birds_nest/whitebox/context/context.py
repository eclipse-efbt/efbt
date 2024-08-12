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


from whitebox.regdna import  ELPackage, ModuleList, GenerationRulesModule, ReportModule, ELAnnotationDirective
from whitebox.context.ecore_lite_types import EcoreLiteTypes

class Context(object):
    '''
    Documentation for Context
    '''
    # variables to configure the behaviour
    

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




    module_list = ModuleList()
    # classesMap keeps a reference between ldm ID's for classes and
    # the class instance
    classes_map = {}

    # A map between the ELDM names for primitive types types, and
    # our standard primitive types such as EString
    datatype_map = {}
    # For the reference output layers we record a map between domains
    # and domain names
    domain_to_domain_name_map = {}
    
    enum_map = {}
    
    arc_to_source_map = {}
    arc_name_to_arc_class_map = {}
    
    arc_target_to_arc_map = {}

    enums_used = []

    load_sdd_from_website =False

    def __init__(self):

        ldm_key_annotation_directive = ELAnnotationDirective(name='key', sourceURI='key')
        ldm_dependency_annotation_directive = ELAnnotationDirective(name='dep', sourceURI='dep')
        ldm_entity_hierarchy_annotation_directive = ELAnnotationDirective(name='entity_hierarchy', sourceURI='entity_hierarchy')
        ldm_relationship_type_annotation_directive = ELAnnotationDirective(name='relationship_type', sourceURI='relationship_type')
        code_annotation_directive = ELAnnotationDirective(name='code', sourceURI='code')
        long_name_directive_ldm_entities = ELAnnotationDirective(name='long_name', sourceURI='long_name')
       
        self.ldm_entities_package.annotationDirectives.append(ldm_key_annotation_directive)
        self.ldm_entities_package.annotationDirectives.append(ldm_dependency_annotation_directive)
        self.ldm_entities_package.annotationDirectives.append(ldm_entity_hierarchy_annotation_directive)
        self.ldm_entities_package.annotationDirectives.append(ldm_relationship_type_annotation_directive)
        self.ldm_entities_package.annotationDirectives.append(long_name_directive_ldm_entities)
        self.ldm_entities_package.annotationDirectives.append(code_annotation_directive)
        types = EcoreLiteTypes()
        self.types_package.eClassifiers.append(types.e_string)
        self.types_package.eClassifiers.append(types.e_double)
        self.types_package.eClassifiers.append(types.e_int)
        self.module_list.modules.append(self.types_package)
        self.module_list.modules.append(self.ldm_domains_package)
        self.module_list.modules.append(self.ldm_entities_package)




        
 
        

