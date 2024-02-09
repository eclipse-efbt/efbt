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


from regdna import  ELPackage, ModuleList, ELAnnotationDirective

from ldm_context.ecore_lite_types import EcoreLiteTypes


class Context(object):
    '''
    Documentation for Context
    '''
    # variables to configure the behaviour
    types = EcoreLiteTypes()
    use_codes = True
    load_eil_from_website = False
    use_variable_long_name = True
    persist_to_regdna = True

    # the directory where we get our input files
    file_directory = ""
    # the directory where we save our outputs.
    output_directory = ""

    # create the moduleList to hold all the modules
    module_list = ModuleList()
    
    main_catogory_to_name_map = {}

    # create  regdna  packages
    types_package = ELPackage(name='types')
    il_domains_package = ELPackage(
        name='il_domains',
        nsURI='http://www.eclipse.org/bird/il_domains',
        nsPrefix='il_domains')

    input_tables_package = ELPackage(
        name='ldm_entities',
        nsURI='http://www.eclipse.org/bird/ldm_entities',
        nsPrefix='ldm_entities')

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
    
    # For the reference output layers we record a map between domains
    # and domain names
    domain_to_domain_name_map = {}

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

    import_logic_strings = []

    input_layer_name = "Input Layer 6.3"
    
    
    
    
    

    def __init__(self):

        key_annotation_directive = ELAnnotationDirective(name='keys', sourceURI='keys')
        il_mapping_annotation_directive = ELAnnotationDirective(name='il_mapping', sourceURI='il_mapping')
        entity_hierarchy_annotation_directive = ELAnnotationDirective(name='entity_hierarchy', sourceURI='entity_hierarchy')
        self.input_tables_package.annotationDirectives.append(key_annotation_directive)
        self.input_tables_package.annotationDirectives.append(il_mapping_annotation_directive)
        self.input_tables_package.annotationDirectives.append(entity_hierarchy_annotation_directive)
        types = EcoreLiteTypes()
        self.types_package.eClassifiers.append(types.e_string)
        self.types_package.eClassifiers.append(types.e_double)
        self.types_package.eClassifiers.append(types.e_int)
        self.module_list.modules.append(self.types_package)
        self.module_list.modules.append(self.il_domains_package)
        self.module_list.modules.append(self.input_tables_package)


