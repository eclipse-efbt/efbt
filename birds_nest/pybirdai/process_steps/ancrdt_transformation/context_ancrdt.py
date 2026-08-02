# coding=UTF-8
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


from pybirdai.context.context import ANNOTATION_DIRECTIVE_NAMES
from pybirdai.model_blueprint import AnnotationDirective, ModelPackage, PrimitiveTypes

class Context:
    '''
    Documentation for Context
    '''
    # variables to configure the behaviour

    ldm_or_il = 'il'
    alternative_folder_for_subdomains = 'sqldev_subdomains'

    enrich_ldm_relationships = False
    use_codes = True

    reference_data_class_list = []

    # the directory where we get our input files
    file_directory = ""
    # the directory where we save our outputs.
    output_directory = ""

    types = PrimitiveTypes()

    # the model blueprint packages built during stage one of the import
    types_package = ModelPackage(name='types')
    ldm_domains_package = ModelPackage(
        name='ldm_domains',
        ns_uri='http://www.eclipse.org/bird/ldm_domains',
        ns_prefix='ldm_domains')

    ldm_entities_package = ModelPackage(
        name='ldm_entities',
        ns_uri='http://www.eclipse.org/bird/ldm_entities',
        ns_prefix='ldm_entities')

    il_domains_package = ModelPackage(
        name='il_domains',
        ns_uri='http://www.eclipse.org/bird/il_domains',
        ns_prefix='ldm_domains')

    il_tables_package = ModelPackage(
        name='il_entities',
        ns_uri='http://www.eclipse.org/bird/il_entities',
        ns_prefix='il_entities')


    skip_reference_data_in_ldm = True
    reports_dictionary = {}

    classification_types = {}

    enum_literals_map = {}

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

    join_for_products_to_main_category_map_finrep = {}
    join_for_products_to_main_category_map_ae = {}

    tables_for_main_category_map_finrep = {}
    tables_for_main_category_map_ae = {}

    join_for_products_to_linked_tables_map_finrep = {}
    join_for_products_to_linked_tables_map_ae = {}

    join_for_products_to_to_filter_map_finrep = {}
    join_for_products_to_to_filter_map_ae = {}

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

        for directive_name in ANNOTATION_DIRECTIVE_NAMES:
            self.ldm_entities_package.annotation_directives.append(
                AnnotationDirective(name=directive_name, source_uri=directive_name))
            self.il_tables_package.annotation_directives.append(
                AnnotationDirective(name=directive_name, source_uri=directive_name))

        types = PrimitiveTypes()
        self.types_package.add_classifier(types.e_string)
        self.types_package.add_classifier(types.e_double)
        self.types_package.add_classifier(types.e_int)
