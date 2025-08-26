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
from pybirdai.context.sdd_context_django import SDDContext
from pybirdai.models.bird_meta_data_model import *
from django.apps import apps
from django.db import connection
from django.db.models.fields import CharField,DateTimeField,BooleanField,FloatField,BigIntegerField
import os
import csv
from typing import List, Any
from django.db import connection

from pybirdai.process_steps.joins_meta_data.ldm_search import ELDMSearch

class TransformationMetaDataDestroyer:
    """
    A class for creating generation rules for reports and tables.
    """

    def delete_output_concepts(self, context: Any, sdd_context: Any, framework: str) -> None:
        """
        Generate generation rules for the given context and framework.

        Args:
            context (Any): The context object containing necessary data.
            sdd_context (Any): The SDD context object.
            framework (str): The framework being used (e.g., "FINREP_REF").
        """
        with connection.cursor() as cursor:
            cursor.execute("DELETE FROM pybirdai_cube_to_combination")
            cursor.execute("DELETE FROM pybirdai_combination_item")
            cursor.execute("DELETE FROM pybirdai_combination")
            cursor.execute("DELETE FROM pybirdai_cube where cube_structure_id_id like '%structure'")
            cursor.execute("DELETE FROM pybirdai_cube_structure_item where cube_structure_id_id like '%structure'")
            cursor.execute("DELETE FROM pybirdai_cube_structure where cube_structure_id like '%structure'")
            print("DELETE FROM pybirdai_cube_structure where cube_structure_id like '%structure'")

        # check if we should really delete all of these or just some.

        for key,value in sdd_context.bird_cube_dictionary.items():
            if key.endswith('_cube_structure'):
                del sdd_context.bird_cube_dictionary[key]
        for key,value in sdd_context.bird_cube_structure_item_dictionary.items():
            if key.endswith('_cube_structure'):
                del sdd_context.bird_cube_structure_item_dictionary[key]
        for key,value in sdd_context.bird_cube_structure_dictionary.items():
            if key.endswith('_cube_structure'):
                del sdd_context.bird_cube_structure_dictionary[key]

        sdd_context.combination_item_dictionary = {}
        sdd_context.combination_dictionary = {}
        sdd_context.combination_to_rol_cube_map = {}


    def delete_joins_meta_data(self, context: Any, sdd_context: Any, framework: str) -> None:
        """
        Generate generation rules for the given context and framework.

        Args:
            context (Any): The context object containing necessary data.
            sdd_context (Any): The SDD context object.
            framework (str): The framework being used (e.g., "FINREP_REF").
        """

        model_classes = [CUBE_LINK,
        CUBE_STRUCTURE_ITEM_LINK]

        for model_cls in model_classes:
            self.delete_items_for_sqlite(model_cls)



        sdd_context.cube_link_dictionary = {}
        sdd_context.cube_link_to_foreign_cube_map = {}
        sdd_context.cube_link_to_join_identifier_map = {}
        sdd_context.cube_link_to_join_for_report_id_map = {}
        sdd_context.cube_structure_item_links_dictionary = {}
        sdd_context.cube_structure_item_link_to_cube_link_map = {}


    def delete_semantic_integration_meta_data(self, context: Any, sdd_context: Any, framework: str) -> None:
        """
        Generate generation rules for the given context and framework.

        Args:
            context (Any): The context object containing necessary data.
            sdd_context (Any): The SDD context object.
            framework (str): The framework being used (e.g., "FINREP_REF").
        """

        model_classes = [MAPPING_TO_CUBE,
        MAPPING_DEFINITION,
        VARIABLE_MAPPING_ITEM,
        VARIABLE_MAPPING,
        MEMBER_MAPPING_ITEM,
        MEMBER_MAPPING]

        for model_cls in model_classes:
            self.delete_items_for_sqlite(model_cls)



        sdd_context.mapping_definition_dictionary = {}
        sdd_context.variable_mapping_dictionary = {}
        sdd_context.variable_mapping_item_dictionary = {}
        sdd_context.member_mapping_dictionary = {}
        sdd_context.member_mapping_items_dictionary = {}
        sdd_context.mapping_to_cube_dictionary = {}

        TransformationMetaDataDestroyer.delete_joins_meta_data(self,context,sdd_context,framework)

    def delete_items_for_sqlite(self,model_clss):
        with connection.cursor() as cursor:
            cursor.execute("PRAGMA foreign_keys = 0;")
            for model_cls in model_clss:
                cursor.execute(f"DELETE FROM pybirdai_{model_cls.__name__.lower()};")
            cursor.execute("PRAGMA foreign_keys = 1;")

    def delete_bird_metadata_database(self, context: Any, sdd_context: Any, framework: str) -> None:
        """
        Delete the Bird Metadata Database.
        """
        model_classes = [
            CUBE_LINK,
            CUBE_STRUCTURE_ITEM_LINK,
            CUBE_STRUCTURE_ITEM,
            CUBE_STRUCTURE,
            CUBE,
            DOMAIN,
            VARIABLE,
            MEMBER,
            MEMBER_MAPPING,
            MEMBER_MAPPING_ITEM,
            VARIABLE_MAPPING,
            VARIABLE_MAPPING_ITEM,
            TABLE_CELL,
            CELL_POSITION,
            AXIS_ORDINATE,
            ORDINATE_ITEM,
            MAPPING_DEFINITION,
            MAPPING_TO_CUBE,
            TABLE,
            CELL_POSITION,
            AXIS,
            SUBDOMAIN,
            SUBDOMAIN_ENUMERATION,
            FACET_COLLECTION,
            MAINTENANCE_AGENCY,
            FRAMEWORK,
            MEMBER_HIERARCHY,
            MEMBER_HIERARCHY_NODE,
            COMBINATION,
            COMBINATION_ITEM,
            CUBE_TO_COMBINATION
        ]
        self.delete_items_for_sqlite(model_classes)

        sdd_context.mapping_definition_dictionary = {}
        sdd_context.variable_mapping_dictionary = {}
        sdd_context.variable_mapping_item_dictionary = {}
        sdd_context.bird_cube_structure_dictionary = {}
        sdd_context.bird_cube_dictionary = {}
        sdd_context.bird_cube_structure_item_dictionary = {}
        sdd_context.mapping_to_cube_dictionary = {}
        sdd_context.agency_dictionary = {}
        sdd_context.framework_dictionary = {}
        sdd_context.domain_dictionary = {}
        sdd_context.member_dictionary = {}
        sdd_context.member_id_to_domain_map = {}
        sdd_context.member_id_to_member_code_map = {}
        sdd_context.variable_dictionary = {}
        sdd_context.variable_to_domain_map = {}
        sdd_context.variable_to_long_names_map = {}
        sdd_context.variable_to_primary_concept_map = {}
        sdd_context.member_hierarchy_dictionary = {}
        sdd_context.member_hierarchy_node_dictionary = {}
        sdd_context.report_tables_dictionary = {}
        sdd_context.axis_dictionary = {}
        sdd_context.axis_ordinate_dictionary = {}
        sdd_context.axis_ordinate_to_ordinate_items_map = {}
        sdd_context.table_cell_dictionary = {}
        sdd_context.table_to_table_cell_dictionary = {}
        sdd_context.cell_positions_dictionary = {}
        sdd_context.member_mapping_dictionary = {}
        sdd_context.member_mapping_items_dictionary = {}
        sdd_context.combination_item_dictionary = {}
        sdd_context.combination_dictionary = {}
        sdd_context.combination_to_rol_cube_map = {}
        sdd_context.cube_link_dictionary = {}
        sdd_context.cube_link_to_foreign_cube_map = {}
        sdd_context.cube_link_to_join_identifier_map = {}
        sdd_context.cube_link_to_join_for_report_id_map = {}
        sdd_context.cube_structure_item_links_dictionary = {}
        sdd_context.cube_structure_item_link_to_cube_link_map = {}
        sdd_context.subdomain_dictionary = {}
        sdd_context.subdomain_to_domain_map ={}
        sdd_context.subdomain_enumeration_dictionary = {}
        sdd_context.members_that_are_nodes = {}
        sdd_context.member_plus_hierarchy_to_child_literals = {}
        sdd_context.domain_to_hierarchy_dictionary = {}



        SDDContext.mapping_definition_dictionary = {}
        SDDContext.variable_mapping_dictionary = {}
        SDDContext.variable_mapping_item_dictionary = {}
        SDDContext.bird_cube_structure_dictionary = {}
        SDDContext.bird_cube_dictionary = {}
        SDDContext.bird_cube_structure_item_dictionary = {}
        SDDContext.mapping_to_cube_dictionary = {}
        SDDContext.agency_dictionary = {}
        SDDContext.framework_dictionary = {}
        SDDContext.domain_dictionary = {}
        SDDContext.member_dictionary = {}
        SDDContext.member_id_to_domain_map = {}
        SDDContext.member_id_to_member_code_map = {}
        SDDContext.variable_dictionary = {}
        SDDContext.variable_to_domain_map = {}
        SDDContext.variable_to_long_names_map = {}
        SDDContext.variable_to_primary_concept_map = {}
        SDDContext.member_hierarchy_dictionary = {}
        SDDContext.member_hierarchy_node_dictionary = {}
        SDDContext.report_tables_dictionary = {}
        SDDContext.axis_dictionary = {}
        SDDContext.axis_ordinate_dictionary = {}
        SDDContext.axis_ordinate_to_ordinate_items_map = {}
        SDDContext.table_cell_dictionary = {}
        SDDContext.table_to_table_cell_dictionary = {}
        SDDContext.cell_positions_dictionary = {}
        SDDContext.member_mapping_dictionary = {}
        SDDContext.member_mapping_items_dictionary = {}
        SDDContext.combination_item_dictionary = {}
        SDDContext.combination_dictionary = {}
        SDDContext.combination_to_rol_cube_map = {}
        SDDContext.cube_link_dictionary = {}
        SDDContext.cube_link_to_foreign_cube_map = {}
        SDDContext.cube_link_to_join_identifier_map = {}
        SDDContext.cube_link_to_join_for_report_id_map = {}
        SDDContext.cube_structure_item_links_dictionary = {}
        SDDContext.cube_structure_item_link_to_cube_link_map = {}
        SDDContext.subdomain_dictionary = {}
        SDDContext.subdomain_to_domain_map ={}
        SDDContext.subdomain_enumeration_dictionary = {}
        SDDContext.members_that_are_nodes = {}
        SDDContext.member_plus_hierarchy_to_child_literals = {}
        SDDContext.domain_to_hierarchy_dictionary = {}
