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

from pybirdai.bird_meta_data_model import *
from django.apps import apps
from django.db.models.fields import CharField,DateTimeField,BooleanField,FloatField,BigIntegerField
import os
import csv
from typing import List, Any

from pybirdai.process_steps.joins_meta_data.ldm_search import ELDMSearch

class TransformationMetaDataDestroyer:
    """
    A class for creating generation rules for reports and tables.
    """

    def delete_joins_meta_data(self, context: Any, sdd_context: Any, framework: str) -> None:
        """
        Generate generation rules for the given context and framework.

        Args:
            context (Any): The context object containing necessary data.
            sdd_context (Any): The SDD context object.
            framework (str): The framework being used (e.g., "FINREP_REF").
        """
        CUBE_LINK.objects.all().delete()
        CUBE_STRUCTURE_ITEM_LINK.objects.all().delete()

    def delete_bird_metadata_database(self, context: Any, sdd_context: Any, framework: str) -> None:
        """
        Delete the Bird Metadata Database.
        """
        CUBE_LINK.objects.all().delete()
        CUBE_STRUCTURE_ITEM_LINK.objects.all().delete()
        CUBE_STRUCTURE_ITEM.objects.all().delete()
        CUBE_STRUCTURE.objects.all().delete()
        CUBE.objects.all().delete()
        DOMAIN.objects.all().delete()
        VARIABLE.objects.all().delete()
        MEMBER.objects.all().delete()
        MEMBER_MAPPING.objects.all().delete()
        MEMBER_MAPPING_ITEM.objects.all().delete()
        VARIABLE_MAPPING.objects.all().delete()
        VARIABLE_MAPPING_ITEM.objects.all().delete()
        TABLE_CELL.objects.all().delete()
        CELL_POSITION.objects.all().delete()
        AXIS_ORDINATE.objects.all().delete()
        ORDINATE_ITEM.objects.all().delete()
        MAPPING_DEFINITION.objects.all().delete()
        MAPPING_TO_CUBE.objects.all().delete()
        TABLE.objects.all().delete()
        CELL_POSITION.objects.all().delete()
        AXIS.objects.all().delete()
        SUBDOMAIN.objects.all().delete()
        SUBDOMAIN_ENUMERATION.objects.all().delete()
        FACET_COLLECTION.objects.all().delete()
        MAINTENANCE_AGENCY.objects.all().delete()
        FRAMEWORK.objects.all().delete()
        MEMBER_HIERARCHY.objects.all().delete()
        MEMBER_HIERARCHY_NODE.objects.all().delete()
        COMBINATION.objects.all().delete()
        COMBINATION_ITEM.objects.all().delete()
        CUBE_TO_COMBINATION.objects.all().delete()

        context.mapping_definition_dictionary = {}
        context.variable_mapping_dictionary = {}
        context.variable_mapping_item_dictionary = {}
        context.bird_cube_structure_dictionary = {}
        context.bird_cube_dictionary = {}
        context.bird_cube_structure_item_dictionary = {}
        context.mapping_to_cube_dictionary = {}
        context.agency_dictionary = {}
        context.framework_dictionary = {}
        context.domain_dictionary = {}
        context.member_dictionary = {}
        context.member_id_to_domain_map = {}
        context.member_id_to_member_code_map = {}
        context.variable_dictionary = {}
        context.variable_to_domain_map = {}
        context.variable_to_long_names_map = {}
        context.variable_to_primary_concept_map = {}
        context.member_hierarchy_dictionary = {}
        context.member_hierarchy_node_dictionary = {}
        context.report_tables_dictionary = {}
        context.axis_dictionary = {}
        context.axis_ordinate_dictionary = {}
        sdd_context.axis_ordinate_to_ordinate_items_map = {}
        context.table_cell_dictionary = {}
        context.table_to_table_cell_dictionary = {}
        context.cell_positions_dictionary = {}
        context.member_mapping_dictionary = {}
        context.member_mapping_items_dictionary = {}
        context.combination_item_dictionary = {}
        context.combination_dictionary = {}
        context.combination_to_rol_cube_map = {}
        context.cube_link_dictionary = {}
        context.cube_link_to_foreign_cube_map = {}
        context.cube_link_to_join_identifier_map = {}
        context.cube_link_to_join_for_report_id_map = {}
        context.cube_structure_item_links_dictionary = {}
        context.cube_structure_item_link_to_cube_link_map = {}
        context.subdomain_dictionary = {}
        context.subdomain_to_domain_map ={}
        context.subdomain_enumeration_dictionary = {}
        context.members_that_are_nodes = {}
        context.member_plus_hierarchy_to_child_literals = {}
        context.domain_to_hierarchy_dictionary = {}



        