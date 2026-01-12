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
#    Benjamin Arfa - improvements and enhancements
#

import logging
from pybirdai.models.bird_meta_data_model import *
from django.apps import apps
from django.db.models.fields import (
    CharField,
    DateTimeField,
    BooleanField,
    FloatField,
    BigIntegerField,
)
import os
import json
import csv
from typing import List, Any, Tuple
from pybirdai.process_steps.website_to_sddmodel.constants import BULK_CREATE_BATCH_SIZE_DEFAULT
from pybirdai.process_steps.joins_meta_data.member_hierarchy_service import (
    MemberHierarchyService,
)
import itertools
import traceback

from pybirdai.process_steps.joins_meta_data.ldm_search import ELDMSearch

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    filename="create_joins_metadata.log",
    filemode="w",  # Use 'w' to overwrite the log file each run
)


class JoinsMetaDataCreator:
    """
    A class for creating generation rules for reports and tables.
    """

    def generate_joins_meta_data(
        self, context: Any, sdd_context: Any, framework: str
    ) -> None:
        """
        Generate generation rules for the given context and framework.

        Args:
            context (Any): The context object containing necessary data.
            sdd_context (Any): The SDD context object.
            framework (str): The framework being used (e.g., "FINREP_REF").
        """
        self.add_reports(context, sdd_context, framework)

    def do_stuff_and_prepare_context(self, context: Any, sdd_context: Any):

        self.member_hierarchy_service = MemberHierarchyService()
        sdd_context = self.member_hierarchy_service.prepare_node_dictionaries_and_lists(
            sdd_context
        )

        # Store member IDs (strings) instead of MEMBER objects to avoid excessive hashing
        all_main_categories_pks = set(sum(context.report_to_main_category_map.values(), []))

        # Structure: dict[str, dict[str, dict[str, str]]]
        # category_id -> combination_id -> variable_id -> member_id
        context.category_to_ci = {}
        # Structure: dict[str, set[str]]
        # domain_id -> set of member_ids
        context.domain_to_member = {}

        for combination in COMBINATION.objects.prefetch_related(
            "combination_item_set__variable_id__domain_id",
            "combination_item_set__member_id__domain_id"):
            combination_items = combination.combination_item_set.all()

            # Pre-extract foreign key values in a single pass to avoid repeated descriptor calls
            items_data = [
                (item.variable_id.variable_id if item.variable_id else None,
                 item.member_id.member_id if item.member_id else None)
                for item in combination_items
            ]

            # Check if any member_id is in main categories
            if any(member_id and member_id in all_main_categories_pks for _, member_id in items_data):
                # Build result dict with IDs
                result = {
                    var_id: mem_id
                    for var_id, mem_id in items_data
                    if var_id and mem_id
                }
                # Find the category (member_id that is in all_main_categories)
                category_id = next(
                    (member_id for member_id in result.values() if member_id in all_main_categories_pks),
                    None
                )
                if category_id:
                    if category_id not in context.category_to_ci:
                        context.category_to_ci[category_id] = {}
                    context.category_to_ci[category_id][combination.combination_id] = result

        for member in MEMBER.objects.select_related("domain_id"):
            if member.domain_id:
                domain_id = member.domain_id.domain_id
                if domain_id not in context.domain_to_member:
                    context.domain_to_member[domain_id] = set()
                context.domain_to_member[domain_id].add(member.member_id)

        # Store variable IDs (strings) instead of VARIABLE objects
        context.facetted_items = {
            output_item.variable_id.variable_id
            for output_item in CUBE_STRUCTURE_ITEM.objects.select_related('variable_id__domain_id')
            if output_item.variable_id
            and output_item.variable_id.domain_id
            and output_item.variable_id.domain_id.domain_id in ["String", "Date", "Integer", "Boolean", "Float"]
        }

        return context, sdd_context

    def add_reports(self, context: Any, sdd_context: Any, framework: str) -> None:
        """
        Add reports based on the given context and framework.

        Args:
            context (Any): The context object containing necessary data.
            sdd_context (Any): The SDD context object.
            framework (str): The framework being used (e.g., "FINREP_REF").
        """
        file_location = os.path.join(
            context.file_directory,
            "joins_configuration",
            f"in_scope_reports_{framework}.csv",
        )
        if context.ldm_or_il == "ldm":
            self.create_ldm_entity_to_linked_entities_map(context, sdd_context)

        context, sdd_context = self.do_stuff_and_prepare_context(context, sdd_context)

        with open(file_location, encoding="utf-8") as csvfile:
            filereader = csv.reader(csvfile, delimiter=",", quotechar='"')
            next(filereader)  # Skip header
            for row in filereader:
                report_template = row[0]
                generated_output_layer = self.find_output_layer_cube(
                    sdd_context, report_template, framework
                )
                if generated_output_layer:
                    self.add_join_for_products_il(
                        context, sdd_context, generated_output_layer, framework
                    )

    def create_ldm_entity_to_linked_entities_map(
        self, context: Any, sdd_context: Any
    ) -> None:
        """
        Create a mapping of LDM entities to their linked entities.

        Args:
            context (Any): The context object containing necessary data.
            sdd_context (Any): The SDD context object.
        """
        output_file = os.path.join(
            context.output_directory, "csv", "ldm_entity_related_entities.csv"
        )
        memoization_parents_from_disjoint_subtyping_eldm_search = {}
        with open(output_file, "w", encoding="utf-8") as f:
            f.write("ldm_entity,related_entities\n")
            for model in apps.get_models():
                if model._meta.app_label == "pybirdai":
                    entities = ELDMSearch.get_all_related_entities(self, context, model, memoization_parents_from_disjoint_subtyping_eldm_search)
                    related_entities_string = ":".join(
                        entity.__name__ for entity in entities
                    )
                    f.write(f"{model.__name__},{related_entities_string}\n")
                    context.ldm_entity_to_linked_tables_map[model.__name__] = (
                        related_entities_string
                    )

    # Map framework names to their suffixes for context attribute lookup
    FRAMEWORK_SUFFIXES = {
        "FINREP_REF": "finrep",
        "AE_REF": "ae",
        "COREP_REF": "corep",
    }

    def _get_framework_map(self, context: Any, map_name: str, framework: str) -> Any:
        """
        Get the appropriate framework-specific map from context.

        Args:
            context: The context object
            map_name: Base name of the map (e.g., 'tables_for_main_category_map')
            framework: Framework name (e.g., 'FINREP_REF', 'AE_REF', 'COREP_REF')

        Returns:
            The framework-specific map from context
        """
        suffix = self.FRAMEWORK_SUFFIXES.get(framework)
        if not suffix:
            raise ValueError(f"Unsupported framework: {framework}. "
                           f"Supported frameworks: {list(self.FRAMEWORK_SUFFIXES.keys())}")
        attr_name = f"{map_name}_{suffix}"
        if not hasattr(context, attr_name):
            raise AttributeError(f"Context missing attribute '{attr_name}' for framework {framework}")
        return getattr(context, attr_name)

    def add_join_for_products_il(
        self,
        context: Any,
        sdd_context: Any,
        generated_output_layer: Any,
        framework: str,
    ) -> None:
        """
        Add join for products for the input layer.

        Args:
            context (Any): The context object containing necessary data.
            sdd_context (Any): The SDD context object.
            generated_output_layer (Any): The generated output layer.
            framework (str): The framework being used (e.g., "FINREP_REF", "AE_REF", "COREP_REF").
        """
        tables_for_main_category_map = self._get_framework_map(
            context, 'tables_for_main_category_map', framework
        )
        join_for_products_to_linked_tables_map = self._get_framework_map(
            context, 'join_for_products_to_linked_tables_map', framework
        )
        table_and_part_tuple_map = self._get_framework_map(
            context, 'table_and_part_tuple_map', framework
        )
        cube_links_to_create = []  # New list to collect CUBE_LINK objects
        cube_structure_item_links_to_create = []  # New list for CUBE_STRUCTURE_ITEM_LINK objects
        num_of_cube_link_items = 0

        # Load bird_cube_dictionary if empty (needed for primary_cube lookup)
        # Only load cubes from the input layer framework (BIRD_EIL or ELDM)
        if len(sdd_context.bird_cube_dictionary) == 0:
            input_framework = "ELDM" if context.ldm_or_il == "ldm" else "BIRD_EIL"
            for cube in CUBE.objects.filter(framework_id=input_framework):
                sdd_context.bird_cube_dictionary[cube.cube_id] = cube

        # Only rebuild dictionary if empty - no new CSIs are created during this process
        # (CUBE_LINK and CUBE_STRUCTURE_ITEM_LINK are created, but not CUBE_STRUCTURE_ITEM)
        if len(sdd_context.bird_cube_structure_item_dictionary) == 0:
            for cube_structure_item in CUBE_STRUCTURE_ITEM.objects.select_related('cube_structure_id', 'variable_id', 'variable_id__domain_id', 'subdomain_id').all():
                cube_structure_key = cube_structure_item.cube_structure_id.cube_structure_id
                if cube_structure_key not in sdd_context.bird_cube_structure_item_dictionary.keys():
                    sdd_context.bird_cube_structure_item_dictionary[cube_structure_key] = []
                sdd_context.bird_cube_structure_item_dictionary[cube_structure_key].append(cube_structure_item)

        try:
            # Use cube_id as report_template since report_to_main_category_map is keyed by cube_id
            report_template = generated_output_layer.cube_id

            main_categories = context.report_to_main_category_map[report_template]
            for mc in main_categories:
                # Defensive check: skip if main category not in map
                if mc not in tables_for_main_category_map:
                    logging.warning(f"no tables for main category:{mc}")
                    continue
                try:
                    tables = tables_for_main_category_map[mc]
                    for table in tables:
                        inputLayerTable = self.find_input_layer_cube(
                            sdd_context, table, framework
                        )

                        # PRE-EXTRACT FK value to avoid repeated descriptor calls in nested loops
                        inputLayerTable_cube_structure_id = inputLayerTable.cube_structure_id if inputLayerTable else None

                        join_for_products = table_and_part_tuple_map[mc]

                        for join_for_product in join_for_products:
                            # print(f"join_for_product:{join_for_product}")
                            # print(inputLayerTable)
                            input_entity_list = [(inputLayerTable, inputLayerTable_cube_structure_id)]
                            linked_tables = join_for_products_to_linked_tables_map[
                                join_for_product
                            ]
                            linked_tables_list = linked_tables.split(":")
                            if (
                                inputLayerTable
                                and inputLayerTable_cube_structure_id
                                not in linked_tables_list
                            ):
                                linked_tables_list.append(
                                    inputLayerTable_cube_structure_id
                                )
                            extra_tables = []
                            for the_table in linked_tables_list:
                                extra_linked_tables = []
                                try:
                                    # if the_table.endswith("_ELDM"):
                                    extra_linked_tables_string = (
                                        context.ldm_entity_to_linked_tables_map[
                                            the_table
                                        ]
                                    )
                                    # else:
                                    #    extra_linked_tables_string = context.ldm_entity_to_linked_tables_map[the_table + "_ELDM"]
                                    extra_linked_tables = (
                                        extra_linked_tables_string.split(":")
                                    )
                                except KeyError:
                                    pass

                                for extra_table in extra_linked_tables:
                                    if extra_table not in extra_tables:
                                        extra_tables.append(extra_table)

                            for extra_table in extra_tables:
                                if extra_table not in linked_tables_list:
                                    linked_tables_list.append(extra_table)

                            for the_table in linked_tables_list:
                                the_input_table = self.find_input_layer_cube(
                                    sdd_context, the_table, framework
                                )
                                if the_input_table:
                                    # Pre-extract cube_structure_id and store as tuple
                                    the_input_table_cube_structure_id = the_input_table.cube_structure_id
                                    input_entity_list.append((the_input_table, the_input_table_cube_structure_id))

                            if join_for_product[0] == table:
                                for input_entity, input_entity_cube_structure_id in input_entity_list:
                                    # print(f"input_entity:{input_entity}")
                                    cube_link = CUBE_LINK()
                                    cube_link.description = f"{join_for_product[0]}:{mc}:{join_for_product[1]}:{input_entity_cube_structure_id}"
                                    cube_link.name = f"{join_for_product[0]}:{join_for_product[1]}:{input_entity_cube_structure_id}"
                                    cube_link.join_identifier = join_for_product[1]
                                    primary_cube = sdd_context.bird_cube_dictionary.get(
                                        input_entity_cube_structure_id
                                    )
                                    if primary_cube:
                                        cube_link.primary_cube_id = primary_cube
                                        cube_link.cube_link_id = (
                                            f"{report_template}:"
                                            f"{input_entity_cube_structure_id}:{join_for_product[1]}"
                                        )
                                    else:
                                        cube_link.cube_link_id = f"{input_entity_cube_structure_id}:{join_for_product[1]}"
                                        # print(f"cube_link.primary_cube_id not found for {table}")
                                    cube_link.foreign_cube_id = generated_output_layer

                                    if (
                                        cube_link.cube_link_id
                                        not in sdd_context.cube_link_dictionary
                                    ):
                                        sdd_context.cube_link_dictionary[
                                            cube_link.cube_link_id
                                        ] = cube_link
                                        foreign_cube = cube_link.foreign_cube_id
                                        join_identifier = cube_link.join_identifier
                                        join_for_report_id = (
                                            foreign_cube.cube_id
                                            + ":"
                                            + cube_link.join_identifier
                                        )

                                        if (
                                            foreign_cube.cube_id
                                            not in sdd_context.cube_link_to_foreign_cube_map
                                        ):
                                            sdd_context.cube_link_to_foreign_cube_map[
                                                foreign_cube.cube_id
                                            ] = []
                                        sdd_context.cube_link_to_foreign_cube_map[
                                            foreign_cube.cube_id
                                        ].append(cube_link)

                                        if (
                                            join_identifier
                                            not in sdd_context.cube_link_to_join_identifier_map
                                        ):
                                            sdd_context.cube_link_to_join_identifier_map[
                                                join_identifier
                                            ] = []
                                        sdd_context.cube_link_to_join_identifier_map[
                                            join_identifier
                                        ].append(cube_link)

                                        if (
                                            join_for_report_id
                                            not in sdd_context.cube_link_to_join_for_report_id_map
                                        ):
                                            sdd_context.cube_link_to_join_for_report_id_map[
                                                join_for_report_id
                                            ] = []
                                        sdd_context.cube_link_to_join_for_report_id_map[
                                            join_for_report_id
                                        ].append(cube_link)

                                        num_of_cube_link_items = self.add_field_to_field_lineage_to_rules_for_join_for_product(
                                            context,
                                            sdd_context,
                                            generated_output_layer,
                                            input_entity,
                                            mc,
                                            report_template,
                                            framework,
                                            cube_link,
                                            cube_structure_item_links_to_create,
                                        )

                                        if (
                                            context.save_derived_sdd_items
                                            and num_of_cube_link_items > 0
                                        ):
                                            cube_links_to_create.append(cube_link)

                except KeyError:
                    logging.warning(f"no tables for main category:{mc}")
        except KeyError:
            logging.warning(f"no main category for report :{report_template}")

        # Phase 1: Bulk create CUBE_LINK objects
        if context.save_derived_sdd_items and cube_links_to_create:
            # Delete existing cube links and their item links for idempotent re-runs
            cube_link_ids = [cl.cube_link_id for cl in cube_links_to_create]
            # First delete CUBE_STRUCTURE_ITEM_LINK (references CUBE_LINK via FK)
            CUBE_STRUCTURE_ITEM_LINK.objects.filter(cube_link_id__cube_link_id__in=cube_link_ids).delete()
            # Then delete CUBE_LINK
            CUBE_LINK.objects.filter(cube_link_id__in=cube_link_ids).delete()

            CUBE_LINK.objects.bulk_create(cube_links_to_create, batch_size=BULK_CREATE_BATCH_SIZE_DEFAULT)

            # Reload cube_links from database to get their assigned PKs
            # (bulk_create doesn't update in-memory objects with PKs)
            cube_link_ids = [cl.cube_link_id for cl in cube_links_to_create]
            saved_cube_links = {
                cl.cube_link_id: cl
                for cl in CUBE_LINK.objects.filter(cube_link_id__in=cube_link_ids)
            }

            # Phase 2: Update CUBE_STRUCTURE_ITEM_LINK references to use saved CUBE_LINK objects
            for csil in cube_structure_item_links_to_create:
                csil.cube_link_id = saved_cube_links[csil.cube_link_id.cube_link_id]

        # Phase 3: Save CUBE_STRUCTURE_ITEM_LINK objects in bulk for better performance
        if context.save_derived_sdd_items:
            CUBE_STRUCTURE_ITEM_LINK.objects.bulk_create(
                cube_structure_item_links_to_create,
                batch_size=BULK_CREATE_BATCH_SIZE_DEFAULT,
                ignore_conflicts=True
            )

    def add_field_to_field_lineage_to_rules_for_join_for_product(
        self,
        context: Any,
        sdd_context: Any,
        output_entity: Any,
        input_entity: Any,
        category: str,
        report_template: str,
        framework: str,
        cube_link: Any,
        cube_structure_item_links_to_create: List,
    ) -> None:
        """
        Add field-to-field lineage rules for a join for product.

        Args:
            context (Any): The context object containing necessary data.
            sdd_context (Any): The SDD context object.
            output_entity (Any): The output entity.
            input_entity_list (List[Any]): List of input entities.
            category (str): The category of the report.
            report_template (str): The report template name.
            framework (str): The framework being used (e.g., "FINREP_REF").
            cube_link (Any): The cube link object.
        """
        num_of_cube_link_items = 0

        # Initialize operation_exists cache if not already present
        if not hasattr(context, 'operation_exists_cache'):
            context.operation_exists_cache = {}

        cube_structure_key = output_entity.cube_id + "_cube_structure"
        # Defensive check: skip if cube structure not found
        if cube_structure_key not in sdd_context.bird_cube_structure_item_dictionary:
            logging.warning(f"No cube structure items for: {cube_structure_key}. Ensure output layers are created first.")
            return 0
        output_structure = sdd_context.bird_cube_structure_item_dictionary[
            cube_structure_key
        ]


        # PRE-EXTRACT all variable FK values from output_structure to avoid ~547K descriptor calls
        # This is called 3,644 times with avg 50 items each
        output_structure_data = []
        for output_item in output_structure:
            variable = output_item.variable_id
            variable_id_str = variable.variable_id if variable else None

            if variable_id_str:  # Only keep items with valid variable_id
                output_structure_data.append({
                    'output_item': output_item,
                    'variable': variable,
                    'variable_id_str': variable_id_str
                })

        for data in output_structure_data:
            output_item = data['output_item']
            variable_id_str = data['variable_id_str']

            # Check cache for operation_exists result
            cache_key = (variable_id_str, category, report_template)
            if cache_key not in context.operation_exists_cache:
                context.operation_exists_cache[cache_key] = self.operation_exists_in_cell_for_report_with_category(
                    context, sdd_context, output_item, category, report_template
                )

            operation_exists = context.operation_exists_cache[cache_key]

            # Compare variable_id string since facetted_items now stores IDs
            in_facetted_items = variable_id_str in context.facetted_items

            if operation_exists or in_facetted_items:
                input_columns = self.find_variables_with_same_members_then_same_name(
                    context, sdd_context, output_item, input_entity, in_facetted_items
                )
                if input_columns:
                    # Pre-extract output variable_id_str to avoid repeated FK access
                    output_var_id_str = data['variable_id_str']

                    for input_column in input_columns:
                        # Pre-extract input variable_id_str to pass to provide_csilink
                        input_var_id_str = input_column.variable_id.variable_id if input_column.variable_id else None

                        csil, sdd_context = self.provide_csilink(
                            output_item, input_column, cube_link, sdd_context,
                            output_var_id_str, input_var_id_str
                        )
                        if context.save_derived_sdd_items:
                            cube_structure_item_links_to_create.append(csil)
                            num_of_cube_link_items = num_of_cube_link_items + 1
        return num_of_cube_link_items

    def provide_csilink(self, output_item, input_column, cube_link, sdd_context,
                        output_var_id_str=None, input_var_id_str=None):
        csil = CUBE_STRUCTURE_ITEM_LINK()
        csil.foreign_cube_variable_code = output_item
        csil.primary_cube_variable_code = input_column
        csil.cube_link_id = cube_link

        # Use pre-extracted variable IDs if provided, otherwise fall back to FK access
        if output_var_id_str is None:
            output_var_id_str = csil.foreign_cube_variable_code.variable_id.variable_id
        if input_var_id_str is None:
            input_var_id_str = csil.primary_cube_variable_code.variable_id.variable_id

        csil.cube_structure_item_link_id = ":".join(
            [
                f"{cube_link.cube_link_id}",
                f"{output_var_id_str}",
                f"{input_var_id_str}",
            ]
        )

        sdd_context.cube_structure_item_links_dictionary[
            csil.cube_structure_item_link_id
        ] = csil

        if (
            cube_link.cube_link_id
            not in sdd_context.cube_structure_item_link_to_cube_link_map
        ):
            sdd_context.cube_structure_item_link_to_cube_link_map[
                cube_link.cube_link_id
            ] = []
        sdd_context.cube_structure_item_link_to_cube_link_map[
            cube_link.cube_link_id
        ].append(csil)
        return csil, sdd_context

    def valid_operation(
        self,
        context: Any,
        output_item: Any,
        framework: str,
        category: str,
        report_template: str,
    ) -> bool:
        """
        Check if the operation is valid for the given output item and context.

        Args:
            context (Any): The context object containing necessary data.
            output_item (Any): The output item to check.
            framework (str): The framework being used (e.g., "FINREP_REF").
            category (str): The category of the report.
            report_template (str): The report template name.

        Returns:
            bool: True if the operation is valid, False otherwise.
        """
        return True

    def operation_exists_in_cell_for_report_with_category(
        self,
        context: Any,
        sdd_context: Any,
        output_item: Any,
        category: str,
        report_template: str,
    ) -> Tuple[bool, dict]:
        """
        Check if an operation exists in a cell for a report with a specific category.

        Args:
            context (Any): The context object containing necessary data.
            sdd_context (Any): The SDD context object.
            output_item (Any): The output item to check.
            framework (str): The framework being used (e.g., "FINREP_REF").
            input_cube_type (str): The input cube type.
            category (str): The category of the report.
            report_template (str): The report template name.

        Returns:
            bool: True if the operation exists, False otherwise.
        """
        # PRE-EXTRACT combination_id strings to avoid FK descriptor calls
        # Cache this per report_template to avoid repeated extraction
        if not hasattr(sdd_context, '_report_combination_ids_cache'):
            sdd_context._report_combination_ids_cache = {}

        if report_template not in sdd_context._report_combination_ids_cache:
            report_combination_ids = {
                el.combination_id.combination_id if hasattr(el.combination_id, 'combination_id') else el.combination_id
                for el in sdd_context.combination_to_rol_cube_map.get(report_template, [])
            }
            sdd_context._report_combination_ids_cache[report_template] = report_combination_ids
        else:
            report_combination_ids = sdd_context._report_combination_ids_cache[report_template]

        # Get variable_id as string (output_item already has variable pre-extracted in calling function)
        output_var_id = output_item.variable_id.variable_id if output_item.variable_id else None
        if not output_var_id:
            return False

        # Find combinations that contain this variable (using IDs)
        concerned_combination_ids = {
            comb_id
            for comb_id, items in context.category_to_ci.get(category, {}).items()
            if output_var_id in items
        }

        # Intersection of combination IDs (strings, not objects)
        concerned_combination_ids = concerned_combination_ids.intersection(report_combination_ids)

        if concerned_combination_ids:
            # Store variable_id -> set of member_ids (all strings)
            context.variable_members_in_combinations = dict()
            for comb_id in concerned_combination_ids:
                cis = context.category_to_ci[category][comb_id]
                for var_id, mem_id in cis.items():
                    if var_id not in context.variable_members_in_combinations:
                        context.variable_members_in_combinations[var_id] = set()
                    context.variable_members_in_combinations[var_id].add(mem_id)
            # print(f"here are the concerned combinations for {report_template}.{output_item.variable_id.variable_id} and category {category} : {concerned_combination_ids}")
            return True
        return False

    def find_variables_with_same_members_then_same_name(
        self,
        context: Any,
        sdd_context: Any,
        output_item: Any,
        input_entity: Any,
        in_facetted_items: bool = False,
    ) -> List[Any]:
        """
        Find variables with the same domain and then name as the output item.

        Args:
            sdd_context (Any): The SDD context object.
            output_item (Any): The output item to find matching variables for.
            input_entity_list (List[Any]): List of input entities to search.

        Returns:
            List[Any]: A list of matching variables.
        """

        related_variables = []

        target_domain = (
            output_item.variable_id.domain_id if output_item.variable_id else None
        )

        field_list = sdd_context.bird_cube_structure_item_dictionary.get(
            input_entity.cube_structure_id, []
        )

        # PRE-EXTRACT all FK values from field_list ONCE to avoid millions of descriptor calls
        # This data structure is reused across both member comparison and name comparison
        field_list_data = []
        for csi in field_list:
            csi_variable = csi.variable_id
            csi_variable_domain = csi_variable.domain_id if csi_variable else None
            csi_subdomain = csi.subdomain_id
            subdomain_id_str = csi_subdomain.subdomain_id if csi_subdomain else None
            variable_name = csi_variable.name if csi_variable else None

            # Store pre-extracted data
            field_list_data.append({
                'csi': csi,
                'variable': csi_variable,
                'variable_domain': csi_variable_domain,
                'subdomain': csi_subdomain,
                'subdomain_id_str': subdomain_id_str,
                'variable_name': variable_name,
            })

        if not in_facetted_items:
            # Same members / combination comparison

            # Get variable_id as string
            output_var_id = output_item.variable_id.variable_id if output_item.variable_id else None

            if not output_var_id:
                return related_variables

            if not context.check_domain_members_during_join_meta_data_creation:
                for data in field_list_data:
                    if data['variable'] :
                        if data['variable'].variable_id == output_var_id:
                            related_variables.append(data['csi'])
                return related_variables

            else:
                # output_members is now a set of member_id strings
                output_member_ids = context.variable_members_in_combinations.get(
                    output_var_id, set()
                )

                # Initialize hierarchy expansion cache if not exists
                if not hasattr(context, 'hierarchy_expansion_cache'):
                    context.hierarchy_expansion_cache = {}

                # Pre-extract domain to reduce descriptor overhead
                output_domain = output_item.variable_id.domain_id if output_item.variable_id else None
                hierarchies = sdd_context.domain_to_hierarchy_dictionary.get(
                    output_domain, []
                )

                # all_output_members is a set of member_id strings
                all_output_member_ids = output_member_ids.copy()
                if hierarchies:
                    # Pre-extract hierarchy IDs once to avoid repeated hasattr checks
                    hierarchy_ids = []
                    for hierarchy in hierarchies:
                        if hasattr(hierarchy, 'member_hierarchy_id'):
                            hierarchy_ids.append((hierarchy, hierarchy.member_hierarchy_id))
                        else:
                            hierarchy_ids.append((hierarchy, str(hierarchy)))

                    # Need to fetch MEMBER objects for hierarchy operations, then extract IDs
                    for output_member_id in output_member_ids:
                        for hierarchy, hierarchy_id in hierarchy_ids:
                            # Check cache first
                            cache_key = (output_member_id, hierarchy_id)
                            if cache_key in context.hierarchy_expansion_cache:
                                all_output_member_ids.update(context.hierarchy_expansion_cache[cache_key])
                            else:
                                # Get MEMBER object from ID for hierarchy service
                                try:
                                    output_member = sdd_context.member_dictionary.get(output_member_id)
                                    if output_member:
                                        hierarchy_members = self.member_hierarchy_service.get_member_list_considering_hierarchies(
                                            sdd_context, output_member, hierarchy
                                        )
                                        # Convert MEMBER objects to IDs - optimize by avoiding hasattr in comprehension
                                        hierarchy_member_ids = set()
                                        for m in hierarchy_members:
                                            if hasattr(m, 'member_id'):
                                                hierarchy_member_ids.add(m.member_id)
                                            else:
                                                hierarchy_member_ids.add(m)

                                        # Cache the result
                                        context.hierarchy_expansion_cache[cache_key] = hierarchy_member_ids
                                        all_output_member_ids.update(hierarchy_member_ids)
                                except (KeyError, AttributeError):
                                    pass
                IGNORED_DOMAINS = ["String", "Date", "Integer", "Boolean", "Float"]

                if (
                    target_domain
                    and target_domain.domain_id
                    and target_domain.domain_id not in IGNORED_DOMAINS
                ):
                    # Early exit if no output members to compare
                    if not all_output_member_ids:
                        return related_variables

                    # Initialize subdomain enumeration cache if not exists
                    if not hasattr(sdd_context, "subdomain_enumeration_cache"):
                        sdd_context.subdomain_enumeration_cache = {}

                    # Collect subdomains to batch load (field_list_data already created earlier)
                    subdomains_to_load = set()
                    for data in field_list_data:
                        if data['variable'] and data['variable_domain'] and data['subdomain']:
                            if data['subdomain_id_str'] not in sdd_context.subdomain_enumeration_cache:
                                subdomains_to_load.add(data['subdomain_id_str'])

                    # Batch load subdomain enumerations for all subdomains at once
                    if subdomains_to_load:
                        from django.db.models import Prefetch

                        subdomain_enums = SUBDOMAIN_ENUMERATION.objects.filter(
                            subdomain_id__subdomain_id__in=subdomains_to_load
                        ).select_related("member_id", "subdomain_id")

                        # Group by subdomain_id for caching (store member_id strings, not objects)
                        for enum in subdomain_enums:
                            subdomain_id = enum.subdomain_id.subdomain_id
                            if subdomain_id not in sdd_context.subdomain_enumeration_cache:
                                sdd_context.subdomain_enumeration_cache[subdomain_id] = set()
                            # Store member_id string to avoid hashing MEMBER objects
                            sdd_context.subdomain_enumeration_cache[subdomain_id].add(
                                enum.member_id.member_id if enum.member_id else None
                            )

                        # Mark empty subdomains to avoid future queries
                        for subdomain_id in subdomains_to_load:
                            if subdomain_id not in sdd_context.subdomain_enumeration_cache:
                                sdd_context.subdomain_enumeration_cache[subdomain_id] = set()

                    # Now process field_list using pre-extracted data (no more FK descriptor calls!)
                    for data in field_list_data:
                        if not data['variable'] or not data['variable_domain']:
                            continue

                        if data['subdomain']:
                            # Use cached subdomain enumeration data (now contains member_id strings)
                            subdomain_member_ids = sdd_context.subdomain_enumeration_cache.get(
                                data['subdomain_id_str'], set()
                            )

                            # Intersection of two sets of strings (no MEMBER object hashing)
                            #if subdomain_member_ids.intersection(all_output_member_ids):
                            if data['variable'].variable_id == output_var_id:
                                related_variables.append(data['csi'])
                    return related_variables

        # Same name comparison

        # logging.warning(f"CHECKING OUTPUT VARIABLE NAME FOR {output_item}")
        output_variable_name = (
            output_item.variable_id.variable_id if output_item.variable_id else None
        )
        if output_variable_name:
            # Use pre-extracted field_list_data to avoid FK descriptor calls
            related_variables = [
                data['csi']
                for data in field_list_data
                if data['variable_name'] == output_variable_name
            ]
        # if not related_variables:
        #     logging.warning(f"No related variables found for {output_item}")
        return related_variables

    def find_output_layer_cube(
        self, sdd_context: Any, output_layer_name: str, framework: str
    ) -> Any:
        """
        Find the output layer cube for a given output layer name and framework.

        Searches by cube name (which equals the table_id) and framework to find
        the correct cube regardless of version in the cube_id.

        Args:
            sdd_context (Any): The SDD context object.
            output_layer_name (str): The name of the output layer (table_id).
            framework (str): The framework being used (e.g., "FINREP_REF").

        Returns:
            Any: The output layer cube if found, None otherwise.
        """
        # Build name-to-cube dictionary if not already built
        if not hasattr(sdd_context, 'bird_cube_by_name_framework') or not sdd_context.bird_cube_by_name_framework:
            sdd_context.bird_cube_by_name_framework = {}
            for cube in CUBE.objects.select_related('framework_id').all():
                fw_id = cube.framework_id.framework_id if cube.framework_id else None
                key = (cube.name, fw_id)
                sdd_context.bird_cube_by_name_framework[key] = cube

        # Look up by name and framework
        key = (output_layer_name, framework)
        rol_cube = sdd_context.bird_cube_by_name_framework.get(key)

        # If not found, try searching for cube names that start with the output_layer_name
        # This handles cases where cube name includes version (e.g., C_07_00_a_4_0 vs C_07_00_a)
        if rol_cube is None:
            for (cube_name, cube_fw), cube in sdd_context.bird_cube_by_name_framework.items():
                if cube_fw == framework and cube_name and cube_name.startswith(output_layer_name + '_'):
                    rol_cube = cube
                    break

        if rol_cube is None:
            logging.debug(f"Could not find cube with name={output_layer_name}, framework={framework}")

        return rol_cube





    def find_input_layer_cube(
        self, sdd_context: Any, input_layer_name: str, framework: str
    ) -> Any:
        """
        Find the input layer cube for a given input layer name and framework.

        Args:
            sdd_context (Any): The SDD context object.
            input_layer_name (str): The name of the input layer.
            framework (str): The framework being used (e.g., "FINREP_REF").

        Returns:
            Any: The input layer cube if found, None otherwise.
        """
        if len(sdd_context.bird_cube_structure_dictionary) == 0:
            sdd_context.bird_cube_structure_dictionary = {}
            for cube_structure in CUBE_STRUCTURE.objects.all():
                sdd_context.bird_cube_structure_dictionary[cube_structure.cube_structure_id] = cube_structure

        try:
            return sdd_context.bird_cube_structure_dictionary.get(input_layer_name)
        except Exception as e:
            logging.error(f"Error getting cube structure for {input_layer_name}: {e}")
            return None
