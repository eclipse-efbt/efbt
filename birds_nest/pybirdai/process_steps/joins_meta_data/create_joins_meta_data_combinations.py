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
#    Benjamin Arfa - improvements and enhancements
#

import logging
from pybirdai.bird_meta_data_model import *
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
        context.combination_item_dictionary = {}
        for combination_item in COMBINATION_ITEM.objects.all():
            try:
                combination_item_list = context.combination_item_dictionary[
                    combination_item.combination_id
                ]
                combination_item_list.append(combination_item)
            except KeyError:
                context.combination_item_dictionary[combination_item.combination_id] = [
                    combination_item
                ]

        self.member_hierarchy_service = MemberHierarchyService()
        sdd_context = self.member_hierarchy_service.prepare_node_dictionaries_and_lists(
            sdd_context
        )

        all_main_categories = set(sum(context.report_to_main_category_map.values(), []))
        context.category_to_combinations = {
            category: {
                item.combination_id
                for item in COMBINATION_ITEM.objects.all().filter(
                    member_id__member_id=category
                )
            }
            for category in all_main_categories
        }
        context.category_to_ci = {
            category: {
                combination_id: {
                    item.variable_id: item.member_id
                    for item in context.combination_item_dictionary[combination_id]
                }
                for combination_id in combinations
            }
            for category, combinations in context.category_to_combinations.items()
        }

        context.domain_to_member = {
            domain: {
                member_id for member_id in MEMBER.objects.all().filter(domain_id=domain)
            }
            for domain in DOMAIN.objects.all()
        }

        context.facetted_items = {
            output_item.variable_id
            for output_item in CUBE_STRUCTURE_ITEM.objects.all()
            if output_item.variable_id.domain_id.domain_id
            in ["String", "Date", "Integer", "Boolean", "Float"]
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
        with open(output_file, "w", encoding="utf-8") as f:
            f.write("ldm_entity,related_entities\n")
            for model in apps.get_models():
                if model._meta.app_label == "pybirdai":
                    entities = ELDMSearch.get_all_related_entities(self, context, model)
                    related_entities_string = ":".join(
                        entity.__name__ for entity in entities
                    )
                    f.write(f"{model.__name__},{related_entities_string}\n")
                    context.ldm_entity_to_linked_tables_map[model.__name__] = (
                        related_entities_string
                    )

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
            framework (str): The framework being used (e.g., "FINREP_REF").
        """
        tables_for_main_category_map = (
            context.tables_for_main_category_map_finrep
            if framework == "FINREP_REF"
            else context.tables_for_main_category_map_ae
        )
        join_for_products_to_linked_tables_map = (
            context.join_for_products_to_linked_tables_map_finrep
            if framework == "FINREP_REF"
            else context.join_for_products_to_linked_tables_map_ae
        )
        table_and_part_tuple_map = (
            context.table_and_part_tuple_map_finrep
            if framework == "FINREP_REF"
            else context.table_and_part_tuple_map_ae
        )
        cube_links_to_create = []  # New list to collect CUBE_LINK objects
        cube_structure_item_links_to_create = []  # New list for CUBE_STRUCTURE_ITEM_LINK objects
        num_of_cube_link_items = 0

        try:
            report_template = generated_output_layer.name
            main_categories = context.report_to_main_category_map[report_template]
            for mc in main_categories:
                try:
                    tables = tables_for_main_category_map[mc]
                    for table in tables:
                        inputLayerTable = self.find_input_layer_cube(
                            sdd_context, table, framework
                        )
                        join_for_products = table_and_part_tuple_map[mc]

                        for join_for_product in join_for_products:
                            # print(f"join_for_product:{join_for_product}")
                            # print(inputLayerTable)
                            input_entity_list = [inputLayerTable]
                            linked_tables = join_for_products_to_linked_tables_map[
                                join_for_product
                            ]
                            linked_tables_list = linked_tables.split(":")
                            if (
                                inputLayerTable
                                and inputLayerTable.cube_structure_id
                                not in linked_tables_list
                            ):
                                linked_tables_list.append(
                                    inputLayerTable.cube_structure_id
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
                                    input_entity_list.append(the_input_table)

                            if join_for_product[0] == table:
                                for input_entity in input_entity_list:
                                    # print(f"input_entity:{input_entity}")
                                    cube_link = CUBE_LINK()
                                    cube_link.description = f"{join_for_product[0]}:{mc}:{join_for_product[1]}:{input_entity.cube_structure_id}"
                                    cube_link.name = f"{join_for_product[0]}:{join_for_product[1]}:{input_entity.cube_structure_id}"
                                    cube_link.join_identifier = join_for_product[1]
                                    primary_cube = sdd_context.bird_cube_dictionary.get(
                                        input_entity.cube_structure_id
                                    )
                                    if primary_cube:
                                        cube_link.primary_cube_id = primary_cube
                                        cube_link.cube_link_id = (
                                            f"{report_template}:"
                                            f"{input_entity.cube_structure_id}:{join_for_product[1]}"
                                        )
                                    else:
                                        cube_link.cube_link_id = f"{input_entity.cube_structure_id}:{join_for_product[1]}"
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
                    # traceback.print_exc()
                    print(f"no tables for main category:{mc}")
        except KeyError:
            print(f"no main category for report :{report_template}")

        # Bulk create all collected CUBE_LINK objects
        if context.save_derived_sdd_items and cube_links_to_create:
            CUBE_LINK.objects.bulk_create(cube_links_to_create, batch_size=1000)

        # Bulk create all collected CUBE_STRUCTURE_ITEM_LINK objects
        for item in cube_structure_item_links_to_create:
            # print(item.foreign_cube_variable_code.variable_id)
            # import pdb;pdb.set_trace()
            # print(item.__dict__)
            item.save()
        # import pdb;pdb.set_trace()
        # if context.save_derived_sdd_items and cube_structure_item_links_to_create:
        #    CUBE_STRUCTURE_ITEM_LINK.objects.bulk_create(cube_structure_item_links_to_create, batch_size=1000)

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
        for output_item in sdd_context.bird_cube_structure_item_dictionary[
            output_entity.cube_id + "_cube_structure"
        ]:
            operation_exists = self.operation_exists_in_cell_for_report_with_category(
                context, sdd_context, output_item, category, report_template
            )
            in_facetted_items = output_item.variable_id in context.facetted_items

            if operation_exists or in_facetted_items:
                input_columns = self.find_variables_with_same_members_then_same_name(
                    context, sdd_context, output_item, input_entity, in_facetted_items
                )
                if input_columns:
                    for input_column in input_columns:
                        csil, sdd_context = self.provide_csilink(
                            output_item, input_column, cube_link, sdd_context
                        )
                        if context.save_derived_sdd_items:
                            cube_structure_item_links_to_create.append(csil)
                            num_of_cube_link_items = num_of_cube_link_items + 1
        return num_of_cube_link_items

    def provide_csilink(self, output_item, input_column, cube_link, sdd_context):
        csil = CUBE_STRUCTURE_ITEM_LINK()
        csil.foreign_cube_variable_code = output_item
        csil.primary_cube_variable_code = input_column
        csil.cube_link_id = cube_link

        csil.cube_structure_item_link_id = ":".join(
            [
                f"{cube_link.cube_link_id}",
                f"{csil.foreign_cube_variable_code.variable_id.variable_id}",
                f"{csil.primary_cube_variable_code.variable_id.variable_id}",
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
        report_combinations = {
            el.combination_id
            for el in sdd_context.combination_to_rol_cube_map.get(report_template, [])
        }
        concerned_combinations_by_category_with_item = {
            combination
            for combination, items in context.category_to_ci[category].items()
            if output_item.variable_id in items
        }
        concerned_combinations = (
            concerned_combinations_by_category_with_item.intersection(
                report_combinations
            )
        )
        if concerned_combinations:
            context.variable_members_in_combinations = dict()
            for combination in concerned_combinations:
                cis = context.category_to_ci[category][combination]
                for var_, mem_ in cis.items():
                    if var_ not in context.variable_members_in_combinations:
                        context.variable_members_in_combinations[var_] = set()
                    context.variable_members_in_combinations[var_].add(mem_)
            # print(f"here are the concerned combinations for {report_template}.{output_item.variable_id.variable_id} and category {category} : {concerned_combinations}")
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

        if not in_facetted_items:
            # Same members / combination comparison

            output_members = context.variable_members_in_combinations.get(
                output_item.variable_id, set()
            )
            hierarchies = sdd_context.domain_to_hierarchy_dictionary.get(
                output_item.variable_id.domain_id, []
            )
            all_output_members = output_members.copy()
            if hierarchies:
                for output_member, hierarchy in itertools.product(
                    output_members, hierarchies
                ):
                    all_output_members.update(
                        self.member_hierarchy_service.get_member_list_considering_hierarchies(
                            sdd_context, output_member, hierarchy
                        )
                    )
            IGNORED_DOMAINS = ["String", "Date", "Integer", "Boolean", "Float"]

            if (
                target_domain
                and target_domain.domain_id
                and target_domain.domain_id not in IGNORED_DOMAINS
            ):
                # Early exit if no output members to compare
                if not all_output_members:
                    return related_variables

                # Initialize subdomain enumeration cache if not exists
                if not hasattr(sdd_context, "subdomain_enumeration_cache"):
                    sdd_context.subdomain_enumeration_cache = {}

                # Collect all unique subdomains from field_list to batch load
                subdomains_to_load = set()
                for csi in field_list:
                    if (
                        csi.variable_id
                        and csi.variable_id.domain_id
                        and csi.subdomain_id
                    ):
                        subdomain_id = csi.subdomain_id.subdomain_id
                        if subdomain_id not in sdd_context.subdomain_enumeration_cache:
                            subdomains_to_load.add(subdomain_id)

                # Batch load subdomain enumerations for all subdomains at once
                if subdomains_to_load:
                    from django.db.models import Prefetch

                    subdomain_enums = SUBDOMAIN_ENUMERATION.objects.filter(
                        subdomain_id__subdomain_id__in=subdomains_to_load
                    ).select_related("member_id", "subdomain_id")

                    # Group by subdomain_id for caching
                    for enum in subdomain_enums:
                        subdomain_id = enum.subdomain_id.subdomain_id
                        if subdomain_id not in sdd_context.subdomain_enumeration_cache:
                            sdd_context.subdomain_enumeration_cache[subdomain_id] = (
                                set()
                            )
                        sdd_context.subdomain_enumeration_cache[subdomain_id].add(
                            enum.member_id
                        )

                    # Mark empty subdomains to avoid future queries
                    for subdomain_id in subdomains_to_load:
                        if subdomain_id not in sdd_context.subdomain_enumeration_cache:
                            sdd_context.subdomain_enumeration_cache[subdomain_id] = (
                                set()
                            )

                # Now process field_list using cached data
                for csi in field_list:
                    bool_1 = csi.variable_id and csi.variable_id.domain_id
                    if not bool_1:
                        continue

                    subdomain = csi.subdomain_id
                    bool_2 = False
                    if subdomain:
                        subdomain_id = subdomain.subdomain_id
                        # Use cached subdomain enumeration data
                        subdomain_members = sdd_context.subdomain_enumeration_cache.get(
                            subdomain_id, set()
                        )
                        bool_2 = bool(
                            subdomain_members.intersection(all_output_members)
                        )
                    else:
                        # print(f"no subdomain for {csi}:{csi.variable_id}:{csi.cube_structure_id.cube_structure_id}")
                        bool_2 = False

                    if bool_1 and bool_2:
                        # if output_item.variable_id.variable_id == 'TYP_INSTRMNT':
                        #    import pdb;pdb.set_trace()
                        related_variables.append(csi)
                return related_variables

        # Same name comparison

        # logging.warning(f"CHECKING OUTPUT VARIABLE NAME FOR {output_item}")
        output_variable_name = (
            output_item.variable_id.variable_id if output_item.variable_id else None
        )
        if output_variable_name:
            related_variables = [
                csi
                for csi in field_list
                if csi.variable_id and csi.variable_id.name == output_variable_name
            ]
        if not related_variables:
            logging.warning(f"No related variables found for {output_item}")
        return related_variables

    def find_output_layer_cube(
        self, sdd_context: Any, output_layer_name: str, framework: str
    ) -> Any:
        """
        Find the output layer cube for a given output layer name and framework.

        Args:
            sdd_context (Any): The SDD context object.
            output_layer_name (str): The name of the output layer.
            framework (str): The framework being used (e.g., "FINREP_REF").

        Returns:
            Any: The output layer cube if found, None otherwise.
        """
        output_layer_name = (
            f"{output_layer_name}_REF_FINREP_3_0"
            if framework == "FINREP_REF"
            else output_layer_name
        )
        return sdd_context.bird_cube_dictionary.get(output_layer_name)

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
        return sdd_context.bird_cube_structure_dictionary.get(input_layer_name)
