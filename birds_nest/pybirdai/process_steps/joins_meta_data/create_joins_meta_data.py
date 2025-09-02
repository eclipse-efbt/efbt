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

from pybirdai.models.bird_meta_data_model import *
from django.apps import apps
from django.db.models.fields import CharField,DateTimeField,BooleanField,FloatField,BigIntegerField
import os
import csv
from typing import List, Any

from pybirdai.process_steps.joins_meta_data.ldm_search import ELDMSearch

class JoinsMetaDataCreator:
    """
    A class for creating generation rules for reports and tables.
    """

    def generate_joins_meta_data(self, context: Any, sdd_context: Any, framework: str) -> None:
        """
        Generate generation rules for the given context and framework.

        Args:
            context (Any): The context object containing necessary data.
            sdd_context (Any): The SDD context object.
            framework (str): The framework being used (e.g., "FINREP_REF").
        """
        self.add_reports(context, sdd_context, framework)

    def add_reports(self, context: Any, sdd_context: Any, framework: str) -> None:
        """
        Add reports based on the given context and framework.

        Args:
            context (Any): The context object containing necessary data.
            sdd_context (Any): The SDD context object.
            framework (str): The framework being used (e.g., "FINREP_REF").
        """
        file_location = os.path.join(context.file_directory, "joins_configuration",
                                     f"in_scope_reports_{framework}.csv")
        self.create_ldm_entity_to_linked_entities_map(context, sdd_context)

        with open(file_location, encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            next(filereader)  # Skip header
            for row in filereader:
                report_template = row[0]
                generated_output_layer = self.find_output_layer_cube(
                    sdd_context, report_template, framework)
                if generated_output_layer:
                    self.add_join_for_products_il(context, sdd_context,
                                         generated_output_layer, framework)

    def create_ldm_entity_to_linked_entities_map(self, context: Any,
                                                 sdd_context: Any) -> None:
        """
        Create a mapping of LDM entities to their linked entities.

        Args:
            context (Any): The context object containing necessary data.
            sdd_context (Any): The SDD context object.
        """
        output_file = os.path.join(context.output_directory, "csv",
                                   "ldm_entity_related_entities.csv")
        with open(output_file, "w", encoding='utf-8') as f:
            f.write("ldm_entity,related_entities\n")
            for model in apps.get_models():
                if model._meta.app_label == 'pybirdai':
                    entities = ELDMSearch.get_all_related_entities(
                        self, context, model
                    )
                    related_entities_string = ":".join(
                        entity.__name__ for entity in entities
                    )
                    f.write(f"{model.__name__},{related_entities_string}\n")
                    context.ldm_entity_to_linked_tables_map[model.__name__] = \
                        related_entities_string

    def add_join_for_products_il(self, context: Any, sdd_context: Any,
                           generated_output_layer: Any,
                           framework: str) -> None:
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
                            print(f"join_for_product:{join_for_product}")
                            print(inputLayerTable)
                            input_entity_list = [inputLayerTable]
                            linked_tables = join_for_products_to_linked_tables_map[join_for_product]
                            linked_tables_list = linked_tables.split(":")
                            if (inputLayerTable and
                                inputLayerTable.cube_structure_id not in linked_tables_list):
                                linked_tables_list.append(inputLayerTable.cube_structure_id)
                            extra_tables = []
                            for the_table in linked_tables_list:
                                extra_linked_tables = []
                                try:
                                    #if the_table.endswith("_ELDM"):
                                    extra_linked_tables_string = context.ldm_entity_to_linked_tables_map[the_table]
                                    #else:
                                    #    extra_linked_tables_string = context.ldm_entity_to_linked_tables_map[the_table + "_ELDM"]
                                    extra_linked_tables = extra_linked_tables_string.split(":")
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
                                    print(f"input_entity:{input_entity}")
                                    cube_link = CUBE_LINK()
                                    cube_link.description = f"{join_for_product[0]}:{mc}:{join_for_product[1]}:{input_entity.cube_structure_id}"
                                    cube_link.name = f"{join_for_product[0]}:{join_for_product[1]}:{input_entity.cube_structure_id}"
                                    cube_link.join_identifier = join_for_product[1]
                                    primary_cube = sdd_context.bird_cube_dictionary.get(input_entity.cube_structure_id)
                                    if primary_cube:
                                        cube_link.primary_cube_id = primary_cube
                                        cube_link.cube_link_id = (
                                            f"{report_template}:"
                                            f"{input_entity.cube_structure_id}:{join_for_product[1]}"
                                        )
                                    else:
                                        cube_link.cube_link_id = f"{input_entity.cube_structure_id}:{join_for_product[1]}"
                                        print(f"cube_link.primary_cube_id not found for {table}")
                                    cube_link.foreign_cube_id = generated_output_layer


                                    if cube_link.cube_link_id not in sdd_context.cube_link_dictionary:
                                        sdd_context.cube_link_dictionary[cube_link.cube_link_id] = cube_link
                                        foreign_cube = cube_link.foreign_cube_id
                                        try:
                                            sdd_context.cube_link_to_foreign_cube_map[foreign_cube.cube_id].append(cube_link)
                                        except KeyError:
                                            sdd_context.cube_link_to_foreign_cube_map[foreign_cube.cube_id] = [cube_link]
                                        join_identifier = cube_link.join_identifier
                                        try:
                                            sdd_context.cube_link_to_join_identifier_map[join_identifier].append(cube_link)
                                        except KeyError:
                                            sdd_context.cube_link_to_join_identifier_map[join_identifier] = [cube_link]

                                        join_for_report_id = foreign_cube.cube_id + ":" + cube_link.join_identifier
                                        try:
                                            sdd_context.cube_link_to_join_for_report_id_map[join_for_report_id].append(cube_link)
                                        except KeyError:
                                            sdd_context.cube_link_to_join_for_report_id_map[join_for_report_id] = [cube_link]



                                        num_of_cube_link_items = self.add_field_to_field_lineage_to_rules_for_join_for_product(
                                            context, sdd_context, generated_output_layer,
                                            input_entity, mc, report_template,
                                            framework, cube_link, cube_structure_item_links_to_create
                                        )

                                        if context.save_derived_sdd_items:
                                            if num_of_cube_link_items > 0:
                                                cube_links_to_create.append(cube_link)


                except KeyError:
                    print(f"no tables for main category:{mc}")
        except KeyError:
            print(f"no main category for report :{report_template}")

        # Bulk create all collected CUBE_LINK objects
        if context.save_derived_sdd_items and cube_links_to_create:
            CUBE_LINK.objects.bulk_create(cube_links_to_create, batch_size=1000)

        # Bulk create all collected CUBE_STRUCTURE_ITEM_LINK objects
        for item in cube_structure_item_links_to_create:
            print(item.foreign_cube_variable_code.variable_id)
            #import pdb;pdb.set_trace()
            item.save()
        #import pdb;pdb.set_trace()
        #if context.save_derived_sdd_items and cube_structure_item_links_to_create:
        #    CUBE_STRUCTURE_ITEM_LINK.objects.bulk_create(cube_structure_item_links_to_create, batch_size=1000)

    def add_field_to_field_lineage_to_rules_for_join_for_product(
            self, context: Any, sdd_context: Any,
            output_entity: Any, input_entity: Any,
            category: str, report_template: str,
            framework: str, cube_link: Any,
            cube_structure_item_links_to_create: List) -> None:
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
                output_entity.cube_id + '_cube_structure']:
            if self.valid_operation(context, output_item, framework,
                                    category, report_template):
                input_columns = self.find_variables_with_same_domain_then_name(
                    sdd_context, output_item, input_entity)

                if input_columns:
                    for input_column in input_columns:
                        csil = CUBE_STRUCTURE_ITEM_LINK()
                        csil.foreign_cube_variable_code = output_item
                        csil.cube_structure_item_link_id = (
                            f"{cube_link.cube_link_id}:"
                            f"{csil.foreign_cube_variable_code.variable_id.variable_id}"
                        )
                        csil.primary_cube_variable_code = input_column
                        if not(csil.primary_cube_variable_code.variable_id is None):
                            csil.cube_structure_item_link_id += (
                            f":{csil.primary_cube_variable_code.variable_id.variable_id}"
                        )
                        csil.cube_link_id = cube_link
                        sdd_context.cube_structure_item_links_dictionary[csil.cube_structure_item_link_id] = csil

                        try:
                            sdd_context.cube_structure_item_link_to_cube_link_map[cube_link.cube_link_id].append(csil)
                        except KeyError:
                            sdd_context.cube_structure_item_link_to_cube_link_map[cube_link.cube_link_id] = [csil]

                        if context.save_derived_sdd_items:
                            cube_structure_item_links_to_create.append(csil)
                            num_of_cube_link_items = num_of_cube_link_items + 1
        return num_of_cube_link_items

    def valid_operation(self, context: Any, output_item: Any, framework: str, category: str, report_template: str) -> bool:
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

    def operation_exists_in_cell_for_report_with_category(self, context: Any, sdd_context: Any, output_item: Any, framework: str, input_cube_type: str, category: str, report_template: str) -> bool:
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
        combinations = sdd_context.combination_to_rol_cube_map.get(report_template, [])
        for combination in combinations:
            if combination in context.cell_to_typ_instrmnt_map[category]:
                combination_items = sdd_context.combination_item_dictionary.get(combination.combination_id, [])
                for combination_item in combination_items:
                    if combination_item.variable_id.name == output_item.name and combination_item.member_id.member_id == category:
                        return True
        return False



    def find_variables_with_same_domain_then_name(self, sdd_context: Any, output_item: Any, input_entity: Any) -> List[Any]:
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
        target_domain = output_item.variable_id.domain_id

        if target_domain and  not ((target_domain.domain_id == "String") or (target_domain.domain_id == "Date") or (target_domain.domain_id == "Integer") or (target_domain.domain_id == "Boolean") or (target_domain.domain_id == "Float")     ):
            if input_entity:
                field_list = []
                try:
                    field_list = sdd_context.bird_cube_structure_item_dictionary[input_entity.cube_structure_id]
                except KeyError:
                    pass
                for csi in field_list:
                    variable = csi.variable_id
                    if variable and variable.domain_id.domain_id == target_domain.domain_id:
                            related_variables.append(csi)
        else:
            output_variable_name = output_item.variable_id.variable_id
            if output_variable_name:
                if input_entity:
                    field_list = []
                    try:
                        field_list = sdd_context.bird_cube_structure_item_dictionary[input_entity.cube_structure_id]
                    except KeyError:
                        pass
                    for csi in field_list:
                        variable = csi.variable_id
                        if variable and variable.name == output_variable_name:
                            related_variables.append(csi)

        return related_variables

    def find_output_layer_cube(self, sdd_context: Any, output_layer_name: str, framework: str) -> Any:
        """
        Find the output layer cube for a given output layer name and framework.

        Args:
            sdd_context (Any): The SDD context object.
            output_layer_name (str): The name of the output layer.
            framework (str): The framework being used (e.g., "FINREP_REF").

        Returns:
            Any: The output layer cube if found, None otherwise.
        """
        output_layer_name = f"{output_layer_name}_REF_FINREP_3_0" if framework == "FINREP_REF" else output_layer_name
        return sdd_context.bird_cube_dictionary.get(output_layer_name)

    def find_input_layer_cube(self, sdd_context: Any, input_layer_name: str, framework: str) -> Any:
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
