# coding=UTF-8#
# Copyright (c) 2024 Bird Software Solutions Ltd
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDE-License-Identifier: EPL-2.0
#
# Contributors:
#    Neil Mackenzie - initial API and implementation
#
"""
@author: Neil
"""

import csv
import os


class MainCategoryFinder(object):
    """
    This class is responsible for creating maps of information
    related to the EBA main category
    """

    def create_report_to_main_category_maps(
        self, context, sdd_context, framework, reporting_framework_version
    ):
        """
        Create maps of information related to the EBA main category
        """
        MainCategoryFinder.create_main_category_to_name_map(
            self, context, sdd_context, framework
        )
        MainCategoryFinder.create_report_to_main_category_map(
            self, context, sdd_context, framework, reporting_framework_version
        )
        MainCategoryFinder.create_draft_table_part_file(
            self, context, sdd_context, framework
        )
        MainCategoryFinder.create_table_part_to_main_category_map(
            self, context, sdd_context, framework
        )
        MainCategoryFinder.create_il_tables_for_main_category_map(
            self, context, sdd_context, framework
        )
        MainCategoryFinder.create_table_parts_for_main_category_map(
            self, context, sdd_context, framework
        )

    def create_main_category_to_name_map(self, context, sdd_context, framework):
        """
        Create a map of EBA main category code to its user-friendly display name
        """
        file_location = os.path.join(
            context.file_directory,
            "joins_configuration",
            f"table_part_to_reference_category_{framework}.csv",
        )

        with open(file_location, encoding="utf-8") as csvfile:
            filereader = csv.reader(csvfile, delimiter=",", quotechar='"')
            next(filereader)  # Skip header
            for main_category, main_category_name, *_ in filereader:
                if framework == "FINREP_REF":
                    context.main_category_to_name_map_finrep[main_category] = (
                        main_category_name
                    )
                elif framework == "AE_REF":
                    context.main_category_to_name_map_ae[main_category] = (
                        main_category_name
                    )

    @staticmethod
    def remove_duplicates(member_mapping_items):
        """
        Remove duplicates from a list of member mapping items.

        Args:
            member_mapping_items (list): List of member mapping items.

        Returns:
            list: Deduplicated list of members.
        """
        return list({item.member for item in member_mapping_items})

    def create_table_part_to_main_category_map(self, context, sdd_context, framework):
        """
        Create a map from table parts to main categories
        """
        file_location = os.path.join(
            context.file_directory,
            "joins_configuration",
            f"table_part_main_category_{framework}.csv",
        )
        table_parts_to_main_category_map = (
            context.table_parts_to_main_category_map_finrep
            if framework == "FINREP_REF"
            else context.table_parts_to_main_category_map_ae
        )

        with open(file_location, encoding="utf-8") as csvfile:
            filereader = csv.reader(csvfile, delimiter=",", quotechar='"')
            next(filereader)  # Skip header
            for main_category, _, table_part in filereader:
                table_parts_to_main_category_map[table_part] = main_category

    def create_report_to_main_category_map(
        self, context, sdd_context, full_framework_name, reporting_framework_version
    ):
        """
        Look through the generated report and create a map of reports to main categories
        """
        main_categories_in_scope = (
            context.main_categories_in_scope_finrep
            if full_framework_name == "FINREP_REF"
            else context.main_categories_in_scope_ae
        )
        for (
            cube_name,
            combination_list,
        ) in sdd_context.combination_to_rol_cube_map.items():
            for combination in combination_list:
                self._process_combination(
                    context,
                    sdd_context,
                    combination,
                    cube_name,
                    main_categories_in_scope,
                )

    def _process_combination(
        self, context, sdd_context, combination, cube_name, main_categories_in_scope
    ):
        """
        Process a single combination and update main categories.

        Args:
            context: The context object.
            sdd_context: The SDD context object.
            combination: The combination to process.
            cube_name (str): The name of the cube.
            main_categories_in_scope (list): List of main categories in scope.
        """
        combination_items = sdd_context.combination_item_dictionary.get(
            combination.combination_id.combination_id, []
        )

        cell_instrmnt_ids_list = self._get_cell_instrmnt_ids(combination_items)
        if cell_instrmnt_ids_list:
            self._update_categories(
                context,
                cube_name,
                cell_instrmnt_ids_list,
                main_categories_in_scope,
                "TYP_INSTRMNT",
            )
        else:
            self._process_accounting_items(
                context, combination_items, cube_name, main_categories_in_scope
            )

    def _get_cell_instrmnt_ids(self, combination_items):
        """
        Get cell instrument IDs from combination items.

        Args:
            combination_items (list): List of combination items.

        Returns:
            list: List of cell instrument IDs.
        """
        cell_instrmnt_ids_list = []
        for combination_item in combination_items:
            if (
                combination_item.variable_id
                and combination_item.variable_id.variable_id == "TYP_INSTRMNT"
            ):
                if combination_item.member_id not in cell_instrmnt_ids_list:
                    cell_instrmnt_ids_list.append(combination_item.member_id)
        return cell_instrmnt_ids_list

    def _update_categories(
        self, context, cube_name, ids_list, main_categories_in_scope, prefix
    ):
        """
        Update main categories based on the given IDs.

        Args:
            context: The context object.
            cube_name (str): The name of the cube.
            ids_list (list): List of IDs to process.
            main_categories_in_scope (list): List of main categories in scope.
            prefix (str): Prefix to use for category names.
        """
        for member_id in ids_list:
            category = member_id.member_id
            if category not in main_categories_in_scope:
                main_categories_in_scope.append(category)
            try:
                category_list = context.report_to_main_category_map[cube_name]
                if category not in category_list:
                    category_list.append(category)
            except KeyError:
                context.report_to_main_category_map[cube_name] = [category]

    def _process_accounting_items(
        self, context, combination_items, cube_name, main_categories_in_scope
    ):
        """
        Process accounting items and update main categories.

        Args:
            context: The context object.
            combination_items (list): List of combination items.
            cube_name (str): The name of the cube.
            main_categories_in_scope (list): List of main categories in scope.
        """
        cell_accntng_itm_ids_list = []
        for combination_item in combination_items:
            if (
                combination_item.variable_id
                and combination_item.variable_id.variable_id == "TYP_ACCNTNG_ITM"
            ):
                if combination_item.member_id not in cell_accntng_itm_ids_list:
                    cell_accntng_itm_ids_list.append(combination_item.member_id)

        self._update_categories(
            context,
            cube_name,
            cell_accntng_itm_ids_list,
            main_categories_in_scope,
            "TYP_ACCNTNG_ITM",
        )

    def create_draft_table_part_file(self, context, sdd_context, framework):
        """
        Create a draft of the table part file, this should be reviewed and edited
        and the edited version used as an input for processing
        """
        main_categories_in_scope = (
            context.main_categories_in_scope_finrep
            if framework == "FINREP_REF"
            else context.main_categories_in_scope_ae
        )
        subdirectory = (
            "finrep_transformation_meta_data_ldm"
            if framework == "FINREP_REF"
            else "ae_transformation_meta_data_ldm"
        )

        output_file = os.path.join(
            context.output_directory,
            "transformation_meta_data_csv",
            subdirectory,
            f"table_parts_draft_{framework}.csv",
        )
        with open(output_file, "a", encoding="utf-8") as f:
            f.write("description,classifier,value,description,Main Category\n")
            f.write(",,,," + "\n,".join(main_categories_in_scope) + "\n")

    def create_table_part_to_main_category_map(self, context, sdd_context, framework):
        """
        Create a map from table parts to main categories
        """
        file_location = os.path.join(
            context.file_directory,
            "joins_configuration",
            f"table_part_to_reference_category_{framework}.csv",
        )
        table_parts_to_main_category_map = (
            context.table_parts_to_main_category_map_finrep
            if framework == "FINREP_REF"
            else context.table_parts_to_main_category_map_ae
        )

        with open(file_location, encoding="utf-8") as csvfile:
            filereader = csv.reader(csvfile, delimiter=",", quotechar='"')
            next(filereader)  # Skip header
            for main_category, _, table_part in filereader:
                table_parts_to_main_category_map[table_part] = main_category

    def create_il_tables_for_main_category_map(self, context, sdd_context, framework):
        """
        Create a map from main categories such as loans and advances
        to the related input layer such as instrument
        """
        file_location = os.path.join(
            context.file_directory,
            "joins_configuration",
            f"table_part_ldm_definitions_{framework}.csv",
        )
        if not (context.ldm_or_il == "ldm"):
            file_location = os.path.join(
                context.file_directory,
                "joins_configuration",
                f"table_part_il_definitions_{framework}.csv",
            )
        tables_for_main_category_map = (
            context.tables_for_main_category_map_finrep
            if framework == "FINREP_REF"
            else context.tables_for_main_category_map_ae
        )

        with open(file_location, encoding="utf-8") as csvfile:
            filereader = csv.reader(csvfile, delimiter=",", quotechar='"')
            next(filereader)  # Skip header
            for table_part_name, il_table, *_ in filereader:
                try:
                    main_category = context.table_parts_to_main_category_map_finrep[
                        table_part_name
                    ]
                    tables_for_main_category_map.setdefault(main_category, []).append(
                        il_table
                    )
                except KeyError:
                    print(
                        f"Could not find main category for table part {table_part_name}"
                    )

    def create_table_parts_for_main_category_map(self, context, sdd_context, framework):
        """
        Create a map from main categories such as loans and advances
        to the related table parts, where table part is a combination
        of an input layer and main category description
        """
        file_location = os.path.join(
            context.file_directory,
            "joins_configuration",
            f"table_part_ldm_definitions_{framework}.csv",
        )
        if not (context.ldm_or_il == "ldm"):
            file_location = os.path.join(
                context.file_directory,
                "joins_configuration",
                f"table_part_il_definitions_{framework}.csv",
            )
        table_parts_to_linked_tables_map = (
            context.table_parts_to_linked_tables_map_finrep
            if framework == "FINREP_REF"
            else context.table_parts_to_linked_tables_map_ae
        )
        table_parts_to_to_filter_map = (
            context.table_parts_to_to_filter_map_finrep
            if framework == "FINREP_REF"
            else context.table_parts_to_to_filter_map_ae
        )
        table_and_part_tuple_map = (
            context.table_and_part_tuple_map_finrep
            if framework == "FINREP_REF"
            else context.table_and_part_tuple_map_ae
        )

        with open(file_location, encoding="utf-8") as csvfile:
            filereader = csv.reader(csvfile, delimiter=",", quotechar='"')
            next(filereader)  # Skip header
            for (
                table_part_name,
                il_table,
                the_filter,
                linked_table_list,
                comments,
            ) in filereader:
                try:
                    main_category = context.table_parts_to_main_category_map_finrep[
                        table_part_name
                    ]
                    table_and_part_tuple = (il_table, table_part_name)
                    table_parts_to_linked_tables_map[table_and_part_tuple] = (
                        linked_table_list
                    )
                    table_parts_to_to_filter_map[table_and_part_tuple] = the_filter
                    table_and_part_tuple_map.setdefault(main_category, []).append(
                        table_and_part_tuple
                    )
                except KeyError:
                    print(
                        f"Could not find main category for the table part {table_part_name}"
                    )
