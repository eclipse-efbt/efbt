# coding=UTF-8
# Copyright (c) 2025 Bird Software Solutions Ltd
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDX-License-Identifier: EPL-2.0
#
# Contributors:
#    Neil Mackenzie - initial API and implementation
#    Benjamin Arfa - improvements
#
'''
Main Category Finder module for creating maps of information related to EBA main categories.

Supports flexible product breakdown formats:
- Legacy format: TYP_INSTRMNT_970 (variable inferred from prefix)
- New single variable format: TYP_INSTRMNT=TYP_INSTRMNT_970
- Multi-variable format: TYP_INSTRMNT=TYP_INSTRMNT_970:TYP_CLLRL=TYP_CLLRL_1
- No breakdown: empty Main Category

@author: Neil
'''
import csv
import os
import logging
from pybirdai.process_steps.website_to_sddmodel.import_website_to_sdd_model_django import ImportWebsiteToSDDModel
from pybirdai.process_steps.joins_meta_data.condition_parser import BreakdownCondition
from pybirdai.models.bird_meta_data_model import CUBE_TO_COMBINATION, COMBINATION_ITEM

logger = logging.getLogger(__name__)

class MainCategoryFinder:
    '''
    This class is responsible for creating maps of information
    related to the EBA main category
    '''

    # Map framework names to their suffixes for context attribute lookup
    FRAMEWORK_SUFFIXES = {
        "FINREP_REF": "finrep",
        "AE_REF": "ae",
        "COREP_REF": "corep",
    }

    def _get_framework_map(self, context, map_name, framework):
        """
        Get the appropriate framework-specific map from context.

        Args:
            context: The context object
            map_name: Base name of the map (e.g., 'main_category_to_name_map')
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

    def create_report_to_main_category_maps(self, context, sdd_context, framework,
                                            reporting_framework_version):
        '''
        Create maps of information related to the EBA main category
        '''

        MainCategoryFinder.create_main_category_to_name_map(self, context,
                                                            sdd_context, framework)
        MainCategoryFinder.create_report_to_main_category_map(
            self, context, sdd_context, framework, reporting_framework_version)
        #MainCategoryFinder.create_draft_join_for_product_file(
        #    self, context, sdd_context, framework)
        
        MainCategoryFinder.create_join_for_product_to_main_category_map(
            self, context, sdd_context, framework)
        MainCategoryFinder.create_il_tables_for_main_category_map(
            self, context, sdd_context, framework)
        MainCategoryFinder.create_join_for_products_for_main_category_map(
            self, context, sdd_context, framework)

    def create_main_category_to_name_map(self, context, sdd_context, framework):
        '''
        Create a map of EBA main category code to its user-friendly display name.
        Uses member ID as the key (extracted from condition string).
        '''
        file_location = os.path.join("resources", "joins_configuration",
                                     f"join_for_product_to_reference_category_{framework}.csv")

        if not hasattr(context, 'main_category_conditions'):
            context.main_category_conditions = {}

        with open(file_location, encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            next(filereader)  # Skip header
            for main_category, main_category_name, *_ in filereader:
                if not main_category.strip():
                    continue

                try:
                    condition = BreakdownCondition(main_category)
                    members = condition.get_members()
                    member_id = members[0] if members else main_category

                    # Store condition by member ID for filter generation
                    context.main_category_conditions[member_id] = condition

                    # Get the appropriate map for this framework
                    main_category_to_name_map = self._get_framework_map(
                        context, 'main_category_to_name_map', framework
                    )
                    main_category_to_name_map[member_id] = main_category_name

                except ValueError as e:
                    logger.warning(f"Failed to parse main category '{main_category}': {e}")

    @staticmethod
    def remove_duplicates(member_mapping_items):
        """
        Remove duplicates from a list of member mapping items.

        Args:
            member_mapping_items (list): List of member mapping items.

        Returns:
            list: Deduplicated list of members.
        """
        return list({item.member_id for item in member_mapping_items})

    def create_join_for_product_to_main_category_map_OLD(self, context, sdd_context, framework):
        '''
        DEPRECATED: Create a map from join for products to main categories.
        This method reads from join_for_product_main_category_{framework}.csv.
        Supports many-to-many: a join_for_product can belong to multiple main categories.
        '''
        file_location = os.path.join(context.file_directory, "joins_configuration",
                                     f"join_for_product_main_category_{framework}.csv")
        join_for_products_to_main_category_map = self._get_framework_map(
            context, 'join_for_products_to_main_category_map', framework
        )

        with open(file_location, encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            next(filereader)  # Skip header
            for main_category, _, join_for_product in filereader:
                # Support many-to-many: store list of main categories for each join_for_product
                join_for_products_to_main_category_map.setdefault(join_for_product, []).append(main_category)

    def create_report_to_main_category_map(self, context, sdd_context,
                                                       full_framework_name,
                                                       reporting_framework_version):
        '''
        Look through the generated report and create a map of reports to main categories.

        If sdd_context is None, recreates the combination_to_rol_cube_map from the database.
        '''
        # If sdd_context is None, create a minimal context with data from database
        if len(sdd_context.combination_to_rol_cube_map.items()) == 0:            

            # Populate from database
            for cube_to_combination in CUBE_TO_COMBINATION.objects.all().select_related(
                'cube_id', 'combination_id'
            ):
                if cube_to_combination.cube_id:
                    cube_id = cube_to_combination.cube_id.cube_id
                    sdd_context.combination_to_rol_cube_map.setdefault(
                        cube_id, []
                    ).append(cube_to_combination)

            logger.info(f"Loaded {len(sdd_context.combination_to_rol_cube_map)} cubes from database")

        main_categories_in_scope = self._get_framework_map(
            context, 'main_categories_in_scope', full_framework_name
        )
        for cube_name, combination_list in sdd_context.combination_to_rol_cube_map.items():
            for combination in combination_list:
                self._process_combination(context, sdd_context, combination,
                                          cube_name, main_categories_in_scope)

    def _process_combination(self, context, sdd_context, combination,
                             cube_name, main_categories_in_scope):
        """
        Process a single combination and update main categories.

        Args:
            context: The context object.
            sdd_context: The SDD context object.
            combination: The combination to process.
            cube_name (str): The name of the cube.
            main_categories_in_scope (list): List of main categories in scope.
        """
        # If combination_item_dictionary is empty, load from database
        if len(sdd_context.combination_item_dictionary) == 0:
            for combination_item in COMBINATION_ITEM.objects.all().select_related(
                'combination_id', 'variable_id', 'member_id', 'subdomain_id'
            ):
                if combination_item.combination_id:
                    combo_id = combination_item.combination_id.combination_id
                    sdd_context.combination_item_dictionary.setdefault(
                        combo_id, []
                    ).append(combination_item)
            logger.info(f"Loaded {len(sdd_context.combination_item_dictionary)} combinations from database")

        combination_items = sdd_context.combination_item_dictionary.get(
            combination.combination_id.combination_id, []
        )

        cell_instrmnt_ids_list = self._get_cell_instrmnt_ids(combination_items)
        cell_scrty_derivative_list = self._get_cell_scrty_derivative_ids(combination_items)
        cell_instrmnt_rl_ids_list = self._get_cell_instrmnt_rl_ids(combination_items)
        if len(cell_instrmnt_ids_list) > 0:
            self._update_categories(context, cube_name, cell_instrmnt_ids_list,
                                    main_categories_in_scope, "INSTRMNT_TYP_PRDCT")
        elif len(cell_scrty_derivative_list)>0:
            self._update_categories(context, cube_name, cell_scrty_derivative_list,
                                    main_categories_in_scope, "SCRTY_EXCHNG_TRDBL_DRVTV_TYP")
        elif len(cell_instrmnt_rl_ids_list) > 0:
            self._update_categories(context, cube_name, cell_instrmnt_rl_ids_list,
                                    main_categories_in_scope, "INSTRMNT_RL_TYP")
        else:
            self._process_accounting_items(context, combination_items,
                                           cube_name, main_categories_in_scope)


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
            if combination_item.variable_id and combination_item.variable_id.variable_id == "INSTRMNT_TYP_PRDCT":
                #ignore the member TYP_INSTRMNT_-1
                if not (combination_item.member_id.member_id == 'TYP_INSTRMNT_-1'):
                    if combination_item.member_id not in cell_instrmnt_ids_list:
                        cell_instrmnt_ids_list.append(combination_item.member_id)
                else:
                    print("ignoring TYP_INSTRMNT_-1")
        return cell_instrmnt_ids_list

    def _get_cell_instrmnt_rl_ids(self, combination_items):
        """
        Get cell instrument IDs from combination items.

        Args:
            combination_items (list): List of combination items.

        Returns:
            list: List of cell instrument IDs.
        """
        cell_instrmnt_rl_ids_list = []
        for combination_item in combination_items:
            if combination_item.variable_id and combination_item.variable_id.variable_id == "INSTRMNT_RL_TYP":
                #ignore the member TYP_INSTRMNT_-1
                if not (combination_item.member_id.member_id == 'ABSTRCT_INSTRMNT_RL_TYP_-1'):
                    if combination_item.member_id not in cell_instrmnt_rl_ids_list:
                        cell_instrmnt_rl_ids_list.append(combination_item.member_id)
                else:
                    print("ignoring ABSTRCT_INSTRMNT_RL_TYP_-1")
        return cell_instrmnt_rl_ids_list

    def _get_cell_scrty_derivative_ids(self, combination_items):
        """
        Get cell instrument IDs from combination items.

        Args:
            combination_items (list): List of combination items.

        Returns:
            list: List of cell instrument IDs.
        """
        cell_scrty_derivative_list = []
        for combination_item in combination_items:
            if combination_item.variable_id and combination_item.variable_id.variable_id == "SCRTY_EXCHNG_TRDBL_DRVTV_TYP":
                #ignore the member SCRTY_EXCHNG_TRDBL_DRVTV_TYP_-1
                if not (combination_item.member_id.member_id == 'SCRTY_EXCHNG_TRDBL_DRVTV_TYP_-1'):
                    if combination_item.member_id not in cell_scrty_derivative_list:
                        cell_scrty_derivative_list.append(combination_item.member_id)
        return cell_scrty_derivative_list

    def _get_cell_scrty_derivative_ids(self, combination_items):
        """
        Get cell instrument IDs from combination items.

        Args:
            combination_items (list): List of combination items.

        Returns:
            list: List of cell instrument IDs.
        """
        cell_scrty_derivative_list = []
        for combination_item in combination_items:
            if combination_item.variable_id and combination_item.variable_id.variable_id == "SCRTY_EXCHNG_TRDBL_DRVTV_TYP":
                #ignore the member SCRTY_EXCHNG_TRDBL_DRVTV_TYP_-1
                if not (combination_item.member_id.member_id == 'SCRTY_EXCHNG_TRDBL_DRVTV_TYP_-1'):
                    if combination_item.member_id not in cell_scrty_derivative_list:
                        cell_scrty_derivative_list.append(combination_item.member_id)
        return cell_scrty_derivative_list

    def _update_categories(self, context, cube_name, ids_list, main_categories_in_scope, prefix):
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

    def _process_accounting_items(self, context, combination_items, cube_name, main_categories_in_scope):
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
            if combination_item.variable_id and combination_item.variable_id.variable_id == "TYP_ACCNTNG_ITM":
                if combination_item.member_id not in cell_accntng_itm_ids_list:
                    cell_accntng_itm_ids_list.append(combination_item.member_id)

        self._update_categories(context, cube_name, cell_accntng_itm_ids_list, main_categories_in_scope, "TYP_ACCNTNG_ITM")

    #def create_draft_join_for_product_file(self, context, sdd_context, framework):
    #    '''
    #    Create a draft of the join for product file, this should be reviewed and edited
    #    and the edited version used as an input for processing
    #    '''
    #    main_categories_in_scope = (
    #        context.main_categories_in_scope_finrep if framework == "FINREP_REF"
    #        else context.main_categories_in_scope_ae
    #    )
    #    subdirectory = ("finrep_transformation_meta_data_ldm" if framework == "FINREP_REF"
    #                    else "ae_transformation_meta_data_ldm")

    #    output_file = os.path.join(context.output_directory,
    #                               'transformation_meta_data_csv',
    #                               subdirectory,
    #                               f'join_for_products_draft_{framework}.csv')
    #    with open(output_file, "a", encoding='utf-8') as f:
    #        f.write("description,classifier,value,description,Main Category\n")
    #        f.write(",,,," + "\n,".join(main_categories_in_scope) + "\n")
    #        for mc in main_categories_in_scope:
    #            mc_member = ImportWebsiteToSDDModel.find_member_with_id(self, mc, sdd_context)
    #            definition = mc_member.name
    #            if ',' in definition :
    #                print(mc_member.member_id + " : " + definition  + \
    #                    " is a composite catagory")
    #            elif '.' in definition :
    #                print(mc_member.member_id + " : " + definition  + \
    #                    " is a sub catagory")
    #            else:
    #                target_instrument_type = MainCategoryFinder.\
    #                           get_target_instrument_type_from_mapping(
    #                        self,sdd_context,mc_member)
    #                if not(target_instrument_type is None):
    #                    f.write(definition + ",TYP_INSTRMNT," + \
    #                            target_instrument_type.replace(',',' ').\
    #                                replace('TYP_INSTRMNT_','') + "," + mc +'\n')
    #            else:
    #                target_accounting_type = MainCategoryFinder.\
    #                            get_target_accounting_type_from_mapping(
    #                            self,sdd_context,mc_member)
    #                if not(target_accounting_type is  None):
    #                    f.write(definition + ",TYP_ACCNTNG_ITM," + \
    #                            target_accounting_type.replace(',',' ').\
    #                                replace('TYP_ACCNTNG_ITM_','') + "," \
    #                                    +  mc +'\n')
    #            else:
    #                f.write(definition + ",NOTHIN_FOUND,," + mc +'\n')

    #    f.close()

    def create_join_for_product_to_main_category_map(self, context, sdd_context, framework):
        '''
        Create a map from join for products to main categories.
        Supports many-to-many: a join_for_product can belong to multiple main categories.

        Uses the member ID as the key (extracted from condition string).
        '''
        file_location = os.path.join("resources", "joins_configuration",
                                     f"join_for_product_to_reference_category_{framework}.csv")
        join_for_products_to_main_category_map = self._get_framework_map(
            context, 'join_for_products_to_main_category_map', framework
        )

        with open(file_location, encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            next(filereader)  # Skip header
            for main_category, _, join_for_product in filereader:
                if not main_category.strip():
                    member_id = "__NO_BREAKDOWN__"
                else:
                    # Extract member ID from condition string for key lookup
                    condition = BreakdownCondition(main_category)
                    members = condition.get_members()
                    member_id = members[0] if members else main_category

                join_for_products_to_main_category_map.setdefault(join_for_product, []).append(member_id)

    def create_il_tables_for_main_category_map(self, context, sdd_context, framework):
        '''
        Create a map from main categories such as loans and advances
        to the related input layer such as instrument
        '''
        file_location = os.path.join("resources", "joins_configuration",
                                     f"join_for_product_ldm_definitions_{framework}.csv")
        if not (context.ldm_or_il == "ldm"):
            file_location = os.path.join("resources", "joins_configuration",
                                     f"join_for_product_il_definitions_{framework}.csv")
        tables_for_main_category_map = self._get_framework_map(
            context, 'tables_for_main_category_map', framework
        )
        join_for_products_to_main_category_map = self._get_framework_map(
            context, 'join_for_products_to_main_category_map', framework
        )

        with open(file_location, encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            next(filereader)  # Skip header
            for join_for_product_name, il_table, *_ in filereader:
                try:
                    # Handle list of main categories (many-to-many support)
                    main_categories = join_for_products_to_main_category_map[join_for_product_name]
                    for main_category in main_categories:
                        tables_for_main_category_map.setdefault(main_category, []).append(il_table)
                except KeyError:
                    # print(f"Could not find main category for join for product {join_for_product_name}")
                    pass

    def create_join_for_products_for_main_category_map(self, context, sdd_context, framework):
        '''
        Create a map from main categories such as loans and advances
        to the related join for products, where join for product is a combination
        of an input layer and main category description
        '''
        file_location = os.path.join("resources", "joins_configuration",
                                     f"join_for_product_ldm_definitions_{framework}.csv")
        if not (context.ldm_or_il == "ldm"):
            file_location = os.path.join("resources", "joins_configuration",
                                     f"join_for_product_il_definitions_{framework}.csv")
        join_for_products_to_linked_tables_map = self._get_framework_map(
            context, 'join_for_products_to_linked_tables_map', framework
        )
        join_for_products_to_to_filter_map = self._get_framework_map(
            context, 'join_for_products_to_to_filter_map', framework
        )
        table_and_part_tuple_map = self._get_framework_map(
            context, 'table_and_part_tuple_map', framework
        )
        join_for_products_to_main_category_map = self._get_framework_map(
            context, 'join_for_products_to_main_category_map', framework
        )

        with open(file_location, encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            next(filereader)  # Skip header
            for join_for_product_name, il_table, the_filter, linked_table_list, comments in filereader:
                try:
                    # Handle list of main categories (many-to-many support)
                    main_categories = join_for_products_to_main_category_map[join_for_product_name]
                    table_and_part_tuple = (il_table, join_for_product_name)
                    join_for_products_to_linked_tables_map[table_and_part_tuple] = linked_table_list
                    join_for_products_to_to_filter_map[table_and_part_tuple] = the_filter
                    for main_category in main_categories:
                        table_and_part_tuple_map.setdefault(main_category, []).append(table_and_part_tuple)
                except KeyError:
                    print(f"Could not find main category for the join for product {join_for_product_name}")
