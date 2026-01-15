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
"""Entry point for creating generation rules."""

import os
from pathlib import Path

import django
from django.apps import AppConfig
from django.conf import settings

class RunCreateJoinsMetadata(AppConfig):
    """Django AppConfig for running the creation of generation rules."""

    path = os.path.join(settings.BASE_DIR, 'birds_nest')

    @staticmethod
    def run_create_joins_meta_data():
        """Execute the process of creating generation rules when the app is ready."""
        print("Running create transformation metadata")
        from pybirdai.process_steps.input_model.import_database_to_sdd_model import (
            ImportDatabaseToSDDModel
        )
        from pybirdai.context.sdd_context_django import SDDContext
        from pybirdai.context.context import Context

        from pybirdai.process_steps.joins_meta_data.create_joins_meta_data import (
            JoinsMetaDataCreator
        )
        from pybirdai.process_steps.joins_meta_data.main_category_finder import (
            MainCategoryFinder
        )

        base_dir = settings.BASE_DIR
        sdd_context = SDDContext()
        sdd_context.file_directory = os.path.join(base_dir, 'resources')
        sdd_context.output_directory = os.path.join(base_dir, 'results')

        context = Context()
        context.file_directory = sdd_context.file_directory
        context.output_directory = sdd_context.output_directory

        #ImportDatabaseToSDDModel().import_sdd(sdd_context)

        MainCategoryFinder().create_report_to_main_category_maps(
            context,
            sdd_context,
            "FINREP_REF",
            ["3", "3.0-Ind", "FINREP 3.0-Ind"]
        )
        JoinsMetaDataCreator().generate_joins_meta_data(
            context,
            sdd_context,
            "FINREP_REF"
        )
        
    @staticmethod
    def run_create_joins_meta_data_DPM(frameworks=None):
        """Execute the process of creating generation rules when the app is ready.

        Args:
            frameworks: List of frameworks to process (e.g., ['FINREP', 'COREP']).
                       If None, defaults to ['FINREP'].
        """
        print("Running create transformation metadata")
        from pybirdai.process_steps.input_model.import_database_to_sdd_model import (
            ImportDatabaseToSDDModel
        )
        from pybirdai.context.sdd_context_django import SDDContext
        from pybirdai.context.context import Context

        from pybirdai.process_steps.joins_meta_data.create_joins_meta_data import (
            JoinsMetaDataCreator
        )
        from pybirdai.process_steps.joins_meta_data.main_category_finder import (
            MainCategoryFinder
        )

        # Framework version mappings
        FRAMEWORK_VERSIONS = {
            'FINREP': ["3", "3.0-Ind", "FINREP 3.0-Ind"],
            'FINREP_REF': ["3", "3.0-Ind", "FINREP 3.0-Ind"],
            'COREP': ["4", "4.0", "COREP 4.0"],
            'COREP_REF': ["4", "4.0", "COREP 4.0"],
        }

        if frameworks is None:
            frameworks = ['FINREP']

        base_dir = settings.BASE_DIR
        sdd_context = SDDContext()
        sdd_context.file_directory = os.path.join(base_dir, 'resources')
        sdd_context.output_directory = os.path.join(base_dir, 'results')

        # Reset sdd_context cube link dictionaries to prevent shared state issues
        sdd_context.cube_link_dictionary = {}
        sdd_context.cube_link_to_foreign_cube_map = {}
        sdd_context.cube_structure_item_links_dictionary = {}
        sdd_context.cube_structure_item_link_to_cube_link_map = {}
        sdd_context.cube_link_to_join_identifier_map = {}
        sdd_context.cube_link_to_join_for_report_id_map = {}
        sdd_context.combination_to_rol_cube_map = {}
        sdd_context.combination_item_dictionary = {}
        sdd_context.bird_cube_dictionary = {}
        sdd_context.bird_cube_structure_item_dictionary = {}

        context = Context()
        context.file_directory = sdd_context.file_directory
        context.output_directory = sdd_context.output_directory

        # Initialize framework-specific maps as instance attributes to prevent shared state issues
        # These are needed by MainCategoryFinder for cube links generation
        context.main_category_to_name_map_finrep = {}
        context.main_category_to_name_map_ae = {}
        context.main_category_to_name_map_corep = {}
        context.join_for_products_to_main_category_map_finrep = {}
        context.join_for_products_to_main_category_map_ae = {}
        context.join_for_products_to_main_category_map_corep = {}
        context.tables_for_main_category_map_finrep = {}
        context.tables_for_main_category_map_ae = {}
        context.tables_for_main_category_map_corep = {}
        context.join_for_products_to_linked_tables_map_finrep = {}
        context.join_for_products_to_linked_tables_map_ae = {}
        context.join_for_products_to_linked_tables_map_corep = {}
        context.join_for_products_to_to_filter_map_finrep = {}
        context.join_for_products_to_to_filter_map_ae = {}
        context.join_for_products_to_to_filter_map_corep = {}
        context.table_and_part_tuple_map_finrep = {}
        context.table_and_part_tuple_map_ae = {}
        context.table_and_part_tuple_map_corep = {}
        context.main_categories_in_scope_finrep = []
        context.main_categories_in_scope_ae = []
        context.main_categories_in_scope_corep = []
        context.report_to_main_category_map = {}

        #ImportDatabaseToSDDModel().import_sdd(sdd_context)

        for framework in frameworks:
            # Convert to REF format if needed
            framework_ref = f"{framework}_REF" if not framework.endswith('_REF') else framework
            version = FRAMEWORK_VERSIONS.get(framework_ref, FRAMEWORK_VERSIONS.get(framework, ["4", "4.0", f"{framework} 4.0"]))

            print(f"Processing joins metadata for framework: {framework_ref}")

            MainCategoryFinder().create_report_to_main_category_maps(
                context,
                sdd_context,
                framework_ref,
                version
            )
            JoinsMetaDataCreator().generate_joins_meta_data(
                context,
                sdd_context,
                framework_ref
            )

def ready(self):
        # This method is still needed for Django's AppConfig
        pass
