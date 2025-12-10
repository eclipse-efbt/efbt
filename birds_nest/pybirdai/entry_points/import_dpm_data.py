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
# This script imports DPM data from the EBA website and converts it to SDD format

import django
import os
from django.apps import AppConfig
from pybirdai.context.sdd_context_django import SDDContext
from django.conf import settings
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class RunImportDPMData(AppConfig):
    """
    Django AppConfig for running the DPM data import process.

    This class sets up the necessary context and runs the import process
    to download and convert DPM data from the EBA website into SDD format.
    """

    path = os.path.join(settings.BASE_DIR, 'birds_nest')
    logger = logging.getLogger(__name__)

    @staticmethod
    def run_import_phase_a(frameworks:list=None):
        """
        Run DPM Step 1: Extract DPM Metadata (lightweight extraction before table selection).

        Extracts basic entities (frameworks, domains, metrics, members, hierarchies, tables)
        and writes all CSVs to results/technical_export/ for user selection.
        Step 2 will reload CSVs and process selected tables.

        Args:
            frameworks: List of framework codes to import (e.g., ['FINREP', 'COREP']).
                       If None, defaults to ['COREP'] for backward compatibility.

        Returns:
            dict: Status information including table count
        """
        logger = logging.getLogger(__name__)

        if frameworks is None:
            frameworks = ['COREP']
            logger.info("No frameworks specified, defaulting to COREP")

        logger.info(f"Starting DPM Step 1 (Extract Metadata) with frameworks={frameworks}")

        from pybirdai.process_steps.dpm_integration.dpm_integration_service import DPMImporterService
        from django.conf import settings

        base_dir = settings.BASE_DIR
        dpm_service = DPMImporterService(output_directory=os.path.join(base_dir, 'results'))

        try:
            # Ensure DPM database is downloaded and extracted (creates CSV files in target/)
            dpm_service.ensure_dpm_database_extracted()

            # Run Step 1 - writes all CSVs to results/technical_export/
            phase_a_data = dpm_service.map_csvs_phase_a(frameworks=frameworks)
            logger.info("Step 1 completed - all CSVs written to results/technical_export/")

            # Count tables available for selection
            import pandas as pd
            table_csv_path = os.path.join(base_dir, 'results', 'technical_export', 'table.csv')
            tables_df = pd.read_csv(table_csv_path)
            table_count = len(tables_df)

            logger.info(f"Step 1 complete - {table_count} tables available for selection")

            return {
                'success': True,
                'table_count': table_count,
                'message': f'{table_count} tables available for selection'
            }

        except Exception as e:
            logger.error(f"Error during DPM Step 1: {e}", exc_info=True)
            raise

    @staticmethod
    def run_import_phase_b(selected_tables:list=None, enable_table_duplication:bool=True):
        """
        Run DPM Step 2: Process Selected Tables & Import to Database.

        Reloads CSVs from Step 1, filters by selected_tables, runs ordinate explosion
        (the expensive operation), and imports everything into the database.
        This combines table processing with database import in a single step.

        Args:
            selected_tables: List of table_ids to process (filters before explosion)
            enable_table_duplication: Whether to enable Z-axis table duplication (default: True)

        Returns:
            dict: Statistics about what was imported
        """
        import pandas as pd
        logger = logging.getLogger(__name__)

        logger.info(f"Starting DPM Step 2 (Process & Import) with selected_tables={len(selected_tables) if selected_tables else 'all'}, duplication={enable_table_duplication}")

        from pybirdai.process_steps.dpm_integration.dpm_integration_service import DPMImporterService
        from pybirdai.process_steps.website_to_sddmodel.import_website_to_sdd_model_django import ImportWebsiteToSDDModel
        import pybirdai.process_steps.dpm_integration.mapping_functions as new_maps
        from django.conf import settings

        base_dir = settings.BASE_DIR
        csv_dir = os.path.join(base_dir, 'results', 'technical_export')

        try:
            # Check if basic CSVs from Step 1 exist
            table_csv_path = os.path.join(csv_dir, 'table.csv')
            if not os.path.exists(table_csv_path):
                logger.warning("Step 1 CSVs not found - running Step 1 first to generate them")
                # Run Step 1 with default frameworks (will default to COREP)
                step_1_result = RunImportDPMData.run_import_phase_a(frameworks=None)
                logger.info(f"Step 1 auto-run completed: {step_1_result.get('table_count', 0)} tables generated")

            # Reload basic metadata CSVs from Step 1
            logger.info("Reloading metadata CSVs from Step 1")

            domains_array = pd.read_csv(os.path.join(csv_dir, 'domain.csv'))
            members_array = pd.read_csv(os.path.join(csv_dir, 'member.csv'))
            dimensions_array = pd.read_csv(os.path.join(csv_dir, 'variable.csv'))
            hierarchy_df = pd.read_csv(os.path.join(csv_dir, 'member_hierarchy.csv'))
            hierarchy_node_df = pd.read_csv(os.path.join(csv_dir, 'member_hierarchy_node.csv'))
            tables_df = pd.read_csv(os.path.join(csv_dir, 'table.csv'))

            # Load ID mapping dictionaries from JSON files (saved by Phase A)
            # These maps convert DPM numeric IDs to string-based BIRD IDs
            logger.info("Loading ID mapping dictionaries from JSON files")
            import json

            def load_id_map(map_name):
                """Load ID map from JSON file"""
                map_path = os.path.join(csv_dir, f'{map_name}.json')
                if os.path.exists(map_path):
                    with open(map_path, 'r', encoding='utf-8') as f:
                        return json.load(f)
                else:
                    logger.warning(f"{map_name}.json not found - Phase A may not have been run correctly")
                    return {}

            member_map = load_id_map('member_map')
            dimension_map = load_id_map('dimension_map')
            hierarchy_map = load_id_map('hierarchy_map')
            metrics_map = load_id_map('metrics_map')
            domain_map = load_id_map('domain_map')

            # table_map maps TABLE_VID → TABLE_ID (loaded from JSON, not CSV)
            # CSV no longer contains TABLE_VID column to maintain compatibility with BIRD import
            table_map = load_id_map('table_map')

            logger.info(f"Loaded ID mapping dictionaries from JSON files:")
            logger.info(f"  - member_map: {len(member_map)} mappings")
            logger.info(f"  - dimension_map: {len(dimension_map)} mappings")
            logger.info(f"  - hierarchy_map: {len(hierarchy_map)} mappings")
            logger.info(f"  - metrics_map: {len(metrics_map)} mappings")
            logger.info(f"  - domain_map: {len(domain_map)} mappings")

            # Log sample mappings for verification
            if member_map:
                sample_member = list(member_map.items())[0]
                logger.info(f"  - Sample member_map: {sample_member[0]} → {sample_member[1]}")
            if dimension_map:
                sample_dim = list(dimension_map.items())[0]
                logger.info(f"  - Sample dimension_map: {sample_dim[0]} → {sample_dim[1]}")

            # Build phase_a_data structure
            phase_a_data = {
                'new_maps': new_maps,
                'member_map': member_map,
                'dimension_map': dimension_map,
                'hierarchy_map': hierarchy_map,
                'metrics_map': metrics_map,
                'dimensions_array': dimensions_array,
                'members_array': members_array,
                'hierarchy_df': hierarchy_df,
                'hierarchy_node_df': hierarchy_node_df,
                'tables_df': tables_df,
                'table_map': table_map,
            }

            logger.info("Metadata CSVs reloaded and maps reconstructed")

            # Run Step 2 table processing with filtering and mapping
            # Step 2 will map axes/ordinates/cells/positions for FILTERED tables only
            from pybirdai.process_steps.dpm_integration.dpm_integration_service import DPMImporterService

            # Create service instance with preserve_existing=True to keep Step 1 metadata CSVs
            dpm_service = DPMImporterService(
                output_directory=os.path.join(base_dir, 'results'),
                preserve_existing=True
            )

            # Process selected tables - map axes/ordinates/cells for filtered tables
            dpm_service.map_csvs_phase_b(
                phase_a_data=phase_a_data,
                selected_tables=selected_tables,
                enable_table_duplication=enable_table_duplication
            )
            logger.info("Step 2 table processing completed successfully")

            # Import into database
            # Note: table.csv no longer contains TABLE_VID column (it's only in JSON mappings)
            sdd_context = SDDContext()
            sdd_context.file_directory = os.path.join(base_dir, 'results')
            sdd_context.output_directory = os.path.join(base_dir, 'results')

            logger.info("Importing report templates into database")
            ImportWebsiteToSDDModel().import_report_templates_from_sdd(sdd_context, dpm=True)

            logger.info("Importing hierarchies into database")
            ImportWebsiteToSDDModel().import_hierarchies_from_sdd(sdd_context)

            logger.info("Step 2 completed successfully (processing + import)")

            return {
                'tables_imported': len(selected_tables) if selected_tables else 'all',
                'success': True
            }

        except Exception as e:
            logger.error(f"Error during DPM Step 2: {e}", exc_info=True)
            raise

    @staticmethod
    def run_import(import_:bool, frameworks:list=None, selected_tables:list=None):
        """
        Run DPM import process (backward compatibility wrapper).

        Args:
            import_: If False, maps DPM metadata to CSV. If True, imports CSV into database.
            frameworks: List of framework codes to import (e.g., ['FINREP', 'COREP']).
                       If None, defaults to ['COREP'] for backward compatibility.
            selected_tables: Optional list of table_ids to filter (only applies if import_=False)
        """
        logger = logging.getLogger(__name__)

        if frameworks is None:
            frameworks = ['COREP']
            logger.info("No frameworks specified, defaulting to COREP")

        logger.info(f"Starting DPM import process with import_={import_}, frameworks={frameworks}, selected_tables={len(selected_tables) if selected_tables else 'all'}")

        from pybirdai.process_steps.dpm_integration.dpm_integration_service import DPMImporterService
        from pybirdai.process_steps.website_to_sddmodel.import_website_to_sdd_model_django import ImportWebsiteToSDDModel
        from pybirdai.context.context import Context
        from django.conf import settings

        base_dir = settings.BASE_DIR

        sdd_context = SDDContext()
        sdd_context.file_directory = os.path.join(base_dir, 'results')
        sdd_context.output_directory = os.path.join(base_dir, 'results')
        sdd_context.selected_frameworks = frameworks

        context = Context()
        context.file_directory = sdd_context.output_directory
        context.output_directory = sdd_context.output_directory
        context.selected_frameworks = frameworks

        try:
            # Run DPM import service
            if not import_:
                logger.info("Mapping the DPM Metadata")
                dpm_service = DPMImporterService(output_directory=context.file_directory)
                dpm_service.run_application(frameworks=frameworks, selected_tables=selected_tables)
                logger.info("DPM metadata mapping completed successfully")

            # Import into database
            if import_:
                logger.info("Running Import on the DPM Metadata")
                ImportWebsiteToSDDModel().import_report_templates_from_sdd(sdd_context, dpm=True)
                logger.info("Report templates import completed successfully")

                ImportWebsiteToSDDModel().import_hierarchies_from_sdd(sdd_context)
                logger.info("Hierarchy import completed successfully")

        except Exception as e:
            logger.error(f"Error during DPM import process: {e}", exc_info=True)
            raise

        logger.info("DPM import process completed")

    def ready(self):
        # This method is still needed for Django's AppConfig
        self.logger.debug("RunImportDPMData AppConfig is ready")
        pass
