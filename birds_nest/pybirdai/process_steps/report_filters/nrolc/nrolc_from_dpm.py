# coding=UTF-8
# Copyright (c) 2025 Arfa Digital Consulting
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDX-License-Identifier: EPL-2.0
#
# Contributors:
#    Benjamin Arfa - initial API and implementation
#

"""
Non-Reference Output Layer Creator from DPM (Data Point Model).
Main orchestrator for creating output layer objects from DPM table renderings.
"""

from pybirdai.models.bird_meta_data_model import TABLE, SUBDOMAIN
from django.db.models import Q
from datetime import datetime
import logging

from pybirdai.process_steps.report_filters.nrolc.nrolc_queries import TableFrameworkQueries
from pybirdai.process_steps.report_filters.nrolc.nrolc_data_fetcher import DataFetcher
from pybirdai.process_steps.report_filters.nrolc.nrolc_subdomain_manager import SubdomainManager
from pybirdai.process_steps.report_filters.nrolc.nrolc_object_builders import OutputLayerBuilder
from pybirdai.process_steps.report_filters.nrolc.nrolc_persistence import PersistenceManager
from pybirdai.process_steps.report_filters.nrolc import nrolc_utils


class NonReferenceOutputLayerCreator:
    """
    Creates non-reference output layers from DPM table renderings.
    Orchestrates all sub-modules to build cubes, combinations, and structures.
    """

    def __init__(self):
        """Initialize the output layer creator."""
        self.memoization = {}

        # Lists to collect objects for bulk creation
        self.cubes_to_create = []
        self.cube_structures_to_create = []
        self.combinations_to_create = []
        self.combination_items_to_create = []
        self.cube_structure_items_to_create = []
        self.cube_to_combinations_to_create = []
        self.subdomains_to_create = []
        self.subdomain_enumerations_to_create = []
        self.cells_to_update = []

        # Counter for unique combination IDs
        self.combination_counter = 0
        self.timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

        # Cache for existing subdomains to avoid N+1 queries
        self.existing_subdomains = {}

        # Initialize sub-modules
        self.queries = TableFrameworkQueries()
        self.data_fetcher = DataFetcher()
        self.subdomain_manager = None  # Will be initialized with references
        self.object_builder = None  # Will be initialized with subdomain_manager

    def create_output_layers(self, table, framework_object, save_to_db=True):
        """
        Main method to create output layers for a given table.

        Args:
            table: TABLE instance
            framework_object: FRAMEWORK instance
            save_to_db: Whether to save objects to database (default: True)

        Returns:
            tuple: (cube, cube_structure)
        """
        self.save_to_db = save_to_db

        # Reset bulk creation lists
        self.cubes_to_create = []
        self.cube_structures_to_create = []
        self.combinations_to_create = []
        self.combination_items_to_create = []
        self.cube_structure_items_to_create = []
        self.cube_to_combinations_to_create = []
        self.subdomains_to_create = []
        self.subdomain_enumerations_to_create = []
        self.cells_to_update = []
        self.combination_counter = 0
        self.timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

        # Fetch all necessary data
        cells, cell_positions, ordinate_items = self.data_fetcher.fetch_objects_for_creation(table)

        # Pre-fetch all existing subdomains that might be needed (avoid N+1 queries)
        potential_subdomain_ids = [
            nrolc_utils.generate_subdomain_id(item.variable_id.variable_id, table.table_id)
            for item in ordinate_items if item.variable_id
        ]

        # Fetch all matching subdomains using chunking
        self.existing_subdomains = {}
        for subdomain_chunk in self.data_fetcher.chunk_list(potential_subdomain_ids):
            chunk_subdomains = SUBDOMAIN.objects.filter(subdomain_id__in=subdomain_chunk)
            for sd in chunk_subdomains:
                self.existing_subdomains[sd.subdomain_id] = sd

        # Initialize subdomain manager with references to lists
        self.subdomain_manager = SubdomainManager(
            self.subdomains_to_create,
            self.subdomain_enumerations_to_create,
            self.existing_subdomains
        )

        # Initialize object builder with subdomain manager
        self.object_builder = OutputLayerBuilder(self.subdomain_manager)

        # Create cube and cube structure
        cube, cube_structure = self.object_builder.create_cube_and_structure(table, framework_object)
        self.cube_structures_to_create.append(cube_structure)
        self.cubes_to_create.append(cube)

        # Create combinations and items from cells
        combination_counter_ref = {'value': self.combination_counter}
        combinations, combination_items, cube_to_combinations, cells_to_update = \
            self.object_builder.create_combinations_and_items(
                cells, cell_positions, ordinate_items, cube,
                self.timestamp, combination_counter_ref
            )
        self.combination_counter = combination_counter_ref['value']
        self.combinations_to_create.extend(combinations)
        self.combination_items_to_create.extend(combination_items)
        self.cube_to_combinations_to_create.extend(cube_to_combinations)
        self.cells_to_update.extend(cells_to_update)

        # Create cube structure items from all variables in the table
        cube_structure_items = self.object_builder.create_cube_structure_items(
            ordinate_items, cube_structure
        )
        self.cube_structure_items_to_create.extend(cube_structure_items)

        # Bulk save all created objects if requested
        if save_to_db:
            PersistenceManager.bulk_save_objects(
                self.cube_structures_to_create,
                self.cubes_to_create,
                self.combinations_to_create,
                self.subdomains_to_create,
                self.subdomain_enumerations_to_create,
                self.combination_items_to_create,
                self.cube_structure_items_to_create,
                self.cube_to_combinations_to_create,
                self.cells_to_update
            )

        return cube, cube_structure

    def process_by_framework_version(self, framework, version, save_to_db=True):
        """
        Process all tables for a specific framework version.

        Args:
            framework: Framework identifier (e.g., 'EBA_FINREP')
            version: Version string (e.g., '3.0.0')
            save_to_db: Whether to save results to database

        Returns:
            dict: Processing results with status, processed tables, and errors
        """
        results = {
            'status': 'success',
            'framework': framework,
            'version': version,
            'processed': [],
            'errors': []
        }

        try:
            # Get framework object
            framework_object = self.queries.get_framework_object(framework)
            if not framework_object:
                results['status'] = 'error'
                results['message'] = f"Framework '{framework}' not found in database"
                return results

            # Get tables for this framework version
            tables = self.queries.get_tables_by_framework_version(framework, version)

            if not tables:
                results['status'] = 'warning'
                results['message'] = f"No tables found for framework '{framework}' version '{version}'"
                return results

            # Process each table
            for table in tables:
                try:
                    cube, cube_structure = self.create_output_layers(table, framework_object, save_to_db)
                    results['processed'].append({
                        'table_id': table.table_id,
                        'table_name': table.name,
                        'cube_id': cube.cube_id,
                        'cube_structure_id': cube_structure.cube_structure_id
                    })
                except Exception as e:
                    results['errors'].append({
                        'table_id': table.table_id,
                        'error': str(e)
                    })

            if results['errors'] and not results['processed']:
                results['status'] = 'error'
            elif results['errors']:
                results['status'] = 'partial'

        except Exception as e:
            results['status'] = 'error'
            results['message'] = f"Error processing framework version: {str(e)}"

        return results

    def process_by_framework(self, framework, save_to_db=True):
        """
        Process all tables for a framework (all versions).

        Args:
            framework: Framework identifier (e.g., 'EBA_FINREP')
            save_to_db: Whether to save results to database

        Returns:
            dict: Processing results with status, processed tables, and errors
        """
        results = {
            'status': 'success',
            'framework': framework,
            'processed': [],
            'errors': []
        }

        try:
            # Get framework object
            framework_object = self.queries.get_framework_object(framework)
            if not framework_object:
                results['status'] = 'error'
                results['message'] = f"Framework '{framework}' not found in database"
                return results

            # Get all tables for this framework
            tables = self.queries.get_tables_by_framework(framework)

            if not tables:
                results['status'] = 'warning'
                results['message'] = f"No tables found for framework '{framework}'"
                return results

            # Process each table
            for table in tables:
                try:
                    cube, cube_structure = self.create_output_layers(table, framework_object, save_to_db)
                    results['processed'].append({
                        'table_id': table.table_id,
                        'table_name': table.name,
                        'cube_id': cube.cube_id,
                        'cube_structure_id': cube_structure.cube_structure_id,
                        'version': nrolc_utils.extract_version_from_table_id(table.table_id)
                    })
                except Exception as e:
                    results['errors'].append({
                        'table_id': table.table_id,
                        'error': str(e)
                    })

            if results['errors'] and not results['processed']:
                results['status'] = 'error'
            elif results['errors']:
                results['status'] = 'partial'

        except Exception as e:
            results['status'] = 'error'
            results['message'] = f"Error processing framework: {str(e)}"

        return results

    def process_by_table_code_version(self, table_code, version, save_to_db=True):
        """
        Process a specific table with a specific version.

        Args:
            table_code: Table code (e.g., 'F01.01')
            version: Version string (e.g., '3.0.0')
            save_to_db: Whether to save results to database

        Returns:
            dict: Processing results with status and table details
        """
        results = {
            'status': 'success',
            'table_code': table_code,
            'version': version,
            'processed': None,
            'error': None
        }

        try:
            # Get table by code and version
            logging.info(f"Looking for table with code: '{table_code}' and version: '{version}'")
            table = self.queries.get_table_by_code_version(table_code, version)

            if not table:
                error_msg = f"Table with code '{table_code}' and version '{version}' not found"
                logging.error(error_msg)
                results['status'] = 'error'
                results['error'] = error_msg
                return results

            logging.info(f"Found table: {table.table_id} (name='{table.name}')")

            # Extract framework from table ID
            framework_id = nrolc_utils.extract_framework_from_table_id(table.table_id)
            logging.debug(f"Extracted framework_id: '{framework_id}' from table_id: '{table.table_id}'")

            framework_object = self.queries.get_framework_object(framework_id) if framework_id else None

            if not framework_object:
                error_msg = f"Framework '{framework_id}' not found for table '{table.table_id}', proceeding without framework"
                logging.warning(error_msg)
                results['status'] = 'warning'
                results['error'] = error_msg

            # Process the table
            logging.info(f"Creating output layers for table {table.table_id} with framework {framework_id}")
            cube, cube_structure = self.create_output_layers(table, framework_object, save_to_db)

            results['processed'] = {
                'table_id': table.table_id,
                'table_name': table.name,
                'cube_id': cube.cube_id,
                'cube_structure_id': cube_structure.cube_structure_id,
                'framework': framework_id
            }
            logging.info(f"Successfully processed table {table.table_id}")

        except Exception as e:
            error_msg = f"Exception processing table code '{table_code}' version '{version}': {str(e)}"
            logging.error(error_msg, exc_info=True)
            results['status'] = 'error'
            results['error'] = str(e)

        return results

    def process_by_table_code(self, table_code, save_to_db=True):
        """
        Process all tables matching a table code (across all versions).

        Args:
            table_code: Table code (e.g., 'F01.01')
            save_to_db: Whether to save results to database

        Returns:
            dict: Processing results with status, processed tables, and errors
        """
        results = {
            'status': 'success',
            'table_code': table_code,
            'processed': [],
            'errors': []
        }

        try:
            # Get all tables with this code
            logging.info(f"Looking for tables with code: '{table_code}'")
            logging.debug(f"Query filters: table_id ends with '_{table_code}' OR name='{table_code}' OR code='{table_code}'")

            tables = TABLE.objects.filter(
                Q(table_id__endswith=f"_{table_code}") |
                Q(name=table_code) |
                Q(code=table_code)
            )

            table_count = tables.count()
            logging.info(f"Found {table_count} table(s) matching code '{table_code}'")

            if not tables:
                error_msg = f"No tables found with code '{table_code}'"
                logging.error(error_msg)
                logging.debug(f"Query attempted: table_id__endswith='_{table_code}', name='{table_code}', code='{table_code}'")
                results['status'] = 'error'
                results['message'] = error_msg
                return results

            # Process each table
            for table in tables:
                try:
                    logging.info(f"Processing table: {table.table_id} (name='{table.name}', code='{getattr(table, 'code', 'N/A')}')")

                    # Extract framework from table ID
                    framework_id = nrolc_utils.extract_framework_from_table_id(table.table_id)
                    logging.debug(f"Extracted framework_id: '{framework_id}' from table_id: '{table.table_id}'")

                    framework_object = self.queries.get_framework_object(framework_id) if framework_id else None

                    if not framework_object:
                        error_msg = f"Framework '{framework_id}' not found for table {table.table_id}, skipping"
                        logging.warning(error_msg)
                        results['errors'].append({
                            'table_id': table.table_id,
                            'error': error_msg
                        })
                        continue

                    logging.info(f"Creating output layers for table {table.table_id} with framework {framework_id}")
                    cube, cube_structure = self.create_output_layers(table, framework_object, save_to_db)

                    results['processed'].append({
                        'table_id': table.table_id,
                        'table_name': table.name,
                        'cube_id': cube.cube_id,
                        'cube_structure_id': cube_structure.cube_structure_id,
                        'framework': framework_id,
                        'version': nrolc_utils.extract_version_from_table_id(table.table_id)
                    })
                    logging.info(f"Successfully processed table {table.table_id}")

                except Exception as e:
                    error_msg = f"Exception processing table {table.table_id}: {str(e)}"
                    logging.error(error_msg, exc_info=True)
                    results['errors'].append({
                        'table_id': table.table_id,
                        'error': str(e)
                    })

            if results['errors'] and not results['processed']:
                results['status'] = 'error'
                logging.error(f"All tables failed for code '{table_code}': {len(results['errors'])} error(s)")
            elif results['errors']:
                results['status'] = 'partial'
                logging.warning(f"Partial success for code '{table_code}': {len(results['processed'])} succeeded, {len(results['errors'])} failed")

        except Exception as e:
            error_msg = f"Error processing table code '{table_code}': {str(e)}"
            logging.error(error_msg, exc_info=True)
            results['status'] = 'error'
            results['message'] = error_msg

        return results
