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

from pybirdai.models.bird_meta_data_model import *
from django.db.models import Q
import re
import os
import csv
from datetime import datetime
import uuid

class CreateNROutputLayers:
    def __init__(self):
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

    def extract_framework_from_table_id(self, table_id):
        """
        Extract framework identifier from table ID.
        Table IDs have format: FRAMEWORK_VERSION_TABLECODE
        e.g., EBA_FINREP_3.0.0_F01.01 -> EBA_FINREP
        """
        # Split by underscore and take all parts except the last two (version and table code)
        parts = table_id.split('_')[:2]
        return '_'.join(parts)

    def extract_version_from_table_id(self, table_id):
        """
        Extract version from table ID.
        e.g., EBA_FINREP_3.0.0_F01.01 -> 3.0.0
        """
        parts = table_id.split('_')
        for part in parts:
            if '.' in part and any(c.isdigit() for c in part):
                return part
        return None

    def get_framework_object(self, framework_id):
        """
        Get FRAMEWORK object from database.
        Returns None if not found.
        """
        try:
            return FRAMEWORK.objects.get(framework_id=framework_id)
        except FRAMEWORK.DoesNotExist:
            return None

    def get_tables_by_framework(self, framework):
        """
        Query tables by framework name.
        """
        framework_upper = framework.upper()
        # Query using various patterns
        tables = TABLE.objects.filter(table_id__contains=framework_upper)
        return tables

    def get_tables_by_framework_version(self, framework, version):
        """
        Query tables by framework name.
        """
        framework_upper = framework.upper()
        # Query using various patterns
        tables = TABLE.objects.filter(
            table_id__contains=framework_upper).filter(
            table_id__contains="_"+version.replace(".","_"))
        return tables

    def get_tables_by_code(self, table_code):
        """
        Get a specific table by its code.
        """
        try:
            return TABLE.objects.get(table_id=table_code)
        except TABLE.DoesNotExist:
            # Try with name
            try:
                return TABLE.objects.get(name=table_code)
            except TABLE.DoesNotExist:
                return None

    def get_table_by_code_version(self, table_code, version):
        """
        Get a specific table by its code.
        """
        try:
            return TABLE.objects.filter(table_id__contains=version).get(table_id=table_code)
        except TABLE.DoesNotExist:
            # Try with name
            try:
                return TABLE.objects.filter(table_id__contains=version).get(name=table_code)
            except TABLE.DoesNotExist:
                return None

    def _fetch_objects_for_creation(self, table: TABLE):
        """
        Fetch all related objects needed for creating output layers.
        """
        # Get all cells for the table
        cells = TABLE_CELL.objects.filter(table_id=table)

        # Get all cell positions for these cells
        cell_positions = CELL_POSITION.objects.filter(
            cell_id__in=cells
        )

        # Get unique axis ordinates from cell positions
        axis_ordinate_ids = cell_positions.values_list('axis_ordinate_id', flat=True).distinct()

        # Get all ordinate items for these axis ordinates
        ordinate_items = ORDINATE_ITEM.objects.filter(
            axis_ordinate_id__in=axis_ordinate_ids
        ).select_related('axis_ordinate_id', 'variable_id', 'member_id')

        return cells, cell_positions, ordinate_items

    def create_output_layers(self, table: TABLE,framework_object:FRAMEWORK, save_to_db=True):
        """
        Main method to create output layers for a given table.
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
        cells, cell_positions, ordinate_items = self._fetch_objects_for_creation(table)

        # Create cube and cube structure
        cube, cube_structure = self.create_cube_and_structure_from_rendering(table,framework_object)

        # Create combinations and items from cells
        self.create_combination_and_items_from_rendering(
            cells, cell_positions, ordinate_items, cube
        )

        # Create cube structure items from all variables in the table
        self.create_cube_structure_items_from_rendering(
            ordinate_items, cube_structure
        )

        # Bulk save all created objects if requested
        if save_to_db:
            self._bulk_save_objects()

        return cube, cube_structure

    def create_cube_and_structure_from_rendering(self, table: TABLE, framework_object: FRAMEWORK):
        """
        Create CUBE and CUBE_STRUCTURE from table rendering.
        """
        # Extract framework for naming
        framework = framework_object.framework_id
        framework_suffix = f"_{framework}" if framework else ""

        # Create cube
        cube_name = table.table_id
        cube = CUBE()
        cube.cube_id = cube_name
        cube.name = table.name or cube_name
        cube.description = table.description
        cube.cube_type = 'RC'  # Report Cube
        cube.framework_id = framework_object

        # Create cube structure
        cube_structure = CUBE_STRUCTURE()
        cube_structure.cube_structure_id = table.table_id
        cube_structure.name = f"{cube.name} Structure"
        cube_structure.description = f"Structure for {cube.name}"

        # Link cube to structure
        cube.cube_structure_id = cube_structure

        # Add to bulk creation lists
        self.cube_structures_to_create.append(cube_structure)
        self.cubes_to_create.append(cube)

        return cube, cube_structure

    def create_combination_and_items_from_rendering(self, cells, cell_positions, ordinate_items, cube):
        """
        Create COMBINATION and COMBINATION_ITEMs for each cell.
        Each cell's related ordinates generate a combination.
        """
        # Create a mapping of axis_ordinate to ordinate_items for efficiency
        ordinate_to_items = {}
        for item in ordinate_items:
            if item.axis_ordinate_id_id not in ordinate_to_items:
                ordinate_to_items[item.axis_ordinate_id_id] = []
            ordinate_to_items[item.axis_ordinate_id_id].append(item)

        # Create a mapping of cell to its positions
        cell_to_positions = {}
        for position in cell_positions:
            if position.cell_id_id not in cell_to_positions:
                cell_to_positions[position.cell_id_id] = []
            cell_to_positions[position.cell_id_id].append(position)

        # Process each cell
        for cell in cells:
            # Generate a new unique combination ID
            self.combination_counter += 1
            combination_id = f"{cube.cube_id}_COMB_{self.timestamp}_{self.combination_counter:04d}"

            # Create combination for this cell
            combination = COMBINATION()
            combination.combination_id = combination_id
            combination.name = f"Combination for {cell.cell_id}"

            # Update the cell with the new combination_id
            cell.table_cell_combination_id = combination
            self.cells_to_update.append(cell)

            # Get all positions for this cell
            positions = cell_to_positions.get(cell.cell_id, [])

            # Collect all ordinate items for this cell
            cell_ordinate_items = []
            for position in positions:
                if position.axis_ordinate_id_id in ordinate_to_items:
                    cell_ordinate_items.extend(
                        ordinate_to_items[position.axis_ordinate_id_id]
                    )

            # Find the metric (variable without member)
            metric = None
            for item in cell_ordinate_items:
                if item.variable_id and not item.member_id:
                    metric = item.variable_id
                    break

            combination.metric = metric
            self.combinations_to_create.append(combination)

            # Create combination items for each ordinate item
            for item in cell_ordinate_items:
                if item.variable_id:  # Only process items with variables
                    combination_item = COMBINATION_ITEM()
                    combination_item.combination_id = combination
                    combination_item.variable_id = item.variable_id
                    combination_item.member_id = item.member_id
                    self.combination_items_to_create.append(combination_item)

            # Create cube to combination link
            cube_to_combination = CUBE_TO_COMBINATION()
            cube_to_combination.cube_id = cube
            cube_to_combination.combination_id = combination
            self.cube_to_combinations_to_create.append(cube_to_combination)

    def create_cube_structure_items_from_rendering(self, ordinate_items, cube_structure):
        """
        Create CUBE_STRUCTURE_ITEMs from ordinate items.
        The sum of variables in ordinate items should be in cube structure items.
        The set of members should be in the subdomain.
        """
        # Collect unique variables and their associated members
        variable_to_members = {}

        for item in ordinate_items:
            if item.variable_id:
                var_id = item.variable_id.variable_id
                if var_id not in variable_to_members:
                    variable_to_members[var_id] = {
                        'variable': item.variable_id,
                        'members': set(),
                        'hierarchies': set()
                    }

                if item.member_id:
                    variable_to_members[var_id]['members'].add(item.member_id)

        # Create cube structure item for each unique variable
        for var_id, var_data in variable_to_members.items():
            csi = CUBE_STRUCTURE_ITEM()
            csi.cube_structure_id = cube_structure
            csi.variable_id = var_data['variable']
            csi.cube_variable_code = f"{cube_structure.cube_structure_id}__{var_id}"

            # Create subdomain from members if any exist
            if var_data['members']:
                subdomain = self._get_or_create_subdomain(
                    var_data['variable'],
                    var_data['members'],
                    cube_structure
                )
                csi.subdomain_id = subdomain
                csi.description = f"Variable with {len(var_data['members'])} members in subdomain"
            else:
                csi.description = f"Variable without member restrictions"

            self.cube_structure_items_to_create.append(csi)

    def _get_or_create_subdomain(self, variable, members, cube_structure):
        """
        Get or create a subdomain for the given variable and members.
        """
        # Create a unique subdomain ID based on variable and cube structure
        subdomain_id = f"{variable.variable_id}_OUTPUT_SD_{cube_structure.cube_structure_id}"

        # Check if subdomain already exists in our creation list
        existing_subdomain = next(
            (sd for sd in self.subdomains_to_create if sd.subdomain_id == subdomain_id),
            None
        )

        if existing_subdomain:
            return existing_subdomain

        # Check if subdomain exists in database
        try:
            existing_subdomain = SUBDOMAIN.objects.get(subdomain_id=subdomain_id)
            return existing_subdomain
        except SUBDOMAIN.DoesNotExist:
            pass

        # Create new subdomain
        subdomain = SUBDOMAIN()
        subdomain.subdomain_id = subdomain_id
        subdomain.name = f"Output subdomain for {variable.name or variable.variable_id}"
        subdomain.code = subdomain_id
        subdomain.is_listed = True
        subdomain.description = f"Generated subdomain for output layer"

        # Get the variable's domain if it exists
        if hasattr(variable, 'domain_id') and variable.domain_id:
            subdomain.domain_id = variable.domain_id

        self.subdomains_to_create.append(subdomain)

        # Create subdomain enumeration entries for each member
        order = 0
        for member in members:
            order += 1
            enum_entry = SUBDOMAIN_ENUMERATION()
            enum_entry.subdomain_id = subdomain
            enum_entry.member_id = member
            enum_entry.order = order
            self.subdomain_enumerations_to_create.append(enum_entry)

        return subdomain

    def _bulk_save_objects(self):
        """
        Bulk save all created objects to the database.
        """

        if self.cube_structures_to_create:
            CUBE_STRUCTURE.objects.bulk_create(
                self.cube_structures_to_create, batch_size=1000, ignore_conflicts=True
            )

        if self.cubes_to_create:
            CUBE.objects.bulk_create(
                self.cubes_to_create, batch_size=1000, ignore_conflicts=True
            )

        if self.combinations_to_create:
            COMBINATION.objects.bulk_create(
                self.combinations_to_create, batch_size=5000
            )

        # Save subdomains first as they are referenced by other objects
        if self.subdomains_to_create:
            SUBDOMAIN.objects.bulk_create(
                self.subdomains_to_create, batch_size=1000
            )

        if self.subdomain_enumerations_to_create:
            SUBDOMAIN_ENUMERATION.objects.bulk_create(
                self.subdomain_enumerations_to_create, batch_size=5000
            )

        if self.combination_items_to_create:
            COMBINATION_ITEM.objects.bulk_create(
                self.combination_items_to_create, batch_size=5000
            )

        if self.cube_structure_items_to_create:
            CUBE_STRUCTURE_ITEM.objects.bulk_create(
                self.cube_structure_items_to_create, batch_size=5000
            )

        if self.cube_to_combinations_to_create:
            CUBE_TO_COMBINATION.objects.bulk_create(
                self.cube_to_combinations_to_create, batch_size=5000
            )

        # Update cells with new combination IDs
        if self.cells_to_update:
            TABLE_CELL.objects.bulk_update(
                self.cells_to_update, ['table_cell_combination_id'], batch_size=5000
            )

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
            framework_object = self.get_framework_object(framework)
            if not framework_object:
                results['status'] = 'error'
                results['message'] = f"Framework '{framework}' not found in database"
                return results

            # Get tables for this framework version
            tables = self.get_tables_by_framework_version(framework, version)

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
            framework_object = self.get_framework_object(framework)
            if not framework_object:
                results['status'] = 'error'
                results['message'] = f"Framework '{framework}' not found in database"
                return results

            # Get all tables for this framework
            tables = self.get_tables_by_framework(framework)

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
                        'version': self.extract_version_from_table_id(table.table_id)
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
            table = self.get_table_by_code_version(table_code, version)

            if not table:
                results['status'] = 'error'
                results['error'] = f"Table with code '{table_code}' and version '{version}' not found"
                return results

            # Extract framework from table ID
            framework_id = self.extract_framework_from_table_id(table.table_id)
            framework_object = self.get_framework_object(framework_id) if framework_id else None

            if not framework_object:
                results['status'] = 'warning'
                results['error'] = f"Framework not found for table '{table.table_id}', proceeding without framework"

            # Process the table
            cube, cube_structure = self.create_output_layers(table, framework_object, save_to_db)

            results['processed'] = {
                'table_id': table.table_id,
                'table_name': table.name,
                'cube_id': cube.cube_id,
                'cube_structure_id': cube_structure.cube_structure_id,
                'framework': framework_id
            }

        except Exception as e:
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
            tables = TABLE.objects.filter(
                Q(table_id__endswith=f"_{table_code}") |
                Q(name=table_code)
            )

            if not tables:
                results['status'] = 'error'
                results['message'] = f"No tables found with code '{table_code}'"
                return results

            # Process each table
            for table in tables:
                try:
                    # Extract framework from table ID
                    framework_id = self.extract_framework_from_table_id(table.table_id)
                    framework_object = self.get_framework_object(framework_id) if framework_id else None

                    if not framework_object:
                        results['errors'].append({
                            'table_id': table.table_id,
                            'error': f"Framework not found for table, skipping"
                        })
                        continue

                    cube, cube_structure = self.create_output_layers(table, framework_object, save_to_db)

                    results['processed'].append({
                        'table_id': table.table_id,
                        'table_name': table.name,
                        'cube_id': cube.cube_id,
                        'cube_structure_id': cube_structure.cube_structure_id,
                        'framework': framework_id,
                        'version': self.extract_version_from_table_id(table.table_id)
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
            results['message'] = f"Error processing table code: {str(e)}"

        return results
