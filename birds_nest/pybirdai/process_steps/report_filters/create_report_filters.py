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
from pybirdai.process_steps.utils import Utils
from pybirdai.models.bird_meta_data_model import *
from pybirdai.process_steps.website_to_sddmodel.constants import BULK_CREATE_BATCH_SIZE_DEFAULT
import os
import csv
from uuid import uuid4

class CreateReportFilters:
    @staticmethod
    def _determine_role(variable):
        """
        Determine role (D/O/A) from variable domain characteristics.

        Returns:
            str: 'D' (dimension), 'O' (observation), or 'A' (attribute)
        """
        if not hasattr(variable, 'domain_id') or not variable.domain_id:
            return "A"  # Default to attribute if no domain

        domain = variable.domain_id
        if hasattr(domain, 'is_enumerated') and domain.is_enumerated:
            return "D"  # Dimension
        elif domain.domain_id in ("Integer", "Float", "MNTRY"):
            return "O"  # Observation
        else:
            return "A"  # Attribute

    def create_report_filters(self, context, sdd_context, framework, version):
        """
        Create report filters based on the given context, SDD context, framework, and version.

        Args:
            context: The context object containing file directory information.
            sdd_context: The SDD context object containing various dictionaries and mappings.
            framework: The framework being used.
            version: The version of the framework.
        """
        file_location = os.path.join(context.file_directory, "joins_configuration", f"in_scope_reports_{framework}.csv")
        in_scope_reports = CreateReportFilters.read_in_scope_reports(file_location)

        cell_to_variable_member_tuple_map = CreateReportFilters.create_cell_to_variable_member_map(sdd_context)

        # Add lists to collect objects for bulk creation
        self.combinations_to_create = []
        self.combination_items_to_create = []
        self.cube_structure_items_to_create = []
        self.cube_to_combinations_to_create = []

        for cell_id, tuples in cell_to_variable_member_tuple_map.items():
            CreateReportFilters.process_cell(self,cell_id, tuples, sdd_context, context, framework, version)

        # Bulk create all collected objects at the end
        if context.save_derived_sdd_items:
            # Delete existing records for idempotent re-runs
            if self.combinations_to_create:
                combination_ids = [c.combination_id for c in self.combinations_to_create]
                COMBINATION_ITEM.objects.filter(combination_id__combination_id__in=combination_ids).delete()
                COMBINATION.objects.filter(combination_id__in=combination_ids).delete()

            if self.cube_structure_items_to_create:
                cube_variable_codes = [csi.cube_variable_code for csi in self.cube_structure_items_to_create]
                CUBE_STRUCTURE_ITEM.objects.filter(cube_variable_code__in=cube_variable_codes).delete()

            COMBINATION.objects.bulk_create(self.combinations_to_create, batch_size=BULK_CREATE_BATCH_SIZE_DEFAULT, ignore_conflicts=True)
            COMBINATION_ITEM.objects.bulk_create(self.combination_items_to_create, batch_size=BULK_CREATE_BATCH_SIZE_DEFAULT, ignore_conflicts=True)
            CUBE_STRUCTURE_ITEM.objects.bulk_create(self.cube_structure_items_to_create, batch_size=BULK_CREATE_BATCH_SIZE_DEFAULT, ignore_conflicts=True)

            # Reload CSI objects from database to get their assigned PKs
            # (ignore_conflicts=True prevents in-memory PK population)
            if self.cube_structure_items_to_create:
                cube_variable_codes = [csi.cube_variable_code for csi in self.cube_structure_items_to_create]
                saved_csis = {
                    csi.cube_variable_code: csi
                    for csi in CUBE_STRUCTURE_ITEM.objects.filter(cube_variable_code__in=cube_variable_codes)
                }

                # Update sdd_context dictionary with saved CSI objects that have PKs
                for cube_structure_id, csi_list in sdd_context.bird_cube_structure_item_dictionary.items():
                    for i, csi in enumerate(csi_list):
                        if csi.cube_variable_code in saved_csis:
                            csi_list[i] = saved_csis[csi.cube_variable_code]

            CUBE_TO_COMBINATION.objects.bulk_create(self.cube_to_combinations_to_create, batch_size=BULK_CREATE_BATCH_SIZE_DEFAULT, ignore_conflicts=True)

    def read_in_scope_reports(file_location):
        """
        Read in-scope reports from a CSV file.

        Args:
            file_location (str): The path to the CSV file.

        Returns:
            list: A list of report templates from the CSV file.
        """
        with open(file_location, encoding='utf-8') as csvfile:
            return [row[0] for row in csv.reader(csvfile) if row and row[0] != "report_template"]

    def create_cell_to_variable_member_map(sdd_context):
        """
        Create a mapping of cell IDs to variable-member tuples.

        Args:
            sdd_context: The SDD context object containing various dictionaries and mappings.

        Returns:
            dict: A dictionary mapping cell IDs to lists of variable-member tuples.
        """

        cell_positions_dict = sdd_context.cell_positions_dictionary
        table_cell_dict = sdd_context.table_cell_dictionary
        axis_ordinate_map = sdd_context.axis_ordinate_to_ordinate_items_map

        # Pre-build a simplified axis_ordinate lookup map to avoid FK descriptor overhead
        # This converts ordinate items to simple tuples upfront
        if not hasattr(sdd_context, '_axis_ordinate_tuples_cache'):
            sdd_context._axis_ordinate_tuples_cache = {}
            for axis_ordinate_id, ordinate_items in axis_ordinate_map.items():
                if ordinate_items:
                    # Extract FK values once and store as tuples
                    sdd_context._axis_ordinate_tuples_cache[axis_ordinate_id] = [
                        (item.variable_id, item.member_id) for item in ordinate_items
                    ]

        # PRE-EXTRACT all cell.table_id FK values to avoid repeated descriptor calls
        if not hasattr(sdd_context, '_cell_table_id_cache'):
            sdd_context._cell_table_id_cache = {}
            for cell_id, cell in table_cell_dict.items():
                if cell and cell.table_id:
                    sdd_context._cell_table_id_cache[cell_id] = True

        # PRE-EXTRACT all cell_position.axis_ordinate_id FK values before processing
        if not hasattr(sdd_context, '_cell_position_axis_ordinate_cache'):
            sdd_context._cell_position_axis_ordinate_cache = {}
            for cell_id, cell_positions in cell_positions_dict.items():
                axis_ordinate_ids = []
                for cell_position in cell_positions:
                    # Extract FK value once per cell_position
                    axis_ordinate_id = cell_position.axis_ordinate_id.axis_ordinate_id
                    axis_ordinate_ids.append(axis_ordinate_id)
                sdd_context._cell_position_axis_ordinate_cache[cell_id] = axis_ordinate_ids

        # Initialize with expected size
        cell_to_variable_member_tuple_map = {}

        for cell_id in cell_positions_dict.keys():
            # Use pre-extracted table_id check (zero FK calls!)
            if cell_id not in sdd_context._cell_table_id_cache:
                continue

            tuples = []
            # Use pre-extracted axis_ordinate_id strings (zero FK calls!)
            for axis_ordinate_id in sdd_context._cell_position_axis_ordinate_cache.get(cell_id, []):
                ordinate_tuples = sdd_context._axis_ordinate_tuples_cache.get(axis_ordinate_id, [])
                tuples.extend(ordinate_tuples)

            if tuples:
                cell_to_variable_member_tuple_map[cell_id] = tuples

        return cell_to_variable_member_tuple_map

    def process_cell(self,cell_id, tuples, sdd_context, context, framework, version):
        """
        Process a single cell, creating combinations and filters.

        Args:
            cell_id: The ID of the cell being processed.
            tuples: A list of variable-member tuples associated with the cell.
            sdd_context: The SDD context object.
            context: The context object.
            framework: The framework being used.
            version: The version of the framework.
        """

        cell = sdd_context.table_cell_dictionary.get(cell_id)

        if not cell or not cell.table_id:
            return

        # PRE-EXTRACT table_id FK to avoid 4 repeated descriptor calls (29,121 times!)
        table_id_str = cell.table_id.table_id

        cube_mapping_id = CreateReportFilters.get_report_cube_mapping_id_for_table_id(table_id_str, framework)
        relevant_mappings = sdd_context.mapping_to_cube_dictionary.get(cube_mapping_id, [])

        report_rol_cube = CreateReportFilters.get_rol_cube_for_table_id(Utils.make_valid_id(table_id_str), sdd_context, framework, version)
        if not report_rol_cube:
            pass
            return
        combination_id = cell.table_cell_combination_id
        if combination_id and not(combination_id == ''):
            CreateReportFilters.create_combination_and_filters(self,combination_id, tuples, relevant_mappings, report_rol_cube, sdd_context, context)

    def create_combination_and_filters(self,table_cell_combination_id, tuples, relevant_mappings, report_rol_cube, sdd_context, context):
        """
        Create a combination and associated filters for a given cell.

        Args:
            cell_id: The ID of the cell.
            tuples: A list of variable-member tuples.
            relevant_mappings: List of relevant mappings.
            report_rol_cube: The ROL cube for the report.
            sdd_context: The SDD context object.
            context: The context object.
        """


        qualified_combination_id = report_rol_cube.cube_id + "_" + table_cell_combination_id
        report_cell = COMBINATION(combination_id=qualified_combination_id)
        metric = CreateReportFilters.get_metric(sdd_context, tuples, relevant_mappings)
        if metric:
            CreateReportFilters.add_variable_to_rol_cube(self,context, sdd_context, report_rol_cube, metric)
        report_cell.metric = metric
        if not(qualified_combination_id in sdd_context.combination_dictionary.keys()):
            sdd_context.combination_dictionary[qualified_combination_id] = report_cell
            if context.save_derived_sdd_items:
                self.combinations_to_create.append(report_cell)  # Changed from save() to append

        CreateReportFilters.create_cube_to_combination(self,report_cell, report_rol_cube, sdd_context, context)
        CreateReportFilters.create_filters(self, report_cell, tuples, relevant_mappings, report_rol_cube, sdd_context, context)

    def add_variable_to_rol_cube(self,context, sdd_context, report_rol_cube, metric):
        """
        Add a variable to the ROL cube if it doesn't already exist.

        Args:
            sdd_context: The SDD context object.
            report_rol_cube: The ROL cube for the report.
            metric: The metric (variable) to be added.
        """
        # PRE-EXTRACT FK values to avoid repeated descriptor calls
        cube_structure_obj = report_rol_cube.cube_structure_id
        cube_structure_id_str = cube_structure_obj.cube_structure_id

        # Cache variable IDs per cube structure to avoid repeated loop lookups
        if not hasattr(sdd_context, '_cube_variable_cache'):
            sdd_context._cube_variable_cache = {}

        if cube_structure_id_str not in sdd_context._cube_variable_cache:
            csis = sdd_context.bird_cube_structure_item_dictionary.get(cube_structure_id_str, [])
            sdd_context._cube_variable_cache[cube_structure_id_str] = {
                csi.variable_id.variable_id for csi in csis
            }

        variable_already_exists = metric.variable_id in sdd_context._cube_variable_cache[cube_structure_id_str]

        if not variable_already_exists:
            # Add to cache immediately to prevent duplicate additions
            sdd_context._cube_variable_cache[cube_structure_id_str].add(metric.variable_id)
            csi = CUBE_STRUCTURE_ITEM()
            csi.cube_structure_id = cube_structure_obj
            csi.variable_id = metric
            csi.cube_variable_code = cube_structure_id_str + "__" + metric.variable_id
            csi.role = CreateReportFilters._determine_role(metric)
            if context.save_derived_sdd_items:
                self.cube_structure_items_to_create.append(csi)  # Changed from save() to append
            sdd_context.bird_cube_structure_item_dictionary.setdefault(
                cube_structure_id_str, []
            ).append(csi)

    def get_metric( sdd_context, tuples, relevant_mappings):
        """
        Get the metric (variable) based on the given tuples and mappings.

        Args:
            sdd_context: The SDD context object.
            tuples: A list of variable-member tuples.
            relevant_mappings: List of relevant mappings.

        Returns:
            The metric (variable) or None if not found.
        """
        var_mapping_dict = sdd_context.variable_mapping_item_dictionary

        # Initialize EBA→DPM variable ID cache if it doesn't exist
        if not hasattr(sdd_context, '_eba_to_dpm_cache'):
            sdd_context._eba_to_dpm_cache = {}

        for var_id, member_id in tuples:
            if member_id is None:
                if var_id is not None:
                    var_id_str = var_id.variable_id

                    # Check cache first
                    if var_id_str not in sdd_context._eba_to_dpm_cache:
                        dpm_var_id = var_id_str.replace('EBA_', 'DPM_')
                        sdd_context._eba_to_dpm_cache[var_id_str] = dpm_var_id
                    else:
                        dpm_var_id = sdd_context._eba_to_dpm_cache[var_id_str]

                    try:
                        variable_mapping_items = var_mapping_dict[dpm_var_id]
                        # Use next() with generator expression for early exit
                        return next((item.variable_id for item in variable_mapping_items
                                if ((item.is_source == 'false') or (item.is_source == 'False'))), None)
                    except KeyError:
                        print(f"Could not find variable mapping for {var_id_str}")
        return None

    def create_filters( self, report_cell, tuples, relevant_mappings, report_rol_cube, sdd_context, context):
        """
        Create filters for a given report cell.

        Args:
            report_cell: The report cell object.
            tuples: A list of variable-member tuples.
            relevant_mappings: List of relevant mappings.
            report_rol_cube: The ROL cube for the report.
            sdd_context: The SDD context object.
            context: The context object.
        """
        ref_tuple_list = CreateReportFilters.get_reference_tuple_list(sdd_context, tuples, relevant_mappings)
        if ref_tuple_list:
            for ref_tuple_in in ref_tuple_list:
                ref_variable, ref_member, ref_member_hierarchy = ref_tuple_in

                the_filter = COMBINATION_ITEM()
                the_filter.combination_id = report_cell
                the_filter.variable_id = ref_variable
                the_filter.member_id = ref_member
                the_filter.member_hierarchy = ref_member_hierarchy

                # Add variable to cube if present (avoids FK descriptor call by passing object directly)
                if ref_variable:
                    CreateReportFilters.add_variable_to_rol_cube(self,context, sdd_context, report_rol_cube, ref_variable)

                sdd_context.combination_item_dictionary.setdefault(report_cell.combination_id, []).append(the_filter)
                if context.save_derived_sdd_items:
                    self.combination_items_to_create.append(the_filter)  # Changed from save() to append

    def get_reference_tuple_list(sdd_context, non_ref_tuple_list, relevant_mappings):
        """
        Get a list of reference tuples based on non-reference tuples and relevant mappings.

        Args:
            sdd_context: The SDD context object.
            non_ref_tuple_list: List of non-reference tuples.
            relevant_mappings: List of relevant mappings.

        Returns:
            list: A list of reference tuples.
        """
        ref_tuple_list = []
        non_ref_set = set(non_ref_tuple_list)  # Convert to set for O(1) lookups

        # Initialize cache if it doesn't exist
        if not hasattr(sdd_context, '_member_mapping_item_row_dict_cache'):
            sdd_context._member_mapping_item_row_dict_cache = {}

        for mapping in relevant_mappings:
            member_mapping = mapping.mapping_id.member_mapping_id
            if not member_mapping:
                continue

            # Check cache first to avoid rebuilding the same dictionary repeatedly
            member_mapping_id = member_mapping.member_mapping_id
            if member_mapping_id not in sdd_context._member_mapping_item_row_dict_cache:
                sdd_context._member_mapping_item_row_dict_cache[member_mapping_id] = \
                    CreateReportFilters.create_member_mapping_item_row_dict(sdd_context, member_mapping)

            # Unpack pre-partitioned source and target dictionaries
            source_items_by_row, target_items_by_row = sdd_context._member_mapping_item_row_dict_cache[member_mapping_id]

            # Iterate over rows - source and target items are already separated!
            for row in source_items_by_row.keys():
                source_items = source_items_by_row[row]
                target_items = target_items_by_row.get(row, [])

                # Check if all source items are in non_ref_tuple_list
                # No more conditional logic inside loop - items are pre-partitioned!
                if all(item in non_ref_set for item in source_items):
                    ref_tuple_list.extend(target_items)

        return ref_tuple_list

    def create_member_mapping_item_row_dict(sdd_context, member_mapping):
        """
        Create dictionaries of member mapping items grouped by row, pre-partitioned by source/target.
        Pre-extracts ALL FK values to avoid millions of descriptor calls later.

        Args:
            sdd_context: The SDD context object.
            member_mapping: The member mapping object.

        Returns:
            tuple: (source_items_by_row, target_items_by_row) dictionaries
        """
        source_items_by_row = {}
        target_items_by_row = {}
        member_mapping_items = sdd_context.member_mapping_items_dictionary[member_mapping.member_mapping_id]

        for member_mapping_item in member_mapping_items:
            # PRE-EXTRACT all FK values once to avoid ~3M descriptor calls later
            is_source_bool = member_mapping_item.is_source.lower() == 'true'
            variable_id = member_mapping_item.variable_id
            member_id = member_mapping_item.member_id
            member_hierarchy = member_mapping_item.member_hierarchy
            row = member_mapping_item.member_mapping_row

            # PRE-PARTITION into source and target dictionaries
            if is_source_bool:
                source_items_by_row.setdefault(row, []).append((variable_id, member_id))
            else:
                target_items_by_row.setdefault(row, []).append((variable_id, member_id, member_hierarchy))

        return (source_items_by_row, target_items_by_row)

    def create_variable_mapping_row_dict(sdd_context, variable_mapping):
        """
        Create a dictionary of variable mapping items grouped by row.

        Args:
            sdd_context: The SDD context object.
            variable_mapping: The variable mapping object.

        Returns:
            dict: A dictionary of variable mapping items grouped by row.
        """
        variable_mapping_item_row_dict = {}
        variable_mapping_items = sdd_context.variable_mapping_item_dictionary[variable_mapping.variable_mapping_id]

        for variable_mapping_item in variable_mapping_items:
            variable_mapping_item_row_dict.setdefault(0, []).append(variable_mapping_item)

        return variable_mapping_item_row_dict

    def get_report_cube_mapping_id_for_table_id( table_id, framework):
        """
        Get the report cube mapping ID for a given table ID and framework.

        Args:
            table_id (str): The ID of the table.
            framework (str): The framework being used.

        Returns:
            str: The report cube mapping ID.
        """
        return 'M_' + table_id.replace(framework + '_', '')

    def get_rol_cube_for_table_id( table_id, sdd_context, framework, version):
        """
        Get the ROL cube for a given table ID.

        Args:
            table_id (str): The ID of the table.
            sdd_context: The SDD context object.
            framework (str): The framework being used.
            version: The version of the framework.

        Returns:
            The ROL cube object or None if not found.
        """
        try:
            key = table_id[11:len(table_id)]
            return sdd_context.bird_cube_dictionary[key]
        except KeyError:
            return None

    def create_cube_to_combination(self,report_cell, report_rol_cube, sdd_context, context):
        """
        Create a cube-to-combination mapping.

        Args:
            report_cell: The report cell object.
            report_rol_cube: The ROL cube for the report.
            sdd_context: The SDD context object.
            context: The context object.
        """
        cube_to_comb = CUBE_TO_COMBINATION()
        cube_to_comb.combination_id = report_cell
        cube_to_comb.cube_id = report_rol_cube
        sdd_context.combination_to_rol_cube_map.setdefault(report_rol_cube.cube_id, []).append(cube_to_comb)
        if context.save_derived_sdd_items:
            self.cube_to_combinations_to_create.append(cube_to_comb)  # Changed from save() to append
