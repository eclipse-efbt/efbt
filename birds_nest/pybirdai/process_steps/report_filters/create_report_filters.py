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
#
from pybirdai.utils.utils import Utils
from pybirdai.bird_meta_data_model import *
import os
import csv
from uuid import uuid4

class CreateReportFilters:
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
            COMBINATION.objects.bulk_create(self.combinations_to_create, batch_size=5000, ignore_conflicts=True)
            COMBINATION_ITEM.objects.bulk_create(self.combination_items_to_create, batch_size=5000, ignore_conflicts=True)
            for item in self.cube_structure_items_to_create:
               item.save()
            #CUBE_STRUCTURE_ITEM.objects.bulk_create(self.cube_structure_items_to_create, batch_size=5000, ignore_conflicts=True)
            CUBE_TO_COMBINATION.objects.bulk_create(self.cube_to_combinations_to_create, batch_size=5000, ignore_conflicts=True)

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

        # Initialize with expected size
        cell_to_variable_member_tuple_map = {}

        for cell_id, cell_positions in cell_positions_dict.items():
            cell = table_cell_dict.get(cell_id)
            if not (cell and cell.table_id):
                continue

            tuples = []
            for cell_position in cell_positions:
                axis_ordinate = cell_position.axis_ordinate_id
                ordinate_items = axis_ordinate_map.get(axis_ordinate.axis_ordinate_id, [])

                if len(ordinate_items) > 0:

                    tuples.extend((item.variable_id, item.member_id) for item in ordinate_items)

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

        cube_mapping_id = CreateReportFilters.get_report_cube_mapping_id_for_table_id(cell.table_id.table_id, framework)
        relevant_mappings = sdd_context.mapping_to_cube_dictionary.get(cube_mapping_id, [])

        report_rol_cube = CreateReportFilters.get_rol_cube_for_table_id(Utils.make_valid_id(cell.table_id.table_id), sdd_context, framework, version)
        if not report_rol_cube:
            #print(f"Could not find report for {cell.table_id.table_id}")
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
        variable_already_exists_in_cube = False
        csis = sdd_context.bird_cube_structure_item_dictionary.get(
            report_rol_cube.cube_structure_id.cube_structure_id, []
        )
        for csi in csis:
            if csi.variable_id.variable_id == metric.variable_id:
                variable_already_exists_in_cube = True

        if not variable_already_exists_in_cube:
            csi = CUBE_STRUCTURE_ITEM()
            csi.cube_structure_id = report_rol_cube.cube_structure_id
            csi.variable_id = metric
            csi.cube_variable_code = csi.cube_structure_id.cube_structure_id + "__" + metric.variable_id
            if context.save_derived_sdd_items:
                self.cube_structure_items_to_create.append(csi)  # Changed from save() to append
            sdd_context.bird_cube_structure_item_dictionary.setdefault(
                report_rol_cube.cube_structure_id.cube_structure_id, []
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

        for var_id, member_id in tuples:
            if member_id is None:
                try:
                    dpm_var_id = var_id.variable_id.replace('EBA_', 'DPM_')
                    variable_mapping_items = var_mapping_dict[dpm_var_id]
                    # Use next() with generator expression for early exit
                    return next((item.variable_id for item in variable_mapping_items 

                               if ((item.is_source == 'false') or (item.is_source == 'False'))), None)
                except KeyError:
                    print(f"Could not find variable mapping for {var_id.variable_id}")
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
                var_string = 'None'
                if ref_variable:
                    var_string = ref_variable.variable_id
                    CreateReportFilters.add_variable_to_rol_cube(self,context, sdd_context, report_rol_cube, ref_variable)

                member_string = 'None'
                if ref_member:
                    member_string = ref_member.member_id

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

        for mapping in relevant_mappings:
            member_mapping = mapping.mapping_id.member_mapping_id
            if not member_mapping:
                continue

            member_mapping_item_row_dict = CreateReportFilters.create_member_mapping_item_row_dict(
                sdd_context, member_mapping)

            for member_mapping_items in member_mapping_item_row_dict.values():
                # Group items by is_source for faster processing

                source_items = [(item.variable_id, item.member_id) for item in member_mapping_items 
                              if ((item.is_source.lower() == 'true') )]
                
                # Check if all source items are in non_ref_tuple_list
                if all(item in non_ref_set for item in source_items):
                    ref_tuple_list.extend(
                        (item.variable_id, item.member_id, item.member_hierarchy)
                        for item in member_mapping_items
                        if ((item.is_source.lower() != 'true'))
                    )

        return ref_tuple_list

    def create_member_mapping_item_row_dict(sdd_context, member_mapping):
        """
        Create a dictionary of member mapping items grouped by row.

        Args:
            sdd_context: The SDD context object.
            member_mapping: The member mapping object.

        Returns:
            dict: A dictionary of member mapping items grouped by row.
        """
        member_mapping_item_row_dict = {}
        member_mapping_items = sdd_context.member_mapping_items_dictionary[member_mapping.member_mapping_id]

        for member_mapping_item in member_mapping_items:
            member_mapping_item_row_dict.setdefault(member_mapping_item.member_mapping_row, []).append(member_mapping_item)

        return member_mapping_item_row_dict

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
