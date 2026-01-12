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
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import logging
class ImportDatabaseToSDDModel:
    '''
    Class responsible for the import of  SDD csv files
    into an instance of the analaysis model
    '''
    def import_sdd(self, sdd_context):
        '''
        Import SDD csv files into an instance of the analysis model, using parallel execution
        where possible for better performance.
        '''
        #print the current time
        print("Starting import at:")
        print(datetime.now())
        # Basic setup - these need to run sequentially as later steps depend on them
        ImportDatabaseToSDDModel.create_maintenance_agencies(self, sdd_context)
        ImportDatabaseToSDDModel.create_frameworks(self, sdd_context)
        ImportDatabaseToSDDModel.create_all_domains(self, sdd_context)

        # Group 1 - Independent base entities
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [
                executor.submit(ImportDatabaseToSDDModel.create_all_members, self, sdd_context),
                executor.submit(ImportDatabaseToSDDModel.create_all_variables, self, sdd_context),
                executor.submit(ImportDatabaseToSDDModel.create_all_rol_cube_structures, self, sdd_context),
                executor.submit(ImportDatabaseToSDDModel.create_all_rol_cubes, self, sdd_context)
            ]
            # Wait for all tasks to complete
            for future in futures:
                future.result()

        # Group 2 - Dependent on base entities but independent of each other
        with ThreadPoolExecutor(max_workers=6) as executor:
            futures = [
                executor.submit(ImportDatabaseToSDDModel.create_all_rol_cube_structure_items, self, sdd_context),
                executor.submit(ImportDatabaseToSDDModel.create_all_nonref_member_hierarchies, self, sdd_context),
                executor.submit(ImportDatabaseToSDDModel.create_member_mappings, self, sdd_context),
                executor.submit(ImportDatabaseToSDDModel.create_all_mapping_definitions, self, sdd_context),
                executor.submit(ImportDatabaseToSDDModel.create_all_variable_mappings, self, sdd_context),
                executor.submit(ImportDatabaseToSDDModel.create_combinations, self, sdd_context)
            ]
            for future in futures:
                future.result()

        # Group 3 - Dependent on previous groups but independent of each other
        with ThreadPoolExecutor(max_workers=6) as executor:
            futures = [
                executor.submit(ImportDatabaseToSDDModel.create_all_nonref_member_hierarchies_nodes, self, sdd_context),
                executor.submit(ImportDatabaseToSDDModel.create_all_member_mapping_items, self, sdd_context),
                executor.submit(ImportDatabaseToSDDModel.create_all_mapping_to_cubes, self, sdd_context),
                executor.submit(ImportDatabaseToSDDModel.create_all_variable_mapping_items, self, sdd_context),
                executor.submit(ImportDatabaseToSDDModel.create_combination_items, self, sdd_context),
                executor.submit(ImportDatabaseToSDDModel.create_cube_to_combination, self, sdd_context)
            ]
            for future in futures:
                future.result()

        # Group 4 - Report-related items that can run in parallel
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [
                executor.submit(ImportDatabaseToSDDModel.create_report_tables, self, sdd_context),
                executor.submit(ImportDatabaseToSDDModel.create_axis, self, sdd_context),
                executor.submit(ImportDatabaseToSDDModel.create_table_cells, self, sdd_context),
                executor.submit(ImportDatabaseToSDDModel.create_cube_links, self, sdd_context)
            ]
            for future in futures:
                future.result()

        # Group 5 - Final dependent items
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [
                executor.submit(ImportDatabaseToSDDModel.create_axis_ordinates, self, sdd_context),
                executor.submit(ImportDatabaseToSDDModel.create_ordinate_items, self, sdd_context),
                executor.submit(ImportDatabaseToSDDModel.create_cell_positions, self, sdd_context),
                executor.submit(ImportDatabaseToSDDModel.create_cube_structure_item_links, self, sdd_context)
            ]
            for future in futures:
                future.result()


            #print the current time
        print("Ending import at:")
        print(datetime.now())

    def import_sdd_for_filters(self, sdd_context, tables_to_import):
        '''
        Import only the necessary tables for filters from the database.
        This is a faster version of import_sdd that only imports tables needed for filters.
        Tables are imported in groups based on their dependencies.
        '''
        print("Starting selective import at:")
        print(datetime.now())

        # Group 1 - Base tables with no dependencies
        if 'MAINTENANCE_AGENCY' in tables_to_import:
            ImportDatabaseToSDDModel.create_maintenance_agencies(self, sdd_context)
        if 'DOMAIN' in tables_to_import:
            ImportDatabaseToSDDModel.create_all_domains(self, sdd_context)

        # Group 2 - Tables that depend only on DOMAIN and MAINTENANCE_AGENCY
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = []
            if 'MEMBER' in tables_to_import:
                futures.append(executor.submit(ImportDatabaseToSDDModel.create_all_members, self, sdd_context))
            if 'VARIABLE' in tables_to_import:
                futures.append(executor.submit(ImportDatabaseToSDDModel.create_all_variables, self, sdd_context))
            if 'MEMBER_HIERARCHY' in tables_to_import:
                futures.append(executor.submit(ImportDatabaseToSDDModel.create_all_nonref_member_hierarchies, self, sdd_context))
            if 'CUBE' in tables_to_import:
                futures.append(executor.submit(ImportDatabaseToSDDModel.create_all_rol_cubes, self, sdd_context))
            for future in futures:
                future.result()

        # Group 3 - Tables that depend on MEMBER and MEMBER_HIERARCHY
        if 'MEMBER_HIERARCHY_NODE' in tables_to_import:
            ImportDatabaseToSDDModel.create_all_nonref_member_hierarchies_nodes(self, sdd_context)

        # Group 4 - Tables that depend on VARIABLE and MAINTENANCE_AGENCY
        if 'COMBINATION' in tables_to_import:
            ImportDatabaseToSDDModel.create_combinations(self, sdd_context)

        # Group 5 - Tables that depend on COMBINATION
        if 'COMBINATION_ITEM' in tables_to_import:
            ImportDatabaseToSDDModel.create_combination_items(self, sdd_context)

        # Group 6 - Tables that depend on both CUBE and COMBINATION
        if 'CUBE_TO_COMBINATION' in tables_to_import:
            ImportDatabaseToSDDModel.create_cube_to_combination(self, sdd_context)

        print("Ending selective import at:")
        print(datetime.now())

    def import_sdd_for_joins(self, sdd_context, tables_to_import):
        '''
        Import only the necessary tables for joins from the database.
        This is a faster version of import_sdd that only imports tables needed for joins.
        Tables are imported in groups based on their dependencies.
        '''
        print("Starting selective import at:")
        print(datetime.now())

        # Group 1 - Base tables with no dependencies
        if 'MAINTENANCE_AGENCY' in tables_to_import:
            ImportDatabaseToSDDModel.create_maintenance_agencies(self, sdd_context)
        if 'DOMAIN' in tables_to_import:
            ImportDatabaseToSDDModel.create_all_domains(self, sdd_context)

        # Group 2 - Tables that depend only on DOMAIN and MAINTENANCE_AGENCY
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = []
            if 'VARIABLE' in tables_to_import:
                futures.append(executor.submit(ImportDatabaseToSDDModel.create_all_variables, self, sdd_context))
            if 'CUBE' in tables_to_import:
                futures.append(executor.submit(ImportDatabaseToSDDModel.create_all_rol_cubes, self, sdd_context))
            for future in futures:
                future.result()

        # Import CUBE_STRUCTURE (depends on CUBE)
        if 'CUBE_STRUCTURE' in tables_to_import:
            ImportDatabaseToSDDModel.create_all_rol_cube_structures(self, sdd_context)

        # Group 3 - Tables that depend on CUBE_STRUCTURE and VARIABLE
        if 'CUBE_STRUCTURE_ITEM' in tables_to_import:
            ImportDatabaseToSDDModel.create_all_rol_cube_structure_items(self, sdd_context)

        # Group 4 - Tables that depend on CUBE
        if 'CUBE_LINK' in tables_to_import:
            ImportDatabaseToSDDModel.create_cube_links(self, sdd_context)

        # Group 5 - Tables that depend on CUBE_LINK and CUBE_STRUCTURE_ITEM
        if 'CUBE_STRUCTURE_ITEM_LINK' in tables_to_import:
            ImportDatabaseToSDDModel.create_cube_structure_item_links(self, sdd_context)

        print("Ending selective import at:")
        print(datetime.now())

    def create_all_mapping_definitions(self, context):
        '''
        import all the mapping definitions
        '''
        context.mapping_definition_dictionary = {}
        for mapping_definition in MAPPING_DEFINITION.objects.all():
            context.mapping_definition_dictionary[
                mapping_definition.mapping_id] = mapping_definition

    def create_all_variable_mappings(self, context):
        '''
        import all the variable mappings
        '''
        context.variable_mapping_dictionary = {}
        for variable_mapping in VARIABLE_MAPPING.objects.all():
            context.variable_mapping_dictionary[
                variable_mapping.variable_mapping_id] = variable_mapping

    def create_all_variable_mapping_items(self, context):
        '''
        import all the variable mapping items
        '''
        context.variable_mapping_item_dictionary = {}
        for variable_mapping_item in VARIABLE_MAPPING_ITEM.objects.all().select_related('variable_mapping_id'):
            try:
                variable_mapping_list = context.variable_mapping_item_dictionary[
                    variable_mapping_item.variable_mapping_id.variable_mapping_id]
                variable_mapping_list.append(variable_mapping_item)
            except KeyError:
                context.variable_mapping_item_dictionary[
                    variable_mapping_item.variable_mapping_id.variable_mapping_id
                ] = [variable_mapping_item]

    def create_all_rol_cube_structures(self, context):
        '''
        import all the rol cube structures
        '''
        context.bird_cube_structure_dictionary = {}
        for rol_cube_structure in CUBE_STRUCTURE.objects.all():
            context.bird_cube_structure_dictionary[
                rol_cube_structure.cube_structure_id] = rol_cube_structure

    def create_all_rol_cubes(self, context, framework_id=None):
        '''
        import all the rol cubes, optionally filtered by framework
        '''
        context.bird_cube_dictionary = {}
        if framework_id:
            queryset = CUBE.objects.filter(framework_id=framework_id).select_related('cube_structure_id')
        else:
            queryset = CUBE.objects.all().select_related('cube_structure_id')
        for rol_cube in queryset:
            context.bird_cube_dictionary[rol_cube.cube_id] = rol_cube

    def create_all_rol_cube_structure_items(self, context):
        '''
        import all the rol cube structure items
        '''
        context.bird_cube_structure_item_dictionary = {}
        # Optimize with select_related to reduce database queries during joins metadata creation
        for rol_cube_structure_item in CUBE_STRUCTURE_ITEM.objects.all().select_related(
            'cube_structure_id',
            'variable_id',
            'variable_id__domain_id',
            'subdomain_id'
        ):
            try:
                context.bird_cube_structure_item_dictionary[
                    rol_cube_structure_item.cube_structure_id.cube_structure_id
                ].append(rol_cube_structure_item)
            except KeyError:
                context.bird_cube_structure_item_dictionary[
                    rol_cube_structure_item.cube_structure_id.cube_structure_id
                ] = [rol_cube_structure_item]
        logging.debug(f"create_rol_cube_structure_items_for_structures: Loaded {len(context.bird_cube_structure_item_dictionary)} structure item groups")

    def create_all_mapping_to_cubes(self, context):
        '''
        import all the mapping to cubes
        '''
        context.mapping_to_cube_dictionary = {}
        for mapping_to_cube in MAPPING_TO_CUBE.objects.all().select_related('mapping_id', 'mapping_id__member_mapping_id'):
            try:
                mapping_to_cube_list = context.mapping_to_cube_dictionary[
                    mapping_to_cube.cube_mapping_id]
                mapping_to_cube_list.append(mapping_to_cube)
            except KeyError:
                context.mapping_to_cube_dictionary[
                    mapping_to_cube.cube_mapping_id] = [mapping_to_cube]

    def create_maintenance_agencies(self, context):
        '''
        Import all maintenance agencies
        '''
        context.agency_dictionary = {}
        for agency in MAINTENANCE_AGENCY.objects.all():
            context.agency_dictionary[agency.maintenance_agency_id] = agency

    def create_frameworks(self, context):
        '''
        Import all frameworks
        '''
        context.framework_dictionary = {}
        for framework in FRAMEWORK.objects.all():
            context.framework_dictionary[framework.framework_id] = framework

    def create_all_domains(self, context):
        '''
        import all the domains
        '''
        context.domain_dictionary = {}
        for domain in DOMAIN.objects.all():
            context.domain_dictionary[domain.domain_id] = domain


    def create_all_members(self, context):
        '''
        Import all the members
        '''
        context.member_dictionary = {}
        context.member_id_to_domain_map = {}
        context.member_id_to_member_code_map = {}
        for member in MEMBER.objects.all().select_related('domain_id', 'maintenance_agency_id'):
            context.member_dictionary[member.member_id] = member
            context.member_id_to_domain_map[member] = member.domain_id
            context.member_id_to_member_code_map[member.member_id] = member.code

    def create_all_variables(self, context):
        '''
        import all the variables
        '''
        context.variable_dictionary = {}
        context.variable_to_domain_map = {}
        context.variable_to_long_names_map = {}
        context.variable_to_primary_concept_map = {}
        for variable in VARIABLE.objects.all().select_related('domain_id', 'maintenance_agency_id'):
            context.variable_dictionary[variable.variable_id] = variable
            context.variable_to_domain_map[variable.variable_id] = variable.domain_id
            context.variable_to_long_names_map[variable.variable_id] = variable.name
            context.variable_to_primary_concept_map[variable.variable_id] = variable.primary_concept

    def create_all_nonref_member_hierarchies(self, context):
        '''
        Import all non-reference member hierarchies
        '''
        context.member_hierarchy_dictionary = {}
        for hierarchy in MEMBER_HIERARCHY.objects.all():
            context.member_hierarchy_dictionary[
                hierarchy.member_hierarchy_id] = hierarchy


    def create_all_nonref_member_hierarchies_nodes(self, context):
        '''
        Import all non-reference member hierarchy nodes
        '''
        context.member_hierarchy_node_dictionary = {}
        for hierarchy_node in MEMBER_HIERARCHY_NODE.objects.all().select_related('member_id', 'member_hierarchy_id'):
            member = hierarchy_node.member_id
            member_name = 'None'
            if not(member is None):
                member_name = member.member_id
            context.member_hierarchy_node_dictionary[
                hierarchy_node.member_hierarchy_id.member_hierarchy_id + ":" + member_name
            ] = hierarchy_node

    def create_report_tables (self, context):
        '''
        import all the tables from the rendering package
        '''
        context.report_tables_dictionary = {}
        for table in TABLE.objects.all():
            context.report_tables_dictionary[table.table_id] = table

    def create_axis (self, context):
        '''
        import all the axes from the rendering package
        '''
        context.axis_dictionary = {}
        for axis in AXIS.objects.all():
            context.axis_dictionary[axis.axis_id] = axis

    def create_axis_ordinates(self, context):
        '''
        import all the axis_ordinate from the rendering package
        '''
        context.axis_ordinate_dictionary = {}
        for axis_ordinate in AXIS_ORDINATE.objects.all():
            context.axis_ordinate_dictionary[
                axis_ordinate.axis_ordinate_id] = axis_ordinate

    def create_ordinate_items(self, sdd_context):
        '''
        import all the axis_ordinate from the rendering package
        '''
        sdd_context.axis_ordinate_to_ordinate_items_map = {}
        for ordinate_item in ORDINATE_ITEM.objects.all().select_related('axis_ordinate_id', 'variable_id', 'member_id'):
            try:
                ordinate_item_list = sdd_context.axis_ordinate_to_ordinate_items_map[
                    ordinate_item.axis_ordinate_id.axis_ordinate_id]
                ordinate_item_list.append(ordinate_item)
            except KeyError:
                sdd_context.axis_ordinate_to_ordinate_items_map[
                    ordinate_item.axis_ordinate_id.axis_ordinate_id] = [ordinate_item]

    def create_table_cells(self, context):
        '''
        import all the axis_ordinate from the rendering package
        '''
        context.table_cell_dictionary = {}
        context.table_to_table_cell_dictionary = {}
        for table_cell in TABLE_CELL.objects.all().select_related('table_id'):
            context.table_cell_dictionary[table_cell.cell_id] = table_cell


            table_cell_list = []
            try:
                table_cell_list = context.table_to_table_cell_dictionary[
                    table_cell.table_id]
            except KeyError:
                context.table_to_table_cell_dictionary[
                    table_cell.table_id] = table_cell_list

            table_cell_list.append(table_cell)


    def create_cell_positions(self, context):
        '''
        import all the axis_ordinate from the rendering package
        '''
        context.cell_positions_dictionary = {}
        for cell_position in CELL_POSITION.objects.all().select_related('cell_id', 'axis_ordinate_id'):
            try:
                cell_position_list = context.cell_positions_dictionary[
                    cell_position.cell_id.cell_id]
                cell_position_list.append(cell_position)
            except KeyError:

                context.cell_positions_dictionary[
                    cell_position.cell_id.cell_id] = [cell_position]


    def create_member_mappings(self, context):
        '''
        Import all the member mappings from the rendering package
        '''
        context.member_mapping_dictionary = {}
        for member_mapping in MEMBER_MAPPING.objects.all():
            context.member_mapping_dictionary[
                member_mapping.member_mapping_id] = member_mapping


    def create_all_member_mapping_items(self, context):
        ''' import all the member mappings from the rendering package'''
        context.member_mapping_items_dictionary = {}
        for member_mapping_item in MEMBER_MAPPING_ITEM.objects.all().select_related('member_mapping_id', 'variable_id', 'member_id', 'member_hierarchy'):
            try:
                member_mapping_list = context.member_mapping_items_dictionary[
                    member_mapping_item.member_mapping_id.member_mapping_id]
                member_mapping_list.append(member_mapping_item)
            except KeyError:
                context.member_mapping_items_dictionary[
                    member_mapping_item.member_mapping_id.member_mapping_id
                ] = [member_mapping_item]

    def create_combination_items(self, context):
        '''
        Import all the combination items
        '''
        context.combination_item_dictionary = {}
        for combination_item in COMBINATION_ITEM.objects.all().select_related('combination_id', 'variable_id', 'member_id', 'subdomain_id'):
            try:
                combination_item_list = context.combination_item_dictionary[
                    combination_item.combination_id.combination_id]
                combination_item_list.append(combination_item)
            except KeyError:
                context.combination_item_dictionary[
                    combination_item.combination_id.combination_id
                ] = [combination_item]

    def create_combinations(self, context):
        '''
        Import all the combinations
        '''
        context.combination_dictionary = {}
        for combination in COMBINATION.objects.all():
            context.combination_dictionary[
                combination.combination_id] = combination

    def create_cube_to_combination(self, context):
        '''
        Import all the cube to combination.

        Includes guardrails to handle orphaned records where cube_id is NULL.
        Such records are automatically cleaned up to maintain data integrity.
        '''
        context.combination_to_rol_cube_map = {}

        # First, check for and clean up orphaned records (cube_id is NULL)
        orphaned_count = CUBE_TO_COMBINATION.objects.filter(cube_id__isnull=True).count()
        if orphaned_count > 0:
            print(f"WARNING: Found {orphaned_count} orphaned CUBE_TO_COMBINATION records (cube_id is NULL). Cleaning up...")
            CUBE_TO_COMBINATION.objects.filter(cube_id__isnull=True).delete()
            print(f"Cleaned up {orphaned_count} orphaned CUBE_TO_COMBINATION records.")

        # Also check for orphaned records where combination_id is NULL
        orphaned_combination_count = CUBE_TO_COMBINATION.objects.filter(combination_id__isnull=True).count()
        if orphaned_combination_count > 0:
            print(f"WARNING: Found {orphaned_combination_count} orphaned CUBE_TO_COMBINATION records (combination_id is NULL). Cleaning up...")
            CUBE_TO_COMBINATION.objects.filter(combination_id__isnull=True).delete()
            print(f"Cleaned up {orphaned_combination_count} orphaned CUBE_TO_COMBINATION records.")

        # Now load valid records only
        for cube_to_combination in CUBE_TO_COMBINATION.objects.filter(
            cube_id__isnull=False,
            combination_id__isnull=False
        ).select_related('cube_id', 'combination_id'):
            try:
                context.combination_to_rol_cube_map[
                    cube_to_combination.cube_id.cube_id
                ].append(cube_to_combination)
            except KeyError:
                context.combination_to_rol_cube_map[
                    cube_to_combination.cube_id.cube_id
                ] = [cube_to_combination]

    def create_cube_links(self, context, framework_id=None):
        '''
        Import all the cube links, optionally filtered by framework.
        When framework_id is provided, only loads CUBE_LINKs where the
        foreign_cube belongs to that framework.
        '''
        context.cube_link_dictionary = {}
        context.cube_link_to_foreign_cube_map = {}
        context.cube_link_to_join_identifier_map = {}
        context.cube_link_to_join_for_report_id_map = {}

        if framework_id:
            # ANCRDT doesn't use _REF suffix, other frameworks do
            if framework_id.upper() in ('ANCRDT', 'ANCRDT_REF'):
                query_framework = 'ANCRDT'
            else:
                # Add _REF suffix if not present (cube links use FINREP_REF, not FINREP)
                query_framework = framework_id if framework_id.endswith('_REF') else f"{framework_id}_REF"
            # Filter cube links by foreign cube's framework
            all_cube_links = CUBE_LINK.objects.filter(
                foreign_cube_id__framework_id=query_framework
            ).select_related('foreign_cube_id', 'foreign_cube_id__framework_id')
        else:
            all_cube_links = CUBE_LINK.objects.all().select_related('foreign_cube_id')

        logging.debug(f"Loading {all_cube_links.count()} CUBE_LINK objects from database" +
              (f" for framework {query_framework if framework_id else ''}" if framework_id else ""))

        for cube_link in all_cube_links:
            context.cube_link_dictionary[cube_link.cube_link_id] = cube_link
            foreign_cube = cube_link.foreign_cube_id
            try:
                context.cube_link_to_foreign_cube_map[foreign_cube.cube_id].append(cube_link)
            except KeyError:
                context.cube_link_to_foreign_cube_map[foreign_cube.cube_id] = [cube_link]
            logging.debug(f"Added to map - cube_link_id={cube_link.cube_link_id}, foreign_cube={foreign_cube.cube_id}")
            join_identifier = cube_link.join_identifier
            try:
                context.cube_link_to_join_identifier_map[join_identifier].append(cube_link)
            except KeyError:
                context.cube_link_to_join_identifier_map[join_identifier] = [cube_link]

            join_for_report_id = foreign_cube.cube_id + ":" + cube_link.join_identifier
            try:
                context.cube_link_to_join_for_report_id_map[join_for_report_id].append(cube_link)
            except KeyError:
                context.cube_link_to_join_for_report_id_map[join_for_report_id] = [cube_link]


    def create_cube_structure_item_links(self, context, framework_id=None):
        '''
        Import all the cube structure item links, optionally filtered by framework.
        When framework_id is provided, only loads links where the cube_link's
        foreign_cube belongs to that framework.
        '''
        context.cube_structure_item_links_dictionary = {}
        context.cube_structure_item_link_to_cube_link_map = {}

        if framework_id:
            # Filter by foreign cube's framework through cube_link
            queryset = CUBE_STRUCTURE_ITEM_LINK.objects.filter(
                cube_link_id__foreign_cube_id__framework_id=framework_id
            ).select_related('cube_link_id', 'cube_link_id__foreign_cube_id')
        else:
            queryset = CUBE_STRUCTURE_ITEM_LINK.objects.all().select_related('cube_link_id')

        for cube_structure_item_link in queryset:
            context.cube_structure_item_links_dictionary[cube_structure_item_link.cube_structure_item_link_id] = cube_structure_item_link
            cube_link = cube_structure_item_link.cube_link_id
            try:
                context.cube_structure_item_link_to_cube_link_map[cube_link.cube_link_id].append(cube_structure_item_link)
            except KeyError:
                context.cube_structure_item_link_to_cube_link_map[cube_link.cube_link_id] = [cube_structure_item_link]

    def import_sdd_for_joins_by_framework(self, sdd_context, tables_to_import, framework_id):
        '''
        Import only the necessary tables for joins from the database, filtered by framework.
        This ensures framework isolation when generating executable joins.
        '''
        print(f"Starting selective import for framework {framework_id} at:")
        print(datetime.now())

        # Group 1 - Base tables with no dependencies (not framework-specific)
        if 'MAINTENANCE_AGENCY' in tables_to_import:
            ImportDatabaseToSDDModel.create_maintenance_agencies(self, sdd_context)
        if 'DOMAIN' in tables_to_import:
            ImportDatabaseToSDDModel.create_all_domains(self, sdd_context)

        # Group 2 - Tables that depend only on DOMAIN and MAINTENANCE_AGENCY
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = []
            if 'VARIABLE' in tables_to_import:
                futures.append(executor.submit(ImportDatabaseToSDDModel.create_all_variables, self, sdd_context))
            if 'CUBE' in tables_to_import:
                # Filter cubes by framework
                futures.append(executor.submit(ImportDatabaseToSDDModel.create_all_rol_cubes, self, sdd_context, framework_id))
            for future in futures:
                future.result()

        # Import CUBE_STRUCTURE (depends on CUBE) - filter by cubes in dictionary
        if 'CUBE_STRUCTURE' in tables_to_import:
            self.create_rol_cube_structures_for_cubes(sdd_context)

        # Group 3 - Tables that depend on CUBE_STRUCTURE and VARIABLE
        if 'CUBE_STRUCTURE_ITEM' in tables_to_import:
            self.create_rol_cube_structure_items_for_structures(sdd_context)

        # Group 4 - Tables that depend on CUBE - filter by framework
        if 'CUBE_LINK' in tables_to_import:
            ImportDatabaseToSDDModel.create_cube_links(self, sdd_context, framework_id)

        # Group 5 - Tables that depend on CUBE_LINK and CUBE_STRUCTURE_ITEM
        if 'CUBE_STRUCTURE_ITEM_LINK' in tables_to_import:
            ImportDatabaseToSDDModel.create_cube_structure_item_links(self, sdd_context, framework_id)

        print(f"Ending selective import for framework {framework_id} at:")
        print(datetime.now())

    def create_rol_cube_structures_for_cubes(self, context):
        '''
        Import cube structures only for cubes already in the bird_cube_dictionary.
        This ensures framework isolation.
        '''
        context.bird_cube_structure_dictionary = {}
        cube_ids = list(context.bird_cube_dictionary.keys())
        logging.debug(f"create_rol_cube_structures_for_cubes: {len(cube_ids)} cubes in dictionary")

        # Get structure IDs from cubes
        structure_ids = set()
        for cube in context.bird_cube_dictionary.values():
            if cube.cube_structure_id:
                structure_ids.add(cube.cube_structure_id.cube_structure_id)
        logging.debug(f"create_rol_cube_structures_for_cubes: Found {len(structure_ids)} structure IDs")

        for rol_cube_structure in CUBE_STRUCTURE.objects.filter(cube_structure_id__in=structure_ids):
            context.bird_cube_structure_dictionary[
                rol_cube_structure.cube_structure_id] = rol_cube_structure
        logging.debug(f"create_rol_cube_structures_for_cubes: Loaded {len(context.bird_cube_structure_dictionary)} structures")

    def create_rol_cube_structure_items_for_structures(self, context):
        '''
        Import cube structure items only for structures already in bird_cube_structure_dictionary.
        This ensures framework isolation.
        '''
        context.bird_cube_structure_item_dictionary = {}
        structure_ids = list(context.bird_cube_structure_dictionary.keys())
        logging.debug(f"create_rol_cube_structure_items_for_structures: Looking for items in {len(structure_ids)} structures")

        for rol_cube_structure_item in CUBE_STRUCTURE_ITEM.objects.filter(
            cube_structure_id__cube_structure_id__in=structure_ids
        ).select_related(
            'cube_structure_id',
            'variable_id',
            'variable_id__domain_id',
            'subdomain_id'
        ):
            try:
                context.bird_cube_structure_item_dictionary[
                    rol_cube_structure_item.cube_structure_id.cube_structure_id
                ].append(rol_cube_structure_item)
            except KeyError:
                context.bird_cube_structure_item_dictionary[
                    rol_cube_structure_item.cube_structure_id.cube_structure_id
                ] = [rol_cube_structure_item]
        logging.debug(f"create_rol_cube_structure_items_for_structures: Loaded items for {len(context.bird_cube_structure_item_dictionary)} structures")
