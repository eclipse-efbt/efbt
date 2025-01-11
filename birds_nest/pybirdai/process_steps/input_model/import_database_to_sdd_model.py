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

from pybirdai.bird_meta_data_model import *
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
class ImportDatabaseToSDDModel(object):
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
        for variable_mapping_item in VARIABLE_MAPPING_ITEM.objects.all():
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

    def create_all_rol_cubes(self, context):
        '''
        import all the rol cubes
        '''
        context.bird_cube_dictionary = {}
        for rol_cube in CUBE.objects.all():
            context.bird_cube_dictionary[rol_cube.cube_id] = rol_cube

    def create_all_rol_cube_structure_items(self, context):
        '''
        import all the rol cube structure items
        '''
        context.bird_cube_structure_item_dictionary = {}
        for rol_cube_structure_item in CUBE_STRUCTURE_ITEM.objects.all():
            try:
                context.bird_cube_structure_item_dictionary[
                    rol_cube_structure_item.cube_structure_id.cube_structure_id
                ].append(rol_cube_structure_item)
            except KeyError:
                context.bird_cube_structure_item_dictionary[
                    rol_cube_structure_item.cube_structure_id.cube_structure_id
                ] = [rol_cube_structure_item]

    def create_all_mapping_to_cubes(self, context):
        '''
        import all the mapping to cubes
        '''
        context.mapping_to_cube_dictionary = {}
        for mapping_to_cube in MAPPING_TO_CUBE.objects.all():
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
        for member in MEMBER.objects.all():
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
        for variable in VARIABLE.objects.all():
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
        for hierarchy_node in MEMBER_HIERARCHY_NODE.objects.all():
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
        for ordinate_item in ORDINATE_ITEM.objects.all():
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
        for table_cell in TABLE_CELL.objects.all():
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
        for cell_position in CELL_POSITION.objects.all():
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
        for member_mapping_item in MEMBER_MAPPING_ITEM.objects.all():
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
        for combination_item in COMBINATION_ITEM.objects.all():
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
        Import all the cube to combination
        '''
        context.combination_to_rol_cube_map = {}
        for cube_to_combination in CUBE_TO_COMBINATION.objects.all():
            try:
                context.combination_to_rol_cube_map[
                    cube_to_combination.cube_id.cube_id
                ].append(cube_to_combination)
            except KeyError:
                context.combination_to_rol_cube_map[
                    cube_to_combination.cube_id.cube_id
                ] = [cube_to_combination]

    def create_cube_links(self, context):
        '''
        Import all the cube links
        '''
        context.cube_link_dictionary = {}
        context.cube_link_to_foreign_cube_map = {}
        context.cube_link_to_join_identifier_map = {}
        context.cube_link_to_join_for_report_id_map = {}
        for cube_link in CUBE_LINK.objects.all():
            context.cube_link_dictionary[cube_link.cube_link_id] = cube_link
            foreign_cube = cube_link.foreign_cube_id
            try:
                context.cube_link_to_foreign_cube_map[foreign_cube.cube_id].append(cube_link)
            except KeyError:
                context.cube_link_to_foreign_cube_map[foreign_cube.cube_id] = [cube_link]
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


    def create_cube_structure_item_links(self, context):
        '''
        Import all the cube structure item links
        '''     
        context.cube_structure_item_links_dictionary = {}
        context.cube_structure_item_link_to_cube_link_map = {}
        for cube_structure_item_link in CUBE_STRUCTURE_ITEM_LINK.objects.all():
            context.cube_structure_item_links_dictionary[cube_structure_item_link.cube_structure_item_link_id] = cube_structure_item_link    
            cube_link = cube_structure_item_link.cube_link_id
            try:
                context.cube_structure_item_link_to_cube_link_map[cube_link.cube_link_id].append(cube_structure_item_link)
            except KeyError:
                context.cube_structure_item_link_to_cube_link_map[cube_link.cube_link_id] = [cube_structure_item_link]