# coding=UTF-8#
# Copyright (c) 2020 Bird Software Solutions Ltd
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

from birdbox.sdd_models import *

class ImportDatabaseToSDDModel(object):
    '''
    Class responsible for the import of  SDD csv files
    into an instance of the analaysis model
    '''
    def import_sdd(self,sdd_context):
        '''
        Import SDD csv files into an instance of the analysis model
        '''
        
        ImportDatabaseToSDDModel.create_maintenance_agencies(self, sdd_context)
        ImportDatabaseToSDDModel.create_frameworks(self, sdd_context)
        ImportDatabaseToSDDModel.create_all_domains(self, sdd_context)
        ImportDatabaseToSDDModel.create_all_members(self, sdd_context)
        ImportDatabaseToSDDModel.create_all_variables(self, sdd_context)
        ImportDatabaseToSDDModel.create_all_rol_cube_structures(self, sdd_context)
        ImportDatabaseToSDDModel.create_all_rol_cubes(self, sdd_context)
        ImportDatabaseToSDDModel.create_all_rol_cube_structure_items(self, sdd_context)

        ImportDatabaseToSDDModel.create_all_nonref_member_hierarchies(self, sdd_context)
        ImportDatabaseToSDDModel.create_all_nonref_member_hierarchies_nodes(
            self, sdd_context)
        ImportDatabaseToSDDModel.create_member_mappings(self, sdd_context)
        ImportDatabaseToSDDModel.create_all_member_mapping_items(self, sdd_context)
        ImportDatabaseToSDDModel.create_all_mapping_to_cubes(self, sdd_context)
        ImportDatabaseToSDDModel.create_all_mapping_definitions(self, sdd_context)
        ImportDatabaseToSDDModel.create_all_variable_mappings(self, sdd_context)
        ImportDatabaseToSDDModel.create_all_variable_mapping_items(self, sdd_context)
                
        ImportDatabaseToSDDModel.create_report_tables(self, sdd_context)
        ImportDatabaseToSDDModel.create_table_cells(self, sdd_context)
        ImportDatabaseToSDDModel.create_axis(self, sdd_context)
        ImportDatabaseToSDDModel.create_axis_ordinates(self, sdd_context)
        ImportDatabaseToSDDModel.create_ordinate_items(self, sdd_context)
        ImportDatabaseToSDDModel.create_cell_positions(self, sdd_context)

        ImportDatabaseToSDDModel.create_combinations(self, sdd_context)
        ImportDatabaseToSDDModel.create_combination_items(self, sdd_context)
        ImportDatabaseToSDDModel.create_cube_to_combination(self, sdd_context)

    def create_all_mapping_definitions(self, context):
        '''
        import all the mapping definitions
        '''
        for mapping_definition in MAPPING_DEFINITION.objects.all():
            context.mapping_definition_dictionary[
                mapping_definition.mapping_id] = mapping_definition
            
    def create_all_variable_mappings(self, context):
        '''
        import all the variable mappings
        '''
        for variable_mapping in VARIABLE_MAPPING.objects.all():
            context.variable_mapping_dictionary[
                variable_mapping.variable_mapping_id] = variable_mapping

    def create_all_variable_mapping_items(self, context):
        '''
        import all the variable mapping items
        '''
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
        for rol_cube_structure in CUBE_STRUCTURE.objects.all():
            context.rol_cube_structure_dictionary[
                rol_cube_structure.cube_structure_id] = rol_cube_structure

    def create_all_rol_cubes(self, context):
        '''
        import all the rol cubes
        '''
        for rol_cube in CUBE.objects.all():
            context.rol_cube_dictionary[rol_cube.cube_id] = rol_cube

    def create_all_rol_cube_structure_items(self, context):
        '''
        import all the rol cube structure items
        '''
        for rol_cube_structure_item in CUBE_STRUCTURE_ITEM.objects.all():
            try:
                context.rol_cube_structure_item_dictionary[
                    rol_cube_structure_item.cube_structure_id.cube_structure_id
                ].append(rol_cube_structure_item)
            except KeyError:
                context.rol_cube_structure_item_dictionary[
                    rol_cube_structure_item.cube_structure_id.cube_structure_id
                ] = [rol_cube_structure_item]

    def create_all_mapping_to_cubes(self, context):
        '''
        import all the mapping to cubes
        '''
        for mapping_to_cube in MAPPING_TO_CUBE.objects.all():
            try:
                mapping_to_cube_list = context.mapping_to_cube_dictionary[
                    mapping_to_cube.cubeMapping]
                mapping_to_cube_list.append(mapping_to_cube)
            except KeyError:
                context.mapping_to_cube_dictionary[
                    mapping_to_cube.cubeMapping] = [mapping_to_cube]

    def create_maintenance_agencies(self, context):
        '''
        Import all maintenance agencies
        '''
        for agency in MAINTENANCE_AGENCY.objects.all():
            context.agency_dictionary[agency.maintenance_agency_id] = agency
        
    def create_frameworks(self, context):
        '''
        Import all frameworks
        '''
        for framework in FRAMEWORK.objects.all():
            context.framework_dictionary[framework.framework_id] = framework
    
    def create_all_domains(self, context):
        '''
        import all the domains
        '''
        for domain in DOMAIN.objects.all():
            context.nonref_domain_dictionary[domain.domain_id] = domain

        
    def create_all_members(self, context):
        '''
        Import all the members
        '''
        for member in MEMBER.objects.all():
            context.nonref_member_dictionary[member.member_id] = member
            context.member_id_to_domain_map[member] = member.domain_id
            context.member_id_to_member_code_map[member.member_id] = member.code

    def create_all_variables(self, context):
        '''
        import all the variables
        '''

        for variable in VARIABLE.objects.all():
            context.nonref_variable_dictionary[variable.variable_id] = variable
            context.variable_to_domain_map[variable.variable_id] = variable.domain_id
            context.variable_to_long_names_map[variable.variable_id] = variable.name
            context.variable_to_primary_concept_map[variable.variable_id] = variable.primary_concept
   
    def create_all_nonref_member_hierarchies(self, context):
        '''
        Import all non-reference member hierarchies
        '''
        for hierarchy in MEMBER_HIERARCHY.objects.all():
            context.member_hierarchy_dictionary[
                hierarchy.member_hierarchy_id] = hierarchy
        
                    
    def create_all_nonref_member_hierarchies_nodes(self, context):
        '''
        Import all non-reference member hierarchy nodes
        '''
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

        for table in TABLE.objects.all():
            context.report_tables_dictionary[table.table_id] = table

    def create_axis (self, context):
        '''
        import all the axes from the rendering package
        '''
        for axis in AXIS.objects.all():
            context.axis_dictionary[axis.axis_id] = axis

    def create_axis_ordinates(self, context):
        '''
        import all the axis_ordinate from the rendering package
        '''

        for axis_ordinate in AXIS_ORDINATE.objects.all():
            context.axis_ordinate_dictionary[
                axis_ordinate.axis_ordinate_id] = axis_ordinate
        
    def create_ordinate_items(self, sdd_context):
        '''
        import all the axis_ordinate from the rendering package
        '''
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
        for member_mapping in MEMBER_MAPPING.objects.all():
            context.member_mapping_dictionary[
                member_mapping.member_mapping_id] = member_mapping
        
                        
    def create_all_member_mapping_items(self, context):
        ''' import all the member mappings from the rendering package'''
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
        for combination in COMBINATION.objects.all():
            context.combination_dictionary[
                combination.combination_id] = combination

    def create_cube_to_combination(self, context):
        '''
        Import all the cube to combination
        '''
        for cube_to_combination in CUBE_TO_COMBINATION.objects.all():
            try:
                context.combination_to_rol_cube_map[
                    cube_to_combination.cube_id.cube_id
                ].append(cube_to_combination)
            except KeyError:
                context.combination_to_rol_cube_map[
                    cube_to_combination.cube_id.cube_id
                ] = [cube_to_combination]
