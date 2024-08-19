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
from birdbox.utils.utils import Utils
from birdbox.sdd_models import *
import os
import csv

class CreateReportFilters:
    '''
    Documentation for CreateReportFilters
    '''
    def create_report_filters(self, context,sdd_context,framework,version):
        
        file_location = context.file_directory + os.sep + "in_scope_reports_" + framework + ".csv"
        in_scope_reports = []
        header_skipped = False
        # Loop through the list of in scope reports
        with open(file_location,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                # skip the first line which is the header.
                if (not header_skipped):
                    header_skipped = True
                else:
                    report_template = row[0]

                    in_scope_reports.append(report_template)
        
       

        cell_to_variable_member_tuple_map = {}
        for cell_id in sdd_context.cell_positions_dictionary.keys():
            try:
                cell = sdd_context.table_cell_dictionary[cell_id]
            except KeyError:
                print("could not find cell for " + cell_id)

            if not(cell is None) and not (cell.table_id is None):
                cube_mapping_id  = CreateReportFilters.get_report_cube_mapping_id_for_table_id(self,cell.table_id.table_id,framework)
                relevant_mappings = sdd_context.mapping_to_cube_dictionary[cube_mapping_id]
                report_rol_cube = CreateReportFilters.get_rol_cube_for_table_id(self, cell.table_id.table_id, sdd_context,framework,version)
                if not(cell is None):
                    table = cell.table_id
                    if not(table is None):               
                        template_code = table.table_id
                        report_cell = COMBINATION()
                        report_cell.combination_id = cell_id
                        report_cell.save()

                        cube_to_comb = CUBE_TO_COMBINATION()
                        cube_to_comb.combination_id = report_cell
                        cube_to_comb.cube_id = report_rol_cube
                        cube_to_comb.save()

                        try:
                            altered_template_code = Utils.make_valid_id(template_code)
                            rol = CreateReportFilters.get_rol_cube_for_table_id(self, altered_template_code, sdd_context,framework,version)
                            if not (rol is None):
                                
                                
                                for cell_position in sdd_context.cell_positions_dictionary[cell_id]:
                                    # get the row and column for the cell
                                    axis_ordinate = cell_position.axis_ordinate_id
                                    axis=axis_ordinate.axis_id
                                    orientation = axis.orientation
                                    
                                    
                                    # get the variables and members from the axis.
                                    ordinate_items = sdd_context.axis_ordinate_to_ordinate_items_map[axis_ordinate.axis_ordinate_id] 
                                    for ordinate_item in ordinate_items:
                                        variable = ordinate_item.variable_id
                                        member = ordinate_item.member_id
                                        try:
                                            cell_to_variable_member_tuple_map[cell_id].append((variable,member))
                                        except KeyError:
                                            cell_to_variable_member_tuple_map[cell_id] = [(variable,member)]

                                    tuples = []
                                    try:
                                        tuples = cell_to_variable_member_tuple_map[cell_id]
                                    except KeyError:
                                        pass
                                    
                                    ref_tuple_list = CreateReportFilters.get_reference_tuple_list(self,sdd_context,tuples,relevant_mappings)
                                    if not(ref_tuple_list is None):
                                        for ref_tuple_in in ref_tuple_list:
                                            ref_variable = ref_tuple_in[0]
                                            ref_member = ref_tuple_in[1]
                                            
                                            the_filter = COMBINATION_ITEM()
                                            the_filter.combination_id = report_cell
                                            the_filter.variable_id = ref_variable
                                            the_filter.member_id = ref_member
                                            the_filter.save()

                            else:
                                print("could not find report for " + template_code)
                        except KeyError:
                            print("could not find report for " + template_code)  
                            
                                            
                        
    def get_reference_tuple_list(self,sdd_context,non_ref_tuple_list,relevant_mappings):
        # for each member_ mapping, apply the mappings to the non_ref_tuple_list
        # and output a reference tuple
        ref_tuple_list = []
        for mapping in relevant_mappings:
            member_mapping = mapping.mapping.memberMapping
            if not(member_mapping is None):
                member_mapping_item_row_dict = CreateReportFilters.create_member_mapping_item_row_dict(self,sdd_context,member_mapping)
                for row,member_mapping_items in member_mapping_item_row_dict.items():
                    match = True
                    for member_mapping_item in member_mapping_items:
                        if member_mapping_item.isSource =='true':
                            if not ((member_mapping_item.variable,member_mapping_item.member) in non_ref_tuple_list):
                                # set match to false if any of the source items in this row do not match
                                match = False
                                break
                    if match:
                        
                        for member_mapping_item in member_mapping_items:
                            if not (member_mapping_item.isSource =='true' ):
                                ref_tuple_list.append((member_mapping_item.variable,member_mapping_item.member))
        return ref_tuple_list 
                            
        
    def create_member_mapping_item_row_dict(self,sdd_context,member_mapping):
        member_mapping_item_row_dict = {}
        member_mapping_items = sdd_context.member_mapping_items_dictionary[member_mapping.member_mapping_id]
            
        for member_mapping_item in member_mapping_items:
            try:
                member_mapping_item_row_dict[member_mapping_item.row].append(member_mapping_item)
            except KeyError:
                member_mapping_item_row_dict[member_mapping_item.row] = [member_mapping_item]

        return member_mapping_item_row_dict
    
    
            
    def get_report_cube_mapping_id_for_table_id(self, table_id,framework):
        ''' 
        e.g. from FINREP_REF_F_05.01_REF_FINREP 3.0 to M_F_05.01_REF_FINREP 3.0
        '''

        return 'M_' + table_id.replace(framework + '_', '')
    
     
    
    def get_rol_cube_for_table_id(self, table_id, sdd_context, framework,version):
 
        try:
            return sdd_context.rol_cube_dictionary[(framework + "_" + table_id + "_" + version).replace(' ','_').replace('.','_')]
        except KeyError:
            return None

        
    