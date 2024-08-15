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
from whitebox.utils.utils import Utils
from whitebox.regdna import CellBasedReport,ReportRow,ReportColumn,ReportCell,Filter
import os
import csv

class CreateReportFilters:
    '''
    Documentation for CreateReportFilters
    '''
    def create_report_filters(self, context,sdd_context,framework,cube_type,input_cube_type,version):
        
        reports_module = None

        if framework == 'FINREP_REF':
            if input_cube_type == 'ELDM':
                reports_module = context.finrep_on_ldm_reports_module

        elif framework == 'AE_REF':
            if input_cube_type == 'ELDM':
                reports_module = context.ae_on_ldm_reports_module

        
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
        
        warning_list = []
        for table in sdd_context.report_tables_dictionary.values():
            table_id  =  table.table_id
            altered_table_name = Utils.make_valid_id(table_id) +"_OutputItem"
            report_rol = CreateReportFilters.get_report_rol_for_table_id(self, altered_table_name, context,framework)
            if not (report_rol is None):
                report = CellBasedReport()
                report.outputLayer = report_rol
                reports_module.reports.append(report)
                context.reports_dictionary[framework + ":" + cube_type+ ":" + input_cube_type+ ":" + report_rol.name] = report
                
        
        cell_to_variable_member_tuple_map = {}
        for cell_id in sdd_context.cell_positions_dictionary.keys():
            try:
                cell = sdd_context.table_cell_dictionary[cell_id]
            except KeyError:
                print("could not find cell for " + cell_id)

            if not(cell is None) and not (cell.table_id is None):
                cube_mapping_id  = CreateReportFilters.get_report_cube_mapping_id_for_table_id(self,cell.table_id.table_id,framework,version)
                relevant_mappings = sdd_context.mapping_to_cube_dictionary[cube_mapping_id]
                if not(cell is None):
                    table = cell.table_id
                    if not(table is None):               
                        template_code = table.table_id
                        report_cell = ReportCell()
                        try:
                            altered_template_code = Utils.make_valid_id(template_code) +"_OutputItem"

                            rol = CreateReportFilters.get_report_rol_for_table_id(self, altered_template_code, context,framework)
                            if not (rol is None):
                                related_report = context.reports_dictionary[framework + ":" + cube_type+ ":" + input_cube_type+ ":" +rol.name]
                                
                                for cell_position in sdd_context.cell_positions_dictionary[cell_id]:
                                    # get the row and column for the cell
                                    axis_ordinate = cell_position.axis_ordinate_id
                                    axis=axis_ordinate.axis_id
                                    orientation = axis.orientation
                                    if orientation == 'Y':
                                        row = ReportRow(name = "row_" +axis_ordinate.code)
                                        report_cell.row = row
                                        row_exists_in_rows = False
                                        for the_row in related_report.rows:
                                            if the_row.name == "row_" +axis_ordinate.code:
                                                row_exists_in_rows =  True
                                        if not (row_exists_in_rows):
                                            related_report.rows.append(row)
                                    if orientation == 'X':
                                        col = ReportColumn(name = "col_" + axis_ordinate.code)   
                                        report_cell.column = col                         
                                        col_exists_in_columns = False
                                        for the_col in related_report.columns:
                                            if the_col.name == "col_" + axis_ordinate.code:
                                                col_exists_in_columns =  True
                                        if not (col_exists_in_columns):
                                            related_report.columns.append(col)
                                    
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
                                            if not(ref_variable is None):
                                                operation = CreateReportFilters.find_operation_with_id(self,related_report,ref_variable.variable_id)
                                            else:
                                                operation = None
                                            try:
                                                domain_id = ref_member.domain_id.domain_id
                                                literal_list = CreateReportFilters.find_literals_with_id(self,context,sdd_context,ref_member,domain_id,warning_list, template_code, cell_id, variable,framework,cube_type,input_cube_type)
                                                the_filter = Filter()
                                                the_filter.operation = operation
                                                if not(literal_list is None):
                                                    for literal in literal_list:
                                                        the_filter.member.append(literal)

                                                if not(operation is None):
                                                        report_cell.filters.append(the_filter)  
                                            except:
                                                if not(ref_member is None):
                                                    print("could not find domain_id for " + ref_member.member_id)

                                            
                                related_report.reportCells.append(report_cell)  
                            else:
                                print("could not find report for " + template_code)
                        except KeyError:
                            print("could not find report for " + template_code)  
                            
                                            
                        
    def get_reference_tuple_list(self,sdd_context,non_ref_tuple_list,relevant_mappings):
        # for each member_ mapping, apply the mappings to the non_ref_tuple_list
        # and output a reference tuple
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
                        ref_tuple_list = []
                        for member_mapping_item in member_mapping_items:
                            if not (member_mapping_item.isSource =='true' ):
                                ref_tuple_list.append((member_mapping_item.variable,member_mapping_item.member))
                        return ref_tuple_list 
                            
        return None
    def create_member_mapping_item_row_dict(self,sdd_context,member_mapping):
        member_mapping_item_row_dict = {}
        member_mapping_items = sdd_context.member_mapping_items_dictionary[member_mapping.member_mapping_id]
            
        for member_mapping_item in member_mapping_items:
            try:
                member_mapping_item_row_dict[member_mapping_item.row].append(member_mapping_item)
            except KeyError:
                member_mapping_item_row_dict[member_mapping_item.row] = [member_mapping_item]

        return member_mapping_item_row_dict
    
    def find_operation_with_id(self,related_report,op_id):

        for op in related_report.outputLayer.eOperations:
            if op.name == op_id:
                return op
            
    def get_report_cube_mapping_id_for_table_id(self, table_id,framework,version):
        ''' 
        e.g. from FINREP_REF_F_05.01_REF_FINREP 3.0 to M_F_05.01_REF_FINREP 3.0
        '''

        return 'M_' + table_id.replace(framework + '_', '')
    
    def get_report_rol_for_table_id(self, table_id, context, framework):
 
        output_tables_package = None
        if framework == 'FINREP_REF':
            output_tables_package = context.finrep_output_tables_package
        elif framework == 'AE_REF':
            output_tables_package = context.ae_output_tables_package

        for rol_class in output_tables_package.eClassifiers:
            if table_id.replace(' ','_').replace('.','_') == framework + '_' + rol_class.name:
                return rol_class
            
        return None   
    
    def find_literals_with_id(self,context,sdd_context,member,domain_id,warning_list, template_code, combination_id, variable_id,framework,cube_type,input_cube_type):
        return_literal = None
        return_literal = CreateReportFilters.find_literal_with_id(self,context,member,domain_id,framework,cube_type,input_cube_type)
        return CreateReportFilters.get_literal_list_considering_hierarchies(self,context,sdd_context,return_literal,member,domain_id,warning_list, template_code,combination_id, variable_id,framework,cube_type,input_cube_type)

       
    def find_literal_with_id(self,context,member,domain_id,framework,cube_type,input_cube_type):
            try:
                return context.enum_literals_map[domain_id + "_domain" +":" +  Utils.make_valid_id_for_literal(member.code)]
            except:
                return None

    def get_literal_list_considering_hierarchies(self,context,sdd_context,literal,member,domain_id, warning_list, template_code,combination_id, variable_id,framework,cube_type,input_cube_type):
        return_list = []
        if literal is None:
            is_node = CreateReportFilters.is_member_a_node(self,sdd_context,member)
            if not (is_node):
                warning_list.append( ("error", "member does not exist in input layer and is not a node", template_code,combination_id, variable_id, member.member_id, None,domain_id))
            pass
        else:
            return_list = [literal]
            
        for domain,hierarchy_list in sdd_context.domain_to_hierarchy_dictionary.items():
            if domain is None:
                print("domain is None in hierarchy_list")
            if domain.domain_id == domain_id:
                for hierarchy in hierarchy_list:
                    hierarchy_id = hierarchy.member_hierarchy_id                
                    literal_list = []
                    CreateReportFilters.get_literal_list_considering_hierarchy(self,context,sdd_context,member,hierarchy_id,literal_list,framework,cube_type,input_cube_type)
                    return_list.extend(literal_list)
                    
        if len(return_list) == 0:
            warning_list.append( ("error","could not find any input layer members or sub members for member", template_code,combination_id, variable_id, member.member_id, None,domain_id))
        return return_list     

    
                

    def get_literal_list_considering_hierarchy(self,context,sdd_context,member,hierarchy,literal_list,framework,cube_type,input_cube_type):
        
        key = member.member_id + ":" + hierarchy
        child_members = []
        try:
            child_members = sdd_context.member_plus_hierarchy_to_child_literals[key]
            for item in child_members:
                if item.domain_id is None:
                    print("domain_id is None for " + item.member_id)
                if item is None:
                    print("item is None for " + item.member_id)
                literal = CreateReportFilters.find_literal_with_id(self,context,item,item.domain_id.domain_id,framework,cube_type,input_cube_type)
                if not(literal is None):
                    if not(literal in literal_list):
                        literal_list.append(literal)
                    
            for item in child_members:
                CreateReportFilters.get_literal_list_considering_hierarchy(self,context,sdd_context,item,hierarchy, literal_list,framework,cube_type,input_cube_type)
        except KeyError:
            pass
        
            
    def is_member_a_node(self,sdd_context,member):

        if member in sdd_context.members_that_are_nodes:
            return True
        else:
            return False

        
    def find_member_node(self,sdd_context,member_id,hierarchy):
        try:
            return sdd_context.member_hierarchy_node_dictionary[hierarchy + ":" + member_id.member_id]
        except:
            pass