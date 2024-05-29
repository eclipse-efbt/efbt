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
from utils.utils import Utils
from regdna import CellBasedReport,ReportRow,ReportColumn,ReportCell
from regdna import Filter,RowColumnBasedReport,RowFilters,ColumnFilters
from regdna import WholeReportFilters
from pickle import TRUE




class TranslateToRowColumnReports:
    '''
    Documentation for CombinationsToReportFilters
    '''
    def translate_to_row_column_reports(self, context,sdd_context, framework,cube_type, input_cube_type):  

        if framework == 'FINREP_REF':
            if input_cube_type == 'RC':
                reports_module = context.finrep_on_sdd_reports_module
            elif input_cube_type == 'EIL':
                reports_module = context.finrep_on_il_reports_module
            elif input_cube_type == 'ELDM':
                reports_module = context.finrep_on_ldm_reports_module

        elif framework == 'AE_REF':
            if input_cube_type == 'RC':
                reports_module = context.ae_on_sdd_reports_module
            elif input_cube_type == 'EIL':
                reports_module = context.ae_on_il_reports_module
            elif input_cube_type == 'ELDM':
                reports_module = context.ae_on_ldm_reports_module


        for report in reports_module.reports:
            if isinstance(report, CellBasedReport):
                row_column_based_report = TranslateToRowColumnReports.calculate_whole_report_filters(self,report)
                TranslateToRowColumnReports.calculate_row_and_column_filters(self,report,row_column_based_report)
                reports_module.reports.append(row_column_based_report)
            
    def calculate_whole_report_filters(self,report):
        print("calculate_whole_report_filter for " + report.outputLayer.name)
        # for each variable in the associated output layer look at the choices of 
        # of filter members per cell, if the filter is the same for all cells for that variable
        # then these can be considered a whole report filter.
        row_column_based_report = RowColumnBasedReport ()
        row_column_based_report.outputLayer = report.outputLayer
        for report_row in report.rows:
            row = ReportRow()
            row.name = report_row.name
            row_column_based_report.rows.append(row)
        for report_column in report.columns:
            col = ReportColumn()
            col.name = report_column.name
            row_column_based_report.columns.append(col)
            
        wholeReportFilters = WholeReportFilters()
        row_column_based_report.wholeReportFilters = wholeReportFilters
            
            
        for operation in report.outputLayer.eOperations:
            same_populated_filter_in_all_cells=True
            members_for_filter_for_this_operation = None
            for report_cell in report.reportCells:
                members_for_filter_for_this_operation_for_this_cell = []
                for the_filter in report_cell.filters:
                    if the_filter.operation == operation:
                        members_for_filter_for_this_operation_for_this_cell = the_filter.member
                if members_for_filter_for_this_operation_for_this_cell == []:
                    same_populated_filter_in_all_cells = False
                else:
                    if members_for_filter_for_this_operation is None:
                        same_as_all_previous_cell = True
                    else:
                        same_as_all_previous_cell = TranslateToRowColumnReports.is_member_list_equal(self,members_for_filter_for_this_operation,members_for_filter_for_this_operation_for_this_cell)
                    if members_for_filter_for_this_operation is None:
                        same_as_all_previous_cell = True
                    if not(same_as_all_previous_cell):
                        same_populated_filter_in_all_cells = False
                    else:
                        members_for_filter_for_this_operation = members_for_filter_for_this_operation_for_this_cell
                        
            if same_populated_filter_in_all_cells and not(members_for_filter_for_this_operation is None):
                print("whole report filter = " + report.outputLayer.name + ":" + operation.name )
                a_filter = Filter()
                a_filter.operation = operation
                a_filter.oututLayer = row_column_based_report.outputLayer
                for the_member in members_for_filter_for_this_operation:
                    
                    a_filter.member.append(the_member)
                    print(the_member.name)
                    
                row_column_based_report.wholeReportFilters.filters.append(a_filter) 
                
        return row_column_based_report
    
                          
    def calculate_row_and_column_filters(self,cell_based_report,row_column_based_report):
        whole_report_filter =row_column_based_report.wholeReportFilters

        row_to_metric_dict = {} 
        for report_row in cell_based_report.rows:
            metric = None
            row_filters = RowFilters()
            row_filters.row = report_row
            row_column_based_report.rowFilters.append(row_filters)
            operation_to_member_dict = {} 
            
            for cell in cell_based_report.reportCells:
                row_metric = None
                if cell.row.name == report_row.name:
                    try:
                        row_metric =  row_to_metric_dict[cell.row.name]
                    except KeyError:
                        row_to_metric_dict[cell.row.name] = cell.metric
                        row_metric =  row_to_metric_dict[cell.row.name]
                        
                    if cell.metric is None:
                        row_to_metric_dict[cell.row.name] = None 
                    elif  not(row_metric is None):
                        if  not(row_metric.name == cell.metric.name):
                            row_to_metric_dict[cell.row.name] = None
                    
                    for a_filter in cell.filters:                            
                        if not(TranslateToRowColumnReports.same_filter_is_in_whole_report_filter(self,a_filter, whole_report_filter)): 
                            op = a_filter.operation
                            stored_member_list = None
                            try:
                                stored_member_list =  operation_to_member_dict[op]
                            except KeyError:
                                operation_to_member_dict[op] = a_filter.member
                                stored_member_list =  operation_to_member_dict[op]
                                
                            # if the filters list and stored list are nt equal then this is a problem
                            # and we will set the dictionary item to None
                            # if it is already none then there was already aproblem
                            if  not(operation_to_member_dict[op] is None):
                                if  not(TranslateToRowColumnReports.is_member_list_equal(self,stored_member_list,a_filter.member)):
                                    operation_to_member_dict[op] = None
                                    
            for operation,member_list in operation_to_member_dict.items():
                if not (member_list is None):
                    if len(member_list) > 0:
                        new_filter = Filter()
                        new_filter.operation = operation
                        for member in member_list:
                            new_filter.member.append(member)
                            
                        row_metric = None
                        try:
                            row_metric =  row_to_metric_dict[row_filters.row.name]
                        except KeyError:
                            pass
                        
                        if not(row_metric is None):
                            row_filters.metric = row_metric
                        row_filters.filters.append(new_filter)
                        
        
        column_to_metric_dict = {}         
        for report_column in cell_based_report.columns:
            metric = None
            column_filters = ColumnFilters()
            column_filters.column = report_column
            row_column_based_report.columnFilters.append(column_filters)
            operation_to_member_dict = {} 
            
            for cell in cell_based_report.reportCells:
                column_metric = None
                if cell.column.name == report_column.name:
                    
                    try:
                        column_metric =  column_to_metric_dict[cell.column.name]
                    except KeyError:
                        column_to_metric_dict[cell.column.name] = cell.metric
                        column_metric =  column_to_metric_dict[cell.column.name]
                     
                    if cell.metric is None:
                        column_to_metric_dict[cell.column.name] = None     
                    elif  not(column_metric is None):
                        if  not(column_metric.name == cell.metric.name):
                            column_to_metric_dict[cell.column.name] = None
                    
                    for a_filter in cell.filters:                            
                        if not(TranslateToRowColumnReports.same_filter_is_in_whole_report_filter(self,a_filter, whole_report_filter)): 
                            op = a_filter.operation
                            stored_member_list = None
                            try:
                                stored_member_list =  operation_to_member_dict[op]
                            except KeyError:
                                operation_to_member_dict[op] = a_filter.member
                                stored_member_list =  operation_to_member_dict[op]

                            # if the filters list and stored list are not equal then this is a problem
                            # and we will set the dictionary item to None
                            # if it is already none then there was already a problem
                            if  not(operation_to_member_dict[op] is None):
                                if  not(TranslateToRowColumnReports.is_member_list_equal(self,stored_member_list,a_filter.member)):
                                    operation_to_member_dict[op] = None
                                    
            for operation,member_list in operation_to_member_dict.items():
                if not (member_list is None):
                    if len(member_list) > 0:
                        new_filter = Filter()
                        new_filter.operation = operation
                        for member in member_list:
                            new_filter.member.append(member)
                            
                        column_metric = None
                        try:
                            column_metric = column_to_metric_dict[column_filters.column.name]
                        except KeyError:
                            pass
                        if not(column_metric is None):
                            column_filters.metric = column_metric
                        column_filters.filters.append(new_filter)


    def same_filter_is_in_whole_report_filter(self,a_filter, whole_report_filter):
        operation = a_filter.operation
        for the_filter in whole_report_filter.filters:
            if the_filter.operation == operation:
                #if TranslateToRowColumnReports.is_member_list_equal(self,a_filter.member,the_filter.member):
                return True 
        return False
                
                
    def is_member_list_equal(self,list1,list2):
        return_val = True
        if (list1 is None) or (list2 is None):
            # if list1 is None,  the represents the first check and so we return True
            return_val = False 
        else:
            # is all items of list 1 ar in list2 and all items of list 2 
            # are in list 1 then they are equals lists.
            for item in list1:
                if not (item in list2):
                    return_val = False
            for item in list2:
                if not (item in list1):
                    return_val = False
                    
        return return_val
                    
          