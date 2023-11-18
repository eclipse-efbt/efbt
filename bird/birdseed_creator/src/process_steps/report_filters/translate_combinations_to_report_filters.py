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
from regdna import Report,ReportRow,ReportColumn,ReportCell,Filter,ELEnum

class CombinationsToReportFilters:
    '''
    Documentation for CombinationsToReportFilters
    '''

    def translate_combinations_to_report_filters(self, context,sdd_context):
        
        
        for table in sdd_context.report_tables.reportTables:
            table_code  =  table.code
            altered_table_name = Utils.make_valid_id(table_code) +"_OutputItem"
            report_rol = CombinationsToReportFilters.get_report_rol_for_table_code(self, altered_table_name, context)
            print("report_rol")
            print(report_rol)
            
            report = Report()
            report.outputLayer = report_rol
            context.reports_module.reports.append(report)
            context.reports_dictionary[altered_table_name] = report
            
            
        for cell_position in sdd_context.cell_positions.cellPositions:
            x_axis = "None"
            y_axis = "None"
            cell = cell_position.cell_id
            if not(cell is None):
                template = cell.table_id
                template_name = template.table_id
                template_code = template.code
                try:
                    altered_template_name = Utils.make_valid_id(template_code) +"_OutputItem"
                    related_report = context.reports_dictionary[altered_template_name]
                    for axis_ordinate in cell_position.axis_ordinate_id:
                        axis=axis_ordinate.axis_id
                        orientation = axis.orientation
                        if orientation == 'Y':
                            row = ReportRow(name = axis_ordinate.code)
                            related_report.rows.append(row)
                        if orientation == 'X':
                            col = ReportColumn(name = axis_ordinate.code)
                            related_report.columns.append(col)

                    report_cell = ReportCell()
                    related_report.reportCells.append(report_cell)
                    comb = cell_position.cell_id.combination_id
                    # if there is no combination, this is because the combination is not
                    # current, according to its valid_to date.
                    if not(comb is None):
                        report_cell.datapointID = comb.combination_id
                        items = comb.combination_items
                        for item in items:
                            variable_id = item.variable_id.variable_id
                            if variable_id == "MTRCS":
                                # todo make sure we can get the metric name from
                                # the associate varaible set
                                variable_set = item.variable_set_id
                                if not(variable_set is None):
                                    for item in variable_set.variable_set_items:
                                        metric = CombinationsToReportFilters.find_operation_with_id(self,context,related_report,item.variable_id.variable_id)
                                        report_cell.metric = metric
                                        if metric is None:
                                            print("no attribute for " + item.variable_id.variable_id)
                                        else:
                                            print("Metric=" + item.variable_id.variable_id + ":")
                                else:
                                    print("combination" + comb.combination_id + "has no metrics for metrics variable")
                            elif variable_id == "SUBA_CD" or variable_id == "VALUE_DECIMAL" or variable_id == "DT_RFRNC" or variable_id == "ENTTY_RIAD_CD_RPRTNG_AGNT" :
                                # these entity and refernce dates are not useful filters , we dont diplay them,
                                pass
                            else:
                                filter = Filter()
                                report_cell.filters.append(filter)
                                try:
                                    operation = CombinationsToReportFilters.find_operation_with_id(self,context,related_report,item.variable_id.variable_id)
                                        
                                    member = item.member_id
                                    if(member is not None):
                                        member_id = member.member_id
                                        member_code = member.code
                                        print(variable_id+"==" +member_code +":")
                                        literal = CombinationsToReportFilters.find_literal_with_id(self,context,member_code)
                                        filter.operation = operation
                                        filter.member.append(literal)
                                except:
                                    print("failed to make filter")
                                    pass
        
                except:
                    print("failed to find report")
                    pass
        # get the reports, create the reports
        # get the rows and column , create the rows and column
        # get the cells, create the cells
        # get the combinations, create the filters
        # when creating filters, get all leaf nodes from hierarchy.
        # persists as xmi and as text.
      
    def find_operation_with_id(self,context,related_report,op_id):

        for op in related_report.outputLayer.eOperations:
            if op.name == op_id:
                return op
            
        
    def get_report_rol_for_table_code(self, table_code, context):
        for rol_class in context.output_tables_package.eClassifiers:

            if table_code == rol_class.name:
                return rol_class
        return None   
    
    def find_literal_with_id(self,context,member_id):
        for enum in context.sdd_domains_package.eClassifiers:
            if isinstance(enum, ELEnum):
                for literal in enum.eLiterals:
                    if literal.literal == "_" + member_id:
                        return literal
        
        
            
        
        