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
import os
from whitebox.utils.utils import Utils


from whitebox.regdna import ELAttribute, ELClass, ELEnum, ELOperation, ELReference 
from whitebox.regdna import SelectColumnAttributeAs,RowColumnBasedReport,CellBasedReport

class PersistToFile:
    '''
    Documentation for PersistToFile
    '''

    def save_model_as_regdna_file(self, context):
        '''
        Save resources as regdna files
        '''

        
        PersistToFile.persist_entity_model(
                self, context, context.ldm_entities_package,
                "regdna", context.ldm_domains_package)

        PersistToFile.persist_entity_model(
            self, context, context.finrep_output_tables_package,
            "regdna", context.ldm_domains_package)
        
        PersistToFile.persist_entity_model(
            self, context, context.ae_output_tables_package,
            "regdna", context.ldm_domains_package)
        

        PersistToFile.persist_enum_model(
            self, context, context.ldm_domains_package, "regdna")

        
        PersistToFile.persist_types_model(
            self, context, context.types_package, "regdna")
        

    def persist_entity_model(self, context, the_package, extension, imported_package):
        '''
        Documentation for persist_entity_model
        '''

        f = open(context.output_directory + os.sep + extension +
                 os.sep + the_package.name + '.' + extension,
                 "a",  encoding='utf-8')
        f.write("\t\t package " + the_package.name + "\r")
        f.write("\t\t import " + imported_package.name + ".*\r")
        
        if extension == "regdna":
            f.write("\t\t import types.*\r")

        for annotationDirective in the_package.annotationDirectives:
            f.write("\t\t annotation \"" + annotationDirective.sourceURI + "\" as " +
                    annotationDirective.name + "\r")

        for classifier in the_package.eClassifiers:
            if isinstance(classifier, ELClass):
                for annotation in classifier.eAnnotations:
                    f.write("\t\t\t@")
                    
                    f.write(annotation.source.name)
                    f.write("(")
                    first_item = True
                    for detail in annotation.details:
                        if first_item:
                            first_item = False
                            f.write(detail.key + "=\"" + detail.value + "\"")
                        else:
                            f.write("," + detail.key + "=\"" + detail.value + "\"" )
                    f.write(")\r")
                
                f.write("\t\t\t")
                if classifier.eAbstract:
                    f.write("abstract ")
                f.write("class " + classifier.name)
                if (hasattr(classifier, "eSuperTypes") and len(classifier.eSuperTypes) > 0):
                    f.write(" extends " + classifier.eSuperTypes[0].name)
                f.write(" {\r")
                for member in classifier.eStructuralFeatures:
                    for annotation in member.eAnnotations:
                        f.write("\t\t\t\t@")

                        f.write(annotation.source.name)
                        f.write("(")
                        first_item = True
                        for detail in annotation.details:
                            if first_item:
                                first_item = False
                                f.write(detail.key + "=\"" + detail.value + "\"")
                            else:
                                f.write("," + detail.key + "=\"" + detail.value + "\"" )
                        f.write(")\r")
                    if isinstance(member, ELReference):
                        if member.containment:
                            f.write("\t\t\t\tcontains ")
                        else:
                            f.write("\t\t\t\trefers ")

                        f.write(member.eType.name + " ")
                        if ((member.lowerBound == 0) and (member.upperBound == 1)):
                            f.write(" ")
                        else:
                            upperBoundNumber = member.upperBound
                            upperBoundString = str(member.upperBound)
                            
                            
                            f.write("[" + str(member.lowerBound) +
                                    ".." + upperBoundString + "] ")

                        f.write(member.name)
                        if not(member.eOpposite is None):
                            f.write(" opposite " + member.eOpposite.eContainer().name + "." + member.eOpposite.name)
                        f.write(" \r")
                    elif isinstance(member, ELAttribute):
                        f.write("\t\t\t\t")
                        if member.iD:
                            f.write("id ")

                        if member.eType.name == "String":
                            f.write("String  ")
                        elif member.eType.name == "Double":
                            f.write("double  ")
                        elif member.eType.name == "Integer":
                            f.write("int  ")
                        elif member.eType.name == "Date":
                            f.write("Date  ")
                        else:
                            f.write(member.eType.name + " ")

                        if ((member.lowerBound == 0) and (member.upperBound == 1)):
                            f.write(" ")
                        else:
                            upperBoundNumber = member.upperBound
                            upperBoundString = str(member.upperBound)
                           
                            f.write("[" + str(member.lowerBound) +
                                    ".." + upperBoundString + "] ")

                        f.write(member.name)
                        f.write(" \r")

                for member in classifier.eOperations:
                    for annotation in member.eAnnotations:
                        f.write("\t\t\t\t@")
                        f.write(annotation.source.name)
                        f.write("(")
                        first_item = True
                        for detail in annotation.details:
                            if first_item:
                                first_item = False
                                f.write(detail.key + "=\"" + detail.value + "\"")
                            else:
                                f.write("," + detail.key + "=\"" + detail.value + "\"")
                        f.write(")\r")
                    if isinstance(member, ELOperation):
                        f.write("\t\t\t\top ")
                        f.write(member.eType.name + " ")
                        if ((member.lowerBound == 0) and (member.upperBound == 1)):
                            f.write(" ")
                        else:
                            upperBoundNumber = member.upperBound
                            upperBoundString = str(member.upperBound)                            
                            
                            f.write("[" + str(member.lowerBound) +
                                    ".." + upperBoundString + "] ")
                            
                        f.write(member.name)
                        f.write("() {}")
                            
                        f.write(" \r")

                f.write("\t\t\t}\r")

        f.close()

    def persist_types_model(self, context, the_package, extension):
        '''
        Documentation for persist_types_model
        '''
        f = open(context.output_directory + os.sep + extension +
                 os.sep + the_package.name +
                 '.' + extension, "a",  encoding='utf-8')
        f.write("\t\t package " + the_package.name + "\r")
        f.write("\t\t\ttype double wraps double\r")
        f.write("\t\t\ttype String wraps String\r")
        f.write("\t\t\ttype int wraps int\r")
        if extension == "regdna":
            f.write("\t\t\ttype Date wraps Date\r")
        else:
            f.write("\t\t\ttype Date wraps java.util.Date\r")
        f.write("\t\t\ttype boolean wraps boolean\r")
        f.close()

    def persist_enum_model(self, context, the_package, extension):
        '''
        Documentation for persist_enum_model
        '''
        f = open(context.output_directory + os.sep + extension +
                 os.sep + the_package.name +
                 '.' + extension, "a",  encoding='utf-8')
        f.write("\t\t package " + the_package.name + "\r")
        for classifier in the_package.eClassifiers:

            if isinstance(classifier, ELEnum):
                f.write("\t\t\tenum " + classifier.name)

                f.write(" { ")
                counter = 0
                splitcount = 1
                for the_literal in classifier.eLiterals:
                    counter = counter+1

                    if counter < 1500:
                        f.write(" " + the_literal.name + " as \"" +
                                the_literal.literal + "\" = " + str(the_literal.value))
                    else:
                        counter = 0
                        splitcount = splitcount+1
                        f.write(" }\r")
                        f.write("\t\t\tenum " + classifier.name +
                                "_" + str(splitcount))
                        f.write(" { ")

                f.write(" }\r")

        f.close()

    
    
    
    
    def persist_generation_transformations_to_csv(self, context):

        PersistToFile.persist_generation_transformations_to_csv_for_module(self, context,
                                                      context.finrep_generation_rules_module_il)
        PersistToFile.persist_generation_transformations_to_csv_for_module(self, context,
                                                      context.ae_generation_rules_module_il)
        PersistToFile.persist_generation_transformations_to_csv_for_module(self, context,
                                                      context.finrep_generation_rules_module_ldm) 
        PersistToFile.persist_generation_transformations_to_csv_for_module(self, context,
                                                      context.ae_generation_rules_module_ldm) 
             
    
    def persist_generation_transformations_to_csv_for_module(self, context,module):
        '''
        Documentation for persist_generation_transformations
        '''
        rules_for_reports = module.rulesForReport
        report_to_table_parts_file = open(context.output_directory + os.sep + 'generations_transformations_csv' +
                         os.sep + module.name + os.sep +
                         'report_to_table_parts.csv', "a",  encoding='utf-8')
        report_to_table_parts_file.write("Report,Table Part,Notes\n")

        for rules_for_report in rules_for_reports:
            if not(rules_for_report.outputLayerCube is None): #column.attribute.eContainer().name + "." + column.attribute.name
                
                template = rules_for_report.outputLayerCube.name
                amended_template_name =  template[0:len(template) - 11]
                f = open(context.output_directory + os.sep + 'generations_transformations_csv' +
                         os.sep + module.name + os.sep + 
                         amended_template_name + '.csv', "a",  encoding='utf-8')
                f.write("Template,Table Part,Main Table,Filter,Lineage type,Source Table,Source Column,Missing,Relevant, Derived,Domain,Member,Value,ROL Cube Item,Notes\n")

                for layer in rules_for_report.rulesForTable:
                    if not(layer.inputLayerTable is None):
                        table = layer.inputLayerTable.name
                    else:
                        print ("no input layer table for " + rules_for_report.outputLayerCube.name  )
                        table = "Null"
                    for table_part in layer.rulesForTablePart:
                        main_catagory = table_part.main_catagory
                        main_catagory_name = 'None'
                        if module.name == "finrep_generation_rules":
                            main_catagory_name = context.main_catogory_to_name_map_finrep[main_catagory]
                        elif module.name == "ae_generation_rules":
                            main_catagory_name = context.main_catogory_to_name_map_ae[main_catagory]
                        
                        table_and_part = table_part.table_and_part_tuple
                        report_to_table_parts_file.write(amended_template_name + "," + table_part.name + ",\n")
                        filter = ''
                        try:
                            filter = context.table_parts_to_to_filter_map[table_and_part]
                        except:
                            filter = ''
                        for column in table_part.columns:
                            if isinstance(column, SelectColumnAttributeAs) and not(column.attribute is None):
                                entity  = column.attribute.eContainer().name
                                attribute = column.attribute.name
                                lineage_type = "attribute"
                                missing = "Not Missing"
                            else:  
                                entity  = ""
                                attribute = ""
                                missing = "Missing"
                                lineage_type = "tbd"
                                
                            variable_id = column.asAttribute.name

                            
                            f.write(amended_template_name +"," + table_part.name +","  +table+"," + filter + ","  +lineage_type+"," +entity+"," +attribute+"," +missing+",,,,,," +variable_id + ",\n")
                
                f.close()
        report_to_table_parts_file.close
        f = open(context.output_directory + os.sep + 'generations_transformations_csv' +
                        os.sep + module.name + os.sep + 
                        'generation_rules_summary.csv', "a",  encoding='utf-8')
        f.write("Key,Table Part, Main Table, ROL cube Item, Source Table,Source Column,ROL Cube Item,Notes\n")

        for key,values in context.table_part_varaible_transformation_map.items():
            
            for value in values:
                column_name = 'None'
                table_name= 'None'
                
                if not(value is None):
                    table_name = value.eContainer().name
                    column_name = value.name
                    
                f.write(key +"," + table_name +","  + column_name +",\n")
                
            
        f.close()
            

    def persist_generation_transformations(self, context):
        PersistToFile.persist_generation_transformations_for_module(self, context,
                                                      context.finrep_generation_rules_module_il)
        PersistToFile.persist_generation_transformations_for_module(self, context,
                                                      context.ae_generation_rules_module_il)
        PersistToFile.persist_generation_transformations_for_module(self, context,
                                                      context.finrep_generation_rules_module_ldm)
        PersistToFile.persist_generation_transformations_for_module(self, context,
                                                      context.ae_generation_rules_module_ldm)  
          
     
    def persist_generation_transformations_for_module(self, context,
                                                      module):
        '''
        Documentation for persist_generation_transformations
        '''
        
        for rules_for_report in module.rulesForReport:
            if not(rules_for_report.outputLayerCube is None):
                f = open(context.output_directory + os.sep + 'regdna' +
                         os.sep +module.name + os.sep + rules_for_report.outputLayerCube.name +
                         '.regdna', "a",  encoding='utf-8')
                f.write("generationRuleModule " + rules_for_report.outputLayerCube.name + "_generationModule\r{\r")
                f.write("\tgenerationRules " + "{\r")
                f.write("\t\toutputLayerEntity output_tables." + rules_for_report.outputLayerCube.name + "{\r")
                for layer in rules_for_report.rulesForTable:
                    if layer.inputLayerTable is None:
                        f.write("\t\t\tmainEntity None {\r")
                    else:
                        f.write("\t\t\tmainEntity input_tables." + layer.inputLayerTable.name + "{\r")
                    for table_part in layer.rulesForTablePart:
                        if table_part.name is None:
                            f.write("\t\t\t\tinputSlice None { \r")
                        else:
                            f.write("\t\t\t\tinputSlice " + Utils.make_valid_id(table_part.name) + " { \r")
                        for column in table_part.columns:
                            if isinstance(column, SelectColumnAttributeAs) and not(column.attribute is None):
                                f.write("\t\t\t\t\tselectAttribute input_tables." + column.attribute.eContainer().name + "." + column.attribute.name)
                            else:  
                                f.write("\t\t\t\t\tselectValue \"TODO\"")
                            f.write(" as output_tables." +
                                    rules_for_report.outputLayerCube.name + "." + column.asAttribute.name + "\r")
                        f.write("\t\t\t\t}\r")
                        
                    f.write("\t\t\t}\r")
                f.write("\t\t}\r")
                f.write("\t}\r")
                f.write("}\r")
               
                f.close()
    
    def persist_cell_based_reports(self, context):        
        PersistToFile.persist_cell_based_reports_for_module(self,context,context.finrep_on_ldm_reports_module)
        PersistToFile.persist_cell_based_reports_for_module(self,context,context.ae_on_ldm_reports_module)

    def persist_row_column_based_reports(self, context):

        PersistToFile.persist_row_column_based_reports_module(self,context,context.finrep_on_sdd_reports_module)
        PersistToFile.persist_row_column_based_reports_module(self,context,context.ae_on_sdd_reports_module)
        PersistToFile.persist_row_column_based_reports_module(self,context,context.finrep_on_ldm_reports_module)
        PersistToFile.persist_row_column_based_reports_module(self,context,context.ae_on_ldm_reports_module)
        PersistToFile.persist_row_column_based_reports_module(self,context,context.finrep_on_il_reports_module)
        PersistToFile.persist_row_column_based_reports_module(self,context,context.ae_on_il_reports_module)
        
    def persist_cell_based_reports_for_module(self, context,module):
        '''
        Documentation for persist_generation_transformations
        '''
        reports = module.reports
        for report in reports:
            if isinstance(report, CellBasedReport): 
                if not(report.outputLayer is None):
                    f = open(context.output_directory + os.sep + 'regdna' +
                             os.sep + module.name + os.sep + report.outputLayer.name +
                             '.regdna', "a",  encoding='utf-8')
                    f.write("ReportModule " + report.outputLayer.name + "_reportModule\r{\r")
                    f.write("\treports " + "{\r")
                    f.write("\t\tCellBasedReport " + "{\r")
                    f.write("\t\t\toutputLayer output_tables." + report.outputLayer.name + "\r")
                    f.write("\t\t\trows{\r")
                    for row in report.rows:                    
                        f.write("\t\t\t\tReportRow " + row.name + "\r")
                    f.write("\t\t\t}\r")
                    f.write("\t\t\tcolumns{\r")
                    for col in report.columns:                    
                        f.write("\t\t\tReportColumn " + col.name + "\r")
                    f.write("\t\t\t}\r")
                    f.write("\t\t\treportCells{\r")  
                    for cell in report.reportCells:                    
                        f.write("\t\t\t\tReportCell{\r")
                        
                        data_point_id = "None"
                        if not (cell.datapointID is None):
                            data_point_id= cell.datapointID
                            
                        row_name = "None"
                        if not (cell.row is None):
                            row_name= cell.row.name
                            
                        col_name = "None"
                        if not (cell.column is None):
                            col_name= cell.column.name
    
                        metric_name = "None"
                        if not (cell.metric is None):
                            metric_name = "output_tables." + cell.metric.eContainer().name + "." + cell.metric.name
                            
                        
                        
                        f.write("\t\t\t\t\tdatapointID \"" + data_point_id + "\" row " + row_name + " column " + col_name  + " metric " + metric_name+ " filters {\r")
                        for filter in cell.filters:
                            operation_name = "none"
                            if not(filter.operation is None):
                                operation_name = "output_tables." +filter.operation.eContainer().name + "." + filter.operation.name
                            f.write("\t\t\t\t\t\tFilter {operation " + operation_name + "  item ( ")
                            for item in filter.member:
                                f.write("sdd_domains." + item.eContainer().name + "." + item.name + " " ) 
                            f.write(")\r")   
                            f.write("\t\t\t\t\t\t}\r") 
                        f.write("\t\t\t\t\t}\r")  
                        f.write("\t\t\t}\r")
                    f.write("\t\t}\r")
                    f.write("\t}\r")
                    f.write("}\r")
                    f.close()
                
    def persist_row_column_based_reports_module(self, context,module):
        '''
        Documentation for persist_generation_transformations
        '''
        reports = module.reports
        for report in reports:
            if isinstance(report, RowColumnBasedReport): 
                if not(report.outputLayer is None):
                    f = open(context.output_directory + os.sep + 'regdna' +
                             os.sep + module.name + os.sep + report.outputLayer.name +
                             '_row_column.regdna', "a",  encoding='utf-8')
                    f.write("ReportModule " + report.outputLayer.name + "_reportModule\r{\r")
                    f.write("\treports " + "{\r")
                    f.write("\t\tRowColumnBasedReport " + "{\r")
                    f.write("\t\t\toutputLayer output_tables." + report.outputLayer.name + "\r")
                    f.write("\t\t\trows{\r")
                    for row in report.rows:                    
                        f.write("\t\t\t\tReportRow " + row.name + "\r")
                    f.write("\t\t\t}\r")
                    f.write("\t\t\tcolumns{\r")
                    for col in report.columns:                    
                        f.write("\t\t\tReportColumn " + col.name + "\r")
                    f.write("\t\t\t}\r")
                    f.write("\t\t\twholeReportFilters{\r")  
                    if not(report.wholeReportFilters is None):                    
                        f.write("\t\t\t\tWholeReportFilters{\r")
                        if len(report.wholeReportFilters.filters) > 0:
                            f.write("\t\t\t\t\tfilters {\r")
                            for filter in report.wholeReportFilters.filters:
                                operation_name = "none"
                                if not(filter.operation is None):
                                    operation_name = "output_tables." +filter.operation.eContainer().name + "." + filter.operation.name
                                f.write("\t\t\t\t\t\tFilter {operation " + operation_name + "  item ( ")
                                for item in filter.member:
                                    f.write("sdd_domains." + item.eContainer().name + "." + item.name + " " ) 
                                f.write(")\r")   
                                f.write("\t\t\t\t\t\t}\r") 
                            f.write("\t\t\t\t\t}\r")  
                        f.write("\t\t\t\t}\r")
                    f.write("\t\t\t}\r")
                    f.write("\t\t\trowFilters{\r")  
                    for row_filter in report.rowFilters:                    
                        f.write("\t\t\t\tRowFilters{\r")
      
                        row_name = "None"
                        if not (row_filter.row is None):
                            row_name= row_filter.row.name
    
                        metric_name = "None"
                        if not (row_filter.metric is None):
                            metric_name = "output_tables." + row_filter.metric.eContainer().name + "." + row_filter.metric.name
    
                        f.write("\t\t\t\t\trow " + row_name + "\r")
                        if not(metric_name == "None"):
                            f.write("\t\t\t\t\tmetric " + metric_name+ "\r")
                        if len(row_filter.filters) > 0:
                            f.write("\t\t\t\t\tfilters {\r")
                            for filter in row_filter.filters:
                                operation_name = "none"
                                if not(filter.operation is None):
                                    operation_name = "output_tables." +filter.operation.eContainer().name + "." + filter.operation.name
                                f.write("\t\t\t\t\t\tFilter {operation " + operation_name + "  item ( ")
                                for item in filter.member:
                                    f.write("sdd_domains." + item.eContainer().name + "." + item.name + " " ) 
                                f.write(")\r")   
                                f.write("\t\t\t\t\t\t}\r") 
                            f.write("\t\t\t\t\t}\r")  
                        f.write("\t\t\t\t}\r")
                    f.write("\t\t\t}\r")
                    f.write("\t\t\tcolumnFilters{\r")  
                    for column_filter in report.columnFilters:                    
                        f.write("\t\t\t\tColumnFilters{\r")
      
                        column_name = "None"
                        if not (column_filter.column is None):
                            column_name= column_filter.column.name
    
                        metric_name = "None"
                        if not (column_filter.metric is None):
                            metric_name = "output_tables." + column_filter.metric.eContainer().name + "." + column_filter.metric.name
    
                        f.write("\t\t\t\t\tcolumn " + column_name + "\r")
                        if not(metric_name == "None"):
                            f.write("\t\t\t\t\tmetric " + metric_name+ "\r")
                        if len(column_filter.filters) > 0:
                            f.write("\t\t\t\t\tfilters {\r")
                            for filter in column_filter.filters:
                                operation_name = "none"
                                if not(filter.operation is None):
                                    operation_name = "output_tables." +filter.operation.eContainer().name + "." + filter.operation.name
                                f.write("\t\t\t\t\t\tFilter {operation " + operation_name + "  item ( ")
                                for item in filter.member:
                                    f.write("sdd_domains." + item.eContainer().name + "." + item.name + " " ) 
                                f.write(")\r")   
                                f.write("\t\t\t\t\t\t}\r") 
                            f.write("\t\t\t\t\t}\r")  
                        f.write("\t\t\t\t}\r")
                    f.write("\t\t\t}\r")
                    f.write("\t\t}\r")
                    f.write("\t}\r")
                    f.write("}\r")
                    f.close()


    def create_example_reports(self, context):
        '''
        Documentation for create_example_reports
        '''
        reports = context.finrep_on_il_reports_module.reports
        for report in reports:
            
            if not(report.outputLayer is None):
                f = open(context.output_directory + os.sep + 'csv' +
                         os.sep + report.outputLayer.name +
                         '.csv', "a",  encoding='utf-8')
                f.write("report,datapoint,row,column,sheet,value")
                 
                for cell in report.reportCells:
                    report_name = "None"
                    if not (report.outputLayer is None):
                        report_name= report.outputLayer.name
                    
                    data_point_id = "None"
                    if not (cell.datapointID is None):
                        data_point_id= cell.datapointID
                        
                    row_name = "None"
                    if not (cell.row is None):
                        row_name= cell.row.name
                        
                    col_name = "None"
                    if not (cell.column is None):
                        col_name= cell.column.name

                    f.write(report_name + "," + data_point_id + "," + row_name + "," + col_name + ",,100\r")                                       
                f.close()
                
    def remove_comment_chars(self, the_string):
        '''
        Documentation for remove_comment_chars
        '''
        return the_string.replace("/**", "").replace("/*", "").replace("*/", "")
