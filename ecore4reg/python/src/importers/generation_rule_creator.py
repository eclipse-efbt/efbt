# coding=UTF-8#
# Copyright (c) 2020 Bird Software Solutions Ltd
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDE-License-Identifier: EPL-2.0
#
# Contributors:
#    Neil Mackenzie - initial API and implementation
#


'''
@author: Neil
'''
import csv
import os
from ecore4reg import RulesForILTable,  SelectColumnAttributeAs , ELOperation
from ecore4reg import RuleForILTablePart , RulesForReport, ELClass, ELEnum, ELAttribute


class GenerationRuleCreator(object):
    '''
    GenerationRuleCreator class is responsable for the generation 
    of generation rules
    '''

    def generate_generation_rules(self, context):
        '''
        generate generation rules
        '''
        GenerationRuleCreator.add_reports(self, context)
        
    def add_reports(self, context):
        '''
        Doc for addReports
        '''
        file_location = context.file_directory + os.sep + "in_scope_reports.csv"

        header_skipped = False
        # Load all the entities from the csv file, make an ELClass per entity,
        # and add the ELClass to the package
        with open(file_location,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                # skip the first line which is the header.
                if (not header_skipped):
                    header_skipped = True
                else:
                    report_template = row[0]
                    view = RulesForReport()
                    context.view_module.views.append(view)
                    print("report_template")
                    print(report_template)
                    generated_output_layer = GenerationRuleCreator.find_output_layer_cube(self, context, report_template + "_REF_OutputItem")
                    if not (generated_output_layer is None):
                        context.view_module.views.append(view)
                        view.outputLayerCube = generated_output_layer
                        GenerationRuleCreator.add_table_parts(self, context, view,report_template)
                        
    def add_table_parts(self, context, view,report_template):
        '''
        Doc for addLayers
        '''
        try:
            main_catagories = context.report_to_main_catogory_map[report_template]
        
            for mc in main_catagories:
                try:
                    tables = context.tables_for_main_catagory_map[mc]
                    for table in tables:
                
                        layer_sql = RulesForILTable()
                        print("table")
                        print(table)
                        layer_sql.inputLayerTable = GenerationRuleCreator.find_input_layer_cube (self,context,table)
                        view.rulesForTable.extend([layer_sql])
                        table_parts = context.table_and_part_tuple_map[mc]
        
                        
                        
                        for table_part in table_parts:
                            
                            # vtlLayer = intermediateLayer.transformations
                            input_entity_list = [layer_sql.inputLayerTable]
                            linked_tables = context.table_parts_to_linked_tables_map[table_part]
                            #table_part.linked_tables = linked_tables
                            linked_tables_list = linked_tables.split(":")
                            for the_table in linked_tables_list:
                                the_input_table  = GenerationRuleCreator.find_input_layer_cube (self,context,the_table)
                                if not (the_input_table is None):
                                    input_entity_list.append(the_input_table)

                            if table_part[0] == table:
                                selection_layer = RuleForILTablePart()
                                selection_layer.main_catagory = mc
                                selection_layer.name = table + "_" + context.main_catogory_to_name_map[mc]
                                selection_layer.table_and_part_tuple = table_part
                                layer_sql.rulesForTablePart.append(selection_layer)
                                GenerationRuleCreator.add_columns_to_layer(self, context, selection_layer, view.outputLayerCube, input_entity_list)
                except KeyError:
                    print ("no tables for main catagory:" + mc)
                    
        except KeyError:
                    print ("no main catagory for report :" + report_template)    
        
    def add_columns_to_layer(self, context, selection_layer, output_entity, input_entity_list):
        '''
        Doc for addColumnsToLayer
        '''

        
        if not output_entity is None:
            for output_item in output_entity.eOperations:
                if isinstance(output_item, ELOperation):
                    select_column = SelectColumnAttributeAs()
                    select_column.asAttribute = output_item
                    selection_layer.columns.extend([select_column])
                    inputColumn = GenerationRuleCreator.findVariableWithSameDomain(self,context,output_item,input_entity_list)
                    if not(inputColumn is None):
                        select_column.attribute = inputColumn
                        
                    
    def findVariableWithSameDomain(self,context,output_item,input_entity_list):
        '''
        
        '''  
        target_domain = None       
        etype = output_item.eType
        if isinstance(etype, ELEnum):
            target_domain = etype
           
        if not (target_domain is None): 
            
            for input_entity in input_entity_list:
                if not (input_entity is None):    
                    for input_item in input_entity.eStructuralFeatures:
                        if isinstance(input_item, ELAttribute):
                            input_item_etype = input_item.eAttributeType
                            if isinstance(etype, ELEnum):
                                if input_item_etype == target_domain:
                                    return input_item
        return None
        
                                            
    def find_output_layer_cube(self, context, output_layer_name):
        '''
        
        '''
        

        for classifier in context.output_tables_package.eClassifiers:
            if isinstance(classifier, ELClass):
                if classifier.name == output_layer_name:
                    return classifier
                
    def find_input_layer_cube(self, context, input_layer_name):
        '''
        Doc for findInputLayerVTL
        '''

        for classifier in context.input_tables_package.eClassifiers:
            if isinstance(classifier, ELClass):
                if classifier.name == input_layer_name:
                    return classifier
                
                
                
   
        
    
        
                