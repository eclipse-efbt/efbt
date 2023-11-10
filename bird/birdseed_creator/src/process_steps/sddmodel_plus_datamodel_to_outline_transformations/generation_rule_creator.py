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
from regdna import RulesForILTable,  SelectColumnAttributeAs , ELPublicOperation
from regdna import RuleForILTablePart , RulesForReport, ELClass, ELEnum, ELAttribute


class GenerationRuleCreator(object):
    '''
    GenerationRuleCreator class is responsible for the generation 
    of generation rules
    '''

    def generate_generation_rules(self, context):
        '''
        generate generation rules
        '''
        GenerationRuleCreator.add_reports(self, context)
        
    def add_reports(self, context):
        '''
        Create the generation rule for each report that is in scope.
        '''
        file_location = context.file_directory + os.sep + "in_scope_reports.csv"

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
                    rules_for_report = RulesForReport()
                    context.generation_rules_module.rulesForReport.append(
                                                            rules_for_report)
                    generated_output_layer = GenerationRuleCreator.\
                        find_output_layer_cube(self, context,
                                               report_template + "_REF_OutputItem")
                    if not (generated_output_layer is None):
                        context.generation_rules_module.rulesForReport.append(
                                                                rules_for_report)
                        rules_for_report.outputLayerCube = generated_output_layer
                        GenerationRuleCreator.add_table_parts(
                                                    self, context,
                                                    rules_for_report,report_template)
                        
    def add_table_parts(self, context, rules_for_report,report_template):
        '''
        For each report, check which main catagories are applicable 
        (e.g loans and advances)
        For each main catagory check what EIL tables are relevant
        (e.g. Instrument)
        For each of those tables create part of the generation transformation
        '''
        try:
            main_catagories = context.report_to_main_catogory_map[report_template]
        
            for mc in main_catagories:
                try:
                    tables = context.tables_for_main_catagory_map[mc]
                    for table in tables:
                
                        rules_for_table = RulesForILTable()
                        rules_for_table.inputLayerTable = GenerationRuleCreator.\
                                        find_input_layer_cube (self,context,table)
                        rules_for_report.rulesForTable.extend([rules_for_table])
                        table_parts = context.table_and_part_tuple_map[mc]

                        for table_part in table_parts:
                            input_entity_list = [rules_for_table.inputLayerTable]
                            # a table like instrument might have linked tables
                            # defined such as Party or Collateral
                            linked_tables = context.table_parts_to_linked_tables_map[table_part]
                            linked_tables_list = linked_tables.split(":")
                            for the_table in linked_tables_list:
                                the_input_table  = GenerationRuleCreator.\
                                                    find_input_layer_cube (
                                                    self,context,the_table)
                                if not (the_input_table is None):
                                    input_entity_list.append(the_input_table)

                            if table_part[0] == table:
                                rules_for_il_table_part = RuleForILTablePart()
                                rules_for_il_table_part.main_catagory = mc
                                rules_for_il_table_part.name = table_part[1]
                                rules_for_il_table_part.table_and_part_tuple = table_part
                                rules_for_table.rulesForTablePart.append(rules_for_il_table_part)
                                GenerationRuleCreator.\
                                    add_field_to_field_lineage_to_rules_for_table_part(
                                                    self, context, rules_for_il_table_part,
                                                     rules_for_report.outputLayerCube,
                                                     input_entity_list)
                except KeyError:
                    print ("no tables for main catagory:" + mc)

        except KeyError:
                    print ("no main catagory for report :" + report_template)

    def add_field_to_field_lineage_to_rules_for_table_part(self, context, rules_for_il_table_part, output_entity, input_entity_list):
        '''
        Add field to field lineage entries to the rules for the table part
        '''
        if not output_entity is None:
            for output_item in output_entity.eOperations:
                if isinstance(output_item, ELPublicOperation):
                    select_column = SelectColumnAttributeAs()
                    select_column.asAttribute = output_item
                    rules_for_il_table_part.columns.extend([select_column])
                    input_column = None
                    if context.match_domains_in_generation_file:
                        input_column = GenerationRuleCreator.\
                            find_variable_with_same_domain(
                            self,output_item,
                            input_entity_list)
                    else:
                        input_column = GenerationRuleCreator.\
                            find_related_variable( 
                            self,context,output_item,input_entity_list)

                    if not(input_column is None):
                        select_column.attribute = input_column


    def find_related_variable(self,context,output_item,input_entity_list):
        '''
        when we have an ROL item it has a specific domain.
        We want to find any column in the limited related input tables
        which has the same domain
        '''
        output_variable_name = output_item.name
           
        if not (output_variable_name is None):
            for input_entity in input_entity_list:
                if not (input_entity is None):
                    for input_item in input_entity.eStructuralFeatures:
                        if isinstance(input_item, ELAttribute):
                            input_item_name = input_item.name
                            if input_item_name == output_variable_name:
                                return input_item
                            try:
                                primary_concept = context.\
                                    variable_to_primary_concept_map[input_item_name]
                                if primary_concept == output_variable_name:
                                    return input_item
                            except KeyError:
                                pass
        return None
    
    def find_variable_with_same_domain(self,output_item,input_entity_list):
        '''
        when we have an ROL item it has a specific domain.
        We want to find any column in the limited related input tables
        which has the same domain
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
        Find the ELClass for the related output layer cube given the cube name
        '''
        for classifier in context.output_tables_package.eClassifiers:
            if isinstance(classifier, ELClass):
                if classifier.name == output_layer_name:
                    return classifier

    def find_input_layer_cube(self, context, input_layer_name):
        '''
        Find the ELClass for the related output layer cube given the cube name
        '''
        for classifier in context.input_tables_package.eClassifiers:
            if isinstance(classifier, ELClass):
                if classifier.name == input_layer_name:
                    return classifier
