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
from regdna import RulesForILTable,  SelectColumnAttributeAs , ELOperation
from regdna import RuleForILTablePart , RulesForReport, ELClass, ELEnum, ELAttribute


class GenerationRuleCreator(object):
    '''
    GenerationRuleCreator class is responsible for the generation 
    of generation rules
    '''

    def generate_generation_rules(self, context,sdd_context):
        '''
        generate generation rules
        '''
        GenerationRuleCreator.add_reports(self, context,sdd_context)
        
    def add_reports(self, context,sdd_context):
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
                                                    self, context,sdd_context,
                                                    rules_for_report,report_template)

    def add_table_parts(self, context, sdd_context, rules_for_report,report_template):

        if context.load_ldm:
            GenerationRuleCreator.add_table_parts_ldm(self, context, sdd_context, rules_for_report,report_template)  
        else:
            GenerationRuleCreator.add_table_parts_il(self, context, sdd_context, rules_for_report,report_template)    

    def add_table_parts_ldm(self, context, sdd_context, rules_for_report,report_template):
        try:
            typ_instrmnts = context.report_to_typ_instrmnt_map[report_template+'_REF']
            for typ_instrmnt in typ_instrmnts:
                try:
                    tables = context.entities_for_typ_instrmnt_map[typ_instrmnt]
                    for table in tables:
                
                        rules_for_table = RulesForILTable()
                        rules_for_table.inputLayerTable = GenerationRuleCreator.\
                                        find_input_layer_cube (self,context,table)
                        rules_for_report.rulesForTable.extend([rules_for_table])
                        table_parts = context.table_and_part_tuple_map[typ_instrmnt]
                        for table_part in table_parts:
                            input_entity_list = [rules_for_table.inputLayerTable]
                            # a table like instrument might have linked tables
                            # defined such as Party or Collateral
                            linked_tables = context.table_parts_to_linked_tables_map[table_part]
                            
                            linked_tables_list = linked_tables.split(":")
                            print("table_part[0]")
                            print(table_part[0])
                            try:
                                first_linked_table = context.ldm_entity_to_first_linked_entities_map[table_part[0]]
                                first_linked_tables = context.ldm_entity_to_linked_tables_map[first_linked_table]
                                linked_tables_list.append(first_linked_table)
                                linked_tables_list.extend(first_linked_tables.split(":"))
                            except KeyError:
                                pass
                            
                            for the_table in linked_tables_list:
                                the_input_table  = GenerationRuleCreator.\
                                                    find_input_layer_cube (
                                                    self,context,the_table)
                                if not (the_input_table is None):
                                    input_entity_list.append(the_input_table)

                            if table_part[0] == table:
                                rules_for_il_table_part = RuleForILTablePart()
                                rules_for_il_table_part.main_catagory = typ_instrmnt
                                rules_for_il_table_part.name = table_part[1]
                                rules_for_il_table_part.table_and_part_tuple = table_part
                                rules_for_table.rulesForTablePart.append(rules_for_il_table_part)
                                GenerationRuleCreator.\
                                    add_field_to_field_lineage_to_rules_for_table_part(
                                                    self, context,sdd_context, rules_for_il_table_part,
                                                     rules_for_report.outputLayerCube,
                                                     input_entity_list,typ_instrmnt,report_template)

                except KeyError:
                    print ("no tables for typ_instrmnt:" + typ_instrmnt)

        except KeyError:
                    print ("no typ_instrmnt for report :" + report_template)

            
    
    def add_table_parts_il(self, context, sdd_context, rules_for_report,report_template):
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
                                                    self, context,sdd_context, rules_for_il_table_part,
                                                     rules_for_report.outputLayerCube,
                                                     input_entity_list,mc,report_template)
                except KeyError:
                    print ("no tables for main catagory:" + mc)

        except KeyError:
                    print ("no main catagory for report :" + report_template)

    def add_field_to_field_lineage_to_rules_for_table_part(self, context,sdd_context, rules_for_il_table_part, output_entity, input_entity_list,mc,report_template):
        '''
        Add field to field lineage entries to the rules for the table part
        '''
        if not output_entity is None:
            for output_item in output_entity.eOperations:
                if isinstance(output_item, ELOperation):
                    if GenerationRuleCreator.operation_exists_in_combination_for_report_and_typ_instrmnt(self,sdd_context,mc,output_item,report_template):                    
                        if context.match_domains_in_generation_file:
                            input_columns = GenerationRuleCreator.\
                                find_variables_with_same_domain_then_name(
                                self,sdd_context,output_item,
                                input_entity_list)
                        else:
                            input_columns = GenerationRuleCreator.\
                                find_related_variables( 
                                self,context,sdd_context,output_item,input_entity_list)

                        if len(input_columns) == 0:
                            select_column = SelectColumnAttributeAs()
                            select_column.asAttribute = output_item
                            rules_for_il_table_part.columns.extend([select_column])
                        else:                        
                            for input_column in input_columns:
                                select_column = SelectColumnAttributeAs()
                                select_column.asAttribute = output_item
                                select_column.attribute = input_column
                                rules_for_il_table_part.columns.extend([select_column])
                                
                                key= rules_for_il_table_part.name + ":" + rules_for_il_table_part.table_and_part_tuple[0] + ":" + output_item.name + "," + rules_for_il_table_part.name + "," + rules_for_il_table_part.table_and_part_tuple[0] + "," + output_item.name
                                try:
                                    values = context.table_part_varaible_transformation_map[key]
                                    values.append(input_column)
                                except KeyError:
                                    values = []
                                    values.append(input_column)
                                    context.table_part_varaible_transformation_map[key] = values
                                    
    def operation_exists_in_combination_for_report_and_typ_instrmnt(self,sdd_context,typ_instrmt,output_item,report_template):
        '''
        Check if the operation exists in the combination of the report and typ_instrmnt
        '''
        try:
            combinations_for_typ_instrmnt = sdd_context.combination_to_typ_instrmnt_map[typ_instrmt]
            combinations_for_report = sdd_context.table_to_combination_dictionary[report_template+'_REF']
            for combination in combinations_for_report:
                if combination in combinations_for_typ_instrmnt:
                    if not (combination is None):
                        for item in combination.combination_items:
                            if not (item.variable_id is None):
                                if item.variable_id.variable_id == output_item.name:
                                    return True

            return False
        except KeyError:
            return False
        

    def operation_exists_in_combination_for_report_and_typ_instrmnt(self,sdd_context,typ_instrmt,output_item,report_template):
        '''
        Check if the operation exists in the combination of the report and typ_instrmnt
        '''
        try:
            combinations_for_typ_instrmnt = sdd_context.combination_to_typ_instrmnt_map[typ_instrmt]
            combinations_for_report = sdd_context.table_to_combination_dictionary[report_template+'_REF']
            for combination in combinations_for_report:
                if combination in combinations_for_typ_instrmnt:
                    if not (combination is None):
                        for item in combination.combination_items:
                            if not (item.variable_id is None):
                                if item.variable_id.variable_id == output_item.name:
                                    return True

            return False
        except KeyError:
            return False

    def find_related_variables(self,context,sdd_context,output_item,input_entity_list):
        '''
        when we have an ROL item it has a specific domain.
        We want to find any column in the limited related input tables
        which has the same domain
        '''
        output_variable_name = output_item.name
        related_variables = []   
        if not (output_variable_name is None):
            for input_entity in input_entity_list:
                if not (input_entity is None):
                    for input_item in input_entity.eStructuralFeatures:
                        if isinstance(input_item, ELAttribute):
                            input_item_name = input_item.name
                            if input_item_name == output_variable_name:
                                related_variables= []
                                related_variables.append(input_item)
                            try:
                                primary_concept = sdd_context.\
                                    variable_to_primary_concept_map[input_item_name]
                                if primary_concept == output_variable_name:
                                    related_variables= []
                                    related_variables.append(input_item)
                            except KeyError:
                                pass
        return related_variables
    
    def find_variables_with_same_domain(self,output_item,input_entity_list):
        '''
        when we have an ROL item it has a specific domain.
        We want to find any column in the limited related input tables
        which has the same domain
        '''
        related_variables = [] 
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
                                    if not (input_item in related_variables):
                                        related_variables.append(input_item)
        return related_variables
    
    def find_variables_with_same_domain_then_name(self,sdd_context,output_item,input_entity_list):
        '''
        when we have an ROL item it has a specific domain.
        We want to find any column in the limited related input tables
        which has the same domain
        '''
        related_variables = []   
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
                                    if not (input_item in related_variables):
                                        related_variables.append(input_item)
        else:
            output_variable_name = output_item.name
           
            if not (output_variable_name is None):
                for input_entity in input_entity_list:
                    if not (input_entity is None):
                        for input_item in input_entity.eStructuralFeatures:
                            if isinstance(input_item, ELAttribute):
                                input_item_name = input_item.name
                                if input_item_name == output_variable_name:
                                    if not (input_item in related_variables):
                                        related_variables.append(input_item)
                                else:
                                    try:
                                        primary_concept = sdd_context.\
                                            variable_to_primary_concept_map[input_item_name]
                                        if primary_concept == output_variable_name:
                                            if not (input_item in related_variables):
                                                related_variables.append(input_item)
                                    except KeyError:
                                        pass

        return related_variables

    def find_output_layer_cube(self, context, output_layer_name):
        '''
        Find the ELClass for the related output layer cube given the cube name
        '''
        for classifier in context.finrep_output_tables_package.eClassifiers:
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
