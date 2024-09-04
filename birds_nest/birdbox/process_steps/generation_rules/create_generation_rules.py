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
from django.apps import apps

from django.db.models.fields import CharField,DateTimeField,BooleanField,FloatField,BigIntegerField


import os
import csv

from birdbox.process_steps.generation_rules.ldm_search import ELDMSearch

class GenerationRuleCreator(object):
    '''
    Documentation for CombinationsToReportFilters
    '''
    
    def generate_generation_rules(self, context,sdd_context, framework):
        '''
        generate generation rules
        '''
        GenerationRuleCreator.add_reports(self, context,sdd_context, framework)
    
    def add_reports(self, context,sdd_context, framework):
        '''
        Create the generation rule for each report that is in scope.
        '''
        file_location = context.file_directory + os.sep + "in_scope_reports_" + framework + ".csv"

        GenerationRuleCreator.create_ldm_entity_to_linked_entities_map(self, context, sdd_context)
        

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

                    generated_output_layer = GenerationRuleCreator.\
                        find_output_layer_cube(self, sdd_context,
                                               report_template,framework)
                    if not (generated_output_layer is None):
                        GenerationRuleCreator.add_table_parts(
                                                    self, context,sdd_context,
                                                    generated_output_layer,
                                                    framework)

    def create_ldm_entity_to_linked_entities_map(self, context, sdd_context):
        '''
        '''
        extension = "csv"
        f = open(context.output_directory + os.sep + extension +
                 os.sep + "ldm_entity_related_entities" + '.' + extension,
                 "a",  encoding='utf-8')
        f.write("ldm_entity, related_entites\r")
        model_list = apps.get_models()
        for model in model_list:
            print(f"{model._meta.app_label}  -> {model.__name__}")
            if model._meta.app_label == 'birdbox':

                entities = ELDMSearch.get_all_related_entities(self, context, model)
                related_entities_string = ""
                first = True
                for entity in entities:
                    if not(first):
                        related_entities_string = related_entities_string + ":"
                    first = False
                    related_entities_string = related_entities_string + entity.__name__

                f.write(model.__name__ + "," + related_entities_string + "\r")
                context.ldm_entity_to_linked_tables_map[model.__name__] = related_entities_string
        f.close()


    def add_table_parts(self, context, sdd_context, generated_output_layer,framework):

        GenerationRuleCreator.add_table_parts_il(self, context, sdd_context,
                                                 generated_output_layer,framework) 
        

    def add_table_parts_il(self, context, sdd_context,
                            generated_output_layer,framework):
        '''
        For each report, check which main catagories are applicable 
        (e.g loans and advances)
        For each main catagory check what EIL tables are relevant
        (e.g. Instrument)
        For each of those tables create part of the generation transformation
        '''
        if framework == "FINREP_REF":
            tables_for_main_catagory_map = context.tables_for_main_catagory_map_finrep
            table_parts_to_linked_tables_map = context.table_parts_to_linked_tables_map_finrep
            table_and_part_tuple_map = context.table_and_part_tuple_map_finrep
        elif framework == "AE_REF":
            tables_for_main_catagory_map = context.tables_for_main_catagory_map_ae
            table_parts_to_linked_tables_map = context.table_parts_to_linked_tables_map_ae
            table_and_part_tuple_map = context.table_and_part_tuple_map_ae

        
        try:
            report_template = generated_output_layer.name 
            main_catagories = context.report_to_main_catogory_map[report_template]
            for mc in main_catagories:
                try:
                    tables = tables_for_main_catagory_map[mc]
                    for table in tables:
                
                        #rules_for_table = RulesForILTable()
                        inputLayerTable = GenerationRuleCreator.\
                                        find_input_layer_cube (self,context,table,framework)
                        #rules_for_report.rulesForTable.extend([rules_for_table])
                        table_parts = table_and_part_tuple_map[mc]

                        for table_part in table_parts:
                            input_entity_list = [inputLayerTable]
                            # a table like instrument might have linked tables
                            # defined such as Party or Collateral
                            linked_tables = table_parts_to_linked_tables_map[table_part]
                            linked_tables_list = linked_tables.split(":")
                            if not (inputLayerTable is None):
                                if not (inputLayerTable.name in linked_tables_list):
                                    linked_tables_list.append(inputLayerTable.name)
                            extra_tables = []
                            for the_table in linked_tables_list:
                                extra_linked_tables = []
                                try:
                                    if the_table.endswith("_ELDM"):
                                        extra_linked_tables_string = context.ldm_entity_to_linked_tables_map[the_table]
                                        extra_linked_tables = extra_linked_tables_string.split(":")
                                    else:
                                        extra_linked_tables_string = context.ldm_entity_to_linked_tables_map[the_table+"_ELDM"]
                                        extra_linked_tables = extra_linked_tables_string.split(":")
                                except KeyError:
                                    pass

                                for extra_table in extra_linked_tables:
                                    if not extra_table in linked_tables_list:
                                        extra_tables.append(extra_table)

                            for extra_table in extra_tables:
                                if extra_table.endswith("_ELDM"):
                                    extra_table = linked_tables_list.append(extra_table[0:len(extra_table)-5])
                                else:
                                    extra_table = linked_tables_list.append(extra_table)


                            for the_table in linked_tables_list:
                                
                                the_input_table  = GenerationRuleCreator.\
                                                    find_input_layer_cube (
                                                    self,context,the_table,framework)
                                if not (the_input_table is None):
                                    input_entity_list.append(the_input_table)

                            if table_part[0] == table:
                                cube_link = CUBE_LINK()
                                
                                cube_link.description = mc
                                cube_link.name = table_part[1]
                                primary_cube = None
                                try:
                                    primary_cube = sdd_context.rol_cube_dictionary[table[5:len(table)]]
                                    cube_link.primary_cube_id = primary_cube
                                except KeyError:
                                    print("cube_link.primary_cube_id not found for " + table)
                                cube_link.foreign_cube_id = generated_output_layer
                                sdd_context.cube_links.append(cube_link)
                                
                                GenerationRuleCreator.\
                                    add_field_to_field_lineage_to_rules_for_table_part(
                                                    self, context,sdd_context, 
                                                     generated_output_layer,
                                                     input_entity_list,mc,report_template,
                                                     framework)
                                print(cube_link)
                                print(cube_link.name)
                                print(cube_link.description)
                                print(cube_link.primary_cube_id)
                                print(cube_link.foreign_cube_id)
                                
                except KeyError:
                    print ("no tables for main catagory:" + mc)

        except KeyError:
                    print ("no main catagory for report :" + report_template)

    def add_field_to_field_lineage_to_rules_for_table_part(self, context,
                                                           sdd_context,
                                                           output_entity,
                                                           input_entity_list,
                                                           catagory,
                                                           report_template, 
                                                           framework):
        '''
        Add field to field lineage entries to the rules for the table part
        '''

        for output_item in sdd_context.rol_cube_structure_item_dictionary[output_entity.cube_id + '_cube_structure']:

            if GenerationRuleCreator.valid_operation(self,context, output_item,framework,catagory,report_template):

                input_columns = GenerationRuleCreator.\
                    find_variables_with_same_domain_then_name(
                    self,sdd_context,output_item,input_entity_list)

                if len(input_columns) == 0:
                    csil = CUBE_STRUCTURE_ITEM_LINK()
                    csil.foreign_cube_variable_code = output_item
                    print(csil)
                    print(csil.foreign_cube_variable_code)

                else:                        
                    for input_column in input_columns:
                        csil = CUBE_STRUCTURE_ITEM_LINK()
                        csil.foreign_cube_variable_code = output_item
                        csil.primary_cube_variable_code = input_column
                        print(csil)
                        print(csil.foreign_cube_variable_code)
                        print(csil.primary_cube_variable_code)  


    
    def valid_operation(self,context, output_item,framework,catagory,report_template):
        '''
        '''
        return True
        
    def operation_exists_in_cell_for_report_with_catagory(self,context, sdd_context, output_item,framework,input_cube_type,catagory,report_template):
        '''
        Check if the operation exists in the combination of the report and typ_instrmnt
        '''
        combinations =[]
        try:
            combinations = sdd_context.combination_to_rol_cube_map[report_template]
        except KeyError:
            pass
        for combination in combinations:
            if combination in context.cell_to_typ_instrmnt_map[catagory]:
                combination_items = []
                try:
                    sdd_context.combination_item_dictionary[combination.combination_id]
                except KeyError:
                    pass
                for combination_item in combination_items:
                    if combination_item.variable_id.name == output_item.name:
                        if combination_item.member_id.member_id == catagory:
                            return True
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
                        if isinstance(input_item, CharField) or  isinstance(input_item, DateTimeField) or  isinstance(input_item, BigIntegerField) or  isinstance(input_item, BooleanField) or  isinstance(input_item, FloatField) or  isinstance(input_item, CharField) :
                            input_item_name = input_item.name
                            if input_item_name == output_variable_name:
                                related_variables= []
                                related_variables.append(input_item)
                            
        return related_variables
    
    def find_variables_with_same_domain_then_name(self,sdd_context,output_item,input_entity_list):
        '''
        when we have an ROL item it has a specific domain.
        We want to find any column in the limited related input tables
        which has the same domain
        '''
        import pdb; pdb.set_trace()
        related_variables = []   
        target_domain = None
        etype = output_item.variable_id
        target_domain = etype.domain_id

        if not (target_domain is None):
            for input_entity in input_entity_list:
                if not (input_entity is None):

                    field_list = input_entity._meta.get_fields()
                    for feature in field_list:
                        #if isinstance(feature, ForeignKey):
                        if isinstance(input_item, CharField) or  isinstance(input_item, DateTimeField) or  isinstance(input_item, BigIntegerField) or  isinstance(input_item, BooleanField) or  isinstance(input_item, FloatField) or  isinstance(input_item, CharField) :
                            enum_name = input_item.name.db_comment
                            if not(enum_name is None):
                                    if enum_name == target_domain.name:
                                        if not (input_item in related_variables):
                                            related_variables.append(input_item)
                            else:
                                print("input_item_etype is None for " + input_item.name)
        else:
            output_variable_name = output_item.name
           
            if not (output_variable_name is None):
                for input_entity in input_entity_list:
                    if not (input_entity is None):
                        for input_item in input_entity.eStructuralFeatures:
                            if isinstance(input_item, CharField) or  isinstance(input_item, DateTimeField) or  isinstance(input_item, BigIntegerField) or  isinstance(input_item, BooleanField) or  isinstance(input_item, FloatField) or  isinstance(input_item, CharField) :
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
    
    def find_output_layer_cube(self, sdd_context, output_layer_name,framework):
        '''
        Find the ELClass for the related output layer cube given the cube name
        '''
        if framework == "FINREP_REF":
            output_layer_name = output_layer_name + "_REF_FINREP_3_0"
        try:
            return sdd_context.rol_cube_dictionary[output_layer_name]
        except:
            return None

    def find_input_layer_cube(self, context, input_layer_name, framework):
        '''
        Find the ELClass for the related output layer cube given the cube name
        '''
        model_list = apps.get_models()
        for model in model_list:
            if model._meta.app_label == 'birdbox':
                if model.__name__ == input_layer_name:
                    return model
