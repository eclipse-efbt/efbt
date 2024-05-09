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
from utils.utils import Utils
from context.sdd_context import SDDContext
from process_steps.website_to_sddmodel.import_website_to_sdd_model import ImportWebsiteToSDDModel


class LDMEntityFinder(object):
    '''
    This class is responsable for creating maps of information
    related to the EBA main catagory
    '''
    def create_report_to_instrmnt_type_maps(self, context,sdd_context):
        '''
        create maps of information
        related to the EBA main catagory
        '''
        LDMEntityFinder.create_ldm_entity_to_first_linked_entities_map(self, context,sdd_context)
        LDMEntityFinder.create_ldm_entity_to_linked_entities_map(self, context,sdd_context)
        LDMEntityFinder.create_instrmnt_typ_to_name_map(self, context,sdd_context)
        LDMEntityFinder.create_report_to_instrmnt_type_map(self, context,sdd_context)
        LDMEntityFinder.create_draft_table_part_file(self, context,sdd_context)
        LDMEntityFinder.create_table_part_to_instrmnt_type_map(self, context,sdd_context)
        LDMEntityFinder.create_ldm_entities_for_instrmnt_type_map(self, context,sdd_context)
        LDMEntityFinder.create_table_parts_for_instrmnt_map(self, context,sdd_context)
        

    def create_ldm_entity_to_first_linked_entities_map(self, context,sdd_context):
        '''

        '''
        file_location = context.file_directory + os.sep + "typ_instrmnt_ldm_entity.csv"
        header_skipped = False
        # Load all the entities from the csv file, make an ELClass per entity,
        # and add the ELClass to the package
        with open(file_location,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                # skip the first line which is the header.
                if not header_skipped:
                    header_skipped = True
                else:
                    ldm_entity= row[6]
                    ldm_linked_entity = row[8]
                    context.ldm_entity_to_first_linked_entities_map[ldm_entity] = ldm_linked_entity

    def create_instrmnt_typ_to_name_map(self, context,sdd_context):
        '''

        '''
        file_location = context.file_directory + os.sep + "typ_instrmnt_ldm_entity.csv"
        header_skipped = False
        # Load all the entities from the csv file, make an ELClass per entity,
        # and add the ELClass to the package
        with open(file_location,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                # skip the first line which is the header.
                if not header_skipped:
                    header_skipped = True
                else:
                    typ_instrmnt = row[3]
                    typ_instrmnt_name = row[1]
                    context.main_catogory_to_name_map[typ_instrmnt] = typ_instrmnt_name

    def create_report_to_instrmnt_type_map(self, context,sdd_context):
        '''
        create a map from report such as F_01_01_FINREP  
        into a list of main catagories used in that report
        '''
        for table in sdd_context.report_tables.reportTables:
            if table.maintenance_agency_id.maintenance_agency_id == "ECB":
                if table.version in context.reporting_framework_version:
                    table_string_prefix = context.reporting_framework + "_REF_"
                    table_string_postfix = context.reporting_framework 
                    if table.table_id.startswith(table_string_prefix):
                        report_name = table.table_id[len(table_string_prefix):table.table_id.\
                                        index("_" + context.reporting_framework,len(table_string_postfix))]
                        amemnded_report_name = Utils.make_valid_id(report_name)

                    table_cells = sdd_context.table_to_table_cell_dictionary[table]
                    for table_cell in table_cells:
                        comb = table_cell.combination_id
                        if not(comb is None):
                            combination_items = comb.combination_items
                            for combination_item in combination_items:
                                variable_id = combination_item.variable_id
                                if variable_id.variable_id == "TYP_INSTRMNT":
                                    member_id = combination_item.member_id.member_id
                                    if not(member_id in context.typ_instrmnt_in_scope):
                                        context.typ_instrmnt_in_scope.append(member_id)
                                    try:
                                        typ_instrmnt_list = context.\
                                            report_to_typ_instrmnt_map[amemnded_report_name]
                                        if not(member_id in typ_instrmnt_list):
                                            typ_instrmnt_list.append(member_id)
                                    except KeyError:
                                        # if we could not find a list of main
                                        # catagories for  report , create a new list
                                        member_list  = []
                                        member_list.append(member_id)
                                        context.\
                                            report_to_typ_instrmnt_map[
                                                amemnded_report_name] = member_list
       
                                    
    def create_draft_table_part_file(self, context,sdd_context):
        '''
        create a draft of the table part file, this should be reviewed and edited
        and the edited version used as an input for processing
        '''

        f = open(context.output_directory + os.sep + 'generations_transformations_csv' +
                         os.sep + 
                         'table_parts_draft.csv', "a",  encoding='utf-8')

        f.write("description,classifier,value,description,Typ_instrmnt\n")
        sdd_context = SDDContext()
        sdd_context.file_directory = context.file_directory
        sdd_context.output_directory = context.output_directory
        for typ_instrmnt in context.typ_instrmnt_in_scope:
            typ_instrmnt_member = ImportWebsiteToSDDModel.find_member_with_id(self, typ_instrmnt, sdd_context)
            definition = typ_instrmnt_member.displayName
            if ',' in definition :
                print(typ_instrmnt_member.name + " : " + definition  + \
                       " is a composite catagory")
            elif '.' in definition :
                print(typ_instrmnt_member.name + " : " + definition  + \
                      " is a sub catagory")
            else:
                f.write(definition + ",,," + typ_instrmnt +'\n')

        f.close()

    def create_table_part_to_instrmnt_type_map(self, context, sdd_context):
        '''
        create a map from table parts to main catagories
        '''
        file_location = context.file_directory + os.sep + "typ_instrmnt_ldm_entity.csv"

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
                    typ_instrmnt = row[3]
                    table_part = row[6]    
                    context.table_parts_to_typ_instrmnt_map[
                        table_part] = typ_instrmnt

    def create_ldm_entities_for_instrmnt_type_map(self, context, sdd_context):
        '''
        create a map from main typ_instrmnt such as loans and advancess
        to the related ldm entity such as loans and advancess
        '''
        file_location = context.file_directory + os.sep + \
            "typ_instrmnt_ldm_entity.csv"

        header_skipped = False
  
        with open(file_location,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                # skip the first line which is the header.
                if not header_skipped:
                    header_skipped = True
                else:
                    table_part_name = row[6]
                    ldm_entity = row[6]
                    typ_instrmnt = context.\
                        table_parts_to_typ_instrmnt_map[table_part_name]
                    try:
                        entity_list = context.\
                            entities_for_typ_instrmnt_map[typ_instrmnt]
                        if not(ldm_entity in entity_list):
                            entity_list.append(ldm_entity)
                    except KeyError:
                        # if we could not find a list of tables for
                        # the typ_instrmnt then create a new list
                        entity_list = []
                        entity_list.append(ldm_entity)
                        context.\
                            entities_for_typ_instrmnt_map[typ_instrmnt] = entity_list

    def create_table_parts_for_instrmnt_map(self, context, sdd_context):
        '''
        create a map from main catagories such as loans and advancess
        to the related table parts, where table part is a combination
        of an ldm entity and typ_instrmnt description
        '''
        file_location = context.file_directory + os.sep + \
            "typ_instrmnt_ldm_entity.csv"
        header_skipped = False
        with open(file_location,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, 
                                    delimiter=',', quotechar='"')
            for row in filereader:
                # skip the first line which is the header.
                if not header_skipped:
                    header_skipped = True
                else:
                    table_part_name = row[6]                    
                    ldm_entity = row[6]
                    print("ldm_entity")
                    print(ldm_entity)

                    linked_table_list = ''
                    try:
                        linked_table_list = context.ldm_entity_to_linked_tables_map[ldm_entity]
                    except KeyError:
                        pass

                    typ_instrmnt = context.\
                        table_parts_to_typ_instrmnt_map[table_part_name]
                    table_and_part_tuple = (ldm_entity,table_part_name)
                    context.table_parts_to_linked_tables_map[
                        table_and_part_tuple] = linked_table_list


                    try:
                        table_and_part_tuple_list = context.\
                            table_and_part_tuple_map[typ_instrmnt]
                        if not(table_and_part_tuple in table_and_part_tuple_list):
                            table_and_part_tuple_list.append(table_and_part_tuple)
                    except KeyError:
                        table_and_part_tuple_list = []
                        table_and_part_tuple_list.append(table_and_part_tuple)
                        context.table_and_part_tuple_map[typ_instrmnt] = \
                            table_and_part_tuple_list
                        
    def create_ldm_entity_to_linked_entities_map(self, context, sdd_context):
        '''
        '''
        file_location = context.file_directory + os.sep + \
            "ldm_entity_related_entities.csv"
        header_skipped = False
        with open(file_location,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, 
                                    delimiter=',', quotechar='"')
            for row in filereader:
                # skip the first line which is the header.
                if not header_skipped:
                    header_skipped = True
                else:
                    ldm_entity = row[0]
                    related_entities = row[1]
                    context.ldm_entity_to_linked_tables_map[ldm_entity] = related_entities
