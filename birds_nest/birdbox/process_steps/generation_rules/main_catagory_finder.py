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



class MainCatagoryFinder(object):
    '''
    This class is responsable for creating maps of information
    related to the EBA main catagory
    '''
    def create_report_to_main_catogory_maps(self, context,sdd_context,framework,reporting_framework_version):
        '''
        create maps of information
        related to the EBA main catagory
        '''
        MainCatagoryFinder.create_main_catogory_to_name_map(self, context,sdd_context,framework)
        MainCatagoryFinder.alternative_create_report_to_main_catogory_map(self, context,sdd_context,framework,reporting_framework_version)
        MainCatagoryFinder.alternative_create_draft_table_part_file(self, context,sdd_context,framework)
        MainCatagoryFinder.alternative_create_table_part_to_main_catagory_map(self, context,sdd_context,framework)
        MainCatagoryFinder.alternative_create_il_tables_for_main_catagory_map(self, context,sdd_context,framework)
        MainCatagoryFinder.alternative_create_table_parts_for_main_catagory_map(self, context,sdd_context,framework)

    def create_main_catogory_to_name_map(self, context,sdd_context,framework):
        '''
        create a map of EBA main catagory code such as EBA_MC_EBA_x469 
        into its more user friendly display name such as loans and advances
        '''
        file_location = context.file_directory + os.sep + "table_part_to_reference_catagory_" + framework + ".csv"

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
                    main_catagory = row[0]
                    main_catagory_name = row[1]
                    if framework == "FINREP_REF":
                        context.main_catogory_to_name_map_finrep[main_catagory] = main_catagory_name
                    elif framework == "AE_REF":
                        context.main_catogory_to_name_map_ae[main_catagory] = main_catagory_name                   

    def remove_duplicates(self,member_mapping_items):
        ''' remove duplicates from a list of member mapping items'''
        unique_list = []
        for item in member_mapping_items:
            member = item.member
            if not (member in unique_list):
                unique_list.append(member)
        return unique_list

    def create_table_part_to_main_catagory_map(self, context, sdd_context,framework):
        '''
        create a map from table parts to main catagories
        '''
        file_location = context.file_directory + os.sep + "table_part_main_catagory_" + framework + ".csv"
        if framework == "FINREP_REF":
            table_parts_to_main_catagory_map = context.table_parts_to_main_catagory_map_finrep
        elif framework == "AE_REF":
            table_parts_to_main_catagory_map = context.table_parts_to_main_catagory_map_ae
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
                    main_catagory = row[0]
                    table_part = row[2]    
                    table_parts_to_main_catagory_map[
                        table_part] = main_catagory

    

    def alternative_create_report_to_main_catogory_map(self, context,sdd_context,full_framework_name,reporting_framework_version  ):
        '''
        looks through the generated report.
        '''

        if full_framework_name == "FINREP_REF":
            main_catagories_in_scope = context.main_catagories_in_scope_finrep
        elif full_framework_name == "AE_REF":
            main_catagories_in_scope = context.main_catagories_in_scope_ae
        for cube_name,combination_list in sdd_context.combination_to_rol_cube_map.items():
            for combination in combination_list:
                cell_instrmnt_ids_list =[]
                combination_items = []
                try:
                    combination_items = sdd_context.combination_item_dictionary[combination.combination_id.combination_id]
                except KeyError:
                    pass
                for combination_item in combination_items:
                    if not(combination_item.variable_id is None):
                        if combination_item.variable_id.variable_id == "TYP_INSTRMNT":
                            cell_instrmnt_ids_list = [combination_item.member_id] # might need to expand out a  hierarchy here
                            # if the only member of the list is '0' (i.e. not applicable) then we 
                        # set the list to be empty
                        if len(cell_instrmnt_ids_list) == 1 :
                            if cell_instrmnt_ids_list[0].code == "0":
                                cell_instrmnt_ids_list = []

                        for member_id in cell_instrmnt_ids_list:
                            catagory = 'TYP_INSTRMNT_' + member_id.code
                            if not(catagory in main_catagories_in_scope):
                                    main_catagories_in_scope.append(catagory)
                            try:
                                catagory_list = context.\
                                    report_to_main_catogory_map[cube_name]                                
                                if not(catagory in catagory_list):
                                    catagory_list.append(catagory)
                            except KeyError:
                                # if we could not find a list of main
                                # catagories for  report , create a new list
                                catagory_list  = []                                
                                catagory_list.append(catagory)
                                context.\
                                    report_to_main_catogory_map[cube_name] = catagory_list
        
                if cell_instrmnt_ids_list ==[]:
                    for combination_item in combination_items:
                        if not(combination_item.variable_id is None):
                            cell_accntng_itm_ids_list = []
                            if combination_item.variable_id.variable_id ==  "TYP_ACCNTNG_ITM":
                                cell_accntng_itm_ids_list = [combination_item.member_id] # might need to expand out a  hierarchy here
                            for member_id in cell_accntng_itm_ids_list:
                                catagory = 'TYP_ACCNTNG_ITM_' + member_id.code
                                if not(catagory in main_catagories_in_scope):
                                    main_catagories_in_scope.append(catagory)
                                try:
                                    catagory_list = context.\
                                        report_to_main_catogory_map[cube_name]
                                    
                                    if not(catagory in catagory_list):
                                        catagory_list.append(catagory)
                                except KeyError:
                                    # if we could not find a list of main
                                    # catagories for  report , create a new list
                                    catagory_list  = []
                                    catagory_list.append(catagory)
                                    context.\
                                        report_to_main_catogory_map[cube_name] = catagory_list
        
           
    def alternative_create_draft_table_part_file(self, context,sdd_context,framework):
        '''
        create a draft of the table part file, this should be reviewed and edited
        and the edited version used as an input for processing
        '''
        if framework == "FINREP_REF":
            main_catagories_in_scope = context.main_catagories_in_scope_finrep
            subdirectory = "finrep_generation_rules_ldm"
        elif framework == "AE_REF":
            main_catagories_in_scope = context.main_catagories_in_scope_ae
            subdirectory = "ae_generation_rules_ldm"

        f = open(context.output_directory + os.sep + 'generations_transformations_csv' +
                         os.sep + subdirectory + os.sep +
                         'table_parts_draft_' + framework + '.csv', "a",  encoding='utf-8')

        f.write("description,classifier,value,description,Main Catagory\n")

        for mc in main_catagories_in_scope:
            f.write(",,,," + mc +'\n')

        f.close()

    def alternative_create_table_part_to_main_catagory_map(self, context, sdd_context,framework):
        '''
        create a map from table parts to main catagories
        '''
        file_location = context.file_directory + os.sep + "table_part_to_reference_catagory_" + framework + ".csv"
        if framework == "FINREP_REF":
            table_parts_to_main_catagory_map = context.table_parts_to_main_catagory_map_finrep
        elif framework == "AE_REF":
            table_parts_to_main_catagory_map = context.table_parts_to_main_catagory_map_ae
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
                    main_catagory = row[0]
                    table_part = row[2]    
                    table_parts_to_main_catagory_map[
                        table_part] = main_catagory

    def alternative_create_il_tables_for_main_catagory_map(self, context, sdd_context,framework):
        '''
        create a map from main catagories such as loans and advancess
        to the related input layer such as instrument
        '''
        file_location = context.file_directory + os.sep + \
            "table_part_ldm_definitions_" + framework + ".csv"

        header_skipped = False

        if framework == "FINREP_REF":
            table_parts_to_main_catagory_map = context.table_parts_to_main_catagory_map_finrep
            tables_for_main_catagory_map = context.tables_for_main_catagory_map_finrep
        elif framework == "AE_REF":
            table_parts_to_main_catagory_map = context.table_parts_to_main_catagory_map_ae
            tables_for_main_catagory_map = context.tables_for_main_catagory_map_ae
        # Load all the entities from the csv file,
        # make an ELClass per entity,
        # and add the ELClass to the package
        with open(file_location,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                # skip the first line which is the header.
                if not header_skipped:
                    header_skipped = True
                else:
                    table_part_name = row[0]
                    il_table = row[1]
                    try:
                        main_catagory = table_parts_to_main_catagory_map[table_part_name]
                        try:
                            table_list = tables_for_main_catagory_map[main_catagory]
                            if not(il_table in table_list):
                                table_list.append(il_table)
                        except KeyError:
                            # if we could not find a list of tables for
                            # the main catagory then create a new list
                            table_list = []
                            table_list.append(il_table)
                            tables_for_main_catagory_map[main_catagory] = table_list
                    except KeyError:
                        print("could not find main catagory for table part " + table_part_name)

    def alternative_create_table_parts_for_main_catagory_map(self, context, sdd_context,framework):
        '''
        create a map from main catagories such as loans and advancess
        to the related table parts, where table part is a combination
        of an input layer and main catagory description
        '''
        file_location = context.file_directory + os.sep + \
            "table_part_ldm_definitions_" + framework + ".csv"
        header_skipped = False
        if framework == "FINREP_REF":
            table_parts_to_main_catagory_map = context.table_parts_to_main_catagory_map_finrep
            table_parts_to_linked_tables_map = context.table_parts_to_linked_tables_map_finrep
            table_parts_to_to_filter_map = context.table_parts_to_to_filter_map_finrep
            table_and_part_tuple_map = context.table_and_part_tuple_map_finrep
        elif framework == "AE_REF":
            table_parts_to_main_catagory_map = context.table_parts_to_main_catagory_map_ae
            table_parts_to_linked_tables_map = context.table_parts_to_linked_tables_map_ae
            table_parts_to_to_filter_map = context.table_parts_to_to_filter_map_ae
            table_and_part_tuple_map = context.table_and_part_tuple_map_ae


        with open(file_location,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, 
                                    delimiter=',', quotechar='"')
            for row in filereader:
                # skip the first line which is the header.
                if not header_skipped:
                    header_skipped = True
                else:
                    table_part_name = row[0]                    
                    il_table = row[1]
                    the_filter = row[2]
                    linked_table_list = row[3]
                    try:
                        main_catagory = table_parts_to_main_catagory_map[table_part_name]
                        table_and_part_tuple = (il_table,table_part_name)
                        table_parts_to_linked_tables_map[
                            table_and_part_tuple] = linked_table_list
                        table_parts_to_to_filter_map[
                            table_and_part_tuple] = the_filter

                        try:
                            table_and_part_tuple_list = table_and_part_tuple_map[main_catagory]
                            if not(table_and_part_tuple in table_and_part_tuple_list):
                                table_and_part_tuple_list.append(table_and_part_tuple)
                        except KeyError:
                            table_and_part_tuple_list = []
                            table_and_part_tuple_list.append(table_and_part_tuple)
                            table_and_part_tuple_map[main_catagory] = \
                                table_and_part_tuple_list
                    except KeyError:
                        print("could not find main catagory for the table part " + table_part_name)