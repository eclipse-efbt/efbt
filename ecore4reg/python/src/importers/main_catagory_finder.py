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
from importers.utils import Utils

class MainCatagoryFinder(object):
    
    def create_report_to_main_catogory_maps(self, context):
        MainCatagoryFinder.create_main_catogory_to_name_map(self, context)
        MainCatagoryFinder.create_report_to_main_catogory_map(self, context)
        MainCatagoryFinder.create_tables_for_main_catagory_map(self, context)
        MainCatagoryFinder.create_table_parts_for_main_catagory_map(self, context)
        

    def create_main_catogory_to_name_map(self, context): 
        file_location = context.file_directory + os.sep + "main_catagory_to_input_layer_analysis.csv"

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
                    main_catagory_name = row[1]
                    
                    context.main_catogory_to_name_map[main_catagory] = main_catagory_name
                    

    def create_report_to_main_catogory_map(self, context):
        
        file_location = context.file_directory + os.sep + "ordinate_item.csv"

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

                    axis_ordinate_id = row[0]
                    if axis_ordinate_id.startswith("EBA_FINREP_EBA_") and  "FINREP_3.0-Ind" in axis_ordinate_id:
                        variable_id = row[1]
                        if variable_id == "EBA_MCY":
                            report_name = axis_ordinate_id[15:axis_ordinate_id.index("_FINREP_3.0-Ind")]
                            member_id = row[2]
                            amemnded_report_name = Utils.make_valid_id(report_name)
                            
                            try:
                                catagory_list = context.report_to_main_catogory_map[amemnded_report_name]
                                if not(member_id in catagory_list):
                                    catagory_list.append(member_id)
                            except KeyError: 
                                list  = []
                                list.append(member_id)
                                context.report_to_main_catogory_map[amemnded_report_name] = list                                
                                    
    def create_tables_for_main_catagory_map(self, context):
        
         
        file_location = context.file_directory + os.sep + "main_catagory_to_input_layer_analysis.csv"

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
                    il_table = row[3]
                    try: 
                        table_list = context.tables_for_main_catagory_map[main_catagory]
                        if not(il_table in table_list):
                            table_list.append(il_table)
                    
                    except KeyError:
                        table_list = []
                        table_list.append(il_table)
                        context.tables_for_main_catagory_map[main_catagory] = table_list
                        
                    
                        
    def create_table_parts_for_main_catagory_map(self, context):
        file_location = context.file_directory + os.sep + "main_catagory_to_input_layer_analysis.csv"

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
                    ldm = row[2] 
                    il_table = row[3]
                    linked_table_list = row[6]
                    filter = row[4]
                    filter_items = row[5]
                    
                    table_and_part_tuple = (il_table,ldm)
                    context.table_parts_to_linked_tables_map[table_and_part_tuple] = linked_table_list
                    context.table_parts_to_to_filter_map[table_and_part_tuple] = filter
                    context.table_parts_to_to_filter_items_map[table_and_part_tuple] = filter_items
                    
                    try:
                        table_and_part_tuple_list = context.table_and_part_tuple_map[main_catagory]
                        if not(table_and_part_tuple in table_and_part_tuple_list):
                            table_and_part_tuple_list.append(table_and_part_tuple)
                            
                    
                    except KeyError:
                        table_and_part_tuple_list = []
                        table_and_part_tuple_list.append(table_and_part_tuple)
                        context.table_and_part_tuple_map[main_catagory] = table_and_part_tuple_list
                        
                        
                    
            
                            
                                        