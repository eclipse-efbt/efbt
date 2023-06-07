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
        MainCatagoryFinder.create_il_tables_for_main_catagory_map(self, context)
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
        
        file_location = context.file_directory + os.sep + "table.csv"

        header_skipped = False
        
        valid_eba_tables = []
        # Load all the entities from the csv file, make an ELClass per entity, 
        # and add the ELClass to the package
        with open(file_location,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                # skip the first line which is the header.
                if (not header_skipped):
                    header_skipped = True
                else:
                    table_id = row[0]
                    maintenence_agency_id = row[4]
                    valid_to = row[7]
                    version = row[5]
                    
                    if maintenence_agency_id == "EBA":
                        # we are only going to include version 3 or version 3.0-ind
                        # note that in the data some older reports are such as 2.8 are 
                        # still marked as valid according to the valid to date.
                        # not yet clear what is the difference between 3 and 3.0-Ind
                        # but there is currently no overlap and it gives us the 
                        # set that we are interested in.
                        
                        if (version == "3.0-Ind") or (version == "3"):
                            if (valid_to == "12/31/9999") or (valid_to == "31/12/9999"):
                                valid_eba_tables.append(table_id)
                        
        file_location = context.file_directory + os.sep + "axis_ordinate.csv"

        header_skipped = False
        valid_eba_axis_ordinates = []
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
                    axis_id = row[6]
                    if axis_ordinate_id.startswith("EBA_FINREP"):
                        amended_axis_id = axis_id[0:len(axis_id)-2]
                        if amended_axis_id in valid_eba_tables:
                            valid_eba_axis_ordinates.append(axis_ordinate_id)
                            
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
                    if axis_ordinate_id.startswith("EBA_FINREP_EBA_"):
                        if axis_ordinate_id in valid_eba_axis_ordinates:
                            variable_id = row[1]
                            if variable_id == "EBA_MCY":
                                # we find the report name by looking for the second
                                # instance of the string FINREP_
                                report_name = axis_ordinate_id[15:axis_ordinate_id.index("_FINREP",10)]
                                print("report_name")
                                print(report_name)
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
                                    
    def create_il_tables_for_main_catagory_map(self, context):
        
         
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
                        
                        
                    
            
                            
                                        