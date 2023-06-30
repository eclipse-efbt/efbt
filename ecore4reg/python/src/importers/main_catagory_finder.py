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
    '''
    This class is responsable for creating maps of information
    related to the EBA main catagory
    '''
    
    def create_report_to_main_catogory_maps(self, context):
        '''
        create maps of information
        related to the EBA main catagory
        '''
        MainCatagoryFinder.create_main_catogory_to_name_map(self, context)
        MainCatagoryFinder.create_report_to_main_catogory_map(self, context)
        MainCatagoryFinder.create_table_part_to_main_catagory_map(self, context)
        MainCatagoryFinder.create_il_tables_for_main_catagory_map(self, context)
        MainCatagoryFinder.create_table_parts_for_main_catagory_map(self, context)
        

    def create_main_catogory_to_name_map(self, context):
        '''
        create a map of EBA main catagory code such as EBA_MC_EBA_x469 
        into its more user friendly display name such as loans and advances
        ''' 
        file_location = context.file_directory + os.sep + "table_part_main_catagory.csv"

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
        '''
        create a map from report such as F_01_01_FINREP  
        into a list of main catagories used in that report
        ''' 
        file_location = context.file_directory + os.sep + "table.csv"
        header_skipped = False
        valid_eba_tables = []
        # Loop through the tables of the rendering package
        # there is one table per report. 
        # Note that in the rendering package a table is a grid of
        # report cells, so is not to be confused with tbles of the 
        # input layer
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
                        # we are only going to include specific versions and 
                        # for FINREP this will be version 3 or version 3.0-ind
                        # note that in the data some older reports exist such as
                        # 2.8 that are still marked as valid according to the
                        # valid to date.
                        # It is not yet clear what is the difference between 3 and 3.0-Ind
                        # but there is currently no overlap and it gives us the 
                        # set that we are interested in.
                        
                        if version in context.reporting_framework_version:
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
                    if axis_ordinate_id.startswith("EBA_" + context.reporting_framework):
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
                    if axis_ordinate_id.startswith("EBA_" + context.reporting_framework + "_EBA_"):
                        if axis_ordinate_id in valid_eba_axis_ordinates:
                            variable_id = row[1]
                            if variable_id == "EBA_MCY":
                                # we find the report name by looking for the second
                                # instance of the string FINREP_
                                report_name = axis_ordinate_id[15:axis_ordinate_id.index("_" + context.reporting_framework,10)]
                                member_id = row[2]
                                amemnded_report_name = Utils.make_valid_id(report_name)
                                
                                try:
                                    catagory_list = context.report_to_main_catogory_map[amemnded_report_name]
                                    if not(member_id in catagory_list):
                                        catagory_list.append(member_id)
                                except KeyError: 
                                    # if we could not find a list of main
                                    # catagories for  report , create a new list
                                    list  = []
                                    list.append(member_id)
                                    context.report_to_main_catogory_map[amemnded_report_name] = list 
                                    
    def create_table_part_to_main_catagory_map(self, context):
        '''
        create a map from table parts to main catagories
        ''' 
        file_location = context.file_directory + os.sep + "table_part_main_catagory.csv"

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
                    context.table_parts_to_main_catagory_map[table_part] = main_catagory
                                
    def create_il_tables_for_main_catagory_map(self, context):
        '''
        create a map from main catagories such as loans and advancess
        to the related input layer such as instrument
        ''' 
        file_location = context.file_directory + os.sep + "table_part_definitions.csv"

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
                    table_part_name = row[0]                    
                    il_table = row[1]
                    main_catagory = context.table_parts_to_main_catagory_map[table_part_name]
                    try: 
                        
                        table_list = context.tables_for_main_catagory_map[main_catagory]
                        if not(il_table in table_list):
                            table_list.append(il_table)
                    
                    except KeyError:
                        # if we could not find a list of tables for 
                        # the main catagory then create a new list
                        table_list = []
                        table_list.append(il_table)
                        context.tables_for_main_catagory_map[main_catagory] = table_list
                        
                    
                        
    def create_table_parts_for_main_catagory_map(self, context):
        '''
        create a map from main catagories such as loans and advancess
        to the related table parts, where table part is a combination
        of an input layer and main catagory description
        ''' 
        file_location = context.file_directory + os.sep + "table_part_definitions.csv"

        header_skipped = False

        with open(file_location,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                # skip the first line which is the header.
                if (not header_skipped):
                    header_skipped = True
                else:
                    table_part_name = row[0]                    
                    il_table = row[1]
                    filter = row[2]
                    linked_table_list = row[3]
                    
                    main_catagory = context.table_parts_to_main_catagory_map[table_part_name]
                    table_and_part_tuple = (il_table,table_part_name)
                    context.table_parts_to_linked_tables_map[table_and_part_tuple] = linked_table_list
                    context.table_parts_to_to_filter_map[table_and_part_tuple] = filter
                    
                    try:
                        table_and_part_tuple_list = context.table_and_part_tuple_map[main_catagory]
                        if not(table_and_part_tuple in table_and_part_tuple_list):
                            table_and_part_tuple_list.append(table_and_part_tuple)
                            
                    
                    except KeyError:
                        table_and_part_tuple_list = []
                        table_and_part_tuple_list.append(table_and_part_tuple)
                        context.table_and_part_tuple_map[main_catagory] = table_and_part_tuple_list
                        
                        
                    
            
                            
                                        