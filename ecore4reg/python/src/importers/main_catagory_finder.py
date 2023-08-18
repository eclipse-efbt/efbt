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
from importers.sdd_context import SDDContext
from importers.import_sdd_to_analysis_model import ImportSDD

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
        MainCatagoryFinder.create_draft_table_part_file(self, context)
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
                                if not(member_id in context.main_catagories_in_scope):
                                    context.main_catagories_in_scope.append(member_id)
                                    
                                
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
                                    
    def create_draft_table_part_file(self, context):
        '''
        create a draft of the table part file, this should be reviewed and edited
        and the edited version used as an input for processing
        
        1.) for each main catagory in scope find the difinition 
        2.) ignore definitions with full stops and commas as these are currently
        all composite or subparts
        3.) in the mappings find the related typ_instrument members(s) print a 
        message if there is more than 1
        4.) find the domain which contains that type of instrument
        5.) find which table and column has that domain
        6.) The table will be the Main Table
        7.) expand the member if it is a node in a heirarchy
        8.) print a warning message if there is more than on heirarchy producing
            different sub-members
        9.) the filter is made up of the column and the expanded node
        '''
        
        f = open(context.output_directory + os.sep + 'generations_transformations_csv' +
                         os.sep + 
                         'table_parts_draft.csv', "a",  encoding='utf-8')
        
        f.write("description,classifier,value,description,Main Catagory\n")
        sdd_context = SDDContext()
        sdd_context.file_directory = context.file_directory
        sdd_context.output_directory = context.output_directory
        ImportSDD.import_sdd(self,sdd_context)
        for mc in context.main_catagories_in_scope:
            mc_member = ImportSDD.find_member_with_id(self, mc, sdd_context)
            definition = mc_member.displayName
            if ',' in definition :
                print(mc_member.name + " : " + definition  + " is a composite catagory")
            elif '.' in definition :
                print(mc_member.name + " : " + definition  + " is a sub catagory")
            else:
                target_instrument_type = MainCatagoryFinder.get_target_instrument_type_from_mapping(self,context,sdd_context,mc_member) 
                if not(target_instrument_type is  None):
                    f.write(definition + ",TYP_INSTRMNT," + target_instrument_type.replace(',',' ').replace('TYP_INSTRMNT_','') + "," + mc +'\n')
                else:
                    target_accounting_type = MainCatagoryFinder.get_target_accounting_type_from_mapping(self,context,sdd_context,mc_member) 
                    if not(target_accounting_type is  None):
                        f.write(definition + ",TYP_ACCNTNG_ITM," + target_accounting_type.replace(',',' ').replace('TYP_ACCNTNG_ITM_','') + "," +  mc +'\n')
                    else:
                        f.write(definition + ",NOTHIN_FOUND,," + mc +'\n')
                
            
        f.close()
        
    def get_target_instrument_type_from_mapping(self,context,sdd_context,mc_member):
        
        instrument_type_variable = ImportSDD.find_variable_with_id(self,sdd_context,"TYP_INSTRMNT")
        member_mapping_items = ImportSDD.get_mappings_with_this_member_as_source_and_this_variable_as_target(self,sdd_context,mc_member,instrument_type_variable)
        unique_member_list = MainCatagoryFinder.remove_duplicates(self,member_mapping_items)
        if (len(unique_member_list) == 0):
            print(mc_member.name + " : "  + mc_member.displayName + " has NO instrument types: " )
            return None
        if (len(unique_member_list) == 1):
            print(mc_member.name + " : "  + mc_member.displayName + " has INSTRMNT_TYP: " + unique_member_list[0].name)
            return unique_member_list[0].name + "_" + unique_member_list[0].displayName 
        if (len(unique_member_list) > 1):
            
            returnString = ""
            for item in unique_member_list:
                 returnString = returnString + item.name + "_" + item.displayName  + ":"   
            return returnString    
        
    
    def warn_if_multiple_instrument_types(self,mc_member,member_mapping_items):
        
        ref_item_name = member_mapping_items[0].member.name
        for item in member_mapping_items:
            if not(item.member.name == ref_item_name):
                print (mc_member.name + " : " + mc_member.displayName + " has non duplicate instrument types " + ref_item_name + " and " + item.member.name)
    
    def warn_if_multiple_accounting_types(self,mc_member,member_mapping_items):
        
        ref_item_name = member_mapping_items[0].member.name
        for item in member_mapping_items:
            if not(item.member.name == ref_item_name):
                print (mc_member.name + " : " +mc_member.displayName + " has non duplicate accounting types " + ref_item_name + " and " + item.member.name)
    
    def get_target_accounting_type_from_mapping(self,context,sdd_context,mc_member):
        
        accounting_type_variable = ImportSDD.find_variable_with_id(self,sdd_context,"TYP_ACCNTNG_ITM")
        member_mapping_items = ImportSDD.get_mappings_with_this_member_as_source_and_this_variable_as_target(self,sdd_context,mc_member,accounting_type_variable)
        unique_member_list = MainCatagoryFinder.remove_duplicates(self,member_mapping_items)
        if (len(unique_member_list) == 0):
            print(mc_member.name + " : "  + mc_member.displayName + " has NO accounting types: " )
            return None
        if (len(unique_member_list) == 1 ):
            print(mc_member.name + " : "  + mc_member.displayName + " has TYP_ACCNTNG_ITM: " + unique_member_list[0].name)
            return unique_member_list[0].name + "_" + unique_member_list[0].displayName
        if (len(unique_member_list) > 1):                    
            returnString = ""
            for item in unique_member_list:
                 returnString = returnString + item.name + "_" + item.displayName + ":"   
            return returnString    
        return None
        
    def remove_duplicates(self,member_mapping_items): 
        unique_list = []
        for item in member_mapping_items:
            member = item.member
            if not (member in unique_list):
                unique_list.append(member)
        return unique_list
                                       
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
                        
                        
                    
            
                            
                                        