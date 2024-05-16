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
        if context.main_catagory_approach == "eba_main_catagory":
            MainCatagoryFinder.create_main_catogory_to_name_map(self, context,sdd_context,framework)
            MainCatagoryFinder.create_report_to_main_catogory_map(self, context,sdd_context,framework,reporting_framework_version)
            MainCatagoryFinder.create_draft_table_part_file(self, context,sdd_context,framework)
            MainCatagoryFinder.create_table_part_to_main_catagory_map(self, context,sdd_context,framework)
            MainCatagoryFinder.create_il_tables_for_main_catagory_map(self, context,sdd_context,framework)
            MainCatagoryFinder.create_table_parts_for_main_catagory_map(self, context,sdd_context,framework)
        else:
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
        if context.main_catagory_approach == "eba_main_catagory":
            file_location = context.file_directory + os.sep + "table_part_main_catagory_" + framework + ".csv"
        else:
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

     
    def create_report_to_main_catogory_map(self, context,sdd_context,full_framework_name,reporting_framework_version  ):
        '''
        create a map from report such as F_01_01_FINREP  
        into a list of main catagories used in that report
        '''
        if full_framework_name == "FINREP_REF":
            framework = "FINREP"
        elif full_framework_name == "AE_REF":
            framework = "AE"


        file_location = context.file_directory + os.sep + "in_scope_reports_" + full_framework_name + ".csv"
        in_scope_reports = []
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

                    in_scope_reports.append(report_template)


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
                if not header_skipped:
                    header_skipped = True
                else:
                    table_id = row[0]
                    table_code = row[2]
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
                        if version in reporting_framework_version:
                            
                            if (valid_to == "12/31/9999") or (valid_to == "31/12/9999") or (valid_to == "9999-12-31"):
                                if ImportWebsiteToSDDModel.replace_dots(self, table_code) in in_scope_reports:
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
                if not header_skipped:
                    header_skipped = True
                else:
                    axis_ordinate_id = row[0]
                    axis_id = row[6]
                    if axis_ordinate_id.startswith("EBA_" + framework):
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
                if not header_skipped:
                    header_skipped = True
                else:
                    axis_ordinate_id = row[0]
                    axis_ordinate_id_string_prefix = "EBA_" + framework + "_EBA_"
                    axis_ordinate_id_string_prefix_short = "EBA_" + framework 
                    if axis_ordinate_id.startswith(axis_ordinate_id_string_prefix):
                        if axis_ordinate_id in valid_eba_axis_ordinates:
                            variable_id = row[1]
                            if variable_id == "EBA_MCY":
                                # we find the report name by looking for the second
                                # instance of the string FINREP_ or AE_ 
                                axis_ordinate_id_string_prefix = "EBA_" + framework + "_EBA_"
                                report_name = axis_ordinate_id[len(axis_ordinate_id_string_prefix):axis_ordinate_id.\
                                              index("_" + framework,len(axis_ordinate_id_string_prefix_short))]
                                member_id = row[2]

                                if full_framework_name == "FINREP_REF":
                                    main_catagories_in_scope = context.main_catagories_in_scope_finrep
                                elif full_framework_name == "AE_REF":
                                    main_catagories_in_scope = context.main_catagories_in_scope_ae
                        



                                amemnded_report_name = Utils.make_valid_id(report_name)
                                if not(member_id in main_catagories_in_scope):
                                    main_catagories_in_scope.append(member_id)
                                try:
                                    catagory_list = context.\
                                        report_to_main_catogory_map[amemnded_report_name]
                                    if not(member_id in catagory_list):
                                        catagory_list.append(member_id)
                                except KeyError:
                                    # if we could not find a list of main
                                    # catagories for  report , create a new list
                                    member_list  = []
                                    member_list.append(member_id)
                                    context.\
                                        report_to_main_catogory_map[
                                            amemnded_report_name] = member_list
                                    
    def create_draft_table_part_file(self, context,sdd_context,framework):
        '''
        create a draft of the table part file, this should be reviewed and edited
        and the edited version used as an input for processing
        '''
        if framework == "FINREP_REF":
            main_catagories_in_scope = context.main_catagories_in_scope_finrep
            subdirectory = "finrep_generation_rules_il"
        elif framework == "AE_REF":
            main_catagories_in_scope = context.main_catagories_in_scope_ae
            subdirectory = "ae_generation_rules_il"

        f = open(context.output_directory + os.sep + 'generations_transformations_csv' +
                         os.sep + subdirectory + os.sep +
                         'table_parts_draft_' + framework + '.csv', "a",  encoding='utf-8')

        f.write("description,classifier,value,description,Main Catagory\n")
        sdd_context = SDDContext()
        sdd_context.file_directory = context.file_directory
        sdd_context.output_directory = context.output_directory

        

        for mc in main_catagories_in_scope:
            mc_member = ImportWebsiteToSDDModel.find_member_with_id(self, mc, sdd_context)
            definition = mc_member.displayName
            if ',' in definition :
                print(mc_member.name + " : " + definition  + \
                       " is a composite catagory")
            elif '.' in definition :
                print(mc_member.name + " : " + definition  + \
                      " is a sub catagory")
            else:
                target_instrument_type = MainCatagoryFinder.\
                    get_target_instrument_type_from_mapping(
                    self,sdd_context,mc_member)
                if not(target_instrument_type is None):
                    f.write(definition + ",TYP_INSTRMNT," + \
                            target_instrument_type.replace(',',' ').\
                                replace('TYP_INSTRMNT_','') + "," + mc +'\n')
                else:
                    target_accounting_type = MainCatagoryFinder.\
                        get_target_accounting_type_from_mapping(
                        self,sdd_context,mc_member) 
                    if not(target_accounting_type is  None):
                        f.write(definition + ",TYP_ACCNTNG_ITM," + \
                                target_accounting_type.replace(',',' ').\
                                    replace('TYP_ACCNTNG_ITM_','') + "," \
                                        +  mc +'\n')
                    else:
                        f.write(definition + ",NOTHIN_FOUND,," + mc +'\n')

        f.close()

    def get_target_instrument_type_from_mapping(self,sdd_context,mc_member):
        ''' get the target instrument type from the mapping'''
        instrument_type_variable = ImportWebsiteToSDDModel.\
            find_variable_with_id(self,sdd_context,"TYP_INSTRMNT")
        member_mapping_items = ImportWebsiteToSDDModel.\
            get_mappings_with_this_member_as_source_and_this_variable_as_target(
            self,sdd_context,mc_member,instrument_type_variable)
        unique_member_list = MainCatagoryFinder.\
            remove_duplicates(self,member_mapping_items)
        if len(unique_member_list) == 0:
            print(mc_member.name + " : "  + mc_member.displayName + \
                  " has NO instrument types: " )
            return None
        if len(unique_member_list) == 1:
            print(mc_member.name + " : "  + mc_member.displayName + \
                  " has INSTRMNT_TYP: " + unique_member_list[0].name)
            return unique_member_list[0].name + "_" + \
                unique_member_list[0].displayName 
        if len(unique_member_list) > 1:

            return_string = ""
            for item in unique_member_list:
                 return_string = return_string + item.name + \
                    "_" + item.displayName  + ":"
            return return_string
   
    def get_target_accounting_type_from_mapping(self,sdd_context,mc_member):
        ''' get the target accounting type from the mapping'''

        accounting_type_variable = ImportWebsiteToSDDModel.\
            find_variable_with_id(self,sdd_context,"TYP_ACCNTNG_ITM")
        member_mapping_items = ImportWebsiteToSDDModel.\
            get_mappings_with_this_member_as_source_and_this_variable_as_target(
            self,sdd_context,mc_member,accounting_type_variable)
        unique_member_list = MainCatagoryFinder.remove_duplicates(
            self,member_mapping_items)
        
        if len(unique_member_list) == 0:
            print(mc_member.name + " : "  + mc_member.displayName + \
                  " has NO accounting types: " )
            return None
        if len(unique_member_list) == 1 :
            print(mc_member.name + " : "  + mc_member.displayName + \
                  " has TYP_ACCNTNG_ITM: " + unique_member_list[0].name)
            return unique_member_list[0].name + "_" + unique_member_list[0].displayName
        if len(unique_member_list) > 1:
            return_string = ""
            for item in unique_member_list:
                return_string = return_string + item.name + "_" + item.displayName + ":"
            return return_string
        return None

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

    def create_il_tables_for_main_catagory_map(self, context, sdd_context,framework):
        '''
        create a map from main catagories such as loans and advancess
        to the related input layer such as instrument
        '''
        file_location = context.file_directory + os.sep + \
            "table_part_definitions_" + framework + ".csv"

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


    def create_table_parts_for_main_catagory_map(self, context, sdd_context,framework):
        '''
        create a map from main catagories such as loans and advancess
        to the related table parts, where table part is a combination
        of an input layer and main catagory description
        '''
        file_location = context.file_directory + os.sep + \
            "table_part_definitions_" + framework + ".csv"
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

    def alternative_create_report_to_main_catogory_map(self, context,sdd_context,full_framework_name,reporting_framework_version  ):
        '''
        looks through the generated report.
        '''
        if full_framework_name == "FINREP_REF":
            reports_module = context.finrep_on_sdd_reports_module
            main_catagories_in_scope = context.main_catagories_in_scope_finrep
        elif full_framework_name == "AE_REF":
            reports_module = context.ae_on_sdd_reports_module
            main_catagories_in_scope = context.main_catagories_in_scope_ae


        for report in reports_module.reports:
            for cell in report.reportCells:
                cell_instrmnt_ids_list =[]
                for filter in cell.filters: 
                    
                    if filter.operation.name == "TYP_INSTRMNT":
                        cell_instrmnt_ids_list = filter.member
                        for member_id in cell_instrmnt_ids_list:
                            catagory = 'TYP_INSTRMNT_' + member_id.literal
                            if not(catagory in main_catagories_in_scope):
                                    main_catagories_in_scope.append(catagory)
                            try:
                                catagory_list = context.\
                                    report_to_main_catogory_map[report.outputLayer.name]                                
                                if not(catagory in catagory_list):
                                    catagory_list.append(catagory)
                            except KeyError:
                                # if we could not find a list of main
                                # catagories for  report , create a new list
                                catagory_list  = []                                
                                catagory_list.append(catagory)
                                context.\
                                    report_to_main_catogory_map[report.outputLayer.name] = catagory_list
                                
                    
                if cell_instrmnt_ids_list ==[]:
                    for filter in cell.filters: 
                        if filter.operation.name == "TYP_ACCNTNG_ITM":
                            cell_accntng_itm_ids_list = filter.member
                            for member_id in cell_accntng_itm_ids_list:
                                catagory = 'TYP_ACCNTNG_ITM_' + member_id.literal
                                if not(catagory in main_catagories_in_scope):
                                    main_catagories_in_scope.append(catagory)
                                try:
                                    catagory_list = context.\
                                        report_to_main_catogory_map[report.outputLayer.name]
                                    
                                    if not(catagory in catagory_list):
                                        catagory_list.append(catagory)
                                except KeyError:
                                    # if we could not find a list of main
                                    # catagories for  report , create a new list
                                    catagory_list  = []
                                    catagory_list.append(catagory)
                                    context.\
                                        report_to_main_catogory_map[report.outputLayer.name] = catagory_list
    
        
                    
        
        
                                    
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
        sdd_context = SDDContext()
        sdd_context.file_directory = context.file_directory
        sdd_context.output_directory = context.output_directory

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