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
import os
import csv
from sdd_model import *

class ImportSDD(object):
    '''
    Class responsible for the import of  SDD csv files
    into an instance of the analaysis model
    '''

    def import_sdd(self, sdd_context):
        '''
        Import SDD csv files into an instance of the analysis model
        '''
        ImportSDD.create_all_domains(self, sdd_context)
        ImportSDD.create_all_members(self, sdd_context)
        ImportSDD.create_all_variables(self, sdd_context)
        ImportSDD.create_all_subdomains(self, sdd_context)
        ImportSDD.create_all_combinations(self, sdd_context)
        ImportSDD.create_report_tables(self, sdd_context)
        ImportSDD.create_table_cells(self, sdd_context)
        ImportSDD.create_axis(self, sdd_context)
        ImportSDD.create_axis_ordinates(self, sdd_context)
        ImportSDD.create_cell_positions(self, sdd_context)
        # ImportSDD.create_all_subdomain_enumerations(self, sdd_context)
        # ImportSDD.createVariableSetToVariableMap(self, context)
        # ImportSDD.createVariableToDomainMap(self, context)
        # ImportSDD.createDomainToDomainNameMap(self, context)
        # ImportSDD.createMemberMaps(self, context)
        # ImportSDD.create_member_mappings(self, sdd_context,'EBA_MCY','TYP_INSTRMNT', 'TYP_ACCNTNG_ITM' )
        # ImportSDD.create_member_hierarchies(self, context)
        

    def create_all_domains(self, context):
        '''
        import all the domains
        '''
        file_location = context.file_directory + os.sep + "domain.csv"
        header_skipped = False

        with open(file_location,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if not header_skipped:
                    header_skipped = True
                else:
                    code = row[context.domain_domain_id_index]
                    data_type = row[context.domain_domain_data_type]
                    description = row[context.domain_domain_description]
                    domain_id = row[context.domain_domain_true_id]
                    is_enumerated = row[context.domain_domain_is_enumerated]
                    is_reference = row[context.domain_domain_is_reference]
                    # domainName = Utils.make_valid_id(row[3])
                    domain_name = row[context.domain_domain_name_index]
                    context.domain_to_domain_name_map[domain_id] = domain_name

                    # creat the Domain abject and set its fields
                    domain = DOMAIN(
                        name=ImportSDD.replace_dots(self, domain_id))
                    domain.code = code

                    # if (dataTypeString != null) {
					# if (dataTypeString.contains("tring")) {
					# valueType = FACET_VALUE_TYPE.STRING
					# domain.setData_type(valueType);
					# }
					# f (dataTypeString.contains("nteger")) {
					# valueType = FACET_VALUE_TYPE.BIG_INTEGER;
					# domain.setData_type(valueType);
					# }
					# if (dataTypeString.contains("ate")) {
					# valueType = FACET_VALUE_TYPE.DATE_TIME;
					# domain.setData_type(valueType);
					# }
					# if (dataTypeString.contains("umber") || dataTypeString.contains("onetary")) {
					# valueType = FACET_VALUE_TYPE.DECIMAL;
					# domain.setData_type(valueType);
					# }
					# if (dataTypeString.contains("oolean")) {
					# valueType = FACET_VALUE_TYPE.BOOLEAN;
					# domain.setData_type(valueType);
					# }

                    domain.description = description
                    domain.domain_id = ImportSDD.replace_dots(self, domain_id)
                    domain.displayName = domain_name
                    if is_enumerated:
                        domain.is_enumerated = True
                    else:
                        domain.is_enumerated = False

                    if is_enumerated:
                        domain.is_reference = True
                    else:
                        domain.is_reference = False

                    context.domains.domains.append(domain)

    def create_all_members(self, context):
        '''
        import all the members
        '''
        fileLocation = context.file_directory + os.sep + "member.csv"
        header_skipped = False

        with open(fileLocation,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if not header_skipped:
                    header_skipped = True
                else:
                    code = row[context.member_member_code_index]
                    description = row[context.member_member_descriptions]
                    domain_id = row[context.member_domain_id_index]
                    member_id = row[context.member_member_id_index]
                    member_name = row[context.member_member_name_index]
                    member = MEMBER(
                        name=ImportSDD.replace_dots(self, member_id))
                    member.member_id = ImportSDD.replace_dots(self, member_id)
                    member.code = code
                    member.description = description
                    member.displayName = member_name
                    domain = ImportSDD.get_domain_with_id(
                        self, context, domain_id)
                    member.domain_id = domain
                    context.members.members.append(member)

    def create_all_variables(self, context):
        '''
        import all the variables
        '''

        fileLocation = context.file_directory + os.sep + "variable.csv"
        header_skipped = False

        with open(fileLocation,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if not header_skipped:
                    header_skipped = True
                else:
                    code = row[context.variable_code_index]
                    description = row[context.variable_variable_description]
                    domain_id = row[context.variable_domain_index]
                    name = row[context.variable_long_name_index]
                    variable_id = row[context.variable_variable_true_id]
                    variable = VARIABLE(
                        name=ImportSDD.replace_dots(self, variable_id))
                    variable.code = code
                    variable.variable_id = ImportSDD.replace_dots(
                        self, variable_id)
                    variable.displayName = name
                    domain = ImportSDD.get_domain_with_id(self, context, domain_id)
                    variable.domain_id =domain
                    variable.description = description

                    context.variables.variables.append(variable)

    def create_all_subdomains(self, context):
        '''
        import all the subdomains
        '''
        fileLocation = context.file_directory + os.sep + "subdomain.csv"
        header_skipped = False

        with open(fileLocation,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if not header_skipped:
                    header_skipped = True
                else:
                    code = row[context.subdomain_subdomain_code]
                    description = row[context.subdomain_subdomain_description]
                    domain_id = row[context.subdomain_domain_id_index]
                    name = row[context.subdomain_subdomain_name]
                    subdomain_id = row[context.subdomain_subdomain_id_index]

                    subDomain = SUBDOMAIN()
                    subDomain.code = code
                    subDomain.subdomain_id = ImportSDD.replace_dots(
                        self, subdomain_id)
                    subDomain.displayName = name
                    subDomain.description = description

                    domain = ImportSDD.get_domain_with_id(self, context, domain_id)
                    subDomain.domain_id = domain
                    context.subdomains.subdomains.append(subDomain)

    def create_all_subdomain_enumerations(self, context):
        '''
        import all the subdomain enumerations
        '''
        file_location = context.file_directory + os.sep + "subdomain_enumeration.csv"
        header_skipped = False

        with open(file_location,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if not header_skipped:
                    header_skipped = True
                else:
                    member_id = row[context.subdomain_enumeration_member_id_index]
                    subdomain_id = row[context.subdomain_enumeration_subdomain_id_index]
                    subdomain = ImportSDD.get_subdomain_with_id(self, context, subdomain_id)
                    domain = subdomain.domain_id
                    member = ImportSDD.get_member_with_id_and_domain(
                        self, context, member_id, domain)
                    subdomain_enum = SUBDOMAIN_ENUMERATION()
                    subdomain_enum.member_id = member
                    subdomain.items.append(subdomain_enum)
        
    def create_all_combinations(self, context):
        file_location = context.file_directory + os.sep + "combination.csv"
        header_skipped = False
        counter = 0
        
        with open(file_location,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if not header_skipped:
                    header_skipped = True
                else:
                    combination_code = row[context.combination_combination_code]
                    combination_id = row[context.combination_combination_id]
                    combination_name = row[context.combination_combination_name]


                    comb = COMBINATION()
                    
                    comb.code = combination_code
                    comb.combination_id = combination_id
                    comb.name = combination_name
                    context.combinations.combinations.append(comb)

        file_location = context.file_directory + os.sep + "combination_item.csv"
        header_skipped = False
        counter = 0
        
        with open(file_location,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if not header_skipped:
                    header_skipped = True
                else:
                    combination_string = row[context.combination_item_combination_id] 
                    variable_string = row[context.combination_item_variable_id]
                    member_string = row[context.combination_member_id]
                    item = COMBINATION_ITEM()
                    com = ImportSDD.find_combination_with_id(self,context, combination_string)
                    mem = ImportSDD.find_member_with_id(self,member_string,context)
                    item.member_id =mem
                    if not ((variable_string is None) or (variable_string == "")):
                        variable = ImportSDD.find_variable_with_id(self,context,variable_string);
                        item.variable_id = variable
                    com.combination_items.append(item)
                    
    def create_report_tables (self, context):
        '''
        import all the tables from the rendering package
        '''
        file_location = context.file_directory + os.sep + "table.csv"
        header_skipped = False

        with open(file_location,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if not header_skipped:
                    header_skipped = True
                else:
                    table_id = row[context.table_id]
                    name = row[context.table_id]
                    display_name = row[context.table_table_name]
                    code = row[context.table_code]
                    description = row[context.table_description]
                    maintenance_ageny = row[context.table_maintenance_ageny]
                    version = row[context.table_version]
                    valid_from = row[context.table_valid_from]
                    valid_to = row[context.table_valid_to]
                    
                    table = TABLE(
                        name=ImportSDD.replace_dots(self, table_id))
                    table.table_id = ImportSDD.replace_dots(self, table_id)
                    table.displayName = name
                    table.code = code
                    table.description = description
                    table.maintenance_ageny = maintenance_ageny
                    table.version = version
                    table.valid_from = valid_from
                    table.valid_to = valid_to

                    context.report_tables.reportTables.append(table)
                    
                    
                   

    def create_member_mappings(self, context, source_variable_filter, target_variable_filter, target_variable_filter2):
        file_location = context.file_directory + os.sep + "member_mapping_item.csv"
        header_skipped = False
        counter = 0
        
        with open(file_location,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if not header_skipped:
                    header_skipped = True
                else:
                    member_mapping_id = row[context.member_mapping_id]
                    row_number = row[context.member_mapping_row]
                    variable_id = row[context.member_mapping_variable_id]
                    is_source = row[context.member_mapping_is_source]

                    member_id = row[context.member_mapping_member_id]
                    if ((variable_id == source_variable_filter) and (is_source == 'TRUE')) or ((variable_id == target_variable_filter) and (is_source == 'FALSE')) or ((variable_id == target_variable_filter2) and (is_source == 'FALSE')):
                        member_mapping_item = MEMBER_MAPPING_ITEM()
                        member_mapping_item.isSource = is_source
                        member_mapping_item.member = ImportSDD.find_member_with_id(self,member_id,context)
                        member_mapping_item.variable = ImportSDD.find_variable_with_id(self,context,variable_id)
                        member_mapping_item.row = row_number
                        
                        
                        member_mapping  = ImportSDD.find_member_mapping_with_id(self,context,member_mapping_id)
                        if member_mapping is None:
                            member_mapping = MEMBER_MAPPING(name = member_mapping_id)
                            member_mapping.code = member_mapping_id
                            member_mapping.member_mapping_id = member_mapping_id
                            context.memberMappingModule.memberMappings.append(member_mapping)
                            
                        member_mapping.memberMappingItems.append(member_mapping_item)
                        

    def find_member_mapping_with_id(self,context,member_mapping_id):
        mappingList = context.memberMappingModule.memberMappings
        return_member_mapping = None
        for mapping in mappingList:
           if mapping.name == member_mapping_id:
               return_member_mapping = mapping

        return return_member_mapping
                             
    def find_member_with_id(self,element_id,context):
        memberList = context.members.members
        returnMember = None
        for mem in memberList:
           if element_id == mem.name:
               returnMember = mem

        return returnMember
    
    
    
    def find_variable_with_id(self,context, element_id):
        variableList = context.variables.variables
        returnVariable = None
        for var in variableList:
           
            if element_id == var.name:
                returnVariable = var
   
        return returnVariable

    def find_combination_with_id(self, context, element_id):
        returnCombination = None
        for com in context.combinations.combinations:
            if element_id == com.combination_id:
                returnCombination = com
        return returnCombination


    def get_domain_with_id(self, context, domain_id_string):
        '''
        get the domain with the given id
        '''
        for domain in context.domains.domains:
            if domain.domain_id == domain_id_string:
                return domain

    def get_subdomain_with_id(self, context, subdomain_id_string):
        '''
        get the subdomain with the given id
        '''

        for subdomain in context.subdomains.subdomains:
            if subdomain.subdomain_id == subdomain_id_string:
                return subdomain

    def get_member_with_id_and_domain(self, context, member_id, domain):
        '''
        get the member with the given id and domain
        '''
        for member in context.members.members:
            if (member_id == member.name) and (member.domain_id == domain):
                return member

    def replace_dots(self, text):
        '''
        replace dots with underscores
        '''
        return text.replace('.', '_')
    
    def get_mappings_with_this_member_as_source_and_this_variable_as_target(self,sdd_context,member, target_variable):
        
        return_target_items = []
        mappingList = sdd_context.memberMappingModule.memberMappings
       
        for mapping in mappingList:
            member_mapping_items = []
            mapping_items = mapping.memberMappingItems
            for item in mapping_items:
                if (item.member == member) and (item.isSource == 'TRUE'):
                    member_mapping_items.append(item)
                if (item.variable == target_variable) and (item.isSource == 'FALSE'):
                    member_mapping_items.append(item)
                    
            for item in member_mapping_items:
                if item.isSource == 'TRUE':
                    row_id = item.row
                    for item2 in member_mapping_items:
                        if (item2.isSource == 'FALSE') and (item2.row == row_id) and not(item2.member.name.endswith("_0")):
                            return_target_items.append(item2) 
                            

        return return_target_items
        
