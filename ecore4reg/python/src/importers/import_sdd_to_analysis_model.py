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
        ImportSDD.create_all_variable_sets(self, sdd_context)
        ImportSDD.create_all_subdomains(self, sdd_context)
        ImportSDD.create_all_combinations(self, sdd_context)
        ImportSDD.create_report_tables(self, sdd_context)
        ImportSDD.create_table_cells(self, sdd_context)
        ImportSDD.create_axis(self, sdd_context)
        ImportSDD.create_axis_ordinates(self, sdd_context)
        ImportSDD.create_cell_positions(self, sdd_context)
        # ImportSDD.create_all_subdomain_enumerations(self, sdd_context)
        # ImportSDD.createVariableSetToVariableMap(self, context)
        # ImportSDD.createvariable_to_domain_map(self, context)
        # ImportSDD.createDomainToDomainNameMap(self, context)
        # ImportSDD.createMemberMaps(self, context)
        ImportSDD.create_member_mappings(self, sdd_context,'EBA_MCY','TYP_INSTRMNT', 'TYP_ACCNTNG_ITM' )
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
                    context.member_dictionary[member_id] = member

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
                    context.variable_dictionary[variable_id] = variable

    def create_all_variable_sets(self, context):
        '''
        import all the variables
        '''

        fileLocation = context.file_directory + os.sep + "variable_set_enumeration.csv"
        header_skipped = False

        with open(fileLocation,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if not header_skipped:
                    header_skipped = True
                else:
                    valid_to = row[context.variable_set_enumeration_valid_to]
                    variable_id = row[context.variable_set_enumeration_variable_id]
                    subdomain_id = row[context.variable_set_enumeration_subdomain_id]
                    variable_set = row[context.variable_set_enumeration_valid_set]
                    
                    if (valid_to == "12/31/9999") or (valid_to == "12/31/2999") or (valid_to == "31/12/9999") or (valid_to == "31/12/2999"):
                        variable_set_enumeration = VARIABLE_SET_ENUMERATION()
                        variable_set_enumeration.variable_id = ImportSDD.find_variable_with_id(self,context,variable_id);
                        variable_set_enumeration.subdomain_id = ImportSDD.get_subdomain_with_id(self,context,subdomain_id);
                        variable_list = None
                        
                        try:
                            variable_list = context.variable_set_to_variable_map[variable_set]
                        except:
                            variable_list = []
                            context.variable_set_to_variable_map[variable_set] = variable_list

                        if not variable_id in variable_list:
                            variable_list.append(variable_set_enumeration)
                            
        fileLocation = context.file_directory + os.sep + "variable_set.csv"
        header_skipped = False

        with open(fileLocation,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if not header_skipped:
                    header_skipped = True
                else:
                    variable_set_id = row[context.variable_set_variable_set_id]
                    variable_set = VARIABLE_SET()
                    variable_set.variable_set_id = variable_set_id
                    try:
                        variable_set_enumerations = context.variable_set_to_variable_map[variable_set_id]
                        for enumeration in variable_set_enumerations:
                            variable_set.variable_set_items.append(enumeration)
                    except:
                        print("no items for " + variable_set_id )
                    
                    context.variable_sets.variableSets.append(variable_set)
                    
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
        
        combination_dict = {}
        with open(file_location,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if not header_skipped:
                    header_skipped = True
                else:
                    combination_code = row[context.combination_combination_code]
                    combination_id = row[context.combination_combination_id]
                    combination_name = row[context.combination_combination_name]
                    combination_valid_to = row[context.combination_combination_valid_to]
                    combination_combination_maintenance_agency = row[context.combination_combination_maintenance_agency]

                    if (combination_combination_maintenance_agency == 'ECB') and (combination_valid_to == '12/31/9999'):
                        comb = COMBINATION()
                        
                        comb.code = combination_code
                        comb.combination_id = combination_id
                        comb.name = combination_name
                        context.combinations.combinations.append(comb)
                        context.combinations_dictionary[combination_id] = comb

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
                    variable_set = row[context.combination_variable_set]
                    com = ImportSDD.find_combination_with_id(self,context, combination_string)
                    if (not (com is None)) and combination_string.endswith('_REF'):
                        item = COMBINATION_ITEM()
                        mem = ImportSDD.find_member_with_id(self,member_string,context)
                        item.member_id =mem
                        if not ((variable_string is None) or (variable_string == "")):
                            variable = ImportSDD.find_variable_with_id(self,context,variable_string)
                            item.variable_id = variable
                        if not(variable_set is None) or not(variable_set == ""):
                            variable_set = ImportSDD.find_variable_set_with_id(self,context,variable_set)
                            item.variable_set_id = variable_set
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
                    table_id = row[context.table_table_id]
                    name = row[context.table_table_id]
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
                    # not needed yet table.valid_from = valid_from
                    # not needed yet table.valid_to = valid_to

                    context.report_tables.reportTables.append(table)
                    
    def create_axis (self, context):
        '''
        import all the axes from the rendering package
        '''
        file_location = context.file_directory + os.sep + "axis.csv"
        header_skipped = False

        with open(file_location,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if not header_skipped:
                    header_skipped = True
                else:
                    axis_id = row[context.axis_id]
                    axis_orientation = row[context.axis_orientation]
                    axis_order = row[context.axis_order]
                    axis_name = row[context.axis_name]
                    axis_description = row[context.axis_description]
                    axis_table_id = row[context.axis_table_id]
                    axis_is_open_axis = row[context.axis_is_open_axis]
                    
                    
                    axis = AXIS(
                        name=ImportSDD.replace_dots(self, axis_id))
                    axis.axis_id = ImportSDD.replace_dots(self, axis_id)
                    axis.orientation = axis_orientation
                    # not needed yet axis.order = axis_order
                    axis.description = axis_description
                    axis.table_id = ImportSDD.find_table_with_id(self, context, axis_table_id)
                    # not needed yet
                    # axis.is_open_axis = axis_is_open_axis
                                        
                    context.axes.axes.append(axis)              
                   
    def create_axis_ordinates(self, context):
        '''
        import all the axis_ordinate from the rendering package
        '''
        file_location = context.file_directory + os.sep + "axis_ordinate.csv"
        header_skipped = False

        with open(file_location,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if not header_skipped:
                    header_skipped = True
                else:
                    axis_ordinate_id = row[context.axis_ordinate_id]
                    axis_ordinate_is_abstract_header = row[context.axis_ordinate_is_abstract_header]
                    axis_ordinate_code = row[context.axis_ordinate_code]
                    axis_ordinate_order = row[context.axis_ordinate_order]
                    axis_ordinate_path = row[context.axis_ordinate_path]
                    axis_ordinate_axis_id = row[context.axis_ordinate_axis_id]
                    axis_ordinate_parent_axis_ordinate_id = row[context.axis_ordinate_parent_axis_ordinate_id]
                    axis_ordinate_name = row[context.axis_ordinate_name]
                    axis_ordinate_description = row[context.axis_ordinate_description]
                    
                    
                    
                    axis_ordinate = AXIS_ORDINATE(
                        name=ImportSDD.replace_dots(self, axis_ordinate_id))
                    axis_ordinate.axis_ordinate_id = ImportSDD.replace_dots(self, axis_ordinate_id)
                    # not needed yet
                    # axis_ordinate.is_abstract_header = axis_ordinate_is_abstract_header
                    axis_ordinate.code = axis_ordinate_code
                    # not needed yet axis_ordinate.order = axis_ordinate_order
                    axis_ordinate.path = axis_ordinate_path
                    axis_ordinate.axis_id = ImportSDD.find_axis_with_id(self, context, axis_ordinate_axis_id)
                    # we don't need the parent axis yet in our processing.
                    # axis_ordinate.axis_ordinate_id = axis_ordinate_parent_axis_ordinate_id
                    axis_ordinate.displayName = axis_ordinate_name
                    axis_ordinate.description = axis_ordinate_description
                    context.axis_ordinates.axis_ordinates.append(axis_ordinate) 
                    context.axis_ordinate_dictionary[axis_ordinate_id] = axis_ordinate
                    
    def create_table_cells(self, context):
        '''
        import all the axis_ordinate from the rendering package
        '''
        file_location = context.file_directory + os.sep + "table_cell.csv"
        header_skipped = False

        with open(file_location,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if not header_skipped:
                    header_skipped = True
                else:
                    table_cell_cell_id = row[context.table_cell_cell_id]
                    table_cell_is_shaded = row[context.table_cell_is_shaded]
                    table_cell_combination_id = row[context.table_cell_combination_id]
                    table_cell_table_id = row[context.table_cell_table_id]
                    table_cell_system_data_code = row[context.table_cell_system_data_code]
                    
                    # we only look at the reference combinations, they all end with the string _REF
                    if table_cell_cell_id.endswith("_REF"):
                        table_cell = TABLE_CELL(
                            name=ImportSDD.replace_dots(self, table_cell_cell_id))
                        table_cell.cell_id = ImportSDD.replace_dots(self, table_cell_cell_id)
                        # table_cell_is_shaded not needed yet
                        # 
                        table_cell.combination_id = ImportSDD.find_combination_with_id(self, context, table_cell_combination_id)
                        table_cell.table_id = ImportSDD.find_table_with_id(self, context, table_cell_table_id)
                        
                        context.table_cells.tableCells.append(table_cell)
                        context.table_cell_dictionary[table_cell_cell_id] = table_cell
                    
    def create_cell_positions(self, context):
        '''
        import all the axis_ordinate from the rendering package
        '''
        file_location = context.file_directory + os.sep + "cell_position.csv"
        header_skipped = False

        with open(file_location,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if not header_skipped:
                    header_skipped = True
                else:
                    cell_positions_cell_id = row[context.cell_positions_cell_id]
                    cell_positions_axis_ordinate_id = row[context.cell_positions_axis_ordinate_id]
                    # check if we already have the table cell created
                    cell_position = None
                    try:
                        cell_position = context.cell_positions_dictionary[cell_positions_cell_id]
                    except:
                        cell_position = CELL_POSITION()
                        context.cell_positions_dictionary[cell_positions_cell_id] = cell_position 
                        
                    cell_position.axis_ordinate_id.append(ImportSDD.find_axis_ordinate_with_id(self, context, cell_positions_axis_ordinate_id))
                    cell_position.cell_id = ImportSDD.find_table_cell_with_id(self, context, cell_positions_cell_id)
                    context.cell_positions.cellPositions.append(cell_position)    
                     
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
                            context.member_mappings.memberMappings.append(member_mapping)
                            
                        member_mapping.memberMappingItems.append(member_mapping_item)
                        context.member_mapping_dictionary[member_mapping_id] = member_mapping
                        

    def find_member_mapping_with_id(self,context,member_mapping_id):
        # mappingList = context.memberMappingModule.memberMappings
        # return_member_mapping = None
        # for mapping in mappingList:
        #    if mapping.name == member_mapping_id:
        #        return_member_mapping = mapping

        # return return_member_mapping
        try:
            return context.member_mapping_dictionary[member_mapping_id]
        except:
            return None
                             
    def find_member_with_id(self,element_id,context):
        # memberList = context.members.members
        # returnMember = None
        # for mem in memberList:
        #    if element_id == mem.name:
        #        returnMember = mem

        # return returnMember
        try:
            return context.member_dictionary[element_id]
        except:
            return None
    
    
    
    def find_variable_with_id(self,context, element_id):
        # variableList = context.variables.variables
        # returnVariable = None
        # for var in variableList:
           
        #     if element_id == var.name:
        #         returnVariable = var
   
        # return returnVariable
        try:
            return context.variable_dictionary[element_id]
        except:
            return None

    def find_combination_with_id(self, context, element_id):
        # returnCombination = None
        # for com in context.combinations.combinations:
        #     if element_id == com.combination_id:
        #         returnCombination = com
        # return returnCombination
        try:
            return context.combinations_dictionary[element_id]
        except:
            return None

    def find_table_with_id(self, context, table_id):
        '''
        get the report table with the given id
        '''
        for table in context.report_tables.reportTables:
            if table.table_id == ImportSDD.replace_dots(self, table_id):
                return table
            
    def find_axis_with_id(self, context, axis_id):
        '''
        get the report table with the given id
        '''
        for axis in context.axes.axes:
            if axis.axis_id == ImportSDD.replace_dots(self, axis_id):
                return axis
            
    def find_table_cell_with_id(self, context, table_cell_id):
        '''
        get the report table with the given id
        '''
        # for table_cell in context.table_cells.tableCells:
        #     if table_cell.cell_id == table_cell_id:
        #         return table_cell       
        
        try:
            return context.table_cell_dictionary[table_cell_id]
        except:
            return None
            
    def find_axis_ordinate_with_id(self, context, axis_ordinate_id):
        '''
        get the report table with the given id
        '''
        # for axis_ordinate in context.axis_ordinates.axis_ordinates:
        #     if axis_ordinate.axis_ordinate_id == axis_ordinate_id:
        #         return axis_ordinate_id
            
        try:
            return context.axis_ordinate_dictionary[axis_ordinate_id]
        except:
            return None
            
            
    def get_domain_with_id(self, context, domain_id_string):
        '''
        get the domain with the given id
        '''
        for domain in context.domains.domains:
            if domain.domain_id == domain_id_string:
                return domain
            
    def find_variable_set_with_id(self, context, variable_set_id):
        '''
        get the VariableSet with the given id
        '''
        for variable_set in context.variable_sets.variableSets:
            if variable_set.variable_set_id == variable_set_id:
                return variable_set

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
        
