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
from whitebox.sdd_models import *
from whitebox.context.csv_column_index_context import ColumnIndexes


class ImportWebsiteToSDDModel(object):
    '''
    Class responsible for the import of  SDD csv files
    into an instance of the analaysis model
    '''
    def import_sdd(self,sdd_context):
        '''
        Import SDD csv files into an instance of the analysis model
        '''
        
        ImportWebsiteToSDDModel.create_maintenance_agencies(self, sdd_context)
        ImportWebsiteToSDDModel.create_frameworks(self, sdd_context)
        ImportWebsiteToSDDModel.create_all_domains(self, sdd_context)
        ImportWebsiteToSDDModel.create_all_members(self, sdd_context)
        ImportWebsiteToSDDModel.create_all_subdomains(self, sdd_context)
        ImportWebsiteToSDDModel.create_all_subdomain_enumerations(self, sdd_context)
        ImportWebsiteToSDDModel.create_all_variables(self, sdd_context)
        ImportWebsiteToSDDModel.create_all_variable_sets(self, sdd_context)

        #ImportWebsiteToSDDModel.create_all_combinations(self, sdd_context)
        ImportWebsiteToSDDModel.create_all_member_hierarchies(self, sdd_context)
        ImportWebsiteToSDDModel.create_all_member_hierarchies_nodes(self, sdd_context)
        ImportWebsiteToSDDModel.create_all_variable_mappings(self, sdd_context)
        ImportWebsiteToSDDModel.create_all_variable_mapping_items( self, sdd_context)
        ImportWebsiteToSDDModel.create_member_mappings(self, sdd_context)
        ImportWebsiteToSDDModel.create_all_member_mappings_items(self, sdd_context)
        ImportWebsiteToSDDModel.create_all_mapping_definitions(self, sdd_context)
        ImportWebsiteToSDDModel.create_all_variable_set_mapping(self, sdd_context)

        ImportWebsiteToSDDModel.create_all_mapping_to_cubes(self, sdd_context)
        ImportWebsiteToSDDModel.create_report_tables(self, sdd_context)
        ImportWebsiteToSDDModel.create_table_cells(self, sdd_context)
        ImportWebsiteToSDDModel.create_axis(self, sdd_context)
        ImportWebsiteToSDDModel.create_axis_ordinates(self, sdd_context)
        ImportWebsiteToSDDModel.create_ordinate_items(self, sdd_context)
        ImportWebsiteToSDDModel.create_cell_positions(self, sdd_context)

    def create_maintenance_agencies(self, context):
        
        file_location = context.file_directory + os.sep + "maintenance_agency.csv"
        header_skipped = False

        with open(file_location,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if not header_skipped:
                    header_skipped = True
                else:
                    code = row[ColumnIndexes().maintenance_agency_code]
                    description = row[ColumnIndexes().maintenance_agency_description]
                    id = row[ColumnIndexes().maintenance_agency_id]
                    name = row[ColumnIndexes().maintenance_agency_name]

                    maintenance_agency = MAINTENANCE_AGENCY(
                        name=ImportWebsiteToSDDModel.replace_dots(self, id))
                    maintenance_agency.code = code

                    maintenance_agency.description = description
                    maintenance_agency.maintenance_agency_id = ImportWebsiteToSDDModel.replace_dots(self, id)
                    if context.save_sdd_to_db:  
                        maintenance_agency.save()
                    context.agency_dictionary[id] = maintenance_agency
        
    def create_frameworks(self, context):
        
        file_location = context.file_directory + os.sep + "framework.csv"
        header_skipped = False

        with open(file_location,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if not header_skipped:
                    header_skipped = True
                else:
                    code = row[ColumnIndexes().framework_code]
                    description = row[ColumnIndexes().framework_description]
                    id = row[ColumnIndexes().framework_id]
                    name = row[ColumnIndexes().framework_name]

                    framework = FRAMEWORK(
                        name=ImportWebsiteToSDDModel.replace_dots(self, id))
                    framework.code = code

                    framework.description = description
                    framework.framework_id = ImportWebsiteToSDDModel.replace_dots(self, id)
                    if context.save_sdd_to_db:  
                        framework.save()
                    context.framework_dictionary[ImportWebsiteToSDDModel.replace_dots(self, id)] = framework    
                
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
                    code = row[ColumnIndexes().domain_domain_id_index]
                    data_type = row[ColumnIndexes().domain_domain_data_type]
                    description = row[ColumnIndexes().domain_domain_description]
                    domain_id = row[ColumnIndexes().domain_domain_true_id]
                    is_enumerated = row[ColumnIndexes().domain_domain_is_enumerated]
                    is_reference = row[ColumnIndexes().domain_domain_is_reference]
                    # domainName = Utils.make_valid_id(row[3])
                    domain_name = row[ColumnIndexes().domain_domain_name_index]
                    context.domain_to_domain_name_map[domain_id] = domain_name

                    # creat the Domain abject and set its fields
                    domain = DOMAIN(
                        name=ImportWebsiteToSDDModel.replace_dots(self, domain_id))
                    domain.code = code

                    domain.description = description
                    domain.domain_id = ImportWebsiteToSDDModel.replace_dots(self, domain_id)
                    domain.displayName = domain_name
                    if is_enumerated:
                        domain.is_enumerated = True
                    else:
                        domain.is_enumerated = False

                    if is_enumerated:
                        domain.is_reference = True
                    else:
                        domain.is_reference = False

                    if context.save_sdd_to_db:  
                        domain.save()
                    context.domain_dictionary[domain_id] = domain


    def create_all_members(self, context):
        '''
        import all the members
        '''
        file_location = context.file_directory + os.sep + "member.csv"
        header_skipped = False

        with open(file_location,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if not header_skipped:
                    header_skipped = True
                else:
                    code = row[ColumnIndexes().member_member_code_index]
                    description = row[ColumnIndexes().member_member_descriptions]
                    domain_id = row[ColumnIndexes().member_domain_id_index]
                    member_id = row[ColumnIndexes().member_member_id_index]
                    member_name = row[ColumnIndexes().member_member_name_index]
                    if (member_name is None) or (member_name == ""):
                        member_name = member_id
                    member = MEMBER(
                        name=ImportWebsiteToSDDModel.replace_dots(self, member_id))
                    member.member_id = ImportWebsiteToSDDModel.replace_dots(self, member_id)
                    member.code = code
                    member.description = description
                    member.displayName = member_name
                    domain = ImportWebsiteToSDDModel.get_domain_with_id(
                        self, context, domain_id)
                    member.domain_id = domain
                    if context.save_sdd_to_db:  
                        member.save()
                    context.member_dictionary[member_id] = member
                    
                    #create a dictionary that is useful later
                    if not (domain_id is None) and not (domain_id == ""):
                        context.member_id_to_domain_map[member] = domain
                        context.member_id_to_member_name_map[member_id] = member_name
                        context.member_id_to_member_code_map[member_id] = code

    def create_all_variables(self, context):
        '''
        import all the variables
        '''
        file_location = context.file_directory + os.sep + "variable.csv"
        header_skipped = False

        with open(file_location,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if not header_skipped:
                    header_skipped = True
                else:
                    code = row[ColumnIndexes().variable_code_index]
                    description = row[ColumnIndexes().variable_variable_description]
                    domain_id = row[ColumnIndexes().variable_domain_index]
                    name = row[ColumnIndexes().variable_long_name_index]
                    variable_id = row[ColumnIndexes().variable_variable_true_id]
                    primary_concept = row[ColumnIndexes().variable_primary_concept]
                    
                    variable = VARIABLE(
                        name=ImportWebsiteToSDDModel.replace_dots(self, variable_id))
                    variable.code = code
                    variable.variable_id = ImportWebsiteToSDDModel.replace_dots(
                        self, variable_id)
                    variable.displayName = name
                    domain = ImportWebsiteToSDDModel.get_domain_with_id(self, context, domain_id)
                    variable.domain_id =domain
                    variable.description = description

                    if context.save_sdd_to_db:  
                        variable.save()
                    context.variable_dictionary[variable_id] = variable
                    
                    #set up some useful dictionaries for later.
                    context.variable_to_domain_map[variable_id] = domain
                    context.variable_to_long_names_map[variable_id] = name
                    if not((primary_concept == "") or (primary_concept == None)):
                        context.variable_to_primary_concept_map[variable_id] = primary_concept
                        

    def create_all_variable_sets(self, context):
        '''
        import all the variables
        '''
        file_location = context.file_directory + os.sep + "variable_set.csv"
        header_skipped = False

        with open(file_location,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if not header_skipped:
                    header_skipped = True
                else:
                    variable_set_id = row[ColumnIndexes().variable_set_variable_set_id]
                    maintenance_agency_id   = row[ColumnIndexes().varaible_set_maintenance_agency_id]
                    name = row[ColumnIndexes().variable_set_name]
                    code = row[ColumnIndexes().variable_set_code]
                    description = row[ColumnIndexes().variable_set_description]

                    variable_set = VARIABLE_SET()
                    variable_set.variable_set_id = variable_set_id
                    variable_set.code = code
                    variable_set.description = description
                    variable_set.name = name
                    maintenance_agency = ImportWebsiteToSDDModel.find_maintenance_agency_with_id(self,context,maintenance_agency_id)
                    variable_set.maintenance_agency_id = maintenance_agency

                    
                    if context.save_sdd_to_db:  
                        variable_set.save()
                    context.variable_set_dictionary[variable_set_id]    = variable_set

        file_location = context.file_directory + os.sep + "variable_set_enumeration.csv"
        header_skipped = False
        variable_set_to_variable_map = {}
        with open(file_location,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if not header_skipped:
                    header_skipped = True
                else:
                    valid_to = row[ColumnIndexes().variable_set_enumeration_valid_to]
                    variable_id = row[ColumnIndexes().variable_set_enumeration_variable_id]
                    subdomain_id = row[ColumnIndexes().variable_set_enumeration_subdomain_id]
                    variable_set = row[ColumnIndexes().variable_set_enumeration_valid_set]

                    variable_set_enumeration = VARIABLE_SET_ENUMERATION()
                    variable_set_enumeration.variable_id = \
                            ImportWebsiteToSDDModel.find_variable_with_id(self,context,variable_id)
                    variable_set_enumeration.subdomain_id = \
                            ImportWebsiteToSDDModel.get_subdomain_with_id(self,context,subdomain_id)
                    variable_list = None
                    variable_set_enumeration.variable_set_id = ImportWebsiteToSDDModel.find_variable_set_with_id(self,context,variable_set)                        

                    try:
                        variable_list = variable_set_to_variable_map[variable_set]
                    except KeyError:
                        variable_list = []
                        variable_set_to_variable_map[variable_set] = variable_list

                    if not variable_set_enumeration in variable_list:
                        variable_list.append(variable_set_enumeration)

                    if not variable_id in variable_list:
                        if context.save_sdd_to_db:  
                            variable_set_enumeration.save()
                        

    def create_all_subdomains(self, context):
        '''
        import all the subdomains
        '''
        file_location = context.file_directory + os.sep + "subdomain.csv"
        header_skipped = False

        with open(file_location,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if not header_skipped:
                    header_skipped = True
                else:
                    code = row[ColumnIndexes().subdomain_subdomain_code]
                    description = row[ColumnIndexes().subdomain_subdomain_description]
                    domain_id = row[ColumnIndexes().subdomain_domain_id_index]
                    name = row[ColumnIndexes().subdomain_subdomain_name]
                    subdomain_id = row[ColumnIndexes().subdomain_subdomain_id_index]

                    sub_domain = SUBDOMAIN()
                    sub_domain.code = code
                    sub_domain.subdomain_id = ImportWebsiteToSDDModel.replace_dots(
                        self, subdomain_id)
                    sub_domain.displayName = name
                    sub_domain.description = description

                    domain = ImportWebsiteToSDDModel.get_domain_with_id(self, context, domain_id)
                    sub_domain.domain_id = domain
                    if context.save_sdd_to_db:  
                        sub_domain.save()
                    context.subdomain_dictionary[subdomain_id] = sub_domain

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
                    member_id = row[ColumnIndexes().subdomain_enumeration_member_id_index]
                    subdomain_id = row[ColumnIndexes().subdomain_enumeration_subdomain_id_index]
                    valid_to = row[ColumnIndexes().subdomain_enumeration_valid_to_index]

                    
                    subdomain = ImportWebsiteToSDDModel.get_subdomain_with_id(self, context, subdomain_id)
                    domain = subdomain.domain_id
                    member = ImportWebsiteToSDDModel.find_member_with_id(
                        self, member_id, context)
                    subdomain_enum = SUBDOMAIN_ENUMERATION()
                    subdomain_enum.member_id = member
                    subdomain_enum.subdomain_id = subdomain
                    subdomain_to_items_map = context.subdomain_to_items_map
                    try:
                        items = subdomain_to_items_map[subdomain_id]
                    except KeyError:
                        items = []
                        subdomain_to_items_map[subdomain_id] = items
                    if not member in items:
                        items.append(member)
                        
                    if context.save_sdd_to_db:  
                        subdomain_enum.save()
        
    

                        
    
    def create_all_member_hierarchies(self, context):
        file_location = context.file_directory + os.sep + "member_hierarchy.csv"
        header_skipped = False
        # or each attribute add an Xattribute to the correct ELClass represtnting the Entity
        # the attribute should have the correct type, which may be a specific
        # enumeration

        with open(file_location,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if not header_skipped:
                    header_skipped = True
                else:                    
                    maintenance_agency_id = row[ColumnIndexes().member_hierarchy_maintenance_agency]
                    code = row[ColumnIndexes().member_hierarchy_code]
                    id = row[ColumnIndexes().member_hierarchy_id]
                    domain_id = row[ColumnIndexes().member_hierarchy_domain_id]
                    is_main_hierarchy = row[ColumnIndexes().member_hierarchy_is_main_hierarchy]
                    name = row[ColumnIndexes().member_hierarchy_name]
                    description = row[ColumnIndexes().member_hierarchy_description]
                    
                    maintenance_agency = ImportWebsiteToSDDModel.find_maintenance_agency_with_id(self,context,maintenance_agency_id)
                    domain = ImportWebsiteToSDDModel.get_domain_with_id(self,context,domain_id)
                    hierarchy = MEMBER_HIERARCHY(name=ImportWebsiteToSDDModel.replace_dots(self, id))
                    hierarchy.member_hierarchy_id = ImportWebsiteToSDDModel.replace_dots(self, id)
                    hierarchy.code = code
                    hierarchy.description = description
                    hierarchy.maintenance_agency_id = maintenance_agency
                    hierarchy.domain_id = domain
                        
                    context.member_hierarchy_dictionary[id] = hierarchy
                    if context.save_sdd_to_db:  
                        hierarchy.save()
                    
    def create_all_member_hierarchies_nodes(self, context):
        file_location = context.file_directory + os.sep + "member_hierarchy_node.csv"
        header_skipped = False
        # or each attribute add an Xattribute to the correct ELClass represtnting the Entity
        # the attribute should have the correct type, which may be a specific
        # enumeration

        with open(file_location,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if not header_skipped:
                    header_skipped = True
                else:             
                    hierarchy_id = row[ColumnIndexes().member_hierarchy_node_hierarchy_id]
                    member_id = row[ColumnIndexes().member_hierarchy_node_member_id]
                    parent_member_id = row[ColumnIndexes().member_hierarchy_node_parent_member_id]
                    node_level = row[ColumnIndexes().member_hierarchy_node_level]
                    comparator = row[ColumnIndexes().member_hierarchy_node_comparator]
                    operator = row[ColumnIndexes().member_hierarchy_node_operator]
                    valid_from = row[ColumnIndexes().member_hierarchy_node_valid_from]
                    valid_to = row[ColumnIndexes().member_hierarchy_node_valid_to]
                    
                    member = ImportWebsiteToSDDModel.find_member_with_id(self,member_id,context)
                    parent_member = ImportWebsiteToSDDModel.find_member_with_id(self,parent_member_id,context)
                    
                    hierarchy = ImportWebsiteToSDDModel.find_member_hierarchy_with_id(self,hierarchy_id,context)
                    hierarchy_node = MEMBER_HIERARCHY_NODE()
                    hierarchy_node.member_hierarchy_id = hierarchy
                    hierarchy_node.comparator = comparator
                    hierarchy_node.operator = operator
                    hierarchy_node.member_id = member
                    hierarchy_node.level = int(node_level)
                    hierarchy_node.parent_member_id = parent_member
                    context.member_hierarchy_node_dictionary[hierarchy_id + ":" + member_id] = hierarchy_node
                    if context.save_sdd_to_db:  
                        hierarchy_node.save()
                        

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
                    table_id = row[ColumnIndexes().table_table_id]
                    name = row[ColumnIndexes().table_table_id]
                    display_name = row[ColumnIndexes().table_table_name]
                    code = row[ColumnIndexes().table_code]
                    description = row[ColumnIndexes().table_description]
                    maintenance_agency_id = row[ColumnIndexes().table_maintenance_agency_id]
                    version = row[ColumnIndexes().table_version]
                    valid_from = row[ColumnIndexes().table_valid_from]
                    valid_to = row[ColumnIndexes().table_valid_to]

                    table = TABLE(
                        name=ImportWebsiteToSDDModel.replace_dots(self, table_id))
                    table.table_id = ImportWebsiteToSDDModel.replace_dots(self, table_id)
                    table.displayName = display_name
                    table.code = code
                    table.description = description
                    maintenance_agency = ImportWebsiteToSDDModel.find_maintenance_agency_with_id(self,context,maintenance_agency_id)
                    table.maintenance_agency_id = maintenance_agency
                    table.version = version

                    if context.save_sdd_to_db:  
                        table.save()
                    context.report_tables_dictionary[table.table_id] = table
                    
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
                    axis_id = row[ColumnIndexes().axis_id]
                    axis_orientation = row[ColumnIndexes().axis_orientation]
                    axis_order = row[ColumnIndexes().axis_order]
                    axis_name = row[ColumnIndexes().axis_name]
                    axis_description = row[ColumnIndexes().axis_description]
                    axis_table_id = row[ColumnIndexes().axis_table_id]
                    axis_is_open_axis = row[ColumnIndexes().axis_is_open_axis]
                    
                    
                    axis = AXIS(
                        name=ImportWebsiteToSDDModel.replace_dots(self, axis_id))
                    axis.axis_id = ImportWebsiteToSDDModel.replace_dots(self, axis_id)
                    axis.orientation = axis_orientation
                    # not needed yet axis.order = axis_order
                    axis.description = axis_description
                    axis.table_id = ImportWebsiteToSDDModel.find_table_with_id(self, context, axis_table_id)
                    # not needed yet
                    # axis.is_open_axis = axis_is_open_axis
                                        
                    if context.save_sdd_to_db:  
                        axis.save()  
                    context.axis_dictionary[axis_id] = axis           
                   
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
                    axis_ordinate_id = row[ColumnIndexes().axis_ordinate_id]
                    axis_ordinate_is_abstract_header = row[
                            ColumnIndexes().axis_ordinate_is_abstract_header]
                    axis_ordinate_code = row[ColumnIndexes().axis_ordinate_code]
                    axis_ordinate_order = row[ColumnIndexes().axis_ordinate_order]
                    axis_ordinate_path = row[ColumnIndexes().axis_ordinate_path]
                    axis_ordinate_axis_id = row[ColumnIndexes().axis_ordinate_axis_id]
                    axis_ordinate_parent_axis_ordinate_id = row[
                            ColumnIndexes().axis_ordinate_parent_axis_ordinate_id]
                    axis_ordinate_name = row[ColumnIndexes().axis_ordinate_name]
                    axis_ordinate_description = row[ColumnIndexes().axis_ordinate_description]

                    axis_ordinate = AXIS_ORDINATE(
                        name=ImportWebsiteToSDDModel.replace_dots(self, axis_ordinate_id))
                    axis_ordinate.axis_ordinate_id = ImportWebsiteToSDDModel.replace_dots(self, axis_ordinate_id)
                    # not needed yet
                    # axis_ordinate.is_abstract_header = axis_ordinate_is_abstract_header
                    axis_ordinate.code = axis_ordinate_code
                    # not needed yet axis_ordinate.order = axis_ordinate_order
                    axis_ordinate.path = axis_ordinate_path
                    axis_ordinate.axis_id = ImportWebsiteToSDDModel.find_axis_with_id(self,
                                                    context, ImportWebsiteToSDDModel.replace_dots(self,axis_ordinate_axis_id))
                    # we don't need the parent axis yet in our processing.
                    # axis_ordinate.axis_ordinate_id = 
                    # axis_ordinate_parent_axis_ordinate_id
                    axis_ordinate.displayName = axis_ordinate_name
                    axis_ordinate.description = axis_ordinate_description
                    if context.save_sdd_to_db:  
                        axis_ordinate.save()
                    context.axis_ordinate_dictionary[axis_ordinate_id] = axis_ordinate

    def create_ordinate_items(self, context):
        '''
        import all the axis_ordinate from the rendering package
        '''
        file_location = context.file_directory + os.sep + "ordinate_item.csv"
        header_skipped = False

        with open(file_location,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if not header_skipped:
                    header_skipped = True
                else:
                    axis_ordinate_id = row[ColumnIndexes().ordinate_item_axis_ordinate_id]
                    variable_id = row[ColumnIndexes().ordinate_item_variable_id]
                    member_id = row[ColumnIndexes().ordinate_item_member_id]
                    member_hierarchy_id = row[ColumnIndexes().ordinate_item_member_hierarchy_id]
                    member_hierarchy_valid_from = row[ColumnIndexes().ordinate_item_member_hierarchy_valid_from]
                    starting_member_id = row[ColumnIndexes().ordinate_item_starting_member_id]
                    is_starting_member_included = row[ColumnIndexes().ordinate_item_is_starting_member_included]


                    ordinate_item = ORDINATE_ITEM()
                    ordinate_item.axis_ordinate_id = ImportWebsiteToSDDModel.find_axis_ordinate_with_id(
                        self, context, axis_ordinate_id)
                    ordinate_item.variable_id = ImportWebsiteToSDDModel.find_variable_with_id(
                        self, context, variable_id)
                    ordinate_item.member_id = ImportWebsiteToSDDModel.find_member_with_id(
                        self, member_id, context)
                    ordinate_item.member_hierarchy_id = ImportWebsiteToSDDModel.find_member_hierarchy_with_id(
                        self, member_hierarchy_id, context)
                    ordinate_item.starting_member_id = ImportWebsiteToSDDModel.find_member_with_id(
                        self, starting_member_id, context)
                    ordinate_item.is_starting_member_included = is_starting_member_included
                    if context.save_sdd_to_db:
                        ordinate_item.save()
                    
                    try:
                        ordinate_items = context.axis_ordinate_to_ordinate_items_map[axis_ordinate_id]
                        ordinate_items.append(ordinate_item)
                    except KeyError:
                        context.axis_ordinate_to_ordinate_items_map[axis_ordinate_id] = [ordinate_item]

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
                    table_cell_cell_id = row[ColumnIndexes().table_cell_cell_id]
                    table_cell_is_shaded = row[ColumnIndexes().table_cell_is_shaded]
                    table_cell_combination_id = row[ColumnIndexes().table_cell_combination_id]
                    table_cell_table_id = row[ColumnIndexes().table_cell_table_id]
                    table_cell_system_data_code = row[ColumnIndexes().table_cell_system_data_code]

                    # we only look at the reference combinations, they all end with the string _REF
                    if table_cell_cell_id.endswith("_REF"):
                        table_cell = TABLE_CELL(
                            name=ImportWebsiteToSDDModel.replace_dots(self, table_cell_cell_id))
                        table_cell.cell_id = ImportWebsiteToSDDModel.replace_dots(self, table_cell_cell_id)
                        # table_cell_is_shaded not needed yet
                        # 
                        
                        table_cell.table_id = ImportWebsiteToSDDModel.\
                            find_table_with_id(self, context,ImportWebsiteToSDDModel.replace_dots(self,table_cell_table_id))

                        if context.save_sdd_to_db:  
                            table_cell.save()
                        context.table_cell_dictionary[table_cell.cell_id] = table_cell
                        
                        table_cell_list = []
                        try:
                            table_cell_list = context.table_to_table_cell_dictionary[table_cell.table_id]
                        except KeyError:
                            context.table_to_table_cell_dictionary[table_cell.table_id] = table_cell_list

                        table_cell_list.append(table_cell)
                        
                        

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
                    cell_positions_cell_id = row[ColumnIndexes().cell_positions_cell_id]
                    cell_positions_axis_ordinate_id = row[ColumnIndexes().cell_positions_axis_ordinate_id]
                    
                    if cell_positions_cell_id.endswith("_REF"):
                    # check if we already have the table cell created
                        cell_position = CELL_POSITION()
                            

                        cell_position.axis_ordinate_id = \
                                ImportWebsiteToSDDModel.find_axis_ordinate_with_id(  
                                    self, context, cell_positions_axis_ordinate_id)
                        cell_position.cell_id = ImportWebsiteToSDDModel.find_table_cell_with_id(
                                                self, context, ImportWebsiteToSDDModel.replace_dots( self,cell_positions_cell_id))
                        if context.save_sdd_to_db:  
                            cell_position.save()

                        try:
                            cell_positions_list = context.cell_positions_dictionary[cell_positions_cell_id]
                            cell_positions_list.append(cell_position)
                        except KeyError:
                            context.cell_positions_dictionary[cell_positions_cell_id] = [cell_position]
                    
    def create_member_mappings(self, context):
        ''' import all the member mappings from the rendering package'''
        file_location = context.file_directory + os.sep + "member_mapping.csv"
        header_skipped = False

        with open(file_location,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if not header_skipped:
                    header_skipped = True
                else:
                    maintenance_agency_id = row[ColumnIndexes().member_map_maintenance_agency_id]
                    member_mapping_id = row[ColumnIndexes().member_map_member_mapping_id]
                    name = row[ColumnIndexes().member_map_name]
                    code = row[ColumnIndexes().member_map_code]
                    member_mapping = MEMBER_MAPPING()
                    member_mapping.member_mapping_id = member_mapping_id
                    member_mapping.name = name
                    member_mapping.code = code
                    member_mapping.maintenance_agency_id = ImportWebsiteToSDDModel.find_maintenance_agency_with_id(
                        self,context,maintenance_agency_id)
                    if context.save_sdd_to_db:
                        member_mapping.save()
                    context.member_mapping_dictionary[member_mapping_id] = member_mapping



    def create_all_member_mappings_items(self, context):
        ''' import all the member mappings from the rendering package'''
        file_location = context.file_directory + os.sep + "member_mapping_item.csv"
        header_skipped = False

        with open(file_location,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if not header_skipped:
                    header_skipped = True
                else:
                    member_mapping_id = row[ColumnIndexes().member_mapping_id]
                    row_number = row[ColumnIndexes().member_mapping_row]
                    variable_id = row[ColumnIndexes().member_mapping_variable_id]
                    is_source = row[ColumnIndexes().member_mapping_is_source]

                    member_id = row[ColumnIndexes().member_mapping_member_id]
                    
                    member_mapping_item = MEMBER_MAPPING_ITEM()
                    member_mapping_item.isSource = is_source
                    member_mapping_item.member = ImportWebsiteToSDDModel.find_member_with_id(
                                                        self,member_id,context)
                    member_mapping_item.variable = ImportWebsiteToSDDModel.find_variable_with_id(
                                                        self,context,variable_id)
                    member_mapping_item.row = row_number

                    member_mapping_item.member_mapping_id  = ImportWebsiteToSDDModel.find_member_mapping_with_id(
                                        self,context,member_mapping_id)
                    
                        
                    if context.save_sdd_to_db:  
                        member_mapping_item.save()
                    try:
                        member_mapping_items_list = context.member_mapping_items_dictionary[member_mapping_id]
                        member_mapping_items_list.append(member_mapping_item)
                    except KeyError:
                        context.member_mapping_items_dictionary[member_mapping_id] = [member_mapping_item]


    def create_all_mapping_definitions(self, context):
        file_location = context.file_directory + os.sep + "mapping_definition.csv"
        header_skipped = False

        with open(file_location,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if not header_skipped:
                    header_skipped = True
                else:
                    mapping_definition_id = row[ColumnIndexes().mapping_definition_id]
                    mapping_definition_name = row[ColumnIndexes().mapping_definition_name]
                    mapping_definition_code = row[ColumnIndexes().mapping_definition_code]
                    mapping_definition_mapping_type = row[ColumnIndexes().mapping_definition_mapping_type]
                    mapping_definition_member_mapping_id = row[ColumnIndexes().mapping_definition_member_mapping_id]
                    mapping_definition_variable_mapping_id = row[ColumnIndexes().mapping_definition_variable_mapping_id]

                    mapping_definition = MAPPING_DEFINITION()
                    mapping_definition.mapping_id = mapping_definition_id
                    mapping_definition.name = mapping_definition_name
                    mapping_definition.code = mapping_definition_code
                    mapping_definition.mapping_type = mapping_definition_mapping_type
                   
                    mapping_definition.memberMapping = ImportWebsiteToSDDModel.find_member_mapping_with_id(
                        self,context,mapping_definition_member_mapping_id)
                    mapping_definition.variableMapping = ImportWebsiteToSDDModel.find_variable_mapping_with_id(
                        self,context,mapping_definition_variable_mapping_id)

                    if context.save_sdd_to_db:  
                        mapping_definition.save()
                    context.mapping_definition_dictionary[mapping_definition_id] = mapping_definition

    def create_all_mapping_to_cubes(self, context):
        filelocation = context.file_directory + os.sep + "mapping_to_cube.csv"
        header_skipped = False
        with open(filelocation,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if not header_skipped:
                    header_skipped = True
                else:
                    mapping_to_cube_mapping_id = row[ColumnIndexes().mapping_to_cube_mapping_id]
                    mapping_to_cube_cube_mapping_id = row[ColumnIndexes().mapping_to_cube_cube_mapping_id]
                    mapping_to_cube_valid_from = row[ColumnIndexes().mapping_to_cube_valid_from]
                    mapping_to_cube_valid_to = row[ColumnIndexes().mapping_to_cube_valid_to]

                    mapping_to_cube = MAPPING_TO_CUBE()
                    mapping_to_cube.mapping = ImportWebsiteToSDDModel.find_mapping_definition_with_id(
                        self,context,mapping_to_cube_mapping_id)
                    mapping_to_cube.cubeMapping = ImportWebsiteToSDDModel.replace_dots(self,mapping_to_cube_cube_mapping_id)
                    mapping_to_cube.valid_from = mapping_to_cube_valid_from
                    mapping_to_cube.valid_to = mapping_to_cube_valid_to

                    if context.save_sdd_to_db:
                        mapping_to_cube.save()
                    try:
                        mapping_to_cube_list = context.mapping_to_cube_dictionary[mapping_to_cube.cubeMapping]
                        mapping_to_cube_list.append(mapping_to_cube)
                    except KeyError:
                        context.mapping_to_cube_dictionary[mapping_to_cube.cubeMapping] = [mapping_to_cube]
                    

    def create_all_variable_mappings(self, context):
        filelocation = context.file_directory + os.sep + "variable_mapping.csv"
        header_skipped = False
        with open(filelocation,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if not header_skipped:
                    header_skipped = True
                else:
                    variable_mapping_id = row[ColumnIndexes().variable_mapping_variable_mapping_id]
                    variable_mapping_maintenance_agency_id = row[ColumnIndexes().variable_mapping_maintenance_agency_id]
                    variable_mapping_code = row[ColumnIndexes().variable_mapping_code]
                    variable_mapping_name = row[ColumnIndexes().variable_mapping_name]

                    variable_mapping = VARIABLE_MAPPING()
                    variable_mapping.variable_mapping_id = variable_mapping_id
                    variable_mapping.maintenance_agency_id = ImportWebsiteToSDDModel.find_maintenance_agency_with_id(
                        self,context,variable_mapping_maintenance_agency_id)
                    variable_mapping.code = variable_mapping_code
                    variable_mapping.name = variable_mapping_name
                    if context.save_sdd_to_db:
                        variable_mapping.save()
                    context.variable_mapping_dictionary[variable_mapping_id] = variable_mapping 

    def create_all_variable_mapping_items(self, context):
        fileloacation = context.file_directory + os.sep + "variable_mapping_item.csv"
        header_skipped = False
        with open(fileloacation,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if not header_skipped:
                    header_skipped = True
                else:
                    variable_mapping_item_variable_mapping_id = row[ColumnIndexes().varaible_mapping_item_variable_mapping_id]
                    variable_mapping_item_variable_id = row[ColumnIndexes().variable_mapping_item_variable_id]
                    variable_mapping_item_is_source = row[ColumnIndexes().variable_mapping_item_is_source]
                    variable_mapping_item_valid_from = row[ColumnIndexes().variable_mapping_item_valid_from]
                    variable_mapping_item_valid_to = row[ColumnIndexes().variable_mapping_item_valid_to]

                    variable_mapping_item = VARIABLE_MAPPING_ITEM()
                    variable_mapping_item.variable_mapping_id = ImportWebsiteToSDDModel.find_variable_mapping_with_id(
                        self,context,variable_mapping_item_variable_mapping_id)
                    variable_mapping_item.variable = ImportWebsiteToSDDModel.find_variable_with_id(
                        self,context,variable_mapping_item_variable_id)
                    variable_mapping_item.isSource = variable_mapping_item_is_source
                    variable_mapping_item.valid_from = variable_mapping_item_valid_from
                    variable_mapping_item.valid_to = variable_mapping_item_valid_to

                    if context.save_sdd_to_db:
                        variable_mapping_item.save()
                    try:
                        variable_mapping_items_list = context.variable_mapping_item_dictionary[variable_mapping_item_variable_mapping_id]
                        variable_mapping_items_list.append(variable_mapping_item)
                    except KeyError:                        
                        context.variable_mapping_item_dictionary[variable_mapping_item_variable_mapping_id] = [variable_mapping_item]

    def create_all_variable_set_mapping(self, context):
        filelocation = context.file_directory + os.sep + "variable_set_mapping.csv"
        header_skipped = False
        with open(filelocation,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if not header_skipped:
                    header_skipped = True
                else:
                    variable_set_mapping_source = row[ColumnIndexes().variable_set_mapping_source_mapping_id]
                    variable_set_mapping_target = row[ColumnIndexes().variable_set_mapping_target_mapping_id]

                    variable_set_mapping = VARIABLE_SET_MAPPING()
                    variable_set_mapping.sourceMapping = ImportWebsiteToSDDModel.find_mapping_definition_with_id(
                        self,context,variable_set_mapping_source)
                    variable_set_mapping.targetMapping = ImportWebsiteToSDDModel.find_mapping_definition_with_id(
                        self,context,variable_set_mapping_target)

                    if context.save_sdd_to_db:  
                        variable_set_mapping.save()
                    context.variable_set_mappings.append(variable_set_mapping)

    def find_member_mapping_with_id(self,context,member_mapping_id):
        ''' find an existing member mapping with this id'''
        try:
            return context.member_mapping_dictionary[member_mapping_id]
        except KeyError:
            return None

    def find_member_with_id(self,element_id,context):
        ''' find an existing member with this id'''
        try:
            return context.member_dictionary[element_id]
        except KeyError:
            return None
        
    def find_member_hierarchy_with_id(self,element_id,context):
        ''' find an existing member with this id'''
        try:
            return context.member_hierarchy_dictionary[element_id]
        except KeyError:
            return None
    
    def find_variable_with_id(self,context, element_id):
        ''' find an existing variable with this id'''
        try:
            return context.variable_dictionary[element_id]
        except KeyError:
            return None

    

    def find_table_with_id(self, context, table_id):
        '''
        get the report table with the given id
        ''' 
        try:
            return context.report_tables_dictionary[table_id]
        except KeyError:
            return None
            
    def find_axis_with_id(self, context, axis_id):
        '''
        get the axis with the given id
        '''
        try:
            return context.axis_dictionary[axis_id]
        except KeyError:
            return None
            
    def find_table_cell_with_id(self, context, table_cell_id):
        '''
        get the table cell with the given id 
        '''
        try:
            return context.table_cell_dictionary[table_cell_id]
        except KeyError:
            return None
            
    def find_axis_ordinate_with_id(self, context, axis_ordinate_id):
        '''
        get the exis ordinate with the given id
        '''
        try:
            return context.axis_ordinate_dictionary[axis_ordinate_id]
        except KeyError:
            return None
    def find_variable_mapping_with_id(self, context, variable_mapping_id):
        '''
        get the variable mapping with the given id
        '''
        try:
            return context.variable_mapping_dictionary[variable_mapping_id]
        except KeyError:
            return None
    def get_domain_with_id(self, context, domain_id_string):
        '''
        get the domain with the given id
        '''
        try:
            return context.domain_dictionary[domain_id_string]
        except KeyError:
            return None
            
    def find_variable_set_with_id(self, context, variable_set_id):
        '''
        get the VariableSet with the given id
        '''
        try:
            return context.variable_set_dictionary[variable_set_id]
        except KeyError:
            return None
    

        
    def find_mapping_definition_with_id(self, context, mapping_definition_id):
        '''
        get the mapping definition with the given id
        '''
        try:
            return context.mapping_definition_dictionary[mapping_definition_id]
        except KeyError:
            return None
        
    def get_subdomain_with_id(self, context, subdomain_id_string):
        '''
        get the subdomain with the given id
        '''
        return context.subdomain_dictionary[subdomain_id_string]


    def find_maintenance_agency_with_id(self,context,maintenance_agency_id):
    
        try:
            return context.agency_dictionary[maintenance_agency_id]
        except KeyError:
            return None
            
    def find_framework_with_id(self,context,framework_id):
    
        try:
            return context.framework_dictionary[framework_id]
        except KeyError:
            return None
            

    def replace_dots(self, text):
        '''
        replace dots with underscores
        '''
        return text.replace('.', '_')

