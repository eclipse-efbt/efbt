# coding=UTF-8#
# Copyright (c) 2024 Bird Software Solutions Ltd
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
from django.conf import settings
from pybirdai.sdd_models import *
from pybirdai.context.csv_column_index_context import ColumnIndexes


class ImportWebsiteToSDDModel(object):
    '''
    Class responsible for importing SDD csv files into an instance of the analysis model
    '''
    
        

 

    def import_report_templates_from_sdd(self, sdd_context):
        '''
        Import SDD csv files into an instance of the analysis model
        '''
        ImportWebsiteToSDDModel.create_maintenance_agencies(self, sdd_context)
        ImportWebsiteToSDDModel.create_frameworks(self, sdd_context)
        ImportWebsiteToSDDModel.create_all_domains(self, sdd_context,False)
        ImportWebsiteToSDDModel.create_all_members(self, sdd_context,False)
        ImportWebsiteToSDDModel.create_all_variables(self, sdd_context,False)

        ImportWebsiteToSDDModel.create_report_tables(self, sdd_context)
        ImportWebsiteToSDDModel.create_table_cells(self, sdd_context)
        ImportWebsiteToSDDModel.create_axis(self, sdd_context)
        ImportWebsiteToSDDModel.create_axis_ordinates(self, sdd_context)
        ImportWebsiteToSDDModel.create_ordinate_items(self, sdd_context)
        ImportWebsiteToSDDModel.create_cell_positions(self, sdd_context)

  
        

    def import_semantic_integrations_from_sdd(self, sdd_context):
        '''
        Import SDD csv files into an instance of the analysis model
        '''
        ImportWebsiteToSDDModel.delete_mapping_warnings_files(self, sdd_context)
        ImportWebsiteToSDDModel.create_all_variable_mappings(self, sdd_context)
        ImportWebsiteToSDDModel.create_all_variable_mapping_items(self, sdd_context)
        ImportWebsiteToSDDModel.create_member_mappings(self, sdd_context)
        ImportWebsiteToSDDModel.create_all_member_mappings_items(self, sdd_context)
        ImportWebsiteToSDDModel.create_all_mapping_definitions(self, sdd_context)
        ImportWebsiteToSDDModel.create_all_mapping_to_cubes(self, sdd_context)

    def import_hierarchies_from_sdd(self, sdd_context):
        '''
        Import hierarchies from CSV file
        '''
        ImportWebsiteToSDDModel.delete_hierarchy_warnings_files(self, sdd_context)
        ImportWebsiteToSDDModel.create_all_member_hierarchies(self, sdd_context)
        ImportWebsiteToSDDModel.create_all_parent_members_with_children_locally(self, sdd_context)
        ImportWebsiteToSDDModel.create_all_member_hierarchies_nodes(self, sdd_context)

    def create_maintenance_agencies(self, context):
        '''
        Import maintenance agencies from CSV file
        '''
        file_location = context.file_directory + os.sep + "maintenance_agency.csv"
        header_skipped = False

        with open(file_location, encoding='utf-8') as csvfile:
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
        '''
        Import frameworks from CSV file
        '''
        file_location = context.file_directory + os.sep + "framework.csv"
        header_skipped = False

        with open(file_location, encoding='utf-8') as csvfile:
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

    
                    
    def create_all_domains(self, context, ref):
        '''
        Import all domains from CSV file
        '''
        file_location = context.file_directory + os.sep + "domain.csv"
        header_skipped = False

        with open(file_location, encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if not header_skipped:
                    header_skipped = True
                else:
                    maintenence_agency = row[ColumnIndexes().domain_maintenence_agency]
                    code = row[ColumnIndexes().domain_domain_id_index]
                    data_type = row[ColumnIndexes().domain_domain_data_type]
                    description = row[ColumnIndexes().domain_domain_description]
                    domain_id = row[ColumnIndexes().domain_domain_true_id]
                    is_enumerated = row[ColumnIndexes().domain_domain_is_enumerated]
                    is_reference = row[ColumnIndexes().domain_domain_is_reference]
                    domain_name = row[ColumnIndexes().domain_domain_name_index]
                    include = False
                    if  (ref) and (maintenence_agency == "ECB"):
                        include = True
                    if  ( not ref) and not (maintenence_agency == "ECB"):
                        include = True
                    if include:
                        domain = DOMAIN(
                            name=ImportWebsiteToSDDModel.replace_dots(self, domain_id))
                        if ref:
                            maintenance_agency = ImportWebsiteToSDDModel.find_maintenance_agency_with_id(self,context,"SDD_DOMAIN")
                        else:
                            maintenance_agency = ImportWebsiteToSDDModel.find_maintenance_agency_with_id(self,context,maintenence_agency)
                        domain.maintenance_agency_id = maintenance_agency
                        domain.code = code

                        domain.description = description
                        domain.domain_id = ImportWebsiteToSDDModel.replace_dots(self, domain_id)
                        domain.name = domain_name
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

                        if ref:
                            context.ref_domain_dictionary[domain_id] = domain
                        else:
                            context.nonref_domain_dictionary[domain_id] = domain

    def create_all_members(self, context,ref):
        '''
        Import all members from CSV file
        '''
        file_location = context.file_directory + os.sep + "member.csv"
        header_skipped = False

        with open(file_location, encoding='utf-8') as csvfile:
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
                    maintenence_agency = row[ColumnIndexes().member_member_maintenence_agency]
                    if (member_name is None) or (member_name == ""):
                        member_name = member_id
                    include = False
                    if  (ref) and (maintenence_agency == "ECB"):
                        include = True
                    if  ( not ref) and not (maintenence_agency == "ECB"):
                        include = True
                    if include:
                        member = MEMBER(
                            name=ImportWebsiteToSDDModel.replace_dots(self, member_id))
                        member.member_id = ImportWebsiteToSDDModel.replace_dots(self, member_id)
                        member.code = code
                        member.description = description
                        member.name = member_name
                        maintenance_agency = ImportWebsiteToSDDModel.find_maintenance_agency_with_id(self,context,maintenence_agency)
                        member.maintenance_agency_id = maintenance_agency
                        domain = ImportWebsiteToSDDModel.find_domain_with_id(
                            self, context, domain_id)
                        member.domain_id = domain
                        if context.save_sdd_to_db:  
                            member.save()
                        context.nonref_member_dictionary[member_id] = member

                        if not (domain_id is None) and not (domain_id == ""):
                            context.member_id_to_domain_map[member] = domain
                            context.member_id_to_member_code_map[member_id] = code

    def create_all_variables(self, context, ref):
        '''
        Import all variables from CSV file
        '''
        file_location = context.file_directory + os.sep + "variable.csv"
        header_skipped = False

        with open(file_location, encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if not header_skipped:
                    header_skipped = True
                else:
                    maintenence_agency = row[ColumnIndexes().variable_variable_maintenence_agency]
                    code = row[ColumnIndexes().variable_code_index]
                    description = row[ColumnIndexes().variable_variable_description]
                    domain_id = row[ColumnIndexes().variable_domain_index]
                    name = row[ColumnIndexes().variable_long_name_index]
                    variable_id = row[ColumnIndexes().variable_variable_true_id]
                    primary_concept = row[ColumnIndexes().variable_primary_concept]
                    include = False
                    if  (ref) and (maintenence_agency == "ECB"):
                        include = True
                    if  ( not ref) and not (maintenence_agency == "ECB"):
                        include = True
                    if include:
                        variable = VARIABLE(
                            name=ImportWebsiteToSDDModel.replace_dots(self, variable_id))
                        maintenance_agency_id = ImportWebsiteToSDDModel.find_maintenance_agency_with_id(self,context,maintenence_agency)
                        variable.code = code
                        variable.variable_id = ImportWebsiteToSDDModel.replace_dots(
                            self, variable_id)
                        variable.name = name
                        domain = ImportWebsiteToSDDModel.find_domain_with_id(self, context, domain_id)
                        variable.domain_id =domain
                        variable.description = description
                        variable.maintenance_agency_id = maintenance_agency_id

                        if context.save_sdd_to_db:  
                            variable.save()
                        context.nonref_variable_dictionary[variable_id] = variable

                        context.variable_to_domain_map[variable_id] = domain
                        context.variable_to_long_names_map[variable_id] = name
                        if not((primary_concept == "") or (primary_concept == None)):
                            context.variable_to_primary_concept_map[variable_id] = primary_concept

    def create_all_parent_members_with_children_locally(self, context):
        print("Creating all parent members with children locally")
        file_location = context.file_directory + os.sep + "member_hierarchy_node.csv"
        header_skipped = False
        parent_members = []
        parent_members_child_triples = []
        missing_children = []
        with open(file_location, encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if not header_skipped:
                    header_skipped = True
                else:
                    parent_member_id = row[ColumnIndexes().member_hierarchy_node_parent_member_id]
                    member_id = row[ColumnIndexes().member_hierarchy_node_member_id]
                    hierarchy_id = row[ColumnIndexes().member_hierarchy_node_hierarchy_id]
                    hierarchy = ImportWebsiteToSDDModel.find_member_hierarchy_with_id(self,hierarchy_id,context)
                    if hierarchy:
                        domain = hierarchy.domain_id
                        if not (parent_member_id is None) and not (parent_member_id == ""):                 
                            parent_members_child_triples.append((
                                    parent_member_id,member_id,domain)
                                )
                        if not (parent_member_id in parent_members):
                            parent_members.append(parent_member_id)
                    else:
                        print(f"Hierarchy {hierarchy_id} not found")
                        
                            
        for parent_member_id,member_id,domain in parent_members_child_triples:
            # checkif child is a parent node
            # if the node has a child that is a parent node, we ad it to the database
            if member_id in parent_members:
                if not (parent_member_id in context.members_that_are_nodes.keys()):
                    parent_member = MEMBER(name=ImportWebsiteToSDDModel.replace_dots(self, parent_member_id))
                    parent_member.member_id = ImportWebsiteToSDDModel.replace_dots(self, parent_member_id)
                    parent_member.maintenance_agency_id = ImportWebsiteToSDDModel.find_maintenance_agency_with_id(self,context,"NODE")
                    parent_member.domain_id = domain
                    if context.save_sdd_to_db:  
                        parent_member.save()
                        context.members_that_are_nodes[parent_member_id] = parent_member
            else:
                # if the node has a child that is a EXISTING member node, we add it to the database
                member = ImportWebsiteToSDDModel.find_member_with_id(self,member_id,context)
                if not (member is None):
                    if not (parent_member_id in context.members_that_are_nodes.keys()):
                        parent_member = MEMBER(name=ImportWebsiteToSDDModel.replace_dots(self, parent_member_id))
                        parent_member.member_id = ImportWebsiteToSDDModel.replace_dots(self, parent_member_id)
                        parent_member.maintenance_agency_id = ImportWebsiteToSDDModel.find_maintenance_agency_with_id(self,context,"NODE")
                        parent_member.domain_id = domain
                        if context.save_sdd_to_db:  
                            parent_member.save()
                            context.members_that_are_nodes[parent_member_id] = parent_member
                else:
                    # if the node has a child that is a not a parent  member node, 
                    # or existing member then we record a warning
                    missing_children.append((
                        parent_member_id,member_id)
                    )   

            
        for missing_member in missing_children:
            print(f"Parent member {missing_member[0]} has missing child {missing_member[1]}")

        ImportWebsiteToSDDModel.save_missing_children_to_csv(context,missing_children)

    def create_all_member_hierarchies(self, context):
        '''
        Import all member hierarchies from CSV file
        '''
        missing_domains = []
        file_location = context.file_directory + os.sep + "member_hierarchy.csv"
        header_skipped = False

        with open(file_location, encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if not header_skipped:
                    header_skipped = True
                else:
                    maintenance_agency_id = row[ColumnIndexes().member_hierarchy_maintenance_agency]
                    code = row[ColumnIndexes().member_hierarchy_code]
                    id = row[ColumnIndexes().member_hierarchy_id]
                    domain_id = row[ColumnIndexes().member_hierarchy_domain_id]
                    description = row[ColumnIndexes().member_hierarchy_description]

                    maintenance_agency = ImportWebsiteToSDDModel.find_maintenance_agency_with_id(self,context,maintenance_agency_id)
                    
                    domain = ImportWebsiteToSDDModel.find_domain_with_id(self,context,domain_id)
                    if domain is None:
                        print(f"Domain {domain_id} not found")
                        missing_domains.append(domain_id)
                    else:
                        hierarchy = MEMBER_HIERARCHY(name=ImportWebsiteToSDDModel.replace_dots(self, id))
                        hierarchy.member_hierarchy_id = ImportWebsiteToSDDModel.replace_dots(self, id)
                        hierarchy.code = code
                        hierarchy.description = description
                        hierarchy.maintenance_agency_id = maintenance_agency
                        hierarchy.domain_id = domain

                        context.member_hierarchy_dictionary[id] = hierarchy
                        if context.save_sdd_to_db:  
                            hierarchy.save()

        ImportWebsiteToSDDModel.save_missing_domains_to_csv(context,missing_domains)

    def save_missing_domains_to_csv(context,missing_domains):
        filename = context.output_directory + os.sep + "generated_hierarchy_warnings" + os.sep + "missing_domains.csv"
        with open(filename, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(missing_domains)

    def save_missing_members_to_csv(context,missing_members):
        filename = context.output_directory + os.sep + "generated_hierarchy_warnings" + os.sep + "missing_members.csv"
        with open(filename, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(missing_members)

    def save_missing_variables_to_csv(context,missing_variables):
        filename = context.output_directory + os.sep + "generated_hierarchy_warnings" + os.sep + "missing_variables.csv"
        with open(filename, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(missing_variables)

    def save_missing_children_to_csv(context,missing_children):
        filename = context.output_directory + os.sep + "generated_hierarchy_warnings" + os.sep + "missing_children.csv"
        with open(filename, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(missing_children)

    
        

    def create_all_member_hierarchies_nodes(self, context):
        '''
        Import all member hierarchy nodes from CSV file
        '''
        file_location = context.file_directory + os.sep + "member_hierarchy_node.csv"
        header_skipped = False
        missing_members = []
        missing_hierarchies = []
        with open(file_location, encoding='utf-8') as csvfile:
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

                    hierarchy = ImportWebsiteToSDDModel.find_member_hierarchy_with_id(self,hierarchy_id,context)
                    if hierarchy is None:
                        print(f"Hierarchy {hierarchy_id} not found")
                        missing_hierarchies.append(hierarchy_id)
                    else:
                        member = ImportWebsiteToSDDModel.find_member_with_id_for_hierarchy(self,member_id,hierarchy,context)
                        if member is None:
                            print(f"Member {member_id} not found in the database for hierarchy {hierarchy_id}")
                            missing_members.append((hierarchy_id,member_id))
                        else:
                            parent_member = ImportWebsiteToSDDModel.find_member_with_id(self,parent_member_id,context)
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
        ImportWebsiteToSDDModel.save_missing_members_to_csv(context,missing_members)
        ImportWebsiteToSDDModel.save_missing_hierarchies_to_csv(context,missing_hierarchies)

    def save_missing_hierarchies_to_csv(context,missing_hierarchies):
        filename = context.output_directory + os.sep + "missing_hierarchies.csv"
        with open(filename, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(missing_hierarchies)

    def find_member_with_id_for_hierarchy(self,member_id,hierarchy,context):
        domain = hierarchy.domain_id
        member = MEMBER.objects.filter(domain_id=domain,member_id=member_id).first()
        return member

    def create_report_tables(self, context):
        '''
        Import all tables from the rendering package CSV file
        '''
        file_location = context.file_directory + os.sep + "table.csv"
        header_skipped = False

        with open(file_location, encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if not header_skipped:
                    header_skipped = True
                else:
                    table_id = row[ColumnIndexes().table_table_id]
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
                    table.name = display_name
                    table.code = code
                    table.description = description
                    maintenance_agency = ImportWebsiteToSDDModel.find_maintenance_agency_with_id(self,context,maintenance_agency_id)
                    table.maintenance_agency_id = maintenance_agency
                    table.version = version

                    if context.save_sdd_to_db:  
                        table.save()
                    context.report_tables_dictionary[table.table_id] = table

    def create_axis(self, context):
        '''
        Import all axes from the rendering package CSV file
        '''
        file_location = context.file_directory + os.sep + "axis.csv"
        header_skipped = False

        with open(file_location, encoding='utf-8') as csvfile:
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
                    axis.description = axis_description
                    axis.table_id = ImportWebsiteToSDDModel.find_table_with_id(self, context, axis_table_id)

                    if context.save_sdd_to_db:  
                        axis.save()
                    context.axis_dictionary[axis_id] = axis

    def create_axis_ordinates(self, context):
        '''
        Import all axis ordinates from the rendering package CSV file
        '''
        file_location = context.file_directory + os.sep + "axis_ordinate.csv"
        header_skipped = False

        with open(file_location, encoding='utf-8') as csvfile:
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
                    axis_ordinate.code = axis_ordinate_code
                    axis_ordinate.path = axis_ordinate_path
                    axis_ordinate.axis_id = ImportWebsiteToSDDModel.find_axis_with_id(self,
                                                    context, ImportWebsiteToSDDModel.replace_dots(self,axis_ordinate_axis_id))
                    axis_ordinate.name = axis_ordinate_name
                    axis_ordinate.description = axis_ordinate_description
                    if context.save_sdd_to_db:  
                        axis_ordinate.save()
                    context.axis_ordinate_dictionary[axis_ordinate_id] = axis_ordinate

    def create_ordinate_items(self, context):
        '''
        Import all ordinate items from the rendering package CSV file
        '''
        file_location = context.file_directory + os.sep + "ordinate_item.csv"
        header_skipped = False

        with open(file_location, encoding='utf-8') as csvfile:
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
        Import all table cells from the rendering package CSV file
        '''
        file_location = context.file_directory + os.sep + "table_cell.csv"
        header_skipped = False

        with open(file_location, encoding='utf-8') as csvfile:
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

                    if table_cell_cell_id.endswith("_REF"):
                        table_cell = TABLE_CELL(
                            name=ImportWebsiteToSDDModel.replace_dots(self, table_cell_cell_id))
                        table_cell.cell_id = ImportWebsiteToSDDModel.replace_dots(self, table_cell_cell_id)
                        table_cell.table_id = ImportWebsiteToSDDModel.\
                            find_table_with_id(self, context,ImportWebsiteToSDDModel.replace_dots(self,table_cell_table_id))
                        table_cell.table_cell_combination_id = table_cell_combination_id
                        
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
        Import all cell positions from the rendering package CSV file
        '''
        file_location = context.file_directory + os.sep + "cell_position.csv"
        header_skipped = False

        with open(file_location, encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if not header_skipped:
                    header_skipped = True
                else:
                    cell_positions_cell_id = row[ColumnIndexes().cell_positions_cell_id]
                    cell_positions_axis_ordinate_id = row[ColumnIndexes().cell_positions_axis_ordinate_id]

                    if cell_positions_cell_id.endswith("_REF"):
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
        '''
        Import all member mappings from the rendering package CSV file
        '''
        file_location = context.file_directory + os.sep + "member_mapping.csv"
        header_skipped = False

        with open(file_location, encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if not header_skipped:
                    header_skipped = True
                else:
                    maintenance_agency_id = row[ColumnIndexes().member_map_maintenance_agency_id]
                    member_mapping_id = row[ColumnIndexes().member_map_member_mapping_id]
                    name = row[ColumnIndexes().member_map_name]
                    code = row[ColumnIndexes().member_map_code]
                    if not member_mapping_id.startswith("SHS_"):
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
        '''
        Import all member mapping items from the rendering package CSV file
        '''
        file_location = context.file_directory + os.sep + "member_mapping_item.csv"
        header_skipped = False
        missing_members = []
        missing_variables = []
        with open(file_location, encoding='utf-8') as csvfile:
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
                    if not member_mapping_id.startswith("SHS_"):
                        member = ImportWebsiteToSDDModel.find_member_with_id(
                                                            self,member_id,context)
                        variable = ImportWebsiteToSDDModel.find_variable_with_id(
                                                            self,context,variable_id)
                        
                        if member is None:
                            if member_id not in missing_members:
                                missing_members.append((member_id,member_mapping_id,row_number,variable_id))
                        if variable is None:
                            if variable_id not in missing_variables:
                                missing_variables.append((variable_id,'',''))
                                

                        if member is None or variable is None:
                            pass
                        else:
                            member_mapping_item = MEMBER_MAPPING_ITEM()
                            member_mapping_item.is_source = is_source
                            member_mapping_item.member = member
                            member_mapping_item.variable = variable
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
        for missing_member in missing_members:
            print(f"Missing member {missing_member}")
        for missing_variable in missing_variables:
            print(f"Missing variable {missing_variable}")
        #ImportWebsiteToSDDModel.save_missing_mapping_variables_to_csv(context,missing_variables)
        ImportWebsiteToSDDModel.save_missing_mapping_members_to_csv(context,missing_members)

    def save_missing_mapping_variables_to_csv(context,missing_variables):
        filename = context.output_directory + os.sep + "generated_mapping_warnings" + os.sep + "mappings_missing_variables.csv"
        with open(filename, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["Varaible","Mapping","Valid_to"])
            for var in missing_variables:
                writer.writerow([var[0],var[1],var[2]])

    def save_missing_mapping_members_to_csv(context,missing_members):   
        filename = context.output_directory + os.sep + "generated_mapping_warnings" + os.sep + "mappings_missing_members.csv"
        with open(filename, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["Member","Mapping","Row","Variable"])
            for mem in missing_members:
                writer.writerow([mem[0],mem[1],mem[2],mem[3]])

        ImportWebsiteToSDDModel.create_mappings_warnings_summary(context,missing_members)

    def create_mappings_warnings_summary(context,missing_members):
        filename = context.output_directory + os.sep + "generated_mapping_warnings" + os.sep + "mappings_warnings_summary.csv"
        #create a list of unique missing variable ids
        # read mappings_missing_variables file into a dictionary
        missing_variables= []
        written_members = []
        varaibles_filename = context.output_directory + os.sep + "generated_mapping_warnings" + os.sep + "mappings_missing_variables.csv"
        with open(varaibles_filename, 'r', newline='') as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                if row[0] not in missing_variables:
                    missing_variables.append(row[0])

        with open(filename, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["Variable","Member"])
            for var in missing_variables:
                writer.writerow([var,''])

            for mem in missing_members:
                variable = mem[3]
                member = mem[0]
                if member not in written_members:
                    if variable not in missing_variables:
                        writer.writerow([variable,member])
                    written_members.append(member)

    def create_all_mapping_definitions(self, context):
        '''
        Import all mapping definitions from the rendering package CSV file
        '''
        file_location = context.file_directory + os.sep + "mapping_definition.csv"
        header_skipped = False

        with open(file_location, encoding='utf-8') as csvfile:
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
                    if not mapping_definition_id.startswith("SHS_"):
                        mapping_definition = MAPPING_DEFINITION()
                        mapping_definition.mapping_id = mapping_definition_id
                        mapping_definition.name = mapping_definition_name
                        mapping_definition.code = mapping_definition_code
                        mapping_definition.mapping_type = mapping_definition_mapping_type
                        mapping_definition.member_mapping_id = ImportWebsiteToSDDModel.find_member_mapping_with_id(
                            self,context,mapping_definition_member_mapping_id)
                        mapping_definition.variable_mapping_id = ImportWebsiteToSDDModel.find_variable_mapping_with_id(
                            self,context,mapping_definition_variable_mapping_id)

                        if context.save_sdd_to_db:  
                            mapping_definition.save()
                        context.mapping_definition_dictionary[mapping_definition_id] = mapping_definition

    def create_all_mapping_to_cubes(self, context):
        '''
        Import all mapping to cubes from the rendering package CSV file
        '''
        filelocation = context.file_directory + os.sep + "mapping_to_cube.csv"
        header_skipped = False
        with open(filelocation, encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if not header_skipped:
                    header_skipped = True
                else:
                    mapping_to_cube_mapping_id = row[ColumnIndexes().mapping_to_cube_mapping_id]
                    mapping_to_cube_cube_mapping_id = row[ColumnIndexes().mapping_to_cube_cube_mapping_id]
                    mapping_to_cube_valid_from = row[ColumnIndexes().mapping_to_cube_valid_from]
                    mapping_to_cube_valid_to = row[ColumnIndexes().mapping_to_cube_valid_to]
                    if not mapping_to_cube_mapping_id.startswith("M_SHS"):
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
        '''
        Import all variable mappings from the rendering package CSV file
        '''
        filelocation = context.file_directory + os.sep + "variable_mapping.csv"
        header_skipped = False
        with open(filelocation, encoding='utf-8') as csvfile:
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
                    if not variable_mapping_id.startswith("SHS_"):
                        if context.save_sdd_to_db:
                            variable_mapping.save()
                        context.variable_mapping_dictionary[variable_mapping_id] = variable_mapping
            

    def create_all_variable_mapping_items(self, context):
        '''
        Import all variable mapping items from the rendering package CSV file
        '''
        missing_variables = []
        fileloacation = context.file_directory + os.sep + "variable_mapping_item.csv"
        header_skipped = False
        with open(fileloacation, encoding='utf-8') as csvfile:
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
                    if not variable_mapping_item_variable_mapping_id.startswith("SHS_"):
                        
                        variable = ImportWebsiteToSDDModel.find_variable_with_id(
                            self,context,variable_mapping_item_variable_id)
                        if variable is None:
                            missing_variables.append((variable_mapping_item_variable_id,variable_mapping_item_variable_mapping_id,variable_mapping_item_valid_to))
                        else:  
                            variable_mapping_item = VARIABLE_MAPPING_ITEM()
                            variable_mapping_item.variable = variable
                            variable_mapping_item.variable_mapping_id = ImportWebsiteToSDDModel.find_variable_mapping_with_id(
                                self,context,variable_mapping_item_variable_mapping_id)
                            
                            if variable_mapping_item.variable is None:
                                print(f"No Variable mapping item variable id: {variable_mapping_item_variable_id}")
                            variable_mapping_item.is_source = variable_mapping_item_is_source
                            variable_mapping_item.valid_from = variable_mapping_item_valid_from
                            variable_mapping_item.valid_to = variable_mapping_item_valid_to

                            
                            if context.save_sdd_to_db:
                                variable_mapping_item.save()
                            try:
                                variable_mapping_items_list = context.variable_mapping_item_dictionary[variable_mapping_item_variable_mapping_id]
                                variable_mapping_items_list.append(variable_mapping_item)
                            except KeyError:
                                context.variable_mapping_item_dictionary[variable_mapping_item_variable_mapping_id] = [variable_mapping_item]
        for missing_variable in missing_variables:
            print(f"Misssing Varaible {missing_variable}")

        ImportWebsiteToSDDModel.save_missing_mapping_variables_to_csv(context,missing_variables)

    

    def find_member_mapping_with_id(self,context,member_mapping_id):
        '''
        Find an existing member mapping with this id
        '''
        try:
            return context.member_mapping_dictionary[member_mapping_id]
        except KeyError:
            return None

    def find_member_with_id(self,element_id,context):
        '''
        Find an existing member with this id
        '''
        try:
            return context.nonref_member_dictionary[element_id]
        except:
            try:
                return context.ref_member_dictionary[element_id]
            except KeyError:
                try:
                    return context.members_that_are_nodes[element_id]
                except KeyError:
                    return None

    def find_member_hierarchy_with_id(self,element_id,context):
        '''
        Find an existing member hierarchy with this id
        '''
        try:
            return context.member_hierarchy_dictionary[element_id]
        except KeyError:
            return None

    def find_variable_with_id(self,context, element_id):
        '''
        Find an existing variable with this id
        '''
        try:
            return context.nonref_variable_dictionary[element_id]
        except KeyError:
            try:
                return context.ref_variable_dictionary[element_id]
            except KeyError:
                return None
            
    def find_maintenance_agency_with_id(self,context, element_id):
        '''
        Find an existing maintenance agency with this id
        '''
        try:
            return context.agency_dictionary[element_id]
        except KeyError:
            return None

    def find_domain_with_id(self,context, element_id):
        '''
        Find an existing domain with this id
        '''
        try:
            return context.ref_domain_dictionary[element_id]
        except KeyError:
            try:
                return_item = context.nonref_domain_dictionary[element_id]
                return return_item
            except KeyError:
                return None
            
    def find_table_with_id(self, context, table_id):
        '''
        Get the report table with the given id
        '''
        try:
            return context.report_tables_dictionary[table_id]
        except KeyError:
            return None

    def find_axis_with_id(self, context, axis_id):
        '''
        Get the axis with the given id
        '''
        try:
            return context.axis_dictionary[axis_id]
        except KeyError:
            return None

    def find_table_cell_with_id(self, context, table_cell_id):
        '''
        Get the table cell with the given id
        '''
        try:
            return context.table_cell_dictionary[table_cell_id]
        except KeyError:
            return None

    def find_axis_ordinate_with_id(self, context, axis_ordinate_id):
        '''
        Get the existing ordinate with the given id
        '''
        try:
            return context.axis_ordinate_dictionary[axis_ordinate_id]
        except KeyError:
            return None

    def replace_dots(self, text):
        '''
        Replace dots with underscores in the given text
        '''
        return text.replace('.', '_')
    
    def find_variable_mapping_with_id(self, context, variable_mapping_id):
        '''
        Get the variable mapping with the given id
        '''
        try:
            return context.variable_mapping_dictionary[variable_mapping_id]
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


    def delete_hierarchy_warnings_files(self, context):
        base_dir = settings.BASE_DIR
        hierarchy_warnings_dir = os.path.join(base_dir, 'results', 'generated_hierarchy_warnings')
        for file in os.listdir(hierarchy_warnings_dir):
            os.remove(os.path.join(hierarchy_warnings_dir, file))

    def delete_mapping_warnings_files(self, context):
        base_dir = settings.BASE_DIR
        mapping_warnings_dir = os.path.join(base_dir, 'results', 'generated_mapping_warnings')
        for file in os.listdir(mapping_warnings_dir):
            os.remove(os.path.join(mapping_warnings_dir, file))

