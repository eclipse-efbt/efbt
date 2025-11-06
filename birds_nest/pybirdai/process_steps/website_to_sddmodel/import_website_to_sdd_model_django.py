# coding=UTF-8
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
from pybirdai.models.bird_meta_data_model import *
from pybirdai.context.csv_column_index_context import ColumnIndexes
from pathlib import Path
from django.db import connection, transaction
import subprocess
import platform

class ImportWebsiteToSDDModel(object):
    '''
    Class responsible for importing SDD csv files into an instance of the analysis model
    '''

    def import_report_templates_from_sdd(self, sdd_context, dpm:bool=False):
        '''
        Import SDD csv files into an instance of the analysis model
        '''

        ImportWebsiteToSDDModel.create_maintenance_agencies(self, sdd_context)
        ImportWebsiteToSDDModel.create_frameworks(self, sdd_context)
        ImportWebsiteToSDDModel.create_all_domains(self, sdd_context,False)
        ImportWebsiteToSDDModel.create_all_members(self, sdd_context,False)
        ImportWebsiteToSDDModel.create_all_variables(self, sdd_context,False)

        ImportWebsiteToSDDModel.create_report_tables(self, sdd_context)
        ImportWebsiteToSDDModel.create_axis(self, sdd_context)
        ImportWebsiteToSDDModel.create_axis_ordinates(self, sdd_context)

        if dpm:
            ImportWebsiteToSDDModel.create_table_cells_csv_copy(self, sdd_context)
            ImportWebsiteToSDDModel.create_ordinate_items_csv_copy(self, sdd_context)
            ImportWebsiteToSDDModel.create_cell_positions_csv_copy(self, sdd_context)
        else:
            ImportWebsiteToSDDModel.create_table_cells(self, sdd_context)
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
        Import maintenance agencies from CSV file using bulk create
        '''
        file_location = context.file_directory + os.sep + "technical_export" + os.sep + "maintenance_agency.csv"
        header_skipped = False
        agencies_to_create = []

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

                    agencies_to_create.append(maintenance_agency)
                    context.agency_dictionary[id] = maintenance_agency

        if context.save_sdd_to_db and agencies_to_create:
            MAINTENANCE_AGENCY.objects.bulk_create(agencies_to_create, batch_size=1000,ignore_conflicts=True)

    def create_frameworks(self, context):
        '''
        Import frameworks from CSV file using bulk create
        '''
        file_location = context.file_directory + os.sep + "technical_export" + os.sep + "framework.csv"
        header_skipped = False
        frameworks_to_create = []

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

                    frameworks_to_create.append(framework)
                    context.framework_dictionary[ImportWebsiteToSDDModel.replace_dots(self, id)] = framework

        if context.save_sdd_to_db and frameworks_to_create:
            FRAMEWORK.objects.bulk_create(frameworks_to_create, batch_size=1000,ignore_conflicts=True)

    def create_all_domains(self, context, ref):
        '''
        Import all domains from CSV file using bulk create
        '''
        file_location = context.file_directory + os.sep + "technical_export" + os.sep + "domain.csv"
        header_skipped = False
        domains_to_create = []

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
                    if (ref) and (maintenence_agency == "ECB"):
                        include = True
                    if (not ref) and not (maintenence_agency == "ECB"):
                        include = True

                    if include:
                        domain = DOMAIN(name=ImportWebsiteToSDDModel.replace_dots(self, domain_id))
                        if maintenence_agency == "":
                            maintenence_agency = ImportWebsiteToSDDModel.find_maintenance_agency_with_id(self,context,"SDD_DOMAIN")
                        else:
                            maintenence_agency = ImportWebsiteToSDDModel.find_maintenance_agency_with_id(self,context,maintenence_agency)
                        domain.maintenance_agency_id = maintenence_agency
                        domain.code = code
                        domain.description = description
                        domain.domain_id = ImportWebsiteToSDDModel.replace_dots(self, domain_id)
                        domain.name = domain_name
                        domain.is_enumerated = True if is_enumerated else False
                        domain.is_reference = True if is_reference else False

                        domains_to_create.append(domain)
                        if ref:
                            context.domain_dictionary[domain.domain_id] = domain
                        else:
                            context.domain_dictionary[domain.domain_id] = domain

        if context.save_sdd_to_db and domains_to_create:
            DOMAIN.objects.bulk_create(domains_to_create, batch_size=1000,ignore_conflicts=True)

    def create_all_members(self, context, ref):
        '''
        Import all members from CSV file using bulk create
        '''
        file_location = context.file_directory + os.sep + "technical_export" + os.sep + "member.csv"
        header_skipped = False
        members_to_create = []

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
                    if (ref) and (maintenence_agency == "ECB"):
                        include = True
                    if (not ref) and not (maintenence_agency == "ECB"):
                        include = True

                    if include:
                        member = MEMBER(name=ImportWebsiteToSDDModel.replace_dots(self, member_id))
                        member.member_id = ImportWebsiteToSDDModel.replace_dots(self, member_id)
                        member.code = code
                        member.description = description
                        member.name = member_name
                        maintenance_agency = ImportWebsiteToSDDModel.find_maintenance_agency_with_id(self,context,maintenence_agency)
                        member.maintenance_agency_id = maintenance_agency
                        domain = ImportWebsiteToSDDModel.find_domain_with_id(self, context, domain_id)
                        member.domain_id = domain

                        members_to_create.append(member)
                        context.member_dictionary[member.member_id] = member

                        if not (domain_id is None) and not (domain_id == ""):
                            context.member_id_to_domain_map[member] = domain
                            context.member_id_to_member_code_map[member.member_id] = code

        if context.save_sdd_to_db and members_to_create:
            MEMBER.objects.bulk_create(members_to_create, batch_size=1000,ignore_conflicts=True)

    def create_all_variables(self, context, ref):
        '''
        Import all variables from CSV file using bulk create
        '''
        file_location = context.file_directory + os.sep + "technical_export" + os.sep + "variable.csv"
        header_skipped = False
        variables_to_create = []

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
                    if (ref) and (maintenence_agency == "ECB"):
                        include = True
                    if (not ref) and not (maintenence_agency == "ECB"):
                        include = True

                    if include:
                        variable = VARIABLE(name=ImportWebsiteToSDDModel.replace_dots(self, variable_id))
                        maintenance_agency_id = ImportWebsiteToSDDModel.find_maintenance_agency_with_id(self,context,maintenence_agency)
                        variable.code = code
                        variable.variable_id = ImportWebsiteToSDDModel.replace_dots(self, variable_id)
                        variable.name = name
                        domain = ImportWebsiteToSDDModel.find_domain_with_id(self, context, domain_id)
                        variable.domain_id = domain
                        variable.description = description
                        variable.maintenance_agency_id = maintenance_agency_id

                        variables_to_create.append(variable)
                        context.variable_dictionary[variable.variable_id] = variable
                        context.variable_to_domain_map[variable.variable_id] = domain
                        context.variable_to_long_names_map[variable.variable_id] = name
                        if not((primary_concept == "") or (primary_concept == None)):
                            context.variable_to_primary_concept_map[variable.variable_id] = primary_concept

        if context.save_sdd_to_db and variables_to_create:
            VARIABLE.objects.bulk_create(variables_to_create, batch_size=1000,ignore_conflicts=True)

    def create_all_parent_members_with_children_locally(self, context):
        print("Creating all parent members with children locally")
        parent_members = set()  # Using set for faster lookups
        parent_members_to_create = []
        parent_members_child_triples = []
        missing_children = []

        # Pre-fetch all hierarchies for faster lookup
        hierarchy_cache = {}

        with open(f"{context.file_directory}/technical_export/member_hierarchy_node.csv", encoding='utf-8') as csvfile:
            header_skipped = False
            id_increment = 0
            for row in csv.reader(csvfile):
                if not header_skipped:
                    header_skipped = True
                    if row[0].upper() == 'ID': #sometimes exported data without a  primary key has an ID field added at the time of export, exported data is re-imported
                        id_increment = 1
                else:
                    parent_member_id = row[ColumnIndexes().member_hierarchy_node_parent_member_id + id_increment]
                    member_id = row[ColumnIndexes().member_hierarchy_node_member_id + id_increment]
                    hierarchy_id = row[ColumnIndexes().member_hierarchy_node_hierarchy_id + id_increment]

                    if not parent_member_id:
                        continue

                    if hierarchy_id not in hierarchy_cache:
                        hierarchy_cache[hierarchy_id] = ImportWebsiteToSDDModel.find_member_hierarchy_with_id(self,hierarchy_id,context)

                    hierarchy = hierarchy_cache[hierarchy_id]
                    if hierarchy:
                        domain = hierarchy.domain_id
                        parent_members_child_triples.append((parent_member_id,member_id,domain))
                        parent_members.add(parent_member_id)

        # Process parent-child relationships in batches
        for parent_member_id, member_id, domain in parent_members_child_triples:
            if member_id in parent_members:
                if not any(parent_member_id in d for d in (context.members_that_are_nodes,
                                                         context.member_dictionary,
                                                         context.member_dictionary)):
                    parent_member = MEMBER(
                        name=ImportWebsiteToSDDModel.replace_dots(self, parent_member_id),
                        member_id=ImportWebsiteToSDDModel.replace_dots(self, parent_member_id),
                        maintenance_agency_id=ImportWebsiteToSDDModel.find_maintenance_agency_with_id(self,context,"NODE"),
                        domain_id=domain
                    )
                    parent_members_to_create.append(parent_member)
                    context.member_dictionary[parent_member.member_id] = parent_member
                    if not (parent_member.domain_id is None) and not (parent_member.domain_id == ""):
                        context.member_id_to_domain_map[parent_member] = domain
                        context.member_id_to_member_code_map[parent_member.member_id] = parent_member.member_id

                    context.members_that_are_nodes[parent_member_id] = parent_member
            else:
                member = ImportWebsiteToSDDModel.find_member_with_id(self,member_id,context)
                if member is None:
                    missing_children.append((parent_member_id,member_id))
                elif not any(parent_member_id in d for d in (context.members_that_are_nodes,
                                                          context.member_dictionary,
                                                          context.member_dictionary)):
                    parent_member = MEMBER(
                        name=ImportWebsiteToSDDModel.replace_dots(self, parent_member_id),
                        member_id=ImportWebsiteToSDDModel.replace_dots(self, parent_member_id),
                        maintenance_agency_id=ImportWebsiteToSDDModel.find_maintenance_agency_with_id(self,context,"NODE"),
                        domain_id=domain
                    )
                    parent_members_to_create.append(parent_member)
                    context.members_that_are_nodes[parent_member_id] = parent_member

                    context.member_dictionary[parent_member.member_id] = parent_member
                    if not (parent_member.domain_id is None) and not (parent_member.domain_id == ""):
                        context.member_id_to_domain_map[parent_member] = domain
                        context.member_id_to_member_code_map[parent_member.member_id] = parent_member.member_id

        if context.save_sdd_to_db and parent_members_to_create:
            MEMBER.objects.bulk_create(parent_members_to_create, batch_size=5000,ignore_conflicts=True)  # Increased batch size

        ImportWebsiteToSDDModel.save_missing_children_to_csv(context,missing_children)

    def create_all_member_hierarchies(self, context):
        '''
        Import all member hierarchies with batch processing
        '''
        missing_domains = set()  # Using set for faster lookups
        hierarchies_to_create = []

        with open(f"{context.file_directory}/technical_export/member_hierarchy.csv", encoding='utf-8') as csvfile:
            next(csvfile)  # Skip header more efficiently
            for row in csv.reader(csvfile):
                maintenance_agency_id = row[ColumnIndexes().member_hierarchy_maintenance_agency]
                code = row[ColumnIndexes().member_hierarchy_code]
                id = row[ColumnIndexes().member_hierarchy_id]
                domain_id = row[ColumnIndexes().member_hierarchy_domain_id]
                description = row[ColumnIndexes().member_hierarchy_description]

                maintenance_agency = ImportWebsiteToSDDModel.find_maintenance_agency_with_id(self,context,maintenance_agency_id)
                domain = ImportWebsiteToSDDModel.find_domain_with_id(self,context,domain_id)

                if domain is None:
                    missing_domains.add(domain_id)
                    continue

                hierarchy = MEMBER_HIERARCHY(
                    name=ImportWebsiteToSDDModel.replace_dots(self, id),
                    member_hierarchy_id=ImportWebsiteToSDDModel.replace_dots(self, id),
                    code=code,
                    description=description,
                    maintenance_agency_id=maintenance_agency,
                    domain_id=domain
                )

                if hierarchy.member_hierarchy_id not in context.member_hierarchy_dictionary:
                    hierarchies_to_create.append(hierarchy)
                    context.member_hierarchy_dictionary[hierarchy.member_hierarchy_id] = hierarchy

        if context.save_sdd_to_db and hierarchies_to_create:
            MEMBER_HIERARCHY.objects.bulk_create(hierarchies_to_create, batch_size=5000,ignore_conflicts=True)  # Increased batch size

        if missing_domains:
            ImportWebsiteToSDDModel.save_missing_domains_to_csv(context, list(missing_domains))

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
        file_location = context.file_directory + os.sep + "technical_export" + os.sep + "member_hierarchy_node.csv"
        header_skipped = False
        missing_members = []
        missing_hierarchies = []
        nodes_to_create = []

        # Pre-fetch all members to avoid N+1 queries (performance optimization)
        # Build a cache: domain_id -> {member_id -> member_object}
        from collections import defaultdict
        member_cache = defaultdict(dict)
        all_members = MEMBER.objects.select_related('domain_id').all()
        for m in all_members:
            if m.domain_id:
                member_cache[m.domain_id.domain_id][m.member_id] = m
            # Also add to general cache for parent member lookup
            context.member_dictionary[m.member_id] = m

        with open(file_location, encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            id_increment = 0
            for row in filereader:
                if not header_skipped:
                    header_skipped = True
                    if row[0].upper() == 'ID': #sometimes exported data without a  primary key has an ID field added at the time of export, exported data is re-imported
                        id_increment = 1
                else:
                    hierarchy_id = row[ColumnIndexes().member_hierarchy_node_hierarchy_id + id_increment]
                    member_id = row[ColumnIndexes().member_hierarchy_node_member_id + id_increment]
                    parent_member_id = row[ColumnIndexes().member_hierarchy_node_parent_member_id + id_increment]
                    node_level = row[ColumnIndexes().member_hierarchy_node_level + id_increment]
                    comparator = row[ColumnIndexes().member_hierarchy_node_comparator + id_increment]
                    operator = row[ColumnIndexes().member_hierarchy_node_operator + id_increment]
                    valid_from = row[ColumnIndexes().member_hierarchy_node_valid_from + id_increment]
                    valid_to = row[ColumnIndexes().member_hierarchy_node_valid_to + id_increment]

                    hierarchy = ImportWebsiteToSDDModel.find_member_hierarchy_with_id(self,hierarchy_id,context)
                    if hierarchy is None:
                        print(f"Hierarchy {hierarchy_id} not found")
                        missing_hierarchies.append(hierarchy_id)
                    else:
                        # Use cache instead of database query (performance optimization)
                        domain_id = hierarchy.domain_id.domain_id if hierarchy.domain_id else None
                        member = member_cache.get(domain_id, {}).get(member_id, None)
                        if member is None:
                            print(f"Member {member_id} not found in the database for hierarchy {hierarchy_id}")
                            missing_members.append((hierarchy_id,member_id))
                        else:
                            # Parent member lookup now uses pre-populated context.member_dictionary
                            parent_member = context.member_dictionary.get(parent_member_id, None)
                            if not (parent_member is None):
                                hierarchy_node = MEMBER_HIERARCHY_NODE()
                                hierarchy_node.member_hierarchy_id = hierarchy
                                hierarchy_node.comparator = comparator
                                hierarchy_node.operator = operator
                                hierarchy_node.member_id = member
                                hierarchy_node.level = int(node_level)
                                hierarchy_node.parent_member_id = parent_member
                                nodes_to_create.append(hierarchy_node)
                                context.member_hierarchy_node_dictionary[hierarchy_id + ":" + member_id] = hierarchy_node

        if context.save_sdd_to_db and nodes_to_create:
            MEMBER_HIERARCHY_NODE.objects.bulk_create(nodes_to_create, batch_size=1000,ignore_conflicts=True)

        ImportWebsiteToSDDModel.save_missing_members_to_csv(context,missing_members)
        ImportWebsiteToSDDModel.save_missing_hierarchies_to_csv(context,missing_hierarchies)

    def save_missing_hierarchies_to_csv(context,missing_hierarchies):
        filename = context.output_directory + os.sep + "generated_hierarchy_warnings" + os.sep + "missing_hierarchies.csv"
        with open(filename, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(missing_hierarchies)

    def find_member_with_id_for_hierarchy(self,member_id,hierarchy,context):
        domain = hierarchy.domain_id
        member = MEMBER.objects.filter(domain_id=domain,member_id=member_id).first()
        return member

    def create_report_tables(self, context):
        '''
        Import all tables from the rendering package CSV file using bulk create
        '''
        file_location = context.file_directory + os.sep + "technical_export" + os.sep + "table.csv"
        header_skipped = False
        tables_to_create = []

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

                    tables_to_create.append(table)
                    context.report_tables_dictionary[table.table_id] = table

        if context.save_sdd_to_db and tables_to_create:
            TABLE.objects.bulk_create(tables_to_create, batch_size=1000,ignore_conflicts=True)

    def create_axis(self, context):
        '''
        Import all axes from the rendering package CSV file using bulk create
        '''
        file_location = context.file_directory + os.sep + "technical_export" + os.sep + "axis.csv"
        header_skipped = False
        axes_to_create = []

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

                    axes_to_create.append(axis)
                    context.axis_dictionary[axis.axis_id] = axis

        if context.save_sdd_to_db and axes_to_create:
            AXIS.objects.bulk_create(axes_to_create, batch_size=1000,ignore_conflicts=True)

    def create_axis_ordinates(self, context):
        '''
        Import all axis ordinates from the rendering package CSV file using bulk create
        '''
        file_location = context.file_directory + os.sep + "technical_export" + os.sep + "axis_ordinate.csv"
        header_skipped = False
        ordinates_to_create = []

        with open(file_location, encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if not header_skipped:
                    header_skipped = True
                else:
                    axis_ordinate_id = row[ColumnIndexes().axis_ordinate_axis_ordinate_id]
                    axis_ordinate_is_abstract_header = row[ColumnIndexes().axis_ordinate_is_abstract_header]
                    axis_ordinate_code = row[ColumnIndexes().axis_ordinate_code]
                    axis_ordinate_order = row[ColumnIndexes().axis_ordinate_order]
                    axis_ordinate_path = row[ColumnIndexes().axis_ordinate_path]
                    axis_ordinate_axis_id = row[ColumnIndexes().axis_ordinate_axis_id]
                    axis_ordinate_parent_axis_ordinate_id = row[ColumnIndexes().axis_ordinate_parent_axis_ordinate_id]
                    axis_ordinate_name = row[ColumnIndexes().axis_ordinate_name]
                    axis_ordinate_description = row[ColumnIndexes().axis_ordinate_description]

                    axis_ordinate = AXIS_ORDINATE(
                        name=ImportWebsiteToSDDModel.replace_dots(self, axis_ordinate_id))
                    axis_ordinate.axis_ordinate_id = ImportWebsiteToSDDModel.replace_dots(self, axis_ordinate_id)
                    axis_ordinate.code = axis_ordinate_code
                    axis_ordinate.path = axis_ordinate_path
                    axis_ordinate.axis_id = ImportWebsiteToSDDModel.find_axis_with_id(self, context, ImportWebsiteToSDDModel.replace_dots(self,axis_ordinate_axis_id))
                    axis_ordinate.name = axis_ordinate_name
                    axis_ordinate.description = axis_ordinate_description

                    ordinates_to_create.append(axis_ordinate)
                    context.axis_ordinate_dictionary[axis_ordinate.axis_ordinate_id] = axis_ordinate

        if context.save_sdd_to_db and ordinates_to_create:
            AXIS_ORDINATE.objects.bulk_create(ordinates_to_create, batch_size=1000,ignore_conflicts=True)

    def _backup_table_data(self, table_name, backup_table_name):
        """
        Create a temporary backup table and copy all existing data.
        Phase 1 of the backup-restore approach.
        """
        with connection.cursor() as cursor:
            if connection.vendor == 'sqlite':
                cursor.execute(f"CREATE TEMP TABLE {backup_table_name} AS SELECT * FROM {table_name}")
            elif connection.vendor == 'postgresql':
                cursor.execute(f"CREATE TEMP TABLE {backup_table_name} AS SELECT * FROM {table_name}")
            else:
                # For other databases, use similar syntax
                cursor.execute(f"CREATE TABLE {backup_table_name} AS SELECT * FROM {table_name}")

    def _get_primary_key_column(self, table_name):
        """
        Get the primary key column name for a given table.
        """
        # Map table names to their primary key columns
        pk_mapping = {
            'pybirdai_table_cell': 'cell_id',
            'pybirdai_ordinate_item': 'id',  # Auto-increment ID
            'pybirdai_cell_position': 'id'   # Auto-increment ID
        }
        return pk_mapping.get(table_name, 'id')

    def _is_duplicate_content(self, table_name, backed_up_row, columns, existing_rows_cache):
        """
        Check if backed-up row content is identical to any existing row.

        For tables with string PKs (TABLE_CELL):
            - Compare by PK first, then all content fields

        For tables with auto-increment PKs (ORDINATE_ITEM, CELL_POSITION):
            - Ignore PK, compare all content fields against all existing rows

        Args:
            table_name: Name of the table being restored
            backed_up_row: Row from backup (list/tuple)
            columns: Column names in order
            existing_rows_cache: Dict (for TABLE_CELL) or list (for others) of existing rows

        Returns:
            True if duplicate found, False otherwise
        """
        pk_column = self._get_primary_key_column(table_name)

        if table_name == 'pybirdai_table_cell':
            # String PK - compare by key first
            pk_index = columns.index('cell_id')
            pk_value = str(backed_up_row[pk_index]) if backed_up_row[pk_index] else None

            if not pk_value:
                return False  # NULL PK, can't be duplicate

            # Check if this PK exists in newly imported data
            if pk_value not in existing_rows_cache:
                return False  # Not a duplicate, new record

            existing_row = existing_rows_cache[pk_value]

            # Compare content fields (exclude PK and 'name' if exists)
            content_fields = ['table_cell_combination_id', 'table_id_id', 'is_shaded', 'system_data_code']

            for field in content_fields:
                if field not in columns:
                    continue  # Skip if field doesn't exist

                backed_up_value = backed_up_row[columns.index(field)]
                existing_value = existing_row[columns.index(field)]

                # Normalize None and empty string for comparison
                backed_up_value = None if backed_up_value in (None, '', 'None') else backed_up_value
                existing_value = None if existing_value in (None, '', 'None') else existing_value

                if backed_up_value != existing_value:
                    return False  # Different content

            return True  # Duplicate - same key, same content

        elif table_name == 'pybirdai_ordinate_item':
            # Auto-increment PK - compare all content fields using hash lookup (O(1))
            content_fields = ['axis_ordinate_id_id', 'variable_id_id', 'member_id_id',
                            'member_hierarchy_id_id', 'starting_member_id_id', 'is_starting_member_included']

            # Build tuple of backed-up row's content field values (normalized)
            backed_up_values = []
            for field in content_fields:
                if field not in columns:
                    continue
                value = backed_up_row[columns.index(field)]
                # Normalize None values
                value = None if value in (None, '', 'None') else value
                backed_up_values.append(value)

            backed_up_hash = tuple(backed_up_values)

            # Check if this content tuple exists in the hash set (O(1) lookup)
            return backed_up_hash in existing_rows_cache

        elif table_name == 'pybirdai_cell_position':
            # Auto-increment PK - compare all content fields using hash lookup (O(1))
            content_fields = ['cell_id_id', 'axis_ordinate_id_id']

            # Build tuple of backed-up row's content field values (normalized)
            backed_up_values = []
            for field in content_fields:
                if field not in columns:
                    continue
                value = backed_up_row[columns.index(field)]
                # Normalize None values
                value = None if value in (None, '', 'None') else value
                backed_up_values.append(value)

            backed_up_hash = tuple(backed_up_values)

            # Check if this content tuple exists in the hash set (O(1) lookup)
            return backed_up_hash in existing_rows_cache

        # Unknown table - don't skip
        return False

    def _restore_backed_up_data_bulk(self, table_name, backup_table_name, csv_file_path):
        """
        Restore backed-up data using bulk insert with key regeneration.
        Phases 3-4 of the backup-restore approach:
        - Query existing keys (in-memory)
        - Generate new keys for conflicts (in-memory)
        - Bulk insert restored data
        """
        import time
        import tempfile

        pk_column = self._get_primary_key_column(table_name)
        timestamp = int(time.time())

        with connection.cursor() as cursor:
            # Get all existing rows (not just keys) for duplicate detection
            cursor.execute(f"SELECT * FROM {table_name}")
            existing_columns = [desc[0] for desc in cursor.description]
            existing_rows = cursor.fetchall()

            # Build cache structure for duplicate detection
            # For TABLE_CELL (string PK): dict {cell_id: row}
            # For others (auto-increment PK): set of content field tuples for O(1) lookup
            if table_name == 'pybirdai_table_cell':
                pk_idx = existing_columns.index(pk_column)
                existing_rows_cache = {str(row[pk_idx]): row for row in existing_rows}
                existing_keys = set(existing_rows_cache.keys())
            else:
                # Build hash set of content tuples for fast duplicate detection
                # Define content fields for each table (exclude auto-increment PK)
                content_fields_map = {
                    'pybirdai_ordinate_item': ['axis_ordinate_id_id', 'variable_id_id', 'member_id_id',
                                               'member_hierarchy_id_id', 'starting_member_id_id', 'is_starting_member_included'],
                    'pybirdai_cell_position': ['cell_id_id', 'axis_ordinate_id_id']
                }

                content_fields = content_fields_map.get(table_name, [])
                existing_rows_cache = set()

                for row in existing_rows:
                    # Build tuple of content field values (normalized)
                    values = []
                    for field in content_fields:
                        if field in existing_columns:
                            value = row[existing_columns.index(field)]
                            # Normalize None values
                            value = None if value in (None, '', 'None') else value
                            values.append(value)
                    existing_rows_cache.add(tuple(values))

                existing_keys = {str(row[existing_columns.index(pk_column)]) for row in existing_rows}

            # Get all backed-up data
            cursor.execute(f"SELECT * FROM {backup_table_name}")
            columns = [desc[0] for desc in cursor.description]
            backed_up_rows = cursor.fetchall()

            if not backed_up_rows:
                return  # No data to restore

            print(f"Checking {len(backed_up_rows)} backed-up rows for duplicates...")

            # Find primary key column index
            pk_index = columns.index(pk_column)

            # Skip duplicate records, restore non-duplicates with ORIGINAL IDs
            modified_rows = []
            skipped_duplicates = 0

            for row in backed_up_rows:
                row_list = list(row)

                # Check for duplicate content
                if self._is_duplicate_content(table_name, row_list, columns, existing_rows_cache):
                    skipped_duplicates += 1
                    continue  # Skip restoration of this duplicate row

                # NOT a duplicate → restore with NEW auto-generated ID
                # For auto-increment tables, set PK to NULL so SQLite generates new sequential IDs
                # This prevents UNIQUE constraint violations with newly imported data
                if table_name in ['pybirdai_ordinate_item', 'pybirdai_cell_position']:
                    row_list[pk_index] = None
                modified_rows.append(row_list)

            # Print statistics before bulk insert
            total_backed_up = len(backed_up_rows)
            total_restored = len(modified_rows)
            print(f"\nRestoration Statistics for {table_name}:")
            print(f"  Total backed-up rows: {total_backed_up}")
            print(f"  Skipped duplicates: {skipped_duplicates}")
            print(f"  Rows to restore: {total_restored}")
            if skipped_duplicates > 0:
                print(f"  → {skipped_duplicates}/{total_backed_up} ({skipped_duplicates*100/total_backed_up:.1f}%) were duplicates and not restored")

            # Bulk insert restored data
            if total_restored > 0:
                print(f"\nBulk inserting {len(modified_rows)} rows into {table_name}...")
                print(f"Columns: {columns}")
                print(f"Database vendor: {connection.vendor}")
            else:
                print(f"No rows to restore for {table_name} - all backed-up data was duplicates")
                return

            try:
                if connection.vendor == 'sqlite':
                    self._bulk_insert_sqlite(table_name, columns, modified_rows, csv_file_path)
                elif connection.vendor == 'postgresql':
                    self._bulk_insert_postgresql(table_name, columns, modified_rows)
                else:
                    self._bulk_insert_generic(table_name, columns, modified_rows)
                print(f"Successfully inserted {len(modified_rows)} rows into {table_name}")
            except Exception as e:
                print(f"ERROR: Failed to bulk insert into {table_name}")
                print(f"  Table: {table_name}")
                print(f"  Rows to insert: {len(modified_rows)}")
                print(f"  Columns: {columns}")
                print(f"  CSV file: {csv_file_path}")
                print(f"  Error: {str(e)}")
                raise

    def _bulk_insert_sqlite(self, table_name, columns, rows, original_csv_path):
        """
        Bulk insert for SQLite using temporary CSV file.
        Handles None values properly for auto-increment columns.
        """
        import tempfile
        import csv as csv_module

        # Find columns and rows where primary key is None (for auto-increment)
        pk_column = self._get_primary_key_column(table_name)

        # Check if any rows have None in primary key position
        pk_index = columns.index(pk_column) if pk_column in columns else -1
        has_null_pks = pk_index >= 0 and any(row[pk_index] is None for row in rows)

        # If there are None primary keys, we need to exclude that column from import
        # so SQLite can auto-generate the IDs
        if has_null_pks:
            # Filter out the primary key column
            filtered_columns = [col for i, col in enumerate(columns) if i != pk_index]
            filtered_rows = [[val for i, val in enumerate(row) if i != pk_index] for row in rows]
            print(f"Excluding primary key column '{pk_column}' from import (contains NULL values for auto-increment)")
        else:
            filtered_columns = columns
            filtered_rows = rows

        # Create temporary CSV file for restored data
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, newline='', encoding='utf-8') as temp_file:
            temp_csv_path = temp_file.name
            writer = csv_module.writer(temp_file)
            # Write header
            writer.writerow(filtered_columns)
            # Write data - convert None to empty string explicitly
            for row in filtered_rows:
                # Convert None to empty string for proper NULL handling in other columns
                cleaned_row = ['' if val is None else val for val in row]
                writer.writerow(cleaned_row)

        try:
            # If there are NULL primary keys, we can't use CSV import
            # Fall back to direct SQL INSERTs with proper column specification
            if has_null_pks and pk_index >= 0:
                print(f"Using SQL INSERT method for {len(filtered_rows)} rows (has NULL primary keys)")
                with connection.cursor() as cursor:
                    # Temporarily disable FK constraints for restoration
                    # This is necessary because backed-up data may reference entities
                    # that were replaced during the new import
                    print(f"Temporarily disabling foreign key constraints for {table_name} restoration...")
                    cursor.execute("PRAGMA foreign_keys = OFF")

                    try:
                        column_list = ', '.join(filtered_columns)
                        placeholders = ', '.join(['?' for _ in filtered_columns])
                        insert_sql = f"INSERT INTO {table_name} ({column_list}) VALUES ({placeholders})"

                        # Convert None to NULL explicitly for SQL
                        cleaned_rows = []
                        for row in filtered_rows:
                            cleaned_row = [None if val == '' or val is None else val for val in row]
                            cleaned_rows.append(cleaned_row)

                        # Bulk insert using executemany
                        cursor.executemany(insert_sql, cleaned_rows)
                        print(f"Successfully inserted {len(cleaned_rows)} rows into {table_name}")
                    finally:
                        # Re-enable FK constraints
                        cursor.execute("PRAGMA foreign_keys = ON")
                        print(f"Re-enabled foreign key constraints")
                return  # Skip CSV import

            # Standard CSV import for rows without NULL primary keys
            # Get database file path
            db_file = Path(connection.settings_dict['NAME']).absolute()

            # Create SQLite commands to import the restored data
            commands = [
                ".mode csv",
                ".separator ','",
                f".import --skip 1 '{temp_csv_path}' {table_name}"
            ]

            sqlite_script = '\n'.join(commands)

            sqlite_program = "sqlite3"
            if platform.system() == 'Windows':
                sqlite_program += ".exe"

            result = subprocess.run(
                [sqlite_program, str(db_file)],
                input=sqlite_script,
                text=True,
                capture_output=True,
                check=False  # Don't raise exception, we'll check returncode manually
            )

            if result.returncode != 0:
                # Display the actual error from sqlite3
                error_msg = f"SQLite import failed with exit code {result.returncode}"
                if result.stderr:
                    error_msg += f"\nSTDERR: {result.stderr}"
                if result.stdout:
                    error_msg += f"\nSTDOUT: {result.stdout}"
                error_msg += f"\nCommand: {' '.join([sqlite_program, str(db_file)])}"
                error_msg += f"\nDatabase: {db_file}"
                error_msg += f"\nCSV file: {temp_csv_path}"
                error_msg += f"\nSQL script:\n{sqlite_script}"
                print(error_msg)
                raise subprocess.CalledProcessError(result.returncode, [sqlite_program, str(db_file)], result.stdout, result.stderr)

            if result.stderr:
                print(f"SQLite restore warning: {result.stderr}")
        finally:
            # Clean up temporary file
            Path(temp_csv_path).unlink(missing_ok=True)

    def _bulk_insert_postgresql(self, table_name, columns, rows):
        """
        Bulk insert for PostgreSQL using COPY.
        """
        import io
        import csv as csv_module

        # Create CSV string in memory
        output = io.StringIO()
        writer = csv_module.writer(output)
        for row in rows:
            writer.writerow(row)

        output.seek(0)

        with connection.cursor() as cursor:
            columns_str = ', '.join(columns)
            cursor.copy_expert(
                f"COPY {table_name} ({columns_str}) FROM STDIN WITH (FORMAT CSV, DELIMITER ',')",
                output
            )

    def _bulk_insert_generic(self, table_name, columns, rows):
        """
        Bulk insert for other databases using executemany.
        """
        if not rows:
            return

        columns_str = ', '.join(columns)
        placeholders = ', '.join(['%s'] * len(columns))

        with connection.cursor() as cursor:
            cursor.executemany(
                f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})",
                rows
            )

    def _cleanup_backup_table(self, backup_table_name):
        """
        Drop the temporary backup table.
        Phase 5 of the backup-restore approach.
        """
        with connection.cursor() as cursor:
            if connection.vendor == 'sqlite':
                cursor.execute(f"DROP TABLE IF EXISTS {backup_table_name}")
            elif connection.vendor == 'postgresql':
                cursor.execute(f"DROP TABLE IF EXISTS {backup_table_name}")
            else:
                cursor.execute(f"DROP TABLE IF EXISTS {backup_table_name}")

    def _create_instances_from_csv_copy(self, context, cls):
        sdd_table_name = cls.__name__.lower()
        table_name = f"pybirdai_{sdd_table_name}"

        csv_file = context.file_directory + os.sep + "technical_export" + os.sep + f"{sdd_table_name}.csv"
        csv_file = Path(csv_file).absolute()
        delimiter = ","

        # Check if CSV file exists
        if not csv_file.exists():
            raise FileNotFoundError(f"CSV file not found: {csv_file}")

        try:

            # Define allowed table names to prevent SQL injection
            ALLOWED_TABLES = {
                'pybirdai_table_cell',
                'pybirdai_ordinate_item',
                'pybirdai_cell_position'
            }

            if table_name not in ALLOWED_TABLES:
                raise ValueError(f"Table '{table_name}' not allowed for deletion")

            # PHASE 1: Backup existing data before truncation
            backup_table_name = f"{table_name}_backup_temp"
            print(f"Backing up existing data from {table_name}...")
            self._backup_table_data(table_name, backup_table_name)

            with connection.cursor() as cursor:
                if connection.vendor == 'sqlite':
                    cursor.execute("PRAGMA foreign_keys = 0;")
                    # Table name is validated against whitelist above
                    cursor.execute(f"DELETE FROM {table_name};")
                    cursor.execute("PRAGMA foreign_keys = 1;")
                elif connection.vendor == 'postgresql':
                    # Table name is validated against whitelist above
                    cursor.execute(f"TRUNCATE TABLE {table_name} CASCADE;")
                elif connection.vendor in ['microsoft', 'mssql']:
                    # Table name is validated against whitelist above
                    cursor.execute(f"TRUNCATE TABLE {table_name};")
                else:
                    # Table name is validated against whitelist above
                    cursor.execute(f"DELETE FROM {table_name};")

            # Import CSV data based on database vendor
            if connection.vendor == 'sqlite':
                # SQLite needs to be handled outside transaction for subprocess
                # Get database file path
                db_file = Path(connection.settings_dict['NAME']).absolute()

                # Create the SQLite commands
                commands = [
                    ".mode csv",
                    f".separator '{delimiter}'",
                    f".import --skip 1 '{csv_file}' {table_name}"
                ]

                # Join commands with newlines
                sqlite_script = '\n'.join(commands)

                sqlite_program = "sqlite3"
                if platform.system() == 'Windows':
                    sqlite_program += ".exe"

                result = subprocess.run(
                        [sqlite_program, str(db_file)],
                        input=sqlite_script,
                        text=True,
                        capture_output=True,
                        check=True
                    )
                if result.stderr:
                    raise Exception(f"SQLite import error: {result.stderr}")

                # PHASE 3-4: Restore backed-up data with key regeneration (bulk)
                print(f"Restoring backed-up data to {table_name} with key regeneration...")
                self._restore_backed_up_data_bulk(table_name, backup_table_name, str(csv_file))

                # PHASE 5: Cleanup backup table
                print(f"Cleaning up backup table {backup_table_name}...")
                self._cleanup_backup_table(backup_table_name)

                return result

            elif connection.vendor == 'postgresql':
                # PostgreSQL COPY command
                with connection.cursor() as cursor:
                    with open(csv_file, 'r', encoding='utf-8') as f:
                        # Skip header line
                        next(f)
                        # Use COPY FROM STDIN
                        cursor.copy_expert(
                            f"COPY {table_name} FROM STDIN WITH (FORMAT CSV, DELIMITER '{delimiter}')",
                            f
                        )

                # PHASE 3-4: Restore backed-up data with key regeneration (bulk)
                print(f"Restoring backed-up data to {table_name} with key regeneration...")
                self._restore_backed_up_data_bulk(table_name, backup_table_name, str(csv_file))

                # PHASE 5: Cleanup backup table
                print(f"Cleaning up backup table {backup_table_name}...")
                self._cleanup_backup_table(backup_table_name)

                return None

            elif connection.vendor in ['microsoft', 'mssql']:
                # MSSQL bulk insert - requires special handling
                # Note: BULK INSERT requires the file to be accessible by SQL Server
                with connection.cursor() as cursor:
                    # Try using BULK INSERT if file is accessible
                    try:
                        cursor.execute(f"""
                            BULK INSERT {table_name}
                            FROM '{csv_file}'
                            WITH (
                                FORMAT = 'CSV',
                                FIRSTROW = 2,
                                FIELDTERMINATOR = '{delimiter}',
                                ROWTERMINATOR = '\\n'
                            )
                        """)
                    except Exception as e:
                        # Fallback to row-by-row insert if BULK INSERT fails
                        print(f"BULK INSERT failed: {e}. Falling back to row-by-row insert.")
                        self._fallback_csv_import(csv_file, table_name, delimiter)

                # PHASE 3-4: Restore backed-up data with key regeneration (bulk)
                print(f"Restoring backed-up data to {table_name} with key regeneration...")
                self._restore_backed_up_data_bulk(table_name, backup_table_name, str(csv_file))

                # PHASE 5: Cleanup backup table
                print(f"Cleaning up backup table {backup_table_name}...")
                self._cleanup_backup_table(backup_table_name)

                return None

            else:
                # Fallback for other databases
                print(f"Database vendor '{connection.vendor}' not explicitly supported. Using fallback method.")
                self._fallback_csv_import(csv_file, table_name, delimiter)

                # PHASE 3-4: Restore backed-up data with key regeneration (bulk)
                print(f"Restoring backed-up data to {table_name} with key regeneration...")
                self._restore_backed_up_data_bulk(table_name, backup_table_name, str(csv_file))

                # PHASE 5: Cleanup backup table
                print(f"Cleaning up backup table {backup_table_name}...")
                self._cleanup_backup_table(backup_table_name)

                return None

        except Exception as e:
            print(f"Error importing CSV for {table_name}: {str(e)}")
            # Try to cleanup backup table even on error
            try:
                backup_table_name = f"{table_name}_backup_temp"
                self._cleanup_backup_table(backup_table_name)
            except:
                pass
            raise

    def _fallback_csv_import(self, context, cls):
        '''
        Fallback method for CSV import using raw SQL inserts
        This method is database-agnostic and works with any database backend
        '''
        import csv

        fallback_import_func = {
            TABLE_CELL: self.create_table_cells,
            ORDINATE_ITEM: self.create_ordinate_items,
            CELL_POSITION: self.create_cell_positions
            }[cls](context)

        try:
            self.fallback_import_func(context)
        except Exception as e:
            print(f"Error in fallback CSV import for {table_name}: {str(e)}")
            raise

    def create_table_cells_csv_copy(self, context):
        # Ensure paths are absolute
        self._create_instances_from_csv_copy(context, TABLE_CELL)

    def create_ordinate_items_csv_copy(self, context):
        # Ensure paths are absolute
        self._create_instances_from_csv_copy(context, ORDINATE_ITEM)

    def create_cell_positions_csv_copy(self, context):
        # Ensure paths are absolute
        self._create_instances_from_csv_copy(context, CELL_POSITION)

    def create_ordinate_items(self, context):
        '''
        Import all ordinate items from the rendering package CSV file
        '''
        file_location = context.file_directory + os.sep + "technical_export" + os.sep + "ordinate_item.csv"
        header_skipped = False
        ordinate_items_to_create = []

        with open(file_location, encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            id_increment = 0
            for row in filereader:
                if not header_skipped:
                    header_skipped = True
                    if row[0].upper() == 'ID': #sometimes exported data without a  primary key has an ID field added at the time of export, exported data is re-imported
                        id_increment = 1
                else:
                    axis_ordinate_id = row[ColumnIndexes().ordinate_item_axis_ordinate_id + id_increment]
                    variable_id = row[ColumnIndexes().ordinate_item_variable_id + id_increment]
                    member_id = row[ColumnIndexes().ordinate_item_member_id + id_increment]
                    member_hierarchy_id = row[ColumnIndexes().ordinate_item_member_hierarchy_id + id_increment]
                    starting_member_id = row[ColumnIndexes().ordinate_item_starting_member_id + id_increment]
                    is_starting_member_included = row[ColumnIndexes().ordinate_item_is_starting_member_included + id_increment]

                    print(f"member_id: {member_id}")
                    print(f"variable_id: {variable_id}")
                    print(f"axis_ordinate_id: {axis_ordinate_id}")
                    print(f"member_hierarchy_id: {member_hierarchy_id}")
                    print(f"starting_member_id: {starting_member_id}")
                    print(f"is_starting_member_included: {is_starting_member_included}")

                    ordinate_item = ORDINATE_ITEM()
                    ordinate_item.axis_ordinate_id = ImportWebsiteToSDDModel.find_axis_ordinate_with_id(
                        self, context, ImportWebsiteToSDDModel.replace_dots(self, axis_ordinate_id))

                    print(ordinate_item.axis_ordinate_id)
                    ordinate_item.variable_id = ImportWebsiteToSDDModel.find_variable_with_id(
                        self, context, ImportWebsiteToSDDModel.replace_dots(self, variable_id))
                    ordinate_item.member_id = ImportWebsiteToSDDModel.find_member_with_id(
                        self, ImportWebsiteToSDDModel.replace_dots(self, member_id), context)
                    ordinate_item.member_hierarchy_id = ImportWebsiteToSDDModel.find_member_hierarchy_with_id(
                        self, ImportWebsiteToSDDModel.replace_dots(self, member_hierarchy_id), context)
                    ordinate_item.starting_member_id = ImportWebsiteToSDDModel.find_member_with_id(
                        self, ImportWebsiteToSDDModel.replace_dots(self, starting_member_id), context)
                    ordinate_item.is_starting_member_included = is_starting_member_included

                    ordinate_items_to_create.append(ordinate_item)

                    try:
                        ordinate_items = context.axis_ordinate_to_ordinate_items_map[ordinate_item.axis_ordinate_id.axis_ordinate_id]
                        ordinate_items.append(ordinate_item)
                    except KeyError:
                        context.axis_ordinate_to_ordinate_items_map[ordinate_item.axis_ordinate_id.axis_ordinate_id] = [ordinate_item]

        if context.save_sdd_to_db and ordinate_items_to_create:
            ORDINATE_ITEM.objects.bulk_create(ordinate_items_to_create, batch_size=50000,ignore_conflicts=True)

    def create_table_cells(self, context, dpm:bool=False):
        '''
        Import all table cells from the rendering package CSV file
        '''
        file_location = context.file_directory + os.sep + "technical_export" + os.sep + "table_cell.csv"
        header_skipped = False
        table_cells_to_create = []

        with open(file_location, encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            id_increment = 0
            for row in filereader:
                if not header_skipped:
                    header_skipped = True
                    if row[0].upper() == 'ID': #sometimes exported data without a  primary key has an ID field added at the time of export, exported data is re-imported
                        id_increment = 1
                else:
                    table_cell_cell_id = row[ColumnIndexes().table_cell_cell_id + id_increment]
                    table_cell_combination_id = row[ColumnIndexes().table_cell_combination_id + id_increment]
                    table_cell_table_id = row[ColumnIndexes().table_cell_table_id + id_increment]

                    if table_cell_cell_id.endswith("_REF") or dpm:
                        table_cell = TABLE_CELL(
                            name=ImportWebsiteToSDDModel.replace_dots(self, table_cell_cell_id))
                        table_cell.cell_id = ImportWebsiteToSDDModel.replace_dots(self, table_cell_cell_id)
                        table_cell.table_id = ImportWebsiteToSDDModel.find_table_with_id(
                            self, context, ImportWebsiteToSDDModel.replace_dots(self, table_cell_table_id))
                        table_cell.table_cell_combination_id = table_cell_combination_id

                        table_cells_to_create.append(table_cell)
                        context.table_cell_dictionary[table_cell.cell_id] = table_cell

                        table_cell_list = context.table_to_table_cell_dictionary.setdefault(table_cell.table_id, [])
                        table_cell_list.append(table_cell)

        if context.save_sdd_to_db and table_cells_to_create:
            TABLE_CELL.objects.bulk_create(table_cells_to_create, batch_size=1000,ignore_conflicts=True)

    def create_cell_positions(self, context, dpm:bool = False):
        '''
        Import all cell positions from the rendering package CSV file
        '''
        file_location = context.file_directory + os.sep + "technical_export" + os.sep + "cell_position.csv"
        header_skipped = False
        cell_positions_to_create = []
        id_increment = 0
        with open(file_location, encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if not header_skipped:
                    header_skipped = True
                    if row[0].upper() == 'ID': #sometimes exported data without a  primary key has an ID field added at the time of export, exported data is re-imported
                        id_increment = 1
                else:
                    cell_positions_cell_id = row[ColumnIndexes().cell_positions_cell_id + id_increment]
                    cell_positions_axis_ordinate_id = row[ColumnIndexes().cell_positions_axis_ordinate_id + id_increment]

                    if cell_positions_cell_id.endswith("_REF") or dpm:
                        cell_position = CELL_POSITION()
                        cell_position.axis_ordinate_id = ImportWebsiteToSDDModel.find_axis_ordinate_with_id(
                            self, context, ImportWebsiteToSDDModel.replace_dots(self, cell_positions_axis_ordinate_id))
                        cell_position.cell_id = ImportWebsiteToSDDModel.find_table_cell_with_id(
                            self, context, ImportWebsiteToSDDModel.replace_dots(self, cell_positions_cell_id))

                        cell_positions_to_create.append(cell_position)

                        cell_positions_list = context.cell_positions_dictionary.setdefault(cell_position.cell_id.cell_id, [])
                        cell_positions_list.append(cell_position)

        if context.save_sdd_to_db and cell_positions_to_create:
            CELL_POSITION.objects.bulk_create(cell_positions_to_create, batch_size=1000,ignore_conflicts=True)

    def create_member_mappings(self, context):
        '''
        Import all member mappings from the rendering package CSV file
        '''
        file_location = context.file_directory + os.sep + "technical_export" + os.sep + "member_mapping.csv"
        header_skipped = False
        member_mappings_to_create = []

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
                            self, context, maintenance_agency_id)

                        member_mappings_to_create.append(member_mapping)
                        context.member_mapping_dictionary[member_mapping_id] = member_mapping

        if context.save_sdd_to_db and member_mappings_to_create:
            MEMBER_MAPPING.objects.bulk_create(member_mappings_to_create, batch_size=1000,ignore_conflicts=True)

    def create_all_member_mappings_items(self, context):
        '''
        Import all member mapping items from the rendering package CSV file using bulk create
        '''

        file_location = context.file_directory + os.sep + "technical_export" + os.sep + "member_mapping_item.csv"
        header_skipped = False
        missing_members = []
        missing_variables = []
        member_mapping_items_to_create = []
        id_increment = 0
        with open(file_location, encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if not header_skipped:
                    header_skipped = True
                    if row[0].upper() == 'ID': #sometimes exported data without a  primary key has an ID field added at the time of export, exported data is re-imported
                        id_increment = 1
                else:
                    member_mapping_id = row[ColumnIndexes().member_mapping_id + id_increment]
                    row_number = row[ColumnIndexes().member_mapping_row + id_increment]
                    variable_id = row[ColumnIndexes().member_mapping_variable_id + id_increment]
                    is_source = row[ColumnIndexes().member_mapping_is_source + id_increment]
                    member_id = row[ColumnIndexes().member_mapping_member_id + id_increment]
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
                            member_mapping_item.member_id = member
                            member_mapping_item.variable_id = variable
                            member_mapping_item.member_mapping_row = row_number
                            member_mapping_item.member_mapping_id  = ImportWebsiteToSDDModel.find_member_mapping_with_id(
                                                self,context,member_mapping_id)

                            if context.save_sdd_to_db:
                                member_mapping_items_to_create.append(member_mapping_item)
                            try:
                                member_mapping_items_list = context.member_mapping_items_dictionary[member_mapping_id]
                                member_mapping_items_list.append(member_mapping_item)
                            except KeyError:
                                context.member_mapping_items_dictionary[member_mapping_id] = [member_mapping_item]
        if context.save_sdd_to_db and member_mapping_items_to_create:
            MEMBER_MAPPING_ITEM.objects.bulk_create(member_mapping_items_to_create, batch_size=1000,ignore_conflicts=True)
        for missing_member in missing_members:
            print(f"Missing member {missing_member}")
        for missing_variable in missing_variables:
            print(f"Missing variable {missing_variable}")
        ImportWebsiteToSDDModel.save_missing_mapping_variables_to_csv(context,missing_variables)
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
        Import all mapping definitions from the rendering package CSV file using bulk create
        '''
        file_location = context.file_directory + os.sep + "technical_export" + os.sep + "mapping_definition.csv"
        mapping_definitions_to_create = []

        # Cache lookups
        member_mapping_cache = {}
        variable_mapping_cache = {}

        with open(file_location, encoding='utf-8') as csvfile:
            rows = list(csv.reader(csvfile))[1:]  # Skip header

            for row in rows:
                mapping_id = row[ColumnIndexes().mapping_definition_mapping_id]
                if mapping_id.startswith("SHS_"):
                    continue

                member_mapping_id = row[ColumnIndexes().mapping_definition_member_mapping_id]
                if member_mapping_id not in member_mapping_cache:
                    member_mapping_cache[member_mapping_id] = ImportWebsiteToSDDModel.find_member_mapping_with_id(
                        self, context, member_mapping_id)

                variable_mapping_id = row[ColumnIndexes().mapping_definition_variable_mapping_id]
                if variable_mapping_id not in variable_mapping_cache:
                    variable_mapping_cache[variable_mapping_id] = ImportWebsiteToSDDModel.find_variable_mapping_with_id(
                        self, context, variable_mapping_id)

                mapping_definition = MAPPING_DEFINITION(
                    mapping_id=mapping_id,
                    name=row[ColumnIndexes().mapping_definition_name],
                    code=row[ColumnIndexes().mapping_definition_code],
                    mapping_type=row[ColumnIndexes().mapping_definition_mapping_type],
                    member_mapping_id=member_mapping_cache[member_mapping_id],
                    variable_mapping_id=variable_mapping_cache[variable_mapping_id]
                )

                mapping_definitions_to_create.append(mapping_definition)
                context.mapping_definition_dictionary[mapping_id] = mapping_definition

        if context.save_sdd_to_db and mapping_definitions_to_create:
            MAPPING_DEFINITION.objects.bulk_create(mapping_definitions_to_create, batch_size=5000,ignore_conflicts=True)

    def create_all_mapping_to_cubes(self, context):
        '''
        Import all mapping to cubes from the rendering package CSV file
        '''
        file_location = context.file_directory + os.sep + "technical_export" + os.sep + "mapping_to_cube.csv"
        header_skipped = False
        mapping_to_cubes_to_create = []

        with open(file_location, encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            id_increment = 0
            for row in filereader:
                if not header_skipped:
                    header_skipped = True
                    if row[0].upper() == 'ID': #sometimes exported data without a  primary key has an ID field added at the time of export, exported data is re-imported
                        id_increment = 1
                else:
                    mapping_to_cube_mapping_id = row[ColumnIndexes().mapping_to_cube_mapping_id + id_increment]
                    mapping_to_cube_cube_mapping_id = row[ColumnIndexes().mapping_to_cube_cube_mapping_id + id_increment]
                    mapping_to_cube_valid_from = row[ColumnIndexes().mapping_to_cube_valid_from + id_increment]
                    mapping_to_cube_valid_to = row[ColumnIndexes().mapping_to_cube_valid_to + id_increment]

                    if not mapping_to_cube_mapping_id.startswith("M_SHS"):
                        mapping_to_cube = MAPPING_TO_CUBE(
                            mapping_id=ImportWebsiteToSDDModel.find_mapping_definition_with_id(self, context, mapping_to_cube_mapping_id),
                            cube_mapping_id=ImportWebsiteToSDDModel.replace_dots(self, mapping_to_cube_cube_mapping_id),
                            valid_from=mapping_to_cube_valid_from,
                            valid_to=mapping_to_cube_valid_to
                        )

                        mapping_to_cubes_to_create.append(mapping_to_cube)

                        mapping_to_cube_list = context.mapping_to_cube_dictionary.setdefault(
                            mapping_to_cube.cube_mapping_id, [])
                        mapping_to_cube_list.append(mapping_to_cube)

        if context.save_sdd_to_db and mapping_to_cubes_to_create:
            MAPPING_TO_CUBE.objects.bulk_create(mapping_to_cubes_to_create, batch_size=1000,ignore_conflicts=True)

    def create_all_variable_mappings(self, context):
        '''
        Import all variable mappings from the rendering package CSV file
        '''
        file_location = context.file_directory + os.sep + "technical_export" + os.sep + "variable_mapping.csv"

        # Pre-filter SHS_ entries and build batch
        variable_mappings_to_create = []

        # Read entire CSV at once instead of line by line
        with open(file_location, encoding='utf-8') as csvfile:
            rows = list(csv.reader(csvfile))[1:]  # Skip header

            # Process in a single pass
            for row in rows:
                variable_mapping_id = row[ColumnIndexes().variable_mapping_variable_mapping_id]

                if not variable_mapping_id.startswith("SHS_") and variable_mapping_id not in context.variable_mapping_dictionary:
                    variable_mapping = VARIABLE_MAPPING(
                        variable_mapping_id=variable_mapping_id,
                        maintenance_agency_id=ImportWebsiteToSDDModel.find_maintenance_agency_with_id(
                            self, context, row[ColumnIndexes().variable_mapping_maintenance_agency_id]),
                        code=row[ColumnIndexes().variable_mapping_code],
                        name=row[ColumnIndexes().variable_mapping_name]
                    )

                    variable_mappings_to_create.append(variable_mapping)
                    context.variable_mapping_dictionary[variable_mapping_id] = variable_mapping

        # Single bulk create with larger batch size
        if context.save_sdd_to_db and variable_mappings_to_create:
            VARIABLE_MAPPING.objects.bulk_create(variable_mappings_to_create, batch_size=5000,ignore_conflicts=True)

    def create_all_variable_mapping_items(self, context):
        '''
        Import all variable mapping items from the rendering package CSV file
        '''
        file_location = context.file_directory + os.sep + "technical_export" + os.sep + "variable_mapping_item.csv"
        missing_variables = []
        variable_mapping_items_to_create = []
        id_increment = 0
        # Cache variable lookups
        variable_cache = {}

        with open(file_location, encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            header_skipped = False
            id_increment = 0
            for row in filereader:
                if not header_skipped:
                    header_skipped = True
                    if row[0].upper() == 'ID': #sometimes exported data without a  primary key has an ID field added at the time of export, exported data is re-imported
                        id_increment = 1
                else:
                    mapping_id = row[ColumnIndexes().varaible_mapping_item_variable_mapping_id + id_increment]
                    if mapping_id.startswith("SHS_"):
                        continue

                    variable_id = row[ColumnIndexes().variable_mapping_item_variable_id + id_increment]

                    # Use cached variable lookup
                    if variable_id not in variable_cache:
                        variable_cache[variable_id] = ImportWebsiteToSDDModel.find_variable_with_id(
                            self, context, variable_id)

                    variable = variable_cache[variable_id]

                    if variable is None:
                        missing_variables.append((
                            variable_id,
                            mapping_id,
                            row[ColumnIndexes().variable_mapping_item_valid_to + id_increment]
                        ))
                        continue

                    variable_mapping_item = VARIABLE_MAPPING_ITEM(
                        variable_id=variable,
                        variable_mapping_id=ImportWebsiteToSDDModel.find_variable_mapping_with_id(
                            self, context, mapping_id),
                        is_source=row[ColumnIndexes().variable_mapping_item_is_source + id_increment],
                        valid_from=row[ColumnIndexes().variable_mapping_item_valid_from + id_increment],
                        valid_to=row[ColumnIndexes().variable_mapping_item_valid_to + id_increment]
                    )

                    variable_mapping_items_to_create.append(variable_mapping_item)

                    # Build dictionary in a single operation
                    context.variable_mapping_item_dictionary.setdefault(mapping_id, []).append(variable_mapping_item)

        # Single bulk create with larger batch size
        if context.save_sdd_to_db and variable_mapping_items_to_create:
            VARIABLE_MAPPING_ITEM.objects.bulk_create(variable_mapping_items_to_create, batch_size=5000,ignore_conflicts=True)

        if missing_variables:
            ImportWebsiteToSDDModel.save_missing_mapping_variables_to_csv(context, missing_variables)

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
            return context.member_dictionary[element_id]
        except:
            try:
                return context.member_dictionary[element_id]
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
            return context.variable_dictionary[element_id]
        except KeyError:
            try:
                return context.variable_dictionary[element_id]
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
            return context.domain_dictionary[element_id]
        except KeyError:
            try:
                return_item = context.domain_dictionary[element_id]
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
        '''
        Delete warning files more efficiently using pathlib
        '''
        warnings_dir = Path(settings.BASE_DIR) / 'results' / 'generated_hierarchy_warnings'
        for file in warnings_dir.glob('*'):
            file.unlink()

    def delete_mapping_warnings_files(self, context):
        base_dir = settings.BASE_DIR
        mapping_warnings_dir = os.path.join(base_dir, 'results', 'generated_mapping_warnings')
        for file in os.listdir(mapping_warnings_dir):
            os.remove(os.path.join(mapping_warnings_dir, file))
