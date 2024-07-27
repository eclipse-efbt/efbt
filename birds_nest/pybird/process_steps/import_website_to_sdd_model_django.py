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
from pybird.models import *
from pybird.context.sdd_context_django import SDDContext
from pybird.utils.utils import Utils
from pybird.context.csv_column_index_context import ColumnIndexes


class ImportWebsiteToSDDModel(object):
    '''
    Class responsible for the import of  SDD csv files
    into an instance of the analaysis model
    '''
    def import_sdd(self):
        '''
        Import SDD csv files into an instance of the analysis model
        '''
        sdd_context = SDDContext()
        sdd_context.file_directory = 'C:\\Users\\neil\\freebirdtools-develop-July11\\git\\efbt\\bird\\birdseed_creator\\resources'
        sdd_context.output_directory = 'C:\\Users\\neil\\freebirdtools-develop-July11\\git\\efbt\\bird\\birdseed_creator\\results'
        ImportWebsiteToSDDModel.create_maintenance_agencies(self, sdd_context)
        ImportWebsiteToSDDModel.create_frameworks(self, sdd_context)
        ImportWebsiteToSDDModel.create_all_domains(self, sdd_context)
        ImportWebsiteToSDDModel.create_all_members(self, sdd_context)
        ImportWebsiteToSDDModel.create_all_subdomains(self, sdd_context)
        ImportWebsiteToSDDModel.create_all_subdomain_enumerations(self, sdd_context)
        ImportWebsiteToSDDModel.create_all_variables(self, sdd_context)
        ImportWebsiteToSDDModel.create_all_variable_sets(self, sdd_context)

        ImportWebsiteToSDDModel.create_all_combinations(self, sdd_context)
        ImportWebsiteToSDDModel.create_all_member_hierarchies(self, sdd_context)
        ImportWebsiteToSDDModel.create_all_member_hierarchies_nodes(self, sdd_context)
        
        ImportWebsiteToSDDModel.create_all_member_mappings(self, sdd_context)
        ImportWebsiteToSDDModel.create_all_cube_structures(self, sdd_context)
        ImportWebsiteToSDDModel.create_all_cubes(self, sdd_context)
        ImportWebsiteToSDDModel.create_all_cube_structure_items(self, sdd_context)
        
        
        ImportWebsiteToSDDModel.create_report_tables(self, sdd_context)
        ImportWebsiteToSDDModel.create_table_cells(self, sdd_context)
        ImportWebsiteToSDDModel.create_axis(self, sdd_context)
        ImportWebsiteToSDDModel.create_axis_ordinates(self, sdd_context)
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
                    framework.save()
                
    def import_core_sdd(self, sdd_context):
        '''
        Import core SDD csv files into an instance of the analysis model.
        This is useful when we dont need all the input files to be provided
        and it is a bit faster
        '''
        ImportWebsiteToSDDModel.create_all_domains(self, sdd_context)
        ImportWebsiteToSDDModel.create_all_members(self, sdd_context)
        ImportWebsiteToSDDModel.create_all_variables(self, sdd_context)
        ImportWebsiteToSDDModel.create_member_mappings(self, sdd_context,
                                         'EBA_MCY','TYP_INSTRMNT',
                                         'TYP_ACCNTNG_ITM' )

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

                    if (valid_to == "12/31/9999") or (valid_to == "12/31/2999") or (valid_to == "9999-12-31")\
                            or (valid_to == "31/12/9999") or (valid_to == "31/12/2999"):
                        variable_set_enumeration = VARIABLE_SET_ENUMERATION()
                        variable_set_enumeration.variable_id = \
                                ImportWebsiteToSDDModel.find_variable_with_id(self,context,variable_id)
                        variable_set_enumeration.subdomain_id = \
                                ImportWebsiteToSDDModel.get_subdomain_with_id(self,context,subdomain_id)
                        variable_list = None

                        try:
                            variable_list = variable_set_to_variable_map[variable_set]
                        except KeyError:
                            variable_list = []
                            variable_set_to_variable_map[variable_set] = variable_list

                        if not variable_id in variable_list:
                            variable_set_enumeration.save()
                            

        file_location = context.file_directory + os.sep + "variable_set.csv"
        header_skipped = False

        with open(file_location,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if not header_skipped:
                    header_skipped = True
                else:
                    variable_set_id = row[ColumnIndexes().variable_set_variable_set_id]
                    variable_set = VARIABLE_SET()
                    variable_set.variable_set_id = variable_set_id
                    try:
                        variable_set_enumerations = \
                            variable_set_to_variable_map[variable_set_id]
                        for enumeration in variable_set_enumerations:
                            variable_set.variable_set_items.append(enumeration)
                    except KeyError:
                        print("no items for " + variable_set_id )

                    variable_set.save()
                    context.variable_set_dictionary[variable_set_id]    = variable_set

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
                    if (valid_to == "12/31/9999") or (valid_to == "12/31/2999") or (valid_to == "9999-12-31")\
                            or (valid_to == "31/12/9999") or (valid_to == "31/12/2999"):
                    
                        subdomain = ImportWebsiteToSDDModel.get_subdomain_with_id(self, context, subdomain_id)
                        domain = subdomain.domain_id
                        member = ImportWebsiteToSDDModel.find_member_with_id(
                            self, member_id, context)
                        subdomain_enum = SUBDOMAIN_ENUMERATION()
                        subdomain_enum.member_id = member
                        subdomain_enum.save()
        
    def create_all_combinations(self, context):
        file_location = context.file_directory + os.sep + "combination.csv"
        header_skipped = False

        with open(file_location,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if not header_skipped:
                    header_skipped = True
                else:
                    combination_code = row[ColumnIndexes().combination_combination_code]
                    combination_id = row[ColumnIndexes().combination_combination_id]
                    combination_name = row[ColumnIndexes().combination_combination_name]
                    combination_valid_to = row[ColumnIndexes().combination_combination_valid_to]
                    combination_combination_maintenance_agency = row[ColumnIndexes().combination_combination_maintenance_agency]

                    if (combination_combination_maintenance_agency == 'ECB') \
                            and ((combination_valid_to == '12/31/9999') or (combination_valid_to == '9999-12-31')):
                        comb = COMBINATION()
                        comb.code = combination_code
                        comb.combination_id = combination_id
                        comb.name = combination_name
                        comb.save()
                        context.combinations_dictionary[combination_id] = comb

        file_location = context.file_directory + os.sep + "combination_item.csv"
        header_skipped = False

        with open(file_location,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if not header_skipped:
                    header_skipped = True
                else:
                    combination_string = row[ColumnIndexes().combination_item_combination_id] 
                    variable_string = row[ColumnIndexes().combination_item_variable_id]
                    member_string = row[ColumnIndexes().combination_member_id]
                    variable_set = row[ColumnIndexes().combination_variable_set]
                    com = ImportWebsiteToSDDModel.find_combination_with_id(self,context, combination_string)
                    if (not (com is None)) and combination_string.endswith('_REF'):
                        item = COMBINATION_ITEM()
                        mem = ImportWebsiteToSDDModel.find_member_with_id(self,member_string,context)
                        item.member_id =mem
                        if not ((variable_string is None) or (variable_string == "")):
                            variable = ImportWebsiteToSDDModel.find_variable_with_id(self,context,variable_string)
                            item.variable_id = variable
                        if not(variable_set is None) or not(variable_set == ""):
                            variable_set = ImportWebsiteToSDDModel.find_variable_set_with_id(
                                                            self,context,variable_set)
                            item.variable_set_id = variable_set
                        item.save()
                        if variable.variable_id == 'TYP_INSTRMNT':
                            try:
                                context.combination_to_typ_instrmnt_map[mem.member_id].append(com)
                            except KeyError:
                                context.combination_to_typ_instrmnt_map[mem.member_id] = [com]

                        
    def create_all_cubes(self, context):
        file_location = context.file_directory + os.sep + "cube.csv"
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

                    framework_id = row[ColumnIndexes().cube_framework_index]
                    cube_code = row[ColumnIndexes().cube_class_code_index]
                    cube_name = row[ColumnIndexes().cube_class_name_index]
                    object_id = row[ColumnIndexes().cube_object_id_index]
                    cube_type = row[ColumnIndexes().cube_cube_type_index]
                    valid_to = row[ColumnIndexes().cube_valid_to_index]
                    
                    if (valid_to == "12/31/9999") or (valid_to == "12/31/2999") or (valid_to == "9999-12-31") or (valid_to == "9999-12-31")\
                            or (valid_to == "31/12/9999") or (valid_to == "31/12/2999"):
                        cube_structure_id = row[ColumnIndexes().cube_cube_structure_id_index] 
                        framework = ImportWebsiteToSDDModel.find_framework_with_id(self,context, framework_id)
                        cube_structure = ImportWebsiteToSDDModel.find_cube_structure_with_id(self,context, cube_structure_id)
                        cube = CUBE(name=ImportWebsiteToSDDModel.replace_dots(self, cube_code))
                        cube.cube_id = ImportWebsiteToSDDModel.replace_dots(self, object_id)
                        cube.displayName = cube_name
                        cube.framework_id = framework
                        cube.code = cube_code
                        cube.cube_type = cube_type
                        cube.cube_structure_id = cube_structure
                        context.cube_dictionary[ImportWebsiteToSDDModel.replace_dots(self, cube_structure_id)] = cube
                        cube.save()

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
                    
                    if (valid_to == "12/31/9999") or (valid_to == "12/31/2999") or (valid_to == "9999-12-31") \
                            or (valid_to == "31/12/9999") or (valid_to == "31/12/2999"):
                        hierarchy = ImportWebsiteToSDDModel.find_member_hierarchy_with_id(self,hierarchy_id,context)
                        hierarchy_node = MEMBER_HIERARCHY_NODE()
                        hierarchy_node.member_hierarchy_id = hierarchy
                        hierarchy_node.comparator = comparator
                        hierarchy_node.operator = operator
                        hierarchy_node.member_id = member
                        hierarchy_node.level = int(node_level)
                        hierarchy_node.parent_member_id = parent_member
                        context.member_hierarchy_node_dictionary[hierarchy_id + ":" + member_id] = hierarchy_node
                        hierarchy_node.save()
                        
    def create_all_cube_structures(self, context):
        if context.recreate_rol_from_mappings:
            ImportWebsiteToSDDModel.create_all_cube_structures_from_mappings(self, context)
        else:
            file_location = context.file_directory + os.sep + "cube_structure.csv"
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

                        code = row[ColumnIndexes().cube_structure_code_index]
                        id = row[ColumnIndexes().cube_structure_id_index]
                        name = row[ColumnIndexes().cube_structure_name_index]
                        valid_to = row[ColumnIndexes().cube_structure_valid_to_index]
                        version = row[ColumnIndexes().cube_structure_version]
                        description = row[ColumnIndexes().cube_structure_description_index]
                        maintenance_agency_id = row[ColumnIndexes().cube_structure_maintenance_agency]
                        if (valid_to == "12/31/9999") or (valid_to == "12/31/2999") or (valid_to == "9999-12-31")\
                                or (valid_to == "31/12/9999") or (valid_to == "31/12/2999"):
                            maintenance_agency = ImportWebsiteToSDDModel.find_maintenance_agency_with_id(self,context,maintenance_agency_id) 
                            cube_structure = CUBE_STRUCTURE(name=ImportWebsiteToSDDModel.replace_dots(self, code))
                            cube_structure.cube_structure_id = ImportWebsiteToSDDModel.replace_dots(self, id)
                            
                            cube_structure.code = code
                            cube_structure.description = description
                            cube_structure.maintenance_agency_id = maintenance_agency
                            cube_structure.version = version
                            context.cube_structure_dictionary[id] = cube_structure
                            cube_structure.save()



    def create_all_cube_structure_items(self, context):
        file_location = context.file_directory + os.sep + "cube_structure_item.csv"
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

                    variable_id = row[ColumnIndexes().cube_structure_item_variable_index]
                    cube_structure_id = row[ColumnIndexes().cube_structure_item_class_id_index]
                    variable_set_id = row[ColumnIndexes().cube_structure_item_variable_set]
                    subdomain_id = row[ColumnIndexes().cube_structure_item_subdomain_index]
                    role = row[ColumnIndexes().cube_structure_item_role_index]
                    
                    
                    # it is possible that the cube structure item realtes to a cube which is
                    # not currently valid according to its valif_to time. in this case
                    # we do not save the cube_structure_item
                    cube_structure = ImportWebsiteToSDDModel.find_cube_structure_with_id(self,context, cube_structure_id)
                    if not (cube_structure is None):
                        cube_structure_item = CUBE_STRUCTURE_ITEM()
                        cube_structure_item.cube_structure_id = cube_structure
                        
                        variable = ImportWebsiteToSDDModel.find_variable_with_id(self,context, variable_id)
                        cube_structure_item.variable_id = variable
                        
                        variable_set = ImportWebsiteToSDDModel.find_variable_set_with_id(self,context, variable_set_id)
                        cube_structure_item.variable_set_id = variable_set
                        if role == 'D':
                            cube_structure_item.role = 'D'
                        if role == 'O':
                            cube_structure_item.role = 'O'
                        if not (subdomain_id is None) and not(subdomain_id == ""):
                            subdomain = ImportWebsiteToSDDModel.get_subdomain_with_id(self,context, subdomain_id)
                            cube_structure_item.subdomain_id = subdomain
    
                        cube_structure_item.save()
                     
                    

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
                    # not needed yet table.valid_from = valid_from
                    # not needed yet table.valid_to = valid_to
                    if (valid_to == "12/31/9999") or (valid_to == "12/31/2999") or (valid_to == "9999-12-31") \
                            or (valid_to == "31/12/9999") or (valid_to == "31/12/2999"):
                        table.save()
                        context.report_tables_dictionary[table_id] = table
                    
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
                                                    context, axis_ordinate_axis_id)
                    # we don't need the parent axis yet in our processing.
                    # axis_ordinate.axis_ordinate_id = 
                    # axis_ordinate_parent_axis_ordinate_id
                    axis_ordinate.displayName = axis_ordinate_name
                    axis_ordinate.description = axis_ordinate_description
                    axis_ordinate.save()
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
                        table_cell.combination_id = ImportWebsiteToSDDModel.\
                            find_combination_with_id(self, context, table_cell_combination_id)
                        table_cell.table_id = ImportWebsiteToSDDModel.\
                            find_table_with_id(self, context, table_cell_table_id)

                        table_cell.save()
                        context.table_cell_dictionary[table_cell_cell_id] = table_cell
                        try:
                            if not(table_cell.table_id is None):
                                context.table_to_combination_dictionary[ImportWebsiteToSDDModel.replace_dots(self,table_cell.table_id.code)].append(table_cell.combination_id)
                        except KeyError:
                            context.table_to_combination_dictionary[ImportWebsiteToSDDModel.replace_dots(self,table_cell.table_id.code)] = [table_cell.combination_id]

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
                    # check if we already have the table cell created
                    cell_position = None
                    try:
                        cell_position = context.cell_positions_dictionary[cell_positions_cell_id]
                    except KeyError:
                        cell_position = CELL_POSITION()
                        context.cell_positions_dictionary[cell_positions_cell_id] = cell_position 

                    # NEED TO REVIEW THIS, IT WAS A 1 TO N RELATIONSHIP HISTRORICALLY, FOR NOW WE JUST
                    # SET TO ONE ITEM, WILL CHACK IN IT SHOULD BE 1 TO N OR NOT
                    cell_position.axis_ordinate_id = \
                            ImportWebsiteToSDDModel.find_axis_ordinate_with_id(  
                                self, context, cell_positions_axis_ordinate_id)
                    cell_position.cell_id = ImportWebsiteToSDDModel.find_table_cell_with_id(
                                            self, context, cell_positions_cell_id)
                    cell_position.save()

    def create_member_mappings(self, context, source_variable_filter,
                                    target_variable_filter,
                                    target_variable_filter2):
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
                    if ((variable_id == source_variable_filter) and \
                        (is_source == 'true')) or \
                        ((variable_id == target_variable_filter) and \
                        (is_source == 'false')) or \
                        ((variable_id == target_variable_filter2) and \
                        (is_source == 'false')):
                        member_mapping_item = MEMBER_MAPPING_ITEM()
                        member_mapping_item.isSource = is_source
                        member_mapping_item.member = ImportWebsiteToSDDModel.find_member_with_id(
                                                            self,member_id,context)
                        member_mapping_item.variable = ImportWebsiteToSDDModel.find_variable_with_id(
                                                            self,context,variable_id)
                        member_mapping_item.row = row_number

                        member_mapping  = ImportWebsiteToSDDModel.find_member_mapping_with_id(
                                            self,context,member_mapping_id)
                        if member_mapping is None:
                            member_mapping = MEMBER_MAPPING(name = member_mapping_id)
                            member_mapping.code = member_mapping_id
                            member_mapping.member_mapping_id = member_mapping_id
                            member_mapping.save()
                            
                        member_mapping_item.save()
                        context.member_mapping_dictionary[member_mapping_id] = member_mapping
                        
    def create_all_member_mappings(self, context):
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

                    member_mapping  = ImportWebsiteToSDDModel.find_member_mapping_with_id(
                                        self,context,member_mapping_id)
                    if member_mapping is None:
                        member_mapping = MEMBER_MAPPING(name = member_mapping_id)
                        member_mapping.code = member_mapping_id
                        member_mapping.member_mapping_id = member_mapping_id
                        member_mapping.save()
                        
                    member_mapping_item.save()
                    context.member_mapping_dictionary[member_mapping_id] = member_mapping
                        

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

    def find_combination_with_id(self, context, element_id):
        ''' find an existing combination with this id'''
        try:
            return context.combinations_dictionary[element_id]
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
    

    def get_subdomain_with_id(self, context, subdomain_id_string):
        '''
        get the subdomain with the given id
        '''
        # for subdomain in context.subdomains.subdomains:
        #    if subdomain.subdomain_id == subdomain_id_string:
        #        return subdomain

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
            
    def find_cube_structure_with_id(self,context,cube_structure_id):
    
        try:
            return context.cube_structure_dictionary[cube_structure_id]
        except KeyError:
            return None

    def replace_dots(self, text):
        '''
        replace dots with underscores
        '''
        return text.replace('.', '_')

    def get_mappings_with_this_member_as_source_and_this_variable_as_target(
            self,sdd_context,member, target_variable):
        ''' 
        get specific amppings with a secific member as source 
        and a specific variable as target
        '''
        return_target_items = []
        mapping_list = sdd_context.member_mappings.memberMappings

        for mapping in mapping_list:
            member_mapping_items = []
            mapping_items = mapping.memberMappingItems
            for item in mapping_items:
                if (item.member == member) and (item.isSource == 'true'):
                    member_mapping_items.append(item)
                if (item.variable == target_variable) and (item.isSource == 'false'):
                    member_mapping_items.append(item)

            for item in member_mapping_items:
                if item.isSource == 'true':
                    row_id = item.row
                    for item2 in member_mapping_items:
                        if (item2.isSource == 'false') and (item2.row == row_id) \
                            and not(item2.member.name.endswith("_0")):
                            return_target_items.append(item2)
 
        return return_target_items