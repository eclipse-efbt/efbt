# coding=UTF-8#
# Copyright (c) 2024 Bird Software Solutions Ltd
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
Created on 22 Jan 2022

@author: Neil
'''

import os
import csv
from pybirdai.utils.utils import Utils
from pybirdai.bird_meta_data_model import *
from pybirdai.context.csv_column_index_context import ColumnIndexes
from pybirdai.process_steps.website_to_sddmodel.import_website_to_sdd_model_django import ImportWebsiteToSDDModel


class SMCubesILImport(object):
    '''
    Documentation for SMCubesILImport
    '''
    def do_import(self, sdd_context):
        ImportWebsiteToSDDModel.create_maintenance_agencies(self, sdd_context)
        ImportWebsiteToSDDModel.create_frameworks(self, sdd_context)
        ImportWebsiteToSDDModel.create_all_domains(self, sdd_context,True)
        ImportWebsiteToSDDModel.create_all_members(self, sdd_context,True)
        ImportWebsiteToSDDModel.create_all_variables(self, sdd_context,True)
        SMCubesILImport.create_all_cube_structures(self, sdd_context)
        SMCubesILImport.create_all_cubes(self, sdd_context)
        SMCubesILImport.create_all_cube_structure_items(self, sdd_context)


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
                        context.cubes.cubes.append(cube)


    def create_all_cube_structures(self, context):
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
                        context.cube_structures.cubeStructures.append(cube_structure)

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
    
                        context.cube_structure_items.cubeStructureItems.append(cube_structure_item)