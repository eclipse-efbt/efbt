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
from pickle import NONE

'''
Created on 22 Jan 2022

@author: Neil
'''
import os
import csv
from utils.utils import Utils
from regdna import ELClass, ELEnum, ELEnumLiteral, ELPublicOperation, ELReference, ELAttribute

class TranslateSDDModelToDataModel(object):
    '''
    Documentation for TranslateSDDModelToDataModel
    '''

    def do_import(self, context,sdd_context):
        '''
        import the items from the Output layer csv files
        '''
        TranslateSDDModelToDataModel.add_classes_to_package(self, context,sdd_context)
        TranslateSDDModelToDataModel.add_enums_and_literals_to_package(self, context,sdd_context)
        TranslateSDDModelToDataModel.add_attributes_to_classes(self, context,sdd_context)

    def add_classes_to_package(self, context,sdd_context):
        '''
        Add the classes to the package
        '''
        for cube  in sdd_context.cubes.cubes:
            framework = cube.framework_id.framework_id
            if (framework == context.reporting_framework + "_REF"):
                class_name = cube.code
            else:
                if (context.use_codes):
                    class_name = cube.code
                else:
                    class_name = cube.name
                    
            altered_class_name = Utils.make_valid_id(class_name)
            object_id = cube.cube_id
            cube_type = cube.cube_type
            derived = True

            if ((((framework == context.reporting_framework +"_REF") and (cube_type == "RC")) or ( context.load_eil_from_website and ((cube_type == "EIL"))) )):

                altered_class_name = Utils.make_valid_id(class_name)
                fullName = None
                if cube_type == "EIL" : 
                    fullName=altered_class_name
                else:
                    fullName=altered_class_name+"_OutputItem"
                
                if cube_type == "EIL":
                    derived = False
                    
                eclass = ELClass(name=fullName)
                eclass.isDerived = derived
                if context.add_pks_to_input_layer_from_website:
                    if cube_type == "EIL":
                        pk_name = fullName + "_uniqueID"
                        attribute = ELAttribute()
                        attribute.name = pk_name
                        attribute.eType = context.types.e_string
                        attribute.eAttributeType = context.types.e_string
                        attribute.iD = True
                        attribute.lowerBound = 0
                        attribute.upperBound = 1
                        eclass.eStructuralFeatures.append(attribute)

                if not (cube_type == "EIL"):
                    context.output_tables_package.eClassifiers.extend([
                                                                       eclass])
                else:
                    context.input_tables_package.eClassifiers.extend([
                                                                       eclass])

                # maintain a map a objectIDs to ELClasses
                context.classes_map[object_id] = eclass




    def add_enums_and_literals_to_package(self, context,sdd_context):
        '''
        Add the Enums and Literals to the package
        '''
       
        for cube_structure_item in sdd_context.cube_structure_items.cubeStructureItems:
            
            
            class_id = cube_structure_item.cube_structure_id.cube_structure_id
            try: 
                the_class = context.classes_map[class_id]               
                variable = cube_structure_item.variable_id.code
                attributeList = [variable]
                if (variable=="MTRCS"):
                    variable_set = cube_structure_item.variable_set_id
                    variable_set_id = None
                    if not(variable_set is None):
                        variable_set_id = variable_set.variable_set_id
                    attributeList = sdd_context.variable_set_to_variable_map[variable_set_id]
                if (variable == "VALUE_DECIMAL") or (variable == "OBSERVATION_VALUE"):
                    attributeList = []
                    
                for attribute_name in attributeList:
                    try: 

                        domain_id = sdd_context.variable_to_domain_map[attribute_name]
                        amended_domain_name = Utils.make_valid_id(domain_id)
                        the_enum = Utils.find_enum(
                            amended_domain_name+"_domain", context.enum_map)
                        if the_enum is None:
                            if not domain_id in context.missing_domains:
                                context.missing_domains.append(domain_id)
                    except:
                        print("missing  class2: ")
                        print(class_id)
            except:
                        print( "missing  class2: " )
                        print(class_id)

        for the_domain in context.missing_domains:
    
            amended_domain_name = Utils.make_valid_id(the_domain) + "_domain"
            if not ((amended_domain_name == "String") or (amended_domain_name == "Date")):
                the_enum = ELEnum()
                the_enum.name = amended_domain_name
                # maintain a map of enum IDS to ELEnum objects
                context.enum_map[amended_domain_name] = the_enum
                context.sdd_domains_package.eClassifiers.extend([the_enum])
                the_domain_members = Utils.get_members_of_the_domain(
                    the_domain, sdd_context.member_id_to_domain_map)
                counter1 = 0
                for member in the_domain_members:
                    enum_literal = ELEnumLiteral()
                    enum_used_name = Utils.make_valid_id(
                        sdd_context.member_id_to_member_code_map[member])
                    adapted_value = Utils.make_valid_id(
                        sdd_context.member_id_to_member_name_map[member])
                    new_adapted_value = Utils.unique_value(the_enum, adapted_value)
                    new_adapted_name = Utils.unique_name(the_enum, enum_used_name)
    
                    enum_literal.name = new_adapted_value
                    enum_literal.literal = new_adapted_name
                    counter1 = counter1 + 1
                    enum_literal.value = counter1
                    the_enum.eLiterals.extend([enum_literal])

    
                            
    def add_attributes_to_classes(self, context,sdd_context):
        '''
        For each attribute add an XAttribute to the correct ELClass representing the Entity
        the attribute should have the correct type, which may be a specific
        enumeration
        '''
        
        for cube_structure_item in sdd_context.cube_structure_items.cubeStructureItems:
            class_id = cube_structure_item.cube_structure_id.cube_structure_id
            try:
                the_class = context.classes_map[class_id]
                the_attribute_name1 = cube_structure_item.variable_id.variable_id
                long_name=None
                variable = cube_structure_item.variable_id.variable_id
                attribute_list = [the_attribute_name1]
                if (the_attribute_name1=="MTRCS"):
                    variable_set = cube_structure_item.variable_set_id.variable_set_id
                    attribute_list = sdd_context.variable_set_to_variable_map[variable_set]
                if (the_attribute_name1 == "VALUE_DECIMAL") or (the_attribute_name1 == "OBSERVATION_VALUE"):
                    attribute_list = []
                for attribute_name in attribute_list: 
                
                    try: 
                        try:
                            long_name = sdd_context.variable_to_long_names_map[attribute_name]
                        except:
                            long_name = attribute_name
                        amended_attribute_name = Utils.make_valid_id(attribute_name)
                        amended_attribute_long_name = Utils.make_valid_id(long_name)
                        
                        the_class = context.classes_map[class_id]
                        
                        class_is_derived = the_class.isDerived
                        
                        
                        
                        if(context.use_codes):   
                            the_attribute_name = amended_attribute_name  
                        else:
                            the_attribute_name = amended_attribute_long_name
                        
                        amended_domain_name = None
                        if (the_attribute_name1=="MTRCS"):
                            domain_id = sdd_context.variable_to_domain_map[attribute_name]
                            amended_domain_name = Utils.make_valid_id(domain_id+"_domain")
                        else:   
                            domain_id = sdd_context.variable_to_domain_map[variable]
                            amended_domain_name = Utils.make_valid_id(domain_id+"_domain")

                      
                        the_enum =  Utils.find_enum(amended_domain_name,context.enum_map)
                        if  the_enum is not None:                     
                            
                            if class_is_derived:
                                operation = ELPublicOperation()
                                operation.lowerBound=0
                                operation.upperBound=1
                                if(the_enum.name == "String"):
                                    operation.name = the_attribute_name
                                    operation.eType = context.types.e_string
                                elif(the_enum.name == "Date"):
                                    operation.name = the_attribute_name
                                    operation.eType = context.types.e_date
                                elif(the_enum.name.startswith("String_")):
                                    operation.name = the_attribute_name
                                    operation.eType = context.types.e_string
                                elif(the_enum.name == "STRNG_domain"):
                                        operation.name = the_attribute_name
                                        operation.eType = context.types.e_string
                                elif(the_enum.name == "EBA_String_domain"):
                                        operation.name = the_attribute_name
                                        operation.eType = context.types.e_string
                                elif(the_enum.name == "DT_domain"):
                                        operation.name = the_attribute_name
                                        operation.eType = context.types.e_date
                                elif(the_enum.name == "Number"):
                                    operation.name = the_attribute_name
                                    operation.eType = context.types.e_double
                                elif(the_enum.name == "RL_domain"):
                                    operation.name = the_attribute_name
                                    operation.eType = context.types.e_double
                                
                                elif(the_enum.name.startswith("Real_")):
                                    operation.name = the_attribute_name
                                    operation.eType = context.types.e_double
                                elif(the_enum.name.startswith("Monetary")): 
                                    operation.name = the_attribute_name
                                    operation.eType = context.types.e_int
                                elif(the_enum.name.startswith("INTGR_domain")): 
                                    operation.name = the_attribute_name
                                    operation.eType = context.types.e_int
                                elif(the_enum.name.startswith("MNTRY")): 
                                    operation.name = the_attribute_name
                                    operation.eType = context.types.e_int
                                elif(the_enum.name.startswith("Monetary_domain")): 
                                    operation.name = the_attribute_name
                                    operation.eType = context.types.e_int
                                elif(the_enum.name.startswith("Non_negative_monetary_amounts_with_2_decimals")): 
                                    operation.name = the_attribute_name
                                    operation.eType = context.types.e_int
                                elif(the_enum.name.startswith("Non_negative_integers")): 
                                    operation.name = the_attribute_name
                                    operation.eType = context.types.e_int
                                elif(the_enum.name.startswith("All_possible_dates")):   
                                    operation.name = the_attribute_name
                                    operation.eType = context.types.e_date  
                                else:
                                    operation.name = the_attribute_name
                                    operation.eType = the_enum  
                                
                                try:
                                    the_class = context.classes_map[class_id]
                                    
                                    if class_is_derived:
                                        the_class.eOperations.extend([operation])
                                except:
                                    print( "missing class2: " )
                                    
                            else:
                                attribute = ELAttribute()
                                attribute.lowerBound=0
                                attribute.upperBound=1
                                if(the_enum.name == "String"):
                                    attribute.name = the_attribute_name
                                    attribute.eType = context.types.e_string
                                    attribute.eAttributeType = context.types.e_string
                                elif(the_enum.name == "Date"):
                                    attribute.name = the_attribute_name
                                    attribute.eType = context.types.e_date
                                    attribute.eAttributeType = context.types.e_string
                                elif(the_enum.name.startswith("String_")):
                                    attribute.name = the_attribute_name
                                    attribute.eType = context.types.e_string
                                    attribute.eAttributeType = context.types.e_string
                                elif(the_enum.name == "STRNG_domain"):
                                        attribute.name = the_attribute_name
                                        attribute.eType = context.types.e_string
                                        attribute.eAttributeType = context.types.e_string
                                elif(the_enum.name == "EBA_String_domain"):
                                        attribute.name = the_attribute_name
                                        attribute.eType = context.types.e_string
                                        attribute.eAttributeType = context.types.e_string
                                elif(the_enum.name == "DT_domain"):
                                        attribute.name = the_attribute_name
                                        attribute.eType = context.types.e_date
                                        attribute.eAttributeType = context.types.e_date
                                elif(the_enum.name == "Number"):
                                    attribute.name = the_attribute_name
                                    attribute.eType = context.types.e_double
                                    attribute.eAttributeType = context.types.e_double
                                elif(the_enum.name == "RL_domain"):
                                    attribute.name = the_attribute_name
                                    attribute.eType = context.types.e_double
                                    attribute.eAttributeType = context.types.e_double
                                
                                elif(the_enum.name.startswith("Real_")):
                                    attribute.name = the_attribute_name
                                    attribute.eType = context.types.e_double
                                    attribute.eAttributeType = context.types.e_double
                                elif(the_enum.name.startswith("Monetary")): 
                                    attribute.name = the_attribute_name
                                    attribute.eType = context.types.e_int
                                    attribute.eAttributeType = context.types.e_int
                                elif(the_enum.name.startswith("MNTRY")): 
                                    attribute.name = the_attribute_name
                                    attribute.eType = context.types.e_int
                                elif(the_enum.name.startswith("INTGR_domain")): 
                                    attribute.name = the_attribute_name
                                    attribute.eType = context.types.e_int 
                                    attribute.eAttributeType = context.types.e_int
                                elif(the_enum.name.startswith("Monetary_domain")): 
                                    attribute.name = the_attribute_name
                                    attribute.eType = context.types.e_int
                                    attribute.eAttributeType = context.types.e_int
                                elif(the_enum.name.startswith("Non_negative_monetary_amounts_with_2_decimals")): 
                                    attribute.name = the_attribute_name
                                    attribute.eType = context.types.e_int
                                    attribute.eAttributeType = context.types.e_int
                                elif(the_enum.name.startswith("Non_negative_integers")): 
                                    attribute.name = the_attribute_name
                                    attribute.eType = context.types.e_int
                                    attribute.eAttributeType = context.types.e_int
                                elif(the_enum.name.startswith("All_possible_dates")):   
                                    attribute.name = the_attribute_name
                                    attribute.eType = context.types.e_date  
                                    attribute.eAttributeType = context.types.e_date
                                else:
                                    attribute.name = the_attribute_name
                                    attribute.eType = the_enum  
                                    attribute.eAttributeType = the_enum
                                
                                try:
                                    the_class = context.classes_map[class_id]
                                    
                                    the_class.eStructuralFeatures.extend([attribute])
                                    
                                except:
                                    print( "missing class2: " )

                        else:
                            print( "XXXXX missing domainID: " )
                            print(domain_id)
                            print(class_id)
                            if not(domain_id in context.missing_domains):
                                context.missing_domains.append(domain_id)
                    except:
                            print( "XX missing class1: " )
                            print(class_id) 
            except:
                print("XX missing class1: ")
                print(class_id)
