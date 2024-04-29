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


'''
Created on 22 Jan 2022

@author: Neil
'''

from utils.utils import Utils
from regdna import ELClass, ELEnum, ELEnumLiteral, ELOperation 
from regdna import ELAttribute, ELAnnotation
from regdna import ELStringToStringMapEntry, ELAnnotationDirective
from sdd_model import TYP_DMNSN

class TranslateSDDModelToDataModel(object):
    '''
    Documentation for TranslateSDDModelToDataModel
    '''

    def do_import(self, context,sdd_context):
        '''
        import the items from the Input and Output layer csv files
        '''
        TranslateSDDModelToDataModel.add_classes_to_package(self, context,sdd_context)
        TranslateSDDModelToDataModel.add_enums_and_literals_to_package_for_input_layer(self, context,sdd_context)
        TranslateSDDModelToDataModel.add_enums_and_literals_to_package_for_output_layer(self, context,sdd_context)
        TranslateSDDModelToDataModel.add_attributes_to_classes(self, context,sdd_context)

    def add_classes_to_package(self, context,sdd_context):
        '''
        Add the classes to the package
        '''
        for cube  in sdd_context.cubes.cubes:
            framework = cube.framework_id.framework_id
            if not(context.load_ldm):
                if (framework == context.reporting_framework + "_REF"):
                    class_name = cube.code
                else:
                    if (context.use_codes):
                        class_name = cube.code
                    else:
                        class_name = cube.name
            else:
                if (context.use_codes):
                    class_name = cube.code
                else:
                    class_name = cube.name
                
                    
            altered_class_name = Utils.make_valid_id(class_name)
            object_id = cube.cube_id
            cube_type = cube.cube_type
            derived = True

            if not(context.load_ldm):
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
                    the_long_name_annotation = ELAnnotation()
                    the_long_name_directive = Utils.get_annotation_directive(eclass.eContainer(), "long_name")
                    the_long_name_annotation.source = the_long_name_directive
                    details = the_long_name_annotation.details
                    mapentry  = ELStringToStringMapEntry()
                    mapentry.key = "long_name"
                    mapentry.value = Utils.make_valid_id(cube.displayName)
                    details.append(mapentry)
                    eclass.eAnnotations.append(the_long_name_annotation)
                    eclass.isDerived = derived
            else:
                if cube_type == "LDM":
                    altered_class_name = Utils.make_valid_id(class_name)
                    fullName = altered_class_name
                    derived = False
                    eclass = ELClass(name=fullName)
                    context.input_tables_package.eClassifiers.extend([
                                                                           eclass])
                    # maintain a map a objectIDs to ELClasses
                    context.classes_map[object_id] = eclass
                    # add an annotation to LDM cubes with the display name
                    the_long_name_annotation = ELAnnotation()
                    the_long_name_directive = Utils.get_annotation_directive(eclass.eContainer(), "long_name")
                    the_long_name_annotation.source = the_long_name_directive
                    details = the_long_name_annotation.details
                    mapentry  = ELStringToStringMapEntry()
                    mapentry.key = "long_name"
                    mapentry.value = Utils.make_valid_id(cube.displayName)
                    details.append(mapentry)
                    eclass.eAnnotations.append(the_long_name_annotation)
                    eclass.isDerived = derived
                    


    def add_enums_and_literals_to_package_for_input_layer(self, context,sdd_context):
        '''
        Add the Enums and Literals to the package from the input layer
        ''' 
        for cube_structure_item in sdd_context.cube_structure_items.cubeStructureItems:
            
            
            class_id = cube_structure_item.cube_structure_id.cube_structure_id
            try: 
                the_class = context.classes_map[class_id]
                if the_class in context.input_tables_package.eClassifiers:
                    TranslateSDDModelToDataModel.add_enums_and_literals_to_package_for_cube_structure_item(self,
                                                                    context,
                                                                    sdd_context,
                                                                    cube_structure_item,
                                                                    True)
            except:
                print("missing  class2: ")
                print(class_id)

    def add_enums_and_literals_to_package_for_output_layer(self, context,sdd_context):
        '''
        Add the Enums and Literals to the package fom the output layer
        '''
        for cube_structure_item in sdd_context.cube_structure_items.cubeStructureItems:
            
            
            class_id = cube_structure_item.cube_structure_id.cube_structure_id
            try: 
                the_class = context.classes_map[class_id]
                if the_class in context.output_tables_package.eClassifiers:
                    TranslateSDDModelToDataModel.add_enums_and_literals_to_package_for_cube_structure_item(self,
                                                                    context,
                                                                    sdd_context,
                                                                    cube_structure_item,
                                                                    False)
            except:
                print("missing  class2: ")
                print(class_id)

    def add_enums_and_literals_to_package_for_cube_structure_item(self,
                                                                    context,
                                                                    sdd_context,
                                                                    cube_structure_item,
                                                                    is_input_layer):
        '''
        Add the Enums and Literals to the package
        '''  
    
        variable = cube_structure_item.variable_id
        subdomain = cube_structure_item.subdomain_id
        attribute_list = [(variable,subdomain)]
        if (variable.code =="MTRCS"):
            variable_set = cube_structure_item.variable_set_id
            attribute_list = TranslateSDDModelToDataModel.get_attribute_and_subdomain_list_from_variable_set(self,variable_set)
        if (variable.code == "VALUE_DECIMAL") or (variable.code == "OBSERVATION_VALUE"):
            attribute_list = []
            
        for attribute_subdomain_tuple in attribute_list:
            attribute =attribute_subdomain_tuple[0]
            subdomain = attribute_subdomain_tuple[1]
            try: 
                domain = sdd_context.variable_to_domain_map[attribute.code]
                domain_id = domain.domain_id
                amended_domain_name = Utils.make_valid_id(domain_id)
                the_enum = Utils.find_enum(
                    amended_domain_name+"_domain", context.enum_map)
                if the_enum is None:
                    amended_domain_name = Utils.make_valid_id(domain_id) + "_domain"
                    if not ((amended_domain_name == "String") or (amended_domain_name == "Date")):
                        the_enum = ELEnum()
                        the_enum.name = amended_domain_name
                        # maintain a map of enum IDS to ELEnum objects
                        context.enum_map[amended_domain_name] = the_enum
                        context.sdd_domains_package.eClassifiers.extend([the_enum])
                        the_domain_members = []
                        # in the usual case we get the members from subdomains in the input layer
                        # since we proces the input layer first we should have all of the related 
                        # domains. 
                        if is_input_layer and context.use_sub_domains_in_input_layer:
                            the_domain_members = Utils.get_members_of_the_subdomain(
                                subdomain)
                        else:
                            # If a domain is in the output layer, but not in the input
                            # layer, then this is a problem. output layers are set from 
                            # the generation rules, so we should be 'collecting' exsting
                            # domains. Furthermore the filters from the report cell
                            # combination should filter on members that exist in the input 
                            # layer domains, and we should record an error if they do not
                            # so that this can be addressed.
                            the_domain_members = []
                            # Utils.get_members_of_the_domain(
                            #    domain, sdd_context.member_id_to_domain_map)
                        counter1 = 0
                        for member in the_domain_members:
                            enum_literal = ELEnumLiteral()
                            enum_used_name = Utils.make_valid_id_for_literal(member.code)
                            adapted_value = Utils.make_valid_id(member.displayName)
                            new_adapted_value = Utils.unique_value(the_enum, adapted_value)
                            new_adapted_name = Utils.unique_name(the_enum, enum_used_name)
            
                            enum_literal.name = new_adapted_value
                            enum_literal.literal = new_adapted_name
                            counter1 = counter1 + 1
                            enum_literal.value = counter1
                            the_enum.eLiterals.extend([enum_literal])
                            context.enum_literals_map[the_enum.name+":" + enum_literal.literal] = enum_literal
                else:
                    # if use_sub_domains_in_input_layer = True then we add any literals
                    # from the columns subdomain that were not added before.
                    # This covers the rare situation where 2 columns in the input
                    # layer have the same domain but different subdomains
                    if is_input_layer and context.use_sub_domains_in_input_layer:

                        the_domain_members = Utils.get_members_of_the_subdomain(
                            subdomain)
                    
                        counter1 = len(the_enum.eLiterals)
                        for member in the_domain_members:
                            enum_used_name = Utils.make_valid_id_for_literal(member.code)
                            adapted_value = Utils.make_valid_id(member.displayName)
                            if not (Utils.contains_literal(the_enum.eLiterals, adapted_value)):
                                enum_literal = ELEnumLiteral()
                                
                                enum_literal.name = adapted_value
                                enum_literal.literal = enum_used_name
                                counter1 = counter1 + 1
                                enum_literal.value = counter1
                                the_enum.eLiterals.extend([enum_literal])
                                context.enum_literals_map[the_enum.name+":" + enum_literal.literal] = enum_literal
        

            except:
                print("missing domain: " + domain.domain_id)

                           
    def add_attributes_to_classes(self, context,sdd_context):
        '''
        For each attribute add an ELAttribute to the correct ELClass representing the Entity
        the attribute should have the correct type, which may be a specific
        enumeration
        '''

        for cube_structure_item in sdd_context.cube_structure_items.cubeStructureItems:
            class_id = cube_structure_item.cube_structure_id.cube_structure_id
            role = cube_structure_item.role
            try:
                the_class = context.classes_map[class_id]
                the_attribute1 = cube_structure_item.variable_id
                long_name=None
                variable = cube_structure_item.variable_id
                attribute_list = [(the_attribute1,None)]
                if (the_attribute1.code=="MTRCS"):
                    variable_set = cube_structure_item.variable_set_id
                    attribute_list = TranslateSDDModelToDataModel.get_attribute_and_subdomain_list_from_variable_set(self,variable_set)
                if (the_attribute1.code== "VALUE_DECIMAL") or (the_attribute1.code == "OBSERVATION_VALUE"):
                    attribute_list = []
                for attribute_subdomain_tuple in attribute_list:
                    attribute =attribute_subdomain_tuple[0]
                    subdomain = attribute_subdomain_tuple[1]
                    
                    print("attribute.code: " + attribute.code )
                    try: 
                        try:
                            long_name = sdd_context.variable_to_long_names_map[attribute.code]
                        except:
                            long_name = attribute.code
                        amended_attribute_name = Utils.make_valid_id(attribute.code)
                        amended_attribute_long_name = Utils.make_valid_id(long_name)
                        
                        the_class = context.classes_map[class_id]
                        
                        class_is_derived = the_class.isDerived
                        
                        
                        
                        if(context.use_codes):   
                            the_attribute_name = amended_attribute_name  
                        else:
                            the_attribute_name = amended_attribute_long_name
                        
                        amended_domain_name = None
                        if (the_attribute1.code=="MTRCS"):
                            domain_id = sdd_context.variable_to_domain_map[attribute.code].domain_id
                            amended_domain_name = Utils.make_valid_id(domain_id+"_domain")
                        else:   
                            domain_id = sdd_context.variable_to_domain_map[variable.code].domain_id
                            amended_domain_name = Utils.make_valid_id(domain_id+"_domain")


                        the_enum =  Utils.find_enum(amended_domain_name,context.enum_map)
                        if  the_enum is not None:                     
                            
                            if class_is_derived:
                                operation = ELOperation()
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
                                    the_long_name_annotation = ELAnnotation()
                                    the_long_name_directive = Utils.get_annotation_directive(the_class.eContainer(), "long_name")
                                    the_long_name_annotation.source = the_long_name_directive
                                    details = the_long_name_annotation.details
                                    mapentry  = ELStringToStringMapEntry()
                                    mapentry.key = "long_name"
                                    mapentry.value = amended_attribute_long_name
                                    details.append(mapentry)
                                    operation.eAnnotations.append(the_long_name_annotation)
                                    
                    
                                    
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
                                        
                                    if role == 'D':
                                        the_attribute_annotation = Utils.get_annotation_with_source(attribute, "key")
                                        if the_attribute_annotation is None: 
                                            the_attribute_annotation = ELAnnotation()
                                            the_attribute_annotation_directive = Utils.get_annotation_directive(the_class.eContainer(), "key")
                                            the_attribute_annotation.source = the_attribute_annotation_directive
                                            attribute.eAnnotations.append(the_attribute_annotation)
                                            
                                        primary_key = None 
                            
                                        for key_value_pair in the_attribute_annotation.details.items:
                                            if key_value_pair.key == 'is_primary_key':
                                                primary_key = key_value_pair
                                                
                                        if primary_key is None:
                                            primary_key = ELStringToStringMapEntry()
                                            primary_key.key = "is_primary_key"
                                            primary_key.value = "true"
                                            the_attribute_annotation.details.append(primary_key)
                                    
                                    the_class = context.classes_map[class_id]
                                    the_long_name_annotation = ELAnnotation()
                                    the_long_name_directive = Utils.get_annotation_directive(the_class.eContainer(), "long_name")
                                    the_long_name_annotation.source = the_long_name_directive
                                    details = the_long_name_annotation.details
                                    mapentry  = ELStringToStringMapEntry()
                                    mapentry.key = "long_name"
                                    mapentry.value = amended_attribute_long_name
                                    details.append(mapentry)
                                    attribute.eAnnotations.append(the_long_name_annotation)
                                    
                                    the_class.eStructuralFeatures.extend([attribute])
                                    
                                except:
                                    print( "missing class2: " )

                        else:
                            print( "XXXXX missing domainID: " )
                            print(domain_id)
                            print(class_id)
                            
                    except:
                            pass
            except:
                pass
                
    def get_attribute_and_subdomain_list_from_variable_set(self,variable_set):
        attribute_subdomain_tuple_list = []
        variable_set_items = variable_set.variable_set_items
        for item in variable_set_items:
            variable = item.variable_id
            subdomain = item.subdomain_id
            attribute_subdomain_tuple_list.append((variable,subdomain))
             
        return attribute_subdomain_tuple_list

    