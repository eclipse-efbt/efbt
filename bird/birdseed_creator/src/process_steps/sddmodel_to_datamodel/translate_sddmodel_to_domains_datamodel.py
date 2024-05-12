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

class TranslateSDDModelToDomainsDataModel(object):
    '''
    Documentation for TranslateSDDModelToDomainsDataModel
    '''

    def do_import(self, context,sdd_context,framework,cube_type):
        '''
        import the items from the Input and Output layer csv files
        '''

        TranslateSDDModelToDomainsDataModel.add_enums_and_literals_to_package(self, context,sdd_context,framework,cube_type)

    def add_enums_and_literals_to_package(self, context,sdd_context,framework,cube_type):
        '''
        Add the Enums and Literals to the package from the input layer
        ''' 

        for cube_structure_item in sdd_context.cube_structure_items.cubeStructureItems:
            try:   
                cube = sdd_context.cube_dictionary[cube_structure_item.cube_structure_id.cube_structure_id] 
                cube_framework = cube.framework_id.framework_id
                cube_cube_type = cube.cube_type

                if (cube_framework == framework) and (cube_cube_type == cube_type):                
                    TranslateSDDModelToDomainsDataModel.add_enums_and_literals_to_package_for_cube_structure_item(self,
                                                                        context,
                                                                        sdd_context,
                                                                        cube_structure_item,
                                                                        framework,
                                                                        cube_type)
            except KeyError:
                pass            


    def add_enums_and_literals_to_package_for_cube_structure_item(self,
                                                                    context,
                                                                    sdd_context,
                                                                    cube_structure_item,
                                                                    framework,
                                                                    cube_type):
        '''
        Add the Enums and Literals to the package
        '''
        package = None
        if framework == "FINREP_REF":
            package = context.finrep_domains_package
        elif framework == "AE":
            package = context.ae_domains_package
        elif framework == "BIRD":      
            if cube_type == "EIL":
                package = context.il_domains_package
            elif cube_type == "LDM":
                package = context.ldm_domains_package

        variable = cube_structure_item.variable_id
        subdomain = cube_structure_item.subdomain_id
        attribute_list = [(variable,subdomain)]
        if (variable.code =="MTRCS"):
            variable_set = cube_structure_item.variable_set_id
            attribute_list = TranslateSDDModelToDomainsDataModel.get_attribute_and_subdomain_list_from_variable_set(self,variable_set)
        if (variable.code == "VALUE_DECIMAL") or (variable.code == "OBSERVATION_VALUE"):
            attribute_list = []
            
        for attribute_subdomain_tuple in attribute_list:
            attribute =attribute_subdomain_tuple[0]
            subdomain = attribute_subdomain_tuple[1]
            try: 
                domain = sdd_context.variable_to_domain_map[attribute.code]
                domain_id = domain.domain_id
                amended_domain_name = Utils.make_valid_id(domain_id)+"_domain"
   
                the_enum = Utils.find_enum(
                    framework+":" + cube_type + ":" + amended_domain_name, context.enum_map)
                if the_enum is None:                    
                    if not ((amended_domain_name == "String_domain") or (amended_domain_name == "Date_domain")):
                        the_enum = ELEnum()
                        the_enum.name = amended_domain_name
                        # maintain a map of enum IDS to ELEnum objects
                        context.enum_map[ framework+":" + cube_type + ":" +amended_domain_name] = the_enum
                        package.eClassifiers.extend([the_enum])
                        the_domain_members = []
                        the_domain_members = Utils.get_members_of_the_subdomain(
                                subdomain)
                       
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
                            context.enum_literals_map[framework+":" + cube_type + ":" + the_enum.name+":" + enum_literal.literal] = enum_literal
                else:
                    # the enum already exists, but we might find that a cube_structure_item has 
                    # members taht have not yet been added, since to items might have the same domain
                    # but different subdomains
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
                            context.enum_literals_map[framework+":" + cube_type + ":" + the_enum.name+":" + enum_literal.literal] = enum_literal

            except:
                print("missing domain: " + domain.domain_id)

  
    def get_attribute_and_subdomain_list_from_variable_set(self,variable_set):
        attribute_subdomain_tuple_list = []
        variable_set_items = variable_set.variable_set_items
        for item in variable_set_items:
            variable = item.variable_id
            subdomain = item.subdomain_id
            attribute_subdomain_tuple_list.append((variable,subdomain))
             
        return attribute_subdomain_tuple_list

