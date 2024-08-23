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

from birdbox.sdd_models import *


class CreateRefDomainsAndVariables(object):
    '''
    Documentation for CombinationsToReportFilters
    '''
    def create_ref_domains_and_variables_and_cubes(self, sdd_context,context):
        '''
       
        '''
        maintenance_agency = MAINTENANCE_AGENCY(name="REF")
        maintenance_agency.code = "REF"
        maintenance_agency.description = "REFERENCE"
        maintenance_agency.maintenance_agency_id = "REF"
        maintenance_agency.save()
        sdd_context.agency_dictionary["REF"] = maintenance_agency
        print("create_ref_domains_and_variables_and_cubes")
        from django.apps import apps
        model_list = apps.get_models()
        sdd_context.ref_domain_dictionary['String'] = DOMAIN()
        sdd_context.ref_domain_dictionary['String'].domain_id = 'String'
        sdd_context.ref_domain_dictionary['String'].name = 'String'
        sdd_context.ref_domain_dictionary['String'].description = 'String'
        sdd_context.ref_domain_dictionary['String'].type = 'String'
        sdd_context.ref_domain_dictionary['String'].save()

        for model in model_list:
            print(f"{model._meta.app_label}  -> {model.__name__}")
            if model._meta.app_label == 'birdbox':
                
                bird_cube = CUBE()
                bird_cube_cube_structure = CUBE_STRUCTURE()
                bird_cube.cube_id = model.__name__
                bird_cube.name = model.__name__
                bird_cube_cube_structure.cube_structure_id = model.__name__
                bird_cube_cube_structure.name = model.__name__
                bird_cube.cube_structure_id = bird_cube_cube_structure

                sdd_context.bird_cube_structure_dictionary[bird_cube_cube_structure.name] = bird_cube_cube_structure
                sdd_context.bird_cube_dictionary[bird_cube.name] = bird_cube
                if context.save_derived_sdd_items:
                    bird_cube_cube_structure.save()
                    bird_cube.save()
                field_list = model._meta.get_fields()
                for field in field_list:
                    variable_id = field.name
                    domain = None
                    domain_id = None
                    try:
                        domain_id = field.db_comment
                        if not(domain_id is None):
                            if not(domain_id in sdd_context.ref_domain_dictionary.keys()):
                                domain = DOMAIN()
                                domain.domain_id = domain_id
                                domain.name = domain_id
                                sdd_context.ref_domain_dictionary[domain_id] = domain
                                if sdd_context.save_sdd_to_db:
                                    domain.save()
                                print("adding domain : " +domain_id)
                                choices = field.choices
                                if not(choices is None):
                                    for choice in choices:
                                        member = MEMBER()
                                        member.member_id = domain_id + "_" + choice[0]
                                        member.displayName = choice[1]
                                        member.domain_id = domain
                                        sdd_context.ref_member_dictionary[member.member_id] = member
                                        if sdd_context.save_sdd_to_db:
                                            member.save()
                                        print("adding member " + member.member_id + ":" + member.displayName)
                    except:
                        pass

                    if not (variable_id in sdd_context.ref_variable_dictionary):
                        variable = VARIABLE()
                        variable.maintenance_agency_id = maintenance_agency
                        variable.variable_id = variable_id
                        try:
                            variable.displayName = field.verbose_name
                        except:
                            pass
                        if not(domain is None):
                            variable.domain_id = domain
                        else:
                            variable.domain_id = sdd_context.ref_domain_dictionary['String']
                            #todo find the domain type from django type, makes sure those
                            # primitive types exist as domains in the sdd
                        sdd_context.ref_variable_dictionary[variable_id] = variable
                        if sdd_context.save_sdd_to_db:
                            variable.save()
                        print("adding variable "  + variable_id)

                    csi = CUBE_STRUCTURE_ITEM()
                    csi.cube_structure_id = bird_cube_cube_structure
                    csi.variable_id = sdd_context.ref_variable_dictionary[variable_id]
                    sdd_context.bird_cube_structure_item_dictionary[csi.cube_structure_id.cube_structure_id + ":" + csi.variable_id.variable_id] = csi
                    if context.save_derived_sdd_items:
                        csi.save()
                    
        

   