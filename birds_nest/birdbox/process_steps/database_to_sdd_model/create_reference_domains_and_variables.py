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
from birdbox.utils.utils import Utils
from birdbox.sdd_models import *
from birdbox.regdna import ELClass,ELOperation,ELAnnotation,ELStringToStringMapEntry
import os
import csv

class CreateRefDomainsAndVariables(object):
    '''
    Documentation for CombinationsToReportFilters
    '''
    def create_ref_domains_and_variables(self, sdd_context):
        '''
       
        '''
        maintenance_agency = MAINTENANCE_AGENCY(name="REF")
        maintenance_agency.code = "REF"

        maintenance_agency.description = "REFERENCE"
        maintenance_agency.maintenance_agency_id = "REF"
        maintenance_agency.save()
        sdd_context.agency_dictionary["REF"] = maintenance_agency
        print("create_ref_domains_and_variables")
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
                        variable.save()
                        print("adding variable "  + variable_id)
                    
        

   