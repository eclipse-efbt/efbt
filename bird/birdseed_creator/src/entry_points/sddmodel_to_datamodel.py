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
Created on 25 April 2022
'''

from entry_points.website_to_sddmodel import RunWebsiteToSDDModel 
from context.context import Context
from context.sdd_context import SDDContext
from process_steps.sddmodel_to_datamodel.translate_sddmodel_to_domains_datamodel import TranslateSDDModelToDomainsDataModel
from process_steps.sddmodel_to_datamodel.translate_sddmodel_to_entity_datamodel import TranslateSDDModelToDataModel
from persister.persist_to_file import PersistToFile
from utils.relationship_enricher import RelationshipEnricher
from utils. ldm_relationship_enricher import LDMRelationshipEnricher


class RunSDDModelToDataModel:
    
    def run(self,context,sdd_context):
        TranslateSDDModelToDomainsDataModel().do_import(context,sdd_context,"AE_REF","RC")
        TranslateSDDModelToDomainsDataModel().do_import(context,sdd_context,"FINREP_REF","RC")
        TranslateSDDModelToDomainsDataModel().do_import(context,sdd_context,"BIRD","EIL")
        TranslateSDDModelToDomainsDataModel().do_import(context,sdd_context,"BIRD","LDM")
        TranslateSDDModelToDataModel().do_import(context,sdd_context,"AE_REF","RC")
        TranslateSDDModelToDataModel().do_import(context,sdd_context,"FINREP_REF","RC")
        TranslateSDDModelToDataModel().do_import(context,sdd_context,"BIRD","EIL")
        TranslateSDDModelToDataModel().do_import(context,sdd_context,"BIRD","LDM")
        RelationshipEnricher().enrich(context)
        LDMRelationshipEnricher().enrich(context)
        

if __name__ == '__main__':
    sdd_context = SDDContext()
    context = Context()
    context.file_directory = '/workspaces/efbt/bird/birdseed_creator/resources'
    context.output_directory = '/workspaces/efbt/bird/birdseed_creator/results'  
    sdd_context.file_directory = '/workspaces/efbt/bird/birdseed_creator/resources'
    sdd_context.output_directory = '/workspaces/efbt/bird/birdseed_creator/results'    
    RunWebsiteToSDDModel().run(sdd_context)
    RunSDDModelToDataModel().run(context,sdd_context)
    persister = PersistToFile()
    persister.save_model_as_regdna_file(context)
    persister.save_model_as_xmi_file(context)
    

    
    

 

    