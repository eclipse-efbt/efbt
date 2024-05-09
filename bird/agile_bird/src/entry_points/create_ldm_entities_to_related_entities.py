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
from context.context import Context
from context.sdd_context import SDDContext
from utils.ldm_search import LDMSearch
from regdna import ELClass
from entry_points.website_to_sddmodel import RunWebsiteToSDDModel
from birdseed_creator.src.entry_points.sddmodel_to_datamodel_finrep_ldm import RunSDDModelToDataModelLDM
from persister.persist_to_file import PersistToFile

import os

class CreateLDMEntitiesToRelatedEntities:
    
    def run(self,context):
        extension = "csv"
        f = open(context.output_directory + os.sep + extension +
                 os.sep + "ldm_entity_related_entities" + '.' + extension,
                 "a",  encoding='utf-8')
        f.write("ldm_entity, related_entites\r")
        for elclass in context.input_tables_package.eClassifiers:
            if isinstance(elclass, ELClass):                 
                entities = LDMSearch.get_all_related_entities(self, context, elclass)
                related_entities_string = ""
                first = True
                for entity in entities:
                    if not(first):
                        related_entities_string = related_entities_string + ":"
                    first = False
                    related_entities_string = related_entities_string + entity.name

                f.write(elclass.name + "," + related_entities_string + "\r")
        f.close()
        
if __name__ == '__main__':
    context = Context()
    context.file_directory = 'C:\\Users\\neil\\freebirdtools-develop_may\\git\\efbt\\bird\\birdseed_creator\\resources'
    context.output_directory = 'C:\\Users\\neil\\freebirdtools-develop_may\\git\\efbt\\bird\\birdseed_creator\\results' 
    sdd_context = SDDContext()
    sdd_context.file_directory = 'C:\\Users\\neil\\freebirdtools-develop_may\\git\\efbt\\bird\\birdseed_creator\\resources'
    sdd_context.output_directory = 'C:\\Users\\neil\\freebirdtools-develop_may\\git\\efbt\\bird\\birdseed_creator\\results'
    RunWebsiteToSDDModel().run(sdd_context)
    RunSDDModelToDataModelLDM().run(context,sdd_context)
    persister = PersistToFile()
    persister.save_model_as_regdna_file(context)
    persister.save_model_as_xmi_file(context)
    CreateLDMEntitiesToRelatedEntities().run(context)

  
   
    
