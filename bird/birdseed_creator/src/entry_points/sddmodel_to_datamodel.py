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

from context.context import Context
from context.sdd_context import SDDContext
from process_steps.website_to_sddmodel.import_sdd_to_analysis_model import ImportWebsiteToSDDModel
from persister.persist_sdd import PersistSDD
from processes.sddmodel_to_datamodel.translate_sddmodel_to_datamodel import TranslateSDDModelToDataModel
from persister.persist_to_file import PersistToFile
from utils.relationship_enricher import RelationshipEnricher



if __name__ == '__main__':
    sdd_context = SDDContext()
    sdd_context.file_directory = 'C:\\Users\\LENOVO\\git\\efbt_the_directories2\\bird\\birdseed_creator\\resources'
    sdd_context.output_directory = 'C:\\Users\\LENOVO\\git\\efbt_the_directories2\\bird\\birdseed_creator\\results'
    ImportWebsiteToSDDModel().import_sdd(sdd_context)
    persister =PersistSDD()
    persister.save_analysis_model_as_xmi_files(sdd_context)
    persister.save_analysis_model_as_csv(sdd_context)
    
    context = Context()
    context.persist_to_regdna = True
    context.use_subdomains = False
    context.load_eil_from_website  = True
    context.file_directory = 'C:\\Users\\LENOVO\\git\\efbt_the_directories2\\bird\\birdseed_creator\\resources'
    context.output_directory = 'C:\\Users\\LENOVO\\git\\efbt_the_directories2\\bird\\birdseed_creator\\results'
    TranslateSDDModelToDataModel().do_import(context,sdd_context)
    RelationshipEnricher().enrich(context)
    persister = PersistToFile()
    persister.save_model_as_regdna_file(context)
    persister.save_model_as_xmi_file(context)

    