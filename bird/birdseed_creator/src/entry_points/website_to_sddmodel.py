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
# This script creates an analysis model from an SDD file and saves it as a CSV filegit add 

from ldm_context.context.sdd_context import SDDContext
from process_steps.website_to_sddmodel.import_website_to_sdd_model import ImportWebsiteToSDDModel
from persister.persist_sdd import PersistSDD

class RunWebsiteToSDDModel:
    
    def run(self,sdd_context):
        ImportWebsiteToSDDModel().import_sdd(sdd_context)

if __name__ == '__main__':
    sdd_context = SDDContext()
    sdd_context.file_directory = 'C:\\Users\\LENOVO\\git\\efbt_the_directories2\\bird\\birdseed_creator\\resources'
    sdd_context.output_directory = 'C:\\Users\\LENOVO\\git\\efbt_the_directories2\\bird\\birdseed_creator\\results'      
    RunWebsiteToSDDModel().run(sdd_context)
    persister =PersistSDD()
    persister.save_analysis_model_as_xmi_files(sdd_context)
    persister.save_analysis_model_as_csv(sdd_context)
    
