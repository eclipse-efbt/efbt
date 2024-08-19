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


import django
from django.apps import AppConfig


class ProcessSDD(AppConfig):
    
    path = '/workspaces/efbt/bird/birdseed_creator/birds_nest'
    def ready(self):
        
        from birdbox.process_steps.database_to_sdd_model.import_database_to_sdd_model import ImportDatabaseToSDDModel
        from birdbox.process_steps.website_to_sddmodel.import_website_to_sdd_model_django import ImportWebsiteToSDDModel
        from birdbox.context.sdd_context_django import SDDContext
        from birdbox.context.context import Context

        sdd_context = SDDContext()
        sdd_context.file_directory = '/workspaces/efbt/birdsnest/resources'
        sdd_context.output_directory = '/workspaces/efbt/birdsnest/results'
        context = Context()
        context.file_directory = '/workspaces/efbt/birdsnest/resources'
        context.output_directory = '/workspaces/efbt/birdsnest/results'
        if context.load_sdd_from_website:            
            ImportWebsiteToSDDModel().import_sdd(sdd_context)
        else:
            ImportDatabaseToSDDModel().import_sdd(sdd_context)

        # Do whatever processign we wish to do here
        pass
if __name__ == '__main__':
    django.setup()
    ProcessSDD('birdbox','birds_nest').ready()
    
    

    
    

 

    