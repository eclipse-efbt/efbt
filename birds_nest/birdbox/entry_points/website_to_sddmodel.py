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

import django
from django.apps import AppConfig
from birdbox.context.sdd_context_django import SDDContext

class RunWebsiteToSDDModel(AppConfig):

    path = 'C:\\Users\\neil\\freebirdtools-develop-July11\\git\\efbt\\birds_nest\\birds_nest'
    def ready(self):
        from birdbox.sdd_models import MAINTENANCE_AGENCY
        from birdbox.process_steps.website_to_sddmodel.import_website_to_sdd_model_django import ImportWebsiteToSDDModel
        from birdbox.process_steps.database_to_sdd_model.create_reference_domains_and_variables import CreateRefDomainsAndVariables
        from birdbox.context.context import Context
        sdd_context = SDDContext()
        sdd_context.file_directory = '/workspaces/efbt/bird/birdseed_creator/resources'
        sdd_context.output_directory = '/workspaces/efbt/bird/birdseed_creator/results'
        context = Context()
        context.file_directory = '/workspaces/efbt/birdsnest/resources'
        context.output_directory = '/workspaces/efbt/birdsnest/results'
        CreateRefDomainsAndVariables.create_ref_domains_and_variables_and_cubes(self,sdd_context,context)
        ImportWebsiteToSDDModel().import_sdd(sdd_context)
        

if __name__ == '__main__':
    django.setup()
    RunWebsiteToSDDModel('birdbox','birds_nest').ready()


      
    
