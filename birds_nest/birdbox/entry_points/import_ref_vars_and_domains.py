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


class ProcessRefVariablesAndDomains(AppConfig):
    
    path = '/workspaces/efbt/bird/birdseed_creator/birds_nest'
    def ready(self):
        
        from birdbox.process_steps.database_to_sdd_model.create_reference_domains_and_variables import CreateRefDomainsAndVariables
        
        from birdbox.context.sdd_context_django import SDDContext
        from birdbox.context.context import Context

        sdd_context = SDDContext()
        sdd_context.file_directory = '/workspaces/efbt/bird/birdseed_creator/resources'
        sdd_context.output_directory = '/workspaces/efbt/bird/birdseed_creator/results'
        context = Context()
        context.file_directory = '/workspaces/efbt/bird/birdseed_creator/resources'
        context.output_directory = '/workspaces/efbt/bird/birdseed_creator/results'
        
        CreateRefDomainsAndVariables.create_ref_domains_and_variables(self,sdd_context)

        # Do whatever processign we wish to do here
        pass
if __name__ == '__main__':
    django.setup()
    ProcessRefVariablesAndDomains('birdbox','birds_nest').ready()
    
    

    
    

 

    