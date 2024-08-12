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
from whitebox.context.sdd_context_django import SDDContext

class RunWebsiteToSDDModel(AppConfig):

    path = '/workspaces/efbt/bird/birdseed_creator/birds_nest'
    def ready(self):
        from whitebox.sdd_models import MAINTENANCE_AGENCY
        from whitebox.process_steps.website_to_sddmodel.import_website_to_sdd_model_django import ImportWebsiteToSDDModel
        sdd_context = SDDContext()
        sdd_context.file_directory = '/workspaces/efbt/birdsnest/resources'
        sdd_context.output_directory = '/workspaces/efbt/birdsnest/results'
        ImportWebsiteToSDDModel().import_sdd(sdd_context)
        

if __name__ == '__main__':
    django.setup()
    RunWebsiteToSDDModel('whitebox','birds_nest').ready()


      
    
