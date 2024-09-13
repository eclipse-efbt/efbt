# coding=UTF-8
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
# This script creates an analysis model from an SDD file and saves it as a CSV file

import django
import os
from django.apps import AppConfig
from agilebird.context.sdd_context_django import SDDContext

class RunImportRefInfoFromWebsite(AppConfig):
    """
    Django AppConfig for running the website to SDD model conversion process.

    This class sets up the necessary context and runs the import process
    to convert website data into an SDD  model.
    """

    path = '/workspaces/efbt/birds_nest/birds_nest'

    def ready(self):
        """
        Prepare and execute the website to SDD model conversion process.

        This method sets up the necessary contexts, creates reference domains
        and variables, and imports the website data into the SDD model.
        """
        from agilebird.sdd_models import MAINTENANCE_AGENCY
        from agilebird.process_steps.website_to_sddmodel.import_website_to_sdd_model_django import (
            ImportWebsiteToSDDModel
        )

        from agilebird.context.context import Context

        base_dir = '/workspaces/efbt/birds_nest/' 
        sdd_context = SDDContext()
        sdd_context.file_directory = os.path.join(base_dir, 'resources')
        sdd_context.output_directory = os.path.join(base_dir, 'results')
        
        context = Context()
        context.file_directory = sdd_context.file_directory
        context.output_directory = sdd_context.output_directory


        if not sdd_context.exclude_reference_info_from_website:
            # Import website data to SDD model
            ImportWebsiteToSDDModel().import_reference_info_from_sdd(sdd_context)

if __name__ == '__main__':
    django.setup()
    RunImportRefInfoFromWebsite('agilebird', 'birds_nest').ready()


      
    
