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
from django.http import HttpResponse

class RunImportBasicInfoFromWebsite(AppConfig):
    """
    Django AppConfig for running the website to SDD model conversion process.

    This class sets up the necessary context and runs the import process
    to convert website data into an SDD  model.
    """

    from django.conf import settings

    @staticmethod
    def run_import():
        """
        Static method to run the import process.
        """
        from agilebird.sdd_models import MAINTENANCE_AGENCY
        from agilebird.process_steps.website_to_sddmodel.import_website_to_sdd_model_django import (
            ImportWebsiteToSDDModel
        )
        from agilebird.context.context import Context

        base_dir = 'C:\\Users\\neil\\freebirdtools-develop-July11\\git\\efbt\\birds_nest\\'
        sdd_context = SDDContext()
        sdd_context.file_directory = os.path.join(base_dir, 'resources')
        sdd_context.output_directory = os.path.join(base_dir, 'results')
        
        context = Context()
        context.file_directory = sdd_context.file_directory
        context.output_directory = sdd_context.output_directory

        # Import website data to SDD model
        ImportWebsiteToSDDModel().import_basic_info_from_website(sdd_context)

    def ready(self):
        """
        Django's ready method, now just a placeholder.
        """
        pass

if __name__ == '__main__':
    django.setup()
    RunImportBasicInfoFromWebsite('agilebird', 'birds_nest').ready()


      
    
