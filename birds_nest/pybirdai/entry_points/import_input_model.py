# coding=UTF-8
# Copyright (c) 2024 Bird Software Solutions Ltd
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
from pybirdai.context.sdd_context_django import SDDContext
from django.conf import settings

class RunImportInputModelFromSQLDev(AppConfig):
    """
    Django AppConfig for running the website to SDD model conversion process.

    This class sets up the necessary context and runs the import process
    to convert website data into an SDD  model.
    """

    path = os.path.join(settings.BASE_DIR, 'birds_nest')
    #path = os.path.join(settings.BASE_DIR, 'birds_nest')


    def ready(self):
        """
        Prepare and execute the website to SDD model conversion process.

        This method sets up the necessary contexts, creates reference domains
        and variables, and imports the website data into the SDD model.
        """
        from pybirdai.bird_meta_data_model import MAINTENANCE_AGENCY

        from pybirdai.process_steps.input_model.import_input_model import (
            ImportInputModel
        )
        from pybirdai.context.context_ancrdt import Context

        base_dir = settings.BASE_DIR
        sdd_context = SDDContext()
        sdd_context.file_directory = os.path.join(base_dir, 'resources')
        sdd_context.output_directory = os.path.join(base_dir, 'results')

        context = Context()
        context.file_directory = sdd_context.file_directory
        context.output_directory = sdd_context.output_directory

        # Create reference domains, variables, and cubes
        ImportInputModel.import_input_model(
             sdd_context, context
        )



if __name__ == '__main__':
    django.setup()
    RunImportInputModelFromSQLDev('pybirdai', 'birds_nest').ready()
