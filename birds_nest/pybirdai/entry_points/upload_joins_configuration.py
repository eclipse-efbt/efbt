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


class UploadJoinsConfiguration(AppConfig):
    """
    Django AppConfig for running the website to SDD model conversion process.

    This class sets up the necessary context and runs the import process
    to convert website data into an SDD  model.
    """

    path = os.path.join(settings.BASE_DIR, "birds_nest")

    @staticmethod
    def upload_joins_configuration(request):
        """
        Prepare and execute the website to SDD model conversion process.

        This method sets up the necessary contexts, creates reference domains
        and variables, and imports the website data into the SDD model.
        """
        from pybirdai.bird_meta_data_model import MAINTENANCE_AGENCY
        from pybirdai.process_steps.upload_files.file_uploader import FileUploader

        from pybirdai.context.context import Context

        base_dir = settings.BASE_DIR
        sdd_context = SDDContext()
        sdd_context.file_directory = os.path.join(base_dir, "resources")
        sdd_context.output_directory = os.path.join(base_dir, "results")

        context = Context()
        context.file_directory = sdd_context.file_directory
        context.output_directory = sdd_context.output_directory

        FileUploader().upload_joins_configuration(sdd_context, request)

    def ready(self):
        # This method is still needed for Django's AppConfig
        pass
