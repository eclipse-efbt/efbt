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

import os
import django
from django.apps import AppConfig
from django.conf import settings

class RunCreateFilters(AppConfig):
    """
    AppConfig class for creating reports in the pybirdai application.

    This class sets up the necessary context and runs a series of import
    and creation processes for SDD (Structured Data Definition) models,
    output layers, and report filters.
    """

    path = os.path.join(settings.BASE_DIR, 'birds_nest')

    @staticmethod
    def run_create_filters():
        """
        Executes the report creation process when the application is ready.

        This method performs the following steps:
        1. Sets up the SDD context and general context
        2. Imports SDD model from either a website or a database
        3. Imports SQL Developer LDM (Logical Data Model)
        4. Creates output layers
        5. Creates report filters

        The specific processes and their parameters are defined within the method.
        """
        from pybirdai.models.bird_meta_data_model import MAINTENANCE_AGENCY
        from pybirdai.process_steps.input_model.import_database_to_sdd_model import (
            ImportDatabaseToSDDModel
        )
        from pybirdai.context.sdd_context_django import SDDContext
        from pybirdai.context.context import Context

        from pybirdai.process_steps.report_filters.create_output_layers import (
            CreateOutputLayers
        )
        from pybirdai.process_steps.report_filters.create_report_filters import (
            CreateReportFilters
        )
        from pybirdai.process_steps.input_model.import_database_to_sdd_model import (
            ImportDatabaseToSDDModel
        )

        base_dir = settings.BASE_DIR

        sdd_context = SDDContext()
        sdd_context.file_directory = os.path.join(base_dir, 'resources')
        sdd_context.output_directory = os.path.join(base_dir, 'results')

        context = Context()
        context.file_directory = sdd_context.file_directory
        context.output_directory = sdd_context.output_directory

        #ImportDatabaseToSDDModel().import_sdd(sdd_context)


        CreateOutputLayers().create_filters(
            context, sdd_context, "FINREP_REF", "3.0"
        )
        CreateReportFilters().create_report_filters(
            context, sdd_context, "FINREP_REF", "3.0"
        )

def ready(self):
        # This method is still needed for Django's AppConfig
        pass
