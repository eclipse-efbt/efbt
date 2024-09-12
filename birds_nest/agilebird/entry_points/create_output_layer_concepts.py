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

import os
import django
from django.apps import AppConfig

class RunCreateReports(AppConfig):
    """
    AppConfig class for creating reports in the agilebird application.

    This class sets up the necessary context and runs a series of import
    and creation processes for SDD (Structured Data Definition) models,
    output layers, and report filters.
    """

    path = '/workspaces/efbt/bird/birds_nest/birds_nest'

    def ready(self):
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
        from agilebird.sdd_models import MAINTENANCE_AGENCY
        from agilebird.process_steps.database_to_sdd_model.import_database_to_sdd_model import (
            ImportDatabaseToSDDModel
        )
        from agilebird.context.sdd_context_django import SDDContext
        from agilebird.context.context import Context
        from agilebird.process_steps.sqldeveloper_import.import_sqldev_ldm_to_regdna import (
            SQLDevLDMImport
        )
        from agilebird.process_steps.output_layers.create_output_layers import (
            CreateOutputLayers
        )
        from agilebird.process_steps.report_filters.create_report_filters import (
            CreateReportFilters
        )
        from agilebird.process_steps.database_to_sdd_model.import_database_to_sdd_model import (
            ImportDatabaseToSDDModel
        )

        base_dir = '/workspaces/efbt/bird/birds_nest/' 
        
        sdd_context = SDDContext()
        sdd_context.file_directory = os.path.join(base_dir, 'resources')
        sdd_context.output_directory = os.path.join(base_dir, 'results')
        
        context = Context()
        context.file_directory = sdd_context.file_directory
        context.output_directory = sdd_context.output_directory

        ImportDatabaseToSDDModel().import_sdd(sdd_context)

        SQLDevLDMImport.do_import(self, context)
        CreateOutputLayers().create_output_layers(
            context, sdd_context, "FINREP_REF", "3.0"
        )
        CreateReportFilters().create_report_filters(
            context, sdd_context, "FINREP_REF", "3.0"
        )

if __name__ == '__main__':
    django.setup()
    RunCreateReports('agilebird', 'birds_nest').ready()

