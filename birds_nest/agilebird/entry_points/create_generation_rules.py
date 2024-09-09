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
"""Entry point for creating generation rules."""

import os
from pathlib import Path

import django
from django.apps import AppConfig

class RunCreateGenerationRules(AppConfig):
    """Django AppConfig for running the creation of generation rules."""

    path = '/workspaces/efbt/bird/birds_nest/birds_nest'

    def ready(self):
        """Execute the process of creating generation rules when the app is ready."""
        from agilebird.process_steps.database_to_sdd_model.import_database_to_sdd_model import (
            ImportDatabaseToSDDModel
        )
        from agilebird.process_steps.website_to_sddmodel.import_website_to_sdd_model_django import (
            ImportWebsiteToSDDModel
        )
        from agilebird.context.sdd_context_django import SDDContext
        from agilebird.context.context import Context
        from agilebird.process_steps.sqldeveloper_import.import_sqldev_ldm_to_regdna import (
            SQLDevLDMImport
        )
        from agilebird.process_steps.generation_rules.create_generation_rules import (
            GenerationRuleCreator
        )
        from agilebird.process_steps.generation_rules.main_category_finder import (
            MainCategoryFinder
        )

        base_dir = '/workspaces/efbt/bird/birds_nest/'   
        sdd_context = SDDContext()
        sdd_context.file_directory = os.path.join(base_dir, 'resources')
        sdd_context.output_directory = os.path.join(base_dir, 'results')
        
        context = Context()
        context.file_directory = sdd_context.file_directory
        context.output_directory = sdd_context.output_directory

        if context.load_sdd_from_website:            
            ImportWebsiteToSDDModel().import_sdd(sdd_context)
        else:
            ImportDatabaseToSDDModel().import_sdd(sdd_context)

        SQLDevLDMImport.do_import(self, context)
        MainCategoryFinder().create_report_to_main_category_maps(
            context,
            sdd_context,
            "FINREP_REF",
            ["3", "3.0-Ind", "FINREP 3.0-Ind"]
        )
        GenerationRuleCreator().generate_generation_rules(
            context,
            sdd_context,
            "FINREP_REF"
        )

if __name__ == '__main__':
    django.setup()
    RunCreateGenerationRules('agilebird', 'birds_nest').ready()

