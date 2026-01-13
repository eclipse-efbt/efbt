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
import logging
from django.apps import AppConfig
from pybirdai.context.sdd_context_django import SDDContext
from django.conf import settings

logger = logging.getLogger(__name__)

class RunImportReportTemplatesFromWebsite(AppConfig):
    """
    Django AppConfig for running the website to SDD model conversion process.

    This class sets up the necessary context and runs the import process
    to convert website data into an SDD  model.
    """

    path = os.path.join(settings.BASE_DIR, 'birds_nest')

    @staticmethod
    def run_import(frameworks=None, pipeline=None):
        """
        Import report templates from website and fetch HTML template files.

        Args:
            frameworks (list, optional): List of frameworks to import templates for.
                                        Defaults to ['FINREP'] for backward compatibility.
                                        Examples: ['FINREP'], ['COREP', 'AE'], ['FINREP', 'COREP']
            pipeline (str, optional): Pipeline name to use for GitHub URL lookup.
                                     Defaults to 'main' for FINREP, 'dpm' for COREP/DPM frameworks.
        """
        from pybirdai.process_steps.website_to_sddmodel.import_website_to_sdd_model_django import (
            ImportWebsiteToSDDModel
        )
        from pybirdai.context.context import Context
        from django.conf import settings

        # Default to FINREP for backward compatibility
        if frameworks is None:
            frameworks = ['FINREP']

        # Determine pipeline based on frameworks if not specified
        if pipeline is None:
            # DPM frameworks use 'dpm' pipeline
            dpm_frameworks = {'COREP', 'AE', 'FP', 'SBP', 'REM', 'RES', 'PAY', 'GSII', 'MREL'}
            if any(fw in dpm_frameworks for fw in frameworks):
                pipeline = 'dpm'
            else:
                pipeline = 'main'

        logger.info(f"Importing report templates for frameworks: {frameworks} (pipeline: {pipeline})")

        base_dir = settings.BASE_DIR
        sdd_context = SDDContext()
        sdd_context.file_directory = os.path.join(base_dir, 'resources')
        sdd_context.output_directory = os.path.join(base_dir, 'results')

        # Set frameworks for import - BIRD + framework_REF for isolation
        framework_refs = ['BIRD'] + [f"{fw}_REF" if not fw.endswith('_REF') else fw for fw in frameworks]
        sdd_context.current_frameworks = framework_refs
        logger.info(f"Framework isolation enabled: {sdd_context.current_frameworks}")

        context = Context()
        context.file_directory = sdd_context.file_directory
        context.output_directory = sdd_context.output_directory
        context.current_frameworks = framework_refs

        if not sdd_context.exclude_reference_info_from_website:
            # Import report templates to database
            ImportWebsiteToSDDModel().import_report_templates_from_sdd(sdd_context)

            # Download report template HTML files from GitHub for each framework
            from pybirdai.utils.github_file_fetcher import GitHubFileFetcher
            from pybirdai.services.pipeline_repo_service import get_configured_pipeline_url

            github_url = get_configured_pipeline_url(pipeline)
            if github_url:
                # Get token from environment for authenticated API requests
                github_token = os.environ.get('GITHUB_TOKEN')
                fetcher = GitHubFileFetcher(github_url, token=github_token)
                for fw in frameworks:
                    logger.info(f"Downloading {fw} report template HTML files from GitHub")
                    fetcher.fetch_report_template_htmls(framework=fw)
            else:
                logger.warning(f"No {pipeline} pipeline URL configured. Skipping report template download.")

    def ready(self):
        # This method is still needed for Django's AppConfig
        pass
