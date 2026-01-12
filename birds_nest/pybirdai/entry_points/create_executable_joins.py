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
"""Entry point for creating generation rules."""

import os
from pathlib import Path

import django
from django.apps import AppConfig
from django.conf import settings

class RunCreateExecutableJoins(AppConfig):
    """Django AppConfig for running the creation of generation rules."""

    path = os.path.join(settings.BASE_DIR, 'birds_nest')

    @staticmethod
    def create_python_joins():
        """Execute the process of creating generation rules when the app is ready."""
        from pybirdai.process_steps.input_model.import_database_to_sdd_model import (
            ImportDatabaseToSDDModel
        )
        from pybirdai.context.sdd_context_django import SDDContext
        from pybirdai.context.context import Context
        from pybirdai.process_steps.pybird.create_python_django_transformations import (
            CreatePythonTransformations
        )

        base_dir = settings.BASE_DIR 
        sdd_context = SDDContext()
        sdd_context.file_directory = os.path.join(base_dir, 'resources')
        sdd_context.output_directory = os.path.join(base_dir, 'results')
        
        context = Context()
        context.file_directory = sdd_context.file_directory
        context.output_directory = sdd_context.output_directory

        #ImportDatabaseToSDDModel().import_sdd(sdd_context)
        CreatePythonTransformations().create_python_joins(context, sdd_context)

    @staticmethod
    def create_python_joins_from_db(framework_id=None):
        """Execute the process of creating generation rules from the database when the app is ready.

        Args:
            framework_id: Optional framework ID to filter data. If provided, only
                         loads and generates joins for that specific framework.
                         If None, loads all frameworks (legacy behavior).
                         Supports framework names (FINREP, ANCRDT) or IDs (FINREP_REF, ANCRDT).
        """
        from pybirdai.process_steps.input_model.import_database_to_sdd_model import (
            ImportDatabaseToSDDModel
        )
        from pybirdai.context.sdd_context_django import SDDContext
        from pybirdai.context.context import Context
        from pybirdai.process_steps.pybird.create_python_django_transformations import (
            CreatePythonTransformations
        )

        # Normalize framework name to ID if needed
        FRAMEWORK_MAP = {
            'FINREP': 'FINREP_REF',
            'COREP': 'COREP_REF',
            'ANCRDT': 'ANCRDT',
            'AE': 'AE_REF',
            'FP': 'FP_REF',
        }
        if framework_id and framework_id in FRAMEWORK_MAP:
            framework_id = FRAMEWORK_MAP[framework_id]

        base_dir = settings.BASE_DIR
        sdd_context = SDDContext()
        sdd_context.file_directory = os.path.join(base_dir, 'resources')
        sdd_context.output_directory = os.path.join(base_dir, 'results')
        sdd_context.current_framework = framework_id  # Store framework for file naming

        context = Context()
        context.file_directory = sdd_context.file_directory
        context.output_directory = sdd_context.output_directory

        # Only import the necessary tables for joins
        importer = ImportDatabaseToSDDModel()

        tables_to_import = [
            'MAINTENANCE_AGENCY',
            'DOMAIN',
            'VARIABLE',
            'CUBE',
            'CUBE_STRUCTURE',
            'CUBE_STRUCTURE_ITEM',
            'CUBE_LINK',
            'CUBE_STRUCTURE_ITEM_LINK'
        ]

        if framework_id:
            # Use framework-filtered import for isolation
            importer.import_sdd_for_joins_by_framework(sdd_context, tables_to_import, framework_id)
        else:
            # Legacy behavior - load all data
            importer.import_sdd_for_joins(sdd_context, tables_to_import)

        CreatePythonTransformations().create_python_joins(context, sdd_context)

    @staticmethod
    def run_create_executable_joins(framework_id=None):
        """Main entry point for creating executable joins.

        Args:
            framework_id: Optional framework ID to filter data. If provided, only
                         generates joins for that specific framework.
                         Supports framework names (FINREP, ANCRDT) or IDs (FINREP_REF, ANCRDT).
        """
        # Normalize framework name to ID if needed
        # Supports both legacy (FINREP_REF) and DPM (EBA_FINREP) naming conventions
        FRAMEWORK_MAP = {
            # Legacy workflow framework IDs
            'FINREP': 'FINREP_REF',
            'COREP': 'COREP_REF',
            'ANCRDT': 'ANCRDT',
            'AE': 'AE_REF',
            'FP': 'FP_REF',
            # DPM workflow framework IDs (EBA_ prefix)
            'EBA_FINREP': 'EBA_FINREP',
            'EBA_COREP': 'EBA_COREP',
            'EBA_AE': 'EBA_AE',
            'EBA_FP': 'EBA_FP',
            'EBA_SBP': 'EBA_SBP',
            'EBA_REM': 'EBA_REM',
            'EBA_RES': 'EBA_RES',
            'EBA_PAY': 'EBA_PAY',
            'EBA_COVID19': 'EBA_COVID19',
            'EBA_IF': 'EBA_IF',
            'EBA_GSII': 'EBA_GSII',
            'EBA_MREL': 'EBA_MREL',
            'EBA_IMPRAC': 'EBA_IMPRAC',
            'EBA_ESG': 'EBA_ESG',
            'EBA_IPU': 'EBA_IPU',
            'EBA_PILLAR3': 'EBA_PILLAR3',
            'EBA_IRRBB': 'EBA_IRRBB',
            'EBA_DORA': 'EBA_DORA',
            'EBA_FC': 'EBA_FC',
            'EBA_MICA': 'EBA_MICA',
        }
        if framework_id and framework_id in FRAMEWORK_MAP:
            framework_id = FRAMEWORK_MAP[framework_id]

        RunCreateExecutableJoins.create_python_joins_from_db(framework_id)

    def ready(self):
        # This method is still needed for Django's AppConfig
        pass