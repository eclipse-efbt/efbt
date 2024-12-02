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

class RunDeleteBirdMetadataDatabase(AppConfig):
    """Django AppConfig for running the creation of generation rules."""

    path = os.path.join(settings.BASE_DIR, 'birds_nest')

    @staticmethod
    def run_delete_bird_metadata_database():
        """Execute the process of creating generation rules when the app is ready."""

        from pybirdai.context.sdd_context_django import SDDContext
        from pybirdai.context.context import Context

        from pybirdai.process_steps.joins_meta_data.delete_joins_meta_data import (
            TransformationMetaDataDestroyer
        )


        base_dir = settings.BASE_DIR 
        sdd_context = SDDContext()
        sdd_context.file_directory = os.path.join(base_dir, 'resources')
        sdd_context.output_directory = os.path.join(base_dir, 'results')
        
        context = Context()
        context.file_directory = sdd_context.file_directory
        context.output_directory = sdd_context.output_directory


        TransformationMetaDataDestroyer().delete_bird_metadata_database(
            context,
            sdd_context,
            "FINREP_REF"
        )

def ready(self):
        # This method is still needed for Django's AppConfig
        pass