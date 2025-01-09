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
from django.apps import AppConfig
from pybirdai.context.sdd_context_django import SDDContext
from django.conf import settings

class RunConvertLDMToSDDHierarchies(AppConfig):
    """
    Django AppConfig for converting LDM hierarchies to SDD hierarchies.
    """

    path = os.path.join(settings.BASE_DIR, 'birds_nest')

    @staticmethod
    def run_convert_hierarchies():
        """
        Execute the process of converting LDM hierarchies to SDD hierarchies.
        """
        from pybirdai.process_steps.hierarchy_conversion.convert_ldm_to_sdd_hierarchies import (
            ConvertLDMToSDDHierarchies
        )
        from pybirdai.context.context import Context

        base_dir = settings.BASE_DIR
        sdd_context = SDDContext()
        sdd_context.file_directory = os.path.join(base_dir, 'resources')
        sdd_context.output_directory = os.path.join(base_dir, 'results')
        
        context = Context()
        context.file_directory = sdd_context.file_directory
        context.output_directory = sdd_context.output_directory

        ConvertLDMToSDDHierarchies().convert_hierarchies(context, sdd_context)

    def ready(self):
        # This method is still needed for Django's AppConfig
        pass 