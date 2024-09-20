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

from agilebird.sdd_models import *
from django.apps import apps
from django.db.models.fields import CharField,DateTimeField,BooleanField,FloatField,BigIntegerField
import os
import csv
from typing import List, Any

from agilebird.process_steps.transformation_meta_data.ldm_search import ELDMSearch

class TransformationMetaDataDestroyer:
    """
    A class for creating generation rules for reports and tables.
    """

    def delete_transformation_meta_data(self, context: Any, sdd_context: Any, framework: str) -> None:
        """
        Generate generation rules for the given context and framework.

        Args:
            context (Any): The context object containing necessary data.
            sdd_context (Any): The SDD context object.
            framework (str): The framework being used (e.g., "FINREP_REF").
        """
        CUBE_LINK.objects.all().delete()
        CUBE_STRUCTURE_ITEM_LINK.objects.all().delete()
    
    