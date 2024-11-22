# coding=UTF-8#
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

from pybirdai.bird_meta_data_model import *
from django.apps import apps
from django.db.models.fields import (
    CharField,
    DateTimeField,
    BooleanField,
    FloatField,
    BigIntegerField,
)
import os
import csv
from typing import List, Any

from pybirdai.process_steps.joins_meta_data.ldm_search import ELDMSearch


class TransformationMetaDataDestroyer:
    """
    A class for creating generation rules for reports and tables.
    """

    def delete_joins_meta_data(
        self, context: Any, sdd_context: Any, framework: str
    ) -> None:
        """
        Generate generation rules for the given context and framework.

        Args:
            context (Any): The context object containing necessary data.
            sdd_context (Any): The SDD context object.
            framework (str): The framework being used (e.g., "FINREP_REF").
        """
        CUBE_LINK.objects.all().delete()
        CUBE_STRUCTURE_ITEM_LINK.objects.all().delete()

    def delete_bird_metadata_database(
        self, context: Any, sdd_context: Any, framework: str
    ) -> None:
        """
        Delete the Bird Metadata Database.
        """
        CUBE_LINK.objects.all().delete()
        CUBE_STRUCTURE_ITEM_LINK.objects.all().delete()
        CUBE_STRUCTURE_ITEM.objects.all().delete()
        CUBE_STRUCTURE.objects.all().delete()
        CUBE.objects.all().delete()
        DOMAIN.objects.all().delete()
        VARIABLE.objects.all().delete()
        MEMBER.objects.all().delete()
        MEMBER_MAPPING.objects.all().delete()
        MEMBER_MAPPING_ITEM.objects.all().delete()
        VARIABLE_MAPPING.objects.all().delete()
        VARIABLE_MAPPING_ITEM.objects.all().delete()
        TABLE_CELL.objects.all().delete()
        CELL_POSITION.objects.all().delete()
        AXIS_ORDINATE.objects.all().delete()
        ORDINATE_ITEM.objects.all().delete()
        MAPPING_DEFINITION.objects.all().delete()
        MAPPING_TO_CUBE.objects.all().delete()
        TABLE.objects.all().delete()
        CELL_POSITION.objects.all().delete()
        AXIS.objects.all().delete()
        SUBDOMAIN.objects.all().delete()
        SUBDOMAIN_ENUMERATION.objects.all().delete()
        FACET_COLLECTION.objects.all().delete()
        MAINTENANCE_AGENCY.objects.all().delete()
        FRAMEWORK.objects.all().delete()
        MEMBER_HIERARCHY.objects.all().delete()
        MEMBER_HIERARCHY_NODE.objects.all().delete()
        COMBINATION.objects.all().delete()
        COMBINATION_ITEM.objects.all().delete()
        CUBE_TO_COMBINATION.objects.all().delete()
