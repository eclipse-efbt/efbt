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

"""Orchestrator function for importing semantic integrations from SDD."""

from .utilities import delete_mapping_warnings_files
from .import_variable_mappings import import_variable_mappings
from .import_variable_mapping_items import import_variable_mapping_items
from .import_member_mappings import import_member_mappings
from .import_member_mapping_items import import_member_mapping_items
from .import_mapping_definitions import import_mapping_definitions
from .import_mapping_to_cubes import import_mapping_to_cubes


def import_semantic_integrations_from_sdd(sdd_context):
    """
    Orchestrate the import of semantic integrations from SDD CSV files.

    This function coordinates the import of all mapping-related entities
    including variable mappings, member mappings, and their relationships.

    Args:
        sdd_context: SDDContext containing file paths and dictionaries
    """
    delete_mapping_warnings_files(sdd_context)
    import_variable_mappings(sdd_context)
    import_variable_mapping_items(sdd_context)
    import_member_mappings(sdd_context)
    import_member_mapping_items(sdd_context)
    import_mapping_definitions(sdd_context)
    import_mapping_to_cubes(sdd_context)
