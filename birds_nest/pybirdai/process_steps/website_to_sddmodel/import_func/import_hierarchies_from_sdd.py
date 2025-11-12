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

"""Orchestrator function for importing hierarchies from SDD."""

from .utilities import delete_hierarchy_warnings_files
from .import_member_hierarchies import import_member_hierarchies
from .import_parent_members_with_children import import_parent_members_with_children
from .import_member_hierarchy_nodes import import_member_hierarchy_nodes


def import_hierarchies_from_sdd(sdd_context):
    """
    Orchestrate the import of hierarchies from SDD CSV files.

    This function coordinates the import of member hierarchies,
    parent-child relationships, and hierarchy nodes.

    Args:
        sdd_context: SDDContext containing file paths and dictionaries
    """
    delete_hierarchy_warnings_files(sdd_context)
    import_member_hierarchies(sdd_context)
    import_parent_members_with_children(sdd_context)
    import_member_hierarchy_nodes(sdd_context)
