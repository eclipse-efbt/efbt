# coding=UTF-8
# Copyright (c) 2025 Bird Software Solutions Ltd
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDX-License-Identifier: EPL-2.0
#
# Contributors:
#    Neil Mackenzie - initial API and implementation

"""
Framework Cleanup Service

This service handles the deletion of all framework-related data using the
FrameworkSubgraphFetcher to identify what belongs to a framework.

Key design decisions:
- Uses FrameworkSubgraphFetcher to identify framework-related records
- Deletes ALL framework-related data directly (not orphan-based)
- PRESERVES: DOMAIN, VARIABLE, MEMBER (ontology layer)
"""

import logging
from typing import Any, Dict

from django.db import connection

from pybirdai.views.core.framework_filters import FrameworkSubgraphFetcher
from pybirdai.context.sdd_context_django import SDDContext

logger = logging.getLogger(__name__)


class FrameworkCleanupService:
    """
    Service for deleting framework-specific data.

    Uses FrameworkSubgraphFetcher to identify all data related to a framework
    and deletes it. Preserves the ontology layer (DOMAIN, VARIABLE, MEMBER).
    """

    # Models to preserve (ontology layer)
    PRESERVED_MODELS = {'DOMAIN', 'VARIABLE', 'MEMBER'}

    def delete_framework(self, framework_id: str, sdd_context: Any = None) -> Dict[str, int]:
        """
        Delete all data related to a specific framework.

        Args:
            framework_id: The framework ID (e.g., 'FINREP_REF', 'COREP_REF')
            sdd_context: Optional SDDContext to clear cached dictionaries

        Returns:
            Dictionary with counts of deleted records per model
        """
        logger.info(f"Starting framework cleanup for: {framework_id}")

        deletion_summary = {}

        with connection.cursor() as cursor:
            # Disable FK constraints during deletion
            cursor.execute("PRAGMA foreign_keys = 0;")

            try:
                # Delete in reverse dependency order (children first)
                # Phase 1: Mapping items and links (leaf level)
                deletion_summary.update(self._delete_mapping_items(framework_id))

                # Phase 2: Mappings
                deletion_summary.update(self._delete_mappings(framework_id))

                # Phase 3: Rendering items (ordinates, cells)
                deletion_summary.update(self._delete_rendering_items(framework_id))

                # Phase 4: Rendering (tables, axes)
                deletion_summary.update(self._delete_rendering(framework_id))

                # Phase 5: Cube links and structure items
                deletion_summary.update(self._delete_cube_relationships(framework_id))

                # Phase 6: Combinations
                deletion_summary.update(self._delete_combinations(framework_id))

                # Phase 7: Cubes and structures
                deletion_summary.update(self._delete_cubes(framework_id))

                # Phase 8: Hierarchies
                deletion_summary.update(self._delete_hierarchies(framework_id))

                # Phase 9: Subdomains (but NOT domains)
                deletion_summary.update(self._delete_subdomains(framework_id))

                # Phase 10: Junction tables
                deletion_summary.update(self._delete_junction_tables(framework_id))

                # Phase 11: Facets
                deletion_summary.update(self._delete_facets(framework_id))

            finally:
                cursor.execute("PRAGMA foreign_keys = 1;")

        # Clear SDDContext caches
        if sdd_context:
            self._clear_sdd_context(sdd_context, framework_id)

        logger.info(f"Framework cleanup complete for: {framework_id}")
        logger.info(f"Deletion summary: {deletion_summary}")

        return deletion_summary

    def _delete_queryset(self, queryset, model_name: str) -> int:
        """Delete all records in a queryset and return count."""
        if queryset is None:
            return 0
        count = queryset.count()
        if count > 0:
            queryset.delete()
            logger.info(f"Deleted {count} {model_name} records")
        return count

    def _delete_mapping_items(self, framework_id: str) -> Dict[str, int]:
        """Delete mapping item records (leaf level)."""
        summary = {}

        # MEMBER_MAPPING_ITEM
        qs = FrameworkSubgraphFetcher.get_member_mapping_items_for_framework(framework_id)
        summary['MEMBER_MAPPING_ITEM'] = self._delete_queryset(qs, 'MEMBER_MAPPING_ITEM')

        # VARIABLE_MAPPING_ITEM
        qs = FrameworkSubgraphFetcher.get_variable_mapping_items_for_framework(framework_id)
        summary['VARIABLE_MAPPING_ITEM'] = self._delete_queryset(qs, 'VARIABLE_MAPPING_ITEM')

        # MAPPING_ORDINATE_LINK
        qs = FrameworkSubgraphFetcher.get_mapping_ordinate_links_for_framework(framework_id)
        summary['MAPPING_ORDINATE_LINK'] = self._delete_queryset(qs, 'MAPPING_ORDINATE_LINK')

        return summary

    def _delete_mappings(self, framework_id: str) -> Dict[str, int]:
        """Delete mapping records."""
        summary = {}

        # MAPPING_TO_CUBE
        qs = FrameworkSubgraphFetcher.get_mapping_to_cubes_for_framework(framework_id)
        summary['MAPPING_TO_CUBE'] = self._delete_queryset(qs, 'MAPPING_TO_CUBE')

        # MEMBER_MAPPING
        qs = FrameworkSubgraphFetcher.get_member_mappings_for_framework(framework_id)
        summary['MEMBER_MAPPING'] = self._delete_queryset(qs, 'MEMBER_MAPPING')

        # VARIABLE_MAPPING
        qs = FrameworkSubgraphFetcher.get_variable_mappings_for_framework(framework_id)
        summary['VARIABLE_MAPPING'] = self._delete_queryset(qs, 'VARIABLE_MAPPING')

        # MAPPING_DEFINITION
        qs = FrameworkSubgraphFetcher.get_mapping_definitions_for_framework(framework_id)
        summary['MAPPING_DEFINITION'] = self._delete_queryset(qs, 'MAPPING_DEFINITION')

        return summary

    def _delete_rendering_items(self, framework_id: str) -> Dict[str, int]:
        """Delete rendering item records (leaf level)."""
        summary = {}

        # CELL_POSITION
        qs = FrameworkSubgraphFetcher.get_cell_positions_for_framework(framework_id)
        summary['CELL_POSITION'] = self._delete_queryset(qs, 'CELL_POSITION')

        # ORDINATE_ITEM
        qs = FrameworkSubgraphFetcher.get_ordinate_items_for_framework(framework_id)
        summary['ORDINATE_ITEM'] = self._delete_queryset(qs, 'ORDINATE_ITEM')

        # TABLE_CELL
        qs = FrameworkSubgraphFetcher.get_table_cells_for_framework(framework_id)
        summary['TABLE_CELL'] = self._delete_queryset(qs, 'TABLE_CELL')

        # AXIS_ORDINATE
        qs = FrameworkSubgraphFetcher.get_axis_ordinates_for_framework(framework_id)
        summary['AXIS_ORDINATE'] = self._delete_queryset(qs, 'AXIS_ORDINATE')

        return summary

    def _delete_rendering(self, framework_id: str) -> Dict[str, int]:
        """Delete rendering records (tables, axes)."""
        summary = {}

        # AXIS
        qs = FrameworkSubgraphFetcher.get_axes_for_framework(framework_id)
        summary['AXIS'] = self._delete_queryset(qs, 'AXIS')

        # TABLE
        qs = FrameworkSubgraphFetcher.get_tables_for_framework(framework_id)
        summary['TABLE'] = self._delete_queryset(qs, 'TABLE')

        return summary

    def _delete_cube_relationships(self, framework_id: str) -> Dict[str, int]:
        """Delete cube links and structure items."""
        summary = {}

        # MEMBER_LINK
        qs = FrameworkSubgraphFetcher.get_member_links_for_framework(framework_id)
        summary['MEMBER_LINK'] = self._delete_queryset(qs, 'MEMBER_LINK')

        # CUBE_STRUCTURE_ITEM_LINK
        qs = FrameworkSubgraphFetcher.get_cube_structure_item_links_for_framework(framework_id)
        summary['CUBE_STRUCTURE_ITEM_LINK'] = self._delete_queryset(qs, 'CUBE_STRUCTURE_ITEM_LINK')

        # CUBE_LINK
        qs = FrameworkSubgraphFetcher.get_cube_links_for_framework(framework_id)
        summary['CUBE_LINK'] = self._delete_queryset(qs, 'CUBE_LINK')

        # CUBE_STRUCTURE_ITEM
        qs = FrameworkSubgraphFetcher.get_cube_structure_items_for_framework(framework_id)
        summary['CUBE_STRUCTURE_ITEM'] = self._delete_queryset(qs, 'CUBE_STRUCTURE_ITEM')

        return summary

    def _delete_combinations(self, framework_id: str) -> Dict[str, int]:
        """Delete combination records."""
        summary = {}

        # COMBINATION_ITEM
        qs = FrameworkSubgraphFetcher.get_combination_items_for_framework(framework_id)
        summary['COMBINATION_ITEM'] = self._delete_queryset(qs, 'COMBINATION_ITEM')

        # CUBE_TO_COMBINATION
        qs = FrameworkSubgraphFetcher.get_cube_to_combinations_for_framework(framework_id)
        summary['CUBE_TO_COMBINATION'] = self._delete_queryset(qs, 'CUBE_TO_COMBINATION')

        # COMBINATION
        qs = FrameworkSubgraphFetcher.get_combinations_for_framework(framework_id)
        summary['COMBINATION'] = self._delete_queryset(qs, 'COMBINATION')

        return summary

    def _delete_cubes(self, framework_id: str) -> Dict[str, int]:
        """Delete cube and cube structure records."""
        summary = {}

        # CUBE (direct framework_id filter)
        qs = FrameworkSubgraphFetcher.get_cubes_for_framework(framework_id)
        summary['CUBE'] = self._delete_queryset(qs, 'CUBE')

        # CUBE_STRUCTURE
        qs = FrameworkSubgraphFetcher.get_cube_structures_for_framework(framework_id)
        summary['CUBE_STRUCTURE'] = self._delete_queryset(qs, 'CUBE_STRUCTURE')

        return summary

    def _delete_hierarchies(self, framework_id: str) -> Dict[str, int]:
        """Delete hierarchy records."""
        summary = {}

        # MEMBER_HIERARCHY_NODE
        qs = FrameworkSubgraphFetcher.get_member_hierarchy_nodes_for_framework(framework_id)
        summary['MEMBER_HIERARCHY_NODE'] = self._delete_queryset(qs, 'MEMBER_HIERARCHY_NODE')

        # MEMBER_HIERARCHY
        qs = FrameworkSubgraphFetcher.get_hierarchies_for_framework(framework_id)
        summary['MEMBER_HIERARCHY'] = self._delete_queryset(qs, 'MEMBER_HIERARCHY')

        return summary

    def _delete_subdomains(self, framework_id: str) -> Dict[str, int]:
        """Delete subdomain records (NOT domains - those are ontology)."""
        summary = {}

        # SUBDOMAIN_ENUMERATION
        qs = FrameworkSubgraphFetcher.get_subdomain_enumerations_for_framework(framework_id)
        summary['SUBDOMAIN_ENUMERATION'] = self._delete_queryset(qs, 'SUBDOMAIN_ENUMERATION')

        # VARIABLE_SET_ENUMERATION
        qs = FrameworkSubgraphFetcher.get_variable_set_enumerations_for_framework(framework_id)
        summary['VARIABLE_SET_ENUMERATION'] = self._delete_queryset(qs, 'VARIABLE_SET_ENUMERATION')

        # VARIABLE_SET
        qs = FrameworkSubgraphFetcher.get_variable_sets_for_framework(framework_id)
        summary['VARIABLE_SET'] = self._delete_queryset(qs, 'VARIABLE_SET')

        # SUBDOMAIN
        qs = FrameworkSubgraphFetcher.get_subdomains_for_framework(framework_id)
        summary['SUBDOMAIN'] = self._delete_queryset(qs, 'SUBDOMAIN')

        # NOTE: DOMAIN, VARIABLE, MEMBER are NOT deleted (ontology layer)

        return summary

    def _delete_junction_tables(self, framework_id: str) -> Dict[str, int]:
        """Delete junction table records."""
        summary = {}

        # FRAMEWORK_TABLE
        qs = FrameworkSubgraphFetcher.get_framework_tables_for_framework(framework_id)
        summary['FRAMEWORK_TABLE'] = self._delete_queryset(qs, 'FRAMEWORK_TABLE')

        # FRAMEWORK_HIERARCHY
        qs = FrameworkSubgraphFetcher.get_framework_hierarchies_for_framework(framework_id)
        summary['FRAMEWORK_HIERARCHY'] = self._delete_queryset(qs, 'FRAMEWORK_HIERARCHY')

        # FRAMEWORK_SUBDOMAIN
        qs = FrameworkSubgraphFetcher.get_framework_subdomains_for_framework(framework_id)
        summary['FRAMEWORK_SUBDOMAIN'] = self._delete_queryset(qs, 'FRAMEWORK_SUBDOMAIN')

        return summary

    def _delete_facets(self, framework_id: str) -> Dict[str, int]:
        """Delete facet records."""
        summary = {}

        # FACET_ENUMERATION
        qs = FrameworkSubgraphFetcher.get_facet_enumerations_for_framework(framework_id)
        summary['FACET_ENUMERATION'] = self._delete_queryset(qs, 'FACET_ENUMERATION')

        # FACET_COLLECTION
        qs = FrameworkSubgraphFetcher.get_facet_collections_for_framework(framework_id)
        summary['FACET_COLLECTION'] = self._delete_queryset(qs, 'FACET_COLLECTION')

        return summary

    def _clear_sdd_context(self, sdd_context: Any, framework_id: str) -> None:
        """Clear framework-specific entries from SDDContext dictionaries."""
        # Framework prefixes to match
        prefixes = [
            framework_id,
            framework_id.upper(),
            framework_id.lower(),
            framework_id.replace('_REF', ''),
        ]

        # List of dictionaries to clear
        dict_names = [
            'bird_cube_dictionary',
            'bird_cube_structure_dictionary',
            'bird_cube_structure_item_dictionary',
            'cube_link_dictionary',
            'cube_link_to_foreign_cube_map',
            'cube_link_to_join_identifier_map',
            'cube_link_to_join_for_report_id_map',
            'cube_structure_item_links_dictionary',
            'cube_structure_item_link_to_cube_link_map',
            'mapping_definition_dictionary',
            'mapping_to_cube_dictionary',
            'member_mapping_dictionary',
            'member_mapping_items_dictionary',
            'variable_mapping_dictionary',
            'variable_mapping_item_dictionary',
            'combination_dictionary',
            'combination_item_dictionary',
            'combination_to_rol_cube_map',
            'report_tables_dictionary',
            'axis_dictionary',
            'axis_ordinate_dictionary',
            'axis_ordinate_to_ordinate_items_map',
            'table_cell_dictionary',
            'table_to_table_cell_dictionary',
            'cell_positions_dictionary',
            'member_hierarchy_dictionary',
            'member_hierarchy_node_dictionary',
            'subdomain_dictionary',
            'subdomain_to_domain_map',
            'subdomain_enumeration_dictionary',
        ]

        for dict_name in dict_names:
            # Clear instance-level dictionary
            if hasattr(sdd_context, dict_name):
                dict_obj = getattr(sdd_context, dict_name)
                if isinstance(dict_obj, dict):
                    keys_to_remove = [
                        key for key in dict_obj.keys()
                        if any(prefix in str(key) for prefix in prefixes)
                    ]
                    for key in keys_to_remove:
                        del dict_obj[key]
                    if keys_to_remove:
                        logger.debug(f"Cleared {len(keys_to_remove)} entries from {dict_name}")

            # Clear class-level dictionary
            if hasattr(SDDContext, dict_name):
                class_dict = getattr(SDDContext, dict_name)
                if isinstance(class_dict, dict):
                    keys_to_remove = [
                        key for key in class_dict.keys()
                        if any(prefix in str(key) for prefix in prefixes)
                    ]
                    for key in keys_to_remove:
                        del class_dict[key]
