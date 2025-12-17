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

"""
Framework Filtering Utilities

This module provides utilities for filtering BIRD data by framework (FINREP, COREP, ANCRDT).
The core strategy is to use CUBE as the anchor point for filtering, since CUBE already has
a framework_id field. Other entities are filtered by traversing relationships:

CUBE → CUBE_STRUCTURE → CUBE_STRUCTURE_ITEM → VARIABLE/MEMBER → SUBDOMAIN/DOMAIN

For entities not reachable via CUBE traversal (TABLE, MEMBER_HIERARCHY, SUBDOMAIN),
junction tables (FRAMEWORK_TABLE, FRAMEWORK_HIERARCHY, FRAMEWORK_SUBDOMAIN) are used.
"""

from typing import List, Dict, Any, Optional
from django.db.models import QuerySet


class FrameworkSubgraphFetcher:
    """Utility class for fetching framework-specific subgraphs of BIRD metadata."""

    @staticmethod
    def get_cubes_for_framework(framework_id: str) -> QuerySet:
        """
        Get all CUBEs for a specific framework.

        Args:
            framework_id: The framework identifier (e.g., 'FINREP', 'COREP', 'ANCRDT')

        Returns:
            QuerySet of CUBE objects filtered by framework_id
        """
        from pybirdai.models.bird_meta_data_model import CUBE
        return CUBE.objects.filter(framework_id=framework_id)

    @staticmethod
    def get_cube_structures_for_framework(framework_id: str) -> QuerySet:
        """
        Get all CUBE_STRUCTUREs for a specific framework.

        Traverses: CUBE (filter by framework) → CUBE_STRUCTURE

        Args:
            framework_id: The framework identifier

        Returns:
            QuerySet of CUBE_STRUCTURE objects
        """
        from pybirdai.models.bird_meta_data_model import CUBE, CUBE_STRUCTURE

        cubes = CUBE.objects.filter(framework_id=framework_id)
        structure_ids = cubes.values_list('cube_structure_id', flat=True).distinct()
        return CUBE_STRUCTURE.objects.filter(cube_structure_id__in=structure_ids)

    @staticmethod
    def get_cube_structure_items_for_framework(framework_id: str) -> QuerySet:
        """
        Get all CUBE_STRUCTURE_ITEMs for a specific framework.

        Traverses: CUBE (filter by framework) → CUBE_STRUCTURE → CUBE_STRUCTURE_ITEM

        Args:
            framework_id: The framework identifier

        Returns:
            QuerySet of CUBE_STRUCTURE_ITEM objects
        """
        from pybirdai.models.bird_meta_data_model import CUBE, CUBE_STRUCTURE_ITEM

        cubes = CUBE.objects.filter(framework_id=framework_id)
        structure_ids = cubes.values_list('cube_structure_id', flat=True).distinct()
        return CUBE_STRUCTURE_ITEM.objects.filter(cube_structure_id__in=structure_ids)

    @staticmethod
    def get_variables_for_framework(framework_id: str) -> QuerySet:
        """
        Get all VARIABLEs for a specific framework.

        Traverses: CUBE → CUBE_STRUCTURE → CUBE_STRUCTURE_ITEM → VARIABLE

        Args:
            framework_id: The framework identifier

        Returns:
            QuerySet of VARIABLE objects
        """
        from pybirdai.models.bird_meta_data_model import VARIABLE

        items = FrameworkSubgraphFetcher.get_cube_structure_items_for_framework(framework_id)
        variable_ids = items.values_list('variable_id', flat=True).distinct()
        return VARIABLE.objects.filter(variable_id__in=variable_ids)

    @staticmethod
    def get_members_for_framework(framework_id: str) -> QuerySet:
        """
        Get all MEMBERs for a specific framework.

        Traverses: CUBE → CUBE_STRUCTURE → CUBE_STRUCTURE_ITEM → MEMBER

        Args:
            framework_id: The framework identifier

        Returns:
            QuerySet of MEMBER objects
        """
        from pybirdai.models.bird_meta_data_model import MEMBER

        items = FrameworkSubgraphFetcher.get_cube_structure_items_for_framework(framework_id)
        member_ids = items.values_list('member_id', flat=True).distinct()
        return MEMBER.objects.filter(member_id__in=member_ids)

    @staticmethod
    def get_domains_for_framework(framework_id: str) -> QuerySet:
        """
        Get all DOMAINs for a specific framework.

        Traverses: CUBE → ... → VARIABLE → SUBDOMAIN → DOMAIN

        Args:
            framework_id: The framework identifier

        Returns:
            QuerySet of DOMAIN objects
        """
        from pybirdai.models.bird_meta_data_model import DOMAIN

        variables = FrameworkSubgraphFetcher.get_variables_for_framework(framework_id)
        subdomain_ids = variables.values_list('domain_id__domain_id', flat=True).distinct()
        domain_ids = subdomain_ids  # VARIABLE.domain_id points to SUBDOMAIN which has domain_id FK
        return DOMAIN.objects.filter(domain_id__in=domain_ids)

    @staticmethod
    def get_subdomains_for_framework(framework_id: str) -> QuerySet:
        """
        Get all SUBDOMAINs for a specific framework via FRAMEWORK_SUBDOMAIN junction table.

        Uses: FRAMEWORK_SUBDOMAIN junction table

        Args:
            framework_id: The framework identifier

        Returns:
            QuerySet of SUBDOMAIN objects
        """
        from pybirdai.models.bird_meta_data_model import SUBDOMAIN, FRAMEWORK_SUBDOMAIN

        framework_subdomain_records = FRAMEWORK_SUBDOMAIN.objects.filter(
            framework_id=framework_id
        )
        subdomain_ids = framework_subdomain_records.values_list('subdomain_id', flat=True).distinct()
        return SUBDOMAIN.objects.filter(subdomain_id__in=subdomain_ids)

    @staticmethod
    def get_tables_for_framework(framework_id: str) -> QuerySet:
        """
        Get all TABLEs (report templates) for a specific framework via FRAMEWORK_TABLE junction table.

        Uses: FRAMEWORK_TABLE junction table

        Args:
            framework_id: The framework identifier

        Returns:
            QuerySet of TABLE objects
        """
        from pybirdai.models.bird_meta_data_model import TABLE, FRAMEWORK_TABLE

        framework_table_records = FRAMEWORK_TABLE.objects.filter(
            framework_id=framework_id
        )
        table_ids = framework_table_records.values_list('table_id', flat=True).distinct()
        return TABLE.objects.filter(table_id__in=table_ids)

    @staticmethod
    def get_hierarchies_for_framework(framework_id: str) -> QuerySet:
        """
        Get all MEMBER_HIERARCHYs for a specific framework via FRAMEWORK_HIERARCHY junction table.

        Uses: FRAMEWORK_HIERARCHY junction table

        Args:
            framework_id: The framework identifier

        Returns:
            QuerySet of MEMBER_HIERARCHY objects
        """
        from pybirdai.models.bird_meta_data_model import MEMBER_HIERARCHY, FRAMEWORK_HIERARCHY

        framework_hierarchy_records = FRAMEWORK_HIERARCHY.objects.filter(
            framework_id=framework_id
        )
        hierarchy_ids = framework_hierarchy_records.values_list('member_hierarchy_id', flat=True).distinct()
        return MEMBER_HIERARCHY.objects.filter(member_hierarchy_id__in=hierarchy_ids)

    @staticmethod
    def get_axes_for_framework(framework_id: str) -> QuerySet:
        """
        Get all AXESs for a specific framework.

        Traverses: TABLE (via FRAMEWORK_TABLE) → AXIS

        Args:
            framework_id: The framework identifier

        Returns:
            QuerySet of AXIS objects
        """
        from pybirdai.models.bird_meta_data_model import AXIS

        tables = FrameworkSubgraphFetcher.get_tables_for_framework(framework_id)
        table_ids = tables.values_list('table_id', flat=True).distinct()
        return AXIS.objects.filter(table_id__in=table_ids)

    @staticmethod
    def get_cube_links_for_framework(framework_id: str) -> QuerySet:
        """
        Get all CUBE_LINKs for a specific framework.

        Traverses: CUBE (filter by framework) → CUBE_LINK

        Args:
            framework_id: The framework identifier

        Returns:
            QuerySet of CUBE_LINK objects
        """
        from pybirdai.models.bird_meta_data_model import CUBE_LINK

        cubes = FrameworkSubgraphFetcher.get_cubes_for_framework(framework_id)
        cube_ids = cubes.values_list('cube_id', flat=True).distinct()
        return CUBE_LINK.objects.filter(
            from_cube_id__in=cube_ids
        ) | CUBE_LINK.objects.filter(
            to_cube_id__in=cube_ids
        )

    @staticmethod
    def prefetch_all_for_framework(framework_id: str) -> Dict[str, List[Any]]:
        """
        Prefetch all framework-related data at once for performance optimization.

        This method executes all queries upfront and returns the results as lists,
        which can be useful when you need to access multiple entity types and want
        to minimize database queries.

        Args:
            framework_id: The framework identifier

        Returns:
            Dictionary mapping entity type names to lists of objects:
            {
                'cubes': [CUBE, ...],
                'cube_structures': [CUBE_STRUCTURE, ...],
                'cube_structure_items': [CUBE_STRUCTURE_ITEM, ...],
                'variables': [VARIABLE, ...],
                'members': [MEMBER, ...],
                'domains': [DOMAIN, ...],
                'subdomains': [SUBDOMAIN, ...],
                'tables': [TABLE, ...],
                'hierarchies': [MEMBER_HIERARCHY, ...],
                'axes': [AXIS, ...],
                'cube_links': [CUBE_LINK, ...],
            }
        """
        return {
            'cubes': list(FrameworkSubgraphFetcher.get_cubes_for_framework(framework_id)),
            'cube_structures': list(FrameworkSubgraphFetcher.get_cube_structures_for_framework(framework_id)),
            'cube_structure_items': list(FrameworkSubgraphFetcher.get_cube_structure_items_for_framework(framework_id)),
            'variables': list(FrameworkSubgraphFetcher.get_variables_for_framework(framework_id)),
            'members': list(FrameworkSubgraphFetcher.get_members_for_framework(framework_id)),
            'domains': list(FrameworkSubgraphFetcher.get_domains_for_framework(framework_id)),
            'subdomains': list(FrameworkSubgraphFetcher.get_subdomains_for_framework(framework_id)),
            'tables': list(FrameworkSubgraphFetcher.get_tables_for_framework(framework_id)),
            'hierarchies': list(FrameworkSubgraphFetcher.get_hierarchies_for_framework(framework_id)),
            'axes': list(FrameworkSubgraphFetcher.get_axes_for_framework(framework_id)),
            'cube_links': list(FrameworkSubgraphFetcher.get_cube_links_for_framework(framework_id)),
        }

    @staticmethod
    def get_framework_statistics(framework_id: str) -> Dict[str, int]:
        """
        Get counts of all entity types for a specific framework.

        Useful for debugging and validation.

        Args:
            framework_id: The framework identifier

        Returns:
            Dictionary mapping entity type names to counts
        """
        return {
            'cubes': FrameworkSubgraphFetcher.get_cubes_for_framework(framework_id).count(),
            'cube_structures': FrameworkSubgraphFetcher.get_cube_structures_for_framework(framework_id).count(),
            'cube_structure_items': FrameworkSubgraphFetcher.get_cube_structure_items_for_framework(framework_id).count(),
            'variables': FrameworkSubgraphFetcher.get_variables_for_framework(framework_id).count(),
            'members': FrameworkSubgraphFetcher.get_members_for_framework(framework_id).count(),
            'domains': FrameworkSubgraphFetcher.get_domains_for_framework(framework_id).count(),
            'subdomains': FrameworkSubgraphFetcher.get_subdomains_for_framework(framework_id).count(),
            'tables': FrameworkSubgraphFetcher.get_tables_for_framework(framework_id).count(),
            'hierarchies': FrameworkSubgraphFetcher.get_hierarchies_for_framework(framework_id).count(),
            'axes': FrameworkSubgraphFetcher.get_axes_for_framework(framework_id).count(),
            'cube_links': FrameworkSubgraphFetcher.get_cube_links_for_framework(framework_id).count(),
        }


def get_current_framework_from_session(request) -> Optional[str]:
    """
    Helper function to get the current framework from the Django session.

    Args:
        request: Django HttpRequest object

    Returns:
        Framework ID string, or None if not set
    """
    return request.session.get('current_framework_id', None)


def set_current_framework_in_session(request, framework_id: str) -> None:
    """
    Helper function to set the current framework in the Django session.

    Args:
        request: Django HttpRequest object
        framework_id: The framework identifier to set
    """
    request.session['current_framework_id'] = framework_id
