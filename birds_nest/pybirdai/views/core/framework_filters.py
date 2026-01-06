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
    def _get_maintenance_agency_for_framework(framework_id: str) -> str:
        """
        Get the maintenance agency ID associated with a framework.

        Rules:
        - EBA_* frameworks (EBA_FINREP, EBA_COREP, etc.) -> 'EBA'
        - *_REF frameworks (FINREP_REF, COREP_REF, etc.) -> 'REF'
        - ANCRDT -> 'ECB'
        - Others -> None (no agency-based filtering)

        Args:
            framework_id: The framework identifier

        Returns:
            Maintenance agency ID string, or None
        """
        if framework_id and framework_id.startswith('EBA_'):
            return 'EBA'
        if framework_id and framework_id.endswith('_REF'):
            return 'REF'
        if framework_id == 'ANCRDT':
            return 'ECB'
        return None

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

        For EBA frameworks (EBA_FINREP, EBA_COREP, etc.), also includes all
        variables owned by the EBA maintenance agency, since EBA cubes may not
        have CUBE_STRUCTURE_ITEMs populated.

        Args:
            framework_id: The framework identifier

        Returns:
            QuerySet of VARIABLE objects
        """
        from pybirdai.models.bird_meta_data_model import VARIABLE

        # Get variables via cube traversal
        items = FrameworkSubgraphFetcher.get_cube_structure_items_for_framework(framework_id)
        variable_ids = set(items.values_list('variable_id', flat=True).distinct())

        # For frameworks with a maintenance agency, also include all variables
        # owned by that agency (e.g., EBA_FINREP -> include EBA-owned variables)
        agency_id = FrameworkSubgraphFetcher._get_maintenance_agency_for_framework(framework_id)
        if agency_id:
            agency_variable_ids = set(
                VARIABLE.objects.filter(maintenance_agency_id=agency_id)
                .values_list('variable_id', flat=True)
            )
            variable_ids = variable_ids | agency_variable_ids

        variable_ids.discard(None)
        return VARIABLE.objects.filter(variable_id__in=variable_ids)

    @staticmethod
    def get_members_for_framework(framework_id: str) -> QuerySet:
        """
        Get all MEMBERs for a specific framework.

        Traverses: CUBE → CUBE_STRUCTURE → CUBE_STRUCTURE_ITEM → VARIABLE → DOMAIN → MEMBER
        Members are retrieved via the DOMAIN of the VARIABLES used in the framework.

        For EBA frameworks, also includes all members owned by the EBA
        maintenance agency (via domain ownership).

        Args:
            framework_id: The framework identifier

        Returns:
            QuerySet of MEMBER objects
        """
        from pybirdai.models.bird_meta_data_model import MEMBER

        # Get domains from variables in the framework (now includes agency-owned domains)
        domains = FrameworkSubgraphFetcher.get_domains_for_framework(framework_id)
        domain_ids = set(domains.values_list('domain_id', flat=True).distinct())

        # For frameworks with a maintenance agency, also include members from
        # domains owned by that agency
        agency_id = FrameworkSubgraphFetcher._get_maintenance_agency_for_framework(framework_id)
        if agency_id:
            from pybirdai.models.bird_meta_data_model import DOMAIN
            agency_domain_ids = set(
                DOMAIN.objects.filter(maintenance_agency_id=agency_id)
                .values_list('domain_id', flat=True)
            )
            domain_ids = domain_ids | agency_domain_ids

        domain_ids.discard(None)

        # Get members that belong to these domains
        return MEMBER.objects.filter(domain_id__in=domain_ids)

    @staticmethod
    def get_domains_for_framework(framework_id: str) -> QuerySet:
        """
        Get all DOMAINs for a specific framework.

        Traverses:
        - CUBE → CUBE_STRUCTURE → CUBE_STRUCTURE_ITEM → VARIABLE → DOMAIN
        - CUBE → CUBE_STRUCTURE → CUBE_STRUCTURE_ITEM → SUBDOMAIN → DOMAIN

        For EBA frameworks, also includes all domains owned by the EBA
        maintenance agency.

        Args:
            framework_id: The framework identifier

        Returns:
            QuerySet of DOMAIN objects
        """
        from pybirdai.models.bird_meta_data_model import DOMAIN

        # Get domains from variables (VARIABLE.domain_id -> DOMAIN)
        variables = FrameworkSubgraphFetcher.get_variables_for_framework(framework_id)
        domain_ids_from_vars = set(variables.values_list('domain_id', flat=True).distinct())

        # Get domains from subdomains (SUBDOMAIN.domain_id -> DOMAIN)
        subdomains = FrameworkSubgraphFetcher.get_subdomains_for_framework(framework_id)
        domain_ids_from_subdomains = set(subdomains.values_list('domain_id', flat=True).distinct())

        # Combine both sources
        all_domain_ids = domain_ids_from_vars | domain_ids_from_subdomains

        # For frameworks with a maintenance agency, also include all domains
        # owned by that agency (e.g., EBA_FINREP -> include EBA-owned domains)
        agency_id = FrameworkSubgraphFetcher._get_maintenance_agency_for_framework(framework_id)
        if agency_id:
            agency_domain_ids = set(
                DOMAIN.objects.filter(maintenance_agency_id=agency_id)
                .values_list('domain_id', flat=True)
            )
            all_domain_ids = all_domain_ids | agency_domain_ids

        # Remove None values
        all_domain_ids.discard(None)

        return DOMAIN.objects.filter(domain_id__in=all_domain_ids)

    @staticmethod
    def get_subdomains_for_framework(framework_id: str) -> QuerySet:
        """
        Get all SUBDOMAINs for a specific framework.

        Traverses: CUBE → CUBE_STRUCTURE → CUBE_STRUCTURE_ITEM → SUBDOMAIN
        Also includes subdomains from COMBINATION_ITEM if combinations exist.

        For EBA frameworks, also includes:
        - All subdomains owned by the EBA maintenance agency
        - All subdomains that belong to EBA-owned domains

        Args:
            framework_id: The framework identifier

        Returns:
            QuerySet of SUBDOMAIN objects
        """
        from pybirdai.models.bird_meta_data_model import SUBDOMAIN, DOMAIN

        # Get subdomains from cube structure items
        items = FrameworkSubgraphFetcher.get_cube_structure_items_for_framework(framework_id)
        subdomain_ids = set(items.values_list('subdomain_id', flat=True).distinct())

        # Also get subdomains from combination items (if combinations exist)
        combination_items = FrameworkSubgraphFetcher.get_combination_items_for_framework(framework_id)
        subdomain_ids_from_combinations = set(
            combination_items.values_list('subdomain_id', flat=True).distinct()
        )

        # Combine both sources
        all_subdomain_ids = subdomain_ids | subdomain_ids_from_combinations

        # For frameworks with a maintenance agency, also include:
        # 1. All subdomains owned by that agency
        # 2. All subdomains belonging to agency-owned domains
        agency_id = FrameworkSubgraphFetcher._get_maintenance_agency_for_framework(framework_id)
        if agency_id:
            # Include subdomains owned by the agency
            agency_subdomain_ids = set(
                SUBDOMAIN.objects.filter(maintenance_agency_id=agency_id)
                .values_list('subdomain_id', flat=True)
            )
            all_subdomain_ids = all_subdomain_ids | agency_subdomain_ids

            # Include subdomains belonging to agency-owned domains
            agency_domain_ids = set(
                DOMAIN.objects.filter(maintenance_agency_id=agency_id)
                .values_list('domain_id', flat=True)
            )
            subdomains_for_agency_domains = set(
                SUBDOMAIN.objects.filter(domain_id__in=agency_domain_ids)
                .values_list('subdomain_id', flat=True)
            )
            all_subdomain_ids = all_subdomain_ids | subdomains_for_agency_domains

        all_subdomain_ids.discard(None)

        return SUBDOMAIN.objects.filter(subdomain_id__in=all_subdomain_ids)

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
        Get all MEMBER_HIERARCHYs for a specific framework.

        Traverses: CUBE → ... → DOMAIN → MEMBER_HIERARCHY (via domain_id)
        Falls back to FRAMEWORK_HIERARCHY junction table if available.

        Args:
            framework_id: The framework identifier

        Returns:
            QuerySet of MEMBER_HIERARCHY objects
        """
        from pybirdai.models.bird_meta_data_model import MEMBER_HIERARCHY, FRAMEWORK_HIERARCHY

        hierarchy_ids = set()

        # Primary method: Get hierarchies via DOMAIN traversal
        # MEMBER_HIERARCHY has domain_id field
        domains = FrameworkSubgraphFetcher.get_domains_for_framework(framework_id)
        domain_ids = domains.values_list('domain_id', flat=True).distinct()
        hierarchy_ids.update(
            MEMBER_HIERARCHY.objects.filter(domain_id__in=domain_ids)
            .values_list('member_hierarchy_id', flat=True).distinct()
        )

        # Secondary method: Also check FRAMEWORK_HIERARCHY junction table
        framework_hierarchy_records = FRAMEWORK_HIERARCHY.objects.filter(
            framework_id=framework_id
        )
        hierarchy_ids.update(
            framework_hierarchy_records.values_list('member_hierarchy_id', flat=True).distinct()
        )

        hierarchy_ids.discard(None)
        return MEMBER_HIERARCHY.objects.filter(member_hierarchy_id__in=hierarchy_ids)

    @staticmethod
    def get_axes_for_framework(framework_id: str) -> QuerySet:
        """
        Get all AXISs for a specific framework.

        Traverses: TABLE (via FRAMEWORK_TABLE) → AXIS
        Falls back to pattern matching on axis_id if table_id links are missing.

        Args:
            framework_id: The framework identifier

        Returns:
            QuerySet of AXIS objects
        """
        from pybirdai.models.bird_meta_data_model import AXIS

        # Try table traversal first
        tables = FrameworkSubgraphFetcher.get_tables_for_framework(framework_id)
        table_ids = list(tables.values_list('table_id', flat=True).distinct())
        axes_via_table = AXIS.objects.filter(table_id__in=table_ids)

        # Also include axes that match the framework_id pattern in their axis_id
        # (handles cases where table_id is NULL but axis_id contains framework info)
        axes_via_pattern = AXIS.objects.filter(axis_id__startswith=framework_id)

        # Union both querysets
        return (axes_via_table | axes_via_pattern).distinct()

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
            primary_cube_id__in=cube_ids
        ) | CUBE_LINK.objects.filter(
            foreign_cube_id__in=cube_ids
        )

    # ==================== Additional Traversal Methods ====================

    @staticmethod
    def get_subdomain_enumerations_for_framework(framework_id: str) -> QuerySet:
        """
        Get all SUBDOMAIN_ENUMERATIONs for a specific framework.

        Traverses: CUBE → ... → SUBDOMAIN → SUBDOMAIN_ENUMERATION

        Args:
            framework_id: The framework identifier

        Returns:
            QuerySet of SUBDOMAIN_ENUMERATION objects
        """
        from pybirdai.models.bird_meta_data_model import SUBDOMAIN_ENUMERATION

        subdomains = FrameworkSubgraphFetcher.get_subdomains_for_framework(framework_id)
        subdomain_ids = subdomains.values_list('subdomain_id', flat=True).distinct()
        return SUBDOMAIN_ENUMERATION.objects.filter(subdomain_id__in=subdomain_ids)

    @staticmethod
    def get_variable_sets_for_framework(framework_id: str) -> QuerySet:
        """
        Get all VARIABLE_SETs for a specific framework.

        Traverses: CUBE → CUBE_STRUCTURE → CUBE_STRUCTURE_ITEM → VARIABLE_SET
        Also includes variable sets from COMBINATION_ITEM.

        Args:
            framework_id: The framework identifier

        Returns:
            QuerySet of VARIABLE_SET objects
        """
        from pybirdai.models.bird_meta_data_model import VARIABLE_SET

        # Get variable sets from cube structure items
        items = FrameworkSubgraphFetcher.get_cube_structure_items_for_framework(framework_id)
        vs_ids = set(items.values_list('variable_set_id', flat=True).distinct())

        # Also get from combination items
        combination_items = FrameworkSubgraphFetcher.get_combination_items_for_framework(framework_id)
        vs_ids_from_combinations = set(
            combination_items.values_list('variable_set_id', flat=True).distinct()
        )

        all_vs_ids = vs_ids | vs_ids_from_combinations
        all_vs_ids.discard(None)

        return VARIABLE_SET.objects.filter(variable_set_id__in=all_vs_ids)

    @staticmethod
    def get_variable_set_enumerations_for_framework(framework_id: str) -> QuerySet:
        """
        Get all VARIABLE_SET_ENUMERATIONs for a specific framework.

        Traverses: CUBE → ... → VARIABLE_SET → VARIABLE_SET_ENUMERATION

        Args:
            framework_id: The framework identifier

        Returns:
            QuerySet of VARIABLE_SET_ENUMERATION objects
        """
        from pybirdai.models.bird_meta_data_model import VARIABLE_SET_ENUMERATION

        variable_sets = FrameworkSubgraphFetcher.get_variable_sets_for_framework(framework_id)
        vs_ids = variable_sets.values_list('variable_set_id', flat=True).distinct()
        return VARIABLE_SET_ENUMERATION.objects.filter(variable_set_id__in=vs_ids)

    @staticmethod
    def get_cube_to_combinations_for_framework(framework_id: str) -> QuerySet:
        """
        Get all CUBE_TO_COMBINATIONs for a specific framework.

        Traverses: CUBE → CUBE_TO_COMBINATION

        Args:
            framework_id: The framework identifier

        Returns:
            QuerySet of CUBE_TO_COMBINATION objects
        """
        from pybirdai.models.bird_meta_data_model import CUBE_TO_COMBINATION

        cubes = FrameworkSubgraphFetcher.get_cubes_for_framework(framework_id)
        cube_ids = cubes.values_list('cube_id', flat=True).distinct()
        return CUBE_TO_COMBINATION.objects.filter(cube_id__in=cube_ids)

    @staticmethod
    def get_combinations_for_framework(framework_id: str) -> QuerySet:
        """
        Get all COMBINATIONs for a specific framework.

        Traverses: CUBE → CUBE_TO_COMBINATION → COMBINATION

        Args:
            framework_id: The framework identifier

        Returns:
            QuerySet of COMBINATION objects
        """
        from pybirdai.models.bird_meta_data_model import COMBINATION

        cube_to_combinations = FrameworkSubgraphFetcher.get_cube_to_combinations_for_framework(framework_id)
        combination_ids = cube_to_combinations.values_list('combination_id', flat=True).distinct()
        return COMBINATION.objects.filter(combination_id__in=combination_ids)

    @staticmethod
    def get_combination_items_for_framework(framework_id: str) -> QuerySet:
        """
        Get all COMBINATION_ITEMs for a specific framework.

        Traverses: CUBE → CUBE_TO_COMBINATION → COMBINATION → COMBINATION_ITEM

        Args:
            framework_id: The framework identifier

        Returns:
            QuerySet of COMBINATION_ITEM objects
        """
        from pybirdai.models.bird_meta_data_model import COMBINATION_ITEM

        combinations = FrameworkSubgraphFetcher.get_combinations_for_framework(framework_id)
        combination_ids = combinations.values_list('combination_id', flat=True).distinct()
        return COMBINATION_ITEM.objects.filter(combination_id__in=combination_ids)

    @staticmethod
    def get_cube_structure_item_links_for_framework(framework_id: str) -> QuerySet:
        """
        Get all CUBE_STRUCTURE_ITEM_LINKs for a specific framework.

        Traverses: CUBE → CUBE_LINK → CUBE_STRUCTURE_ITEM_LINK

        Args:
            framework_id: The framework identifier

        Returns:
            QuerySet of CUBE_STRUCTURE_ITEM_LINK objects
        """
        from pybirdai.models.bird_meta_data_model import CUBE_STRUCTURE_ITEM_LINK

        cube_links = FrameworkSubgraphFetcher.get_cube_links_for_framework(framework_id)
        cube_link_ids = cube_links.values_list('cube_link_id', flat=True).distinct()
        return CUBE_STRUCTURE_ITEM_LINK.objects.filter(cube_link_id__in=cube_link_ids)

    @staticmethod
    def get_member_links_for_framework(framework_id: str) -> QuerySet:
        """
        Get all MEMBER_LINKs for a specific framework.

        Traverses: CUBE → CUBE_LINK → CUBE_STRUCTURE_ITEM_LINK → MEMBER_LINK

        Args:
            framework_id: The framework identifier

        Returns:
            QuerySet of MEMBER_LINK objects
        """
        from pybirdai.models.bird_meta_data_model import MEMBER_LINK

        csil = FrameworkSubgraphFetcher.get_cube_structure_item_links_for_framework(framework_id)
        csil_ids = csil.values_list('cube_structure_item_link_id', flat=True).distinct()
        return MEMBER_LINK.objects.filter(cube_structure_item_link_id__in=csil_ids)

    @staticmethod
    def get_member_hierarchy_nodes_for_framework(framework_id: str) -> QuerySet:
        """
        Get all MEMBER_HIERARCHY_NODEs for a specific framework.

        Traverses: FRAMEWORK_HIERARCHY → MEMBER_HIERARCHY → MEMBER_HIERARCHY_NODE

        Args:
            framework_id: The framework identifier

        Returns:
            QuerySet of MEMBER_HIERARCHY_NODE objects
        """
        from pybirdai.models.bird_meta_data_model import MEMBER_HIERARCHY_NODE

        hierarchies = FrameworkSubgraphFetcher.get_hierarchies_for_framework(framework_id)
        hierarchy_ids = hierarchies.values_list('member_hierarchy_id', flat=True).distinct()
        return MEMBER_HIERARCHY_NODE.objects.filter(member_hierarchy_id__in=hierarchy_ids)

    @staticmethod
    def get_facet_collections_for_framework(framework_id: str) -> QuerySet:
        """
        Get all FACET_COLLECTIONs for a specific framework.

        Traverses: DOMAIN → FACET_COLLECTION and SUBDOMAIN → FACET_COLLECTION

        Args:
            framework_id: The framework identifier

        Returns:
            QuerySet of FACET_COLLECTION objects
        """
        from pybirdai.models.bird_meta_data_model import FACET_COLLECTION

        # Get facet collections from domains
        domains = FrameworkSubgraphFetcher.get_domains_for_framework(framework_id)
        facet_ids_from_domains = set(domains.values_list('facet_id', flat=True).distinct())

        # Get facet collections from subdomains
        subdomains = FrameworkSubgraphFetcher.get_subdomains_for_framework(framework_id)
        facet_ids_from_subdomains = set(subdomains.values_list('facet_id', flat=True).distinct())

        all_facet_ids = facet_ids_from_domains | facet_ids_from_subdomains
        all_facet_ids.discard(None)

        return FACET_COLLECTION.objects.filter(facet_id__in=all_facet_ids)

    # ==================== Rendering Package Methods (for reporting frameworks) ====================

    @staticmethod
    def get_axis_ordinates_for_framework(framework_id: str) -> QuerySet:
        """
        Get all AXIS_ORDINATEs for a specific framework.

        Traverses: TABLE → AXIS → AXIS_ORDINATE

        Args:
            framework_id: The framework identifier

        Returns:
            QuerySet of AXIS_ORDINATE objects
        """
        from pybirdai.models.bird_meta_data_model import AXIS_ORDINATE

        axes = FrameworkSubgraphFetcher.get_axes_for_framework(framework_id)
        axis_ids = axes.values_list('axis_id', flat=True).distinct()
        return AXIS_ORDINATE.objects.filter(axis_id__in=axis_ids)

    @staticmethod
    def get_table_cells_for_framework(framework_id: str) -> QuerySet:
        """
        Get all TABLE_CELLs for a specific framework.

        Traverses: TABLE → TABLE_CELL

        Args:
            framework_id: The framework identifier

        Returns:
            QuerySet of TABLE_CELL objects
        """
        from pybirdai.models.bird_meta_data_model import TABLE_CELL

        tables = FrameworkSubgraphFetcher.get_tables_for_framework(framework_id)
        table_ids = tables.values_list('table_id', flat=True).distinct()
        return TABLE_CELL.objects.filter(table_id__in=table_ids)

    @staticmethod
    def get_ordinate_items_for_framework(framework_id: str) -> QuerySet:
        """
        Get all ORDINATE_ITEMs for a specific framework.

        Traverses: TABLE → AXIS → AXIS_ORDINATE → ORDINATE_ITEM

        Args:
            framework_id: The framework identifier

        Returns:
            QuerySet of ORDINATE_ITEM objects
        """
        from pybirdai.models.bird_meta_data_model import ORDINATE_ITEM

        axis_ordinates = FrameworkSubgraphFetcher.get_axis_ordinates_for_framework(framework_id)
        ao_ids = axis_ordinates.values_list('axis_ordinate_id', flat=True).distinct()
        return ORDINATE_ITEM.objects.filter(axis_ordinate_id__in=ao_ids)

    @staticmethod
    def get_cell_positions_for_framework(framework_id: str) -> QuerySet:
        """
        Get all CELL_POSITIONs for a specific framework.

        Traverses: TABLE → TABLE_CELL → CELL_POSITION

        Args:
            framework_id: The framework identifier

        Returns:
            QuerySet of CELL_POSITION objects
        """
        from pybirdai.models.bird_meta_data_model import CELL_POSITION

        table_cells = FrameworkSubgraphFetcher.get_table_cells_for_framework(framework_id)
        cell_ids = table_cells.values_list('cell_id', flat=True).distinct()
        return CELL_POSITION.objects.filter(cell_id__in=cell_ids)

    @staticmethod
    def get_cube_to_tables_for_framework(framework_id: str) -> QuerySet:
        """
        Get all CUBE_TO_TABLEs for a specific framework.

        Note: This model links CUBEs to TABLEs. We filter by both.

        Args:
            framework_id: The framework identifier

        Returns:
            QuerySet of CUBE_TO_TABLE objects (if model exists)
        """
        try:
            from pybirdai.models.bird_meta_data_model import CUBE_TO_TABLE
        except ImportError:
            # Model doesn't exist
            return None

        cubes = FrameworkSubgraphFetcher.get_cubes_for_framework(framework_id)
        cube_ids = cubes.values_list('cube_id', flat=True).distinct()

        tables = FrameworkSubgraphFetcher.get_tables_for_framework(framework_id)
        table_ids = tables.values_list('table_id', flat=True).distinct()

        return CUBE_TO_TABLE.objects.filter(cube_id__in=cube_ids, table_id__in=table_ids)

    @staticmethod
    def get_framework_for_framework(framework_id: str) -> QuerySet:
        """
        Get the FRAMEWORK object itself.

        Args:
            framework_id: The framework identifier

        Returns:
            QuerySet containing the FRAMEWORK object
        """
        from pybirdai.models.bird_meta_data_model import FRAMEWORK
        return FRAMEWORK.objects.filter(framework_id=framework_id)

    @staticmethod
    def get_maintenance_agencies_for_framework(framework_id: str) -> QuerySet:
        """
        Get all MAINTENANCE_AGENCYs used by the framework.

        Collects maintenance_agency_id from various models in the subgraph.

        Args:
            framework_id: The framework identifier

        Returns:
            QuerySet of MAINTENANCE_AGENCY objects
        """
        from pybirdai.models.bird_meta_data_model import MAINTENANCE_AGENCY

        agency_ids = set()

        # Get from cubes
        cubes = FrameworkSubgraphFetcher.get_cubes_for_framework(framework_id)
        agency_ids.update(cubes.values_list('maintenance_agency_id', flat=True).distinct())

        # Get from variables
        variables = FrameworkSubgraphFetcher.get_variables_for_framework(framework_id)
        agency_ids.update(variables.values_list('maintenance_agency_id', flat=True).distinct())

        # Get from domains
        domains = FrameworkSubgraphFetcher.get_domains_for_framework(framework_id)
        agency_ids.update(domains.values_list('maintenance_agency_id', flat=True).distinct())

        agency_ids.discard(None)

        return MAINTENANCE_AGENCY.objects.filter(maintenance_agency_id__in=agency_ids)

    # ==================== Mapping Package Methods ====================

    @staticmethod
    def get_mapping_to_cubes_for_framework(framework_id: str) -> QuerySet:
        """
        Get all MAPPING_TO_CUBEs for a specific framework.

        The relationship is: MAPPING_TO_CUBE.cube_mapping_id is a substring of CUBE.cube_id.
        We filter by finding CUBEs in the framework and matching their cube_ids.

        Args:
            framework_id: The framework identifier

        Returns:
            QuerySet of MAPPING_TO_CUBE objects
        """
        from pybirdai.models.bird_meta_data_model import MAPPING_TO_CUBE, CUBE
        from django.db.models import Q

        # Get all cube_ids for the framework
        cubes = CUBE.objects.filter(framework_id=framework_id)
        cube_ids = list(cubes.values_list('cube_id', flat=True))

        if not cube_ids:
            return MAPPING_TO_CUBE.objects.none()

        # Find MAPPING_TO_CUBE records where cube_mapping_id is a substring of any cube_id
        # This requires checking each cube_id to see if it contains the cube_mapping_id
        matching_mapping_to_cubes = set()
        all_mapping_to_cubes = MAPPING_TO_CUBE.objects.all()

        for mtc in all_mapping_to_cubes:
            if mtc.cube_mapping_id:
                for cube_id in cube_ids:
                    if mtc.cube_mapping_id in cube_id:
                        matching_mapping_to_cubes.add(mtc.pk)
                        break

        return MAPPING_TO_CUBE.objects.filter(pk__in=matching_mapping_to_cubes)

    @staticmethod
    def get_mapping_definitions_for_framework(framework_id: str) -> QuerySet:
        """
        Get all MAPPING_DEFINITIONs for a specific framework.

        Filters based on MAPPING_TO_CUBE relationship to CUBEs in the framework.

        Args:
            framework_id: The framework identifier

        Returns:
            QuerySet of MAPPING_DEFINITION objects
        """
        from pybirdai.models.bird_meta_data_model import MAPPING_DEFINITION

        # Get mapping_to_cubes for the framework
        mapping_to_cubes = FrameworkSubgraphFetcher.get_mapping_to_cubes_for_framework(framework_id)

        # Get the mapping_ids from those MAPPING_TO_CUBE records
        mapping_ids = mapping_to_cubes.values_list('mapping_id', flat=True).distinct()

        return MAPPING_DEFINITION.objects.filter(mapping_id__in=mapping_ids)

    @staticmethod
    def get_variable_mappings_for_framework(framework_id: str) -> QuerySet:
        """
        Get all VARIABLE_MAPPINGs for a specific framework.

        Filters based on MAPPING_DEFINITION relationship.

        Args:
            framework_id: The framework identifier

        Returns:
            QuerySet of VARIABLE_MAPPING objects
        """
        from pybirdai.models.bird_meta_data_model import VARIABLE_MAPPING

        # Get mapping_definitions for the framework
        mapping_defs = FrameworkSubgraphFetcher.get_mapping_definitions_for_framework(framework_id)

        # Get variable_mapping_ids from those MAPPING_DEFINITIONs
        variable_mapping_ids = mapping_defs.exclude(
            variable_mapping_id__isnull=True
        ).values_list('variable_mapping_id', flat=True).distinct()

        return VARIABLE_MAPPING.objects.filter(variable_mapping_id__in=variable_mapping_ids)

    @staticmethod
    def get_variable_mapping_items_for_framework(framework_id: str) -> QuerySet:
        """
        Get all VARIABLE_MAPPING_ITEMs for a specific framework.

        Filters based on VARIABLE_MAPPING relationship.

        Args:
            framework_id: The framework identifier

        Returns:
            QuerySet of VARIABLE_MAPPING_ITEM objects
        """
        from pybirdai.models.bird_meta_data_model import VARIABLE_MAPPING_ITEM

        # Get variable_mappings for the framework
        variable_mappings = FrameworkSubgraphFetcher.get_variable_mappings_for_framework(framework_id)

        return VARIABLE_MAPPING_ITEM.objects.filter(variable_mapping_id__in=variable_mappings)

    @staticmethod
    def get_member_mappings_for_framework(framework_id: str) -> QuerySet:
        """
        Get all MEMBER_MAPPINGs for a specific framework.

        Filters based on MAPPING_DEFINITION relationship.

        Args:
            framework_id: The framework identifier

        Returns:
            QuerySet of MEMBER_MAPPING objects
        """
        from pybirdai.models.bird_meta_data_model import MEMBER_MAPPING

        # Get mapping_definitions for the framework
        mapping_defs = FrameworkSubgraphFetcher.get_mapping_definitions_for_framework(framework_id)

        # Get member_mapping_ids from those MAPPING_DEFINITIONs
        member_mapping_ids = mapping_defs.exclude(
            member_mapping_id__isnull=True
        ).values_list('member_mapping_id', flat=True).distinct()

        return MEMBER_MAPPING.objects.filter(member_mapping_id__in=member_mapping_ids)

    @staticmethod
    def get_member_mapping_items_for_framework(framework_id: str) -> QuerySet:
        """
        Get all MEMBER_MAPPING_ITEMs for a specific framework.

        Filters based on MEMBER_MAPPING relationship.

        Args:
            framework_id: The framework identifier

        Returns:
            QuerySet of MEMBER_MAPPING_ITEM objects
        """
        from pybirdai.models.bird_meta_data_model import MEMBER_MAPPING_ITEM

        # Get member_mappings for the framework
        member_mappings = FrameworkSubgraphFetcher.get_member_mappings_for_framework(framework_id)

        return MEMBER_MAPPING_ITEM.objects.filter(member_mapping_id__in=member_mappings)

    @staticmethod
    def get_mapping_ordinate_links_for_framework(framework_id: str) -> QuerySet:
        """
        Get all MAPPING_ORDINATE_LINK records for a specific framework.

        Filters based on MAPPING_DEFINITION relationship.

        Args:
            framework_id: The framework identifier

        Returns:
            QuerySet of MAPPING_ORDINATE_LINK objects
        """
        from pybirdai.models.bird_meta_data_model import MAPPING_ORDINATE_LINK

        # Get mapping_definitions for the framework
        mapping_definitions = FrameworkSubgraphFetcher.get_mapping_definitions_for_framework(framework_id)

        return MAPPING_ORDINATE_LINK.objects.filter(mapping_definition_id__in=mapping_definitions)

    # ==================== Junction Table Fetchers ====================

    @staticmethod
    def get_framework_tables_for_framework(framework_id: str) -> QuerySet:
        """
        Get all FRAMEWORK_TABLE junction records for a specific framework.

        Args:
            framework_id: The framework identifier

        Returns:
            QuerySet of FRAMEWORK_TABLE objects
        """
        from pybirdai.models.bird_meta_data_model import FRAMEWORK_TABLE
        return FRAMEWORK_TABLE.objects.filter(framework_id=framework_id)

    @staticmethod
    def get_framework_hierarchies_for_framework(framework_id: str) -> QuerySet:
        """
        Get all FRAMEWORK_HIERARCHY junction records for a specific framework.

        Args:
            framework_id: The framework identifier

        Returns:
            QuerySet of FRAMEWORK_HIERARCHY objects
        """
        from pybirdai.models.bird_meta_data_model import FRAMEWORK_HIERARCHY
        return FRAMEWORK_HIERARCHY.objects.filter(framework_id=framework_id)

    @staticmethod
    def get_framework_subdomains_for_framework(framework_id: str) -> QuerySet:
        """
        Get all FRAMEWORK_SUBDOMAIN junction records for a specific framework.

        Args:
            framework_id: The framework identifier

        Returns:
            QuerySet of FRAMEWORK_SUBDOMAIN objects
        """
        from pybirdai.models.bird_meta_data_model import FRAMEWORK_SUBDOMAIN
        return FRAMEWORK_SUBDOMAIN.objects.filter(framework_id=framework_id)

    @staticmethod
    def get_facet_enumerations_for_framework(framework_id: str) -> QuerySet:
        """
        Get all FACET_ENUMERATIONs for a specific framework.

        Traverses: FACET_COLLECTION → FACET_ENUMERATION

        Args:
            framework_id: The framework identifier

        Returns:
            QuerySet of FACET_ENUMERATION objects
        """
        from pybirdai.models.bird_meta_data_model import FACET_ENUMERATION

        facet_collections = FrameworkSubgraphFetcher.get_facet_collections_for_framework(framework_id)
        facet_ids = facet_collections.values_list('facet_id', flat=True).distinct()
        return FACET_ENUMERATION.objects.filter(facet_id__in=facet_ids)

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
