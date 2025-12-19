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
#
"""
Framework Selection Service

Provides a unified service for framework-specific data selection based on
SMCubes category whitelisting. Used by both export and deletion features.

Framework Types:
- Dataset frameworks (ANCRDT): Core + Data Definition + Transformation
- Reporting frameworks (FINREP, COREP, etc.): Core + Data Definition + Rendering + Mapping
"""

import logging

logger = logging.getLogger(__name__)

# Cache for maintenance agency objects
_maintenance_agency_cache = {}


def validate_framework_selection(framework_ids: list) -> tuple:
    """
    Validate framework selection for export.

    Allowed combinations:
    - Single non-reference framework (e.g., EBA_FINREP, EBA_COREP, ANCRDT)
    - Single reference framework (e.g., FINREP_REF, COREP_REF)
    - Paired non-ref + ref framework (e.g., EBA_FINREP + FINREP_REF)

    Pairing rule: EBA_X pairs with X_REF (e.g., EBA_FINREP + FINREP_REF)

    All other multi-framework combinations are disallowed.

    Args:
        framework_ids: List of framework IDs selected for export

    Returns:
        Tuple of (is_valid: bool, error_message: str)
    """
    if not framework_ids:
        return True, ""

    # Filter out empty strings
    framework_ids = [fid for fid in framework_ids if fid]

    if len(framework_ids) == 0:
        return True, ""

    if len(framework_ids) == 1:
        # Single framework is always valid
        return True, ""

    if len(framework_ids) == 2:
        # Check if it's a valid pair (EBA_X + X_REF)
        fid1, fid2 = framework_ids

        # Identify which is reference and which is non-reference
        if fid1.endswith('_REF'):
            ref_framework = fid1
            non_ref_framework = fid2
        elif fid2.endswith('_REF'):
            ref_framework = fid2
            non_ref_framework = fid1
        else:
            # Neither is a reference framework - invalid combination
            return False, f"Invalid combination: {fid1} and {fid2}. Multi-framework export is only allowed for a framework and its reference version (e.g., EBA_FINREP + FINREP_REF)."

        # Extract base names for comparison
        # EBA_FINREP -> FINREP, FINREP_REF -> FINREP
        ref_base = ref_framework[:-4]  # Remove '_REF'
        if non_ref_framework.startswith('EBA_'):
            non_ref_base = non_ref_framework[4:]  # Remove 'EBA_'
        else:
            non_ref_base = non_ref_framework

        # Check if base names match
        if ref_base != non_ref_base:
            return False, f"Invalid combination: {non_ref_framework} and {ref_framework}. Only paired frameworks are allowed (e.g., EBA_{ref_base} + {ref_base}_REF)."

        return True, ""

    # More than 2 frameworks - always invalid
    return False, f"Too many frameworks selected ({len(framework_ids)}). Maximum 2 frameworks allowed (a framework and its reference version)."


def is_reference_framework(framework_id: str) -> bool:
    """
    Check if a framework is a reference framework.

    Args:
        framework_id: The framework ID

    Returns:
        True if the framework is a reference framework (ends with _REF)
    """
    return framework_id and framework_id.endswith('_REF')


def get_base_framework(framework_id: str) -> str:
    """
    Get the base framework ID (without _REF suffix).

    Args:
        framework_id: The framework ID (e.g., 'FINREP_REF' or 'FINREP')

    Returns:
        Base framework ID without _REF suffix
    """
    if framework_id and framework_id.endswith('_REF'):
        return framework_id[:-4]
    return framework_id


def get_or_create_maintenance_agency_for_framework(framework_id: str):
    """
    Get or create the appropriate MAINTENANCE_AGENCY for a framework.

    Uses memoization to avoid repeated database queries.

    Rules:
    - EBA_* frameworks (EBA_FINREP, EBA_AE, etc.) -> EBA
    - *_REF frameworks (FINREP_REF, COREP_REF, etc.) -> ECB
    - Default (BIRD, EFBT, etc.) -> EFBT

    Args:
        framework_id: The framework identifier

    Returns:
        MAINTENANCE_AGENCY object
    """
    from pybirdai.models.bird_meta_data_model import MAINTENANCE_AGENCY

    # Determine agency_id based on framework pattern
    if framework_id and framework_id.startswith('EBA_'):
        agency_id = 'EBA'
    elif framework_id and framework_id.endswith('_REF'):
        agency_id = 'ECB'
    else:
        agency_id = 'EFBT'

    # Check cache first
    if agency_id not in _maintenance_agency_cache:
        agency, _ = MAINTENANCE_AGENCY.objects.get_or_create(
            maintenance_agency_id=agency_id,
            defaults={'name': agency_id, 'code': agency_id}
        )
        _maintenance_agency_cache[agency_id] = agency

    return _maintenance_agency_cache[agency_id]


class FrameworkSelectionService:
    """
    Service for determining which SMCubes tables belong to a framework type.

    Uses whitelisting by SMCubes category to ensure proper data isolation.
    """

    # SMCubes Categories and their tables (from SMCubes specification)
    # Table names are Django model names (uppercase), will be converted to db_table format

    CORE_TABLES = {
        'MAINTENANCE_AGENCY',
        'FRAMEWORK',
        'DOMAIN',
        'FACET_COLLECTION',
        'FACET_ENUMERATION',
        'MEMBER',
        'MEMBER_HIERARCHY',
        'MEMBER_HIERARCHY_NODE',
        'SUBDOMAIN',
        'SUBDOMAIN_ENUMERATION',
        'VARIABLE',
        'VARIABLE_SET',
        'VARIABLE_SET_ENUMERATION',
    }

    DATA_DEFINITION_TABLES = {
        'COMBINATION',
        'COMBINATION_ITEM',
        'MEMBER_LINK',
        'CUBE',
        'CUBE_LINK',
        'CUBE_GROUP',
        'CUBE_GROUP_ENUMERATION',
        'CUBE_HIERARCHY',
        'CUBE_HIERARCHY_NODE',
        'CUBE_RELATIONSHIP',
        'CUBE_STRUCTURE',
        'CUBE_STRUCTURE_ITEM_LINK',
        'CUBE_STRUCTURE_ITEM',
        'CUBE_TO_COMBINATION',
        # Framework extension tables (application-specific, not SMCubes standard)
        'FRAMEWORK_HIERARCHY',
        'FRAMEWORK_SUBDOMAIN',
        'FRAMEWORK_VARIABLE_SET',
        'FRAMEWORK_TABLE',
    }

    MAPPING_TABLES = {
        'CUBE_MAPPING',
        'COMBINATION_MAPPING',
        'MAPPING_DEFINITION',
        'MAPPING_TO_CUBE',
        'MEMBER_MAPPING',
        'MEMBER_MAPPING_ITEM',
        'VARIABLE_MAPPING',
        'VARIABLE_MAPPING_ITEM',
        'VARIABLE_SET_MAPPING',
        'CUBE_STRUCTURE_MAPPING',
        'CUBE_STRUCTURE_MAPPING_ITEM',
    }

    RENDERING_TABLES = {
        'AXIS',
        'AXIS_ORDINATE',
        'CELL_POSITION',
        'ORDINATE_ITEM',
        'TABLE',
        'TABLE_CELL',
        'CUBE_TO_TABLE',
    }

    TRANSFORMATION_TABLES = {
        'TRANSFORMATION',
        'TRANSFORMATION_NODE',
        'TRANSFORMATION_SCHEME',
        'SEMANTIC_TRANSFORMATION_RULE',
        'TRANSFORMATION_TO_CUBE',
        'TRANSFORMATION_TO_VARIABLE',
        'LOGICAL_TRANSFORMATION_RULE',
    }

    LEGAL_REFERENCE_TABLES = {
        'LEGAL_REFERENCE',
        'LEGAL_TEXT',
        'CLASSIFICATION',
        'CLASSIFICATION_ASSIGNMENT',
    }

    # Dataset frameworks - no rendering package
    DATASET_FRAMEWORKS = {
        'ANCRDT',
    }

    # Reporting frameworks - include rendering + mapping
    REPORTING_FRAMEWORKS = {
        'FINREP',
        'FINREP_REF',
        'COREP',
        'COREP_REF',
        'AE',
        'AE_REF',
        'FP',
        'FP_REF',
        'SBP',
        'SBP_REF',
        'PILLAR3',
        'PILLAR3_REF',
        'DPM',
        'DPM_REF',
    }

    @classmethod
    def _normalize_framework_id(cls, framework_id: str) -> str:
        """
        Normalize framework ID by removing _REF suffix for type lookup.

        Args:
            framework_id: The framework ID (e.g., 'FINREP_REF' or 'FINREP')

        Returns:
            Normalized framework ID without _REF suffix
        """
        if framework_id and framework_id.endswith('_REF'):
            return framework_id[:-4]  # Remove '_REF'
        return framework_id

    @classmethod
    def is_dataset_framework(cls, framework_id: str) -> bool:
        """
        Check if a framework is a dataset framework (no rendering package).

        Args:
            framework_id: The framework ID (e.g., 'ANCRDT')

        Returns:
            True if dataset framework, False otherwise
        """
        normalized = cls._normalize_framework_id(framework_id)
        return normalized in cls.DATASET_FRAMEWORKS

    @classmethod
    def is_reporting_framework(cls, framework_id: str) -> bool:
        """
        Check if a framework is a reporting framework (includes rendering).

        Args:
            framework_id: The framework ID (e.g., 'FINREP_REF')

        Returns:
            True if reporting framework, False otherwise
        """
        normalized = cls._normalize_framework_id(framework_id)
        # Check both normalized and original in reporting frameworks
        return (
            normalized in cls.REPORTING_FRAMEWORKS or
            framework_id in cls.REPORTING_FRAMEWORKS
        )

    @classmethod
    def get_allowed_tables_for_framework(cls, framework_id: str) -> set:
        """
        Get the set of allowed table names for a framework.

        Args:
            framework_id: The framework ID

        Returns:
            Set of allowed table names (uppercase model names)
        """
        allowed = set()

        # All frameworks get Core + Data Definition
        allowed.update(cls.CORE_TABLES)
        allowed.update(cls.DATA_DEFINITION_TABLES)

        if cls.is_dataset_framework(framework_id):
            # Dataset frameworks: Core + Data Definition + Transformation
            allowed.update(cls.TRANSFORMATION_TABLES)
            logger.debug(f"Framework {framework_id} is a dataset framework - excluding Rendering and Mapping")
        else:
            # Reporting frameworks: Core + Data Definition + Rendering + Mapping
            allowed.update(cls.RENDERING_TABLES)
            allowed.update(cls.MAPPING_TABLES)
            logger.debug(f"Framework {framework_id} is a reporting framework - including Rendering and Mapping")

        return allowed

    @classmethod
    def get_excluded_tables_for_framework(cls, framework_id: str) -> set:
        """
        Get the set of excluded table names for a framework.

        Args:
            framework_id: The framework ID

        Returns:
            Set of excluded table names (uppercase model names)
        """
        excluded = set()

        if cls.is_dataset_framework(framework_id):
            # Dataset frameworks exclude Rendering and Mapping
            excluded.update(cls.RENDERING_TABLES)
            excluded.update(cls.MAPPING_TABLES)
        else:
            # Reporting frameworks exclude Transformation (for now)
            excluded.update(cls.TRANSFORMATION_TABLES)

        # Both exclude Legal Reference (rarely used)
        excluded.update(cls.LEGAL_REFERENCE_TABLES)

        return excluded

    @classmethod
    def should_include_table(cls, framework_id: str, table_name: str) -> bool:
        """
        Check if a table should be included for a given framework.

        Args:
            framework_id: The framework ID
            table_name: The table name (can be db_table format like 'pybirdai_cube'
                       or model name like 'CUBE')

        Returns:
            True if table should be included, False otherwise
        """
        # Normalize table name: remove 'pybirdai_' prefix and uppercase
        normalized_table = table_name.upper()
        if normalized_table.startswith('PYBIRDAI_'):
            normalized_table = normalized_table[9:]  # Remove 'PYBIRDAI_'

        allowed = cls.get_allowed_tables_for_framework(framework_id)
        return normalized_table in allowed

    @classmethod
    def get_all_tables(cls) -> set:
        """
        Get all known SMCubes tables.

        Returns:
            Set of all table names
        """
        all_tables = set()
        all_tables.update(cls.CORE_TABLES)
        all_tables.update(cls.DATA_DEFINITION_TABLES)
        all_tables.update(cls.MAPPING_TABLES)
        all_tables.update(cls.RENDERING_TABLES)
        all_tables.update(cls.TRANSFORMATION_TABLES)
        all_tables.update(cls.LEGAL_REFERENCE_TABLES)
        return all_tables

    # ==================== Model-to-Fetcher Registry ====================

    @classmethod
    def get_fetcher_for_model(cls, model_name: str):
        """
        Get the FrameworkSubgraphFetcher method for a given model name.

        Args:
            model_name: The model name (e.g., 'CUBE', 'MEMBER', 'VARIABLE')

        Returns:
            The fetcher method, or None if no specific fetcher exists
        """
        from pybirdai.views.core.framework_filters import FrameworkSubgraphFetcher

        # Map model names to fetcher methods
        MODEL_TO_FETCHER = {
            # Core
            'FRAMEWORK': FrameworkSubgraphFetcher.get_framework_for_framework,
            'MAINTENANCE_AGENCY': FrameworkSubgraphFetcher.get_maintenance_agencies_for_framework,
            'DOMAIN': FrameworkSubgraphFetcher.get_domains_for_framework,
            'FACET_COLLECTION': FrameworkSubgraphFetcher.get_facet_collections_for_framework,
            'MEMBER': FrameworkSubgraphFetcher.get_members_for_framework,
            'MEMBER_HIERARCHY': FrameworkSubgraphFetcher.get_hierarchies_for_framework,
            'MEMBER_HIERARCHY_NODE': FrameworkSubgraphFetcher.get_member_hierarchy_nodes_for_framework,
            'SUBDOMAIN': FrameworkSubgraphFetcher.get_subdomains_for_framework,
            'SUBDOMAIN_ENUMERATION': FrameworkSubgraphFetcher.get_subdomain_enumerations_for_framework,
            'VARIABLE': FrameworkSubgraphFetcher.get_variables_for_framework,
            'VARIABLE_SET': FrameworkSubgraphFetcher.get_variable_sets_for_framework,
            'VARIABLE_SET_ENUMERATION': FrameworkSubgraphFetcher.get_variable_set_enumerations_for_framework,

            # Data Definition
            'CUBE': FrameworkSubgraphFetcher.get_cubes_for_framework,
            'CUBE_STRUCTURE': FrameworkSubgraphFetcher.get_cube_structures_for_framework,
            'CUBE_STRUCTURE_ITEM': FrameworkSubgraphFetcher.get_cube_structure_items_for_framework,
            'CUBE_LINK': FrameworkSubgraphFetcher.get_cube_links_for_framework,
            'CUBE_STRUCTURE_ITEM_LINK': FrameworkSubgraphFetcher.get_cube_structure_item_links_for_framework,
            'MEMBER_LINK': FrameworkSubgraphFetcher.get_member_links_for_framework,
            'COMBINATION': FrameworkSubgraphFetcher.get_combinations_for_framework,
            'COMBINATION_ITEM': FrameworkSubgraphFetcher.get_combination_items_for_framework,
            'CUBE_TO_COMBINATION': FrameworkSubgraphFetcher.get_cube_to_combinations_for_framework,

            # Rendering (for reporting frameworks)
            'TABLE': FrameworkSubgraphFetcher.get_tables_for_framework,
            'AXIS': FrameworkSubgraphFetcher.get_axes_for_framework,
            'AXIS_ORDINATE': FrameworkSubgraphFetcher.get_axis_ordinates_for_framework,
            'TABLE_CELL': FrameworkSubgraphFetcher.get_table_cells_for_framework,
            'ORDINATE_ITEM': FrameworkSubgraphFetcher.get_ordinate_items_for_framework,
            'CELL_POSITION': FrameworkSubgraphFetcher.get_cell_positions_for_framework,
            'CUBE_TO_TABLE': FrameworkSubgraphFetcher.get_cube_to_tables_for_framework,

            # Mapping (for reference output layer generation)
            'MAPPING_DEFINITION': FrameworkSubgraphFetcher.get_mapping_definitions_for_framework,
            'MAPPING_TO_CUBE': FrameworkSubgraphFetcher.get_mapping_to_cubes_for_framework,
            'VARIABLE_MAPPING': FrameworkSubgraphFetcher.get_variable_mappings_for_framework,
            'VARIABLE_MAPPING_ITEM': FrameworkSubgraphFetcher.get_variable_mapping_items_for_framework,
            'MEMBER_MAPPING': FrameworkSubgraphFetcher.get_member_mappings_for_framework,
            'MEMBER_MAPPING_ITEM': FrameworkSubgraphFetcher.get_member_mapping_items_for_framework,

            # Junction tables (filter by framework_id directly)
            'FRAMEWORK_TABLE': FrameworkSubgraphFetcher.get_framework_tables_for_framework,
            'FRAMEWORK_HIERARCHY': FrameworkSubgraphFetcher.get_framework_hierarchies_for_framework,
            'FRAMEWORK_SUBDOMAIN': FrameworkSubgraphFetcher.get_framework_subdomains_for_framework,

            # Core - additional
            'FACET_ENUMERATION': FrameworkSubgraphFetcher.get_facet_enumerations_for_framework,
        }

        return MODEL_TO_FETCHER.get(model_name.upper())

    @classmethod
    def get_filtered_ids_for_model(cls, model_name: str, framework_id: str, pk_field: str = None) -> set:
        """
        Get the set of primary key IDs for a model filtered by framework.

        Args:
            model_name: The model name (e.g., 'CUBE', 'MEMBER')
            framework_id: The framework ID
            pk_field: The primary key field name (auto-detected if not provided)

        Returns:
            Set of primary key IDs that belong to the framework subgraph,
            or None if no specific filter is available (export all)
        """
        fetcher = cls.get_fetcher_for_model(model_name)
        if fetcher is None:
            # No specific fetcher - can't filter by framework
            logger.debug(f"No fetcher for model {model_name} - will export all records")
            return None

        try:
            queryset = fetcher(framework_id)
            if queryset is None:
                return None

            # Determine the primary key field
            if pk_field is None:
                # Try to get from model meta
                model = queryset.model
                pk_field = model._meta.pk.name

            # Get the IDs
            ids = set(queryset.values_list(pk_field, flat=True).distinct())
            logger.debug(f"Model {model_name}: found {len(ids)} IDs for framework {framework_id}")
            return ids
        except Exception as e:
            logger.warning(f"Error fetching IDs for model {model_name}: {e}")
            return None

    @classmethod
    def get_filtered_queryset_for_model(cls, model_name: str, framework_id: str):
        """
        Get the filtered QuerySet for a model by framework.

        Args:
            model_name: The model name
            framework_id: The framework ID

        Returns:
            QuerySet filtered by framework, or None if no filter available
        """
        fetcher = cls.get_fetcher_for_model(model_name)
        if fetcher is None:
            return None

        try:
            return fetcher(framework_id)
        except Exception as e:
            logger.warning(f"Error getting queryset for model {model_name}: {e}")
            return None
