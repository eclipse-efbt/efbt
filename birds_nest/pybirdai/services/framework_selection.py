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

Also includes validation and change detection for linked artifacts:
- CUBE_LINK
- CUBE_STRUCTURE_ITEM_LINK
- MEMBER_LINK
"""

import csv
import logging
import os
from dataclasses import dataclass, field
from typing import Optional

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
            'MAPPING_ORDINATE_LINK': FrameworkSubgraphFetcher.get_mapping_ordinate_links_for_framework,
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


# ==================== Validation Data Classes ====================


@dataclass
class ValidationResult:
    """Result of validating a single artifact."""
    is_valid: bool
    artifact_id: str
    artifact_type: str
    errors: list = field(default_factory=list)
    warnings: list = field(default_factory=list)

    def add_error(self, message: str):
        """Add an error message."""
        self.errors.append(message)
        self.is_valid = False

    def add_warning(self, message: str):
        """Add a warning message."""
        self.warnings.append(message)


@dataclass
class ValidationReport:
    """Aggregate validation report for multiple artifacts."""
    total_checked: int = 0
    total_valid: int = 0
    total_invalid: int = 0
    results: list = field(default_factory=list)

    @property
    def all_valid(self) -> bool:
        return self.total_invalid == 0

    def add_result(self, result: ValidationResult):
        """Add a validation result to the report."""
        self.results.append(result)
        self.total_checked += 1
        if result.is_valid:
            self.total_valid += 1
        else:
            self.total_invalid += 1

    def get_invalid_results(self) -> list:
        """Get only the invalid results."""
        return [r for r in self.results if not r.is_valid]

    def get_summary(self) -> dict:
        """Get a summary of the validation report."""
        return {
            'total_checked': self.total_checked,
            'total_valid': self.total_valid,
            'total_invalid': self.total_invalid,
            'all_valid': self.all_valid,
            'invalid_artifacts': [
                {
                    'id': r.artifact_id,
                    'type': r.artifact_type,
                    'errors': r.errors,
                }
                for r in self.get_invalid_results()
            ]
        }


@dataclass
class ChangeReport:
    """Report of changes between CSV files and database state."""
    artifact_type: str
    new_artifacts: list = field(default_factory=list)
    modified_artifacts: list = field(default_factory=list)
    deleted_artifacts: list = field(default_factory=list)

    @property
    def has_changes(self) -> bool:
        return bool(self.new_artifacts or self.modified_artifacts or self.deleted_artifacts)

    def get_summary(self) -> dict:
        return {
            'artifact_type': self.artifact_type,
            'new_count': len(self.new_artifacts),
            'modified_count': len(self.modified_artifacts),
            'deleted_count': len(self.deleted_artifacts),
            'has_changes': self.has_changes,
        }


@dataclass
class AggregateChangeReport:
    """Aggregate change report for all linked artifact types."""
    cube_link_changes: Optional[ChangeReport] = None
    cube_structure_item_link_changes: Optional[ChangeReport] = None
    member_link_changes: Optional[ChangeReport] = None
    validation_report: Optional[ValidationReport] = None

    @property
    def has_changes(self) -> bool:
        return any([
            self.cube_link_changes and self.cube_link_changes.has_changes,
            self.cube_structure_item_link_changes and self.cube_structure_item_link_changes.has_changes,
            self.member_link_changes and self.member_link_changes.has_changes,
        ])

    def get_summary(self) -> dict:
        return {
            'has_changes': self.has_changes,
            'cube_link': self.cube_link_changes.get_summary() if self.cube_link_changes else None,
            'cube_structure_item_link': self.cube_structure_item_link_changes.get_summary() if self.cube_structure_item_link_changes else None,
            'member_link': self.member_link_changes.get_summary() if self.member_link_changes else None,
            'validation': self.validation_report.get_summary() if self.validation_report else None,
        }


# ==================== Linked Artifact Validation ====================


class LinkedArtifactValidator:
    """
    Validates linked artifacts (CUBE_LINK, CUBE_STRUCTURE_ITEM_LINK, MEMBER_LINK)
    to ensure their FK references can be resolved within the framework subgraph.
    """

    @staticmethod
    def validate_cube_link(cube_link, framework_id: str = None) -> ValidationResult:
        """
        Validate a CUBE_LINK artifact.

        Checks:
        - primary_cube_id exists
        - foreign_cube_id exists
        - If framework_id provided, both cubes are in the framework subgraph

        Args:
            cube_link: CUBE_LINK model instance or dict with keys
            framework_id: Optional framework ID for subgraph validation

        Returns:
            ValidationResult with validation status and any errors
        """
        from pybirdai.models.bird_meta_data_model import CUBE

        # Handle both model instance and dict
        if hasattr(cube_link, 'cube_link_id'):
            link_id = cube_link.cube_link_id
            primary_cube_id = getattr(cube_link.primary_cube_id, 'cube_id', None) if cube_link.primary_cube_id else None
            foreign_cube_id = getattr(cube_link.foreign_cube_id, 'cube_id', None) if cube_link.foreign_cube_id else None
        else:
            link_id = cube_link.get('cube_link_id', 'unknown')
            primary_cube_id = cube_link.get('primary_cube_id')
            foreign_cube_id = cube_link.get('foreign_cube_id')

        result = ValidationResult(
            is_valid=True,
            artifact_id=link_id,
            artifact_type='CUBE_LINK'
        )

        # Check primary_cube_id
        if not primary_cube_id:
            result.add_error("primary_cube_id is missing or null")
        elif not CUBE.objects.filter(cube_id=primary_cube_id).exists():
            result.add_error(f"primary_cube_id '{primary_cube_id}' does not exist")

        # Check foreign_cube_id
        if not foreign_cube_id:
            result.add_error("foreign_cube_id is missing or null")
        elif not CUBE.objects.filter(cube_id=foreign_cube_id).exists():
            result.add_error(f"foreign_cube_id '{foreign_cube_id}' does not exist")

        # Framework subgraph validation
        if framework_id and result.is_valid:
            from pybirdai.views.core.framework_filters import FrameworkSubgraphFetcher
            cube_ids = set(
                FrameworkSubgraphFetcher.get_cubes_for_framework(framework_id)
                .values_list('cube_id', flat=True)
            )
            if primary_cube_id not in cube_ids:
                result.add_warning(f"primary_cube_id '{primary_cube_id}' not in framework {framework_id} subgraph")
            if foreign_cube_id not in cube_ids:
                result.add_warning(f"foreign_cube_id '{foreign_cube_id}' not in framework {framework_id} subgraph")

        return result

    @staticmethod
    def validate_cube_structure_item_link(csil, framework_id: str = None) -> ValidationResult:
        """
        Validate a CUBE_STRUCTURE_ITEM_LINK artifact.

        Checks:
        - cube_link_id exists
        - primary_cube_variable_code (CSI) exists
        - foreign_cube_variable_code (CSI) exists
        - If framework_id provided, all references are in the framework subgraph

        Args:
            csil: CUBE_STRUCTURE_ITEM_LINK model instance or dict
            framework_id: Optional framework ID for subgraph validation

        Returns:
            ValidationResult with validation status and any errors
        """
        from pybirdai.models.bird_meta_data_model import CUBE_LINK, CUBE_STRUCTURE_ITEM

        # Handle both model instance and dict
        if hasattr(csil, 'cube_structure_item_link_id'):
            link_id = csil.cube_structure_item_link_id
            cube_link_id = getattr(csil.cube_link_id, 'cube_link_id', None) if csil.cube_link_id else None
            primary_csi = getattr(csil.primary_cube_variable_code, 'cube_structure_item_id', None) if csil.primary_cube_variable_code else None
            foreign_csi = getattr(csil.foreign_cube_variable_code, 'cube_structure_item_id', None) if csil.foreign_cube_variable_code else None
        else:
            link_id = csil.get('cube_structure_item_link_id', 'unknown')
            cube_link_id = csil.get('cube_link_id')
            primary_csi = csil.get('primary_cube_variable_code')
            foreign_csi = csil.get('foreign_cube_variable_code')

        result = ValidationResult(
            is_valid=True,
            artifact_id=link_id,
            artifact_type='CUBE_STRUCTURE_ITEM_LINK'
        )

        # Check cube_link_id
        if not cube_link_id:
            result.add_error("cube_link_id is missing or null")
        elif not CUBE_LINK.objects.filter(cube_link_id=cube_link_id).exists():
            result.add_error(f"cube_link_id '{cube_link_id}' does not exist")

        # Check primary_cube_variable_code
        if not primary_csi:
            result.add_error("primary_cube_variable_code is missing or null")
        elif not CUBE_STRUCTURE_ITEM.objects.filter(cube_structure_item_id=primary_csi).exists():
            result.add_error(f"primary_cube_variable_code '{primary_csi}' does not exist")

        # Check foreign_cube_variable_code
        if not foreign_csi:
            result.add_error("foreign_cube_variable_code is missing or null")
        elif not CUBE_STRUCTURE_ITEM.objects.filter(cube_structure_item_id=foreign_csi).exists():
            result.add_error(f"foreign_cube_variable_code '{foreign_csi}' does not exist")

        # Framework subgraph validation
        if framework_id and result.is_valid:
            from pybirdai.views.core.framework_filters import FrameworkSubgraphFetcher
            csi_ids = set(
                FrameworkSubgraphFetcher.get_cube_structure_items_for_framework(framework_id)
                .values_list('cube_structure_item_id', flat=True)
            )
            if primary_csi not in csi_ids:
                result.add_warning(f"primary_cube_variable_code '{primary_csi}' not in framework {framework_id} subgraph")
            if foreign_csi not in csi_ids:
                result.add_warning(f"foreign_cube_variable_code '{foreign_csi}' not in framework {framework_id} subgraph")

        return result

    @staticmethod
    def validate_member_link(member_link, framework_id: str = None) -> ValidationResult:
        """
        Validate a MEMBER_LINK artifact.

        Checks:
        - cube_structure_item_link_id exists
        - primary_member_id exists
        - foreign_member_id exists
        - If framework_id provided, all references are in the framework subgraph

        Args:
            member_link: MEMBER_LINK model instance or dict
            framework_id: Optional framework ID for subgraph validation

        Returns:
            ValidationResult with validation status and any errors
        """
        from pybirdai.models.bird_meta_data_model import CUBE_STRUCTURE_ITEM_LINK, MEMBER

        # Handle both model instance and dict
        if hasattr(member_link, 'id'):
            link_id = str(member_link.id)
            csil_id = getattr(member_link.cube_structure_item_link_id, 'cube_structure_item_link_id', None) if member_link.cube_structure_item_link_id else None
            primary_member = getattr(member_link.primary_member_id, 'member_id', None) if member_link.primary_member_id else None
            foreign_member = getattr(member_link.foreign_member_id, 'member_id', None) if member_link.foreign_member_id else None
        else:
            link_id = str(member_link.get('id', 'unknown'))
            csil_id = member_link.get('cube_structure_item_link_id')
            primary_member = member_link.get('primary_member_id')
            foreign_member = member_link.get('foreign_member_id')

        result = ValidationResult(
            is_valid=True,
            artifact_id=link_id,
            artifact_type='MEMBER_LINK'
        )

        # Check cube_structure_item_link_id
        if not csil_id:
            result.add_error("cube_structure_item_link_id is missing or null")
        elif not CUBE_STRUCTURE_ITEM_LINK.objects.filter(cube_structure_item_link_id=csil_id).exists():
            result.add_error(f"cube_structure_item_link_id '{csil_id}' does not exist")

        # Check primary_member_id
        if not primary_member:
            result.add_error("primary_member_id is missing or null")
        elif not MEMBER.objects.filter(member_id=primary_member).exists():
            result.add_error(f"primary_member_id '{primary_member}' does not exist")

        # Check foreign_member_id
        if not foreign_member:
            result.add_error("foreign_member_id is missing or null")
        elif not MEMBER.objects.filter(member_id=foreign_member).exists():
            result.add_error(f"foreign_member_id '{foreign_member}' does not exist")

        # Framework subgraph validation
        if framework_id and result.is_valid:
            from pybirdai.views.core.framework_filters import FrameworkSubgraphFetcher
            member_ids = set(
                FrameworkSubgraphFetcher.get_members_for_framework(framework_id)
                .values_list('member_id', flat=True)
            )
            if primary_member not in member_ids:
                result.add_warning(f"primary_member_id '{primary_member}' not in framework {framework_id} subgraph")
            if foreign_member not in member_ids:
                result.add_warning(f"foreign_member_id '{foreign_member}' not in framework {framework_id} subgraph")

        return result

    @classmethod
    def validate_all_linked_artifacts(cls, framework_id: str = None) -> ValidationReport:
        """
        Validate all linked artifacts in the database.

        Args:
            framework_id: Optional framework ID for subgraph validation

        Returns:
            ValidationReport with all validation results
        """
        from pybirdai.models.bird_meta_data_model import (
            CUBE_LINK, CUBE_STRUCTURE_ITEM_LINK, MEMBER_LINK
        )

        report = ValidationReport()

        # Validate CUBE_LINKs
        for cube_link in CUBE_LINK.objects.all():
            result = cls.validate_cube_link(cube_link, framework_id)
            report.add_result(result)

        # Validate CUBE_STRUCTURE_ITEM_LINKs
        for csil in CUBE_STRUCTURE_ITEM_LINK.objects.all():
            result = cls.validate_cube_structure_item_link(csil, framework_id)
            report.add_result(result)

        # Validate MEMBER_LINKs
        for member_link in MEMBER_LINK.objects.all():
            result = cls.validate_member_link(member_link, framework_id)
            report.add_result(result)

        return report

    @classmethod
    def validate_linked_artifacts_for_framework(cls, framework_id: str) -> ValidationReport:
        """
        Validate only the linked artifacts within a framework's subgraph.

        Args:
            framework_id: The framework ID

        Returns:
            ValidationReport with validation results for framework-specific artifacts
        """
        from pybirdai.views.core.framework_filters import FrameworkSubgraphFetcher

        report = ValidationReport()

        # Validate CUBE_LINKs in framework
        cube_links = FrameworkSubgraphFetcher.get_cube_links_for_framework(framework_id)
        for cube_link in cube_links:
            result = cls.validate_cube_link(cube_link, framework_id)
            report.add_result(result)

        # Validate CUBE_STRUCTURE_ITEM_LINKs in framework
        csils = FrameworkSubgraphFetcher.get_cube_structure_item_links_for_framework(framework_id)
        for csil in csils:
            result = cls.validate_cube_structure_item_link(csil, framework_id)
            report.add_result(result)

        # Validate MEMBER_LINKs in framework
        member_links = FrameworkSubgraphFetcher.get_member_links_for_framework(framework_id)
        for member_link in member_links:
            result = cls.validate_member_link(member_link, framework_id)
            report.add_result(result)

        return report


# ==================== Change Detection ====================


class LinkedArtifactChangeDetector:
    """
    Detects changes between CSV files and database state for linked artifacts.
    Used before PR creation to show what will be pushed.
    """

    @staticmethod
    def _parse_csv_to_dict(csv_path: str, id_field: str) -> dict:
        """
        Parse a CSV file into a dict keyed by the ID field.

        Args:
            csv_path: Path to the CSV file
            id_field: The field name to use as the key

        Returns:
            Dict mapping ID -> row dict
        """
        if not os.path.exists(csv_path):
            return {}

        result = {}
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                row_id = row.get(id_field)
                if row_id:
                    result[row_id] = row
        return result

    @staticmethod
    def _get_db_cube_links_as_dict(framework_id: str = None) -> dict:
        """Get CUBE_LINKs from database as a dict keyed by cube_link_id."""
        from pybirdai.models.bird_meta_data_model import CUBE_LINK
        from pybirdai.views.core.framework_filters import FrameworkSubgraphFetcher

        if framework_id:
            queryset = FrameworkSubgraphFetcher.get_cube_links_for_framework(framework_id)
        else:
            queryset = CUBE_LINK.objects.all()

        result = {}
        for cl in queryset:
            result[cl.cube_link_id] = {
                'cube_link_id': cl.cube_link_id,
                'primary_cube_id': cl.primary_cube_id.cube_id if cl.primary_cube_id else None,
                'foreign_cube_id': cl.foreign_cube_id.cube_id if cl.foreign_cube_id else None,
                'name': cl.name,
                'code': cl.code,
            }
        return result

    @staticmethod
    def _get_db_cube_structure_item_links_as_dict(framework_id: str = None) -> dict:
        """Get CUBE_STRUCTURE_ITEM_LINKs from database as a dict."""
        from pybirdai.models.bird_meta_data_model import CUBE_STRUCTURE_ITEM_LINK
        from pybirdai.views.core.framework_filters import FrameworkSubgraphFetcher

        if framework_id:
            queryset = FrameworkSubgraphFetcher.get_cube_structure_item_links_for_framework(framework_id)
        else:
            queryset = CUBE_STRUCTURE_ITEM_LINK.objects.all()

        result = {}
        for csil in queryset:
            result[csil.cube_structure_item_link_id] = {
                'cube_structure_item_link_id': csil.cube_structure_item_link_id,
                'cube_link_id': csil.cube_link_id.cube_link_id if csil.cube_link_id else None,
                'primary_cube_variable_code': csil.primary_cube_variable_code.cube_structure_item_id if csil.primary_cube_variable_code else None,
                'foreign_cube_variable_code': csil.foreign_cube_variable_code.cube_structure_item_id if csil.foreign_cube_variable_code else None,
            }
        return result

    @staticmethod
    def _get_db_member_links_as_dict(framework_id: str = None) -> dict:
        """Get MEMBER_LINKs from database as a dict keyed by composite key."""
        from pybirdai.models.bird_meta_data_model import MEMBER_LINK
        from pybirdai.views.core.framework_filters import FrameworkSubgraphFetcher

        if framework_id:
            queryset = FrameworkSubgraphFetcher.get_member_links_for_framework(framework_id)
        else:
            queryset = MEMBER_LINK.objects.all()

        result = {}
        for ml in queryset:
            # Use composite key: csil_id + primary_member + foreign_member
            csil_id = ml.cube_structure_item_link_id.cube_structure_item_link_id if ml.cube_structure_item_link_id else ''
            primary = ml.primary_member_id.member_id if ml.primary_member_id else ''
            foreign = ml.foreign_member_id.member_id if ml.foreign_member_id else ''
            key = f"{csil_id}|{primary}|{foreign}"
            result[key] = {
                'id': ml.id,
                'cube_structure_item_link_id': csil_id,
                'primary_member_id': primary,
                'foreign_member_id': foreign,
                'is_linked': ml.is_linked,
            }
        return result

    @classmethod
    def compare_cube_links(cls, csv_path: str, framework_id: str = None) -> ChangeReport:
        """
        Compare CUBE_LINK CSV with database state.

        Args:
            csv_path: Path to cube_link.csv
            framework_id: Optional framework ID for filtering

        Returns:
            ChangeReport with new, modified, and deleted artifacts
        """
        csv_data = cls._parse_csv_to_dict(csv_path, 'CUBE_LINK_ID')
        db_data = cls._get_db_cube_links_as_dict(framework_id)

        report = ChangeReport(artifact_type='CUBE_LINK')

        csv_ids = set(csv_data.keys())
        db_ids = set(db_data.keys())

        # New in DB (not in CSV)
        for link_id in db_ids - csv_ids:
            report.new_artifacts.append(db_data[link_id])

        # Deleted from DB (in CSV but not in DB)
        for link_id in csv_ids - db_ids:
            report.deleted_artifacts.append(csv_data[link_id])

        # Check for modifications
        for link_id in csv_ids & db_ids:
            csv_row = csv_data[link_id]
            db_row = db_data[link_id]
            # Compare key fields
            if (csv_row.get('PRIMARY_CUBE_ID') != db_row.get('primary_cube_id') or
                csv_row.get('FOREIGN_CUBE_ID') != db_row.get('foreign_cube_id')):
                report.modified_artifacts.append({
                    'id': link_id,
                    'csv': csv_row,
                    'db': db_row,
                })

        return report

    @classmethod
    def compare_cube_structure_item_links(cls, csv_path: str, framework_id: str = None) -> ChangeReport:
        """
        Compare CUBE_STRUCTURE_ITEM_LINK CSV with database state.

        Args:
            csv_path: Path to cube_structure_item_link.csv
            framework_id: Optional framework ID for filtering

        Returns:
            ChangeReport with new, modified, and deleted artifacts
        """
        csv_data = cls._parse_csv_to_dict(csv_path, 'CUBE_STRUCTURE_ITEM_LINK_ID')
        db_data = cls._get_db_cube_structure_item_links_as_dict(framework_id)

        report = ChangeReport(artifact_type='CUBE_STRUCTURE_ITEM_LINK')

        csv_ids = set(csv_data.keys())
        db_ids = set(db_data.keys())

        # New in DB
        for link_id in db_ids - csv_ids:
            report.new_artifacts.append(db_data[link_id])

        # Deleted from DB
        for link_id in csv_ids - db_ids:
            report.deleted_artifacts.append(csv_data[link_id])

        # Check for modifications
        for link_id in csv_ids & db_ids:
            csv_row = csv_data[link_id]
            db_row = db_data[link_id]
            if (csv_row.get('CUBE_LINK_ID') != db_row.get('cube_link_id') or
                csv_row.get('PRIMARY_CUBE_VARIABLE_CODE') != db_row.get('primary_cube_variable_code') or
                csv_row.get('FOREIGN_CUBE_VARIABLE_CODE') != db_row.get('foreign_cube_variable_code')):
                report.modified_artifacts.append({
                    'id': link_id,
                    'csv': csv_row,
                    'db': db_row,
                })

        return report

    @classmethod
    def compare_member_links(cls, csv_path: str, framework_id: str = None) -> ChangeReport:
        """
        Compare MEMBER_LINK CSV with database state.

        Args:
            csv_path: Path to member_link.csv
            framework_id: Optional framework ID for filtering

        Returns:
            ChangeReport with new, modified, and deleted artifacts
        """
        # Parse CSV with composite key
        csv_data = {}
        if os.path.exists(csv_path):
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    csil_id = row.get('CUBE_STRUCTURE_ITEM_LINK_ID', '')
                    primary = row.get('PRIMARY_MEMBER_ID', '')
                    foreign = row.get('FOREIGN_MEMBER_ID', '')
                    key = f"{csil_id}|{primary}|{foreign}"
                    csv_data[key] = row

        db_data = cls._get_db_member_links_as_dict(framework_id)

        report = ChangeReport(artifact_type='MEMBER_LINK')

        csv_ids = set(csv_data.keys())
        db_ids = set(db_data.keys())

        # New in DB
        for key in db_ids - csv_ids:
            report.new_artifacts.append(db_data[key])

        # Deleted from DB
        for key in csv_ids - db_ids:
            report.deleted_artifacts.append(csv_data[key])

        # Check for modifications (is_linked field changes)
        for key in csv_ids & db_ids:
            csv_row = csv_data[key]
            db_row = db_data[key]
            csv_is_linked = csv_row.get('IS_LINKED', '').lower() in ('true', '1', 'yes')
            db_is_linked = db_row.get('is_linked', False)
            if csv_is_linked != db_is_linked:
                report.modified_artifacts.append({
                    'key': key,
                    'csv': csv_row,
                    'db': db_row,
                })

        return report

    @classmethod
    def compare_all_linked_artifacts(
        cls,
        csv_dir: str,
        framework_id: str = None,
        validate: bool = True
    ) -> AggregateChangeReport:
        """
        Compare all linked artifact types between CSV files and database.

        Args:
            csv_dir: Directory containing the CSV files
            framework_id: Optional framework ID for filtering
            validate: Whether to validate new artifacts

        Returns:
            AggregateChangeReport with all change and validation reports
        """
        report = AggregateChangeReport()

        # Compare each artifact type
        cube_link_csv = os.path.join(csv_dir, 'cube_link.csv')
        report.cube_link_changes = cls.compare_cube_links(cube_link_csv, framework_id)

        csil_csv = os.path.join(csv_dir, 'cube_structure_item_link.csv')
        report.cube_structure_item_link_changes = cls.compare_cube_structure_item_links(csil_csv, framework_id)

        member_link_csv = os.path.join(csv_dir, 'member_link.csv')
        report.member_link_changes = cls.compare_member_links(member_link_csv, framework_id)

        # Optionally validate new artifacts
        if validate:
            validation_report = ValidationReport()

            # Validate new CUBE_LINKs
            for artifact in report.cube_link_changes.new_artifacts:
                result = LinkedArtifactValidator.validate_cube_link(artifact, framework_id)
                validation_report.add_result(result)

            # Validate new CUBE_STRUCTURE_ITEM_LINKs
            for artifact in report.cube_structure_item_link_changes.new_artifacts:
                result = LinkedArtifactValidator.validate_cube_structure_item_link(artifact, framework_id)
                validation_report.add_result(result)

            # Validate new MEMBER_LINKs
            for artifact in report.member_link_changes.new_artifacts:
                result = LinkedArtifactValidator.validate_member_link(artifact, framework_id)
                validation_report.add_result(result)

            report.validation_report = validation_report

        return report
