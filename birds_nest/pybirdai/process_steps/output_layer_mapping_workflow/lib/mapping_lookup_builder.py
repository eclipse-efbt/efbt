"""
Mapping lookup builder for output layer mapping workflow.

Builds lookup tables from mapping definitions to map source variables/members
to target variables/members. Used by reference_table_generator and combination_creator.

Includes dictionary-based caching for performance.
"""

import logging
from itertools import groupby
from typing import Dict, List, Tuple, Optional, Any

from pybirdai.models.bird_meta_data_model import (
    VARIABLE, MEMBER, MAPPING_DEFINITION,
    VARIABLE_MAPPING_ITEM, MEMBER_MAPPING_ITEM
)

logger = logging.getLogger(__name__)

# Module-level caches
_variable_mapping_cache: Dict[str, Dict[str, VARIABLE]] = {}
_member_mapping_cache: Dict[str, Dict[Tuple[str, str], MEMBER]] = {}


def clear_mapping_lookup_cache():
    """Clear all mapping lookup caches. Call between workflow runs."""
    global _variable_mapping_cache, _member_mapping_cache
    _variable_mapping_cache.clear()
    _member_mapping_cache.clear()
    logger.debug("Mapping lookup caches cleared")


class MappingLookupBuilder:
    """
    Builds lookup tables from mapping definitions.

    Creates mappings from source to target variables and members,
    useful for reference table generation and combination creation.
    """

    def __init__(self, mapping_definitions: List[Dict[str, Any]]):
        """
        Initialize with mapping definitions.

        Args:
            mapping_definitions: List of dicts with 'mapping_definition' key
                containing MAPPING_DEFINITION objects
        """
        self.mapping_definitions = mapping_definitions or []
        self._variable_lookup: Optional[Dict[str, VARIABLE]] = None
        self._member_lookup: Optional[Dict[Tuple[str, str], MEMBER]] = None

    @property
    def variable_lookup(self) -> Dict[str, VARIABLE]:
        """
        Get the variable mapping lookup (source_var_id -> target_variable).

        Lazy-loads and caches the lookup table.
        """
        if self._variable_lookup is None:
            self._build_lookups()
        return self._variable_lookup

    @property
    def member_lookup(self) -> Dict[Tuple[str, str], MEMBER]:
        """
        Get the member mapping lookup ((source_var_id, source_member_id) -> target_member).

        Lazy-loads and caches the lookup table.
        """
        if self._member_lookup is None:
            self._build_lookups()
        return self._member_lookup

    def _build_lookups(self) -> None:
        """Build both variable and member lookup tables from mapping definitions."""
        self._variable_lookup = {}
        self._member_lookup = {}

        if not self.mapping_definitions:
            logger.debug("No mapping definitions provided, lookups will be empty")
            return

        logger.info(f"Building lookups from {len(self.mapping_definitions)} mapping definitions")

        for mapping_info in self.mapping_definitions:
            mapping_def = mapping_info.get('mapping_definition')
            if not mapping_def:
                continue

            self._process_mapping_definition(mapping_def)

        logger.info(
            f"Built {len(self._variable_lookup)} variable mappings, "
            f"{len(self._member_lookup)} member mappings"
        )

    def _process_mapping_definition(self, mapping_def: MAPPING_DEFINITION) -> None:
        """
        Process a single mapping definition to extract variable and member mappings.

        Args:
            mapping_def: MAPPING_DEFINITION object to process
        """
        # Process member_mapping for variable mappings (gives proper row-based pairing)
        if mapping_def.member_mapping_id:
            self._build_variable_mapping_from_member_mapping(mapping_def)
            self._build_member_mapping(mapping_def)

        # Process variable_mapping for observation variables (may not have member mappings)
        if mapping_def.variable_mapping_id:
            self._build_variable_mapping_from_variable_mapping(mapping_def)

    def _build_variable_mapping_from_member_mapping(
        self,
        mapping_def: MAPPING_DEFINITION
    ) -> None:
        """
        Build variable mappings from MEMBER_MAPPING_ITEM rows.

        Each row pairs source and target items, giving proper variable mapping.
        """
        mm_items = MEMBER_MAPPING_ITEM.objects.filter(
            member_mapping_id=mapping_def.member_mapping_id
        ).select_related('variable_id', 'member_id').order_by('member_mapping_row')

        # Group by row to get source-target pairs
        for row_num, items_iter in groupby(mm_items, key=lambda x: x.member_mapping_row):
            row_items = list(items_iter)
            source_items = [item for item in row_items if item.is_source == "true"]
            target_items = [item for item in row_items if item.is_source == "false"]

            # Map each source variable to its paired target variable by position
            for idx, source_item in enumerate(source_items):
                source_var_id = (
                    source_item.variable_id.variable_id
                    if source_item.variable_id else None
                )
                if source_var_id and target_items:
                    # Pair source with target at same position, or first target if no match
                    target_item = (
                        target_items[idx]
                        if idx < len(target_items) else target_items[0]
                    )
                    if source_var_id not in self._variable_lookup:
                        if target_item.variable_id:
                            self._variable_lookup[source_var_id] = target_item.variable_id
                            logger.debug(
                                f"Variable mapping (member_mapping): "
                                f"{source_var_id} -> {target_item.variable_id.variable_id}"
                            )

    def _build_variable_mapping_from_variable_mapping(
        self,
        mapping_def: MAPPING_DEFINITION
    ) -> None:
        """
        Build variable mappings from VARIABLE_MAPPING_ITEM.

        Used for observation variables that may not have member mappings.
        Only adds mappings for variables not already mapped.
        """
        vm_items = VARIABLE_MAPPING_ITEM.objects.filter(
            variable_mapping_id=mapping_def.variable_mapping_id
        ).select_related('variable_id')

        # Separate source and target variables
        source_vars = [item for item in vm_items if item.is_source == "true"]
        target_vars = [item for item in vm_items if item.is_source == "false"]

        # Only add mappings for variables not already mapped
        for source_item in source_vars:
            source_var_id = (
                source_item.variable_id.variable_id
                if source_item.variable_id else None
            )
            if source_var_id and target_vars and source_var_id not in self._variable_lookup:
                self._variable_lookup[source_var_id] = target_vars[0].variable_id
                logger.debug(
                    f"Variable mapping (variable_mapping): "
                    f"{source_var_id} -> {target_vars[0].variable_id.variable_id}"
                )

    def _build_member_mapping(self, mapping_def: MAPPING_DEFINITION) -> None:
        """
        Build member mapping lookup: (source_var, source_member) -> target_member.
        """
        mm_items = MEMBER_MAPPING_ITEM.objects.filter(
            member_mapping_id=mapping_def.member_mapping_id
        ).select_related('variable_id', 'member_id').order_by('member_mapping_row')

        # Group by row to get source-target pairs
        for row_num, items_iter in groupby(mm_items, key=lambda x: x.member_mapping_row):
            row_items = list(items_iter)
            source_items = [item for item in row_items if item.is_source == "true"]
            target_items = [item for item in row_items if item.is_source == "false"]

            # Map each source (var, member) to corresponding target member
            for source_item in source_items:
                source_var_id = (
                    source_item.variable_id.variable_id
                    if source_item.variable_id else None
                )
                source_mem_id = (
                    source_item.member_id.member_id
                    if source_item.member_id else None
                )

                if source_var_id and source_mem_id and target_items:
                    # Find target with matching variable (or use first target)
                    target_item = self._find_matching_target(
                        source_var_id, target_items
                    )

                    if target_item and target_item.member_id:
                        key = (source_var_id, source_mem_id)
                        self._member_lookup[key] = target_item.member_id
                        logger.debug(
                            f"Member mapping: ({source_var_id}, {source_mem_id}) -> "
                            f"{target_item.member_id.member_id}"
                        )

    def _find_matching_target(
        self,
        source_var_id: str,
        target_items: List[MEMBER_MAPPING_ITEM]
    ) -> Optional[MEMBER_MAPPING_ITEM]:
        """
        Find the target item that matches the source variable.

        Args:
            source_var_id: Source variable ID
            target_items: List of target MEMBER_MAPPING_ITEM objects

        Returns:
            Matching target item, or first target if no match found
        """
        if not target_items:
            return None

        # Default to first target
        result = target_items[0]

        # Try to find target with matching mapped variable
        if source_var_id in self._variable_lookup:
            target_var = self._variable_lookup[source_var_id]
            for t in target_items:
                if t.variable_id and t.variable_id.variable_id == target_var.variable_id:
                    result = t
                    break

        return result

    def get_target_variable(self, source_var_id: str) -> Optional[VARIABLE]:
        """
        Get the target variable for a source variable.

        Args:
            source_var_id: Source variable ID

        Returns:
            Target VARIABLE object, or None if no mapping exists
        """
        return self.variable_lookup.get(source_var_id)

    def get_target_member(
        self,
        source_var_id: str,
        source_member_id: str
    ) -> Optional[MEMBER]:
        """
        Get the target member for a source variable/member pair.

        Args:
            source_var_id: Source variable ID
            source_member_id: Source member ID

        Returns:
            Target MEMBER object, or None if no mapping exists
        """
        return self.member_lookup.get((source_var_id, source_member_id))

    def has_variable_mapping(self, source_var_id: str) -> bool:
        """Check if a variable mapping exists for the source variable."""
        return source_var_id in self.variable_lookup

    def has_member_mapping(self, source_var_id: str, source_member_id: str) -> bool:
        """Check if a member mapping exists for the source variable/member pair."""
        return (source_var_id, source_member_id) in self.member_lookup


def build_mapping_lookups(
    mapping_definitions: List[Dict[str, Any]]
) -> Tuple[Dict[str, VARIABLE], Dict[Tuple[str, str], MEMBER]]:
    """
    Convenience function to build both mapping lookups at once.

    Args:
        mapping_definitions: List of dicts with 'mapping_definition' key

    Returns:
        Tuple of (variable_lookup, member_lookup)
    """
    builder = MappingLookupBuilder(mapping_definitions)
    return (builder.variable_lookup, builder.member_lookup)
