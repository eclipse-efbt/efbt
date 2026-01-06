"""
Module for creating non-reference combinations.
Handles the creation of combinations from table cells while preserving links.

Includes dictionary-based caching for performance optimization.
"""

import logging
from typing import Optional, List, Dict, Any

from pybirdai.models.bird_meta_data_model import (
    TABLE_CELL, COMBINATION, COMBINATION_ITEM,
    CUBE, VARIABLE, MEMBER, SUBDOMAIN,
    MAINTENANCE_AGENCY, VARIABLE_SET
)

logger = logging.getLogger(__name__)

# Module-level caches
_variable_cache: Dict[str, Optional[VARIABLE]] = {}
_member_cache: Dict[str, Optional[MEMBER]] = {}
_subdomain_single_member_cache: Dict[str, Optional[MEMBER]] = {}


def clear_combination_cache():
    """Clear all caches in this module. Call between workflow runs."""
    global _variable_cache, _member_cache, _subdomain_single_member_cache
    _variable_cache.clear()
    _member_cache.clear()
    _subdomain_single_member_cache.clear()
    logger.debug("Combination creator caches cleared")


class CombinationCreator:
    """
    Creates non-reference combinations for table cells.
    Preserves the link: TABLE → TABLE_CELL → COMBINATION → CUBE
    """

    def __init__(self, table_code: str, table_version: str, sdd_context=None, context=None):
        """
        Initialize the combination creator.

        Args:
            table_code: The table code (e.g., 'FINREP')
            table_version: The table version (e.g., '3_0')
            sdd_context: Optional SDD context for cube-to-combination mapping
            context: Optional context for save settings
        """
        self.table_code = table_code
        self.table_version = table_version
        self.combination_counter = 0
        self.sdd_context = sdd_context
        self.context = context
        self.cube_to_combinations_to_create = []

    def create_combination_for_cell(
        self,
        cell: TABLE_CELL,
        cube: CUBE,
        timestamp: str,
        source_cell: Optional[TABLE_CELL] = None
    ) -> Optional[COMBINATION]:
        """
        Create a non-reference combination for a table cell.

        Args:
            cell: The TABLE_CELL object (may be reference cell)
            cube: The CUBE object to link to
            timestamp: Timestamp string for the entire generation run
            source_cell: Optional original DPM cell for metric lookup (use when cell is a reference cell)

        Returns:
            COMBINATION object if created successfully, None otherwise
        """
        self.combination_counter += 1

        try:
            # Generate combination ID with correct naming format
            # Combinations are Z-agnostic (shared across Z-variants)
            # Format: {table_code}_REF_{framework}_{version}_{cell_number}_REF
            # Example: C_07_00_a_REF_COREP_4_0_4151089_REF

            import re

            # Parse cell.cell_id to extract base cell number (Z-agnostic)
            # Formats:
            #   Z-variant with _REF: {cell_number}_REF__{z_member} (e.g., "4152944_REF__EBA_qEC_EBA_qx16")
            #   Z-variant without _REF: {cell_number}__{z_member} (e.g., "EBA_4151089__EBA_qEC_EBA_qx16")
            #   Base cell: {cell_number}_REF (e.g., "4152944_REF")
            cell_number = cell.cell_id

            # Step 1: Strip Z-variant suffix first (using '__' delimiter)
            # This must be done BEFORE other processing to ensure Z-agnostic IDs
            if '__' in cell_number:
                parts = cell_number.split('__', 1)
                cell_number = parts[0]  # e.g., "4152944_REF" or "EBA_4151089"

            # Step 2: Remove _REF suffix if present
            cell_number = cell_number.replace('_REF', '')

            # Step 3: Remove EBA_ prefix if present
            cell_number = cell_number.replace('EBA_', '', 1)

            # Parse cube.cube_id to extract table_code and framework_version
            # cube.cube_id may have format: EBA_{framework}_{table}_{version}_REF_{framework}_{version}
            # or: {table}_REF_{framework}_{version}
            cube_parts = cube.cube_id.split('_REF_')
            raw_table_part = cube_parts[0] if cube_parts else cube.cube_id
            framework_version = cube_parts[1] if len(cube_parts) > 1 else ''

            # Extract framework from framework_version (e.g., "COREP_4_0" -> "COREP")
            framework = ''
            if framework_version:
                fw_parts = framework_version.split('_')
                framework = fw_parts[0] if fw_parts else ''

            # Remove EBA_{framework}_ prefix from table part if present
            table_code = raw_table_part

            # Step 1: Strip Z-variant suffix from table part FIRST (uses '__' delimiter)
            # Example: "EBA_COREP_C_07_00_a_4_0__EBA_qEC_EBA_qx16" -> "EBA_COREP_C_07_00_a_4_0"
            if '__' in table_code:
                table_code = table_code.split('__', 1)[0]

            # Step 2: Remove EBA_{framework}_ prefix
            if framework:
                prefixes = [f'EBA_{framework}_', 'EBA_']
                for prefix in prefixes:
                    if table_code.startswith(prefix):
                        table_code = table_code[len(prefix):]
                        break

            # Step 3: Remove version suffix from table_code (e.g., "C_07_00_a_4_0" -> "C_07_00_a")
            version_match = re.search(r'_\d+_\d+$', table_code)
            if version_match:
                table_code = table_code[:version_match.start()]

            # Build combination_id (Z-agnostic): {table_code}_REF_{framework}_{version}_{cell_number}_REF
            combination_id = f"{table_code}_REF_{framework_version}_{cell_number}_REF"
            # Get or create maintenance agency
            maintenance_agency = self._get_maintenance_agency()

            # Get or create the combination (Z-agnostic - shared across variants)
            # Use cell_number (without Z-suffix) for code to ensure consistency
            combination, created = COMBINATION.objects.get_or_create(
                combination_id=combination_id,
                defaults={
                    'code': f"COMB_{cell_number}_REF",
                    'name': f"Combination for cell EBA_{cell_number}",
                    'maintenance_agency_id': maintenance_agency
                }
            )

            # Only create combination items if this is a new combination
            # Use source_cell for metric lookup if provided (for reference cells)
            if created:
                metric_cell = source_cell if source_cell else cell
                self._create_default_combination_items(combination, cube, cell, metric_cell)

            # Update the cell to reference this combination
            cell.table_cell_combination_id = combination.combination_id
            cell.save()


            logger.info(f"Created combination {combination_id} for cell {cell.cell_id}")
            return combination

        except Exception as e:
            logger.error(f"Error creating combination for cell {cell.cell_id}: {str(e)}")
            return None

    # Combination naming format (implemented above in create_combination_for_cell):
    # Combinations are Z-agnostic - they are shared across ALL Z-variants of the same cell
    # Format: {table_code}_REF_{framework}_{version}_{cell_number}_REF
    # Example: C_07_00_a_REF_COREP_4_0_4151089_REF
    #
    # Components:
    # - table_code: Table code (e.g., C_07_00_a) - extracted from cube.cube_id, stripped of EBA_ prefix and version
    # - framework_version: Framework and version (e.g., COREP_4_0) - from cube.cube_id (part after _REF_)
    # - cell_number: Numeric cell ID (e.g., 4151089) - extracted from cell.cell_id with Z-suffix and prefixes removed
    #
    # cell.cell_id formats (using '__' delimiter for Z-variants):
    #   Z-variant with _REF: {cell_number}_REF__{z_member} (e.g., 4152944_REF__EBA_qEC_EBA_qx16)
    #   Z-variant without _REF: EBA_{cell_number}__{z_member} (e.g., EBA_4151089__EBA_qEC_EBA_qx16)
    #   Base cell: {cell_number}_REF (e.g., 4152944_REF)
    #
    # Processing order (to ensure Z-agnostic cell_number):
    #   1. Strip Z-variant suffix first (split on '__', take first part)
    #   2. Remove _REF suffix
    #   3. Remove EBA_ prefix

    def _get_maintenance_agency(self) -> MAINTENANCE_AGENCY:
        """Get or create the default maintenance agency using AgencyManager."""
        from pybirdai.process_steps.output_layer_mapping_workflow.lib.entity_managers import (
            AgencyManager
        )
        return AgencyManager().get_efbt_agency()

    def _get_single_member_from_subdomain(
        self,
        subdomain: Optional[SUBDOMAIN]
    ) -> Optional[MEMBER]:
        """
        Check if subdomain has exactly one member and return it.

        Args:
            subdomain: The SUBDOMAIN object to check

        Returns:
            The single MEMBER if subdomain has exactly 1 member, None otherwise
        """
        if not subdomain:
            return None

        from pybirdai.models.bird_meta_data_model import SUBDOMAIN_ENUMERATION

        # Get members in this subdomain
        subdomain_members = SUBDOMAIN_ENUMERATION.objects.filter(
            subdomain_id=subdomain
        ).select_related('member_id')

        if subdomain_members.count() == 1:
            return subdomain_members.first().member_id

        return None

    def _create_default_combination_items(
        self,
        combination: COMBINATION,
        cube: CUBE,
        cell: Optional['TABLE_CELL'] = None,
        metric_cell: Optional['TABLE_CELL'] = None
    ):
        """
        Create default combination items based on cube structure.
        Uses cell's ordinate_items and member_mapping to populate member_id.
        Falls back to subdomain optimization if no mapping is found.

        Args:
            combination: The COMBINATION object
            cube: The CUBE object with structure
            cell: Optional TABLE_CELL to get ordinate_items for member lookup
            metric_cell: Optional TABLE_CELL for metric lookup (use original DPM cell for reference cells)
        """
        if not cube.cube_structure_id:
            logger.warning(f"Cube {cube.cube_id} has no structure")
            return

        # Import here to avoid circular dependency
        from pybirdai.models.bird_meta_data_model import CUBE_STRUCTURE_ITEM

        # Get cube structure items
        csi_items = CUBE_STRUCTURE_ITEM.objects.filter(
            cube_structure_id=cube.cube_structure_id
        ).order_by('order')

        observation_variables = []
        dimension_items = []

        for csi in csi_items:
            if csi.role == "O":  # Observation/Metric
                if csi.variable_id:
                    observation_variables.append(csi.variable_id)
            elif csi.role == "D":  # Dimension
                dimension_items.append(csi)

        # Determine the correct metric for this cell based on EBA_FIELD variable
        # Use metric_cell if provided (original DPM cell), otherwise use cell
        metric_variable = None
        lookup_cell = metric_cell if metric_cell else cell
        if lookup_cell and observation_variables:
            metric_variable = self._get_metric_for_cell(lookup_cell, observation_variables)
        elif observation_variables:
            # No cell provided - use first observation
            metric_variable = observation_variables[0]

        # Set metric for the combination
        if metric_variable:
            combination.metric = metric_variable
            combination.save()

        # Build member lookup map from cell's ordinate_items if cell is provided
        # Map: LDM variable_id -> LDM member (via DPM ordinate -> member_mapping)
        ldm_var_to_member = {}
        if cell:
            ldm_var_to_member = self._build_member_lookup_from_cell(cell)
            if ldm_var_to_member:
                logger.info(f"Built member lookup map with {len(ldm_var_to_member)} entries for cell {cell.cell_id}")

        # Create combination items for dimensions
        for csi in dimension_items:
            # Determine final subdomain and member
            final_subdomain = csi.subdomain_id
            final_member = csi.member_id
            member_source = None  # Track how we got the member

            # Priority 1: Check if CSI already has a member set
            if csi.member_id:
                final_member = csi.member_id
                final_subdomain = csi.subdomain_id
                member_source = "csi"
            # Priority 2: Try to get member from ordinate_items via variable mapping
            elif csi.variable_id and csi.variable_id.variable_id in ldm_var_to_member:
                final_member = ldm_var_to_member[csi.variable_id.variable_id]
                final_subdomain = None  # Use member directly
                member_source = "ordinate_mapping"
                logger.debug(
                    f"Set member {final_member.member_id if final_member else 'None'} for variable "
                    f"{csi.variable_id.variable_id} from ordinate_items mapping"
                )
            # Priority 3: Check if subdomain has only 1 member
            elif csi.subdomain_id:
                single_member = self._get_single_member_from_subdomain(csi.subdomain_id)
                if single_member:
                    final_member = single_member
                    final_subdomain = None
                    member_source = "single_member_subdomain"
                    logger.debug(
                        f"Optimized single-member subdomain {csi.subdomain_id.subdomain_id} "
                        f"to member {single_member.code} in combination item"
                    )

            # Skip if no member found - don't create combination_item without a member
            if not final_member:
                var_id = csi.variable_id.variable_id if csi.variable_id else 'None'
                logger.debug(f"Skipping combination_item for {var_id} - no member found")
                continue

            # Validate all foreign keys before creating COMBINATION_ITEM
            # Skip if variable is invalid (minimum requirement)
            if not csi.variable_id or not VARIABLE.objects.filter(
                variable_id=csi.variable_id.variable_id
            ).exists():
                logger.warning(
                    f"Skipping COMBINATION_ITEM - invalid or non-existent variable "
                    f"{csi.variable_id.variable_id if csi.variable_id else 'None'}"
                )
                continue

            # Validate subdomain - set to None if doesn't exist
            if final_subdomain and not SUBDOMAIN.objects.filter(
                subdomain_id=final_subdomain.subdomain_id
            ).exists():
                logger.warning(
                    f"Subdomain {final_subdomain.subdomain_id} doesn't exist, setting to None"
                )
                final_subdomain = None

            # Validate member - set to None if doesn't exist
            if final_member and not MEMBER.objects.filter(
                member_id=final_member.member_id
            ).exists():
                logger.warning(
                    f"Member {final_member.member_id} doesn't exist, setting to None"
                )
                final_member = None

            # Validate variable_set - set to None if doesn't exist
            validated_variable_set = csi.variable_set_id
            if validated_variable_set and not VARIABLE_SET.objects.filter(
                variable_set_id=validated_variable_set.variable_set_id
            ).exists():
                logger.warning(
                    f"Variable set {validated_variable_set.variable_set_id} doesn't exist, setting to None"
                )
                validated_variable_set = None

            COMBINATION_ITEM.objects.create(
                combination_id=combination,
                variable_id=csi.variable_id,
                member_id=final_member,
                subdomain_id=final_subdomain,
                variable_set_id=validated_variable_set
            )

        logger.info(f"Created default items for combination {combination.combination_id}")

    def _build_member_lookup_from_cell(self, cell: 'TABLE_CELL') -> Dict[str, 'MEMBER']:
        """
        Build a lookup map from variable_id to member using cell's ordinate_items.

        The process:
        1. Get cell's ordinate_items via CELL_POSITION
        2. Extract Z-axis member from table ID (for deduplicated tables)
        3. Build direct mapping (variable -> member) for LDM variables
        4. For DPM variables, map to LDM variables via VARIABLE_MAPPING_ITEM
           and map DPM members to LDM members via MEMBER_MAPPING_ITEM

        Args:
            cell: The TABLE_CELL object

        Returns:
            Dict mapping LDM variable_id to LDM MEMBER object
        """
        from pybirdai.models.bird_meta_data_model import (
            CELL_POSITION, ORDINATE_ITEM, MEMBER_MAPPING_ITEM
        )
        from .table_cell_utils import (
            extract_z_axis_member_from_table_id, resolve_full_member_id
        )

        var_to_member = {}

        try:
            # Extract Z-axis member from table ID (for deduplicated tables)
            z_axis_member = None
            z_axis_dpm_variable = None  # The DPM variable that uses z-axis (e.g., EBA_qEBB)
            table_id = cell.table_id_id if cell.table_id_id else ''
            z_member_suffix = extract_z_axis_member_from_table_id(table_id)
            if z_member_suffix:
                # The suffix is like 'EBA_qx16', we need to find the full member
                # by looking at what domain it belongs to (typically exposure class)
                logger.debug(f"Extracted Z-axis member suffix: {z_member_suffix} from table {table_id}")

            # Get cell's ordinate_items via CELL_POSITION
            cell_positions = CELL_POSITION.objects.filter(cell_id=cell).select_related('axis_ordinate_id')
            axis_ordinate_ids = [cp.axis_ordinate_id_id for cp in cell_positions]

            ordinate_items = ORDINATE_ITEM.objects.filter(
                axis_ordinate_id__in=axis_ordinate_ids
            ).select_related('variable_id', 'member_id')

            # Step 1: Build direct mappings (variable_id -> member)
            # Also identify z-axis variable and resolve the actual z-axis member from table ID
            direct_mappings = {}
            ldm_vars_with_dpm_members = {}  # Track LDM vars that have DPM members
            for oi in ordinate_items:
                if oi.variable_id and oi.member_id:
                    var_id = oi.variable_id.variable_id
                    member_id = oi.member_id.member_id
                    member_to_use = oi.member_id

                    # Check if this is a z-axis variable with a "total" member
                    # If so, replace with the actual z-axis member from table ID
                    if z_member_suffix and member_id.startswith('EBA_') and oi.variable_id.domain_id:
                        domain_id = oi.variable_id.domain_id.domain_id if hasattr(oi.variable_id.domain_id, 'domain_id') else str(oi.variable_id.domain_id)
                        # Check if this is likely a z-axis domain (exposure class)
                        if 'EC' in domain_id or 'qEC' in domain_id:
                            # Check if current member is a "total" member (x0 pattern)
                            if '_x0' in member_id or member_id.endswith('_x0'):
                                # Resolve full z-axis member ID
                                full_z_member_id = resolve_full_member_id(z_member_suffix, domain_id)
                                z_axis_member_obj = MEMBER.objects.filter(member_id=full_z_member_id).first()
                                if z_axis_member_obj:
                                    member_to_use = z_axis_member_obj
                                    z_axis_member = z_axis_member_obj
                                    z_axis_dpm_variable = var_id
                                    logger.info(
                                        f"Resolved Z-axis member: {member_id} -> {full_z_member_id} "
                                        f"for variable {var_id}"
                                    )

                    direct_mappings[var_id] = member_to_use

                    # For LDM variables (non-EBA prefixed)
                    if not var_id.startswith('EBA_'):
                        # Check if member is in the correct domain or is DPM-style
                        if member_to_use.member_id.startswith('EBA_'):
                            # LDM variable with DPM member - need to find correct LDM member
                            ldm_vars_with_dpm_members[var_id] = (oi.variable_id, member_to_use)
                            logger.debug(f"LDM variable {var_id} has DPM member {member_to_use.member_id} - will resolve")
                        else:
                            # LDM variable with LDM member - use directly
                            var_to_member[var_id] = member_to_use
                            logger.debug(f"Direct LDM mapping: {var_id} -> {member_to_use.member_id}")

            if not direct_mappings:
                logger.debug(f"No variable->member pairs found for cell {cell.cell_id}")
                return var_to_member

            # Step 2: For DPM variables, find LDM variable and LDM member via mappings
            # Build a lookup of cell's DPM (variable -> member) pairs for row matching
            cell_dpm_members = {
                var_id: member.member_id
                for var_id, member in direct_mappings.items()
                if var_id.startswith('EBA_')
            }

            # Find all relevant MEMBER_MAPPINGs and do row-based matching
            # A row matches if ALL its source members match the cell's members
            matched_rows = {}  # member_mapping_id -> matching row number

            for dpm_var_id, dpm_member in direct_mappings.items():
                if not dpm_var_id.startswith('EBA_'):
                    continue

                # Find member mappings that include this DPM member as source
                member_mapping_items = MEMBER_MAPPING_ITEM.objects.filter(
                    member_id=dpm_member,
                    is_source="true"
                ).select_related('member_mapping_id')

                for mmi in member_mapping_items:
                    mapping_id = mmi.member_mapping_id.member_mapping_id
                    row_num = mmi.member_mapping_row

                    # Skip if we already found a matching row for this mapping
                    if mapping_id in matched_rows:
                        continue

                    # Get all source items in this row
                    row_sources = MEMBER_MAPPING_ITEM.objects.filter(
                        member_mapping_id=mmi.member_mapping_id,
                        member_mapping_row=row_num,
                        is_source="true"
                    ).select_related('variable_id', 'member_id')

                    # Check if ALL source members in this row match the cell's members
                    row_matches = True
                    for row_source in row_sources:
                        if row_source.variable_id and row_source.member_id:
                            src_var = row_source.variable_id.variable_id
                            src_mem = row_source.member_id.member_id
                            cell_mem = cell_dpm_members.get(src_var)
                            if cell_mem != src_mem:
                                row_matches = False
                                break

                    if row_matches:
                        matched_rows[mapping_id] = row_num
                        logger.debug(f"Row {row_num} of {mapping_id} matches cell's members")

            # Now extract target members from matched rows
            for mapping_id, row_num in matched_rows.items():
                # Get target items from the matched row
                target_items = MEMBER_MAPPING_ITEM.objects.filter(
                    member_mapping_id__member_mapping_id=mapping_id,
                    member_mapping_row=row_num,
                    is_source="false"
                ).select_related('variable_id', 'member_id')

                for target_item in target_items:
                    if target_item.variable_id and target_item.member_id:
                        ldm_var_id = target_item.variable_id.variable_id
                        ldm_member = target_item.member_id
                        if ldm_var_id not in var_to_member:
                            var_to_member[ldm_var_id] = ldm_member
                            logger.debug(
                                f"Row-matched: {ldm_var_id} -> {ldm_member.member_id} "
                                f"(from {mapping_id} row {row_num})"
                            )

            # Note: DPM variables without matched mappings are intentionally skipped
            # No combination_item will be created for variables without a matching row

            # Step 3: Handle LDM variables that have DPM-style members
            # These occur when ordinate_items directly reference LDM variables but with DPM members
            for ldm_var_id, (ldm_variable, dpm_member) in ldm_vars_with_dpm_members.items():
                if ldm_var_id in var_to_member:
                    # Already resolved via another path
                    continue

                # First try to find via MEMBER_MAPPING_ITEM
                member_mapping_items = MEMBER_MAPPING_ITEM.objects.filter(
                    member_id=dpm_member,
                    is_source="true"
                ).select_related('member_mapping_id')

                ldm_member_found = False
                for mmi in member_mapping_items:
                    # Find the target LDM member for this LDM variable
                    target_mmis = MEMBER_MAPPING_ITEM.objects.filter(
                        member_mapping_id=mmi.member_mapping_id,
                        variable_id__variable_id=ldm_var_id,
                        is_source="false"
                    ).select_related('member_id')

                    for target_mmi in target_mmis:
                        if target_mmi.member_id:
                            var_to_member[ldm_var_id] = target_mmi.member_id
                            logger.debug(
                                f"Resolved LDM var with DPM member: {ldm_var_id}:{dpm_member.member_id} -> "
                                f"{target_mmi.member_id.member_id}"
                            )
                            ldm_member_found = True
                            break
                    if ldm_member_found:
                        break

                # If no explicit mapping found, skip this variable (no combination_item created)
                if not ldm_member_found:
                    logger.debug(
                        f"No mapping found for LDM var: {ldm_var_id} with DPM member {dpm_member.member_id} - skipping"
                    )

            logger.info(f"Built member lookup with {len(var_to_member)} entries for cell {cell.cell_id}")

        except Exception as e:
            logger.warning(f"Error building member lookup for cell {cell.cell_id}: {str(e)}")

        return var_to_member

    def _get_metric_for_cell(
        self,
        cell: 'TABLE_CELL',
        observation_variables: List['VARIABLE']
    ) -> Optional['VARIABLE']:
        """
        Determine the correct observation/metric variable for a cell based on its EBA_FIELD variable.

        The process:
        1. Get the cell's EBA_FIELD_* variable from ordinate_items
        2. Find the VARIABLE_MAPPING that contains this EBA_FIELD as a source
        3. Get the target observation variables from the mapping
        4. Return the matching LDM observation variable

        Args:
            cell: The TABLE_CELL object
            observation_variables: List of observation VARIABLE objects from cube structure

        Returns:
            The correct LDM observation VARIABLE for this cell, or first observation if not determined
        """
        from pybirdai.models.bird_meta_data_model import (
            CELL_POSITION, ORDINATE_ITEM, VARIABLE_MAPPING_ITEM
        )

        if not observation_variables:
            return None

        if len(observation_variables) == 1:
            # Only one observation variable - use it
            return observation_variables[0]

        try:
            # Get cell's ordinate_items via CELL_POSITION
            cell_positions = CELL_POSITION.objects.filter(cell_id=cell).select_related('axis_ordinate_id')
            axis_ordinate_ids = [cp.axis_ordinate_id_id for cp in cell_positions]

            # Find EBA_FIELD_* variable in ordinate items
            field_ordinate = ORDINATE_ITEM.objects.filter(
                axis_ordinate_id__in=axis_ordinate_ids,
                variable_id__variable_id__startswith='EBA_FIELD'
            ).select_related('variable_id').first()

            if not field_ordinate or not field_ordinate.variable_id:
                logger.debug(f"No EBA_FIELD variable found for cell {cell.cell_id}")
                return observation_variables[0]

            eba_field_var_id = field_ordinate.variable_id.variable_id
            logger.debug(f"Cell {cell.cell_id} has EBA_FIELD variable: {eba_field_var_id}")

            # Find variable mapping that has this EBA_FIELD as source
            source_vmi = VARIABLE_MAPPING_ITEM.objects.filter(
                variable_id__variable_id=eba_field_var_id,
                is_source="true"
            ).select_related('variable_mapping_id').first()

            if not source_vmi:
                logger.debug(f"No variable mapping found for EBA_FIELD: {eba_field_var_id}")
                return observation_variables[0]

            # Get all items in this variable mapping
            mapping_items = VARIABLE_MAPPING_ITEM.objects.filter(
                variable_mapping_id=source_vmi.variable_mapping_id
            ).select_related('variable_id').order_by('pk')

            # Build lists of sources and targets in order
            sources = []
            targets = []
            for vmi in mapping_items:
                if vmi.variable_id:
                    if vmi.is_source == "true":
                        sources.append(vmi.variable_id.variable_id)
                    else:
                        targets.append(vmi.variable_id)

            # Find the index of our EBA_FIELD in sources
            source_index = None
            for i, src in enumerate(sources):
                if src == eba_field_var_id:
                    source_index = i
                    break

            if source_index is not None and source_index < len(targets):
                # Use the corresponding target observation
                target_var = targets[source_index]
                # Verify it's one of our observation variables
                for obs_var in observation_variables:
                    if obs_var.variable_id == target_var.variable_id:
                        logger.info(
                            f"Mapped EBA_FIELD {eba_field_var_id} to observation {target_var.variable_id}"
                        )
                        return obs_var

                # Target found but not in observation_variables - use the target anyway if it exists
                logger.info(
                    f"Mapped EBA_FIELD {eba_field_var_id} to observation {target_var.variable_id} (from mapping)"
                )
                return target_var

            logger.debug(
                f"Could not determine specific observation for EBA_FIELD {eba_field_var_id}, "
                f"sources={sources}, source_index={source_index}, targets={[t.variable_id for t in targets]}"
            )

        except Exception as e:
            logger.warning(f"Error determining metric for cell {cell.cell_id}: {str(e)}")

        # Default to first observation variable
        return observation_variables[0]

    # ========== Batch Creation Methods ==========

    def create_combinations_for_cells(
        self,
        cells,
        cube: 'CUBE',
        debug_data: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """
        Create combinations for multiple cells in a batch.

        Args:
            cells: QuerySet of TABLE_CELL objects
            cube: The CUBE object to link to
            debug_data: Optional dict to track created objects

        Returns:
            Dict with 'created_combinations' and 'cells_count'
        """
        from pybirdai.models.bird_meta_data_model import CUBE_TO_COMBINATION
        import datetime

        # Generate single timestamp for entire batch
        timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S')

        created_combinations = []

        for cell in cells:
            # Create combination for this cell
            combination = self.create_combination_for_cell(cell, cube, timestamp)

            if combination:
                created_combinations.append(combination)

                # Track in debug_data
                if debug_data is not None and 'COMBINATION' in debug_data:
                    if combination not in debug_data['COMBINATION']:
                        debug_data['COMBINATION'].append(combination)

                # Create CUBE_TO_COMBINATION link if not using sdd_context
                if self.sdd_context is None or self.context is None:
                    cube_to_combo, _ = CUBE_TO_COMBINATION.objects.get_or_create(
                        cube_id=cube,
                        combination_id=combination
                    )
                    if debug_data is not None and 'CUBE_TO_COMBINATION' in debug_data:
                        debug_data['CUBE_TO_COMBINATION'].append(cube_to_combo)

        logger.info(f"Created {len(created_combinations)} combinations and linked to cube")

        return {
            'created_combinations': created_combinations,
            'cells_count': cells.count() if hasattr(cells, 'count') else len(cells)
        }
