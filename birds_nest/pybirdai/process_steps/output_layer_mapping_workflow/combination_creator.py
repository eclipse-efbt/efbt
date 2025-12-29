"""
Module for creating non-reference combinations.
Handles the creation of combinations from table cells while preserving links.
"""

import logging
from typing import Optional, List, Dict
from datetime import datetime

from pybirdai.models.bird_meta_data_model import (
    TABLE_CELL, COMBINATION, COMBINATION_ITEM,
    CUBE, VARIABLE, MEMBER, SUBDOMAIN,
    MAINTENANCE_AGENCY, VARIABLE_SET, MEMBER_HIERARCHY,
    CUBE_TO_COMBINATION
)

logger = logging.getLogger(__name__)


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
        timestamp: str
    ) -> Optional[COMBINATION]:
        """
        Create a non-reference combination for a table cell.

        Args:
            cell: The TABLE_CELL object
            cube: The CUBE object to link to
            timestamp: Timestamp string for the entire generation run

        Returns:
            COMBINATION object if created successfully, None otherwise
        """
        self.combination_counter += 1

        try:
            # Generate combination ID
            combination_id = self._generate_combination_id(
                timestamp, self.combination_counter
            )

            # Get or create maintenance agency
            maintenance_agency = self._get_maintenance_agency()

            # Create the combination
            combination = COMBINATION.objects.create(
                combination_id=combination_id,
                code=f"COMB_{cell.cell_id}",
                name=f"Combination for cell {cell.name or cell.cell_id}",
                maintenance_agency_id=maintenance_agency
            )

            # If the cell already has a combination, copy its structure
            if cell.table_cell_combination_id:
                self._copy_combination_items(
                    cell.table_cell_combination_id,
                    combination
                )
            else:
                # Create default combination items based on cube structure
                # Pass cell to enable member lookup from ordinate_items
                self._create_default_combination_items(combination, cube, cell)

            # Update the cell to reference this combination
            cell.table_cell_combination_id = combination.combination_id
            cell.save()

           
            logger.info(f"Created combination {combination_id} for cell {cell.cell_id}")
            return combination

        except Exception as e:
            logger.error(f"Error creating combination for cell {cell.cell_id}: {str(e)}")
            return None

    def create_combinations_for_cells(
        self,
        cells: List[TABLE_CELL],
        cube: CUBE,
        timestamp: str
    ) -> List[COMBINATION]:
        """
        Create combinations for multiple table cells.

        Args:
            cells: List of TABLE_CELL objects
            cube: The CUBE object to link to
            timestamp: Timestamp string for the entire generation run

        Returns:
            List of created COMBINATION objects
        """
        combinations = []
        for cell in cells:
            combination = self.create_combination_for_cell(cell, cube, timestamp)
            if combination:
                combinations.append(combination)

        logger.info(f"Created {len(combinations)} combinations for {len(cells)} cells")
        return combinations

    def _generate_combination_id(
        self,
        timestamp: str,
        counter: int
    ) -> str:
        """
        Generate a unique combination ID.

        Format: combination_{table_code}_{table_version}_{timestamp}_{index}

        Args:
            timestamp: Timestamp string for the entire generation run
            counter: Counter for uniqueness within the run

        Returns:
            Generated combination ID
        """
        return f"combination_{self.table_code}_{self.table_version}_{timestamp}_{counter:04d}"

    def _get_maintenance_agency(self) -> MAINTENANCE_AGENCY:
        """Get or create the default maintenance agency."""
        agency = MAINTENANCE_AGENCY.objects.first()
        if not agency:
            agency = MAINTENANCE_AGENCY.objects.create(
                maintenance_agency_id='EFBT',
                name='EFBT System',
                code='EFBT'
            )
        return agency

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

    def _copy_combination_items(
        self,
        source_combination_id: str,
        target_combination: COMBINATION
    ):
        """
        Copy combination items from source to target combination.
        Optimizes single-member subdomains to use direct member references.

        Args:
            source_combination_id: ID of the source combination
            target_combination: Target COMBINATION object
        """
        try:
            source_combination = COMBINATION.objects.get(
                combination_id=source_combination_id
            )

            # Get source combination items
            source_items = COMBINATION_ITEM.objects.filter(
                combination_id=source_combination
            )

            # Create copies for target combination
            for item in source_items:
                # Check if we should optimize subdomain to member
                final_subdomain = item.subdomain_id
                final_member = item.member_id

                # If no member is set but subdomain has only 1 member, use that member instead
                if not final_member and final_subdomain:
                    single_member = self._get_single_member_from_subdomain(final_subdomain)
                    if single_member:
                        final_member = single_member
                        final_subdomain = None
                        logger.info(
                            f"Optimized single-member subdomain {item.subdomain_id.subdomain_id} "
                            f"to member {single_member.code}"
                        )

                # Validate all foreign keys before creating COMBINATION_ITEM
                # Skip if variable is invalid (minimum requirement)
                if not item.variable_id or not VARIABLE.objects.filter(
                    variable_id=item.variable_id.variable_id
                ).exists():
                    logger.warning(
                        f"Skipping COMBINATION_ITEM - invalid or non-existent variable "
                        f"{item.variable_id.variable_id if item.variable_id else 'None'}"
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
                validated_variable_set = item.variable_set_id
                if validated_variable_set and not VARIABLE_SET.objects.filter(
                    variable_set_id=validated_variable_set.variable_set_id
                ).exists():
                    logger.warning(
                        f"Variable set {validated_variable_set.variable_set_id} doesn't exist, setting to None"
                    )
                    validated_variable_set = None

                # Validate member_hierarchy - set to None if doesn't exist
                validated_member_hierarchy = item.member_hierarchy
                if validated_member_hierarchy and not MEMBER_HIERARCHY.objects.filter(
                    member_hierarchy_id=validated_member_hierarchy.member_hierarchy_id
                ).exists():
                    logger.warning(
                        f"Member hierarchy {validated_member_hierarchy.member_hierarchy_id} doesn't exist, setting to None"
                    )
                    validated_member_hierarchy = None

                COMBINATION_ITEM.objects.create(
                    combination_id=target_combination,
                    variable_id=item.variable_id,
                    subdomain_id=final_subdomain,
                    variable_set_id=validated_variable_set,
                    member_id=final_member,
                    member_hierarchy=validated_member_hierarchy
                )

            # Also copy the metric if present
            if source_combination.metric:
                target_combination.metric = source_combination.metric
                target_combination.save()

            logger.info(f"Copied {len(source_items)} items from {source_combination_id}")

        except COMBINATION.DoesNotExist:
            logger.warning(f"Source combination {source_combination_id} not found")

    def _create_default_combination_items(
        self,
        combination: COMBINATION,
        cube: CUBE,
        cell: Optional['TABLE_CELL'] = None
    ):
        """
        Create default combination items based on cube structure.
        Uses cell's ordinate_items and member_mapping to populate member_id.
        Falls back to subdomain optimization if no mapping is found.

        Args:
            combination: The COMBINATION object
            cube: The CUBE object with structure
            cell: Optional TABLE_CELL to get ordinate_items for member lookup
        """
        if not cube.cube_structure_id:
            logger.warning(f"Cube {cube.cube_id} has no structure")
            return

        # Import here to avoid circular dependency
        from pybirdai.models.bird_meta_data_model import (
            CUBE_STRUCTURE_ITEM, CELL_POSITION, ORDINATE_ITEM,
            VARIABLE_MAPPING_ITEM, MEMBER_MAPPING_ITEM
        )

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
        metric_variable = None
        if cell and observation_variables:
            metric_variable = self._get_metric_for_cell(cell, observation_variables)
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
            CELL_POSITION, ORDINATE_ITEM, VARIABLE_MAPPING_ITEM, MEMBER_MAPPING_ITEM
        )
        from pybirdai.process_steps.output_layer_mapping_workflow.table_cell_utils import (
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

    def link_combination_to_cube(
        self,
        combination: COMBINATION,
        cube: CUBE
    ) -> bool:
        """
        Create a link between combination and cube.

        Args:
            combination: The COMBINATION object
            cube: The CUBE object

        Returns:
            True if successful, False otherwise
        """
        from pybirdai.models.bird_meta_data_model import CUBE_TO_COMBINATION

        try:
            # Check if link already exists
            existing = CUBE_TO_COMBINATION.objects.filter(
                cube_id=cube,
                combination_id=combination
            ).first()

            if existing:
                logger.info(f"Link already exists between {cube.cube_id} and {combination.combination_id}")
                return True

            # Create new link
            CUBE_TO_COMBINATION.objects.create(
                cube_id=cube,
                combination_id=combination
            )

            logger.info(f"Linked combination {combination.combination_id} to cube {cube.cube_id}")
            return True

        except Exception as e:
            logger.error(f"Error linking combination to cube: {str(e)}")
            return False

    def update_combination_with_mapping(
        self,
        combination: COMBINATION,
        variable_mappings: Dict[str, str],
        member_mappings: Dict[str, Dict[str, str]]
    ):
        """
        Update combination items based on mappings.

        Args:
            combination: The COMBINATION object to update
            variable_mappings: Dict mapping source to target variables
            member_mappings: Dict mapping source to target members per variable
        """
        # Get existing combination items
        items = COMBINATION_ITEM.objects.filter(combination_id=combination)

        for item in items:
            if not item.variable_id:
                continue

            var_id = item.variable_id.variable_id

            # Check if there's a variable mapping
            if var_id in variable_mappings:
                target_var_id = variable_mappings[var_id]
                target_variable = VARIABLE.objects.filter(
                    variable_id=target_var_id
                ).first()

                if target_variable:
                    # Update variable
                    item.variable_id = target_variable

                    # Check if there's a member mapping
                    if item.member_id and var_id in member_mappings:
                        member_map = member_mappings[var_id]
                        source_member_code = item.member_id.code

                        if source_member_code in member_map:
                            target_member_code = member_map[source_member_code]
                            target_member = MEMBER.objects.filter(
                                domain_id=target_variable.domain_id,
                                code=target_member_code
                            ).first()

                            if target_member:
                                item.member_id = target_member

                    # Update subdomain if needed
                    if hasattr(target_variable, 'domain_id') and target_variable.domain_id:
                        # Try to find appropriate subdomain
                        subdomain = SUBDOMAIN.objects.filter(
                            domain_id=target_variable.domain_id
                        ).first()
                        if subdomain:
                            item.subdomain_id = subdomain

                    item.save()
                    logger.info(f"Updated combination item for variable {var_id} -> {target_var_id}")

    def validate_combination(self, combination: COMBINATION) -> Dict:
        """
        Validate a combination for completeness and consistency.

        Args:
            combination: The COMBINATION object to validate

        Returns:
            Dict with validation results
        """
        issues = []
        warnings = []

        # Check if combination has a metric
        if not combination.metric:
            warnings.append("Combination has no metric variable defined")

        # Get combination items
        items = COMBINATION_ITEM.objects.filter(combination_id=combination)

        if not items:
            issues.append("Combination has no items")
        else:
            # Check each item
            for item in items:
                if not item.variable_id:
                    issues.append(f"Combination item has no variable")
                else:
                    # Check if subdomain is consistent with variable domain
                    if item.subdomain_id:
                        if hasattr(item.variable_id, 'domain_id'):
                            if item.subdomain_id.domain_id != item.variable_id.domain_id:
                                warnings.append(
                                    f"Subdomain {item.subdomain_id.subdomain_id} "
                                    f"doesn't match variable domain"
                                )

                    # Check if member is consistent with domain
                    if item.member_id:
                        if hasattr(item.variable_id, 'domain_id'):
                            if item.member_id.domain_id != item.variable_id.domain_id:
                                issues.append(
                                    f"Member {item.member_id.member_id} "
                                    f"doesn't match variable domain"
                                )

        is_valid = len(issues) == 0

        return {
            'valid': is_valid,
            'issues': issues,
            'warnings': warnings,
            'item_count': len(items)
        }

    def get_combination_summary(self, combination: COMBINATION) -> Dict:
        """
        Get a summary of a combination's structure.

        Args:
            combination: The COMBINATION object

        Returns:
            Dict with combination summary
        """
        items = COMBINATION_ITEM.objects.filter(
            combination_id=combination
        ).select_related('variable_id', 'member_id', 'subdomain_id')

        dimensions = []
        for item in items:
            dim_info = {
                'variable': item.variable_id.name if item.variable_id else None,
                'variable_id': item.variable_id.variable_id if item.variable_id else None,
                'member': item.member_id.name if item.member_id else None,
                'member_code': item.member_id.code if item.member_id else None,
                'subdomain': item.subdomain_id.name if item.subdomain_id else None
            }
            dimensions.append(dim_info)

        return {
            'combination_id': combination.combination_id,
            'name': combination.name,
            'code': combination.code,
            'metric': combination.metric.name if combination.metric else None,
            'metric_id': combination.metric.variable_id if combination.metric else None,
            'dimensions': dimensions,
            'dimension_count': len(dimensions)
        }

