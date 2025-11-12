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
    MAINTENANCE_AGENCY
)

logger = logging.getLogger(__name__)


class CombinationCreator:
    """
    Creates non-reference combinations for table cells.
    Preserves the link: TABLE → TABLE_CELL → COMBINATION → CUBE
    """

    def __init__(self, table_code: str, table_version: str):
        """
        Initialize the combination creator.

        Args:
            table_code: The table code (e.g., 'FINREP')
            table_version: The table version (e.g., '3_0')
        """
        self.table_code = table_code
        self.table_version = table_version
        self.combination_counter = 0

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
                self._create_default_combination_items(combination, cube)

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

                COMBINATION_ITEM.objects.create(
                    combination_id=target_combination,
                    variable_id=item.variable_id,
                    subdomain_id=final_subdomain,
                    variable_set_id=item.variable_set_id,
                    member_id=final_member,
                    member_hierarchy=item.member_hierarchy
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
        cube: CUBE
    ):
        """
        Create default combination items based on cube structure.
        Optimizes single-member subdomains to use direct member references.

        Args:
            combination: The COMBINATION object
            cube: The CUBE object with structure
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

        metric_variable = None
        dimension_items = []

        for csi in csi_items:
            if csi.role == "O":  # Observation/Metric
                metric_variable = csi.variable_id
            elif csi.role == "D":  # Dimension
                dimension_items.append(csi)

        # Set metric for the combination
        if metric_variable:
            combination.metric = metric_variable
            combination.save()

        # Create combination items for dimensions
        for csi in dimension_items:
            # Determine final subdomain and member
            final_subdomain = csi.subdomain_id
            final_member = csi.member_id

            # If CSI has a member (fixed or from single-member subdomain), use it
            if csi.member_id:
                # CSI already has a member set, use it directly
                final_member = csi.member_id
                # Keep subdomain as None if it was already None in CSI
                final_subdomain = csi.subdomain_id
            elif csi.subdomain_id:
                # No member set, but subdomain exists - check if it has only 1 member
                single_member = self._get_single_member_from_subdomain(csi.subdomain_id)
                if single_member:
                    # Optimize: use member directly instead of subdomain
                    final_member = single_member
                    final_subdomain = None
                    logger.info(
                        f"Optimized single-member subdomain {csi.subdomain_id.subdomain_id} "
                        f"to member {single_member.code} in combination item"
                    )

            COMBINATION_ITEM.objects.create(
                combination_id=combination,
                variable_id=csi.variable_id,
                member_id=final_member,
                subdomain_id=final_subdomain,
                variable_set_id=csi.variable_set_id
            )

        logger.info(f"Created default items for combination {combination.combination_id}")

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