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

    def __init__(self):
        """Initialize the combination creator."""
        self.combination_counter = 0

    def create_combination_for_cell(
        self,
        cell: TABLE_CELL,
        cube: CUBE,
        timestamp: str = None
    ) -> Optional[COMBINATION]:
        """
        Create a non-reference combination for a table cell.

        Args:
            cell: The TABLE_CELL object
            cube: The CUBE object to link to
            timestamp: Optional timestamp string for ID generation

        Returns:
            COMBINATION object if created successfully, None otherwise
        """
        if not timestamp:
            timestamp = datetime.now().strftime('%Y%m%d%H%M%S')

        self.combination_counter += 1

        try:
            # Generate combination ID
            combination_id = self._generate_combination_id(
                cube.cube_id, timestamp, self.combination_counter
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
        timestamp: str = None
    ) -> List[COMBINATION]:
        """
        Create combinations for multiple table cells.

        Args:
            cells: List of TABLE_CELL objects
            cube: The CUBE object to link to
            timestamp: Optional timestamp string for ID generation

        Returns:
            List of created COMBINATION objects
        """
        if not timestamp:
            timestamp = datetime.now().strftime('%Y%m%d%H%M%S')

        combinations = []
        for cell in cells:
            combination = self.create_combination_for_cell(cell, cube, timestamp)
            if combination:
                combinations.append(combination)

        logger.info(f"Created {len(combinations)} combinations for {len(cells)} cells")
        return combinations

    def _generate_combination_id(
        self,
        cube_id: str,
        timestamp: str,
        counter: int
    ) -> str:
        """
        Generate a unique combination ID.

        Args:
            cube_id: The cube ID
            timestamp: Timestamp string
            counter: Counter for uniqueness

        Returns:
            Generated combination ID
        """
        # Extract base from cube_id (remove timestamp if present)
        base = cube_id.split('_')[0] if '_' in cube_id else cube_id

        return f"{base}_COMB_{timestamp}_{counter:04d}"

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

    def _copy_combination_items(
        self,
        source_combination_id: str,
        target_combination: COMBINATION
    ):
        """
        Copy combination items from source to target combination.

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
                COMBINATION_ITEM.objects.create(
                    combination_id=target_combination,
                    variable_id=item.variable_id,
                    subdomain_id=item.subdomain_id,
                    variable_set_id=item.variable_set_id,
                    member_id=item.member_id,
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
            # If CSI has a fixed member, use it
            if csi.member_id:
                COMBINATION_ITEM.objects.create(
                    combination_id=combination,
                    variable_id=csi.variable_id,
                    member_id=csi.member_id,
                    subdomain_id=csi.subdomain_id,
                    variable_set_id=csi.variable_set_id
                )
            else:
                # Create without specific member (will be determined at runtime)
                COMBINATION_ITEM.objects.create(
                    combination_id=combination,
                    variable_id=csi.variable_id,
                    subdomain_id=csi.subdomain_id,
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