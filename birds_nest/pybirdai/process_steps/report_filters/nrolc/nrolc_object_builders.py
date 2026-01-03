# coding=UTF-8
# Copyright (c) 2025 Arfa Digital Consulting
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDX-License-Identifier: EPL-2.0
#
# Contributors:
#    Benjamin Arfa - initial API and implementation
#

"""
Object builders for NROLC (Non-Reference Output Layer Creator).
Creates CUBE, CUBE_STRUCTURE, COMBINATION, and related objects.
"""

from pybirdai.models.bird_meta_data_model import (
    CUBE, CUBE_STRUCTURE, COMBINATION, COMBINATION_ITEM,
    CUBE_STRUCTURE_ITEM, CUBE_TO_COMBINATION, SUBDOMAIN, VARIABLE
)
from pybirdai.process_steps.report_filters.nrolc import nrolc_utils
from pybirdai.services.framework_selection import get_or_create_maintenance_agency_for_framework
import logging


class OutputLayerBuilder:
    """Builds output layer objects (cubes, combinations, structures)."""

    METRIC_META_VARIABLE_ID = 'EBA_ATY'

    def __init__(self, subdomain_manager):
        """
        Initialize output layer builder.

        Args:
            subdomain_manager: SubdomainManager instance for subdomain operations
        """
        self.subdomain_manager = subdomain_manager

    def _is_metric_meta_variable(self, variable):
        """
        Check if a variable is the EBA_ATY metric meta-variable.

        Args:
            variable: VARIABLE instance to check

        Returns:
            bool: True if variable is EBA_ATY, False otherwise
        """
        if not variable:
            return False
        return variable.variable_id == self.METRIC_META_VARIABLE_ID

    def _determine_variable_role(self, variable):
        """
        Determine the role (D/O/A) for a variable based on its domain characteristics.

        Classification rules:
        1. EBA_Float or EBA_Integer domains → Measures (role='O')
        2. EBA_String domains OR non-enumerated domains → Attributes (role='A')
        3. Enumerated domains → Dimensions (role='D')

        Args:
            variable: VARIABLE instance

        Returns:
            str: 'D' (Dimension), 'O' (Observation/Measure), or 'A' (Attribute)
        """
        # Default to dimension if no variable or domain
        if not variable:
            return 'D'

        domain = variable.domain_id
        if not domain:
            # Variables without domains are treated as attributes
            return 'A'

        domain_id = domain.domain_id if hasattr(domain, 'domain_id') else str(domain)

        # Rule 1: Numeric domains are measures (observations)
        # Reference domains (Float, MNTRY, etc.) and EBA_ prefixed versions
        MEASURE_DOMAINS = {
            'Float', 'Integer', 'Decimal', 'Monetary', 'MNTRY',
            'Double', 'Long',
            # EBA_ prefixed versions for backwards compatibility
            'EBA_Float', 'EBA_Integer', 'EBA_Decimal', 'EBA_Monetary',
            'EBA_Double', 'EBA_Long'
        }
        if domain_id in MEASURE_DOMAINS:
            return 'O'

        # Rule 2a: String domains are attributes
        if domain_id in ('String', 'EBA_String'):
            return 'A'

        # Rule 2b: Non-enumerated domains are attributes
        if hasattr(domain, 'is_enumerated') and domain.is_enumerated == False:
            return 'A'

        # Rule 3: Enumerated domains are dimensions
        if hasattr(domain, 'is_enumerated') and domain.is_enumerated == True:
            return 'D'

        # Default to dimension for unknown cases
        return 'D'

    def create_cube_and_structure(self, table, framework_object):
        """
        Create CUBE and CUBE_STRUCTURE from table rendering.

        Args:
            table: TABLE instance
            framework_object: FRAMEWORK instance

        Returns:
            tuple: (cube, cube_structure)
        """
        # Get maintenance agency based on framework
        framework_id = framework_object.framework_id if framework_object else None
        maintenance_agency = get_or_create_maintenance_agency_for_framework(framework_id)

        # Create cube
        cube_name = table.table_id
        cube = CUBE()
        cube.cube_id = cube_name
        cube.name = table.name or cube_name
        cube.description = table.description
        cube.cube_type = 'RC'  # Report Cube
        cube.framework_id = framework_object
        cube.maintenance_agency_id = maintenance_agency

        # Create cube structure
        cube_structure = CUBE_STRUCTURE()
        cube_structure.cube_structure_id = table.table_id
        cube_structure.name = f"{cube.name} Structure"
        cube_structure.description = f"Structure for {cube.name}"
        cube_structure.maintenance_agency_id = maintenance_agency

        # Link cube to structure
        cube.cube_structure_id = cube_structure

        return cube, cube_structure

    def _compute_combination_signature(self, ordinate_items, metric):
        """
        Compute a hashable signature for a combination based on its items.

        Two combinations are considered identical if they have:
        1. The same metric (measure variable)
        2. The same set of (variable_id, member_id) pairs

        Args:
            ordinate_items: List of ordinate items that will become combination items
            metric: VARIABLE instance representing the measure (or None)

        Returns:
            tuple: (metric_id, frozenset of (variable_id, member_id) tuples)
        """
        items = []
        for item in ordinate_items:
            if item.variable_id:
                var_id = item.variable_id.variable_id
                mem_id = item.member_id.member_id if item.member_id else None
                items.append((var_id, mem_id))

        metric_id = metric.variable_id if metric else None
        # Use frozenset for order-independent comparison
        return (metric_id, frozenset(items))

    def create_combinations_and_items(
        self,
        cells,
        cell_positions,
        ordinate_items,
        cube,
        timestamp,
        combination_counter_ref
    ):
        """
        Create COMBINATION and COMBINATION_ITEMs for each cell.
        Each cell's related ordinates generate a combination.

        Args:
            cells: List of TABLE_CELL instances
            cell_positions: List of CELL_POSITION instances
            ordinate_items: List of ORDINATE_ITEM instances
            cube: CUBE instance
            timestamp: Timestamp string for ID generation
            combination_counter_ref: Dict with 'value' key for counter (mutable reference)

        Returns:
            tuple: (combinations, combination_items, cube_to_combinations, cells_to_update)
        """
        combinations = []
        combination_items = []
        cube_to_combinations = []
        cells_to_update = []

        # Pre-fetch existing combinations for this cube to enable deduplication
        combination_cache = {}  # signature -> COMBINATION instance
        try:
            existing_combinations = COMBINATION.objects.filter(
                cube_to_combination__cube_id=cube
            ).prefetch_related('combination_item_set').distinct()

            # Build signature cache from existing combinations
            for existing_combo in existing_combinations:
                # Get all combination items for this combination
                combo_items = existing_combo.combination_item_set.all()

                # Build list of (variable_id, member_id) tuples
                item_tuples = []
                for item in combo_items:
                    var_id = item.variable_id.variable_id if item.variable_id else None
                    mem_id = item.member_id.member_id if item.member_id else None
                    item_tuples.append((var_id, mem_id))

                # Compute signature
                metric_id = existing_combo.metric.variable_id if existing_combo.metric else None
                signature = (metric_id, frozenset(item_tuples))

                # Add to cache
                combination_cache[signature] = existing_combo

            logging.info(f"Pre-fetched {len(combination_cache)} existing combinations for cube {cube.cube_id}")
        except Exception as e:
            logging.warning(f"Could not pre-fetch existing combinations: {e}")
            # Continue with empty cache

        # Create a mapping of axis_ordinate to ordinate_items for efficiency
        ordinate_to_items = {}
        for item in ordinate_items:
            if item.axis_ordinate_id_id not in ordinate_to_items:
                ordinate_to_items[item.axis_ordinate_id_id] = []
            ordinate_to_items[item.axis_ordinate_id_id].append(item)

        # Create a mapping of cell to its positions
        cell_to_positions = {}
        for position in cell_positions:
            if position.cell_id_id not in cell_to_positions:
                cell_to_positions[position.cell_id_id] = []
            cell_to_positions[position.cell_id_id].append(position)

        # Process each cell
        for cell in cells:
            # Get all positions for this cell
            positions = cell_to_positions.get(cell.cell_id, [])

            # Collect all ordinate items for this cell
            cell_ordinate_items = []
            for position in positions:
                if position.axis_ordinate_id_id in ordinate_to_items:
                    cell_ordinate_items.extend(
                        ordinate_to_items[position.axis_ordinate_id_id]
                    )

            # Find the metric (measure variable)
            # Metric is a variable without member
            metric = None
            for item in cell_ordinate_items:
                if item.variable_id and not item.member_id:
                    metric = item.variable_id
                    break

            # Compute signature for this cell's combination
            signature = self._compute_combination_signature(cell_ordinate_items, metric)

            # Check if combination with this signature already exists
            if signature in combination_cache:
                # Reuse existing combination
                combination = combination_cache[signature]
                logging.debug(f"Reusing existing combination {combination.combination_id} for cell {cell.cell_id}")
            else:
                # Create new combination
                combination_counter_ref['value'] += 1
                combination_id = nrolc_utils.generate_combination_id(
                    cube.cube_id,
                    timestamp,
                    combination_counter_ref['value']
                )

                combination = COMBINATION()
                combination.combination_id = combination_id
                combination.name = f"Combination for {cell.cell_id}"
                combination.metric = metric

                # Add to list of combinations to create
                combinations.append(combination)

                # Create combination items for this new combination
                for item in cell_ordinate_items:
                    if item.variable_id:  # Only process items with variables
                        combination_item = COMBINATION_ITEM()
                        combination_item.combination_id = combination
                        combination_item.variable_id = item.variable_id
                        combination_item.member_id = item.member_id
                        combination_items.append(combination_item)

                # Cache this new combination
                combination_cache[signature] = combination
                logging.debug(f"Created new combination {combination.combination_id} for cell {cell.cell_id}")

            # Update the cell with the combination (new or reused)
            cell.table_cell_combination_id = combination
            cells_to_update.append(cell)

            # Create cube to combination link (always created, even for reused combinations)
            cube_to_combination = CUBE_TO_COMBINATION()
            cube_to_combination.cube_id = cube
            cube_to_combination.combination_id = combination
            cube_to_combinations.append(cube_to_combination)

        # Log deduplication statistics
        total_cells = len(cells)
        unique_combinations = len(combinations)
        reused_count = total_cells - unique_combinations
        if reused_count > 0:
            logging.info(f"Combination deduplication: {total_cells} cells mapped to {unique_combinations} unique combinations ({reused_count} reused)")
        else:
            logging.info(f"Created {unique_combinations} combinations for {total_cells} cells (no duplicates found)")

        return combinations, combination_items, cube_to_combinations, cells_to_update

    def create_cube_structure_items(self, ordinate_items, cube_structure):
        """
        Create CUBE_STRUCTURE_ITEMs from ordinate items.
        Skips EBA_ATY meta-variable (it's not a real variable for cube structures).

        Args:
            ordinate_items: List of ORDINATE_ITEM instances
            cube_structure: CUBE_STRUCTURE instance

        Returns:
            list: List of CUBE_STRUCTURE_ITEM instances
        """
        cube_structure_items = []

        # PASS 1: Collect variables (skip EBA_ATY meta-variable)
        variable_to_members = {}

        for item in ordinate_items:
            if item.variable_id:
                # Skip EBA_ATY meta-variable
                if self._is_metric_meta_variable(item.variable_id):
                    logging.info(f"EBA_ATY detected in ordinate items, skipping")
                    continue

                # Regular variable - add to collection
                var_id = item.variable_id.variable_id
                if var_id not in variable_to_members:
                    variable_to_members[var_id] = {
                        'variable': item.variable_id,
                        'members': set()
                    }

                if item.member_id:
                    variable_to_members[var_id]['members'].add(item.member_id)

        # PASS 2: Create cube structure items
        for var_id, var_data in variable_to_members.items():
            variable = var_data['variable']

            # Safety check: ensure EBA_ATY never makes it to cube structure
            if self._is_metric_meta_variable(variable):
                logging.error(f"SAFETY CHECK FAILED: EBA_ATY attempted to be added to cube structure")
                continue

            csi = CUBE_STRUCTURE_ITEM()
            csi.cube_structure_id = cube_structure
            csi.variable_id = variable
            csi.cube_variable_code = nrolc_utils.generate_csi_code(
                cube_structure.cube_structure_id,
                var_id
            )

            # Determine and set variable role based on member presence
            # - WITH member mappings → Dimension ('D')
            # - WITHOUT member mappings + Float/MNTRY domain → Observation ('O')
            # - WITHOUT member mappings + other domain → Attribute ('A')
            if var_data['members']:
                csi.role = 'D'  # Variables with member mappings are always Dimensions
            else:
                csi.role = self._determine_variable_role(variable)

            # Determine subdomain based on variable type
            if var_data['members']:
                # Variable with members: create output-specific subdomain
                subdomain = self.subdomain_manager.get_or_create_subdomain(
                    var_data['variable'],
                    var_data['members'],
                    cube_structure
                )
                csi.subdomain_id = subdomain
                csi.description = f"Variable with {len(var_data['members'])} members in subdomain"
            else:
                # Variable without members
                csi.description = f"Variable without member restrictions"

            cube_structure_items.append(csi)

        logging.info(f"Created {len(cube_structure_items)} cube structure items")
        return cube_structure_items
