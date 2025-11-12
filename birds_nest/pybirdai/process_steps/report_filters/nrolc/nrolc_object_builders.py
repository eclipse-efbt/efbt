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
    CUBE_STRUCTURE_ITEM, CUBE_TO_COMBINATION, SUBDOMAIN,
    SUBDOMAIN_ENUMERATION, DOMAIN, MEMBER, VARIABLE
)
from pybirdai.process_steps.report_filters.nrolc import nrolc_utils
from pybirdai.process_steps.report_filters.nrolc.measure_variable_utils import MeasureVariableConverter
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
        self.measure_converter = MeasureVariableConverter()

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

    def _get_aty_subdomain_members(self):
        """
        Retrieve all members from the EBA_ATY subdomain.

        Returns:
            set: Set of MEMBER instances from EBA_ATY subdomain
        """
        try:
            # Get the EBA_ATY domain
            aty_domain = DOMAIN.objects.get(domain_id=self.METRIC_META_VARIABLE_ID)

            # Get all subdomains for this domain
            aty_subdomains = SUBDOMAIN.objects.filter(domain_id=aty_domain)

            # Collect all members from all subdomains
            members = set()
            for subdomain in aty_subdomains:
                subdomain_enums = SUBDOMAIN_ENUMERATION.objects.filter(
                    subdomain_id=subdomain
                )
                for enum in subdomain_enums:
                    members.add(enum.member_id)

            logging.info(f"Retrieved {len(members)} members from EBA_ATY subdomain(s)")
            return members
        except DOMAIN.DoesNotExist:
            logging.warning(f"EBA_ATY domain not found")
            return set()
        except Exception as e:
            logging.error(f"Error retrieving EBA_ATY subdomain members: {e}")
            return set()

    def _get_or_create_domain_subdomain(self, variable, cube_structure):
        """
        Get or create a subdomain for a variable's domain.
        If the variable's domain has a configured subdomain, use it.
        Otherwise, create a new subdomain as a copy of the domain (all members).

        Args:
            variable: VARIABLE instance
            cube_structure: CUBE_STRUCTURE instance for naming

        Returns:
            SUBDOMAIN instance or None
        """
        if not variable or not variable.domain_id:
            return None

        domain = variable.domain_id

        # Check if domain already has a subdomain configured
        existing_subdomains = SUBDOMAIN.objects.filter(domain_id=domain)

        # If there's exactly one subdomain, use it
        if existing_subdomains.count() == 1:
            return existing_subdomains.first()

        # If multiple subdomains exist, prefer one with same ID as domain
        if existing_subdomains.count() > 1:
            domain_match = existing_subdomains.filter(subdomain_id=domain.domain_id).first()
            if domain_match:
                return domain_match
            # Otherwise use the first one
            return existing_subdomains.first()

        # No subdomain exists - create one as a copy of the domain
        # Get all members from the domain
        domain_members = MEMBER.objects.filter(domain_id=domain)

        listed = True
        if not domain_members.exists():
            listed = False
            logging.warning(f"No members found for domain {domain.domain_id}")


        # Create subdomain ID
        subdomain_id = f"{domain.domain_id}_OUTPUT_SD_{cube_structure.cube_structure_id}"

        # Check if subdomain already exists
        subdomain, created = SUBDOMAIN.objects.get_or_create(
            subdomain_id=subdomain_id,
            defaults={
                'name': f"{domain.name} Subdomain",
                'domain_id': domain,
                'is_listed': listed,
                'code': domain.code,
                'description': f"Copy of domain {domain.domain_id} for output layer"
            }
        )

        if created and listed:
            # Create subdomain enumerations for all domain members
            order = 1
            for member in domain_members:
                SUBDOMAIN_ENUMERATION.objects.get_or_create(
                    subdomain_id=subdomain,
                    member_id=member,
                    defaults={
                        'order': order,
                        'valid_from': None,
                        'valid_to': None
                    }
                )
                order += 1

            logging.info(f"Created subdomain {subdomain_id} with {domain_members.count()} members")

        return subdomain

    def create_cube_and_structure(self, table, framework_object):
        """
        Create CUBE and CUBE_STRUCTURE from table rendering.

        Args:
            table: TABLE instance
            framework_object: FRAMEWORK instance

        Returns:
            tuple: (cube, cube_structure)
        """
        # Create cube
        cube_name = table.table_id
        cube = CUBE()
        cube.cube_id = cube_name
        cube.name = table.name or cube_name
        cube.description = table.description
        cube.cube_type = 'RC'  # Report Cube
        cube.framework_id = framework_object

        # Create cube structure
        cube_structure = CUBE_STRUCTURE()
        cube_structure.cube_structure_id = table.table_id
        cube_structure.name = f"{cube.name} Structure"
        cube_structure.description = f"Structure for {cube.name}"

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
        Special handling for EBA_ATY: expands to all subdomain members as measure variables.

        Args:
            ordinate_items: List of ORDINATE_ITEM instances
            cube_structure: CUBE_STRUCTURE instance

        Returns:
            list: List of CUBE_STRUCTURE_ITEM instances
        """
        cube_structure_items = []

        # PASS 1: Collect variables and detect EBA_ATY
        variable_to_members = {}
        aty_members = set()
        has_aty = False

        for item in ordinate_items:
            if item.variable_id:
                is_aty = self._is_metric_meta_variable(item.variable_id)

                if is_aty:
                    has_aty = True
                    # Don't add ATY itself to variable collection
                    logging.info(f"EBA_ATY detected in ordinate items")
                    aty_members.add(item.member_id)
                else:
                    # Regular variable - add to collection
                    var_id = item.variable_id.variable_id
                    if var_id not in variable_to_members:
                        variable_to_members[var_id] = {
                            'variable': item.variable_id,
                            'members': set(),
                            'is_measure': False
                        }

                    if item.member_id:
                        variable_to_members[var_id]['members'].add(item.member_id)

        # PASS 2: If EBA_ATY detected, expand to all measure variables
        if has_aty:
            logging.info("Expanding EBA_ATY to all subdomain members as measure variables")

            for member in aty_members:
                # Convert ATY member to measure variable
                measure_var = self.measure_converter.get_or_create_measure_variable(member)

                if measure_var:
                    var_id = measure_var.variable_id
                    if var_id not in variable_to_members:
                        variable_to_members[var_id] = {
                            'variable': measure_var,
                            'members': set(),  # Measure variables don't have members
                            'is_measure': True
                        }
                        logging.debug(f"Added measure variable {var_id} from ATY member {member.member_id}")
                else:
                    logging.warning(f"Could not convert ATY member {member.member_id} to measure variable")

        # PASS 3: Create cube structure items
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

            # Determine subdomain based on variable type
            if var_data['is_measure']:
                # Measure variable: use domain subdomain or create copy
                subdomain = self._get_or_create_domain_subdomain(variable, cube_structure)
                if subdomain:
                    csi.subdomain_id = subdomain
                    csi.description = f"Measure variable with domain subdomain {subdomain.subdomain_id}"
                else:
                    csi.description = f"Measure variable without subdomain restrictions"
            elif var_data['members']:
                # Regular variable with members: create output-specific subdomain
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
