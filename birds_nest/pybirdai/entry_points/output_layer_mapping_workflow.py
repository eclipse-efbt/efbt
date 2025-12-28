"""
Entry point for the Output Layer Mapping Workflow.
This module orchestrates the complete workflow for creating output layer mappings.
"""

import os
import sys
import json
import logging
from datetime import datetime

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pybirdai.context import Context
from pybirdai.models.bird_meta_data_model import (
    TABLE, TABLE_CELL, COMBINATION, COMBINATION_ITEM,
    CUBE, CUBE_STRUCTURE, CUBE_STRUCTURE_ITEM, CUBE_TO_COMBINATION,
    MAPPING_DEFINITION, VARIABLE_MAPPING, VARIABLE_MAPPING_ITEM,
    MEMBER_MAPPING, MEMBER_MAPPING_ITEM,
    DOMAIN, MEMBER, SUBDOMAIN, SUBDOMAIN_ENUMERATION,
    VARIABLE, FRAMEWORK, MAINTENANCE_AGENCY
)
from pybirdai.process_steps.output_layer_mapping_workflow.mapping_orchestrator import OutputLayerMappingOrchestrator
from pybirdai.process_steps.output_layer_mapping_workflow.combination_creator import CombinationCreator
from pybirdai.process_steps.output_layer_mapping_workflow.domain_manager import DomainManager
from pybirdai.process_steps.output_layer_mapping_workflow.cube_structure_generator import CubeStructureGenerator

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class OutputLayerMappingWorkflow:
    """
    Main workflow orchestrator for output layer mapping creation.
    """

    def __init__(self, context=None, sdd_context=None):
        """
        Initialize the workflow with optional context.

        Args:
            context: Context object for file and configuration management
            sdd_context: SDD context for cube-to-combination mapping
        """
        self.context = context or Context()
        self.sdd_context = sdd_context
        self.orchestrator = OutputLayerMappingOrchestrator(sdd_context, self.context)
        self.domain_manager = DomainManager()
        self.cube_generator = CubeStructureGenerator()

        # Get or create maintenance agency
        self.maintenance_agency = self._get_or_create_maintenance_agency()

    def _get_or_create_maintenance_agency(self):
        """Get or create the default maintenance agency."""
        agency = MAINTENANCE_AGENCY.objects.first()
        if not agency:
            agency = MAINTENANCE_AGENCY.objects.create(
                maintenance_agency_id='EFBT',
                name='EFBT System',
                code='EFBT'
            )
            logger.info("Created default maintenance agency: EFBT")
        return agency

    def run_interactive(self):
        """
        Run the workflow in interactive mode.
        This is the main entry point when called from command line.
        """
        logger.info("Starting Output Layer Mapping Workflow (Interactive Mode)")

        print("\n=== Output Layer Mapping Workflow ===\n")

        # Step 1: Select table
        table = self._select_table_interactive()
        if not table:
            logger.error("No table selected. Exiting.")
            return

        # Step 2: Define variable breakdowns
        breakdowns = self._define_breakdowns_interactive(table)
        if not breakdowns:
            logger.error("No breakdowns defined. Exiting.")
            return

        # Step 3: Create mappings
        mappings = self._create_mappings_interactive(breakdowns)
        if not mappings:
            logger.error("No mappings created. Exiting.")
            return

        # Step 4: Name the mapping
        mapping_name = self._get_mapping_name_interactive()
        if not mapping_name:
            logger.error("No mapping name provided. Exiting.")
            return

        # Step 5: Generate structures
        success = self._generate_structures(
            table=table,
            breakdowns=breakdowns,
            mappings=mappings,
            mapping_name=mapping_name
        )

        if success:
            print("\n✓ Output layer mapping created successfully!")
        else:
            print("\n✗ Failed to create output layer mapping.")

    def _select_table_interactive(self):
        """Interactive table selection."""
        print("\n--- Step 1: Select Table ---")

        # List available frameworks
        frameworks = FRAMEWORK.objects.all()
        if not frameworks:
            print("No frameworks found in database.")
            return None

        print("\nAvailable frameworks:")
        for i, fw in enumerate(frameworks, 1):
            print(f"  {i}. {fw.framework_id} - {fw.name}")

        fw_choice = input("\nSelect framework number: ").strip()
        try:
            framework = frameworks[int(fw_choice) - 1]
        except (ValueError, IndexError):
            print("Invalid selection.")
            return None

        # List tables for selected framework
        tables = TABLE.objects.all()  # You might want to filter by framework
        if not tables:
            print("No tables found.")
            return None

        print(f"\nAvailable tables:")
        for i, table in enumerate(tables[:20], 1):  # Show first 20
            print(f"  {i}. {table.code} - {table.name}")

        table_choice = input("\nSelect table number: ").strip()
        try:
            table = tables[int(table_choice) - 1]
            logger.info(f"Selected table: {table.table_id}")
            return table
        except (ValueError, IndexError):
            print("Invalid selection.")
            return None

    def _define_breakdowns_interactive(self, table):
        """Interactive breakdown definition."""
        print("\n--- Step 2: Define Variable Breakdowns ---")

        # Get cells and combinations for the table
        cells = TABLE_CELL.objects.filter(table_id=table)
        if not cells:
            print("No cells found for this table.")
            return {}

        # Get unique combinations
        combination_ids = cells.values_list('table_cell_combination_id', flat=True).distinct()
        combinations = COMBINATION.objects.filter(combination_id__in=combination_ids)

        # Get variables from combinations
        combo_items = COMBINATION_ITEM.objects.filter(
            combination_id__in=combinations
        ).select_related('variable_id')

        variables = {}
        for item in combo_items:
            if item.variable_id:
                var_id = item.variable_id.variable_id
                if var_id not in variables:
                    variables[var_id] = item.variable_id

        if not variables:
            print("No variables found in combinations.")
            return {}

        print(f"\nFound {len(variables)} variables in table combinations.")

        breakdowns = {}
        for var_id, variable in list(variables.items())[:5]:  # Process first 5 for demo
            print(f"\nVariable: {variable.name} ({var_id})")

            source = input("  Enter source variable (or press Enter to skip): ").strip()
            if source:
                rule = input("  Enter transformation rule (DIRECT_MAP/SUM/AVG/etc.): ").strip()
                breakdowns[var_id] = {
                    'source': source or var_id,
                    'rule': rule or 'DIRECT_MAP'
                }

        logger.info(f"Defined {len(breakdowns)} breakdowns")
        return breakdowns

    def _create_mappings_interactive(self, breakdowns):
        """Interactive mapping creation."""
        print("\n--- Step 3: Create Mappings ---")

        mappings = {
            'dimensions': {},
            'measures': {},
            'filters': {}
        }

        for var_id, breakdown in breakdowns.items():
            # Simplified logic - classify based on variable ID pattern
            if var_id.startswith('TYP_') or var_id.endswith('_ID'):
                print(f"\nDimension mapping for: {var_id}")
                mappings['dimensions'][var_id] = {
                    'source': breakdown['source'],
                    'target': var_id,
                    'rule': breakdown.get('rule', 'DIRECT_MAP')
                }
            else:
                print(f"\nMeasure mapping for: {var_id}")
                mappings['measures'][var_id] = {
                    'source': breakdown['source'],
                    'target': var_id,
                    'aggregation': breakdown.get('rule', 'SUM')
                }

        # Option to add filters
        add_filter = input("\nAdd filter conditions? (y/n): ").strip().lower()
        if add_filter == 'y':
            filter_var = input("  Filter variable: ").strip()
            filter_op = input("  Operator (=, !=, >, <, IN, NOT IN): ").strip()
            filter_val = input("  Value: ").strip()

            if filter_var and filter_op and filter_val:
                mappings['filters']['filter_0'] = {
                    'variable': filter_var,
                    'operator': filter_op,
                    'value': filter_val
                }

        logger.info(f"Created mappings: {len(mappings['dimensions'])} dimensions, "
                    f"{len(mappings['measures'])} measures, {len(mappings['filters'])} filters")
        return mappings

    def _get_mapping_name_interactive(self):
        """Interactive mapping name input."""
        print("\n--- Step 4: Name the Mapping ---")

        name = input("\nEnter a name for this mapping: ").strip()
        if not name:
            return None

        description = input("Enter description (optional): ").strip()

        logger.info(f"Mapping name: {name}")
        return {
            'name': name,
            'description': description
        }

    def _generate_structures(self, table, breakdowns, mappings, mapping_name):
        """
        Generate all required structures.

        Args:
            table: TABLE object
            breakdowns: Dictionary of variable breakdowns
            mappings: Dictionary of dimension/measure/filter mappings
            mapping_name: Dictionary with name and description

        Returns:
            bool: True if successful, False otherwise
        """
        print("\n--- Step 5: Generating Structures ---")

        try:
            timestamp = datetime.now().strftime('%Y%m%d%H%M%S')

            # Generate internal ID
            from pybirdai.process_steps.output_layer_mapping_workflow.naming_utils import NamingUtils
            internal_id = NamingUtils.generate_internal_id(mapping_name['name'])

            print(f"Internal ID: {internal_id}")

            # Create variable mapping
            print("Creating variable mapping...")
            var_mapping = self._create_variable_mapping(
                internal_id, mapping_name['name'], mappings, timestamp
            )

            # Create member mapping if needed
            mem_mapping = None
            if mappings.get('dimensions'):
                print("Creating member mapping...")
                mem_mapping = self._create_member_mapping(
                    internal_id, mapping_name['name'], mappings, timestamp
                )

            # Create mapping definition
            print("Creating mapping definition...")
            mapping_def = self._create_mapping_definition(
                internal_id, mapping_name, var_mapping, mem_mapping, mappings, timestamp
            )

            # Create cube structure
            print("Creating cube structure...")
            cube_structure = self._create_cube_structure(
                table, internal_id, mapping_name, mappings, timestamp
            )

            # Create cube
            print("Creating cube...")
            cube = self._create_cube(
                table, cube_structure, internal_id, mapping_name, timestamp
            )

            # Create non-reference combinations
            print("Creating non-reference combinations...")
            self._create_combinations(table, cube, timestamp)

            # Update domains if needed
            print("Updating domains...")
            self._update_domains(mappings)

            logger.info("Successfully generated all structures")
            print("\n✓ All structures created successfully!")

            print(f"\nGenerated IDs:")
            print(f"  Variable Mapping: {var_mapping.variable_mapping_id}")
            print(f"  Mapping Definition: {mapping_def.mapping_id}")
            print(f"  Cube Structure: {cube_structure.cube_structure_id}")
            print(f"  Cube: {cube.cube_id}")

            return True

        except Exception as e:
            logger.error(f"Error generating structures: {str(e)}")
            print(f"\n✗ Error: {str(e)}")
            return False

    def _create_variable_mapping(self, internal_id, name, mappings, timestamp):
        """Create VARIABLE_MAPPING and items."""
        var_mapping = VARIABLE_MAPPING.objects.create(
            variable_mapping_id=f"{internal_id}_VAR_MAP_{timestamp}",
            maintenance_agency_id=self.maintenance_agency,
            name=name,
            code=internal_id
        )

        # Create mapping items
        all_mappings = list(mappings['dimensions'].items()) + list(mappings['measures'].items())
        for var_id, mapping_def in all_mappings:
            # Source
            source_var = VARIABLE.objects.filter(variable_id=mapping_def['source']).first()
            if source_var:
                VARIABLE_MAPPING_ITEM.objects.create(
                    variable_mapping_id=var_mapping,
                    variable_id=source_var,
                    is_source="true"
                )

            # Target
            target_var = VARIABLE.objects.filter(
                variable_id=mapping_def.get('target', var_id)
            ).first()
            if target_var:
                VARIABLE_MAPPING_ITEM.objects.create(
                    variable_mapping_id=var_mapping,
                    variable_id=target_var,
                    is_source="false"
                )

        return var_mapping

    def _create_member_mapping(self, internal_id, name, mappings, timestamp):
        """Create MEMBER_MAPPING and items."""
        mem_mapping = MEMBER_MAPPING.objects.create(
            member_mapping_id=f"{internal_id}_MEM_MAP_{timestamp}",
            maintenance_agency_id=self.maintenance_agency,
            name=f"{name} - Member Mappings",
            code=f"{internal_id}_MEM"
        )

        # Create member mapping items
        row_counter = 1
        for var_id in mappings['dimensions']:
            variable = VARIABLE.objects.filter(variable_id=var_id).first()
            if variable and hasattr(variable, 'domain_id') and variable.domain_id:
                members = MEMBER.objects.filter(domain_id=variable.domain_id)
                for member in members[:10]:  # Limit for demo
                    MEMBER_MAPPING_ITEM.objects.create(
                        member_mapping_id=mem_mapping,
                        member_mapping_row=str(row_counter),
                        variable_id=variable,
                        is_source="true",
                        member_id=member
                    )
                    row_counter += 1

        return mem_mapping

    def _create_mapping_definition(self, internal_id, mapping_name, var_mapping,
                                    mem_mapping, mappings, timestamp):
        """Create MAPPING_DEFINITION."""
        # Build algorithm
        algorithm_parts = []
        for var_id, mapping_def in mappings['dimensions'].items():
            algorithm_parts.append(
                f"DIM: {mapping_def['source']} -> {mapping_def.get('target', var_id)} "
                f"[{mapping_def.get('rule', 'DIRECT_MAP')}]"
            )

        for var_id, mapping_def in mappings['measures'].items():
            algorithm_parts.append(
                f"MEASURE: {mapping_def['source']} -> {mapping_def.get('target', var_id)} "
                f"[{mapping_def.get('aggregation', 'SUM')}]"
            )

        algorithm = "\n".join(algorithm_parts)

        mapping_def = MAPPING_DEFINITION.objects.create(
            mapping_id=f"{internal_id}_MAP_DEF_{timestamp}",
            maintenance_agency_id=self.maintenance_agency,
            name=mapping_name['name'],
            code=internal_id,
            mapping_type="VARIABLE_TO_VARIABLE",
            algorithm=algorithm,
            variable_mapping_id=var_mapping,
            member_mapping_id=mem_mapping
        )

        return mapping_def

    def _create_cube_structure(self, table, internal_id, mapping_name, mappings, timestamp):
        """Create CUBE_STRUCTURE and items."""
        cube_structure = CUBE_STRUCTURE.objects.create(
            cube_structure_id=f"{table.code}_REF_STRUCTURE_{timestamp}",
            maintenance_agency_id=self.maintenance_agency,
            name=f"Reference structure for {mapping_name['name']}",
            code=f"{internal_id}_CS",
            description=mapping_name.get('description', ''),
            version=table.version if table.version else '1.0'
        )

        # Create cube structure items
        order = 1

        # Dimensions
        for var_id in mappings['dimensions']:
            variable = VARIABLE.objects.filter(variable_id=var_id).first()
            if variable:
                # Create or get subdomain (returns tuple: subdomain, single_member)
                subdomain, single_member = self.cube_generator.create_or_get_subdomain(
                    variable, cube_structure.cube_structure_id
                )

                CUBE_STRUCTURE_ITEM.objects.create(
                    cube_structure_id=cube_structure,
                    cube_variable_code=f"{cube_structure.code}__{var_id}",
                    variable_id=variable,
                    role="D",  # Dimension
                    order=order,
                    subdomain_id=subdomain,
                    member_id=single_member,
                    dimension_type="B",  # Business
                    is_mandatory=True
                )
                order += 1

        # Measures
        for var_id in mappings['measures']:
            variable = VARIABLE.objects.filter(variable_id=var_id).first()
            if variable:
                CUBE_STRUCTURE_ITEM.objects.create(
                    cube_structure_id=cube_structure,
                    cube_variable_code=f"{cube_structure.code}__{var_id}",
                    variable_id=variable,
                    role="O",  # Observation
                    order=order,
                    is_mandatory=True,
                    is_flow=True
                )
                order += 1

        return cube_structure

    def _create_cube(self, table, cube_structure, internal_id, mapping_name, timestamp):
        """Create CUBE."""
        # Try to get framework from table or use default
        framework = FRAMEWORK.objects.first()

        cube = CUBE.objects.create(
            cube_id=f"{table.code}_REF_CUBE_{timestamp}",
            maintenance_agency_id=self.maintenance_agency,
            name=f"{mapping_name['name']}",
            code=f"{internal_id}_CUBE",
            framework_id=framework,
            cube_structure_id=cube_structure,
            cube_type="RC",  # Reference Cube
            is_allowed=True,
            published=False,
            version=table.version if table.version else '1.0'
        )

        return cube

    def _create_combinations(self, table, cube, timestamp):
        """Create non-reference combinations and link to cube."""
        cells = TABLE_CELL.objects.filter(table_id=table)

        # Create combination creator with table code, version, and context
        table_code = table.code if hasattr(table, 'code') else 'TABLE'
        table_version = table.version.replace('.', '_') if hasattr(table, 'version') and table.version else '1_0'
        combination_creator = CombinationCreator(
            table_code, table_version, self.sdd_context, self.context
        )

        counter = 1
        for cell in cells[:10]:  # Limit for demo
            # Create combination
            combination = combination_creator.create_combination_for_cell(
                cell, cube, timestamp
            )

            if combination:
                # Add to sdd_context mapping if available
                if self.sdd_context is not None:
                    cube_to_comb = CUBE_TO_COMBINATION()
                    cube_to_comb.combination_id = combination
                    cube_to_comb.cube_id = cube
                    self.sdd_context.combination_to_rol_cube_map.setdefault(
                        cube.cube_id, []
                    ).append(cube_to_comb)

                    # Also save to database if context allows
                    if self.context and self.context.save_derived_sdd_items:
                        cube_to_comb.save()
                else:
                    # Fallback: create link directly in database
                    CUBE_TO_COMBINATION.objects.create(
                        cube_id=cube,
                        combination_id=combination
                    )
                counter += 1

        logger.info(f"Created {counter} combinations")

    def _update_domains(self, mappings):
        """Update domains and members as needed."""
        for var_id in mappings['dimensions']:
            variable = VARIABLE.objects.filter(variable_id=var_id).first()
            if variable:
                self.domain_manager.ensure_domain_and_members(
                    variable, self.maintenance_agency
                )

    def run_automated(self, config_file):
        """
        Run the workflow in automated mode using a configuration file.

        Args:
            config_file: Path to JSON configuration file
        """
        logger.info(f"Starting Output Layer Mapping Workflow (Automated Mode)")
        logger.info(f"Configuration file: {config_file}")

        try:
            with open(config_file, 'r') as f:
                config = json.load(f)

            # Extract configuration
            table_id = config['table_id']
            breakdowns = config['breakdowns']
            mappings = config['mappings']
            mapping_name = config['mapping_name']

            # Get table
            table = TABLE.objects.get(table_id=table_id)

            # Generate structures
            success = self._generate_structures(
                table=table,
                breakdowns=breakdowns,
                mappings=mappings,
                mapping_name=mapping_name
            )

            if success:
                logger.info("Automated workflow completed successfully")
            else:
                logger.error("Automated workflow failed")

            return success

        except Exception as e:
            logger.error(f"Error in automated workflow: {str(e)}")
            return False


def main():
    """
    Main entry point for the script.
    """
    import argparse

    parser = argparse.ArgumentParser(
        description='Output Layer Mapping Workflow'
    )
    parser.add_argument(
        '--config',
        help='Configuration file for automated mode'
    )
    parser.add_argument(
        '--interactive',
        action='store_true',
        help='Run in interactive mode'
    )

    args = parser.parse_args()

    # Initialize Django
    import django
    django.setup()

    # Create workflow instance
    workflow = OutputLayerMappingWorkflow()

    if args.config:
        # Automated mode
        success = workflow.run_automated(args.config)
        sys.exit(0 if success else 1)
    else:
        # Interactive mode (default)
        workflow.run_interactive()


if __name__ == '__main__':
    main()