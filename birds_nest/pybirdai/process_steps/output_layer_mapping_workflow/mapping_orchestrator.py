"""
Main orchestrator for the Output Layer Mapping workflow.
Coordinates the creation of mappings, cube structures, and combinations.
"""

import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from pybirdai.models.bird_meta_data_model import (
    TABLE, TABLE_CELL, COMBINATION, COMBINATION_ITEM,
    CUBE, CUBE_STRUCTURE, CUBE_STRUCTURE_ITEM, CUBE_TO_COMBINATION,
    MAPPING_DEFINITION, VARIABLE_MAPPING, VARIABLE_MAPPING_ITEM,
    MEMBER_MAPPING, MEMBER_MAPPING_ITEM, MAPPING_TO_CUBE,
    DOMAIN, MEMBER, SUBDOMAIN, SUBDOMAIN_ENUMERATION,
    VARIABLE, FRAMEWORK, MAINTENANCE_AGENCY
)

logger = logging.getLogger(__name__)


class OutputLayerMappingOrchestrator:
    """
    Orchestrates the complete output layer mapping workflow.
    Manages the creation of all required metadata structures.
    """

    def __init__(self):
        """Initialize the orchestrator."""
        self.timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        self.created_objects = {
            'variable_mappings': [],
            'member_mappings': [],
            'mapping_definitions': [],
            'cube_structures': [],
            'cubes': [],
            'combinations': []
        }

    def create_complete_mapping(
        self,
        table: TABLE,
        mapping_name: str,
        mapping_config: Dict,
        maintenance_agency: MAINTENANCE_AGENCY
    ) -> Dict:
        """
        Create a complete output layer mapping with all required structures.

        Args:
            table: The TABLE object to map
            mapping_name: User-friendly name for the mapping
            mapping_config: Dictionary containing mapping configuration
            maintenance_agency: The maintenance agency

        Returns:
            Dict containing created object IDs and status
        """
        logger.info(f"Starting complete mapping creation for table {table.table_id}")

        try:
            # Step 1: Create Variable Mapping
            variable_mapping = self._create_variable_mapping(
                mapping_name, mapping_config, maintenance_agency
            )
            self.created_objects['variable_mappings'].append(variable_mapping)

            # Step 2: Create Member Mapping (if dimensions exist)
            member_mapping = None
            if mapping_config.get('dimensions'):
                member_mapping = self._create_member_mapping(
                    mapping_name, mapping_config, maintenance_agency
                )
                self.created_objects['member_mappings'].append(member_mapping)

            # Step 3: Create Mapping Definition
            mapping_def = self._create_mapping_definition(
                mapping_name,
                mapping_config,
                variable_mapping,
                member_mapping,
                maintenance_agency
            )
            self.created_objects['mapping_definitions'].append(mapping_def)

            # Step 4: Create Cube Structure
            cube_structure = self._create_cube_structure(
                table, mapping_name, mapping_config, maintenance_agency
            )
            self.created_objects['cube_structures'].append(cube_structure)

            # Step 5: Create Cube
            cube = self._create_cube(
                table, cube_structure, mapping_name, maintenance_agency
            )
            self.created_objects['cubes'].append(cube)

            # Step 6: Create Combinations
            combinations = self._create_combinations_for_table(
                table, cube, maintenance_agency
            )
            self.created_objects['combinations'].extend(combinations)

            # Step 7: Create Mapping to Cube link
            self._create_mapping_to_cube(mapping_def, cube)

            logger.info(f"Successfully created complete mapping: {mapping_name}")

            return {
                'success': True,
                'variable_mapping_id': variable_mapping.variable_mapping_id,
                'member_mapping_id': member_mapping.member_mapping_id if member_mapping else None,
                'mapping_definition_id': mapping_def.mapping_id,
                'cube_structure_id': cube_structure.cube_structure_id,
                'cube_id': cube.cube_id,
                'combinations_created': len(combinations)
            }

        except Exception as e:
            logger.error(f"Error creating complete mapping: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }

    def _create_variable_mapping(
        self,
        mapping_name: str,
        mapping_config: Dict,
        maintenance_agency: MAINTENANCE_AGENCY
    ) -> VARIABLE_MAPPING:
        """Create VARIABLE_MAPPING and its items."""
        from .naming_utils import NamingUtils

        internal_id = NamingUtils.generate_internal_id(mapping_name)

        variable_mapping = VARIABLE_MAPPING.objects.create(
            variable_mapping_id=f"{internal_id}_VAR_MAP_{self.timestamp}",
            maintenance_agency_id=maintenance_agency,
            name=mapping_name,
            code=internal_id
        )

        # Create items for dimensions and measures
        all_mappings = []
        if 'dimensions' in mapping_config:
            all_mappings.extend(mapping_config['dimensions'].items())
        if 'measures' in mapping_config:
            all_mappings.extend(mapping_config['measures'].items())

        for var_id, mapping_def in all_mappings:
            # Create source item
            source_var = VARIABLE.objects.filter(
                variable_id=mapping_def.get('source', var_id)
            ).first()
            if source_var:
                VARIABLE_MAPPING_ITEM.objects.create(
                    variable_mapping_id=variable_mapping,
                    variable_id=source_var,
                    is_source="true"
                )

            # Create target item
            target_var = VARIABLE.objects.filter(
                variable_id=mapping_def.get('target', var_id)
            ).first()
            if target_var:
                VARIABLE_MAPPING_ITEM.objects.create(
                    variable_mapping_id=variable_mapping,
                    variable_id=target_var,
                    is_source="false"
                )

        logger.info(f"Created variable mapping: {variable_mapping.variable_mapping_id}")
        return variable_mapping

    def _create_member_mapping(
        self,
        mapping_name: str,
        mapping_config: Dict,
        maintenance_agency: MAINTENANCE_AGENCY
    ) -> MEMBER_MAPPING:
        """Create MEMBER_MAPPING and its items."""
        from .naming_utils import NamingUtils

        internal_id = NamingUtils.generate_internal_id(mapping_name)

        member_mapping = MEMBER_MAPPING.objects.create(
            member_mapping_id=f"{internal_id}_MEM_MAP_{self.timestamp}",
            maintenance_agency_id=maintenance_agency,
            name=f"{mapping_name} - Member Mappings",
            code=f"{internal_id}_MEM"
        )

        # Create member mapping items for dimensions
        row_counter = 1
        for var_id, mapping_def in mapping_config.get('dimensions', {}).items():
            variable = VARIABLE.objects.filter(variable_id=var_id).first()

            if variable and hasattr(variable, 'domain_id') and variable.domain_id:
                # Get members from the domain
                members = MEMBER.objects.filter(domain_id=variable.domain_id)

                for member in members:
                    # Create source member mapping item
                    MEMBER_MAPPING_ITEM.objects.create(
                        member_mapping_id=member_mapping,
                        member_mapping_row=str(row_counter),
                        variable_id=variable,
                        is_source="true",
                        member_id=member
                    )

                    # If there's a target mapping, create target item
                    target_var_id = mapping_def.get('target')
                    if target_var_id and target_var_id != var_id:
                        target_var = VARIABLE.objects.filter(variable_id=target_var_id).first()
                        if target_var:
                            # Try to find equivalent member in target domain
                            if hasattr(target_var, 'domain_id') and target_var.domain_id:
                                target_member = MEMBER.objects.filter(
                                    domain_id=target_var.domain_id,
                                    code=member.code
                                ).first()
                                if target_member:
                                    MEMBER_MAPPING_ITEM.objects.create(
                                        member_mapping_id=member_mapping,
                                        member_mapping_row=str(row_counter),
                                        variable_id=target_var,
                                        is_source="false",
                                        member_id=target_member
                                    )

                    row_counter += 1

        logger.info(f"Created member mapping: {member_mapping.member_mapping_id}")
        return member_mapping

    def _create_mapping_definition(
        self,
        mapping_name: str,
        mapping_config: Dict,
        variable_mapping: VARIABLE_MAPPING,
        member_mapping: Optional[MEMBER_MAPPING],
        maintenance_agency: MAINTENANCE_AGENCY
    ) -> MAPPING_DEFINITION:
        """Create MAPPING_DEFINITION with algorithm description."""
        from .naming_utils import NamingUtils

        internal_id = NamingUtils.generate_internal_id(mapping_name)

        # Build algorithm description
        algorithm_lines = []

        # Add dimension mappings
        if 'dimensions' in mapping_config:
            algorithm_lines.append("=== DIMENSION MAPPINGS ===")
            for var_id, mapping_def in mapping_config['dimensions'].items():
                source = mapping_def.get('source', var_id)
                target = mapping_def.get('target', var_id)
                rule = mapping_def.get('rule', 'DIRECT_MAP')
                when = mapping_def.get('when', '')

                algo_text = f"SOURCE: {source}\nTARGET: {target}\nRULE: {rule}"
                if when:
                    algo_text += f"\nWHEN: {when}"
                algorithm_lines.append(algo_text)

        # Add measure mappings
        if 'measures' in mapping_config:
            algorithm_lines.append("\n=== MEASURE MAPPINGS ===")
            for var_id, mapping_def in mapping_config['measures'].items():
                source = mapping_def.get('source', var_id)
                target = mapping_def.get('target', var_id)
                aggregation = mapping_def.get('aggregation', 'SUM')
                formula = mapping_def.get('formula', '')

                algo_text = f"MEASURE: {source}\nTARGET: {target}\nAGGREGATION: {aggregation}"
                if formula:
                    algo_text += f"\nFORMULA: {formula}"
                algorithm_lines.append(algo_text)

        # Add filters
        if 'filters' in mapping_config:
            algorithm_lines.append("\n=== FILTERS ===")
            for filter_id, filter_def in mapping_config['filters'].items():
                algo_text = (
                    f"VARIABLE: {filter_def['variable']}\n"
                    f"OPERATOR: {filter_def['operator']}\n"
                    f"VALUE: {filter_def['value']}"
                )
                algorithm_lines.append(algo_text)

        algorithm = "\n\n".join(algorithm_lines)

        mapping_def = MAPPING_DEFINITION.objects.create(
            mapping_id=f"{internal_id}_MAP_DEF_{self.timestamp}",
            maintenance_agency_id=maintenance_agency,
            name=mapping_name,
            code=internal_id,
            mapping_type="VARIABLE_TO_VARIABLE",
            algorithm=algorithm,
            variable_mapping_id=variable_mapping,
            member_mapping_id=member_mapping
        )

        logger.info(f"Created mapping definition: {mapping_def.mapping_id}")
        return mapping_def

    def _create_cube_structure(
        self,
        table: TABLE,
        mapping_name: str,
        mapping_config: Dict,
        maintenance_agency: MAINTENANCE_AGENCY
    ) -> CUBE_STRUCTURE:
        """Create CUBE_STRUCTURE and its items."""
        from .naming_utils import NamingUtils
        from .cube_structure_generator import CubeStructureGenerator

        internal_id = NamingUtils.generate_internal_id(mapping_name)
        generator = CubeStructureGenerator()

        # Create cube structure
        cube_structure = CUBE_STRUCTURE.objects.create(
            cube_structure_id=f"{table.code}_REF_STRUCTURE_{self.timestamp}",
            maintenance_agency_id=maintenance_agency,
            name=f"Reference structure for {mapping_name}",
            code=f"{internal_id}_CS",
            description=f"Generated reference cube structure for {table.name}",
            version=table.version if table.version else '1.0'
        )

        order_counter = 1

        # Create cube structure items for dimensions
        if 'dimensions' in mapping_config:
            for var_id, mapping_def in mapping_config['dimensions'].items():
                target_var_id = mapping_def.get('target', var_id)
                variable = VARIABLE.objects.filter(variable_id=target_var_id).first()

                if variable:
                    # Create or get subdomain (returns tuple: subdomain, single_member)
                    subdomain, single_member = generator.create_or_get_subdomain(
                        variable, cube_structure.cube_structure_id
                    )

                    CUBE_STRUCTURE_ITEM.objects.create(
                        cube_structure_id=cube_structure,
                        cube_variable_code=f"{cube_structure.code}__{target_var_id}",
                        variable_id=variable,
                        role="D",  # Dimension
                        order=order_counter,
                        subdomain_id=subdomain,
                        member_id=single_member,
                        dimension_type=self._determine_dimension_type(variable),
                        is_mandatory=True,
                        is_implemented=True,
                        description=f"Dimension: {variable.name}"
                    )
                    order_counter += 1

        # Create cube structure items for measures
        if 'measures' in mapping_config:
            for var_id, mapping_def in mapping_config['measures'].items():
                target_var_id = mapping_def.get('target', var_id)
                variable = VARIABLE.objects.filter(variable_id=target_var_id).first()

                if variable:
                    CUBE_STRUCTURE_ITEM.objects.create(
                        cube_structure_id=cube_structure,
                        cube_variable_code=f"{cube_structure.code}__{target_var_id}",
                        variable_id=variable,
                        role="O",  # Observation/Metric
                        order=order_counter,
                        is_mandatory=True,
                        is_implemented=True,
                        is_flow=True,
                        description=f"Measure: {variable.name}"
                    )
                    order_counter += 1

        logger.info(f"Created cube structure: {cube_structure.cube_structure_id}")
        return cube_structure

    def _determine_dimension_type(self, variable: VARIABLE) -> str:
        """
        Determine the dimension type based on variable characteristics.

        Args:
            variable: The VARIABLE object

        Returns:
            str: Dimension type code (B, M, T, or U)
        """
        var_id = variable.variable_id.upper()

        # Temporal dimensions
        if any(term in var_id for term in ['DATE', 'TIME', 'PERIOD', 'YEAR', 'MONTH', 'DAY']):
            return "T"

        # Unit dimensions
        if any(term in var_id for term in ['UNIT', 'CURRENCY', 'CCY']):
            return "U"

        # Methodological dimensions
        if any(term in var_id for term in ['METHOD', 'APPROACH', 'CALC']):
            return "M"

        # Default to Business dimension
        return "B"

    def _create_cube(
        self,
        table: TABLE,
        cube_structure: CUBE_STRUCTURE,
        mapping_name: str,
        maintenance_agency: MAINTENANCE_AGENCY
    ) -> CUBE:
        """Create CUBE linked to the cube structure."""
        from .naming_utils import NamingUtils

        internal_id = NamingUtils.generate_internal_id(mapping_name)

        # Try to get framework from context or use first available
        framework = FRAMEWORK.objects.first()
        if not framework:
            # Create a default framework if none exists
            framework = FRAMEWORK.objects.create(
                framework_id='DEFAULT',
                name='Default Framework',
                code='DEFAULT'
            )

        cube = CUBE.objects.create(
            cube_id=f"{table.code}_REF_CUBE_{self.timestamp}",
            maintenance_agency_id=maintenance_agency,
            name=f"Reference cube for {mapping_name}",
            code=f"{internal_id}_CUBE",
            framework_id=framework,
            cube_structure_id=cube_structure,
            cube_type="RC",  # Reference Cube
            is_allowed=True,
            published=False,
            version=table.version if table.version else '1.0',
            description=f"Generated reference cube for mapping: {mapping_name}"
        )

        logger.info(f"Created cube: {cube.cube_id}")
        return cube

    def _create_combinations_for_table(
        self,
        table: TABLE,
        cube: CUBE,
        maintenance_agency: MAINTENANCE_AGENCY
    ) -> List[COMBINATION]:
        """Create non-reference combinations for all cells in the table."""
        from .combination_creator import CombinationCreator

        # Extract table code and version for combination naming
        table_code = table.code if hasattr(table, 'code') else 'TABLE'
        table_version = table.version.replace('.', '_') if hasattr(table, 'version') and table.version else '1_0'

        creator = CombinationCreator(table_code, table_version)
        cells = TABLE_CELL.objects.filter(table_id=table)
        combinations = []

        for cell in cells:
            combination = creator.create_combination_for_cell(
                cell, cube, self.timestamp
            )
            if combination:
                combinations.append(combination)

                # Create CUBE_TO_COMBINATION link
                CUBE_TO_COMBINATION.objects.create(
                    cube_id=cube,
                    combination_id=combination
                )

        logger.info(f"Created {len(combinations)} combinations for table {table.table_id}")
        return combinations

    def _create_mapping_to_cube(
        self,
        mapping_def: MAPPING_DEFINITION,
        cube: CUBE
    ) -> MAPPING_TO_CUBE:
        """Create MAPPING_TO_CUBE link."""
        mapping_to_cube = MAPPING_TO_CUBE.objects.create(
            cube_mapping_id=f"{mapping_def.mapping_id}_TO_{cube.cube_id}",
            mapping_id=mapping_def
        )

        logger.info(f"Created mapping to cube link: {mapping_to_cube.cube_mapping_id}")
        return mapping_to_cube

    def validate_mapping_config(self, mapping_config: Dict) -> Tuple[bool, List[str]]:
        """
        Validate the mapping configuration.

        Args:
            mapping_config: Dictionary containing mapping configuration

        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        errors = []

        # Check for required sections
        if not mapping_config.get('dimensions') and not mapping_config.get('measures'):
            errors.append("Mapping must contain at least dimensions or measures")

        # Validate dimensions
        if 'dimensions' in mapping_config:
            for var_id, mapping_def in mapping_config['dimensions'].items():
                if not mapping_def.get('source'):
                    errors.append(f"Dimension {var_id} missing source")
                # Check if source variable exists
                source_var = VARIABLE.objects.filter(
                    variable_id=mapping_def.get('source')
                ).first()
                if not source_var:
                    errors.append(f"Source variable {mapping_def.get('source')} not found")

        # Validate measures
        if 'measures' in mapping_config:
            for var_id, mapping_def in mapping_config['measures'].items():
                if not mapping_def.get('source'):
                    errors.append(f"Measure {var_id} missing source")
                # Check if source variable exists
                source_var = VARIABLE.objects.filter(
                    variable_id=mapping_def.get('source')
                ).first()
                if not source_var:
                    errors.append(f"Source variable {mapping_def.get('source')} not found")

        # Validate filters
        if 'filters' in mapping_config:
            for filter_id, filter_def in mapping_config['filters'].items():
                if not filter_def.get('variable'):
                    errors.append(f"Filter {filter_id} missing variable")
                if not filter_def.get('operator'):
                    errors.append(f"Filter {filter_id} missing operator")
                if not filter_def.get('value'):
                    errors.append(f"Filter {filter_id} missing value")

        is_valid = len(errors) == 0
        return is_valid, errors

    def rollback_created_objects(self):
        """
        Rollback all objects created during the current orchestration.
        This is useful if an error occurs partway through.
        """
        logger.warning("Rolling back created objects")

        # Delete in reverse order of creation to handle dependencies
        for combination in self.created_objects['combinations']:
            combination.delete()

        for cube in self.created_objects['cubes']:
            cube.delete()

        for cube_structure in self.created_objects['cube_structures']:
            # Delete cube structure items first
            CUBE_STRUCTURE_ITEM.objects.filter(cube_structure_id=cube_structure).delete()
            cube_structure.delete()

        for mapping_def in self.created_objects['mapping_definitions']:
            mapping_def.delete()

        for member_mapping in self.created_objects['member_mappings']:
            # Delete member mapping items first
            MEMBER_MAPPING_ITEM.objects.filter(member_mapping_id=member_mapping).delete()
            member_mapping.delete()

        for variable_mapping in self.created_objects['variable_mappings']:
            # Delete variable mapping items first
            VARIABLE_MAPPING_ITEM.objects.filter(variable_mapping_id=variable_mapping).delete()
            variable_mapping.delete()

        logger.info("Rollback completed")