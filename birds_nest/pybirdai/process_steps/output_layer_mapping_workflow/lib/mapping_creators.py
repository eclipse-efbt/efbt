"""
Mapping creation utilities for output layer mapping workflow.

Contains classes for creating variable mappings, member mappings,
mapping definitions, and ordinate links.
"""

import logging
from typing import Dict, List, Set, Optional, Any
from pybirdai.models.bird_meta_data_model import (
    VARIABLE, MEMBER, VARIABLE_MAPPING, VARIABLE_MAPPING_ITEM,
    MEMBER_MAPPING, MEMBER_MAPPING_ITEM, MAPPING_DEFINITION,
    MAPPING_ORDINATE_LINK, AXIS_ORDINATE
)

logger = logging.getLogger(__name__)


class VariableMappingCreator:
    """Creates VARIABLE_MAPPING and VARIABLE_MAPPING_ITEM records."""

    def __init__(self, maintenance_agency):
        """
        Initialize with maintenance agency.

        Args:
            maintenance_agency: MAINTENANCE_AGENCY object for FK references
        """
        self.maintenance_agency = maintenance_agency

    def create_variable_mapping(
        self,
        mapping_id: str,
        name: str,
        code: str,
        debug_data: Optional[Dict] = None
    ) -> VARIABLE_MAPPING:
        """
        Create a VARIABLE_MAPPING record.

        Args:
            mapping_id: Unique ID for the variable mapping
            name: Human-readable name
            code: Internal code
            debug_data: Optional dict to track created objects

        Returns:
            Created VARIABLE_MAPPING object

        Raises:
            Exception: If creation fails
        """
        try:
            variable_mapping = VARIABLE_MAPPING.objects.create(
                variable_mapping_id=mapping_id,
                maintenance_agency_id=self.maintenance_agency,
                name=name,
                code=code
            )
            logger.info(f"Created VARIABLE_MAPPING: {mapping_id}")

            if debug_data is not None:
                if 'VARIABLE_MAPPING' not in debug_data:
                    debug_data['VARIABLE_MAPPING'] = []
                debug_data['VARIABLE_MAPPING'].append(variable_mapping)

            return variable_mapping

        except Exception as e:
            logger.error(f"Failed to create VARIABLE_MAPPING {mapping_id}: {e}")
            raise

    def create_variable_mapping_items(
        self,
        variable_mapping: VARIABLE_MAPPING,
        source_var_ids: Set[str],
        target_var_ids: Set[str],
        debug_data: Optional[Dict] = None
    ) -> List[VARIABLE_MAPPING_ITEM]:
        """
        Create VARIABLE_MAPPING_ITEM records for source and target variables.

        Args:
            variable_mapping: Parent VARIABLE_MAPPING object
            source_var_ids: Set of source variable IDs
            target_var_ids: Set of target variable IDs
            debug_data: Optional dict to track created objects

        Returns:
            List of created VARIABLE_MAPPING_ITEM objects
        """
        created_items = []
        all_var_ids = source_var_ids | target_var_ids
        created_var_ids = set()

        for var_id in all_var_ids:
            if var_id in created_var_ids:
                continue

            variable = VARIABLE.objects.filter(variable_id=var_id).first()
            if variable:
                is_source = "true" if var_id in source_var_ids else "false"
                vmi = VARIABLE_MAPPING_ITEM.objects.create(
                    variable_mapping_id=variable_mapping,
                    variable_id=variable,
                    is_source=is_source
                )
                created_items.append(vmi)
                created_var_ids.add(var_id)

                if debug_data is not None:
                    if 'VARIABLE_MAPPING_ITEM' not in debug_data:
                        debug_data['VARIABLE_MAPPING_ITEM'] = []
                    debug_data['VARIABLE_MAPPING_ITEM'].append(vmi)

        logger.debug(f"Created {len(created_items)} VARIABLE_MAPPING_ITEMs")
        return created_items


class MemberMappingCreator:
    """Creates MEMBER_MAPPING and MEMBER_MAPPING_ITEM records."""

    def __init__(self, maintenance_agency):
        """
        Initialize with maintenance agency.

        Args:
            maintenance_agency: MAINTENANCE_AGENCY object for FK references
        """
        self.maintenance_agency = maintenance_agency

    def create_member_mapping(
        self,
        mapping_id: str,
        name: str,
        code: str,
        debug_data: Optional[Dict] = None
    ) -> MEMBER_MAPPING:
        """
        Create a MEMBER_MAPPING record.

        Args:
            mapping_id: Unique ID for the member mapping
            name: Human-readable name
            code: Internal code
            debug_data: Optional dict to track created objects

        Returns:
            Created MEMBER_MAPPING object

        Raises:
            Exception: If creation fails
        """
        try:
            member_mapping = MEMBER_MAPPING.objects.create(
                member_mapping_id=mapping_id,
                maintenance_agency_id=self.maintenance_agency,
                name=name,
                code=code
            )
            logger.info(f"Created MEMBER_MAPPING: {mapping_id}")

            if debug_data is not None:
                if 'MEMBER_MAPPING' not in debug_data:
                    debug_data['MEMBER_MAPPING'] = []
                debug_data['MEMBER_MAPPING'].append(member_mapping)

            return member_mapping

        except Exception as e:
            logger.error(f"Failed to create MEMBER_MAPPING {mapping_id}: {e}")
            raise

    def create_member_mapping_items(
        self,
        member_mapping: MEMBER_MAPPING,
        dimensions: List[Dict[str, str]],
        source_var_ids: Set[str],
        debug_data: Optional[Dict] = None
    ) -> List[MEMBER_MAPPING_ITEM]:
        """
        Create MEMBER_MAPPING_ITEM records from dimension rows.

        Args:
            member_mapping: Parent MEMBER_MAPPING object
            dimensions: List of dicts mapping var_id -> member_id
            source_var_ids: Set of source variable IDs (for is_source flag)
            debug_data: Optional dict to track created objects

        Returns:
            List of created MEMBER_MAPPING_ITEM objects
        """
        created_items = []

        for row_idx, row in enumerate(dimensions):
            for var_id, member_id in row.items():
                if not member_id:
                    continue

                variable = VARIABLE.objects.filter(variable_id=var_id).first()
                member = MEMBER.objects.filter(member_id=member_id).first()

                if variable and member:
                    is_source_value = "true" if var_id in source_var_ids else "false"
                    mmi = MEMBER_MAPPING_ITEM.objects.create(
                        member_mapping_id=member_mapping,
                        member_mapping_row=str(row_idx + 1),
                        variable_id=variable,
                        is_source=is_source_value,
                        member_id=member
                    )
                    created_items.append(mmi)

                    if debug_data is not None:
                        if 'MEMBER_MAPPING_ITEM' not in debug_data:
                            debug_data['MEMBER_MAPPING_ITEM'] = []
                        debug_data['MEMBER_MAPPING_ITEM'].append(mmi)

        logger.debug(f"Created {len(created_items)} MEMBER_MAPPING_ITEMs")
        return created_items


class MappingDefinitionCreator:
    """Creates MAPPING_DEFINITION records."""

    # Mapping type codes
    TYPE_ENUMERATION = 'E'
    TYPE_OBSERVATION = 'O'
    TYPE_ATTRIBUTE = 'A'

    def __init__(self, maintenance_agency):
        """
        Initialize with maintenance agency.

        Args:
            maintenance_agency: MAINTENANCE_AGENCY object for FK references
        """
        self.maintenance_agency = maintenance_agency

    @classmethod
    def get_mapping_type(cls, group_type: str) -> str:
        """
        Get mapping type code from group type string.

        Args:
            group_type: Group type ('dimension', 'observation', 'attribute')

        Returns:
            Mapping type code ('E', 'O', or 'A')
        """
        group_type_lower = group_type.lower() if group_type else 'dimension'
        if group_type_lower == 'dimension':
            return cls.TYPE_ENUMERATION
        elif group_type_lower == 'observation':
            return cls.TYPE_OBSERVATION
        else:
            return cls.TYPE_ATTRIBUTE

    def validate_mappings_exist(
        self,
        variable_mapping: VARIABLE_MAPPING,
        member_mapping: Optional[MEMBER_MAPPING]
    ) -> None:
        """
        Validate that variable_mapping and member_mapping exist in database.

        Args:
            variable_mapping: VARIABLE_MAPPING to validate
            member_mapping: Optional MEMBER_MAPPING to validate

        Raises:
            ValueError: If validation fails
        """
        if not variable_mapping:
            raise ValueError("VARIABLE_MAPPING is None - cannot create MAPPING_DEFINITION")

        if not VARIABLE_MAPPING.objects.filter(
            variable_mapping_id=variable_mapping.variable_mapping_id
        ).exists():
            raise ValueError(
                f"VARIABLE_MAPPING {variable_mapping.variable_mapping_id} doesn't exist in database"
            )

        if member_mapping and not MEMBER_MAPPING.objects.filter(
            member_mapping_id=member_mapping.member_mapping_id
        ).exists():
            raise ValueError(
                f"MEMBER_MAPPING {member_mapping.member_mapping_id} doesn't exist in database"
            )

    def create_mapping_definition(
        self,
        mapping_id: str,
        name: str,
        code: str,
        mapping_type: str,
        variable_mapping: VARIABLE_MAPPING,
        member_mapping: Optional[MEMBER_MAPPING],
        algorithm: str = "",
        debug_data: Optional[Dict] = None
    ) -> MAPPING_DEFINITION:
        """
        Create a MAPPING_DEFINITION record.

        Args:
            mapping_id: Unique ID for the mapping definition
            name: Human-readable name
            code: Internal code
            mapping_type: Type code ('E', 'O', or 'A')
            variable_mapping: Associated VARIABLE_MAPPING object
            member_mapping: Optional associated MEMBER_MAPPING object
            algorithm: Optional algorithm description
            debug_data: Optional dict to track created objects

        Returns:
            Created MAPPING_DEFINITION object

        Raises:
            ValueError: If FK validation fails
            Exception: If creation fails
        """
        # Validate FKs before creation
        self.validate_mappings_exist(variable_mapping, member_mapping)

        logger.info(
            f"Creating MAPPING_DEFINITION with variable_mapping={variable_mapping.variable_mapping_id}, "
            f"member_mapping={member_mapping.member_mapping_id if member_mapping else 'None'}"
        )

        try:
            mapping_definition = MAPPING_DEFINITION.objects.create(
                mapping_id=mapping_id,
                maintenance_agency_id=self.maintenance_agency,
                name=name,
                code=code,
                mapping_type=mapping_type,
                algorithm=algorithm,
                variable_mapping_id=variable_mapping,
                member_mapping_id=member_mapping
            )
            logger.info(f"Created MAPPING_DEFINITION: {mapping_id}")

            if debug_data is not None:
                if 'MAPPING_DEFINITION' not in debug_data:
                    debug_data['MAPPING_DEFINITION'] = []
                debug_data['MAPPING_DEFINITION'].append(mapping_definition)

            return mapping_definition

        except Exception as e:
            logger.error(f"Failed to create MAPPING_DEFINITION {mapping_id}: {e}")
            logger.error(f"variable_mapping type: {type(variable_mapping)}, value: {variable_mapping}")
            logger.error(f"member_mapping type: {type(member_mapping)}, value: {member_mapping}")
            raise


class OrdinateLinkCreator:
    """Creates MAPPING_ORDINATE_LINK records."""

    def create_ordinate_links(
        self,
        mapping_definitions: List[Dict[str, Any]],
        selected_ordinates: List[str],
        debug_data: Optional[Dict] = None
    ) -> int:
        """
        Create MAPPING_ORDINATE_LINK records linking mappings to ordinates.

        Args:
            mapping_definitions: List of dicts with 'mapping_definition' key
            selected_ordinates: List of axis_ordinate_id strings
            debug_data: Optional dict to track created objects

        Returns:
            Number of links created
        """
        if not selected_ordinates or not mapping_definitions:
            logger.debug("No ordinates or mappings to link")
            return 0

        logger.info(f"Creating MAPPING_ORDINATE_LINK records for {len(selected_ordinates)} ordinates")

        # Get all AXIS_ORDINATE objects
        ordinate_objects = AXIS_ORDINATE.objects.filter(
            axis_ordinate_id__in=selected_ordinates
        )
        ordinate_map = {o.axis_ordinate_id: o for o in ordinate_objects}

        links_created = 0
        for mapping_info in mapping_definitions:
            mapping_def = mapping_info.get('mapping_definition')
            if not mapping_def:
                continue

            for ordinate_id in selected_ordinates:
                ordinate = ordinate_map.get(ordinate_id)
                if ordinate:
                    MAPPING_ORDINATE_LINK.objects.get_or_create(
                        mapping_id=mapping_def,
                        axis_ordinate_id=ordinate
                    )
                    links_created += 1

        logger.info(f"Created {links_created} MAPPING_ORDINATE_LINK records")

        if debug_data is not None:
            if 'MAPPING_ORDINATE_LINK' not in debug_data:
                debug_data['MAPPING_ORDINATE_LINK'] = []
            debug_data['MAPPING_ORDINATE_LINK'].append(f"{links_created} links created")

        return links_created


class MappingBatchCreator:
    """
    Batch creator for processing multiple mappings from session data.

    Extracts the mapping creation loop from phase3_mappings.py to make
    it a reusable, testable component.
    """

    def __init__(self, maintenance_agency, table_code: str, version: str, table_id: str = ""):
        """
        Initialize the batch creator.

        Args:
            maintenance_agency: MAINTENANCE_AGENCY object for FK references
            table_code: Base table code (e.g., "F01_01")
            version: Version string (e.g., "3.2.0")
            table_id: Optional full table ID with potential Z-axis suffix
        """
        self.maintenance_agency = maintenance_agency
        self.table_code = table_code
        self.version = version
        self.table_id = table_id

        # Initialize sub-creators
        self.var_mapping_creator = VariableMappingCreator(maintenance_agency)
        self.member_mapping_creator = MemberMappingCreator(maintenance_agency)
        self.mapping_def_creator = MappingDefinitionCreator(maintenance_agency)

    def create_mappings_from_session(
        self,
        all_mappings: Dict[str, Any],
        variable_groups: Dict[str, Any],
        debug_data: Optional[Dict] = None
    ) -> List[Dict[str, Any]]:
        """
        Create all mappings from session data.

        Args:
            all_mappings: Dict of group_id -> mapping_data from session
            variable_groups: Dict of group_id -> group_info with variable IDs
            debug_data: Optional dict to track created objects

        Returns:
            List of dicts with 'name', 'mapping_definition', 'internal_id'
        """
        from pybirdai.process_steps.output_layer_mapping_workflow.lib.naming_utils import (
            NamingUtils
        )

        # Generate mapping prefix and calculate starting sequence
        mapping_prefix = NamingUtils.generate_mapping_prefix(
            self.table_code, self.version, self.table_id
        )
        mapping_sequence_start = NamingUtils.calculate_next_sequence(mapping_prefix)

        created_mapping_definitions = []
        mapping_counter = 0

        for group_id, mapping_data in all_mappings.items():
            result = self._create_single_mapping(
                group_id=group_id,
                mapping_data=mapping_data,
                variable_groups=variable_groups,
                mapping_prefix=mapping_prefix,
                sequence_number=mapping_sequence_start + mapping_counter,
                debug_data=debug_data
            )

            if result:
                created_mapping_definitions.append(result)
                mapping_counter += 1

        logger.info(f"Created {len(created_mapping_definitions)} mapping definitions")
        return created_mapping_definitions

    def _create_single_mapping(
        self,
        group_id: str,
        mapping_data: Dict[str, Any],
        variable_groups: Dict[str, Any],
        mapping_prefix: str,
        sequence_number: int,
        debug_data: Optional[Dict] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Create a single mapping with all its components.

        Args:
            group_id: The group ID
            mapping_data: Mapping data dict
            variable_groups: Variable groups dict
            mapping_prefix: Base prefix for mapping IDs
            sequence_number: Sequence number for this mapping
            debug_data: Optional dict to track created objects

        Returns:
            Dict with 'name', 'mapping_definition', 'internal_id', or None if failed
        """
        from pybirdai.process_steps.output_layer_mapping_workflow.lib.naming_utils import (
            NamingUtils
        )

        mapping_name = mapping_data.get('mapping_name', f'Mapping_{group_id}')
        internal_id = mapping_data.get('internal_id', group_id)
        group_type = mapping_data.get('group_type', 'dimension')
        dimensions = mapping_data.get('dimensions', [])
        observations = mapping_data.get('observations', {})

        # Get source and target variable IDs
        group_info = variable_groups.get(group_id, {})
        source_var_ids = set(group_info.get('variable_ids', []))
        target_var_ids = set(group_info.get('targets', []))

        # Generate mapping ID suffix
        mapping_id_suffix = NamingUtils.format_mapping_id_suffix(sequence_number)

        try:
            # 1. Create VARIABLE_MAPPING
            variable_mapping = self.var_mapping_creator.create_variable_mapping(
                mapping_id=f"{mapping_prefix}_{mapping_id_suffix}_VAR",
                name=mapping_name,
                code=internal_id,
                debug_data=debug_data
            )

            # 2. Create VARIABLE_MAPPING_ITEMs
            self.var_mapping_creator.create_variable_mapping_items(
                variable_mapping=variable_mapping,
                source_var_ids=source_var_ids,
                target_var_ids=target_var_ids,
                debug_data=debug_data
            )

            # 3. Create MEMBER_MAPPING if dimensions exist
            member_mapping = None
            if dimensions:
                member_mapping = self.member_mapping_creator.create_member_mapping(
                    mapping_id=f"{mapping_prefix}_{mapping_id_suffix}_MEM",
                    name=f"{mapping_name} - Member Mappings",
                    code=f"{internal_id}_MEM",
                    debug_data=debug_data
                )

                self.member_mapping_creator.create_member_mapping_items(
                    member_mapping=member_mapping,
                    dimensions=dimensions,
                    source_var_ids=source_var_ids,
                    debug_data=debug_data
                )

            # 4. Create MAPPING_DEFINITION
            mapping_type = MappingDefinitionCreator.get_mapping_type(group_type)
            algorithm = (
                f"Mapping: {mapping_name}\n"
                f"{len(dimensions)} dimension rows, {len(observations)} observation rows"
            )

            mapping_definition = self.mapping_def_creator.create_mapping_definition(
                mapping_id=f"{mapping_prefix}_{mapping_id_suffix}",
                name=mapping_name,
                code=internal_id,
                mapping_type=mapping_type,
                variable_mapping=variable_mapping,
                member_mapping=member_mapping,
                algorithm=algorithm,
                debug_data=debug_data
            )

            logger.info(f"Created mapping: {mapping_definition.mapping_id}")

            return {
                'name': mapping_name,
                'mapping_definition': mapping_definition,
                'internal_id': internal_id
            }

        except Exception as e:
            logger.error(f"Failed to create mapping for group {group_id}: {e}")
            raise
