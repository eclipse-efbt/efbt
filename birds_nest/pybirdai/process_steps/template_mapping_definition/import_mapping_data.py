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

from datetime import datetime
from django.db import transaction
from pybirdai.models.bird_meta_data_model import (
    MAPPING_DEFINITION,
    MEMBER_MAPPING,
    MEMBER_MAPPING_ITEM,
    VARIABLE_MAPPING,
    VARIABLE_MAPPING_ITEM,
    MAPPING_TO_CUBE,
    MAINTENANCE_AGENCY,
    VARIABLE,
    MEMBER,
    MEMBER_HIERARCHY,
)


class ImportMappingData:
    """
    Imports mapping data from parsed CSV into the database.

    This class handles the complete database import workflow, creating
    MAPPING_DEFINITION, MEMBER_MAPPING, VARIABLE_MAPPING, and related items.
    """

    @staticmethod
    def handle(parsed_data, mapping_name, mapping_code, mapping_type, algorithm,
               cube_ids, maintenance_agency_id, overwrite=False):
        """
        Import mapping data into the database.

        Args:
            parsed_data: Validated mapping data structure from ParseMappingCSV
            mapping_name: Name for the mapping definition
            mapping_code: Unique code for the mapping
            mapping_type: Type of mapping (e.g., 'ONE_TO_ONE', 'ONE_TO_MANY')
            algorithm: Algorithm description (optional)
            cube_ids: List of cube IDs to associate with this mapping
            maintenance_agency_id: ID of the maintenance agency
            overwrite: Whether to overwrite existing mapping with same code

        Returns:
            int: The ID of the created/updated MAPPING_DEFINITION

        Raises:
            ValueError: If mapping code already exists and overwrite is False
            Exception: If database transaction fails
        """
        rows = parsed_data.get('rows', [])

        if not rows:
            raise ValueError("No data rows to import")

        # Check if mapping already exists
        existing_mapping = MAPPING_DEFINITION.objects.filter(mapping_id=mapping_code).first()

        if existing_mapping and not overwrite:
            raise ValueError(
                f"Mapping with code '{mapping_code}' already exists. "
                f"Please enable overwrite or choose a different code."
            )

        try:
            with transaction.atomic():
                # Get or create maintenance agency
                if maintenance_agency_id:
                    agency = MAINTENANCE_AGENCY.objects.filter(
                        maintenance_agency_id=maintenance_agency_id
                    ).first()
                else:
                    agency = None

                # Determine if we need member or variable mappings
                has_members = any(
                    row.get('source_members') or row.get('target_members')
                    for row in rows
                )
                has_variables = any(
                    row.get('source_variables') or row.get('target_variables')
                    for row in rows
                )

                # Create or update member mapping if needed
                member_mapping = None
                if has_members:
                    member_mapping = ImportMappingData._create_member_mapping(
                        mapping_code, mapping_name, agency, rows, overwrite
                    )

                # Create or update variable mapping if needed
                variable_mapping = None
                if has_variables:
                    variable_mapping = ImportMappingData._create_variable_mapping(
                        mapping_code, mapping_name, agency, rows, overwrite
                    )

                # Create or update mapping definition
                if overwrite and existing_mapping:
                    mapping_def = existing_mapping
                    mapping_def.name = mapping_name
                    mapping_def.mapping_type = mapping_type or ''
                    mapping_def.algorithm = algorithm or ''
                    mapping_def.member_mapping_id = member_mapping
                    mapping_def.variable_mapping_id = variable_mapping
                    mapping_def.maintenance_agency_id = agency
                    mapping_def.save()
                else:
                    mapping_def = MAPPING_DEFINITION.objects.create(
                        mapping_id=mapping_code,
                        name=mapping_name,
                        code=mapping_code,
                        mapping_type=mapping_type or '',
                        algorithm=algorithm or '',
                        member_mapping_id=member_mapping,
                        variable_mapping_id=variable_mapping,
                        maintenance_agency_id=agency
                    )

                # Create or update mapping to cube associations
                if cube_ids:
                    ImportMappingData._create_mapping_to_cubes(
                        mapping_def, cube_ids, overwrite
                    )

                return mapping_def.mapping_id

        except Exception as e:
            raise Exception(f"Failed to import mapping data: {str(e)}")

    @staticmethod
    def _create_member_mapping(mapping_code, mapping_name, agency, rows, overwrite):
        """
        Create or update MEMBER_MAPPING and MEMBER_MAPPING_ITEM records.

        Args:
            mapping_code: Code for the member mapping (derived from mapping code)
            mapping_name: Name for the member mapping
            agency: MAINTENANCE_AGENCY object
            rows: List of row data
            overwrite: Whether to overwrite existing records

        Returns:
            MEMBER_MAPPING: The created or updated member mapping object
        """
        member_mapping_id = f"mm_{mapping_code}"

        # Delete existing items if overwriting
        if overwrite:
            MEMBER_MAPPING_ITEM.objects.filter(
                member_mapping_id__member_mapping_id=member_mapping_id
            ).delete()

        # Create or update member mapping
        member_mapping, created = MEMBER_MAPPING.objects.update_or_create(
            member_mapping_id=member_mapping_id,
            defaults={
                'name': f"Member mapping for {mapping_name}",
                'code': member_mapping_id,
                'maintenance_agency_id': agency
            }
        )

        # Create member mapping items
        items_to_create = []

        for row in rows:
            row_num = str(row.get('row_number', 1))
            source_members = row.get('source_members', [])
            target_members = row.get('target_members', [])
            hierarchy_id = row.get('member_hierarchy')
            valid_from = row.get('valid_from')
            valid_to = row.get('valid_to')

            # Get hierarchy object if specified
            hierarchy = None
            if hierarchy_id:
                hierarchy = MEMBER_HIERARCHY.objects.filter(
                    member_hierarchy_id=hierarchy_id
                ).first()

            # Convert dates
            valid_from_date = ImportMappingData._parse_date(valid_from) if valid_from else None
            valid_to_date = ImportMappingData._parse_date(valid_to) if valid_to else None

            # Create source member items
            for member_id in source_members:
                member = MEMBER.objects.filter(member_id=member_id).first()
                if member:
                    # Also get the variable for this member (if any)
                    variable = None
                    # Note: In the current model, member mapping items can have a variable_id
                    # but we don't have that information in the CSV, so we leave it None

                    items_to_create.append(
                        MEMBER_MAPPING_ITEM(
                            member_mapping_id=member_mapping,
                            member_mapping_row=row_num,
                            variable_id=variable,
                            is_source='true',
                            member_id=member,
                            valid_from=valid_from_date,
                            valid_to=valid_to_date,
                            member_hierarchy=hierarchy
                        )
                    )

            # Create target member items
            for member_id in target_members:
                member = MEMBER.objects.filter(member_id=member_id).first()
                if member:
                    items_to_create.append(
                        MEMBER_MAPPING_ITEM(
                            member_mapping_id=member_mapping,
                            member_mapping_row=row_num,
                            variable_id=None,
                            is_source='false',
                            member_id=member,
                            valid_from=valid_from_date,
                            valid_to=valid_to_date,
                            member_hierarchy=hierarchy
                        )
                    )

        # Bulk create items
        if items_to_create:
            MEMBER_MAPPING_ITEM.objects.bulk_create(items_to_create)

        return member_mapping

    @staticmethod
    def _create_variable_mapping(mapping_code, mapping_name, agency, rows, overwrite):
        """
        Create or update VARIABLE_MAPPING and VARIABLE_MAPPING_ITEM records.

        Args:
            mapping_code: Code for the variable mapping (derived from mapping code)
            mapping_name: Name for the variable mapping
            agency: MAINTENANCE_AGENCY object
            rows: List of row data
            overwrite: Whether to overwrite existing records

        Returns:
            VARIABLE_MAPPING: The created or updated variable mapping object
        """
        variable_mapping_id = f"vm_{mapping_code}"

        # Delete existing items if overwriting
        if overwrite:
            VARIABLE_MAPPING_ITEM.objects.filter(
                variable_mapping_id__variable_mapping_id=variable_mapping_id
            ).delete()

        # Create or update variable mapping
        variable_mapping, created = VARIABLE_MAPPING.objects.update_or_create(
            variable_mapping_id=variable_mapping_id,
            defaults={
                'name': f"Variable mapping for {mapping_name}",
                'code': variable_mapping_id,
                'maintenance_agency_id': agency
            }
        )

        # Collect all unique variables (to avoid duplicates)
        source_variables = set()
        target_variables = set()

        for row in rows:
            source_variables.update(row.get('source_variables', []))
            target_variables.update(row.get('target_variables', []))
            valid_from = row.get('valid_from')
            valid_to = row.get('valid_to')

        # Convert dates (use first row's dates for all variables)
        valid_from_date = None
        valid_to_date = None
        if rows:
            first_row = rows[0]
            valid_from = first_row.get('valid_from')
            valid_to = first_row.get('valid_to')
            valid_from_date = ImportMappingData._parse_date(valid_from) if valid_from else None
            valid_to_date = ImportMappingData._parse_date(valid_to) if valid_to else None

        # Create variable mapping items
        items_to_create = []

        # Create source variable items
        for var_id in source_variables:
            variable = VARIABLE.objects.filter(variable_id=var_id).first()
            if variable:
                items_to_create.append(
                    VARIABLE_MAPPING_ITEM(
                        variable_mapping_id=variable_mapping,
                        variable_id=variable,
                        is_source='true',
                        valid_from=valid_from_date,
                        valid_to=valid_to_date
                    )
                )

        # Create target variable items
        for var_id in target_variables:
            variable = VARIABLE.objects.filter(variable_id=var_id).first()
            if variable:
                items_to_create.append(
                    VARIABLE_MAPPING_ITEM(
                        variable_mapping_id=variable_mapping,
                        variable_id=variable,
                        is_source='false',
                        valid_from=valid_from_date,
                        valid_to=valid_to_date
                    )
                )

        # Bulk create items
        if items_to_create:
            VARIABLE_MAPPING_ITEM.objects.bulk_create(items_to_create)

        return variable_mapping

    @staticmethod
    def _create_mapping_to_cubes(mapping_def, cube_ids, overwrite):
        """
        Create or update MAPPING_TO_CUBE associations.

        Args:
            mapping_def: MAPPING_DEFINITION object
            cube_ids: List of cube IDs to associate
            overwrite: Whether to delete existing associations first
        """
        if overwrite:
            MAPPING_TO_CUBE.objects.filter(mapping_id=mapping_def).delete()

        for cube_id in cube_ids:
            MAPPING_TO_CUBE.objects.create(
                cube_mapping_id=f"{mapping_def.mapping_id}_{cube_id}",
                mapping_id=mapping_def,
                valid_from=None,
                valid_to=None
            )

    @staticmethod
    def _parse_date(date_string):
        """
        Parse a date string in YYYY-MM-DD format.

        Args:
            date_string: Date string to parse

        Returns:
            datetime: Parsed datetime object, or None if invalid
        """
        if not date_string:
            return None

        try:
            return datetime.strptime(date_string, '%Y-%m-%d')
        except ValueError:
            return None
