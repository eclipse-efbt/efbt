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
import os
import sys
import logging
import ast
from django.db import models
from django.db.models import Q
import django
from django.db import models
from django.conf import settings


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("visualization_service.log"),
        logging.StreamHandler(),
    ],
)

logger = logging.getLogger(__name__)


class DjangoSetup:
    @staticmethod
    def configure_django():
        """Configure Django settings without starting the application"""
        if not settings.configured:
            # Set up Django settings module for birds_nest in parent directory
            project_root = os.path.abspath(
                os.path.join(os.path.dirname(__file__), "../..")
            )
            sys.path.insert(0, project_root)
            os.environ["DJANGO_SETTINGS_MODULE"] = "birds_nest.settings"
            logger.info(
                "Configuring Django with settings module: %s",
                os.environ["DJANGO_SETTINGS_MODULE"],
            )
            django.setup()
            logger.debug("Django setup complete")


class TransformationBuildr:
    @staticmethod
    def define_filter_from_structure_link(
        cube_structure_item_link_id: str,
    ):
        DjangoSetup.configure_django()
        from pybirdai.models.bird_meta_data_model import CUBE_LINK, MEMBER_LINK,CUBE_STRUCTURE_ITEM_LINK

        conditions = []
        # Query related MEMBER_LINK objects with select_related to avoid N+1 queries
        cube_structure_item_link = CUBE_STRUCTURE_ITEM_LINK.objects.select_related(
            'foreign_cube_variable_code',
            'primary_cube_variable_code'
        ).get(
            cube_structure_item_link_id = cube_structure_item_link_id
        )
        member_links = MEMBER_LINK.objects.filter(
            cube_structure_item_link_id=cube_structure_item_link
        ).select_related(
            'foreign_member_id',
            'primary_member_id',
            'cube_structure_item_link_id__foreign_cube_variable_code',
            'cube_structure_item_link_id__primary_cube_variable_code'
        )

        # Group member links by their associated foreign variables
        variable_links = {}
        for member_link in member_links:
            foreign_var_code = member_link.cube_structure_item_link_id.foreign_cube_variable_code.cube_variable_code
            if foreign_var_code not in variable_links:
                variable_links[foreign_var_code] = []
            variable_links[foreign_var_code].append(member_link)

        # Build boolean assignments for each foreign variable
        boolean_assignments = []
        boolean_var_names = []

        for foreign_var_code, links in variable_links.items():
            # Create boolean variable name (lowercase)
            bool_var_name = f"bool_{foreign_var_code.lower()}"
            boolean_var_names.append(bool_var_name)

            # Collect all member codes for this variable
            member_codes = [member_link.foreign_member_id.code for member_link in links]

            if len(member_codes) > 1:
                # Use 'in' operator: item.VAR in ['val1', 'val2', ...]
                members_str = ', '.join([f"'{code}'" for code in member_codes])
                condition_str = f"item.{foreign_var_code} in [{members_str}]"
            elif len(member_codes) == 1:
                # Single value: item.VAR == 'val'
                condition_str = f"item.{foreign_var_code} == '{member_codes[0]}'"
            else:
                # No codes - skip this variable
                boolean_var_names.pop()  # Remove the variable name we just added
                continue

            # Create assignment: bool_var = condition
            boolean_assignments.append(f"{bool_var_name} = {condition_str}")

        # Build the final filter code
        # Return tuple: (assignments_list, bool_var_names_list)
        # The calling code will combine all boolean variable names into a single all([...])
        return (boolean_assignments, boolean_var_names)

    @staticmethod
    def reverse_apply_member_links(
        cube_structure_item_link_id: str,
    ):
        DjangoSetup.configure_django()
        from pybirdai.models.bird_meta_data_model import CUBE_LINK, MEMBER_LINK,CUBE_STRUCTURE_ITEM_LINK
        conditions = []
        # Query related MEMBER_LINK objects with select_related to avoid N+1 queries
        cube_structure_item_link = CUBE_STRUCTURE_ITEM_LINK.objects.select_related(
            'foreign_cube_variable_code',
            'primary_cube_variable_code'
        ).get(
            cube_structure_item_link_id = cube_structure_item_link_id
        )
        member_links = MEMBER_LINK.objects.filter(
            cube_structure_item_link_id=cube_structure_item_link
        ).select_related(
            'foreign_member_id',
            'primary_member_id',
            'cube_structure_item_link_id__foreign_cube_variable_code',
            'cube_structure_item_link_id__primary_cube_variable_code'
        )

        # Group member links by their associated foreign variables
        variable_links = {}
        for member_link in member_links:
            foreign_var_code = member_link.cube_structure_item_link_id.foreign_cube_variable_code.cube_variable_code
            if foreign_var_code not in variable_links:
                variable_links[foreign_var_code] = []
            variable_links[foreign_var_code].append({
                "source":member_link.primary_member_id.code,
                "target":member_link.foreign_member_id.code
            })

        return variable_links


if __name__ == "__main__":
    DjangoSetup.configure_django()
    from pybirdai.models.bird_meta_data_model import (
        CUBE_LINK,
        MEMBER_LINK,
        CUBE_STRUCTURE_ITEM_LINK,
    )

    # TransformationBuildr.print_cube_variables_comparison(
    #     "ANCRDT_INSTRMNT_C_1:INSTRMNT_RL:Loans and advances"
    # )
    # TransformationBuildr.find_similarities()
    print(
    TransformationBuildr.define_filter_from_structure_link(
        "ANCRDT_INSTRMNT_C_1:INSTRMNT_RL:Loans and advances:INSTRMNT_TYP_PRDCT:TYP_INSTRMNT_RL"
    )
    )
