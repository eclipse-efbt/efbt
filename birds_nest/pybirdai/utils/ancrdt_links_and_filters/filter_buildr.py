"""
Copyright 2025 Arfa Digital Consulting

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
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
        from pybirdai.bird_meta_data_model import CUBE_LINK, MEMBER_LINK,CUBE_STRUCTURE_ITEM_LINK

        conditions = []
        # Query related MEMBER_LINK objects for the given cube_structure_item_link
        cube_structure_item_link = CUBE_STRUCTURE_ITEM_LINK.objects.get(
            cube_structure_item_link_id = cube_structure_item_link_id
        )
        member_links = MEMBER_LINK.objects.filter(
            cube_structure_item_link_id=cube_structure_item_link
        )

        # Group member links by their associated foreign variables
        variable_links = {}
        for member_link in member_links:
            foreign_var_code = member_link.cube_structure_item_link_id.foreign_cube_variable_code.cube_variable_code
            if foreign_var_code not in variable_links:
                variable_links[foreign_var_code] = []
            variable_links[foreign_var_code].append(member_link)

        # Build conditions for each foreign variable
        for foreign_var_code, links in variable_links.items():
            foreign_var_name = ast.Name(id="item."+foreign_var_code, ctx=ast.Load())
            member_comparisons = []

            for member_link in links:
                member_code = ast.Constant(value=member_link.foreign_member_id.code)
                comparison = ast.Compare(
                    left=foreign_var_name, ops=[ast.Eq()], comparators=[member_code]
                )
                member_comparisons.append(comparison)

            if len(member_comparisons) > 1:
                conditions.append(ast.BoolOp(op=ast.Or(), values=member_comparisons))
            elif len(member_comparisons) == 1:
                conditions.append(member_comparisons[0])

        if len(conditions) > 1:
            filter_rule = ast.BoolOp(op=ast.And(), values=conditions)
        elif len(conditions) == 1:
            filter_rule = conditions[0]
        else:
            filter_rule = ast.Constant(value=True)

        return "("+ast.unparse(filter_rule)+")"

    @staticmethod
    def reverse_apply_member_links(
        cube_structure_item_link_id: str,
    ):
        DjangoSetup.configure_django()
        from pybirdai.bird_meta_data_model import CUBE_LINK, MEMBER_LINK,CUBE_STRUCTURE_ITEM_LINK
        conditions = []
        # Query related MEMBER_LINK objects for the given cube_structure_item_link
        cube_structure_item_link = CUBE_STRUCTURE_ITEM_LINK.objects.get(
            cube_structure_item_link_id = cube_structure_item_link_id
        )
        member_links = MEMBER_LINK.objects.filter(
            cube_structure_item_link_id=cube_structure_item_link
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
    from pybirdai.bird_meta_data_model import (
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
        "ANCRDT_INSTRMNT_C_1:INSTRMNT_RL:Loans and advances:TYP_INSTRMNT:TYP_INSTRMNT_RL"
    )
    )
