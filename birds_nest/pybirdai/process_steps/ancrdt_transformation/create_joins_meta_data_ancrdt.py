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
import csv
import itertools

import sys
import django
from django.db.models import Q

IGNORED_DOMAINS = [
    "String",
    "Integer",
    "Date",
    "Float",
    "Boolean",
    "FRQNCY"
]
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("log.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

class DjangoSetup:
    _initialized = False

    @classmethod
    def configure_django(cls):
        """Configure Django settings without starting the application"""
        if cls._initialized:
            return

        try:
            # Set up Django settings module for birds_nest in parent directory
            project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..'))
            sys.path.insert(0, project_root)
            os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'birds_nest.settings')

            # This allows us to use Django models without running the server
            django.setup()

            logger.info("Django configured successfully with settings module: %s",
                       os.environ['DJANGO_SETTINGS_MODULE'])
            cls._initialized = True
        except Exception as e:
            logger.error(f"Django configuration failed: {str(e)}")
            raise


class JoinsMetaDataCreatorANCRDT:
    """
    A class for creating generation rules for reports and tables.
    """

    def __init__(self):
        DjangoSetup.configure_django()
        self.join_map = {}
        join_config_file1 = os.path.join(os.getcwd(),"resources/joins_configuration", "join_for_product_to_reference_category_ANCRDT_REF.csv")
        join_config_file2 = os.path.join(os.getcwd(),"resources/joins_configuration", "join_for_product_il_definitions_ANCRDT_REF.csv")
        try:
            # First, read all data from file2 into a dictionary for efficient lookup
            file2_data = {}
            with open(join_config_file2, encoding='utf-8') as f2:
                reader2 = csv.DictReader(f2)
                for row2 in reader2:
                    file2_data[row2['Name']] = row2

            # Now read file1 and match with file2 data
            with open(join_config_file1, encoding='utf-8') as f1:
                reader1 = csv.DictReader(f1)
                for row1 in reader1:
                    key = (row1['rolc'], row1['join_identifier'])
                    # Look for matching entry in file2 data
                    if row1['join_identifier'] in file2_data:
                        row2 = file2_data[row1['join_identifier']]
                        self.join_map[key] = {
                            "rolc": row1['rolc'],
                            "join_identifier": row1['join_identifier'],
                            "ilc": [_
                                for _ in [row2["Main Table"]] + row2["Related Tables"].split(":")
                                if _
                            ]
                        }
                    else:
                        logger.warning(f"No matching entry found in file2 for join_identifier: {row1['join_identifier']}")

            logger.info(f"Successfully loaded {len(self.join_map)} join configurations")
        except FileNotFoundError as e:
            logger.error(f"Join configuration file not found: {e.filename}")
            raise
        except KeyError as e:
            logger.error(f"Missing expected column in CSV file: {e}")
            raise

    def generate_joins_meta_data(self) -> dict:
        """
        Generate generation rules for the given context and framework.
        """
        # Import here to ensure Django is fully configured first
        from pybirdai.models.bird_meta_data_model import CUBE, CUBE_STRUCTURE_ITEM, CUBE_STRUCTURE, DOMAIN, VARIABLE, CUBE_LINK, CUBE_STRUCTURE_ITEM_LINK,MAINTENANCE_AGENCY,MEMBER_LINK

        ignored_domains = [DOMAIN.objects.get(domain_id=domain_id) for domain_id in IGNORED_DOMAINS]

        mock_join_identifier = "mock_join_identifier"

        comparison_results = {}

        for val in self.join_map.values():

            mock_join_identifier = val["join_identifier"]
            rolc_cube = CUBE.objects.get(cube_id=val["rolc"])
            ilc_cubes = []
            for cube in val["ilc"]:
                try:
                    ilc_cubes += [CUBE.objects.get(cube_id=cube)]
                except:
                    logger.warning(f"{cube} not found")

            rolc_items_to_match = {
                rolc_cube.cube_id: self.fetch_cube_structure_items_dict(rolc_cube)
            }

            ilc_items_to_match = {
                cube.cube_id: self.fetch_cube_structure_items_dict(cube)
                for cube in ilc_cubes
            }
            comparison_results = self.compare(rolc_items_to_match, ilc_items_to_match,ignored_domains)

            for (rolc, ilc), matches in comparison_results.items():
                rolc_cube = CUBE.objects.get(cube_id=rolc)
                ilc_cube = CUBE.objects.get(cube_id=ilc)
                rolc_cube_structure = CUBE_STRUCTURE.objects.get(cube=rolc_cube)
                ilc_cube_structure = CUBE_STRUCTURE.objects.get(cube=ilc_cube)
                name_code_description = f"{ilc}:{mock_join_identifier}:{rolc}"
                cube_link, exists = CUBE_LINK.objects.get_or_create(
                    primary_cube_id=ilc_cube,
                    foreign_cube_id=rolc_cube,
                    maintenance_agency_id=MAINTENANCE_AGENCY.objects.get(maintenance_agency_id="NODE"),
                    join_identifier=mock_join_identifier,
                    cube_link_id=name_code_description,
                    code=name_code_description,
                    name=name_code_description,
                    description=name_code_description,
                    valid_from=None,
                    valid_to=None,
                    version=1
                )

                for (variable_rolc, variable_ilc), domain in matches.items():
                    rolc_cube_structure_item = CUBE_STRUCTURE_ITEM.objects.all().get(
                        cube_structure_id = rolc_cube_structure,variable_id=variable_rolc
                    )
                    ilc_cube_structure_item = CUBE_STRUCTURE_ITEM.objects.all().get(
                        cube_structure_id = ilc_cube_structure,variable_id=variable_ilc
                    )

                    csilink, exists = CUBE_STRUCTURE_ITEM_LINK.objects.get_or_create(
                        cube_structure_item_link_id = f"{ilc}:{variable_ilc}:{mock_join_identifier}:{rolc}:{variable_rolc}",
                        cube_link_id = cube_link,
                        primary_cube_variable_code=ilc_cube_structure_item,
                        foreign_cube_variable_code=rolc_cube_structure_item,
                    )

                    for member in domain:
                        member_link, exists = MEMBER_LINK.objects.get_or_create(
                            cube_structure_item_link_id=csilink,
                            primary_member_id=member,
                            foreign_member_id=member,
                            is_linked=True,
                            valid_from=None,
                            valid_to=None
                        )

                    if not MEMBER_LINK.objects.all().filter(cube_structure_item_link_id=csilink):
                        csilink.delete()


        return comparison_results

    def compare(self, cube_items_1: dict, cube_items_2: dict,ignored_domains:list,flag_log:bool=False):
        from pybirdai.models.bird_meta_data_model import CUBE_LINK, CUBE_STRUCTURE_ITEM_LINK,MAINTENANCE_AGENCY,MEMBER

        matched_variables = dict()
        cube_iter = itertools.product(cube_items_1.items(), cube_items_2.items())
        for (key_rolc, value_rolc), (key_ilc, value_ilc) in cube_iter:
            cube_items_iter = itertools.product(value_rolc.items(), value_ilc.items())
            for (variable_rolc, infos_rolc), (variable_ilc, infos_ilc) in cube_items_iter:
                if "NEVS" in variable_rolc.name:
                    continue
                if infos_rolc["domain"] in ignored_domains or infos_ilc["domain"] in ignored_domains:
                    continue
                members = self.fetch_members(infos_rolc["subdomain"]).intersection(
                    self.fetch_members(infos_ilc["subdomain"])
                )
                if members:
                    if (key_rolc, key_ilc) not in matched_variables:
                        matched_variables[(key_rolc, key_ilc)] = {}
                    matched_variables[(key_rolc, key_ilc)][(variable_rolc.variable_id, variable_ilc.variable_id)] = members
        return matched_variables

    def fetch_members(self,subdomain):
        from pybirdai.models.bird_meta_data_model import MEMBER
        members = MEMBER.objects.all().filter(
            subdomain_enumeration__subdomain_id = subdomain
        )
        if not members:
            return set()
        return set(members)

    def fetch_cube_structure_items_dict(self, cube):
        # Import here to ensure Django is fully configured first
        from pybirdai.models.bird_meta_data_model import CUBE, CUBE_STRUCTURE_ITEM, CUBE_STRUCTURE

        cube_structure = CUBE_STRUCTURE.objects.get(cube=cube)
        cube_structure_items = CUBE_STRUCTURE_ITEM.objects.all().filter(
            cube_structure_id=cube_structure
        )

        return {
            csi.variable_id: {
                "variable_or_cvc": csi.cube_variable_code or csi.variable_id,
                "domain": csi.variable_id.domain_id,
                "subdomain": csi.subdomain_id,
            }
            for csi in cube_structure_items
        }

def main():
    DjangoSetup.configure_django()
    creator = JoinsMetaDataCreatorANCRDT()
    result = creator.generate_joins_meta_data()

if __name__ == "__main__":
    main()
