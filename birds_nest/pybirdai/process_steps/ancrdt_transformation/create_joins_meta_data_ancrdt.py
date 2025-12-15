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

    Supports per-output-table configuration with FINREP-style headers:
    - Main Category,Name,slice_name (for product to reference category)
    - Name,Main Table,Filter,Related Tables,Comments (for IL definitions)
    """

    # All ANACREDIT output tables
    OUTPUT_TABLES = [
        'ANCRDT_INSTRMNT_C_1',
        'ANCRDT_FNNCL_C_1',
        'ANCRDT_ACCNTNG_C_1',
        'ANCRDT_CNTRPRTY_RFRNC_C_1',
        'ANCRDT_CNTRPRTY_DFLT_C_1',
        'ANCRDT_CNTRPRTY_RSK_C_1',
        'ANCRDT_PRTCTN_RCVD_C_1',
        'ANCRDT_INSTRMNT_PRTCTN_RCVD_C_1',
        'ANCRDT_JNT_LBLTS_C_1',
        'ANCRDT_CNTRPRTY_INSTRMNT_C_1'
    ]

    def __init__(self, output_table: str = None):
        DjangoSetup.configure_django()
        self.join_map = {}
        self.config_dir = os.path.join(os.getcwd(), "resources/joins_configuration")

        if output_table:
            # Load config for specific output table
            self._load_per_table_config(output_table)
        else:
            # Load config for all output tables that have config files
            self._load_all_tables_config()

        # Prefetch all necessary data into memory for performance optimization
        self._prefetch_data()

    def _load_all_tables_config(self):
        """Load configuration for all output tables that have config files."""
        for output_table in self.OUTPUT_TABLES:
            file1 = os.path.join(
                self.config_dir,
                f"join_for_product_to_reference_category_ANCRDT_REF_{output_table}.csv"
            )
            if os.path.exists(file1):
                self._load_per_table_config(output_table)
            else:
                logger.debug(f"No per-table config for {output_table}, skipping")

        logger.info(f"Successfully loaded {len(self.join_map)} join configurations")

    def _load_per_table_config(self, output_table: str):
        """Load configuration for a specific output table using FINREP-style headers."""
        file1 = os.path.join(
            self.config_dir,
            f"join_for_product_to_reference_category_ANCRDT_REF_{output_table}.csv"
        )
        file2 = os.path.join(
            self.config_dir,
            f"join_for_product_il_definitions_ANCRDT_REF_{output_table}.csv"
        )

        try:
            self._parse_config_files(file1, file2, output_table)
        except FileNotFoundError as e:
            logger.error(f"Config file not found for {output_table}: {e.filename}")
            raise
        except KeyError as e:
            logger.error(f"Missing expected column in CSV file for {output_table}: {e}")
            raise

    def _parse_config_files(self, file1_path: str, file2_path: str, output_table: str):
        """Parse harmonized config files (FINREP-style headers)."""
        # Read IL definitions (file2)
        il_definitions = {}
        with open(file2_path, encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                il_definitions[row['Name']] = row

        # Read product to reference category mapping (file1)
        with open(file1_path, encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                join_identifier = row['Name']
                main_category = row.get('Main Category', '').strip()

                if join_identifier in il_definitions:
                    il_def = il_definitions[join_identifier]
                    key = (output_table, join_identifier)
                    self.join_map[key] = {
                        "rolc": output_table,
                        "join_identifier": join_identifier,
                        "main_category": main_category,  # For future filter generation
                        "ilc": [t for t in [il_def["Main Table"]] + il_def["Related Tables"].split(":") if t]
                    }
                else:
                    logger.warning(f"No IL definition found for join_identifier: {join_identifier}")

    def _prefetch_data(self):
        """
        Prefetch all necessary database data into memory to avoid repeated queries.
        This significantly improves performance by eliminating N+1 query problems.
        """
        from pybirdai.models.bird_meta_data_model import (
            CUBE, CUBE_STRUCTURE, CUBE_STRUCTURE_ITEM, DOMAIN,
            VARIABLE, MAINTENANCE_AGENCY, MEMBER, SUBDOMAIN
        )

        logger.info("Starting data prefetch...")

        # 1. Extract all unique cube IDs from join_map
        all_cube_ids = set()
        for val in self.join_map.values():
            all_cube_ids.add(val["rolc"])
            all_cube_ids.update(val["ilc"])

        logger.info(f"Prefetching data for {len(all_cube_ids)} cubes")

        # 2. Prefetch all CUBE objects with their CUBE_STRUCTURE
        self.cubes_cache = {
            cube.cube_id: cube
            for cube in CUBE.objects.filter(cube_id__in=all_cube_ids).select_related('cube_structure_id')
        }
        logger.info(f"Cached {len(self.cubes_cache)} CUBE objects")

        # 3. Build CUBE_STRUCTURE cache from already-fetched CUBE objects
        # Note: CUBE has FK to CUBE_STRUCTURE, so we extract it from cube.cube_structure_id
        self.cube_structures_cache = {}
        cube_structure_ids_set = set()
        for cube_id, cube in self.cubes_cache.items():
            if cube.cube_structure_id:
                self.cube_structures_cache[cube_id] = cube.cube_structure_id
                cube_structure_ids_set.add(cube.cube_structure_id.cube_structure_id)
        logger.info(f"Cached {len(self.cube_structures_cache)} CUBE_STRUCTURE mappings")

        # 4. Prefetch all CUBE_STRUCTURE_ITEM with related data
        # Build a nested dictionary: cube_id -> {variable_id: cube_structure_item}
        # First, create reverse mapping: cube_structure_id -> cube_id
        structure_to_cube_map = {
            cube.cube_structure_id.cube_structure_id: cube_id
            for cube_id, cube in self.cubes_cache.items()
            if cube.cube_structure_id
        }

        self.cube_structure_items_cache = {}
        if cube_structure_ids_set:
            cube_structure_items = CUBE_STRUCTURE_ITEM.objects.filter(
                cube_structure_id__in=cube_structure_ids_set
            ).select_related(
                'cube_structure_id',
                'variable_id',
                'variable_id__domain_id',
                'subdomain_id'
            )

            for csi in cube_structure_items:
                # Map from cube_structure_id to cube_id using our reverse mapping
                cube_structure_id_str = csi.cube_structure_id.cube_structure_id
                cube_id = structure_to_cube_map.get(cube_structure_id_str)
                if cube_id:
                    if cube_id not in self.cube_structure_items_cache:
                        self.cube_structure_items_cache[cube_id] = {}
                    self.cube_structure_items_cache[cube_id][csi.variable_id] = csi

        logger.info(f"Cached CUBE_STRUCTURE_ITEM objects for {len(self.cube_structure_items_cache)} cubes")

        # 5. Cache MAINTENANCE_AGENCY "NODE" (queried repeatedly in loops, create if missing)
        self.node_agency, created = MAINTENANCE_AGENCY.objects.get_or_create(
            maintenance_agency_id="NODE",
            defaults={
                "code": "NODE",
                "name": "Node Maintenance Agency",
                "description": "Auto-generated maintenance agency for ANCRDT joins"
            }
        )
        if created:
            logger.info("Created MAINTENANCE_AGENCY 'NODE' (did not exist)")
        else:
            logger.info("Cached MAINTENANCE_AGENCY 'NODE'")

        # 6. Prefetch ignored DOMAIN objects
        self.ignored_domains = []
        for domain_id in IGNORED_DOMAINS:
            domain, created = DOMAIN.objects.get_or_create(
                domain_id=domain_id,
                defaults={
                    'name': domain_id,
                    'data_type': domain_id,
                    'is_enumerated': False,
                    'description': f'Primitive type: {domain_id}'
                }
            )
            if created:
                logger.info(f"Created missing DOMAIN: {domain_id}")
            self.ignored_domains.append(domain)

        logger.info(f"Cached {len(self.ignored_domains)} ignored DOMAIN objects")

        # 7. Prefetch all MEMBERS grouped by subdomain
        # First, collect all subdomain IDs from cube structure items
        all_subdomain_ids = set()
        for csi_dict in self.cube_structure_items_cache.values():
            for csi in csi_dict.values():
                if csi.subdomain_id:
                    all_subdomain_ids.add(csi.subdomain_id.subdomain_id)

        logger.info(f"Prefetching MEMBER objects for {len(all_subdomain_ids)} subdomains")
        logger.info(f"Sample subdomain IDs: {list(all_subdomain_ids)[:5]}")

        # Prefetch all members for these subdomains via SUBDOMAIN_ENUMERATION
        # Note: SUBDOMAIN_ENUMERATION links SUBDOMAIN and MEMBER (many-to-many relationship)
        self.members_by_subdomain_cache = {}
        if all_subdomain_ids:
            from pybirdai.models.bird_meta_data_model import SUBDOMAIN_ENUMERATION

            # Query SUBDOMAIN_ENUMERATION and prefetch related members
            enumerations = SUBDOMAIN_ENUMERATION.objects.filter(
                subdomain_id__subdomain_id__in=all_subdomain_ids
            ).select_related('member_id', 'subdomain_id')

            enum_count = 0
            for enum in enumerations:
                enum_count += 1
                if enum.member_id and enum.subdomain_id:
                    subdomain_id_str = enum.subdomain_id.subdomain_id
                    if subdomain_id_str not in self.members_by_subdomain_cache:
                        self.members_by_subdomain_cache[subdomain_id_str] = set()
                    self.members_by_subdomain_cache[subdomain_id_str].add(enum.member_id)

            logger.info(f"Processed {enum_count} subdomain enumeration entries")

        logger.info(f"Cached MEMBER objects for {len(self.members_by_subdomain_cache)} subdomains")
        # Log sample member counts per subdomain
        if self.members_by_subdomain_cache:
            sample_counts = [(sd_id, len(members)) for sd_id, members in list(self.members_by_subdomain_cache.items())[:5]]
            logger.info(f"Sample member counts: {sample_counts}")
        logger.info("Data prefetch completed successfully")

    def generate_joins_meta_data(self) -> dict:
        """
        Generate joins metadata by comparing cube structures.

        Only creates links that don't already exist in the database
        (links may already be loaded from BIRD step 1).
        """
        # Import here to ensure Django is fully configured first
        from pybirdai.models.bird_meta_data_model import CUBE_LINK, CUBE_STRUCTURE_ITEM_LINK, MEMBER_LINK

        # Use pre-cached ignored domains
        ignored_domains = self.ignored_domains

        mock_join_identifier = "mock_join_identifier"

        comparison_results = {}

        for val in self.join_map.values():

            mock_join_identifier = val["join_identifier"]
            # Use cached CUBE objects
            rolc_cube = self.cubes_cache.get(val["rolc"])
            if not rolc_cube:
                logger.warning(f"ROLC cube {val['rolc']} not found in cache")
                continue

            ilc_cubes = []
            for cube_id in val["ilc"]:
                cube = self.cubes_cache.get(cube_id)
                if cube:
                    ilc_cubes.append(cube)
                else:
                    logger.warning(f"{cube_id} not found in cache")

            rolc_items_to_match = {
                rolc_cube.cube_id: self.fetch_cube_structure_items_dict(rolc_cube)
            }

            ilc_items_to_match = {
                cube.cube_id: self.fetch_cube_structure_items_dict(cube)
                for cube in ilc_cubes
            }
            comparison_results = self.compare(rolc_items_to_match, ilc_items_to_match, ignored_domains)

            # DEBUG: Log comparison results
            logger.info(f"Join '{mock_join_identifier}': Comparing {val['rolc']} with {len(ilc_cubes)} ILC cubes")
            logger.info(f"  ROLC items to match: {len(rolc_items_to_match.get(val['rolc'], {}))}")
            for ilc_cube in ilc_cubes:
                logger.info(f"  ILC items to match for {ilc_cube.cube_id}: {len(ilc_items_to_match.get(ilc_cube.cube_id, {}))}")
            logger.info(f"  Comparison found {len(comparison_results)} cube pair(s) with matches")
            for (rolc_id, ilc_id), match_dict in comparison_results.items():
                logger.info(f"    Pair ({rolc_id}, {ilc_id}): {len(match_dict)} variable match(es)")

            for (rolc, ilc), matches in comparison_results.items():
                logger.info(f"  Processing cube pair: {rolc} <-> {ilc} with {len(matches)} variable match(es)")

                # Use cached CUBE and CUBE_STRUCTURE objects
                rolc_cube = self.cubes_cache.get(rolc)
                ilc_cube = self.cubes_cache.get(ilc)

                if not rolc_cube or not ilc_cube:
                    logger.warning(f"Cube not found in cache: rolc={rolc}, ilc={ilc}")
                    continue

                rolc_cube_structure = self.cube_structures_cache.get(rolc)
                ilc_cube_structure = self.cube_structures_cache.get(ilc)

                if not rolc_cube_structure or not ilc_cube_structure:
                    logger.warning(f"Cube structure not found in cache: rolc={rolc}, ilc={ilc}")
                    continue

                name_code_description = f"{ilc}:{mock_join_identifier}:{rolc}"

                # Use cached MAINTENANCE_AGENCY (guaranteed to exist after get_or_create)
                cube_link, exists = CUBE_LINK.objects.get_or_create(
                    primary_cube_id=ilc_cube,
                    foreign_cube_id=rolc_cube,
                    maintenance_agency_id=self.node_agency,
                    join_identifier=mock_join_identifier,
                    cube_link_id=name_code_description,
                    code=name_code_description,
                    name=name_code_description,
                    description=name_code_description,
                    valid_from=None,
                    valid_to=None,
                    version=1
                )
                logger.info(f"    Created/retrieved CUBE_LINK: {name_code_description} (new={not exists})")

                for (variable_rolc, variable_ilc), domain in matches.items():
                    logger.info(f"      Variable match: {variable_rolc.variable_id} <-> {variable_ilc.variable_id} with {len(domain)} common member(s)")

                    # Use cached CUBE_STRUCTURE_ITEM objects
                    rolc_cube_structure_item = self.cube_structure_items_cache.get(rolc, {}).get(variable_rolc)
                    ilc_cube_structure_item = self.cube_structure_items_cache.get(ilc, {}).get(variable_ilc)

                    if not rolc_cube_structure_item or not ilc_cube_structure_item:
                        logger.warning(f"        Cube structure item not found in cache: rolc={variable_rolc.variable_id}, ilc={variable_ilc.variable_id}")
                        logger.warning(f"        Cache keys available for {rolc}: {[v.variable_id for v in list(self.cube_structure_items_cache.get(rolc, {}).keys())[:5]]}")
                        logger.warning(f"        Cache keys available for {ilc}: {[v.variable_id for v in list(self.cube_structure_items_cache.get(ilc, {}).keys())[:5]]}")
                        continue

                    csilink, csilink_created = CUBE_STRUCTURE_ITEM_LINK.objects.get_or_create(
                        cube_structure_item_link_id=f"{ilc}:{variable_ilc.variable_id}:{mock_join_identifier}:{rolc}:{variable_rolc.variable_id}",
                        cube_link_id=cube_link,
                        primary_cube_variable_code=ilc_cube_structure_item,
                        foreign_cube_variable_code=rolc_cube_structure_item,
                    )
                    logger.info(f"        Created/retrieved CUBE_STRUCTURE_ITEM_LINK (new={csilink_created})")

                    member_link_count = 0
                    for member in domain:
                        member_link, member_created = MEMBER_LINK.objects.get_or_create(
                            cube_structure_item_link_id=csilink,
                            primary_member_id=member,
                            foreign_member_id=member,
                            is_linked=True,
                            valid_from=None,
                            valid_to=None
                        )
                        if member_created:
                            member_link_count += 1

                    logger.info(f"        Created {member_link_count} new MEMBER_LINK(s) for this variable pair")

                    # Fixed: removed unnecessary .all() call
                    # We wish to include items with no member link so that we can still ahve fucntions and cube_Structure_item _links
                    #existing_member_links = MEMBER_LINK.objects.filter(cube_structure_item_link_id=csilink).exists()
                    #if not existing_member_links:
                    #    logger.warning(f"        No MEMBER_LINK objects exist for {csilink.cube_structure_item_link_id}, deleting CUBE_STRUCTURE_ITEM_LINK")
                    #    csilink.delete()
                    #else:
                    #    logger.info(f"        CUBE_STRUCTURE_ITEM_LINK retained (has member links)")


        return comparison_results

    def compare(self, cube_items_1: dict, cube_items_2: dict,ignored_domains:list,flag_log:bool=False):
        from pybirdai.models.bird_meta_data_model import CUBE_LINK, CUBE_STRUCTURE_ITEM_LINK,MAINTENANCE_AGENCY,MEMBER

        matched_variables = dict()
        total_pairs = 0
        filtered_nevs = 0
        filtered_domain = 0
        filtered_no_members = 0
        matched_count = 0
        cube_iter = itertools.product(cube_items_1.items(), cube_items_2.items())
        for (key_rolc, value_rolc), (key_ilc, value_ilc) in cube_iter:
            cube_items_iter = itertools.product(value_rolc.items(), value_ilc.items())
            for (variable_rolc, infos_rolc), (variable_ilc, infos_ilc) in cube_items_iter:
                total_pairs += 1

                if "NEVS" in variable_rolc.name:
                    filtered_nevs += 1
                    continue

                # We should replace this with a check on the context.check_domain_members_during_join_meta_data_creation flag
                # for now we assume we are not doing the member checks, but we will need them later , especially when we
                # fix the LDM processing.
                if True: 
                    
                    if variable_rolc.variable_id == variable_ilc.variable_id:
                        members_rolc = []
                        if infos_rolc["domain"] in ignored_domains:
                            filtered_domain += 1
                        else:
                            members_rolc = self.fetch_members(infos_rolc["subdomain"])
                        
                        matched_count += 1
                        if (key_rolc, key_ilc) not in matched_variables:
                            matched_variables[(key_rolc, key_ilc)] = {}
                        matched_variables[(key_rolc, key_ilc)][(variable_rolc, variable_ilc)] = members_rolc

                else:
                    if infos_rolc["domain"] in ignored_domains or infos_ilc["domain"] in ignored_domains:
                        filtered_domain += 1
                        continue

                    members_rolc = self.fetch_members(infos_rolc["subdomain"])
                    members_ilc = self.fetch_members(infos_ilc["subdomain"])
                    members = members_rolc.intersection(members_ilc)

                    if members:
                        matched_count += 1
                        if (key_rolc, key_ilc) not in matched_variables:
                            matched_variables[(key_rolc, key_ilc)] = {}
                        # Use VARIABLE objects as keys (not variable_id strings) to match cache structure
                        matched_variables[(key_rolc, key_ilc)][(variable_rolc, variable_ilc)] = members
                    else:
                        filtered_no_members += 1

        logger.info(f"  compare() statistics:")
        logger.info(f"    Total variable pairs examined: {total_pairs}")
        logger.info(f"    Filtered by NEVS: {filtered_nevs}")
        logger.info(f"    Filtered by ignored domains: {filtered_domain}")
        logger.info(f"    Filtered by no common members: {filtered_no_members}")
        logger.info(f"    Matched pairs: {matched_count}")

        return matched_variables

    def fetch_members(self, subdomain):
        """
        Fetch members from cache instead of querying the database.
        """
        if not subdomain:
            return set()

        # Extract subdomain_id if subdomain is an object
        subdomain_id = subdomain.subdomain_id if hasattr(subdomain, 'subdomain_id') else subdomain

        # Use cached members
        return self.members_by_subdomain_cache.get(subdomain_id, set())

    def fetch_cube_structure_items_dict(self, cube):
        """
        Fetch cube structure items from cache instead of querying the database.
        """
        cube_id = cube.cube_id if hasattr(cube, 'cube_id') else cube

        # Use cached cube structure items
        cube_structure_items_dict = self.cube_structure_items_cache.get(cube_id, {})

        return {
            variable_id: {
                "variable_or_cvc": csi.cube_variable_code or csi.variable_id,
                "domain": csi.variable_id.domain_id,
                "subdomain": csi.subdomain_id,
            }
            for variable_id, csi in cube_structure_items_dict.items()
        }

def main():
    DjangoSetup.configure_django()
    creator = JoinsMetaDataCreatorANCRDT()
    result = creator.generate_joins_meta_data()

if __name__ == "__main__":
    main()
