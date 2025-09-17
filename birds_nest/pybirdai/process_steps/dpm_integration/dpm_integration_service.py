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
import requests
import os
import zipfile
import shutil
import platform
import logging

logger = logging.getLogger(__name__)

FILES_ROOT = "https://www.eba.europa.eu/sites/default/files"
DEFAULT_DB_VERSION = f"{FILES_ROOT}/2024-12/330f4dba-be0d-4cdd-b0ed-b5a6b1fbc049/dpm_database_v4_0_20241218.zip"
DEFAULT_DB_LOCAL_PATH = "dpm_database.zip"
SCRIPT_RUNNER = "" if platform.system() == "Windows" else "bash "
PROCESS_FILE_END = ".bat" if platform.system() == "Windows" else ".sh"
SCRIPT_PATH = f"pybirdai{os.sep}process_steps{os.sep}dpm_integration{os.sep}process{PROCESS_FILE_END}"
EXTRACTED_DB_PATH = f"dpm_database{os.sep}dpm_database.accdb"

class DPMImporterService:

    def __init__(self, output_directory:str = f"export_debug{os.sep}"):
        logger.info(f"Initializing DPMImporterService with output directory: {output_directory}")
        self.link_db = None
        self.output_directory = f"{output_directory}{os.sep}technical_export{os.sep}"
        logger.debug(f"Setting output directory to: {self.output_directory}")

        if os.path.exists(self.output_directory):
            logger.info(f"Removing existing output directory: {self.output_directory}")
        shutil.rmtree(self.output_directory, ignore_errors=True)

        logger.info(f"Creating output directory: {self.output_directory}")
        os.makedirs(self.output_directory, exist_ok=True)

    def fetch_link_for_database_download(self):
        logger.info("Starting to fetch DPM database download link")
        self.link_db = DEFAULT_DB_VERSION
        logger.debug(f"Default database version: {DEFAULT_DB_VERSION}")

        try:
            from bs4 import BeautifulSoup
            main_page = "https://www.eba.europa.eu/risk-and-data-analysis/reporting-frameworks/dpm-data-dictionary"
            logger.info(f"Fetching main page: {main_page}")

            response = requests.get(main_page)
            logger.debug(f"Response status code: {response.status_code}")
            soup = BeautifulSoup(response.text)
            a_links = soup.find_all("a")
            logger.info(f"Found {len(a_links)} links on the page")
            if a_links:
                logger.debug(f"First link found: {a_links[0].get('href', 'No href')}")

            def is_right_link(a_el: any) -> bool:
                return "https://www.eba.europa.eu/sites/default/files" in a_el.get("href","") \
                and "/dpm_database_v" in a_el.get("href","") \
                and ".zip" in a_el.get("href","")

            for i, a_el in enumerate(a_links):
                if a_el and is_right_link(a_el):
                    self.link_db = a_el.get("href",DEFAULT_DB_VERSION)
                    logger.info(f"Found DPM database link at position {i}: {self.link_db}")
                    break
            else:
                logger.warning("No matching DPM database link found, using default")
        except ImportError as e:
            logger.warning(f"BeautifulSoup not available: {e}. Using default database link")
        except Exception as e:
            logger.error(f"Error fetching database link: {e}. Using default database link")

    def download_dpm_database(self):
        logger.info(f"Starting DPM database download from: {self.link_db}")
        logger.info(f"Downloading to: {DEFAULT_DB_LOCAL_PATH}")

        try:
            response = requests.get(self.link_db)
            response.raise_for_status()

            with open(DEFAULT_DB_LOCAL_PATH, "wb") as f:
                db_data = response.content
                f.write(db_data)
                logger.info(f"Successfully downloaded {len(db_data)} bytes to {DEFAULT_DB_LOCAL_PATH}")
        except requests.RequestException as e:
            logger.error(f"Failed to download DPM database: {e}")
            raise

    def extract_dpm_database(self):
        logger.info(f"Starting extraction of {DEFAULT_DB_LOCAL_PATH}")

        with zipfile.ZipFile(DEFAULT_DB_LOCAL_PATH, 'r') as zip_ref:
            logger.info("Extracting zip file to dpm_database directory")
            zip_ref.extractall("dpm_database")

            extracted_files = os.listdir("dpm_database")
            logger.info(f"Extracted {len(extracted_files)} file(s): {extracted_files}")

            if len(extracted_files) > 1:
                logger.error(f"Expected 1 file but found {len(extracted_files)} files in zip")
                raise Exception("More than one file in the zip")

            for file in extracted_files:
                if file.endswith(".accdb"):
                    old_path = os.path.join("dpm_database", file)
                    new_path = os.path.join("dpm_database", "dpm_database.accdb")
                    logger.info(f"Moving {old_path} to {new_path}")
                    shutil.move(old_path, new_path)

        command = f"{SCRIPT_RUNNER}{SCRIPT_PATH} {EXTRACTED_DB_PATH}"
        logger.info(f"Running extraction script: {command}")
        result = os.system(command)
        logger.info(f"Script execution completed with return code: {result}")

    def run_application(self,extract_cleanup:bool=False,download_cleanup:bool=False,with_extract:bool=True):
        logger.info(f"Starting DPM application run with parameters: extract_cleanup={extract_cleanup}, download_cleanup={download_cleanup}, with_extract={with_extract}")

        if with_extract:
            if os.path.exists("dpm_database"):
                logger.info("Removing existing dpm_database directory")
                shutil.rmtree("dpm_database")

            if not os.path.exists(DEFAULT_DB_LOCAL_PATH):
                logger.info(f"{DEFAULT_DB_LOCAL_PATH} not found, will download")
                self.fetch_link_for_database_download()
                self.download_dpm_database()
            else:
                logger.info(f"Using existing database file: {DEFAULT_DB_LOCAL_PATH}")

            self.extract_dpm_database()
        else:
            logger.info("Skipping extraction phase")

        logger.info("Starting CSV mapping phase")
        self.map_csvs_to_sdd_exchange_format()

        if extract_cleanup:
            logger.info("Cleaning up extracted database directory")
            shutil.rmtree("dpm_database")
        if download_cleanup:
            logger.info("Cleaning up downloaded files")
            if os.path.exists("target"):
                shutil.rmtree("target")
            if os.path.exists(DEFAULT_DB_LOCAL_PATH):
                os.remove(DEFAULT_DB_LOCAL_PATH)

        logger.info("DPM application run completed")

    def write_csv_maintenance_agency(self):
        """
        Core Package
        """
        output_file = f"{self.output_directory}maintenance_agency.csv"
        logger.debug(f"Writing maintenance agency CSV to: {output_file}")

        with open(output_file, "w") as f:
            f.write("""MAINTENANCE_AGENCY_ID,CODE,NAME,DESCRIPTION
EBA,EBA,European Banking Authority,European Banking Authority""")

        logger.info(f"Created maintenance agency file: {output_file}")

    def map_csvs_to_sdd_exchange_format(self):
        logger.info("Starting CSV to SDD exchange format mapping")

        import pybirdai.process_steps.dpm_integration.mapping_functions as new_maps
        logger.debug("Imported mapping functions module")

        """
        Core Package
        """
        logger.info("Processing Core Package")
        self.write_csv_maintenance_agency()
        logger.info("Mapping Framework entities")
        _, framework_map = new_maps.map_frameworks() # frameworks
        output_file = f"{self.output_directory}framework.csv"
        _.to_csv(output_file, index=False)
        logger.info(f"Mapped {len(_)} Framework entities to {output_file}")
        logger.info("Mapping Domain entities")
        _, domain_map = new_maps.map_domains() # domains
        output_file = f"{self.output_directory}domain.csv"
        _.to_csv(output_file, index=False)
        logger.info(f"Mapped {len(_)} Domain entities to {output_file}")
        logger.info("Mapping Member entities")
        _, member_map = new_maps.map_members(domain_id_map=domain_map) # members
        output_file = f"{self.output_directory}member.csv"
        _.to_csv(output_file, index=False)
        logger.info(f"Mapped {len(_)} Member entities to {output_file}")
        logger.info("Mapping Dimension/Variable entities")
        _, dimension_map = new_maps.map_dimensions(domain_id_map=domain_map) # to enumerated variables
        output_file = f"{self.output_directory}variable.csv"
        _.to_csv(output_file, index=False)
        logger.info(f"Mapped {len(_)} Variable entities to {output_file}")
        logger.info("Mapping Member Hierarchy entities")
        _, hierarchy_map = new_maps.map_hierarchy(domain_id_map=domain_map) # member hierarchies
        output_file = f"{self.output_directory}member_hierarchy.csv"
        _.to_csv(output_file, index=False)
        logger.info(f"Mapped {len(_)} Hierarchy entities to {output_file}")
        logger.info("Mapping Hierarchy Node entities")
        _, hierarchy_node_map = new_maps.map_hierarchy_node(hierarchy_map=hierarchy_map, member_map=member_map) # member hierarchy node
        output_file = f"{self.output_directory}member_hierarchy_node.csv"
        _.to_csv(output_file, index=False)
        logger.info(f"Mapped {len(_)} HierarchyNode entities to {output_file}")

        """
        Data Definition Package
        """
        logger.info("Data Definition Package mapping skipped (commented out)")
        # context_data, context_map = new_maps.map_context_definition(dimension_map=dimension_map,member_map=member_map) # to combination_items (need to improve EBA_ATY and subdomain generation)

        # (combination, combination_item), dpv_map = new_maps.map_datapoint_version(context_map=context_map,context_data=context_data,dimension_map=dimension_map,member_map=member_map) # to combinations and items
        # combination.to_csv(f"{self.output_directory}combination.csv",index=False)
        # combination_item.to_csv(f"{self.output_directory}combination_item.csv",index=False)
        # logger.info("Mapped Combination(and Items) Entities")

        """
        Rendering Package
        """
        logger.info("Processing Rendering Package")
        logger.info("Mapping Table entities")
        _, table_map = new_maps.map_tables(framework_id_map=framework_map)
        output_file = f"{self.output_directory}table.csv"
        _.to_csv(output_file, index=False)
        logger.info(f"Mapped {len(_)} Table entities to {output_file}")
        logger.info("Mapping Axis entities")
        _, axis_map = new_maps.map_axis(table_map=table_map)
        output_file = f"{self.output_directory}axis.csv"
        _.to_csv(output_file, index=False)
        logger.info(f"Mapped {len(_)} Axis entities to {output_file}")
        logger.info("Mapping Axis Ordinate entities")
        _, ordinate_map = new_maps.map_axis_ordinate(axis_map=axis_map)
        output_file = f"{self.output_directory}axis_ordinate.csv"
        _.to_csv(output_file, index=False)
        logger.info(f"Mapped {len(_)} AxisOrdinate entities to {output_file}")
        logger.info("Mapping Table Cell entities")
        _, cell_map = new_maps.map_table_cell(table_map=table_map)
        output_file = f"{self.output_directory}table_cell.csv"
        _.to_csv(output_file, index=False)
        logger.info(f"Mapped {len(_)} TableCell entities to {output_file}")
        logger.info("Mapping Cell Position entities")
        _, cell_position_map = new_maps.map_cell_position(cell_map=cell_map,ordinate_map=ordinate_map,start_index_after_last=False)
        output_file = f"{self.output_directory}cell_position.csv"
        _.to_csv(output_file, index=False)
        logger.info(f"Mapped {len(_)} CellPosition entities to {output_file}")
        logger.info("Mapping Ordinate Item entities")
        _, ordinate_item_map = new_maps.map_ordinate_categorisation(member_map=member_map,dimension_map=dimension_map,ordinate_map=ordinate_map,hierarchy_map=hierarchy_map,start_index_after_last=False)
        output_file = f"{self.output_directory}ordinate_item.csv"
        _.to_csv(output_file, index=False)
        logger.info(f"Mapped {len(_)} OrdinateItem entities to {output_file}")

        logger.info("CSV to SDD exchange format mapping completed")

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger.info("Starting DPM Integration Service as main")
    DPMImporterService().run_application()
