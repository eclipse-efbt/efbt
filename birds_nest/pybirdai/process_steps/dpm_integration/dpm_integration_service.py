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
from bs4 import BeautifulSoup
import os
import zipfile
import shutil
import platform
import logging

FILES_ROOT = "https://www.eba.europa.eu/sites/default/files"
DEFAULT_DB_VERSION = f"{FILES_ROOT}/2024-12/330f4dba-be0d-4cdd-b0ed-b5a6b1fbc049/dpm_database_v4_0_20241218.zip"
DEFAULT_DB_LOCAL_PATH = "dpm_database.zip"
SCRIPT_RUNNER = "" if platform.system() == "Windows" else "bash "
PROCESS_FILE_END = ".bat" if platform.system() == "Windows" else ".sh"
SCRIPT_PATH = f"pybirdai{os.sep}process_steps{os.sep}dpm_integration{os.sep}process{PROCESS_FILE_END}"
EXTRACTED_DB_PATH = f"dpm_database{os.sep}dpm_database.accdb"

class DPMImporterService:

    def __init__(self, output_directory:str = f"export_debug{os.sep}"):
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"Initializing DPMImporterService with output directory: {output_directory}")

        self.link_db = None
        self.output_directory = f"{output_directory}{os.sep}technical_export{os.sep}"

        self.logger.debug(f"Cleaning up output directory: {self.output_directory}")
        shutil.rmtree(self.output_directory, ignore_errors=True)
        os.makedirs(self.output_directory, exist_ok=True)
        self.logger.info(f"Output directory created: {self.output_directory}")

    def fetch_link_for_database_download(self):
        main_page = "https://www.eba.europa.eu/risk-and-data-analysis/reporting-frameworks/dpm-data-dictionary"
        self.logger.info(f"Fetching DPM database link from: {main_page}")

        try:
            response = requests.get(main_page)
            response.raise_for_status()
            self.logger.debug(f"Successfully fetched page, status code: {response.status_code}")
        except requests.RequestException as e:
            self.logger.error(f"Failed to fetch DPM page: {e}")
            raise

        soup = BeautifulSoup(response.text)
        a_links = soup.find_all("a")
        self.logger.debug(f"Found {len(a_links)} links on the page")

        def is_right_link(a_el: any) -> bool:
            return "https://www.eba.europa.eu/sites/default/files" in a_el.get("href","") \
            and "/dpm_database_v" in a_el.get("href","") \
            and ".zip" in a_el.get("href","")

        for a_el in a_links:
            if a_el and is_right_link(a_el):
                self.link_db = a_el.get("href",DEFAULT_DB_VERSION)
                self.logger.info(f"Found DPM database link: {self.link_db}")
                break

        if not self.link_db:
            self.link_db = DEFAULT_DB_VERSION
            self.logger.warning(f"No DPM database link found, using default: {DEFAULT_DB_VERSION}")

    def download_dpm_database(self):
        self.logger.info(f"Starting download of DPM database from: {self.link_db}")

        try:
            response = requests.get(self.link_db)
            response.raise_for_status()
            db_data = response.content

            file_size_mb = len(db_data) / (1024 * 1024)
            self.logger.info(f"Downloaded {file_size_mb:.2f} MB")

            with open("dpm_database.zip","wb") as f:
                f.write(db_data)

            self.logger.info("DPM database successfully downloaded and saved as dpm_database.zip")
        except requests.RequestException as e:
            self.logger.error(f"Failed to download DPM database: {e}")
            raise
        except IOError as e:
            self.logger.error(f"Failed to write DPM database file: {e}")
            raise

    def extract_dpm_database(self):
        self.logger.info("Starting extraction of DPM database")

        try:
            with zipfile.ZipFile("dpm_database.zip", 'r') as zip_ref:
                self.logger.debug("Extracting zip file to dpm_database directory")
                zip_ref.extractall("dpm_database")

                extracted_files = os.listdir("dpm_database")
                self.logger.debug(f"Extracted files: {extracted_files}")

                if len(extracted_files) > 1:
                    self.logger.error(f"Expected single file but found {len(extracted_files)} files in zip")
                    raise Exception("More than one file in the zip")

                for file in extracted_files:
                    if file.endswith(".accdb"):
                        source = os.path.join("dpm_database", file)
                        target = os.path.join("dpm_database", "dpm_database.accdb")
                        self.logger.info(f"Renaming {file} to dpm_database.accdb")
                        shutil.move(source, target)

            script_cmd = f"{SCRIPT_RUNNER}{SCRIPT_PATH} {EXTRACTED_DB_PATH}"
            self.logger.info(f"Executing script: {script_cmd}")
            result = os.system(script_cmd)

            if result == 0:
                self.logger.info("Script executed successfully")
            else:
                self.logger.warning(f"Script execution returned non-zero exit code: {result}")

        except zipfile.BadZipFile as e:
            self.logger.error(f"Invalid zip file: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Failed to extract DPM database: {e}")
            raise

    def run_application(self,extract_cleanup:bool=False,download_cleanup:bool=False,with_extract:bool=True):
        self.logger.info(f"Starting DPM application run with parameters: extract_cleanup={extract_cleanup}, download_cleanup={download_cleanup}, with_extract={with_extract}")

        if with_extract:
            if os.path.exists("dpm_database"):
                self.logger.debug("Cleaning up existing dpm_database directory")
                shutil.rmtree("dpm_database")

        if with_extract:
            if not os.path.exists(DEFAULT_DB_LOCAL_PATH):
                self.logger.info("DPM database not found locally, downloading...")
                self.fetch_link_for_database_download()
                self.download_dpm_database()
            else:
                self.logger.info(f"Using existing DPM database file: {DEFAULT_DB_LOCAL_PATH}")

            self.extract_dpm_database()

        self.logger.info("Starting CSV mapping to SDD exchange format")
        self.map_csvs_to_sdd_exchange_format()

        if extract_cleanup:
            self.logger.debug("Cleaning up extracted dpm_database directory")
            shutil.rmtree("dpm_database")

        if download_cleanup:
            self.logger.debug("Cleaning up download artifacts")
            if os.path.exists("target"):
                shutil.rmtree("target")
            if os.path.exists("dpm_database.zip"):
                os.remove("dpm_database.zip")

        self.logger.info("DPM application run completed successfully")

    def write_csv_maintenance_agency(self):
        """
        Core Package
        """
        output_file = f"{self.output_directory}maintenance_agency.csv"
        self.logger.debug(f"Writing maintenance agency CSV to: {output_file}")

        try:
            with open(output_file,"w") as f:
                f.write("""MAINTENANCE_AGENCY_ID,CODE,NAME,DESCRIPTION
EBA,EBA,European Banking Authority,European Banking Authority""")
            self.logger.debug("Maintenance agency CSV written successfully")
        except IOError as e:
            self.logger.error(f"Failed to write maintenance agency CSV: {e}")
            raise

    def map_csvs_to_sdd_exchange_format(self):
        self.logger.info("Starting CSV to SDD exchange format mapping")

        try:
            import pybirdai.process_steps.dpm_integration.mapping_functions as new_maps
        except ImportError as e:
            self.logger.error(f"Failed to import mapping functions: {e}")
            raise

        """
        Core Package
        """

        self.write_csv_maintenance_agency()
        self.logger.info("Created Maintenance Agency File")

        try:
            self.logger.debug("Mapping framework entities...")
            _, framework_map = new_maps.map_frameworks() # frameworks
            _.to_csv(f"{self.output_directory}framework.csv",index=False)
            self.logger.info(f"Mapped Framework Entities - {len(_)} records")

            self.logger.debug("Mapping domain entities...")
            _, domain_map = new_maps.map_domains() # domains
            _.to_csv(f"{self.output_directory}domain.csv",index=False)
            self.logger.info(f"Mapped Domain Entities - {len(_)} records")

            self.logger.debug("Mapping member entities...")
            _, member_map = new_maps.map_members(domain_id_map=domain_map) # members
            _.to_csv(f"{self.output_directory}member.csv",index=False)
            self.logger.info(f"Mapped Members Entities - {len(_)} records")

            self.logger.debug("Mapping dimension entities...")
            _, dimension_map = new_maps.map_dimensions(domain_id_map=domain_map) # to enumerated variables
            _.to_csv(f"{self.output_directory}variable.csv",index=False)
            self.logger.info(f"Mapped Variables Entities - {len(_)} records")

            self.logger.debug("Mapping hierarchy entities...")
            _, hierarchy_map = new_maps.map_hierarchy(domain_id_map=domain_map) # member hierarchies
            _.to_csv(f"{self.output_directory}member_hierarchy.csv",index=False)
            self.logger.info(f"Mapped Hierarchy Entities - {len(_)} records")

            self.logger.debug("Mapping hierarchy node entities...")
            _, hierarchy_node_map = new_maps.map_hierarchy_node(hierarchy_map=hierarchy_map, member_map=member_map) # member hierarchy node
            _.to_csv(f"{self.output_directory}member_hierarchy_node.csv",index=False)
            self.logger.info(f"Mapped HierarchyNode Entities - {len(_)} records")
        except Exception as e:
            self.logger.error(f"Failed during mapping process: {e}")
            raise

        """
        Data Definition Package
        """

        # context_data, context_map = new_maps.map_context_definition(dimension_map=dimension_map,member_map=member_map) # to combination_items (need to improve EBA_ATY and subdomain generation)

        # (combination, combination_item), dpv_map = new_maps.map_datapoint_version(context_map=context_map,context_data=context_data,dimension_map=dimension_map,member_map=member_map) # to combinations and items
        # combination.to_csv(f"{self.output_directory}combination.csv",index=False)
        # combination_item.to_csv(f"{self.output_directory}combination_item.csv",index=False)
        # logging.info("Mapped Combination(and Items) Entities")

        """
        Rendering Package
        """
        self.logger.info("Starting Rendering Package mapping")

        try:
            self.logger.debug("Mapping table entities...")
            _, table_map = new_maps.map_tables(framework_id_map=framework_map)
            _.to_csv(f"{self.output_directory}table.csv",index=False)
            self.logger.info(f"Mapped Table Entities - {len(_)} records")

            self.logger.debug("Mapping axis entities...")
            _, axis_map = new_maps.map_axis(table_map=table_map)
            _.to_csv(f"{self.output_directory}axis.csv",index=False)
            self.logger.info(f"Mapped Axis Entities - {len(_)} records")

            self.logger.debug("Mapping axis ordinate entities...")
            _, ordinate_map = new_maps.map_axis_ordinate(axis_map=axis_map)
            _.to_csv(f"{self.output_directory}axis_ordinate.csv",index=False)
            self.logger.info(f"Mapped AxisOrdinates Entities - {len(_)} records")

            self.logger.debug("Mapping table cell entities...")
            _, cell_map = new_maps.map_table_cell(table_map=table_map)
            _.to_csv(f"{self.output_directory}table_cell.csv",index=False)
            self.logger.info(f"Mapped TableCell Entities - {len(_)} records")

            self.logger.debug("Mapping cell position entities...")
            _, cell_position_map = new_maps.map_cell_position(cell_map=cell_map,ordinate_map=ordinate_map,start_index_after_last=False)
            _.to_csv(f"{self.output_directory}cell_position.csv",index=False)
            self.logger.info(f"Mapped CellPositions Entities - {len(_)} records")

            self.logger.debug("Mapping ordinate categorisation entities...")
            _, ordinate_item_map = new_maps.map_ordinate_categorisation(member_map=member_map,dimension_map=dimension_map,ordinate_map=ordinate_map,hierarchy_map=hierarchy_map,start_index_after_last=False)
            _.to_csv(f"{self.output_directory}ordinate_item.csv",index=False)
            self.logger.info(f"Mapped OrdinateItems Entities - {len(_)} records")

            self.logger.info("Successfully completed all CSV to SDD mappings")
        except Exception as e:
            self.logger.error(f"Failed during rendering package mapping: {e}")
            raise

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    logger = logging.getLogger(__name__)
    logger.info("Starting DPM Importer Service as main module")

    try:
        DPMImporterService().run_application()
        logger.info("DPM Importer Service completed successfully")
    except Exception as e:
        logger.error(f"DPM Importer Service failed: {e}")
        raise
