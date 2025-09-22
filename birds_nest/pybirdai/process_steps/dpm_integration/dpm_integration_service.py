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
import numpy as np
import csv

logger = logging.getLogger(__name__)

FILES_ROOT = "https://www.eba.europa.eu/sites/default/files"
DEFAULT_DB_VERSION = f"{FILES_ROOT}/2024-12/330f4dba-be0d-4cdd-b0ed-b5a6b1fbc049/dpm_database_v4_0_20241218.zip"
DEFAULT_DB_LOCAL_PATH = "dpm_database.zip"
SCRIPT_RUNNER = "" if platform.system() == "Windows" else "bash "
PROCESS_FILE_END = ".bat" if platform.system() == "Windows" else ".sh"
SCRIPT_PATH = f"pybirdai{os.sep}process_steps{os.sep}dpm_integration{os.sep}process{PROCESS_FILE_END}"
EXTRACTED_DB_PATH = f"dpm_database{os.sep}dpm_database.accdb"

def save_numpy_array_to_csv(array, filepath, index=False):
    """Save numpy structured array to CSV file"""
    if len(array) == 0:
        # Write empty file with headers only
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            if hasattr(array, 'dtype') and array.dtype.names:
                writer = csv.writer(f)
                writer.writerow(array.dtype.names)
        return

    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)

        # Write header
        writer.writerow(array.dtype.names)

        # Write data rows
        for row in array:
            row_data = []
            for field in array.dtype.names:
                val = row[field]
                # Convert numpy types to Python types for proper CSV writing
                if isinstance(val, (np.bool_, bool)):
                    row_data.append(str(val))
                elif isinstance(val, (np.integer, int)):
                    row_data.append(str(val))
                elif isinstance(val, (np.floating, float)):
                    # Check for NaN
                    if np.isnan(val):
                        row_data.append('')
                    else:
                        row_data.append(str(val))
                else:
                    # String or other types
                    row_data.append(str(val))
            writer.writerow(row_data)

class DPMImporterService:

    def __init__(self, output_directory:str = f"export_debug{os.sep}"):

        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.info(f"Initializing DPMImporterService with output directory: {output_directory}")
        self.link_db = None
        self.output_directory = f"{output_directory}{os.sep}technical_export{os.sep}"
        self.logger.debug(f"Setting output directory to: {self.output_directory}")

        if os.path.exists(self.output_directory):
            self.logger.info(f"Removing existing output directory: {self.output_directory}")

        shutil.rmtree(self.output_directory, ignore_errors=True)

        self.logger.info(f"Creating output directory: {self.output_directory}")
        os.makedirs(self.output_directory, exist_ok=True)
        self.logger.info(f"Output directory created: {self.output_directory}")

    def fetch_link_for_database_download(self):
        try:
            from bs4 import BeautifulSoup
            main_page = "https://www.eba.europa.eu/risk-and-data-analysis/reporting-frameworks/dpm-data-dictionary"
            soup = BeautifulSoup(requests.get(main_page).text)
            a_links = soup.find_all("a")
            print(a_links[0]["href"])

            def is_right_link(a_el: any) -> bool:
                return "https://www.eba.europa.eu/sites/default/files" in a_el.get("href","") \
                and "/dpm_database_v" in a_el.get("href","") \
                and ".zip" in a_el.get("href","")

            for a_el in a_links:
                if a_el and is_right_link(a_el):
                    self.link_db = a_el.get("href",DEFAULT_DB_VERSION)
                    break
        except Exception as e:
            pass
        finally:
            if not self.link_db:
                self.link_db = DEFAULT_DB_VERSION

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

        logger.info(f"Created maintenance agency file: {output_file}")

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
        logger.info("Processing Core Package")
        self.write_csv_maintenance_agency()
        logging.info("Created Maintenance Agency File")

        frameworks_array, framework_map = new_maps.map_frameworks() # frameworks
        save_numpy_array_to_csv(frameworks_array, f"{self.output_directory}framework.csv", index=False)
        logging.info("Mapped Framework Entities")

        domains_array, domain_map = new_maps.map_domains() # domains
        save_numpy_array_to_csv(domains_array, f"{self.output_directory}domain.csv", index=False)
        logging.info("Mapped Domain Entities")

        members_array, member_map = new_maps.map_members(domain_id_map=domain_map) # members
        save_numpy_array_to_csv(members_array, f"{self.output_directory}member.csv", index=False)
        logging.info("Mapped Members Entities")

        dimensions_array, dimension_map = new_maps.map_dimensions(domain_id_map=domain_map) # to enumerated variables
        save_numpy_array_to_csv(dimensions_array, f"{self.output_directory}variable.csv", index=False)
        logging.info("Mapped Variables Entities")

        hierarchy_array, hierarchy_map = new_maps.map_hierarchy(domain_id_map=domain_map) # member hierarchies
        save_numpy_array_to_csv(hierarchy_array, f"{self.output_directory}member_hierarchy.csv", index=False)
        logging.info("Mapped Hierarchy Entities")

        hierarchy_node_array, hierarchy_node_map = new_maps.map_hierarchy_node(hierarchy_map=hierarchy_map, member_map=member_map) # member hierarchy node
        save_numpy_array_to_csv(hierarchy_node_array, f"{self.output_directory}member_hierarchy_node.csv", index=False)
        logging.info("Mapped HierarchyNode Entities")


        """
        Data Definition Package
        """
        logger.info("Data Definition Package mapping skipped (commented out)")
        # context_data, context_map = new_maps.map_context_definition(dimension_map=dimension_map,member_map=member_map) # to combination_items (need to improve EBA_ATY and subdomain generation)

        # (combination, combination_item), dpv_map = new_maps.map_datapoint_version(context_map=context_map,context_data=context_data,dimension_map=dimension_map,member_map=member_map) # to combinations and items
        # save_numpy_array_to_csv(combination, f"{self.output_directory}combination.csv", index=False)
        # save_numpy_array_to_csv(combination_item, f"{self.output_directory}combination_item.csv", index=False)
        # logging.info("Mapped Combination(and Items) Entities")


        """
        Rendering Package
        """

        tables_array, table_map = new_maps.map_tables(framework_id_map=framework_map)
        save_numpy_array_to_csv(tables_array, f"{self.output_directory}table.csv", index=False)
        logging.info("Mapped Table Entities")

        axes_array, axis_map = new_maps.map_axis(table_map=table_map)
        save_numpy_array_to_csv(axes_array, f"{self.output_directory}axis.csv", index=False)
        logging.info("Mapped Axis Entities")

        ordinates_array, ordinate_map = new_maps.map_axis_ordinate(axis_map=axis_map)
        save_numpy_array_to_csv(ordinates_array, f"{self.output_directory}axis_ordinate.csv", index=False)
        logging.info("Mapped AxisOrdinates Entities")

        cells_array, cell_map = new_maps.map_table_cell(table_map=table_map)
        save_numpy_array_to_csv(cells_array, f"{self.output_directory}table_cell.csv", index=False)
        logging.info("Mapped TableCell Entities")

        cell_positions_array, cell_position_map = new_maps.map_cell_position(cell_map=cell_map,ordinate_map=ordinate_map,start_index_after_last=False)
        save_numpy_array_to_csv(cell_positions_array, f"{self.output_directory}cell_position.csv", index=False)
        logging.info("Mapped CellPositions Entities")

        ordinate_items_array, ordinate_item_map = new_maps.map_ordinate_categorisation(member_map=member_map,dimension_map=dimension_map,ordinate_map=ordinate_map,hierarchy_map=hierarchy_map,start_index_after_last=False)
        save_numpy_array_to_csv(ordinate_items_array, f"{self.output_directory}ordinate_item.csv", index=False)
        logging.info("Mapped OrdinateItems Entities")

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

