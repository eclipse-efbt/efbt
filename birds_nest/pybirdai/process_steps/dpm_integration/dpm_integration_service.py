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
import pandas as pd
import csv

logger = logging.getLogger(__name__)

FILES_ROOT = "https://www.eba.europa.eu/sites/default/files"
DEFAULT_DB_VERSION = f"{FILES_ROOT}/2024-12/330f4dba-be0d-4cdd-b0ed-b5a6b1fbc049/dpm_database_v4_0_20241218.zip"
DEFAULT_DB_LOCAL_PATH = "dpm_database.zip"
SCRIPT_RUNNER = "" if platform.system() == "Windows" else "bash "
PROCESS_FILE_END = ".bat" if platform.system() == "Windows" else ".sh"
SCRIPT_PATH = f"pybirdai{os.sep}process_steps{os.sep}dpm_integration{os.sep}process{PROCESS_FILE_END}"
EXTRACTED_DB_PATH = f"dpm_database{os.sep}dpm_database.accdb"

# HTTP headers to mimic a real browser and avoid 403 Forbidden errors
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
}


class DPMImporterService:

    def __init__(self, output_directory:str = f"export_debug{os.sep}", preserve_existing:bool = False):

        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.info(f"Initializing DPMImporterService with output directory: {output_directory}, preserve_existing: {preserve_existing}")
        self.link_db = None
        self.output_directory = f"{output_directory}{os.sep}technical_export{os.sep}"
        # Store absolute path to target directory to ensure consistent path resolution
        self.target_path = os.path.abspath("target")
        self.logger.debug(f"Setting output directory to: {self.output_directory}")
        self.logger.debug(f"Setting target path to: {self.target_path}")

        if not preserve_existing:
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
            soup = BeautifulSoup(requests.get(main_page, headers=HEADERS).text)
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
            response = requests.get(self.link_db, headers=HEADERS)
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

    def run_application(self,extract_cleanup:bool=False,download_cleanup:bool=False,with_extract:bool=True,enable_table_duplication:bool=True,frameworks:list=None,selected_tables:list=None):
        """
        Run the DPM application to extract and map DPM database.

        Args:
            frameworks: List of framework codes to import (e.g., ['FINREP', 'COREP']).
                       If None, all frameworks are imported.
            selected_tables: List of table_ids to process (filters ordinates/cells).
                            If None, all tables are processed.
        """
        self.logger.info(f"Starting DPM application run with parameters: extract_cleanup={extract_cleanup}, download_cleanup={download_cleanup}, with_extract={with_extract}, enable_table_duplication={enable_table_duplication}, frameworks={frameworks}, selected_tables={len(selected_tables) if selected_tables else 'all'}")

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

        self.logger.info(f"Starting CSV mapping to SDD exchange format with frameworks: {frameworks}, selected_tables: {len(selected_tables) if selected_tables else 'all'}")
        self.map_csvs_to_sdd_exchange_format(enable_table_duplication=enable_table_duplication, frameworks=frameworks, selected_tables=selected_tables)

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

    def map_csvs_phase_a(self, frameworks:list=None):
        """
        Phase A: Lightweight metadata extraction (before ordinate explosion).

        Extracts basic entities up to cell_positions. This is fast and allows
        users to select tables before the expensive ordinate explosion.

        Args:
            frameworks: List of framework codes to import

        Returns:
            dict: Contains all dataframes and maps needed for Phase B
        """
        self.logger.info(f"Starting Phase A: Lightweight metadata extraction (frameworks={frameworks})")

        try:
            import pybirdai.process_steps.dpm_integration.mapping_functions as new_maps
        except ImportError as e:
            self.logger.error(f"Failed to import mapping functions: {e}")
            raise

        # Core Package
        logger.info("Processing Core Package")
        self.write_csv_maintenance_agency()
        logging.info("Created Maintenance Agency File")

        framework_df, framework_map = new_maps.map_frameworks(frameworks=frameworks, base_path=self.target_path)
        framework_df.to_csv(f"{self.output_directory}framework.csv", index=False, encoding='utf-8')
        logging.info(f"Mapped {len(framework_df)} Framework Entities (filtered: {frameworks})")

        domains_array, domain_map = new_maps.map_domains(base_path=self.target_path)

        # Map metrics from configuration
        metrics_array, metrics_map, data_type_domains = new_maps.map_metrics(domain_map=domain_map)
        logging.info(f"Mapped {len(metrics_array)} Metric Variables from configuration")
        logging.info(f"Created {len(data_type_domains)} Data Type Domains for metrics")

        # Merge DPM domains with data type domains
        combined_domains = pd.concat([domains_array, data_type_domains])
        combined_domains.to_csv(f"{self.output_directory}domain.csv", index=False, encoding='utf-8')
        logging.info(f"Saved {len(combined_domains)} total Domains")

        members_array, member_map = new_maps.map_members(domain_id_map=domain_map, base_path=self.target_path)
        members_array.to_csv(f"{self.output_directory}member.csv", index=False)
        logging.info("Mapped Members Entities")

        dimensions_array, dimension_map = new_maps.map_dimensions(domain_id_map=domain_map, base_path=self.target_path)

        # Merge dimensions and metrics
        combined_variables = pd.concat([dimensions_array, metrics_array])
        combined_variables.to_csv(f"{self.output_directory}variable.csv", index=False)
        logging.info(f"Saved {len(combined_variables)} total Variables")

        hierarchy_df, hierarchy_map = new_maps.map_hierarchy(domain_id_map=domain_map, base_path=self.target_path)
        hierarchy_df.to_csv(f"{self.output_directory}member_hierarchy.csv", index=False)
        logging.info("Mapped Hierarchy Entities")

        hierarchy_node_df, hierarchy_node_map = new_maps.map_hierarchy_node(hierarchy_map=hierarchy_map, member_map=member_map, base_path=self.target_path)
        hierarchy_node_df.to_csv(f"{self.output_directory}member_hierarchy_node.csv", index=False)
        logging.info("Mapped HierarchyNode Entities")

        # Rendering Package - Tables only (axes/ordinates/cells moved to Phase B)
        tables_df, table_map, framework_table_df = new_maps.map_tables(framework_id_map=framework_map, frameworks=frameworks, generate_framework_table=True, base_path=self.target_path)
        framework_table_df.to_csv(f"{self.output_directory}framework_table.csv", index=False)
        tables_df.to_csv(f"{self.output_directory}table.csv", index=False)
        logging.info(f"Mapped {len(tables_df)} Table Entities")
        logging.info("Phase A complete - table.csv written for user selection")

        # Save ID mapping dictionaries for Phase B to use
        # These maps convert DPM numeric IDs to string-based BIRD IDs
        import json
        maps_to_save = {
            'member_map': {str(k): v for k, v in member_map.items()},
            'dimension_map': {str(k): v for k, v in dimension_map.items()},
            'hierarchy_map': {str(k): v for k, v in hierarchy_map.items()},
            'metrics_map': {str(k): v for k, v in metrics_map.items()},
            'domain_map': {str(k): v for k, v in domain_map.items()},
            'table_map': {str(k): v for k, v in table_map.items()},
        }

        for map_name, map_data in maps_to_save.items():
            map_path = f"{self.output_directory}{map_name}.json"
            with open(map_path, 'w', encoding='utf-8') as f:
                json.dump(map_data, f, indent=2, ensure_ascii=False)
            logging.info(f"Saved {map_name} with {len(map_data)} mappings to {map_path}")

        # Return all data needed for Phase B (axes/ordinates/cells will be created in Phase B)
        return {
            'new_maps': new_maps,
            'target_path': self.target_path,  # Include target_path for Phase B
            'framework_df': framework_df,
            'framework_map': framework_map,
            'domains_array': domains_array,
            'domain_map': domain_map,
            'metrics_array': metrics_array,
            'metrics_map': metrics_map,
            'members_array': members_array,
            'member_map': member_map,
            'dimensions_array': dimensions_array,
            'dimension_map': dimension_map,
            'hierarchy_df': hierarchy_df,
            'hierarchy_map': hierarchy_map,
            'hierarchy_node_df': hierarchy_node_df,
            'tables_df': tables_df,
            'table_map': table_map,
        }

    def map_csvs_phase_b(self, phase_a_data: dict, selected_tables: list = None, enable_table_duplication: bool = False):
        """
        Phase B: Filtered ordinate explosion and finalization.

        Takes Phase A data and optionally filters by selected_tables before
        running the expensive ordinate explosion.

        Args:
            phase_a_data: Dictionary returned from map_csvs_phase_a()
            selected_tables: List of table_ids to process (filters ordinates/cells)
            enable_table_duplication: Whether to enable Z-axis table duplication
        """
        self.logger.info(f"Starting Phase B: Ordinate explosion (selected_tables={len(selected_tables) if selected_tables else 'all'}, duplication={enable_table_duplication})")

        # Extract data from phase_a_data
        new_maps = phase_a_data['new_maps']
        member_map = phase_a_data['member_map']
        dimension_map = phase_a_data['dimension_map']
        hierarchy_map = phase_a_data['hierarchy_map']
        metrics_map = phase_a_data['metrics_map']
        dimensions_array = phase_a_data['dimensions_array']
        members_array = phase_a_data['members_array']
        hierarchy_df = phase_a_data['hierarchy_df']
        hierarchy_node_df = phase_a_data['hierarchy_node_df']
        tables_df = phase_a_data['tables_df']
        table_map = phase_a_data['table_map']

        # Filter tables if selected_tables provided
        if selected_tables:
            self.logger.info(f"Filtering to {len(selected_tables)} selected tables BEFORE mapping")
            tables_df = tables_df[tables_df['TABLE_ID'].isin(selected_tables)]

            # Get table_map from phase_a_data and filter to only selected tables
            # CSV no longer contains TABLE_VID column - mapping comes from JSON
            table_map = phase_a_data.get('table_map', {})
            table_map = {vid: tid for vid, tid in table_map.items() if tid in selected_tables}

            self.logger.info(f"Filtered to {len(tables_df)} tables (table_map: {len(table_map)} entries)")
        else:
            # No filtering - use full table_map
            table_map = phase_a_data.get('table_map', {})

        # Now map axes/ordinates/cells/positions for the FILTERED tables only
        self.logger.info("Mapping axes/ordinates/cells for filtered tables...")

        axes_df, axis_map = new_maps.map_axis(table_map=table_map, save_z_axis_config=False, output_directory="results/dpm_z_axis_configuration", base_path=self.target_path)
        logging.info(f"Mapped {len(axes_df)} Axis Entities")

        ordinates_df, ordinate_map = new_maps.map_axis_ordinate(axis_map=axis_map, base_path=self.target_path)
        logging.info(f"Mapped {len(ordinates_df)} AxisOrdinates Entities")

        cells_df, cell_map = new_maps.map_table_cell(table_map=table_map, base_path=self.target_path)
        logging.info(f"Mapped {len(cells_df)} TableCell Entities")

        cell_positions_df, cell_position_map = new_maps.map_cell_position(cell_map=cell_map, ordinate_map=ordinate_map, start_index_after_last=False, base_path=self.target_path)
        logging.info(f"Mapped {len(cell_positions_df)} CellPositions Entities")

        # Run ordinate explosion (THE EXPENSIVE OPERATION)
        # Use absolute target_path for consistent path resolution
        ordinate_cat_path = os.path.join(self.target_path, "OrdinateCategorisation.csv")
        logging.info(f"Reading OrdinateCategorisation from: {ordinate_cat_path}")

        ordinate_items_df, ordinate_item_info = new_maps.map_ordinate_categorisation(
            path=ordinate_cat_path,
            base_path=self.target_path,
            member_map=member_map,
            dimension_map=dimension_map,
            ordinate_map=ordinate_map,
            hierarchy_map=hierarchy_map,
            metrics_map=metrics_map,
            start_index_after_last=False
        )
        logging.info("Mapped OrdinateItems Entities (explosion complete)")

        # Log ATY transformation statistics
        aty_stats = ordinate_item_info.get("aty_transformation", {})
        if aty_stats.get("aty_items_found", 0) > 0:
            logging.info(f"ATY Transformation: Found {aty_stats['aty_items_found']} ATY ordinate items")
            logging.info(f"  - Transformed to field variables: {aty_stats['aty_items_transformed']}")
            logging.info(f"  - Not found in metrics map: {aty_stats['aty_items_not_in_metrics_map']}")

        # Handle ordinate items with null members
        variables_with_null_members = ordinate_item_info.get("variables_with_null_members", [])
        if variables_with_null_members:
            logging.info(f"Found {len(variables_with_null_members)} variables with null members, creating default hierarchies")

            variable_to_domain_map = dict(zip(
                dimensions_array['VARIABLE_ID'].astype(str),
                dimensions_array['DOMAIN_ID'].astype(str)
            ))

            domains_needing_hierarchy = list(set(
                variable_to_domain_map.get(var_id) for var_id in variables_with_null_members
                if variable_to_domain_map.get(var_id)
            ))
            logging.info(f"Domains needing default hierarchies: {domains_needing_hierarchy}")

            members_array = new_maps.ensure_x0_members(members_array, domains_needing_hierarchy)
            default_hierarchies_df, domain_to_hierarchy_map = new_maps.create_default_hierarchies(domains_needing_hierarchy)
            hierarchy_df = pd.concat([hierarchy_df, default_hierarchies_df], ignore_index=True)

            default_hierarchy_nodes_df = new_maps.create_all_default_hierarchy_nodes(domain_to_hierarchy_map, members_array)
            hierarchy_node_df = pd.concat([hierarchy_node_df, default_hierarchy_nodes_df], ignore_index=True)

            ordinate_items_df = new_maps.update_ordinate_items_with_default_hierarchies(
                ordinate_items_df, variable_to_domain_map, domain_to_hierarchy_map
            )
            logging.info("Updated ordinate items with default hierarchy information")

            # Re-save updated CSVs
            hierarchy_df.to_csv(f"{self.output_directory}member_hierarchy.csv", index=False)
            hierarchy_node_df.to_csv(f"{self.output_directory}member_hierarchy_node.csv", index=False)
            members_array.to_csv(f"{self.output_directory}member.csv", index=False)

        # Conditionally duplicate tables for Z-axis members
        if enable_table_duplication:
            logging.info("="*60)
            logging.info("STARTING Z-AXIS TABLE DUPLICATION")
            logging.info(f"  Tables to check: {len(tables_df)}")
            logging.info(f"  Ordinates: {len(ordinates_df)}")
            logging.info(f"  Ordinate items: {len(ordinate_items_df)}")
            logging.info("="*60)

            # Remove any existing CSV files from previous runs to ensure clean write
            # (process_all_tables writes new files with correct database column names)
            for csv_file in ['table.csv', 'axis.csv', 'axis_ordinate.csv',
                           'table_cell.csv', 'cell_position.csv', 'ordinate_item.csv']:
                file_path = f"{self.output_directory}{csv_file}"
                if os.path.exists(file_path):
                    os.remove(file_path)
                    logging.debug(f"Removed existing file: {file_path}")

            duplication_stats = new_maps.process_all_tables(
                tables_df=tables_df,
                axes_df=axes_df,
                ordinates_df=ordinates_df,
                cells_df=cells_df,
                cell_positions_df=cell_positions_df,
                ordinate_cat_df=ordinate_items_df,
                members_df=members_array,
                dimensions_df=dimensions_array,
                output_directory=self.output_directory,
                min_table_version=0.0,
                batch_size=200,
                hierarchy_nodes_df=hierarchy_node_df
            )

            logging.info("="*60)
            logging.info("Z-AXIS TABLE DUPLICATION COMPLETE")
            logging.info(f"  Stats: {duplication_stats}")
            logging.info("="*60)
        else:
            logging.info("Table duplication disabled - writing final CSVs")
            tables_df.to_csv(f"{self.output_directory}table.csv", index=False)
            axes_df.to_csv(f"{self.output_directory}axis.csv", index=False)
            ordinates_df.to_csv(f"{self.output_directory}axis_ordinate.csv", index=False)
            cells_df.to_csv(f"{self.output_directory}table_cell.csv", index=False)

            # Write cell_positions without ID column (SQLite will auto-generate)
            cell_positions_to_write = cell_positions_df.copy()
            if 'ID' in cell_positions_to_write.columns:
                cell_positions_to_write = cell_positions_to_write.drop(columns=['ID'])
            cell_positions_to_write.to_csv(f"{self.output_directory}cell_position.csv", index=False)

            # Write ordinate_items with database column names (for SQLite import compatibility)
            from pybirdai.process_steps.dpm_integration.mapping_functions.table_duplication import prepare_ordinate_item_for_csv
            prepare_ordinate_item_for_csv(ordinate_items_df).to_csv(f"{self.output_directory}ordinate_item.csv", index=False, na_rep='')

        logging.info("Phase B complete - all CSVs written")

        # Memory cleanup after heavy DataFrame operations
        import gc
        del ordinate_items_df, cells_df, cell_positions_df, ordinates_df, axes_df
        gc.collect()
        logging.info("Memory cleanup completed after Phase B")

    def map_csvs_to_sdd_exchange_format(self, enable_table_duplication:bool=False, frameworks:list=None, selected_tables:list=None):
        """
        Complete CSV mapping (backward compatibility wrapper).

        Calls Phase A then Phase B. If selected_tables provided, filters before ordinate explosion.

        Args:
            enable_table_duplication: Whether to enable Z-axis table duplication
            frameworks: List of framework codes to import
            selected_tables: Optional list of table_ids to process
        """
        self.logger.info(f"Running complete mapping (duplication={enable_table_duplication}, frameworks={frameworks}, selected_tables={len(selected_tables) if selected_tables else 'all'})")

        # Run Phase A
        phase_a_data = self.map_csvs_phase_a(frameworks=frameworks)

        # Run Phase B with optional table selection
        self.map_csvs_phase_b(
            phase_a_data=phase_a_data,
            selected_tables=selected_tables,
            enable_table_duplication=enable_table_duplication
        )

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
