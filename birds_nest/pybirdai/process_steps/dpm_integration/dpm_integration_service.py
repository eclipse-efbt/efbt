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
        self.link_db = None
        self.output_directory = f"{output_directory}{os.sep}technical_export{os.sep}"
        shutil.rmtree(self.output_directory, ignore_errors=True)
        os.makedirs(self.output_directory, exist_ok=True)

    def fetch_link_for_database_download(self):
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
        if not self.link_db:
            self.link_db = DEFAULT_DB_VERSION

    def download_dpm_database(self):
        with open("dpm_database.zip","wb") as f:
            db_data = requests.get(self.link_db).content
            f.write(db_data)

    def extract_dpm_database(self):
        with zipfile.ZipFile("dpm_database.zip", 'r') as zip_ref:
            zip_ref.extractall("dpm_database")

            if len(os.listdir("dpm_database")) > 1:
                raise Exception("More than one file in the zip")

            for file in os.listdir("dpm_database"):
                if file.endswith(".accdb"):
                    shutil.move(os.path.join("dpm_database", file), os.path.join("dpm_database", "dpm_database.accdb"))

        os.system(f"{SCRIPT_RUNNER}{SCRIPT_PATH} {EXTRACTED_DB_PATH}")

    def run_application(self,extract_cleanup:bool=False,download_cleanup:bool=False,with_extract:bool=True):
        if with_extract:
            shutil.rmtree("dpm_database")

        if with_extract:
            if not os.path.exists(DEFAULT_DB_LOCAL_PATH):
                self.fetch_link_for_database_download()
                self.download_dpm_database()
            if not (os.path.exists("target") and os.listdir("target")):
                self.extract_dpm_database()
        logging.info("Starting Mapping")
        self.map_csvs_to_sdd_exchange_format()

        if extract_cleanup:
            shutil.rmtree("dpm_database")
        if download_cleanup:
            shutil.rmtree("target")
            os.remove("dpm_database.zip")

    def write_csv_maintenance_agency(self):
        """
        Core Package
        """

        with open(f"{self.output_directory}maintenance_agency.csv","w") as f:
            f.write("""MAINTENANCE_AGENCY_ID,CODE,NAME,DESCRIPTION
EBA,EBA,European Banking Authority,European Banking Authority""")

    def map_csvs_to_sdd_exchange_format(self):
        import pybirdai.process_steps.dpm_integration.mapping_functions as new_maps
        """
        Core Package
        """

        self.write_csv_maintenance_agency()
        logging.info("Created Maintenance Agency File")
        _, framework_map = new_maps.map_frameworks() # frameworks
        _.to_csv(f"{self.output_directory}framework.csv",index=False)
        logging.info("Mapped Framework Entities")
        _, domain_map = new_maps.map_domains() # domains
        _.to_csv(f"{self.output_directory}domain.csv",index=False)
        logging.info("Mapped Domain Entities")
        _, member_map = new_maps.map_members(domain_id_map=domain_map) # members
        _.to_csv(f"{self.output_directory}member.csv",index=False)
        logging.info("Mapped Members Entities")
        _, dimension_map = new_maps.map_dimensions(domain_id_map=domain_map) # to enumerated variables
        _.to_csv(f"{self.output_directory}variable.csv",index=False)
        logging.info("Mapped Variables Entities")
        _, hierarchy_map = new_maps.map_hierarchy(domain_id_map=domain_map) # member hierarchies
        _.to_csv(f"{self.output_directory}member_hierarchy.csv",index=False)
        logging.info("Mapped Hierarchy Entities")
        _, hierarchy_node_map = new_maps.map_hierarchy_node(hierarchy_map=hierarchy_map, member_map=member_map) # member hierarchy node
        _.to_csv(f"{self.output_directory}member_hierarchy_node.csv",index=False)
        logging.info("Mapped HierarchyNode Entities")

        """
        Data Definition Package
        """

        context_data, context_map = new_maps.map_context_definition(dimension_map=dimension_map,member_map=member_map) # to combination_items (need to improve EBA_ATY and subdomain generation)
        
        (combination, combination_item), dpv_map = new_maps.map_datapoint_version(context_map=context_map,context_data=context_data,dimension_map=dimension_map,member_map=member_map) # to combinations and items
        combination.to_csv(f"{self.output_directory}combination.csv",index=False)
        combination_item.to_csv(f"{self.output_directory}combination_item.csv",index=False)
        logging.info("Mapped Combination(and Items) Entities")

        """
        Rendering Package
        """
        _, table_map = new_maps.map_tables(framework_id_map=framework_map)
        _.to_csv(f"{self.output_directory}table.csv",index=False)
        logging.info("Mapped Table Entities")
        _, axis_map = new_maps.map_axis(table_map=table_map)
        _.to_csv(f"{self.output_directory}axis.csv",index=False)
        logging.info("Mapped Table Entities")
        _, ordinate_map = new_maps.map_axis_ordinate(axis_map=axis_map)
        _.to_csv(f"{self.output_directory}axis_ordinate.csv",index=False)
        logging.info("Mapped AxisOrdinates Entities")
        _, cell_map = new_maps.map_table_cell(table_map=table_map,dp_map=dpv_map)
        _.to_csv(f"{self.output_directory}table_cell.csv",index=False)
        logging.info("Mapped TableCell Entities")
        _, cell_position_map = new_maps.map_cell_position(cell_map=cell_map,ordinate_map=ordinate_map)
        _.to_csv(f"{self.output_directory}cell_position.csv",index=False)
        logging.info("Mapped CellPositions Entities")
        _, ordinate_item_map = new_maps.map_ordinate_categorisation(member_map=member_map,dimension_map=dimension_map,ordinate_map=ordinate_map,hierarchy_map=hierarchy_map)
        _.to_csv(f"{self.output_directory}ordinate_item.csv",index=False)
        logging.info("Mapped OrdinateItems Entities")

if __name__ == "__main__":
    DPMImporterService().run_application()
