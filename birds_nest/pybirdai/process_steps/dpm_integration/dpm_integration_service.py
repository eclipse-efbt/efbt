import requests
from bs4 import BeautifulSoup
import os
import zipfile
import shutil
import platform

FILES_ROOT = "https://www.eba.europa.eu/sites/default/files"
DEFAULT_DB_VERSION = f"{FILES_ROOT}/2024-12/330f4dba-be0d-4cdd-b0ed-b5a6b1fbc049/dpm_database_v4_0_20241218.zip"
DEFAULT_DB_LOCAL_PATH = "dpm_database.zip"
SCRIPT_RUNNER = "" if platform.system() == "Windows" else "bash "
PROCESS_FILE_END = ".bat" if platform.system() == "Windows" else ".sh"
SCRIPT_PATH = f"pybirdai{os.sep}process_steps{os.sep}dpm_integration{os.sep}process{PROCESS_FILE_END}"
EXTRACTED_DB_PATH = f"dpm_database{os.sep}dpm_database.accdb"

class DPMImporterService:

    def __init__(self):
        self.link_db = None

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

    def run_application(self,extract_cleanup:bool=False,download_cleanup:bool=False):
        # if not os.path.exists(DEFAULT_DB_LOCAL_PATH):
        #     self.fetch_link_for_database_download()
        #     self.download_dpm_database()
        # self.extract_dpm_database()
        self.map_csvs_to_sdd_exchange_format()

        if extract_cleanup:
            shutil.rmtree("dpm_database")
        if download_cleanup:
            os.remove("dpm_database.zip")



    def map_csvs_to_sdd_exchange_format(self):
        from . import mapping_functions as new_maps
        """
        Core Package
        """
        _, framework_map = new_maps.map_frameworks() # frameworks
        _.to_csv("export_debug/mapped/framework.csv",index=False)
        _, domain_map = new_maps.map_domains() # domains
        _.to_csv("export_debug/mapped/domain.csv",index=False)
        _, member_map = new_maps.map_members(domain_id_map=domain_map) # members
        _.to_csv("export_debug/mapped/member.csv",index=False)
        _, dimension_map = new_maps.map_dimensions(domain_id_map=domain_map) # to enumerated variables
        _.to_csv("export_debug/mapped/variable.csv",index=False)
        _, hierarchy_map = new_maps.map_hierarchy(domain_id_map=domain_map) # member hierarchies
        _.to_csv("export_debug/mapped/member_hierarchy.csv",index=False)
        _, hierarchy_node_map = new_maps.map_hierarchy_node(hierarchy_map=hierarchy_map, member_map=member_map) # member hierarchy node
        _.to_csv("export_debug/mapped/member_hierarchy_node.csv",index=False)

        """
        Data Definition Package
        """

        context_data, context_map = new_maps.map_context_definition(dimension_map=dimension_map,member_map=member_map) # to combination_items (need to improve EBA_ATY and subdomain generation)
        # _, metric_map = new_maps.map_metrics()
        (combination, combination_item), dpv_map = new_maps.map_datapoint_version(context_map=context_map,context_data=context_data,dimension_map=dimension_map,member_map=member_map) # to combinations and items
        combination.to_csv("export_debug/mapped/combination.csv",index=False)
        combination_item.to_csv("export_debug/mapped/combination_item.csv",index=False)

        """
        Rendering Package
        """
        _, table_map = new_maps.map_tables(framework_id_map=framework_map)
        _.to_csv("export_debug/mapped/table.csv",index=False)
        _, axis_map = new_maps.map_axis(table_map=table_map)
        _.to_csv("export_debug/mapped/axis.csv",index=False)
        _, ordinate_map = new_maps.map_axis_ordinate(axis_map=axis_map)
        _.to_csv("export_debug/mapped/axis_ordinate.csv",index=False)
        _, cell_map = new_maps.map_table_cell(table_map=table_map,dp_map=dpv_map)
        _.to_csv("export_debug/mapped/table_cell.csv",index=False)
        _, cell_position_map = new_maps.map_cell_position(cell_map=cell_map,ordinate_map=ordinate_map)
        _.to_csv("export_debug/mapped/cell_position.csv",index=False)
        _, ordinate_item_map = new_maps.map_ordinate_categorisation(member_map=member_map,dimension_map=dimension_map,ordinate_map=ordinate_map,hierarchy_map=hierarchy_map)
        _.to_csv("export_debug/mapped/ordinate_item.csv",index=False)

if __name__ == "__main__":
    DPMImporterService().run_application()
