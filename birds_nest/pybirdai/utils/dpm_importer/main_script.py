import requests
import os
import shutil
import zipfile
import subprocess

import dpm_importer as csv_importer

BASE_URL = "https://www.eba.europa.eu/sites/default/files/"
URL_TO__2_0 = BASE_URL+"2024-12/11b02b99-1486-4a54-815d-289558589773/dpm_2.0_release_4.0.zip"
URL_TO__1_0 = BASE_URL+"2024-12/330f4dba-be0d-4cdd-b0ed-b5a6b1fbc049/dpm_database_v4_0_20241218.zip"

def fetch_dpm_database(
    url_to_file : str = URL_TO__1_0
):
    content = requests.get(url_to_file)
    print(content.status_code)
    path_to_zip_file = "dpm_database.zip"
    directory_to_extract_to = "dpm_database"
    with open(path_to_zip_file,"wb") as f:
        f.write(content.content)
    with zipfile.ZipFile(path_to_zip_file, 'r') as zip_ref:
        zip_ref.extractall(directory_to_extract_to)
    db_ = [_ for _ in os.listdir(directory_to_extract_to) if ".accdb" in _].pop()
    path_of_src_file = "/".join([directory_to_extract_to,db_])
    path_of_target_file = "/".join([directory_to_extract_to,directory_to_extract_to])+".accdb"
    shutil.copyfile(path_of_src_file,path_of_target_file)

def main():
    # fetch_dpm_database()
    # subprocess.run(["bash","pybirdai/utils/dpm_importer/process.sh","dpm_database/dpm_database.accdb"])
    csv_importer.main()

if __name__ == "__main__":
    main()
