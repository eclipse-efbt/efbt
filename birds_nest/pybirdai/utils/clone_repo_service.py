# Standard library imports for HTTP requests, file operations, and system utilities
import requests
import zipfile
import os
import shutil
import logging
import time

# Set up logging configuration to write to both file and console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('clone_repo_setup.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Base directory path for the birds_nest project
BASE = f"birds_nest{os.sep}"

# Mapping configuration that defines how source folders from the repository
# should be copied to target folders, with optional file filtering functions
REPO_MAPPING = {
    # Database export files with specific filtering rules
    f"export{os.sep}database_export_ldm":{
        f"resources{os.sep}admin" : (lambda file: file.startswith("auth_")),  # Only auth-related files
        f"resources{os.sep}bird" : (lambda file: file.startswith("bird_")),   # Only bird-related files
        f"resources{os.sep}technical_export" : (lambda file: True)            # All files
    },
    # Join configuration files
    "joins_configuration":{
        f"resources{os.sep}joins_configuration" : (lambda file: True),        # All files
    },
    # Initial correction files
    "initial_correction":{
        f"resources{os.sep}extra_variables" : (lambda file: True),            # All files
    },
    # Derivation files from birds_nest resources
    f"birds_nest{os.sep}resources{os.sep}derivation_files":{
        f"resources{os.sep}derivation_files" : (lambda file: True),           # All files
    },
    # LDM (Logical Data Model) files from birds_nest resources
    f"birds_nest{os.sep}resources{os.sep}ldm":{
        f"resources{os.sep}ldm" : (lambda file: True),                        # All files
    },
    # Test files from birds_nest
    f"birds_nest{os.sep}tests":{
        "tests" : (lambda file: True),                                        # All files
    }
}

class CloneRepoService:
    """
    Service class for cloning a repository and setting up files according to the mapping configuration.
    Handles downloading, extracting, organizing files, and cleanup operations.
    """
    pass

    def __init__(self):
        """
        Initialize the service by cleaning up existing target directories.
        This ensures a fresh setup by removing any previously copied files.
        """
        # Delete all target directories from REPO_MAPPING to enable clean setup
        for source_folder, target_mappings in REPO_MAPPING.items():
            for target_folder, filter_func in target_mappings.items():
                if os.path.exists(target_folder):
                    logger.info(f"Removing existing target directory: {target_folder}")
                    shutil.rmtree(target_folder)

    def clone_repo(self, base_url = "https://github.com/regcommunity/FreeBIRD/", destination_path: str = "FreeBIRD"):
        """
        Download and extract a repository from GitHub as a ZIP file.

        Args:
            base_url (str): The base URL of the GitHub repository
            destination_path (str): Local directory to extract the repository to
        """
        start_time = time.time()
        logger.info(f"Starting repository clone from {base_url} to {destination_path}")

        # Construct the ZIP download URL for the main branch
        repo_url = f"{base_url}archive/refs/heads/main.zip"
        logger.info(f"Downloading repository from {repo_url}")

        # Download the repository ZIP file
        response = requests.get(repo_url)
        os.makedirs(destination_path, exist_ok=True)

        if response.status_code == 200:
            logger.info("Repository downloaded successfully, extracting files")
            # Save the ZIP file temporarily
            with open(f"{destination_path}{os.sep}repo.zip", "wb") as f:
                f.write(response.content)

            # Extract the ZIP file contents
            with zipfile.ZipFile(f"{destination_path}{os.sep}repo.zip", "r") as zip_ref:
                zip_ref.extractall(destination_path)

            # Clean up the temporary ZIP file
            os.remove(f"{destination_path}{os.sep}repo.zip")
            logger.info("Repository extraction completed")
        else:
            # Handle download failure
            logger.error(f"Failed to clone repository: {response.status_code}")
            print(f"Failed to clone repository: {response.status_code}")

        end_time = time.time()
        logger.info(f"Clone repo completed in {end_time - start_time:.2f} seconds")

    def setup_files(self, destination_path: str = "FreeBIRD"):
        """
        Organize and copy files from the extracted repository according to REPO_MAPPING configuration.

        Args:
            destination_path (str): Path where the repository was extracted
        """
        start_time = time.time()
        logger.info(f"Starting file setup from {destination_path}")

        # Find the extracted folder (typically FreeBIRD-main)
        extracted_folder = None
        for item in os.listdir(destination_path):
            item_path = os.path.join(destination_path, item)
            if os.path.isdir(item_path) and item.startswith("FreeBIRD"):
                extracted_folder = item_path
                break

        # Validate that we found the extracted repository folder
        if not extracted_folder:
            logger.error("Could not find extracted repository folder")
            print("Could not find extracted repository folder")
            return

        logger.info(f"Found extracted folder: {extracted_folder}")

        # Process each mapping in REPO_MAPPING
        for source_folder, target_mappings in REPO_MAPPING.items():
            source_path = os.path.join(extracted_folder, source_folder)
            logger.debug(f"Processing source folder: {source_path}")

            # Skip if source folder doesn't exist
            if not os.path.exists(source_path):
                logger.warning(f"Source path does not exist: {source_path}")
                continue

            # Special handling for database export files that need filtering
            if f"export{os.sep}database_export_ldm" == source_folder:
                # Create all target directories first
                for target_folder, filter_func in target_mappings.items():
                    logger.debug(f"Creating target folder: {target_folder}")
                    os.makedirs(target_folder, exist_ok=True)
                    # Copy files that match the filter

                # Process each file in the source directory
                for file_name in os.listdir(source_path):
                    # Check which target folder this file should go to based on filter functions
                    for target_folder, filter_func in target_mappings.items():
                        if filter_func(file_name):
                            source_file = os.path.join(source_path, file_name)
                            target_file = os.path.join(target_folder, file_name)
                            logger.debug(f"Copying {source_file} to {target_file}")

                            # Handle both files and directories
                            if os.path.isfile(source_file):
                                shutil.copy2(source_file, target_file)
                                logger.debug(f"File copied: {file_name}")
                            elif os.path.isdir(source_file):
                                # Remove existing directory if it exists
                                if os.path.exists(target_file):
                                    shutil.rmtree(target_file)
                                shutil.copytree(source_file, target_file)
                                logger.debug(f"Directory copied: {file_name}")
                            break  # File matched a filter, move to next file
                continue  # Move to next source folder

            # Standard handling for other folders (copy entire directory)
            target_folder = list(REPO_MAPPING[source_folder].keys())[0]
            source_folder = f"{destination_path}{os.sep}{destination_path}-main{os.sep}{source_folder}"
            shutil.copytree(source_folder, target_folder)

        end_time = time.time()
        logger.info(f"File setup completed in {end_time - start_time:.2f} seconds")

    def remove_fetched_files(self, destination_path: str = "FreeBIRD"):
        """
        Clean up by removing the downloaded repository files after setup is complete.

        Args:
            destination_path (str): Path of the downloaded repository to remove
        """
        start_time = time.time()
        logger.info(f"Removing fetched files from {destination_path}")
        shutil.rmtree(destination_path)
        end_time = time.time()
        logger.info(f"Fetched files removed in {end_time - start_time:.2f} seconds")

def main():
    """
    Main function that orchestrates the complete repository cloning and setup process.
    Executes clone, setup, and cleanup operations in sequence.
    """
    start_time = time.time()
    logger.info("Starting CloneRepoService execution")

    # Create service instance and execute the complete workflow
    service = CloneRepoService()
    service.clone_repo()        # Download and extract repository
    service.setup_files()       # Organize files according to mapping
    service.remove_fetched_files()  # Clean up downloaded files

    end_time = time.time()
    logger.info(f"CloneRepoService execution completed in {end_time - start_time:.2f} seconds")

# Entry point for script execution
if __name__ == "__main__":
    main()
