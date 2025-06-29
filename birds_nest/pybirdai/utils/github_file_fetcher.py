import requests
import os
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('github_fetcher.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class GitHubFileFetcher:
    def __init__(self, base_url):
        """
        Initialize GitHub file fetcher with repository URL.

        Args:
            base_url (str): GitHub repository URL (e.g., https://github.com/owner/repo)
        """
        logger.info(f"Initializing GitHubFileFetcher with URL: {base_url}")

        self.base_url = base_url
        # Parse the GitHub URL to extract owner and repository name
        parts = base_url.replace('https://github.com/', '').split('/')
        self.owner = parts[0]
        self.repo = parts[1]
        # Construct the GitHub API base URL
        self.api_base = f"https://api.github.com/repos/{self.owner}/{self.repo}"
        # Dictionary to cache file information
        self.files = {}

        logger.info(f"Configured for repository: {self.owner}/{self.repo}")

    def _handle_request_error(self, error, context):
        """
        Common error handling for HTTP requests.

        Args:
            error (Exception): The exception that occurred
            context (str): Context description for logging

        Returns:
            bool: True if error was a 404 (not found), False for other errors
        """
        if "404" in str(error) or "Not Found" in str(error):
            logger.warning(f"URL not found: {context}")
            print(f"URL not found: {context}")
            return True
        logger.error(f"Request exception for {context}: {error}")
        return False

    def _ensure_directory_exists(self, path):
        """
        Ensure a directory exists, creating it if necessary.

        Args:
            path (str): Directory path to create
        """
        os.makedirs(path, exist_ok=True)

    def _construct_raw_url(self, file_path, branch="main"):
        """
        Construct a raw GitHub URL for direct file download.

        Args:
            file_path (str): Path to the file in the repository
            branch (str): Git branch name

        Returns:
            str: Raw GitHub URL
        """
        return f"https://raw.githubusercontent.com/{self.owner}/{self.repo}/{branch}/{file_path}"

    def _download_from_raw_url(self, raw_url, local_path):
        """
        Download a file from a raw GitHub URL.

        Args:
            raw_url (str): Raw GitHub URL
            local_path (str): Local path to save the file

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            response = requests.get(raw_url)
            response.raise_for_status()

            with open(local_path, 'wb') as f:
                f.write(response.content)

            # logger.info(f"Successfully downloaded file to: {local_path}")
            return True
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to download from {raw_url}: {e}")
            return False

    def get_commit_info(self, folder_path="", branch="main"):
        """
        Get commit information for files in a GitHub repository folder.

        Args:
            folder_path (str): Path to the folder within the repository
            branch (str): Git branch name (default: main)

        Returns:
            dict: Commit information or empty dict if not found
        """
        logger.info(f"Getting commit info for folder: {folder_path} on branch: {branch}")

        try:
            result = self._get_commit_info_recursive(folder_path, branch)
            logger.info(f"Successfully retrieved commit info for {folder_path}")
            return result
        except requests.exceptions.RequestException as e:
            if self._handle_request_error(e, folder_path):
                return {}
            raise e

    def _get_commit_info_recursive(self, folder_path="", branch="main"):
        """
        Recursive function to get commit information for files in a GitHub repository folder.

        Args:
            folder_path (str): Path to the folder within the repository
            branch (str): Git branch name

        Returns:
            dict: Tree structure with commit information
        """
        # Construct the GitHub tree URL for the specific folder
        commit_info_url = f"https://github.com/{self.owner}/{self.repo}/tree/{branch}/birds_nest/{folder_path}"
        logger.debug(f"Fetching commit info from URL: {commit_info_url}")

        # Make HTTP request to GitHub with JSON headers
        response = requests.get(commit_info_url, headers={'Accept': 'application/json'})
        response.raise_for_status()

        # Parse the response JSON to extract payload data
        payload_data = response.json()["payload"]
        tree_items = payload_data["tree"]["items"]
        file_tree = payload_data.get("fileTree", {})

        logger.debug(f"Processing {len(tree_items)} items in {folder_path}")

        # Process each item in the tree (files and directories)
        for item in tree_items:
            item_path = item["path"]
            content_type = item["contentType"]

            if content_type == "file":
                # Organize files into directory structure
                dir_path = "/".join(item_path.split("/")[:-1])
                if dir_path not in file_tree:
                    file_tree[dir_path] = {"items": []}
                file_tree[dir_path]["items"].append(item)
                logger.debug(f"Added file: {item_path}")

            elif content_type == "directory":
                # Recursively process subdirectories
                subfolder_path = item_path.replace("birds_nest", ".") if folder_path else item["name"]
                logger.debug(f"Processing subdirectory: {subfolder_path}")

                try:
                    self._get_commit_info_recursive(subfolder_path, branch)
                except requests.exceptions.RequestException as e:
                    if self._handle_request_error(e, f"subfolder: {subfolder_path}"):
                        continue
                    raise e

        # Construct result structure
        result = {
            "tree": tree_items,
            "fileTree": file_tree,
            "refInfo": payload_data.get("refInfo", {}),
            "repo": payload_data.get("repo", {})
        }

        # Cache the result for this folder path
        self.files[folder_path] = result
        logger.info(f"Cached commit info for folder: {folder_path}")
        return result

    def fetch_files(self, folder_path="", branch="main"):
        """
        Fetch public files from a GitHub repository folder using the GitHub API.

        Args:
            folder_path (str): Path to the folder within the repository
            branch (str): Git branch name (default: main)

        Returns:
            list: List of file information dictionaries
        """
        # Construct GitHub API URL for contents
        api_url = f"{self.api_base}/contents/{folder_path}"
        if branch != "main":
            api_url += f"?ref={branch}"

        logger.info(f"Fetching files from API: {api_url}")

        try:
            response = requests.get(api_url)
            response.raise_for_status()
            files_data = response.json()

            # Ensure we return a list (single files are returned as dict)
            result = files_data if isinstance(files_data, list) else [files_data]
            logger.info(f"Successfully fetched {len(result)} items from {folder_path}")
            return result
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching files from {api_url}: {e}")
            print(f"Error fetching files: {e}")
            return []

    def download_file(self, file_info, local_path=None):
        """
        Download a specific file from GitHub.

        Args:
            file_info (dict): File information dictionary from GitHub API
            local_path (str, optional): Local path to save the file

        Returns:
            str or None: Local file path if saved, file content if not saved, None on error
        """
        # Validate that this is actually a file
        if file_info.get('type') != 'file':
            logger.warning(f"Attempted to download non-file item: {file_info.get('name', 'Unknown')}")
            return None

        # Get the download URL from file info
        download_url = file_info.get('download_url')
        if not download_url:
            logger.warning(f"No download URL found for file: {file_info.get('name', 'Unknown')}")
            return None

        file_name = file_info.get('name', 'Unknown')
        logger.info(f"Downloading file: {file_name}")

        try:
            # Download the file content
            response = requests.get(download_url)
            response.raise_for_status()

            if local_path:
                # Save to local file system
                logger.debug(f"Saving file to: {local_path}")
                with open(local_path, 'wb') as f:
                    f.write(response.content)
                logger.info(f"Successfully saved file: {local_path}")
                return local_path
            else:
                # Return file content as text
                logger.debug(f"Returning file content for: {file_name}")
                return response.text
        except requests.exceptions.RequestException as e:
            logger.error(f"Error downloading file {file_name}: {e}")
            print(f"Error downloading file: {e}")
            return None

    def fetch_directory_recursively(self, folder_path, local_base_path):
        """
        Recursively fetch all files and directories from a GitHub folder.

        Args:
            folder_path (str): Remote folder path in the repository
            local_base_path (str): Local base path to save files
        """
        logger.info(f"Recursively fetching directory: {folder_path} to {local_base_path}")

        # Get list of files and directories in the current folder
        files = self.fetch_files(folder_path)

        for item in files:
            name = item.get('name', 'Unknown')
            item_type = item.get('type', 'Unknown')

            if item_type == 'dir':
                # Process subdirectory recursively
                sub_folder_path = f"{folder_path}/{name}" if folder_path else name
                local_sub_path = os.path.join(local_base_path, name)

                logger.debug(f"Creating directory: {local_sub_path}")
                self._ensure_directory_exists(local_sub_path)

                # Recursive call for subdirectory
                self.fetch_directory_recursively(sub_folder_path, local_sub_path)
            elif item_type == 'file':
                # Download individual file
                local_file_path = os.path.join(local_base_path, name)
                logger.debug(f"Downloading file: {name} to {local_file_path}")
                self.download_file(item, local_file_path)

    def fetch_database_export_files(self):
        """
        Fetch and organize database export files into categorized folders.
        """
        # Fetch files from the database export directory
        files = self.fetch_files("export/database_export_ldm")
        logger.info(f"Found {len(files)} database export files")

        for file_info in files:
            name = file_info.get('name', 'Unknown')
            logger.debug(f"Processing database export file: {name}")

            # Categorize files based on their names
            if "bird" in name:
                folder = "bird"
            elif "auth" in name:
                folder = "admin"
            else:
                folder = "technical_export"

            logger.debug(f"Categorized {name} into folder: {folder}")

            # Create the target directory structure
            save_path = os.path.join("resources", folder)
            self._ensure_directory_exists(save_path)

            # Download the file to the appropriate category folder
            local_file_path = os.path.join(save_path, name)
            result = self.download_file(file_info, local_file_path)

            if result:
                logger.info(f"Successfully saved database export file: {local_file_path}")
            else:
                logger.warning(f"Failed to download database export file: {name}")

    def fetch_test_fixture(self, folder_data, path_downloaded):
        file_tree = folder_data.get('fileTree', {})
        logger.debug(f"Processing file tree with {len(file_tree)} directories")
        it_ = sum(list(map(lambda item_data : item_data.get("items", []), file_tree.values())), [])
        success = False

        for item in it_:
            right_content_type = item['contentType'] == 'file'
            right_path = f"pybirdai{os.sep}tests" in os.path.join("pybirdai", item['path'].replace(f'birds_nest{os.sep}', ''))

            if not (right_content_type and right_path):
                continue

            file_path = item['path']
            relative_path = file_path.replace(f'birds_nest/', '').replace(f'birds_nest{os.sep}', '')
            local_file_path = relative_path

            if local_file_path in path_downloaded:
                continue

            logger.debug(f"Processing test fixture file: {file_path}")

            # Create directory structure if it doesn't exist
            self._ensure_directory_exists(os.path.dirname(local_file_path))

            # Only download Python and SQL files
            if not local_file_path.endswith(('.py', '.sql', ".json")):
                continue

            logger.debug(f"Downloading test fixture: {relative_path}")

            # Use the reusable download method
            raw_url = self._construct_raw_url(file_path)
            success = self._download_from_raw_url(raw_url, local_file_path)

            if success:
                path_downloaded.add(local_file_path)
                logger.info(f"Successfully downloaded test fixture: {local_file_path}")
            else:
                logger.error(f"Failed to download test fixture {file_path}")

    def fetch_test_fixtures(self, base_url:str=""):
        """
        Fetch test fixtures and templates from the repository.

        Args:
            base_url (str): Base URL (currently unused but kept for compatibility)
        """
        # Get commit information for the test fixtures directory
        self.get_commit_info("tests/fixtures/templates/")
        logger.info("Retrieved commit info for test fixtures")
        os.makedirs(f"pybirdai{os.sep}tests{os.sep}fixtures{os.sep}templates{os.sep}", exist_ok=True)

        # Process all cached file information
        path_downloaded = set()
        for folder_data in self.files.values():
            self.fetch_test_fixture(folder_data, path_downloaded)

    def fetch_derivation_model_file(
            self,
            remote_dir = "birds_nest/pybirdai",
            remote_file_name = "bird_data_model.py",
            local_target_dir = f"resources{os.sep}derivation_implementation",
            local_target_file_name = "bird_data_model_with_derivation.py"
    ):
        """Fetches the derivation model file from the specified remote directory"""
        # Fetch contents of the directory containing the target file
        files_in_dir = self.fetch_files(remote_dir)
        found_file_info = None
        for item in files_in_dir:
            if item.get('name') == remote_file_name and item.get('type') == 'file':
                found_file_info = item
                break

        if found_file_info:
            # Construct the local path
            local_path = os.path.join(local_target_dir, local_target_file_name)
            # Ensure the local directory exists
            self._ensure_directory_exists(local_target_dir)
            logger.info(f"Attempting to download {remote_file_name} to {local_path}")
            # Download the file
            download_success = self.download_file(found_file_info, local_path)
            if download_success:
                logger.info(f"Successfully downloaded {remote_file_name} to {local_path}")
            else:
                logger.error(f"Failed to download {remote_file_name}")
        else:
            logger.warning(f"File {remote_file_name} not found in {remote_dir}")
            print(f"File {remote_file_name} not found in {remote_dir}")

    def fetch_filter_code(
            self,
            remote_dir = "birds_nest/pybirdai",
            local_target_dir = f"birds_nest{os.sep}pybirdai{os.sep}process_steps{os.sep}filter_code"):
        """Fetches the derivation model file from the specified remote directory"""

        files_in_dir = self.fetch_files(remote_dir)
        for remote_file_name in files_in_dir:
            local_file_path = os.path.join(local_target_dir, remote_file_name)
            # Ensure the local directory exists
            self._ensure_directory_exists(local_target_dir)
            logger.info(f"Attempting to download {remote_file_name} to {local_file_path}")
            # Download the file
            download_success = self.download_file(remote_file_name, local_file_path)
            if download_success:
                logger.info(f"Successfully downloaded {remote_file_name} to {local_file_path}")
            else:
                logger.error(f"Failed to download {remote_file_name}")

        return 0

def main():
    """Main function to orchestrate the file fetching process"""
    logger.info("Starting GitHub file fetching process")

    # Initialize the fetcher with the FreeBIRD repository
    fetcher = GitHubFileFetcher("https://github.com/regcommunity/FreeBIRD")


    logger.info("STEP 1: Fetching specific derivation model file")

    fetcher.fetch_derivation_model_file(
        "birds_nest/pybirdai",
        "bird_data_model.py",
        f"resources{os.sep}derivation_implementation",
        "bird_data_model_with_derivation.py"
    )


    logger.info("STEP 2: Fetching database export files")
    fetcher.fetch_database_export_files()


    logger.info("STEP 3: Fetching test fixtures and templates")
    fetcher.fetch_test_fixtures()

    logger.info("STEP 4: Fetching test templates")
    fetcher.fetch_filter_code()

    logger.info("File fetching process completed successfully!")
    print("File fetching process completed!")

if __name__ == "__main__":
    main()
