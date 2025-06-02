import requests
import json
from urllib.parse import urljoin
import os
import traceback

from requests.api import head

class GitHubFileFetcher:
    def __init__(self, base_url):
        self.base_url = base_url
        # Extract owner and repo from GitHub URL
        parts = base_url.replace('https://github.com/', '').split('/')
        self.owner = parts[0]
        self.repo = parts[1]
        self.api_base = f"https://api.github.com/repos/{self.owner}/{self.repo}"
        self.files = {}

    def get_commit_info(self, folder_path="", branch="main"):
        """
        Get commit information for files in a GitHub repository folder (wrapper function)

        Args:
            folder_path (str): Path to the folder in the repository
            branch (str): Branch name (default: main)

        Returns:
            dict: Repository structure information for files and folders
        """
        try:
            result = self._get_commit_info_recursive(folder_path, branch)
            return result
        except requests.exceptions.RequestException as e:
            if "404" in str(e) or "Not Found" in str(e):
                print(f"URL not found: {folder_path}")
                return {}
            raise e

    def _get_commit_info_recursive(self, folder_path="", branch="main"):
        """
        Recursive function to get commit information for files in a GitHub repository folder
        TODO()! fetch the sql_fixtures
        Args:
            folder_path (str): Path to the folder in the repository
            branch (str): Branch name (default: main)

        Returns:
            dict: Repository structure information for files and folders
        """
        # Construct the tree-commit-info URL
        commit_info_url = f"https://github.com/{self.owner}/{self.repo}/tree/{branch}/birds_nest/{folder_path}"

        response = requests.get(commit_info_url, headers={'Accept': 'application/json'})
        response.raise_for_status()

        payload_data = response.json()["payload"]

        # Extract tree items from the current folder
        tree_items = payload_data["tree"]["items"]

        # Extract file tree for full repository structure
        file_tree = payload_data.get("fileTree", {})

        print(f"Found {len(tree_items)} items in current folder:")

        # Process the tree items to identify folders and files
        for item in tree_items:
            item_name = item["name"]
            item_path = item["path"]
            content_type = item["contentType"]

            # Add files to the file tree structure
            if content_type == "file":
                # Extract the directory path from the item path
                dir_path = "/".join(item_path.split("/")[:-1])
                if dir_path not in file_tree:
                    file_tree[dir_path] = {"items": []}
                file_tree[dir_path]["items"].append(item)

            if content_type == "directory":
                print(f"Found folder: {item_name}")
                # Recursively get commit info for subfolder
                subfolder_path = item_path.replace(f"birds_nest/{folder_path}/", "") if folder_path else item_name
                try:
                    self._get_commit_info_recursive(subfolder_path, branch)
                except requests.exceptions.RequestException as e:
                    if "404" in str(e) or "Not Found" in str(e):
                        print(f"URL not found for subfolder: {subfolder_path}")
                        continue
                    raise e
            else:
                print(f"Found file: {item_name}")

        result = {
            "tree": tree_items,
            "fileTree": file_tree,
            "refInfo": payload_data.get("refInfo", {}),
            "repo": payload_data.get("repo", {})
        }

        # Save result in self.files
        self.files[folder_path] = result

        return result

    def fetch_files(self, folder_path="", branch="main"):
        """
        Fetch public files from a GitHub repository folder

        Args:
            folder_path (str): Path to the folder in the repository
            branch (str): Branch name (default: main)

        Returns:
            list: List of file information dictionaries
        """
        api_url = f"{self.api_base}/contents/{folder_path}"
        if branch != "main":
            api_url += f"?ref={branch}"

        try:
            response = requests.get(api_url)
            response.raise_for_status()

            files_data = response.json()
            if isinstance(files_data, list):
                return files_data
            else:
                return [files_data]  # Single file

        except requests.exceptions.RequestException as e:
            print(f"Error fetching files: {e}")
            return []

    def download_file(self, file_info, local_path=None):
        """
        Download a specific file

        Args:
            file_info (dict): File information from fetch_files()
            local_path (str): Local path to save the file (optional)

        Returns:
            str: File content or path to saved file
        """
        if file_info.get('type') != 'file':
            print(f"Skipping {file_info.get('name')}: not a file")
            return None

        download_url = file_info.get('download_url')
        if not download_url:
            print(f"No download URL for {file_info.get('name')}")
            return None

        try:
            response = requests.get(download_url)
            response.raise_for_status()

            if local_path:
                with open(local_path, 'wb') as f:
                    f.write(response.content)
                return local_path
            else:
                return response.text

        except requests.exceptions.RequestException as e:
            print(f"Error downloading file: {e}")
            return None

def fetch_directory_recursively(fetcher, folder_path, local_base_path):
    """
    Recursively fetch all files and directories from a GitHub folder
    """
    import os

    files = fetcher.fetch_files(folder_path)

    for item in files:
        name = item.get('name', 'Unknown')
        item_type = item.get('type', 'Unknown')

        if item_type == 'dir':
            # It's a directory, recurse into it
            sub_folder_path = f"{folder_path}/{name}" if folder_path else name
            local_sub_path = os.path.join(local_base_path, name)
            os.makedirs(local_sub_path, exist_ok=True)
            print(f"Created directory: {local_sub_path}")
            fetch_directory_recursively(fetcher, sub_folder_path, local_sub_path)
        elif item_type == 'file':
            # It's a file, download it
            size = item.get('size', 0)
            print(f"  - {name} (file) - {size} bytes")

            local_file_path = os.path.join(local_base_path, name)
            result = fetcher.download_file(item, local_file_path)
            if result:
                print(f"    Saved to: {local_file_path}")


def fetch_database_export_files(fetcher, base_url):
    """
    Step 1: Fetch and organize database export files

    Args:
        fetcher: GitHubFileFetcher instance
        base_url: GitHub repository URL
    """
    print("=" * 60)
    print("STEP 1: Fetching Database Export Files")
    print("=" * 60)

    folder_path = "export/database_export_ldm"
    print(f"Fetching files from {base_url}/tree/main/{folder_path}")

    files = fetcher.fetch_files(folder_path)

    if files:
        print(f"Found {len(files)} items:")

        for file_info in files:
            name = file_info.get('name', 'Unknown')
            file_type = file_info.get('type', 'Unknown')
            size = file_info.get('size', 0)

            # Categorize files based on their names
            if "bird" in name:
                folder = "bird"
                print(f"  - {name} ({file_type}) - {size} bytes -> bird")
            elif "auth" in name:
                folder = "admin"
                print(f"  - {name} ({file_type}) - {size} bytes -> admin")
            else:
                folder = "technical_export"
                print(f"  - {name} ({file_type}) - {size} bytes -> technical export")

            # Create directory structure and save file
            save_path = os.path.join("resources", folder)
            os.makedirs(save_path, exist_ok=True)

            local_file_path = os.path.join(save_path, name)
            result = fetcher.download_file(file_info, local_file_path)
            if result:
                print(f"    Saved to: {local_file_path}")
    else:
        print("No files found or error occurred")

def fetch_test_fixtures(fetcher, base_url):
    """
    Step 2: Fetch test fixtures and templates

    Args:
        fetcher: GitHubFileFetcher instance
        base_url: GitHub repository URL
    """
    print("\n" + "=" * 60)
    print("STEP 2: Fetching Test Fixtures")
    print("=" * 60)

    templates_folder_path = "tests/fixtures/templates/"
    print(f"Fetching files from {base_url}/tree/main/{templates_folder_path}")

    # Set up local destination path
    local_templates_path = os.path.join("pybirdai", "tests", "fixtures", "templates")
    os.makedirs(local_templates_path, exist_ok=True)

    fetcher.get_commit_info(templates_folder_path)
    # Only fetch files from fetcher.files
    if fetcher.files:
        for folder_path, folder_data in fetcher.files.items():
            # print(f"\nProcessing folder: {folder_path}")
            tree_items = folder_data.get('fileTree', [])

            for path_dir, item_data in tree_items.items():
                for item in item_data["items"]:
                    if item['contentType'] == 'file' and "pybirdai/tests/" in os.path.join("pybirdai", item['path'].replace('birds_nest/', '')):
                        file_name = item["name"]
                        file_path = item['path']
                        # Create the local file path
                        relative_path = file_path.replace('birds_nest/', '')
                        local_file_path = os.path.join("pybirdai", relative_path)

                        # Create directories if they don't exist
                        os.makedirs(os.path.dirname(local_file_path), exist_ok=True)

                        # print(f"  Found file: {file_name} -> {local_file_path}")

                        # Use the GitHub raw content URL to download the file
                        raw_url = f"https://raw.githubusercontent.com/{fetcher.owner}/{fetcher.repo}/main/{file_path}"

                        response = requests.get(raw_url)
                        response.raise_for_status()

                        if local_file_path.endswith('.py') or local_file_path.endswith('.sql'):
                            with open(local_file_path, 'wb') as f:
                                print("I wrote file; ", local_file_path, raw_url)
                                f.write(response.content)

    # Recursively fetch all files and directories
    # fetch_directory_recursively(fetcher, templates_folder_path, local_templates_path)


def main():
    """
    Main function to orchestrate the file fetching process
    """
    base_url = "https://github.com/regcommunity/FreeBIRD"

    # Initialize the GitHub file fetcher
    fetcher = GitHubFileFetcher(base_url)

    # Step 1: Fetch database export files
    # fetch_database_export_files(fetcher, base_url)

    # Step 2: Fetch test fixtures
    fetch_test_fixtures(fetcher, base_url)

    print("\n" + "=" * 60)
    print("File fetching process completed!")
    print("=" * 60)


if __name__ == "__main__":
    main()
