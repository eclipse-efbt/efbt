# coding=UTF-8
# Copyright (c) 2024 Bird Software Solutions Ltd
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDX-License-Identifier: EPL-2.0
#
# Contributors:
#    Neil Mackenzie - initial API and implementation

import os
import logging
import requests
from django.core.exceptions import ValidationError
from .bird_meta_data_model import AutomodeConfiguration
from .utils.github_file_fetcher import GitHubFileFetcher
from .utils.bird_ecb_website_fetcher import BirdEcbWebsiteClient
from .context.context import Context

logger = logging.getLogger(__name__)


class ConfigurableGitHubFileFetcher(GitHubFileFetcher):
    """Enhanced GitHub file fetcher that supports configurable repositories and specific file types."""
    
    def __init__(self, repository_url: str, token: str = None):
        """Initialize with a configurable repository URL and optional token."""
        # Normalize the URL - remove .git suffix if present
        normalized_url = repository_url.rstrip('/')
        if normalized_url.endswith('.git'):
            normalized_url = normalized_url[:-4]
        
        super().__init__(normalized_url)
        self.token = token
        
    def _get_authenticated_headers(self):
        """Get headers with authentication if token is provided."""
        headers = {}
        if self.token:
            headers['Authorization'] = f'token {self.token}'
        return headers
    
    def _ensure_directory_exists(self, path):
        """Ensure a directory exists, creating it if necessary."""
        os.makedirs(path, exist_ok=True)
        logger.debug(f"Ensured directory exists: {path}")
    
    def _clear_directory(self, path):
        """Clear all files from a directory if it exists."""
        if os.path.exists(path):
            import shutil
            for filename in os.listdir(path):
                file_path = os.path.join(path, filename)
                try:
                    if os.path.isfile(file_path) or os.path.islink(file_path):
                        os.unlink(file_path)
                        logger.debug(f"Deleted file: {file_path}")
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)
                        logger.debug(f"Deleted directory: {file_path}")
                except Exception as e:
                    logger.error(f"Failed to delete {file_path}: {e}")
            logger.info(f"Cleared directory: {path}")
        else:
            logger.debug(f"Directory does not exist, skipping clear: {path}")
    
    def fetch_files(self, folder_path="", branch="main"):
        """Override to add authentication for private repositories."""
        # Construct GitHub API URL for contents
        api_url = f"{self.api_base}/contents/{folder_path}"
        if branch != "main":
            api_url += f"?ref={branch}"

        logger.info(f"Fetching files from API: {api_url}")

        try:
            headers = self._get_authenticated_headers()
            response = requests.get(api_url, headers=headers)
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
        """Override to add authentication for private repositories."""
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
        logger.info(f"Downloading file: {file_name} from {download_url}")

        try:
            # Download the file content with authentication headers
            headers = self._get_authenticated_headers()
            response = requests.get(download_url, headers=headers)
            response.raise_for_status()

            if local_path:
                # Ensure directory exists
                os.makedirs(os.path.dirname(local_path), exist_ok=True)
                
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
        
    def explore_repository_structure(self, max_depth=2):
        """
        Explore the repository structure to find CSV files and directories.
        
        Args:
            max_depth (int): Maximum depth to explore
            
        Returns:
            dict: Directory structure with CSV file locations
        """
        structure = {}
        
        def explore_directory(path="", depth=0):
            if depth > max_depth:
                return
            
            try:
                files = self.fetch_files(path)
                csv_files = []
                subdirs = []
                
                for item in files:
                    if item.get('type') == 'file' and item.get('name', '').endswith('.csv'):
                        csv_files.append(item)
                    elif item.get('type') == 'dir':
                        subdirs.append(item.get('name'))
                
                if csv_files:
                    structure[path or 'root'] = {
                        'csv_files': csv_files,
                        'count': len(csv_files)
                    }
                    logger.info(f"Found {len(csv_files)} CSV files in /{path}")
                
                # Explore subdirectories
                for subdir in subdirs:
                    subpath = f"{path}/{subdir}" if path else subdir
                    explore_directory(subpath, depth + 1)
                    
            except Exception as e:
                logger.debug(f"Could not explore path /{path}: {e}")
        
        logger.info("Exploring repository structure for CSV files...")
        explore_directory()
        return structure

    def fetch_technical_exports(self, target_directory: str, force_refresh: bool = False):
        """
        Fetch technical export files from the export/database_export_ldm directory.
        Files starting with 'bird_' go to bird/ subdirectory.
        Files starting with 'admin_' go to admin/ subdirectory.
        All other CSV files are considered technical export files.
        
        Args:
            target_directory (str): Local directory to save files
            force_refresh (bool): Force re-download even if files exist
        """
        logger.info(f"Fetching technical export files from export/database_export_ldm to {target_directory}")
        print(f"DEBUG: Fetching technical export files to {target_directory}")
        
        # Clear directories if force refresh is requested
        if force_refresh:
            print("DEBUG: Force refresh enabled - clearing existing files")
            bird_dir = os.path.join(target_directory, '..', 'bird')
            admin_dir = os.path.join(target_directory, '..', 'admin')
            
            self._clear_directory(target_directory)
            self._clear_directory(bird_dir)
            self._clear_directory(admin_dir)
            logger.info("Cleared existing technical export files due to force refresh")
        
        # Ensure target directory exists
        self._ensure_directory_exists(target_directory)
        
        # Look specifically in the export/database_export_ldm directory
        export_path = "export/database_export_ldm"
        files_downloaded = 0
        
        try:
            print(f"DEBUG: Fetching files from {export_path}")
            files = self.fetch_files(export_path)
            print(f"DEBUG: Raw files response: {len(files)} items")
            
            csv_files = [f for f in files if f.get('type') == 'file' and f.get('name', '').endswith('.csv')]
            print(f"DEBUG: Found {len(csv_files)} CSV files")
            
            if not csv_files:
                logger.warning(f"No CSV files found in {export_path}")
                print(f"DEBUG: No CSV files found. All files: {[f.get('name', 'Unknown') for f in files]}")
                return 0
            
            logger.info(f"Found {len(csv_files)} CSV files in {export_path}")
            print(f"DEBUG: CSV files found: {[f.get('name', 'Unknown') for f in csv_files]}")
            
            # Categorize files based on naming patterns
            bird_files = []
            admin_files = []
            technical_export_files = []
            
            for file_info in csv_files:
                file_name = file_info.get('name', '')
                print(f"DEBUG: Categorizing file: {file_name}")
                if file_name.lower().startswith('bird_'):
                    bird_files.append(file_info)
                    print(f"DEBUG: -> bird_ file")
                elif file_name.lower().startswith('auth_'):
                    admin_files.append(file_info)
                    print(f"DEBUG: -> admin_ file")
                else:
                    technical_export_files.append(file_info)
                    print(f"DEBUG: -> technical export file")
            
            logger.info(f"Categorized files: {len(bird_files)} bird_ files, {len(admin_files)} admin_ files, {len(technical_export_files)} technical export files")
            print(f"DEBUG: Categories: bird_={len(bird_files)}, admin_={len(admin_files)}, technical_export={len(technical_export_files)}")
            
            # Download bird_ files to bird/ subdirectory
            if bird_files:
                bird_dir = os.path.join(target_directory, '..', 'bird')
                print(f"DEBUG: Bird directory: {bird_dir}")
                self._ensure_directory_exists(bird_dir)
                
                for file_info in bird_files:
                    file_name = file_info.get('name')
                    local_path = os.path.join(bird_dir, file_name)
                    print(f"DEBUG: Downloading bird file {file_name} to {local_path}")
                    
                    if os.path.exists(local_path) and not force_refresh:
                        logger.debug(f"Skipping existing bird file: {file_name}")
                        continue
                        
                    result = self.download_file(file_info, local_path)
                    print(f"DEBUG: Download result for {file_name}: {result}")
                    if result:
                        files_downloaded += 1
                        logger.info(f"Downloaded bird file: {file_name} to bird/")
            
            # Download admin_ files to admin/ subdirectory  
            if admin_files:
                admin_dir = os.path.join(target_directory, '..', 'admin')
                print(f"DEBUG: Admin directory: {admin_dir}")
                self._ensure_directory_exists(admin_dir)
                
                for file_info in admin_files:
                    file_name = file_info.get('name')
                    local_path = os.path.join(admin_dir, file_name)
                    print(f"DEBUG: Downloading admin file {file_name} to {local_path}")
                    
                    if os.path.exists(local_path) and not force_refresh:
                        logger.debug(f"Skipping existing admin file: {file_name}")
                        continue
                        
                    result = self.download_file(file_info, local_path)
                    print(f"DEBUG: Download result for {file_name}: {result}")
                    if result:
                        files_downloaded += 1
                        logger.info(f"Downloaded admin file: {file_name} to admin/")
            
            # Download technical export files to main technical_export directory
            for file_info in technical_export_files:
                file_name = file_info.get('name')
                local_path = os.path.join(target_directory, file_name)
                print(f"DEBUG: Downloading technical export file {file_name} to {local_path}")
                
                if os.path.exists(local_path) and not force_refresh:
                    logger.debug(f"Skipping existing technical export file: {file_name}")
                    continue
                    
                result = self.download_file(file_info, local_path)
                print(f"DEBUG: Download result for {file_name}: {result}")
                if result:
                    files_downloaded += 1
                    logger.info(f"Downloaded technical export file: {file_name}")
            
        except Exception as e:
            logger.error(f"Error accessing {export_path}: {e}")
            print(f"DEBUG: Exception occurred: {e}")
            import traceback
            traceback.print_exc()
            return 0
        
        if files_downloaded == 0:
            logger.warning("No new files downloaded from export/database_export_ldm")
            print("DEBUG: No files were downloaded")
        else:
            logger.info(f"Successfully downloaded {files_downloaded} files from export/database_export_ldm")
            print(f"DEBUG: Successfully downloaded {files_downloaded} files")
        
        return files_downloaded
    
    def fetch_configuration_files(self, target_base_directory: str, force_refresh: bool = False):
        """
        Fetch configuration files (joins configuration, extra variables) from the repository.
        
        Args:
            target_base_directory (str): Base directory containing subdirectories for different file types
            force_refresh (bool): Force re-download even if files exist
        """
        logger.info(f"Fetching configuration files to {target_base_directory}")
        
        # Clear configuration directories if force refresh is requested
        if force_refresh:
            logger.info("Force refresh enabled - clearing existing configuration files")
            config_subdirs = ["ldm", "il", "joins_configuration", "extra_variables"]
            for subdir in config_subdirs:
                config_dir = os.path.join(target_base_directory, subdir)
                self._clear_directory(config_dir)
            logger.info("Cleared existing configuration files due to force refresh")
        
        files_downloaded = 0
        
        # Configuration file mappings: remote_path -> local_subdirectory
        config_mappings = {
            "birds_nest/resources/ldm": "ldm",
            "birds_nest/resources/il": "il", 
            "birds_nest/resources/joins_configuration": "joins_configuration",
            "birds_nest/resources/extra_variables": "extra_variables",
            "resources/ldm": "ldm",
            "resources/il": "il",
            "resources/joins_configuration": "joins_configuration", 
            "resources/extra_variables": "extra_variables",
            "ldm": "ldm",
            "il": "il",
            "joins_configuration": "joins_configuration",
            "extra_variables": "extra_variables",
            "config": "joins_configuration",  # Alternative name
            "configuration": "joins_configuration"  # Alternative name
        }
        
        for remote_path, local_subdir in config_mappings.items():
            try:
                files = self.fetch_files(remote_path)
                csv_files = [f for f in files if f.get('type') == 'file' and f.get('name', '').endswith('.csv')]
                if csv_files:
                    target_dir = os.path.join(target_base_directory, local_subdir)
                    self._ensure_directory_exists(target_dir)
                    
                    logger.info(f"Found {len(csv_files)} CSV files in {remote_path}")
                    for file_info in csv_files:
                        file_name = file_info.get('name')
                        local_path = os.path.join(target_dir, file_name)
                        
                        # Skip if file exists and not forcing refresh
                        if os.path.exists(local_path) and not force_refresh:
                            logger.debug(f"Skipping existing file: {file_name}")
                            continue
                            
                        if self.download_file(file_info, local_path):
                            files_downloaded += 1
                            logger.info(f"Downloaded config file: {file_name} to {local_subdir}/")
            except Exception as e:
                logger.debug(f"Path {remote_path} not found or inaccessible: {e}")
                continue
        
        logger.info(f"Successfully downloaded {files_downloaded} configuration files")
        return files_downloaded
    
    def fetch_ldm_files(self, target_directory: str, force_refresh: bool = False):
        """
        Fetch LDM export files from the repository.
        
        Args:
            target_directory (str): Local directory to save LDM files
            force_refresh (bool): Force re-download even if files exist
        """
        logger.info(f"Fetching LDM files to {target_directory}")
        
        # Ensure target directory exists
        self._ensure_directory_exists(target_directory)
        
        # Try common paths for LDM files
        possible_paths = [
            "birds_nest/resources/ldm",
            "resources/ldm",
            "ldm",
            "export/database_export_ldm"
        ]
        
        files_downloaded = 0
        for path in possible_paths:
            try:
                files = self.fetch_files(path)
                if files:
                    logger.info(f"Found {len(files)} files in {path}")
                    for file_info in files:
                        if file_info.get('type') == 'file':
                            file_name = file_info.get('name')
                            local_path = os.path.join(target_directory, file_name)
                            
                            # Skip if file exists and not forcing refresh
                            if os.path.exists(local_path) and not force_refresh:
                                logger.debug(f"Skipping existing file: {file_name}")
                                continue
                                
                            if self.download_file(file_info, local_path):
                                files_downloaded += 1
                                logger.info(f"Downloaded LDM file: {file_name}")
                    break  # Stop after finding first valid path
            except Exception as e:
                logger.debug(f"Path {path} not found or inaccessible: {e}")
                continue
        
        if files_downloaded == 0:
            logger.warning("No LDM files found in repository")
        else:
            logger.info(f"Successfully downloaded {files_downloaded} LDM files")
        
        return files_downloaded


class AutomodeConfigurationService:
    """Service class for handling automode configuration and execution."""
    
    def __init__(self):
        self.context = Context()
    
    def validate_github_repository(self, url: str, token: str = None) -> bool:
        """
        Validate that a GitHub repository URL is accessible.
        
        Args:
            url (str): GitHub repository URL
            token (str, optional): GitHub personal access token for private repos
            
        Returns:
            bool: True if repository is accessible, False otherwise
        """
        try:
            # Normalize the URL - remove .git suffix if present
            normalized_url = url.rstrip('/')
            if normalized_url.endswith('.git'):
                normalized_url = normalized_url[:-4]
            
            # Extract owner and repo from URL
            parts = normalized_url.replace('https://github.com/', '').split('/')
            if len(parts) < 2:
                return False
                
            owner, repo = parts[0], parts[1]
            api_url = f"https://api.github.com/repos/{owner}/{repo}"
            
            headers = {}
            if token:
                headers['Authorization'] = f'token {token}'
            
            response = requests.get(api_url, headers=headers, timeout=10)
            if response.status_code == 200:
                logger.info(f"GitHub repository validation successful: {normalized_url}")
                return True
            else:
                # Log detailed error information for debugging
                logger.error(f"GitHub repository validation failed: {normalized_url}")
                logger.error(f"Status code: {response.status_code}")
                logger.error(f"Response: {response.text}")
                if token:
                    logger.error("Token was provided but authentication failed")
                    # Check if it's a token format issue
                    if not token.startswith(('ghp_', 'github_pat_')):
                        logger.error("Token doesn't appear to be in correct format (should start with 'ghp_' or 'github_pat_')")
                else:
                    logger.error("No token provided for repository access")
                return False
        except Exception as e:
            logger.error(f"Error validating GitHub repository {url}: {e}")
            return False
    
    def apply_configuration(self, config: AutomodeConfiguration):
        """
        Apply the given configuration to the system context.
        
        Args:
            config (AutomodeConfiguration): Configuration to apply
        """
        logger.info(f"Applying automode configuration: {config}")
        
        # Update context with data model type
        self.context.ldm_or_il = 'ldm' if config.data_model_type == 'ELDM' else 'il'
        
        # Set as active configuration
        config.is_active = True
        config.save()
        
        logger.info(f"Configuration applied successfully: data_model={config.data_model_type}")
    
    def fetch_files_from_source(self, config: AutomodeConfiguration, github_token: str = None, force_refresh: bool = False):
        """
        Fetch files based on the configuration settings.
        
        Args:
            config (AutomodeConfiguration): Configuration specifying data sources
            github_token (str, optional): GitHub personal access token for private repositories
            force_refresh (bool): Force re-download of existing files
            
        Returns:
            dict: Summary of files downloaded from each source
        """
        logger.info("Starting file fetching based on configuration")
        
        results = {
            'technical_export': 0,
            'config_files': 0,
            'errors': []
        }
        
        # Fetch technical export files
        try:
            if config.technical_export_source == 'BIRD_WEBSITE':
                results['technical_export'] = self._fetch_from_bird_website(force_refresh)
            elif config.technical_export_source == 'GITHUB':
                results['technical_export'] = self._fetch_technical_export_from_github(
                    config.technical_export_github_url, github_token, force_refresh
                )
            elif config.technical_export_source == 'MANUAL_UPLOAD':
                # Manual upload - no automatic fetching, user uploads files manually
                results['technical_export'] = self._check_manual_technical_export_files()
        except Exception as e:
            error_msg = f"Error fetching technical export files: {str(e)}"
            logger.error(error_msg)
            results['errors'].append(error_msg)
        
        # Fetch configuration files
        try:
            if config.config_files_source == 'GITHUB':
                results['config_files'] = self._fetch_config_files_from_github(
                    config.config_files_github_url, github_token, force_refresh
                )
            # MANUAL source doesn't need automatic fetching
        except Exception as e:
            error_msg = f"Error fetching configuration files: {str(e)}"
            logger.error(error_msg)
            results['errors'].append(error_msg)
        
        logger.info(f"File fetching completed: {results}")
        return results
    
    def _fetch_from_bird_website(self, force_refresh: bool = False) -> int:
        """Fetch technical export files from BIRD website."""
        logger.info("Fetching technical export files from BIRD website")
        
        client = BirdEcbWebsiteClient()
        target_dir = "resources/technical_export"
        
        # Clear directory if force refresh is requested
        if force_refresh:
            logger.info("Force refresh enabled - clearing existing BIRD website files")
            if os.path.exists(target_dir):
                import shutil
                shutil.rmtree(target_dir)
                logger.info("Cleared existing technical export directory due to force refresh")
        
        # Create target directory if it doesn't exist
        os.makedirs(target_dir, exist_ok=True)
        
        try:
            # Check if files already exist and not forcing refresh
            if not force_refresh and os.path.exists(target_dir) and os.listdir(target_dir):
                existing_files = [f for f in os.listdir(target_dir) if f.endswith('.csv')]
                if existing_files:
                    logger.info(f"Technical export files already exist ({len(existing_files)} files), skipping download")
                    return len(existing_files)
            
            # Use the existing BIRD website client to download all metadata
            logger.info("Downloading all BIRD metadata from ECB website...")
            client.request_and_save_all(output_dir=target_dir)
            
            # Count downloaded CSV files
            if os.path.exists(target_dir):
                downloaded_files = [f for f in os.listdir(target_dir) if f.endswith('.csv')]
                logger.info(f"Successfully downloaded {len(downloaded_files)} technical export files from BIRD website")
                return len(downloaded_files)
            else:
                logger.warning("Download completed but target directory is empty")
                return 0
            
        except Exception as e:
            logger.error(f"Error fetching from BIRD website: {e}")
            raise
    
    def _fetch_technical_export_from_github(self, github_url: str, token: str = None, force_refresh: bool = False) -> int:
        """Fetch technical export files from GitHub repository."""
        logger.info(f"Fetching technical export files from GitHub: {github_url}")
        
        fetcher = ConfigurableGitHubFileFetcher(github_url, token)
        target_dir = "resources/technical_export"
        
        return fetcher.fetch_technical_exports(target_dir, force_refresh)
    
    def _fetch_config_files_from_github(self, github_url: str, token: str = None, force_refresh: bool = False) -> int:
        """Fetch configuration files from GitHub repository.""" 
        logger.info(f"Fetching configuration files from GitHub: {github_url}")
        
        fetcher = ConfigurableGitHubFileFetcher(github_url, token)
        base_dir = "resources"
        
        return fetcher.fetch_configuration_files(base_dir, force_refresh)
    
    def _check_manual_technical_export_files(self) -> int:
        """
        Check for manually uploaded technical export files.
        
        Returns:
            int: Number of technical export files found
        """
        logger.info("Checking for manually uploaded technical export files")
        
        target_dir = "resources/technical_export"
        file_count = 0
        
        if os.path.exists(target_dir):
            csv_files = [f for f in os.listdir(target_dir) if f.endswith('.csv')]
            file_count = len(csv_files)
            logger.info(f"Found {file_count} manually uploaded CSV files in {target_dir}")
            
            if file_count > 0:
                logger.info(f"Manual technical export files: {csv_files}")
            else:
                logger.warning("No CSV files found in technical export directory for manual upload mode")
        else:
            logger.warning(f"Technical export directory {target_dir} does not exist for manual upload mode")
        
        return file_count
    
    def execute_automode_setup(self, config: AutomodeConfiguration, github_token: str = None, force_refresh: bool = False):
        """
        Execute the complete automode setup process with the given configuration.
        
        Args:
            config (AutomodeConfiguration): Configuration to use for setup
            github_token (str, optional): GitHub personal access token for private repositories
            force_refresh (bool): Force refresh of all data
            
        Returns:
            dict: Results of the setup process
        """
        logger.info("Starting automode setup execution")
        
        results = {
            'configuration_applied': False,
            'files_fetched': {},
            'database_setup': False,
            'errors': []
        }
        
        try:
            # Step 1: Apply configuration
            self.apply_configuration(config)
            results['configuration_applied'] = True
            
            # Step 2: Fetch files based on configuration
            fetch_results = self.fetch_files_from_source(config, github_token, force_refresh)
            results['files_fetched'] = fetch_results
            
            # Step 3: Check stopping point and execute accordingly
            if config.when_to_stop == 'RESOURCE_DOWNLOAD':
                logger.info("Stopping after resource download as configured")
                results['stopped_at'] = 'RESOURCE_DOWNLOAD'
                results['next_steps'] = 'Move to step by step mode for manual processing'
                
            elif config.when_to_stop == 'DATABASE_CREATION':
                logger.info("Proceeding to database creation...")
                database_results = self._create_bird_database()
                results['database_creation'] = database_results
                results['stopped_at'] = 'DATABASE_CREATION'
                results['next_steps'] = 'BIRD database created successfully. Ready for next steps.'
                
            elif config.when_to_stop == 'SMCUBES_RULES':
                logger.info("Proceeding to SMCubes rules creation...")
                smcubes_results = self._create_smcubes_transformations()
                results['smcubes_rules'] = smcubes_results
                results['stopped_at'] = 'SMCUBES_RULES'
                results['next_steps'] = 'SMCubes generation rules created. Ready for custom configuration.'
                
            elif config.when_to_stop == 'PYTHON_CODE':
                logger.info("Python code generation not yet implemented")
                results['errors'].append("Python code generation option is not yet implemented")
                
            elif config.when_to_stop == 'FULL_EXECUTION':
                logger.info("Full execution with testing not yet implemented")
                results['errors'].append("Full execution option is not yet implemented")
            
            logger.info("Automode setup execution completed successfully")
            
        except Exception as e:
            error_msg = f"Error during automode setup: {str(e)}"
            logger.error(error_msg)
            results['errors'].append(error_msg)
        
        return results
    
    def _create_smcubes_transformations(self):
        """
        Create SMCubes transformation rules for custom configuration.
        This reuses the existing run_full_setup functionality from views.py.
        
        Returns:
            dict: Results of the SMCubes transformation creation process
        """
        logger.info("Starting SMCubes transformations creation using run_full_setup functionality...")
        
        results = {
            'database_setup': False,
            'metadata_population': False,
            'filters_creation': False,
            'joins_creation': False,
            'errors': []
        }
        
        try:
            # Call the core full setup logic from views.py
            from . import views
            views.execute_full_setup_core()
            
            results['database_setup'] = True
            results['metadata_population'] = True
            results['filters_creation'] = True
            results['joins_creation'] = True
            
            logger.info("SMCubes transformations creation completed successfully")
            
        except Exception as e:
            error_msg = f"Error during SMCubes transformations creation: {str(e)}"
            logger.error(error_msg)
            results['errors'].append(error_msg)
            
        return results
    
    def _create_bird_database(self):
        """
        Create the BIRD database using the automode database setup functionality.
        This implements the equivalent of the 'Create the database' step.
        
        Returns:
            dict: Results of the database creation process
        """
        logger.info("Starting BIRD database creation...")
        
        results = {
            'django_models_created': False,
            'database_setup_completed': False,
            'errors': []
        }
        
        try:
            # Import the automode database setup module
            from .entry_points.automode_database_setup import RunAutomodeDatabaseSetup
            
            # Run the automode database setup
            logger.info("Executing automode database setup...")
            database_setup = RunAutomodeDatabaseSetup('pybirdai', 'birds_nest')
            database_setup.run_automode_database_setup()
            
            results['django_models_created'] = True
            results['database_setup_completed'] = True
            
            logger.info("BIRD database creation completed successfully")
            
        except Exception as e:
            error_msg = f"Error during BIRD database creation: {str(e)}"
            logger.error(error_msg)
            results['errors'].append(error_msg)
            
        return results