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
#    Benjamin Arfa - mirror service integration

"""
Automode configuration service for handling setup and workflow execution.

This service orchestrates file fetching based on configuration:
- For AUTOMODE SETUP: Uses SetupRepoService (destructive)
- For WORKFLOW EXECUTION: Uses MirrorRepoService (non-destructive)
"""

import os
import logging
import requests
import traceback

from pybirdai.utils.bird_ecb_website_fetcher import BirdEcbWebsiteClient
from pybirdai.context.context import Context

from .setup_service import SetupRepoService
from .mirror_service import MirrorRepoService

logger = logging.getLogger(__name__)
logger.level = logging.DEBUG


class AutomodeConfigurationService:
    """Service class for handling automode configuration and execution."""

    def __init__(self, token: str = None):
        """
        Initialize the service.

        Args:
            token (str, optional): GitHub token for authenticated requests
        """
        self.context = Context()
        self.token = token

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
                logger.error(f"GitHub repository validation failed: {normalized_url}")
                logger.error(f"Status code: {response.status_code}")
                logger.error(f"Response: {response.text}")
                if token:
                    logger.error("Token was provided but authentication failed")
                    if not token.startswith(('ghp_', 'github_pat_')):
                        logger.error("Token doesn't appear to be in correct format (should start with 'ghp_' or 'github_pat_')")
                else:
                    logger.error("No token provided for repository access")
                return False
        except Exception as e:
            logger.error(f"Error validating GitHub repository {url}: {e}")
            return False

    def apply_configuration(self, config):
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

    def fetch_files_from_source(self, config, github_token: str = None, force_refresh: bool = False,
                                 use_mirror: bool = False):
        """
        Fetch files based on the configuration settings.

        Args:
            config (AutomodeConfiguration): Configuration specifying data sources
            github_token (str, optional): GitHub personal access token for private repositories
            force_refresh (bool): Force re-download of existing files
            use_mirror (bool): If True, use MirrorRepoService (non-destructive);
                              If False, use SetupRepoService (destructive, default for setup)

        Returns:
            dict: Summary of files downloaded from each source
        """
        logger.info("Starting file fetching based on configuration")
        if use_mirror:
            logger.info("Using MIRROR mode (non-destructive)")
        else:
            logger.info("Using SETUP mode (destructive)")

        results = {
            'technical_export': 0,
            'config_files': 0,
            'test_suite': 0,
            'generated_python': 0,
            'filter_code': 0,
            'test_fixtures': 0,
            'report_templates': 0,
            'errors': []
        }

        # Use BIRD Content Repository URL as primary source for Input Layer artifacts
        from pybirdai.services.pipeline_repo_service import get_configured_pipeline_url, detect_pipeline
        selected_frameworks = getattr(config, 'selected_frameworks', []) or []
        pipeline_name = detect_pipeline(selected_frameworks) if selected_frameworks else 'main'
        github_url = config.technical_export_github_url

        # Fall back to pipeline URL if BIRD Content Repository URL not configured
        if not github_url:
            github_url = get_configured_pipeline_url(pipeline_name)
            if github_url:
                logger.warning(f"No BIRD Content Repository URL configured, falling back to {pipeline_name} pipeline: {github_url}")
        else:
            logger.info(f"Using BIRD Content Repository URL: {github_url}")

        # Fetch BIRD content repository (contains both config files and technical export)
        if config.technical_export_source == 'BIRD_WEBSITE':
            results['technical_export'] = self._fetch_from_bird_website(force_refresh)
        elif config.technical_export_source == 'GITHUB':
            branch = getattr(config, 'bird_content_branch', getattr(config, 'github_branch', 'main'))
            bird_content_result = self._fetch_from_github(
                github_url, github_token, force_refresh, branch, use_mirror=use_mirror,
                pipeline=pipeline_name
            )
            results['technical_export'] = bird_content_result
            results['config_files'] = bird_content_result

        # Fetch REF_FINREP report template HTML files (use pipeline-aware URL)
        if github_url:
            try:
                logger.info("Fetching REF_FINREP report template HTML files from GitHub...")
                from pybirdai.utils.github_file_fetcher import GitHubFileFetcher
                fetcher = GitHubFileFetcher(github_url)
                results['report_templates'] = fetcher.fetch_report_template_htmls()
                logger.info(f"Downloaded {results['report_templates']} REF_FINREP report templates")
            except Exception as e:
                error_msg = f"Error fetching report templates: {str(e)}"
                logger.error(error_msg)
                results['errors'].append(error_msg)

        return results

    def fetch_files_for_framework(self, framework: str, github_token: str = None, force_refresh: bool = False,
                                   branch: str = 'main', use_mirror: bool = True):
        """
        Fetch files for a specific framework using the pipeline-specific GitHub URL.

        This method is used by standalone scripts to import framework-specific data
        without re-running the full setup process.

        Args:
            framework (str): Framework identifier ('FINREP', 'ANCRDT', 'COREP', etc.)
            github_token (str, optional): GitHub personal access token for private repositories
            force_refresh (bool): Force re-download of existing files
            branch (str): Git branch to fetch from (default: main)
            use_mirror (bool): If True, use MirrorRepoService (non-destructive, default);
                              If False, use SetupRepoService (destructive)

        Returns:
            dict: Summary of files downloaded
        """
        import json
        from pybirdai.models.workflow_model import AutomodeConfiguration

        logger.info(f"Fetching files for framework: {framework}")

        # First, check for environment variables (highest priority)
        env_urls = {
            'main': os.environ.get('PIPELINE_URL_MAIN', ''),
            'ancrdt': os.environ.get('PIPELINE_URL_ANCRDT', '') or os.environ.get('ANCRDT_PIPELINE_URL', ''),
            'dpm': os.environ.get('PIPELINE_URL_DPM', ''),
        }
        if any(env_urls.values()):
            logger.info("Found pipeline URLs in environment variables")

        # Second, try to read from automode_config.json
        config_file_urls = {}
        config_paths = ['./automode_config.json', 'automode_config.json']
        for config_path in config_paths:
            if os.path.exists(config_path):
                try:
                    with open(config_path, 'r') as f:
                        file_config = json.load(f)
                        config_file_urls = {
                            'main': file_config.get('pipeline_url_main', ''),
                            'ancrdt': file_config.get('pipeline_url_ancrdt', ''),
                            'dpm': file_config.get('pipeline_url_dpm', ''),
                        }
                        logger.info(f"Loaded pipeline URLs from config file: {config_path}")
                        break
                except Exception as e:
                    logger.warning(f"Could not read config file {config_path}: {e}")

        # Fall back to database configuration
        config = AutomodeConfiguration.get_active_configuration()
        if not config:
            config = AutomodeConfiguration.objects.create(is_active=True)

        # Determine which pipeline URL to use based on framework
        from pybirdai.context.framework_config import get_pipeline_for_framework
        pipeline_name = get_pipeline_for_framework(framework)

        # Get URL for the determined pipeline
        if pipeline_name == 'main':
            github_url = env_urls.get('main') or config_file_urls.get('main') or config.pipeline_url_main
        elif pipeline_name == 'ancrdt':
            github_url = env_urls.get('ancrdt') or config_file_urls.get('ancrdt') or config.pipeline_url_ancrdt
        elif pipeline_name == 'dpm':
            github_url = env_urls.get('dpm') or config_file_urls.get('dpm') or config.pipeline_url_dpm
        else:
            github_url = env_urls.get('main') or config_file_urls.get('main') or config.pipeline_url_main

        if not github_url:
            raise ValueError(f"No pipeline URL configured for {pipeline_name}. Please configure it in the dashboard settings.")

        logger.info(f"Using pipeline '{pipeline_name}' URL: {github_url}")

        results = {
            'framework': framework,
            'pipeline': pipeline_name,
            'github_url': github_url,
            'technical_export': 0,
            'config_files': 0,
            'errors': []
        }

        try:
            # Fetch only framework-specific files
            file_count = self._fetch_framework_files_only(
                github_url, github_token, force_refresh, branch, use_mirror=use_mirror
            )
            results['technical_export'] = file_count
            results['config_files'] = file_count

            # Fetch report templates if available
            try:
                from pybirdai.utils.github_file_fetcher import GitHubFileFetcher
                fetcher = GitHubFileFetcher(github_url)
                results['report_templates'] = fetcher.fetch_report_template_htmls()
            except Exception as e:
                logger.warning(f"Could not fetch report templates: {e}")

            logger.info(f"Successfully fetched {file_count} files for framework {framework}")

        except Exception as e:
            error_msg = f"Error fetching files for framework {framework}: {str(e)}"
            logger.error(error_msg)
            results['errors'].append(error_msg)

        return results

    def _fetch_framework_files_only(self, github_url: str, token: str = None, force_refresh: bool = False,
                                     branch: str = "main", use_mirror: bool = True) -> int:
        """
        Fetch only framework-specific files from GitHub.

        Args:
            github_url: GitHub repository URL
            token: Optional GitHub token for private repos
            force_refresh: Force re-download
            branch: Git branch to fetch from
            use_mirror: If True, use non-destructive mirroring

        Returns:
            int: Number of files/directories copied
        """
        import shutil
        import zipfile
        import tempfile

        logger.info(f"Fetching framework-specific files from GitHub: {github_url} (branch: {branch})")
        logger.info(f"Mode: {'MIRROR (non-destructive)' if use_mirror else 'SETUP (destructive)'}")

        # Framework-specific directories to fetch (whitelist)
        # New unified structure: filter_code/{type}/{FRAMEWORK}/filter|joins/
        FRAMEWORK_WHITELIST = [
            (f"export{os.sep}database_export_ldm", f"resources{os.sep}technical_export"),
            (f"joins_configuration", f"resources{os.sep}joins_configuration"),
            (f"birds_nest{os.sep}resources{os.sep}extra_variables", f"resources{os.sep}extra_variables"),
            (f"birds_nest{os.sep}resources{os.sep}derivation_files", f"resources{os.sep}derivation_files"),
            # Filter code - export structure (new location)
            (f"export{os.sep}filter_code", f"pybirdai{os.sep}process_steps{os.sep}filter_code"),
            # Filter code - legacy birds_nest structure
            (f"birds_nest{os.sep}pybirdai{os.sep}process_steps{os.sep}filter_code", f"pybirdai{os.sep}process_steps{os.sep}filter_code"),
            (f"birds_nest{os.sep}resources{os.sep}il", f"resources{os.sep}il"),
            # Generated Python files with new unified structure
            (f"birds_nest{os.sep}results{os.sep}generated_python", f"results{os.sep}generated_python"),
        ]

        # Files to preserve (backup before copy, restore after)
        PRESERVE_FILES = [
            os.path.join("resources", "derivation_files", "derivation_config.csv"),
            os.path.join("pybirdai", "process_steps", "filter_code", "lib", "automatic_tracking_wrapper.py"),
        ]

        # If using mirror mode, also protect generated code directories
        # Note: filter_code is NOT protected - it uses merge strategy instead
        if use_mirror:
            PROTECTED_TARGETS = [
                # New unified structure
                f"results{os.sep}generated_python",
                # Legacy locations (for backward compatibility)
                f"results{os.sep}generated_python_filters",
                f"results{os.sep}generated_python_joins",
            ]
        else:
            PROTECTED_TARGETS = []

        try:
            repo_name = github_url.split("/")[-1]
            temp_dir = tempfile.mkdtemp()

            # Download repository
            headers = {'Authorization': f'Bearer {token}'} if token else {}
            zip_url = f"{github_url}/archive/refs/heads/{branch}.zip"
            logger.info(f"Downloading repository from {zip_url}")

            import urllib.request
            req = urllib.request.Request(zip_url, headers=headers)
            zip_path = os.path.join(temp_dir, f"{repo_name}.zip")

            with urllib.request.urlopen(req) as response:
                with open(zip_path, 'wb') as f:
                    f.write(response.read())

            # Extract repository
            extract_dir = os.path.join(temp_dir, "extracted")
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(extract_dir)

            # Find extracted folder
            extracted_folders = os.listdir(extract_dir)
            if not extracted_folders:
                raise RuntimeError("No extracted folder found")
            repo_folder = os.path.join(extract_dir, extracted_folders[0])
            logger.info(f"Extracted repository to: {repo_folder}")

            # Backup files to preserve
            backup_dir = os.path.join(temp_dir, "backup")
            os.makedirs(backup_dir, exist_ok=True)
            backed_up = {}
            for preserve_file in PRESERVE_FILES:
                if os.path.exists(preserve_file):
                    backup_path = os.path.join(backup_dir, preserve_file)
                    os.makedirs(os.path.dirname(backup_path), exist_ok=True)
                    shutil.copy2(preserve_file, backup_path)
                    backed_up[preserve_file] = backup_path
                    logger.info(f"Backed up: {preserve_file}")

            # Copy only whitelisted directories
            copied_count = 0
            for source_rel, target_rel in FRAMEWORK_WHITELIST:
                source_path = os.path.join(repo_folder, source_rel)
                target_path = target_rel

                if not os.path.exists(source_path):
                    logger.warning(f"Source path does not exist, skipping: {source_path}")
                    continue

                # Skip protected targets in mirror mode
                if use_mirror and target_rel in PROTECTED_TARGETS:
                    logger.info(f"Skipping protected directory in mirror mode: {target_rel}")
                    continue

                # Remove existing target directory content
                if os.path.exists(target_path):
                    if not use_mirror:
                        shutil.rmtree(target_path)
                        logger.info(f"Removed existing: {target_path}")
                    # In mirror mode, we'll merge instead

                # Copy from source
                if os.path.isdir(source_path):
                    if use_mirror and os.path.exists(target_path):
                        # Merge: copy only new/updated files
                        for root, dirs, files in os.walk(source_path):
                            rel_root = os.path.relpath(root, source_path)
                            target_root = os.path.join(target_path, rel_root) if rel_root != '.' else target_path
                            os.makedirs(target_root, exist_ok=True)
                            for file in files:
                                src_file = os.path.join(root, file)
                                tgt_file = os.path.join(target_root, file)
                                shutil.copy2(src_file, tgt_file)
                        logger.info(f"Merged directory: {source_rel} -> {target_path}")
                    else:
                        shutil.copytree(source_path, target_path)
                        logger.info(f"Copied directory: {source_rel} -> {target_path}")
                else:
                    os.makedirs(os.path.dirname(target_path), exist_ok=True)
                    shutil.copy2(source_path, target_path)
                    logger.info(f"Copied file: {source_rel} -> {target_path}")

                copied_count += 1

            # Restore preserved files
            for orig_path, backup_path in backed_up.items():
                os.makedirs(os.path.dirname(orig_path), exist_ok=True)
                shutil.copy2(backup_path, orig_path)
                logger.info(f"Restored: {orig_path}")

            # Cleanup temp directory
            shutil.rmtree(temp_dir)
            logger.info("Cleaned up temporary files")

            # Fetch logical transformation rules from ECB API
            try:
                ecb_client = BirdEcbWebsiteClient()
                target_dir = "resources/technical_export"
                os.makedirs(target_dir, exist_ok=True)
                ecb_client.request_logical_transformation_rules(output_dir=target_dir)
                logger.info("Downloaded logical transformation rules from ECB API")
            except Exception as e:
                logger.warning(f"Could not download logical transformation rules: {e}")

            return copied_count

        except Exception as e:
            logger.error(f"Error fetching framework files: {e}")
            raise

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

        os.makedirs(target_dir, exist_ok=True)

        # Create derivation file directories and tmp files
        derivation_dirs = [
            "resources/derivation_files/generated_from_logical_transformation_rules",
            "resources/derivation_files/generated_from_member_links",
            "resources/derivation_files/manually_generated",
        ]
        for d in derivation_dirs:
            os.makedirs(d, exist_ok=True)
            tmp_file = os.path.join(d, 'tmp')
            if not os.path.exists(tmp_file):
                with open(tmp_file, 'w') as f:
                    pass

        try:
            # Check if files already exist and not forcing refresh
            skip_technical_export = False
            if not force_refresh and os.path.exists(target_dir) and os.listdir(target_dir):
                existing_files = [f for f in os.listdir(target_dir) if f.endswith('.csv')]
                if existing_files:
                    logger.info(f"Technical export files already exist ({len(existing_files)} files), skipping download")
                    skip_technical_export = True

            if not skip_technical_export:
                logger.info("Downloading all BIRD metadata from ECB website...")
                client.request_and_save_all(output_dir=target_dir)

                try:
                    client.request_logical_transformation_rules(output_dir=target_dir)
                    logger.info("Downloaded logical transformation rules for derived fields")
                except Exception as e:
                    logger.warning(f"Could not download logical transformation rules: {e}")

            # Member link derivation generation
            self._generate_member_link_derivations(client, force_refresh)

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

    def _fetch_from_github(self, github_url: str, token: str = None, force_refresh: bool = False,
                           branch: str = "main", use_mirror: bool = False, pipeline: str = None) -> int:
        """
        Fetch BIRD content files from GitHub repository.

        Args:
            github_url: GitHub repository URL
            token: Optional GitHub token
            force_refresh: Force re-download
            branch: Git branch to fetch from
            use_mirror: If True, use MirrorRepoService (non-destructive);
                       If False, use SetupRepoService (destructive)
            pipeline: Pipeline name for filter_code filtering (datasets/templates)

        Returns:
            int: Number of items fetched
        """
        logger.info(f"Fetching BIRD content from GitHub: {github_url} (branch: {branch})")
        logger.info(f"Mode: {'MIRROR (non-destructive)' if use_mirror else 'SETUP (destructive)'}")

        try:
            repo_name = github_url.split("/")[-1]

            if use_mirror:
                # Use MirrorRepoService for non-destructive updates
                fetcher = MirrorRepoService(token)
                fetcher.clone_repo(github_url, repo_name, branch)
                fetcher.mirror_files(repo_name)
                fetcher.remove_fetched_files(repo_name)
            else:
                # Use SetupRepoService for destructive setup
                fetcher = SetupRepoService(token)
                fetcher.clone_repo(github_url, repo_name, branch)
                fetcher.setup_files(repo_name, pipeline=pipeline)
                fetcher.remove_fetched_files(repo_name)

            # Fetch logical transformation rules from ECB API
            try:
                ecb_client = BirdEcbWebsiteClient()
                target_dir = "resources/technical_export"
                os.makedirs(target_dir, exist_ok=True)
                ecb_client.request_logical_transformation_rules(output_dir=target_dir)
                logger.info("Downloaded logical transformation rules for derived fields from ECB API")
            except Exception as e:
                logger.warning(f"Could not download logical transformation rules from ECB API: {e}")

            # Member link derivation generation
            self._generate_member_link_derivations(BirdEcbWebsiteClient(), force_refresh)

            return 1

        except Exception as e:
            logger.error(f"Error fetching from GitHub repository: {e}")
            raise

    def _fetch_test_suite_from_github(self, github_url: str = "https://github.com/regcommunity/bird-default-test-suite",
                                       token: str = None, force_refresh: bool = False, branch: str = "main",
                                       use_mirror: bool = True) -> int:
        """
        Fetch test suite files from GitHub repository.

        Args:
            github_url: GitHub repository URL for test suite
            token: Optional GitHub token
            force_refresh: Force re-download
            branch: Git branch to fetch from
            use_mirror: If True, use MirrorRepoService (non-destructive, default);
                       If False, use SetupRepoService (destructive)

        Returns:
            int: Number of items fetched
        """
        logger.info(f"Fetching test suite files from GitHub: {github_url} (branch: {branch})")
        logger.info(f"Mode: {'MIRROR (non-destructive)' if use_mirror else 'SETUP (destructive)'}")

        try:
            repo_name = github_url.split("/")[-1]

            if use_mirror:
                # Use MirrorRepoService for non-destructive updates
                fetcher = MirrorRepoService(token)
                fetcher.clone_repo(github_url, repo_name, branch)
                fetcher.mirror_test_suite_files(repo_name)
                fetcher.remove_fetched_files(repo_name)
            else:
                # Use SetupRepoService for destructive setup
                fetcher = SetupRepoService(token)
                fetcher.clone_repo(github_url, repo_name, branch)
                fetcher.setup_test_suite_files(repo_name)
                fetcher.remove_fetched_files(repo_name)

            return 1

        except Exception as e:
            logger.error(f"Error fetching test suite from GitHub repository: {e}")
            raise

    def _generate_member_link_derivations(self, client, force_refresh: bool = False):
        """Generate member link derivations from ECB API data."""
        logger.info("=== Starting member link derivation generation ===")
        output_dir_derivations = "resources/derivation_files/generated_from_member_links/"
        existing_derivations = []
        if os.path.exists(output_dir_derivations):
            existing_derivations = [f for f in os.listdir(output_dir_derivations)
                                    if f.endswith('.py') and not f.startswith('__')]
            logger.info(f"Found {len(existing_derivations)} existing derivation files in {output_dir_derivations}")

        if existing_derivations and not force_refresh:
            logger.info(f"Member link derivation files already exist ({len(existing_derivations)} files), skipping generation")
        else:
            logger.info(f"Generating member link derivations (force_refresh={force_refresh}, existing={len(existing_derivations)})")
            member_link_path = None
            try:
                derivation_files_dir = "resources/derivation_files"
                logger.info(f"Fetching ANCRDT member_link data from ECB API...")
                member_link_path = client.request_ancrdt_member_link(output_dir=derivation_files_dir)
                logger.info(f"Downloaded ANCRDT member_link data to: {member_link_path}")

                if os.path.exists(member_link_path):
                    file_size = os.path.getsize(member_link_path)
                    logger.info(f"member_link_for_derivation.csv exists: {member_link_path} ({file_size} bytes)")
                    try:
                        from pybirdai.process_steps.derivation_generation.member_link_derivation import (
                            run_derivation_generation
                        )
                        os.makedirs(output_dir_derivations, exist_ok=True)
                        tmp_file = os.path.join(output_dir_derivations, 'tmp')
                        if not os.path.exists(tmp_file):
                            with open(tmp_file, 'w') as f:
                                pass
                        logger.info(f"Generating TYP_INSTRMNT_ANCRDT derivation from member links")
                        output_path = run_derivation_generation(
                            csv_path=member_link_path,
                            output_dir=output_dir_derivations,
                            target_cube="ANCRDT_INSTRMNT_C",
                            target_variable="TYP_INSTRMNT",
                            output_variable="TYP_INSTRMNT_ANCRDT",
                            class_name="INSTRMNT",
                            verbose=True
                        )
                        if output_path:
                            logger.info(f"Generated member link derivation file: {output_path}")
                        else:
                            logger.warning("No member link derivation file was generated for TYP_INSTRMNT_ANCRDT.")
                    except Exception as e:
                        logger.error(f"Failed to generate member link derivations: {e}")
                        logger.error(f"Traceback: {traceback.format_exc()}")
                else:
                    logger.warning(f"member_link_for_derivation.csv was not created at: {member_link_path}")
            except Exception as e:
                logger.error(f"Could not download ANCRDT member_link data: {e}")
                logger.error(f"Traceback: {traceback.format_exc()}")
            finally:
                if member_link_path and os.path.exists(member_link_path):
                    os.remove(member_link_path)
                    logger.info(f"Cleaned up temporary file: {member_link_path}")

    def _check_manual_technical_export_files(self) -> int:
        """Check for manually uploaded technical export files."""
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

    def execute_automode_setup(self, config, github_token: str = None, force_refresh: bool = False):
        """
        Execute the complete automode setup process with the given configuration.

        NOTE: This uses SetupRepoService (destructive mode) as it's initial setup.

        Args:
            config (AutomodeConfiguration): Configuration to use for setup
            github_token (str, optional): GitHub personal access token for private repositories
            force_refresh (bool): Force refresh of all data

        Returns:
            dict: Results of the setup process
        """
        logger.info("Starting automode setup execution (DESTRUCTIVE MODE)")

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

            # Step 2: Fetch files (destructive mode - use_mirror=False)
            fetch_results = self.fetch_files_from_source(config, github_token, force_refresh, use_mirror=False)
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
                _ = self._run_tests_suite()
                logger.info("Full execution with testing not yet implemented")
                results['errors'].append("Full execution option is not yet implemented")

            logger.info("Automode setup execution completed successfully")

        except Exception as e:
            error_msg = f"Error during automode setup: {str(e)}"
            logger.error(error_msg)
            results['errors'].append(error_msg)

        return results

    def _create_smcubes_transformations(self):
        """Create SMCubes transformation rules for custom configuration."""
        logger.info("Starting SMCubes transformations creation using run_full_setup functionality...")

        results = {
            'database_setup': False,
            'metadata_population': False,
            'filters_creation': False,
            'joins_creation': False,
            'errors': []
        }

        try:
            from ..views import core_views as views
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

    def execute_automode_setup_with_database_creation(self, config, github_token: str = None, force_refresh: bool = False):
        """
        Execute the complete automode setup process with correct step ordering.

        NOTE: This uses SetupRepoService (destructive mode) as it's initial setup.

        Args:
            config: Configuration object
            github_token (str, optional): GitHub personal access token
            force_refresh (bool): Force refresh of all data

        Returns:
            dict: Results of the setup process
        """
        logger.info("Starting automode setup with correct step ordering (DESTRUCTIVE MODE)")

        results = {
            'files_fetched': {},
            'database_created': False,
            'server_restart_required': False,
            'setup_completed': False,
            'errors': []
        }

        try:
            # Step 1: Fetch files (destructive mode)
            logger.info("Step 1: Fetching technical resources and configuration files...")
            fetch_results = self.fetch_files_from_source(config, github_token, force_refresh, use_mirror=False)
            results['files_fetched'] = fetch_results
            logger.info(f"Resource fetching completed: {fetch_results}")

            if config.when_to_stop == 'RESOURCE_DOWNLOAD':
                logger.info("Stopping after resource download as configured")
                results['stopped_at'] = 'RESOURCE_DOWNLOAD'
                results['next_steps'] = 'Move to step by step mode for manual processing'
                results['setup_completed'] = True
                return results

            # Step 2: Create the database and Django models
            logger.info("Step 2: Creating database and Django models...")
            database_results = self._create_bird_database()
            results['database_created'] = True
            results['server_restart_required'] = True
            logger.info("Database and Django models created successfully")

            logger.info("SERVER RESTART REQUIRED - User must restart server manually to apply model changes")

            if config.when_to_stop == 'DATABASE_CREATION':
                logger.info("Setup complete - stopped after database creation as configured")
                results['stopped_at'] = 'DATABASE_CREATION'
                results['next_steps'] = 'Please restart the server manually to apply Django model changes, then proceed with next steps.'
                results['setup_completed'] = True
                return results

            if config.when_to_stop == 'FULL_EXECUTION':
                results['stopped_at'] = 'SERVER_RESTART_REQUIRED'
                results['next_steps'] = 'Please restart the server manually to apply Django model changes. Generated Python files will be transferred during restart process.'
                results['setup_completed'] = False
            else:
                results['stopped_at'] = 'SERVER_RESTART_REQUIRED'
                results['next_steps'] = 'Please restart the server manually to apply Django model changes. After restart, continue based on your when_to_stop setting.'
                results['setup_completed'] = False

            logger.info("Automode setup initial phase completed - awaiting manual server restart")

        except Exception as e:
            error_msg = f"Error during automode setup with database creation: {str(e)}"
            logger.error(error_msg)
            results['errors'].append(error_msg)

        return results

    def execute_automode_post_restart(self, config):
        """Execute automode steps that require the database to be available (after server restart)."""
        logger.info("Starting automode execution after server restart")

        results = {
            'smcubes_rules': {},
            'python_code': {},
            'full_execution': {},
            'setup_completed': False,
            'errors': []
        }

        try:
            if config.when_to_stop == 'SMCUBES_RULES':
                logger.info("Proceeding to SMCubes rules creation...")
                smcubes_results = self._create_smcubes_transformations()
                results['smcubes_rules'] = smcubes_results
                results['stopped_at'] = 'SMCUBES_RULES'
                results['next_steps'] = 'SMCubes generation rules created. Ready for custom configuration.'
                results['setup_completed'] = True

            elif config.when_to_stop == 'PYTHON_CODE':
                logger.info("Python code generation not yet implemented")
                results['errors'].append("Python code generation option is not yet implemented")
                results['stopped_at'] = 'PYTHON_CODE'
                results['setup_completed'] = False

            elif config.when_to_stop == 'FULL_EXECUTION':
                logger.info("Full execution with testing completed during database setup")
                _ = self._run_tests_suite()
                results['stopped_at'] = 'FULL_EXECUTION'
                results['next_steps'] = 'Generated Python files have been transferred to filter_code directory. System ready for testing.'
                results['setup_completed'] = True
            else:
                results['stopped_at'] = 'UNKNOWN'
                results['next_steps'] = 'Unknown when_to_stop configuration'
                results['setup_completed'] = True

            logger.info("Automode post-restart execution completed")

        except Exception as e:
            error_msg = f"Error during automode post-restart execution: {str(e)}"
            logger.error(error_msg)
            results['errors'].append(error_msg)

        return results

    def _create_bird_database(self):
        """Create the BIRD database using the automode database setup functionality."""
        logger.info("Starting BIRD database creation...")

        results = {
            'django_models_created': False,
            'database_setup_completed': False,
            'errors': []
        }

        try:
            from pybirdai.entry_points.database_setup import RunApplicationSetup

            logger.info("Executing automode database setup...")
            database_setup = RunApplicationSetup('pybirdai', 'birds_nest', token=self.token)
            database_setup.run_automode_setup()

            results['django_models_created'] = True
            results['database_setup_completed'] = True

            logger.info("BIRD database creation completed successfully")

        except Exception as e:
            error_msg = f"Error during BIRD database creation: {str(e)}"
            logger.error(error_msg)
            results['errors'].append(error_msg)

        return results

    def _discover_test_suites(self) -> list:
        """Discover test suites in the tests/ directory."""
        test_suites = []
        tests_dir = "tests"

        for item in os.listdir(tests_dir):
            item_path = os.path.join(tests_dir, item)

            if not os.path.isdir(item_path):
                continue

            manifest_bool = os.path.exists(os.path.join(item_path, "suite_manifest.json")) or os.path.exists(os.path.join(item_path, "suite_manifest.yaml"))
            if manifest_bool:
                test_suites.append(item)
                logger.info(f"Discovered test suite: {item}")

        return test_suites

    def _run_tests_suite(self):
        """Run all test suites found in the tests/ directory."""
        logger.info("Starting test suite execution...")

        results = {
            'tests_executed': False,
            'test_results': {},
            'suites_run': [],
            'errors': []
        }

        try:
            test_suites = self._discover_test_suites()

            if not test_suites:
                logger.warning("No test suites found in tests/ directory")
                results['errors'].append("No test suites found")
                return results

            logger.info(f"Found {len(test_suites)} test suite(s): {', '.join(test_suites)}")

            from pybirdai.utils.datapoint_test_run.run_tests import RegulatoryTemplateTestRunner

            for suite_name in test_suites:
                logger.info(f"Running tests for suite: {suite_name}")

                try:
                    test_runner = RegulatoryTemplateTestRunner()

                    config_file = f"tests/{suite_name}/configuration_file_tests.json"
                    test_runner.args.uv = "False"
                    test_runner.args.config_file = config_file
                    test_runner.args.dp_value = None
                    test_runner.args.reg_tid = None
                    test_runner.args.dp_suffix = None
                    test_runner.args.scenario = None

                    logger.info(f"Executing test runner for {suite_name} with config: {config_file}")
                    test_runner.main()

                    results['suites_run'].append(suite_name)
                    results['test_results'][suite_name] = {'status': 'completed', 'config_file': config_file}

                except Exception as suite_error:
                    error_msg = f"Error running tests for suite '{suite_name}': {str(suite_error)}"
                    logger.error(error_msg)
                    results['errors'].append(error_msg)

            results['tests_executed'] = len(results['suites_run']) > 0
            logger.info(f"Test suite execution completed. Suites run: {len(results['suites_run'])}")

        except Exception as e:
            error_msg = f"Error during test suite execution: {str(e)}"
            logger.error(error_msg)
            results['errors'].append(error_msg)

        return results
