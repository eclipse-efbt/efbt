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

"""
GitHub integration service for pushing files and creating pull requests.
"""

import os
import io
import logging
import requests
import traceback
import time
import zipfile
import shutil
import concurrent.futures

from .helpers import DEFAULT_GITHUB_BRANCH

logger = logging.getLogger(__name__)


def _cleanup_export_intermediates():
    """
    Clean up intermediate folders created during export.

    These folders are temporary artifacts used for building the export
    and should not persist after the export is complete.
    """
    from django.conf import settings

    # Intermediate folders to clean up
    folders_to_delete = [
        os.path.join(settings.BASE_DIR, 'export', 'database_export_ldm'),
        os.path.join(settings.BASE_DIR, 'joins_configuration'),
    ]

    for folder in folders_to_delete:
        if os.path.exists(folder):
            try:
                shutil.rmtree(folder)
                logger.info(f"Cleaned up intermediate folder: {folder}")
            except Exception as e:
                logger.warning(f"Failed to clean up {folder}: {e}")


logger.level = logging.DEBUG

# Timeout settings: (connect_timeout, read_timeout) in seconds
DEFAULT_TIMEOUT = (10, 30)  # 10s connect, 30s read for normal requests
LARGE_FILE_TIMEOUT = (30, 600)  # 30s connect, 10min read for large file uploads

# Compression threshold in MB - files larger than this will be zipped before upload
COMPRESSION_THRESHOLD_MB = 10.0


class GitHubIntegrationService:
    """Service class for GitHub integration operations including pushing CSV files and creating pull requests."""

    def __init__(self, github_token: str = None):
        """
        Initialize the GitHub integration service.

        Args:
            github_token (str, optional): GitHub personal access token
        """
        self.github_token = github_token or os.getenv('GITHUB_TOKEN')
        if not self.github_token:
            raise ValueError("GitHub token is required. Set GITHUB_TOKEN environment variable or provide token parameter.")

        # Use a session for connection pooling
        self.session = requests.Session()
        self.session.headers.update(self._get_headers())

    def _get_headers(self):
        """Get headers for GitHub API requests."""
        return {
            'Authorization': f'token {self.github_token}',
            'Accept': 'application/vnd.github.v3+json',
            'Content-Type': 'application/json'
        }

    def _request_with_retry(self, method: str, url: str, max_retries: int = 3,
                            timeout=None, **kwargs):
        """
        Make an HTTP request with retry logic and exponential backoff.

        Args:
            method: HTTP method ('get', 'post', 'patch', etc.)
            url: Request URL
            max_retries: Maximum number of retry attempts
            timeout: Request timeout (connect, read) tuple
            **kwargs: Additional arguments passed to requests

        Returns:
            requests.Response object

        Raises:
            requests.RequestException on final failure
        """
        if timeout is None:
            timeout = DEFAULT_TIMEOUT

        last_exception = None
        for attempt in range(max_retries):
            try:
                response = getattr(self.session, method)(url, timeout=timeout, **kwargs)

                # Don't retry on client errors (4xx) except rate limiting (429)
                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 60))
                    logger.warning(f"Rate limited, waiting {retry_after}s before retry...")
                    time.sleep(retry_after)
                    continue

                return response

            except requests.exceptions.Timeout as e:
                last_exception = e
                wait_time = (2 ** attempt) * 5  # 5s, 10s, 20s
                logger.warning(f"Request timeout (attempt {attempt + 1}/{max_retries}), "
                             f"retrying in {wait_time}s... URL: {url}")
                if attempt < max_retries - 1:
                    time.sleep(wait_time)

            except requests.exceptions.ConnectionError as e:
                last_exception = e
                wait_time = (2 ** attempt) * 5
                logger.warning(f"Connection error (attempt {attempt + 1}/{max_retries}), "
                             f"retrying in {wait_time}s... URL: {url}")
                if attempt < max_retries - 1:
                    time.sleep(wait_time)

            except requests.exceptions.RequestException as e:
                last_exception = e
                logger.error(f"Request failed (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(5)

        # All retries exhausted
        raise last_exception or requests.exceptions.RequestException(
            f"Request failed after {max_retries} attempts")

    def _parse_github_url(self, repository_url: str):
        """
        Parse GitHub repository URL to extract owner and repo.

        Args:
            repository_url (str): GitHub repository URL

        Returns:
            tuple: (owner, repo) or (None, None) if parsing fails
        """
        try:
            normalized_url = repository_url.rstrip('/')
            if normalized_url.endswith('.git'):
                normalized_url = normalized_url[:-4]

            parts = normalized_url.replace('https://github.com/', '').split('/')
            if len(parts) >= 2:
                return parts[0], parts[1]
            return None, None
        except Exception as e:
            logger.error(f"Error parsing GitHub URL {repository_url}: {e}")
            return None, None

    def get_github_url_from_automode_config(self):
        """
        Get the active automode configuration from the database.

        Returns:
            str: The technical export GitHub URL from config
        """
        import json
        with open("automode_config.json", "r") as f:
            config = json.load(f)
            return config["technical_export_github_url"]

    def create_branch(self, owner: str, repo: str, branch_name: str, base_branch: str = 'main'):
        """
        Create a new branch in the repository.

        Args:
            owner (str): Repository owner
            repo (str): Repository name
            branch_name (str): Name of the new branch
            base_branch (str): Base branch to create from (default: 'main')

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Get the SHA of the base branch
            base_url = f"https://api.github.com/repos/{owner}/{repo}/git/refs/heads/{base_branch}"
            response = self._request_with_retry('get', base_url)

            if response.status_code != 200:
                logger.error(f"Failed to get base branch {base_branch}: {response.status_code}")
                return False

            base_sha = response.json()['object']['sha']

            # Create the new branch
            create_url = f"https://api.github.com/repos/{owner}/{repo}/git/refs"
            data = {
                'ref': f'refs/heads/{branch_name}',
                'sha': base_sha
            }

            response = self._request_with_retry('post', create_url, json=data)

            if response.status_code == 201:
                logger.info(f"Successfully created branch {branch_name}")
                return True
            else:
                logger.error(f"Failed to create branch {branch_name}: {response.status_code} - {response.text}")
                return False

        except Exception as e:
            logger.error(f"Error creating branch {branch_name}: {e}")
            return False

    def fork_repository(self, source_owner: str, source_repo: str, organization: str = ""):
        """
        Fork a repository to the authenticated user's account or organization.

        Args:
            source_owner (str): Owner of the source repository
            source_repo (str): Name of the source repository
            organization (str, optional): Organization to fork to (if None, forks to user account)

        Returns:
            tuple: (success: bool, fork_data: dict or None)
        """
        try:
            # Check if fork already exists
            fork_owner = organization if organization else self._get_authenticated_user()
            logger.debug(f"fork owner: {fork_owner}")
            if not fork_owner:
                logger.error("Could not determine fork owner")
                return False, None

            check_url = f"https://api.github.com/repos/{fork_owner}/{source_repo}"
            check_response = self._request_with_retry('get', check_url)
            logger.debug(f"check_url: {check_url}")
            try:
                import json
                is_fork = json.loads(check_response.text).get("fork", False)
            except:
                is_fork = False

            if check_response.status_code == 200 and is_fork:
                logger.info(f"Fork already exists: {fork_owner}/{source_repo}")
                return True, check_response.json()

            # Create the fork
            fork_url = f"https://api.github.com/repos/{source_owner}/{source_repo}/forks"
            logger.debug(f"fork_url: {fork_url}")
            data = {}
            if organization:
                data['organization'] = organization

            response = self._request_with_retry('post', fork_url, json=data)

            if response.status_code in [202, 201]:
                logger.debug(f"fork creation response.status_code: {response.status_code}")
                fork_data = response.json()
                logger.info(f"Successfully created fork: {fork_data['full_name']}")
                import json
                os.makedirs(os.path.dirname(f"{fork_data['full_name']}.json"), exist_ok=True)
                with open(f"{fork_data['full_name']}.json", "w") as f:
                    json.dump(fork_data, f)
                return True, fork_data
            else:
                logger.error(f"Failed to create fork: {response.status_code} - {response.text}")
                return False, None

        except Exception as e:
            logger.error(f"Error forking repository {source_owner}/{source_repo}: {e}")
            return False, None

    def _get_authenticated_user(self):
        """Get the authenticated user's username."""
        try:
            user_url = "https://api.github.com/user"
            response = self._request_with_retry('get', user_url)

            if response.status_code == 200:
                return response.json()['login']
            else:
                logger.error(f"Failed to get authenticated user: {response.status_code}")
                return None
        except Exception as e:
            logger.error(f"Error getting authenticated user: {e}")
            return None

    def _compress_if_large(self, content: bytes, filename: str, threshold_mb: float = COMPRESSION_THRESHOLD_MB):
        """
        Compress file content if it exceeds the size threshold.

        Args:
            content: Raw file content as bytes
            filename: Original filename
            threshold_mb: Size threshold in MB above which to compress

        Returns:
            tuple: (content, filename, was_compressed)
        """
        threshold_bytes = int(threshold_mb * 1024 * 1024)

        if len(content) <= threshold_bytes:
            return content, filename, False

        original_size_mb = len(content) / (1024 * 1024)
        logger.info(f"Compressing {filename} ({original_size_mb:.2f} MB > {threshold_mb} MB threshold)")

        # Compress the content using ZIP
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED, compresslevel=9) as zf:
            zf.writestr(filename, content)

        compressed_content = zip_buffer.getvalue()
        compressed_filename = f"{filename}.zip"

        compressed_size_mb = len(compressed_content) / (1024 * 1024)
        compression_ratio = (1 - compressed_size_mb / original_size_mb) * 100

        logger.info(f"Compressed {filename}: {original_size_mb:.2f} MB -> {compressed_size_mb:.2f} MB "
                   f"({compression_ratio:.1f}% reduction)")

        return compressed_content, compressed_filename, True

    def create_repository(self, repo_name: str, description: str = "", private: bool = False, organization: str = ""):
        """
        Create a new GitHub repository for the authenticated user or organization.

        Args:
            repo_name (str): Name of the repository to create
            description (str): Repository description
            private (bool): Whether the repository should be private
            organization (str, optional): Organization to create repo in

        Returns:
            tuple: (success: bool, repo_data: dict or None, error_message: str or None)
        """
        try:
            if organization:
                create_url = f"https://api.github.com/orgs/{organization}/repos"
            else:
                create_url = "https://api.github.com/user/repos"

            data = {
                'name': repo_name,
                'description': description,
                'private': private,
                'auto_init': True,
            }

            response = self._request_with_retry('post', create_url, json=data)

            if response.status_code == 201:
                repo_data = response.json()
                logger.info(f"Successfully created repository: {repo_data['full_name']}")
                return True, repo_data, None
            elif response.status_code == 422:
                error_data = response.json()
                error_msg = error_data.get('message', 'Repository creation failed')
                if 'errors' in error_data:
                    error_details = [e.get('message', str(e)) for e in error_data['errors']]
                    error_msg = f"{error_msg}: {', '.join(error_details)}"
                logger.error(f"Failed to create repository: {error_msg}")
                return False, None, error_msg
            else:
                error_msg = f"Failed to create repository: {response.status_code} - {response.text}"
                logger.error(error_msg)
                return False, None, error_msg

        except Exception as e:
            error_msg = f"Error creating repository {repo_name}: {e}"
            logger.error(error_msg)
            return False, None, error_msg

    def wait_for_fork_completion(self, owner: str, repo: str, max_attempts: int = 30):
        """
        Wait for fork to be ready (GitHub forks can take time to complete).

        Args:
            owner (str): Fork owner
            repo (str): Fork repository name
            max_attempts (int): Maximum attempts to check fork status

        Returns:
            bool: True if fork is ready, False otherwise
        """
        import time

        logger.info(f"Waiting for fork {owner}/{repo} to be ready...")

        for attempt in range(max_attempts):
            try:
                check_url = f"https://api.github.com/repos/{owner}/{repo}"
                response = self._request_with_retry('get', check_url, max_retries=1)

                if response.status_code == 200:
                    branches_url = f"https://api.github.com/repos/{owner}/{repo}/branches"
                    branches_response = self._request_with_retry('get', branches_url, max_retries=1)

                    if branches_response.status_code == 200:
                        logger.info(f"Fork {owner}/{repo} is ready")
                        return True

                logger.debug(f"Fork not ready yet, attempt {attempt + 1}/{max_attempts}")

                if attempt < max_attempts - 1:
                    time.sleep(2)

            except Exception as e:
                logger.error(f"Error checking fork status: {e}")
                if attempt < max_attempts - 1:
                    time.sleep(2)

        logger.error(f"Fork {owner}/{repo} did not become ready after {max_attempts} attempts")
        return False

    def push_csv_files(self, owner: str, repo: str, branch_name: str, csv_directory: str,
                        pipeline: str = None, framework_ids: list = None):
        """
        Push CSV files to a GitHub repository branch using bulk upload (single commit).

        Args:
            owner (str): Repository owner
            repo (str): Repository name
            branch_name (str): Branch to push to
            csv_directory (str): Local directory containing CSV files
            pipeline (str, optional): Pipeline name to filter filter_code by type.
                                     If provided, only exports 'datasets' for ancrdt
                                     or 'templates' for dpm/main pipelines.
            framework_ids (list, optional): List of framework IDs to filter filter_code.
                                           Only exports files for matching frameworks.

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            import base64
            import glob
            from pybirdai.services.pipeline_repo_service import PipelineRepoService

            csv_files = glob.glob(os.path.join(csv_directory, '*.csv'))

            export_base_dir = os.path.dirname(os.path.dirname(csv_directory))

            # Also collect process_metadata.json
            for check_dir in [csv_directory, export_base_dir]:
                metadata_file = os.path.join(check_dir, 'process_metadata.json')
                if os.path.exists(metadata_file):
                    csv_files.append(metadata_file)
                    logger.info(f"Found process_metadata.json at {check_dir}")
                    break

            # Collect joins_configuration CSVs
            joins_config_dir = os.path.join(export_base_dir, 'joins_configuration')
            if os.path.exists(joins_config_dir):
                joins_csv_files = glob.glob(os.path.join(joins_config_dir, '*.csv'))
                csv_files.extend(joins_csv_files)
                logger.info(f"Found {len(joins_csv_files)} join configuration files to push from {joins_config_dir}")

            # Collect filter_code Python files (recursive, pipeline and framework aware)
            # Filter code is in the project directory, not the export directory
            from django.conf import settings
            filter_code_dir = os.path.join(settings.BASE_DIR, 'pybirdai', 'process_steps', 'filter_code')
            if os.path.exists(filter_code_dir):
                # Determine which code type to export based on pipeline
                if pipeline:
                    code_type = PipelineRepoService.get_code_type_for_pipeline(pipeline)
                    logger.info(f"Pipeline '{pipeline}' -> exporting '{code_type}' filter code only")

                    # Normalize framework IDs to uppercase without _REF suffix for directory matching
                    normalized_frameworks = []
                    if framework_ids:
                        for fw in framework_ids:
                            # Remove common prefixes/suffixes and normalize
                            fw_upper = fw.upper().replace('_REF', '').replace('EBA_', '')
                            if fw_upper not in normalized_frameworks:
                                normalized_frameworks.append(fw_upper)
                        logger.info(f"Filtering for frameworks: {normalized_frameworks}")

                    # Only collect files from the appropriate type directory
                    type_dir = os.path.join(filter_code_dir, code_type)
                    if os.path.exists(type_dir):
                        if normalized_frameworks:
                            # Only collect files from specific framework subdirectories
                            filter_py_files = []
                            for fw in normalized_frameworks:
                                fw_dir = os.path.join(type_dir, fw)
                                if os.path.exists(fw_dir):
                                    fw_files = glob.glob(os.path.join(fw_dir, '**', '*.py'), recursive=True)
                                    filter_py_files.extend(fw_files)
                                    logger.info(f"Found {len(fw_files)} filter code files from {code_type}/{fw}/")
                            csv_files.extend(filter_py_files)
                            logger.info(f"Total: {len(filter_py_files)} filter code files from selected frameworks")
                        else:
                            # No framework filter - collect all from type directory
                            filter_py_files = glob.glob(os.path.join(type_dir, '**', '*.py'), recursive=True)
                            csv_files.extend(filter_py_files)
                            logger.info(f"Found {len(filter_py_files)} filter code files from {code_type}/ to push")

                    # Also collect lib files (shared utilities)
                    lib_dir = os.path.join(filter_code_dir, 'lib')
                    if os.path.exists(lib_dir):
                        lib_py_files = glob.glob(os.path.join(lib_dir, '**', '*.py'), recursive=True)
                        csv_files.extend(lib_py_files)
                        logger.info(f"Found {len(lib_py_files)} shared lib files to push")
                else:
                    # No pipeline specified - collect all .py files (legacy behavior)
                    filter_py_files = glob.glob(os.path.join(filter_code_dir, '**', '*.py'), recursive=True)
                    csv_files.extend(filter_py_files)
                    logger.info(f"Found {len(filter_py_files)} filter code files to push from {filter_code_dir}")

            FILES_NOT_TO_PUSH = [
                "workflowtaskexecution.csv",
                "workflowsession.csv",
                "workflowtaskdependency.csv",
                "automodeconfiguration.csv"
            ]

            files_to_push = []
            for csv_file in csv_files:
                file_name = os.path.basename(csv_file)
                if file_name not in FILES_NOT_TO_PUSH:
                    files_to_push.append(csv_file)

            if not files_to_push:
                logger.warning(f"No files to push found in export directory: {csv_directory}")
                return False

            logger.info(f"Found {len(files_to_push)} files to push using bulk upload")

            # Step 1: Get the current commit SHA for the branch
            logger.info(f"Getting current commit SHA for branch {branch_name}")
            ref_url = f"https://api.github.com/repos/{owner}/{repo}/git/refs/heads/{branch_name}"
            ref_response = self._request_with_retry('get', ref_url)

            if ref_response.status_code != 200:
                logger.error(f"Failed to get branch reference: {ref_response.status_code} - {ref_response.text}")
                return False

            current_commit_sha = ref_response.json()['object']['sha']
            logger.info(f"Current commit SHA: {current_commit_sha}")

            # Step 2: Get the base tree SHA from the current commit
            commit_url = f"https://api.github.com/repos/{owner}/{repo}/git/commits/{current_commit_sha}"
            commit_response = self._request_with_retry('get', commit_url)

            if commit_response.status_code != 200:
                logger.error(f"Failed to get commit details: {commit_response.status_code} - {commit_response.text}")
                return False

            base_tree_sha = commit_response.json()['tree']['sha']
            logger.info(f"Base tree SHA: {base_tree_sha}")

            # Step 3: Create blobs for all files in parallel
            logger.info(f"Creating blobs for {len(files_to_push)} files in parallel...")
            blobs = []
            errors = []
            max_workers = 10

            def create_blob_for_file(csv_file):
                """Create a blob for a single file. Returns blob dict or raises exception."""
                original_file_name = os.path.basename(csv_file)

                with open(csv_file, 'rb') as f:
                    content = f.read()

                # Compress large files to avoid GitHub API limits
                content, file_name, was_compressed = self._compress_if_large(content, original_file_name)
                content_size_mb = len(content) / (1024 * 1024)

                blob_url = f"https://api.github.com/repos/{owner}/{repo}/git/blobs"
                blob_data = {
                    'content': base64.b64encode(content).decode('utf-8'),
                    'encoding': 'base64'
                }

                # Use longer timeout for large files (> 5MB)
                timeout = LARGE_FILE_TIMEOUT if content_size_mb > 5 else DEFAULT_TIMEOUT

                blob_response = self._request_with_retry('post', blob_url, timeout=timeout, json=blob_data)

                if blob_response.status_code != 201:
                    raise Exception(f"Failed to create blob: {blob_response.status_code} - {blob_response.text[:500]}")

                blob_sha = blob_response.json()['sha']

                # Determine remote path based on file type
                if 'joins_configuration' in csv_file:
                    remote_path = f"export/joins_configuration/{file_name}"
                elif 'filter_code' in csv_file:
                    filter_code_marker = f"filter_code{os.sep}"
                    if filter_code_marker in csv_file:
                        rel_path = csv_file.split(filter_code_marker, 1)[1]
                        rel_path = rel_path.replace(os.sep, '/')
                        remote_path = f"export/filter_code/{rel_path}"
                    else:
                        remote_path = f"export/filter_code/{file_name}"
                elif original_file_name == 'process_metadata.json':
                    remote_path = f"export/{file_name}"
                else:
                    remote_path = f"export/database_export_ldm/{file_name}"

                return {
                    'path': remote_path,
                    'mode': '100644',
                    'type': 'blob',
                    'sha': blob_sha
                }

            # Execute blob creation in parallel
            total_files = len(files_to_push)
            logger.info(f"Uploading {total_files} files in parallel (max_workers={max_workers})")

            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_file = {executor.submit(create_blob_for_file, f): f for f in files_to_push}

                for future in concurrent.futures.as_completed(future_to_file):
                    csv_file = future_to_file[future]
                    try:
                        blob = future.result()
                        blobs.append(blob)
                        logger.info(f"Uploaded {len(blobs)}/{total_files}: {blob['path']}")
                    except Exception as e:
                        errors.append(f"{os.path.basename(csv_file)}: {str(e)}")
                        logger.error(f"Failed to upload {csv_file}: {e}")

            # Check for any errors during parallel upload
            if errors:
                logger.error(f"Failed to upload {len(errors)} files")
                return False

            # Step 4: Create tree with all blobs
            logger.info("Creating tree with all file blobs...")
            tree_url = f"https://api.github.com/repos/{owner}/{repo}/git/trees"
            tree_data = {
                'tree': blobs,
                'base_tree': base_tree_sha
            }

            tree_response = self._request_with_retry('post', tree_url, json=tree_data)

            if tree_response.status_code != 201:
                logger.error(f"Failed to create tree: {tree_response.status_code} - {tree_response.text}")
                return False

            tree_sha = tree_response.json()['sha']
            logger.info(f"Created tree: {tree_sha}")

            # Step 5: Create commit with the tree
            logger.info("Creating commit...")
            commit_url = f"https://api.github.com/repos/{owner}/{repo}/git/commits"
            commit_data = {
                'tree': tree_sha,
                'message': f'PyBIRD AI database export ({len(files_to_push)} files)',
                'parents': [current_commit_sha]
            }

            commit_response = self._request_with_retry('post', commit_url, json=commit_data)

            if commit_response.status_code != 201:
                logger.error(f"Failed to create commit: {commit_response.status_code} - {commit_response.text}")
                return False

            new_commit_sha = commit_response.json()['sha']
            logger.info(f"Created commit: {new_commit_sha}")

            # Step 6: Update branch reference to point to new commit
            logger.info(f"Updating branch {branch_name} to point to new commit...")
            update_ref_data = {
                'sha': new_commit_sha
            }

            update_response = self._request_with_retry('patch', ref_url, json=update_ref_data)

            if update_response.status_code != 200:
                logger.error(f"Failed to update branch reference: {update_response.status_code} - {update_response.text}")
                return False

            logger.info(f"Successfully pushed {len(files_to_push)} files in a single commit")
            return True

        except Exception as e:
            logger.error(f"Error pushing export files: {e}")
            traceback.print_exc()
            return False

    def create_pull_request(self, owner: str, repo: str, branch_name: str, fork_repo: str = None,
                           base_branch: str = 'main', title: str = None, body: str = None,
                           head_owner: str = None):
        """
        Create a pull request for the branch.

        Args:
            owner (str): Repository owner
            repo (str): Repository name
            branch_name (str): Source branch for the pull request
            fork_repo (str, optional): Fork repository name
            base_branch (str): Target branch for the pull request (default: 'main')
            title (str): Pull request title
            body (str): Pull request body
            head_owner (str, optional): Owner of the head repository (for cross-repo PRs)

        Returns:
            tuple: (success: bool, pr_url: str or None)
        """
        try:
            from datetime import datetime

            if not title:
                title = f"PyBIRD AI CSV Export - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"

            if not body:
                body = f"""## Database Export from PyBIRD AI

This pull request contains CSV files exported from the PyBIRD AI database.

### Files Updated:
- Database export CSV files in `export/database_export_ldm/`

### Export Details:
- Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
- Source: PyBIRD AI automated export
- Branch: {branch_name}

### Testing:
- [ ] Verify CSV file integrity
- [ ] Check data completeness
- [ ] Validate against expected schema

This export was generated automatically by PyBIRD AI's database export functionality."""

            pr_url = f"https://api.github.com/repos/{owner}/{repo}/pulls"

            if head_owner and head_owner != owner:
                head = f"{head_owner}:{branch_name}"
            else:
                head = branch_name

            logger.info(f"PR to head -> {head}")

            data = {
                'title': title,
                'body': body,
                'head': head,
                'base': base_branch
            }
            if fork_repo and fork_repo != repo:
                data['head_repo'] = f"{head_owner}/{fork_repo}"

            response = self._request_with_retry('post', pr_url, json=data)

            if response.status_code == 201:
                pr_data = response.json()
                pr_html_url = pr_data['html_url']
                logger.info(f"Successfully created pull request: {pr_html_url}")
                return True, pr_html_url
            else:
                logger.error(f"Failed to create pull request: {response.status_code} - {response.text}")
                return False, None

        except Exception as e:
            logger.error(f"Error creating pull request: {e}")
            return False, None

    def create_cross_fork_pull_request(self, source_owner: str, source_repo: str, fork_repo: str,
                                       fork_owner: str, branch_name: str,
                                       base_branch: str = 'develop',
                                       title: str = None, body: str = None):
        """
        Create a pull request from a fork to the upstream repository.

        Args:
            source_owner (str): Original repository owner
            source_repo (str): Original repository name
            fork_repo (str): Fork repository name
            fork_owner (str): Fork owner (user or organization)
            branch_name (str): Branch in the fork with changes
            base_branch (str): Target branch in upstream (default: 'develop')
            title (str, optional): PR title
            body (str, optional): PR body

        Returns:
            tuple: (success: bool, pr_url: str or None)
        """
        logger.info(f"Creating cross-fork PR from {fork_owner}/{source_repo}:{branch_name} to {source_owner}/{source_repo}:{base_branch}")

        return self.create_pull_request(
            owner=source_owner,
            repo=source_repo,
            fork_repo=fork_repo,
            branch_name=branch_name,
            base_branch=base_branch,
            title=title,
            body=body,
            head_owner=fork_owner
        )

    def export_and_push_to_github(self, branch_name: str = "", repository_url: str = "",
                                    framework_ids=None, pipeline: str = None):
        """
        Complete workflow: export database to CSV and push to GitHub with PR.

        Args:
            branch_name (str, optional): Custom branch name
            repository_url (str, optional): GitHub repository URL
            framework_ids (list, optional): List of framework IDs to filter export
            pipeline (str, optional): Pipeline name to filter filter_code by type.
                                     If not provided, auto-detects from framework_ids.

        Returns:
            dict: Results of the operation
        """
        from datetime import datetime
        from pybirdai.views.core.export_db import _export_database_to_csv_logic

        results = {
            'success': False,
            'csv_exported': False,
            'branch_created': False,
            'files_pushed': False,
            'pr_created': False,
            'pr_url': None,
            'error': None
        }

        try:
            if not repository_url:
                repository_url = self.get_github_url_from_automode_config()
                if not repository_url:
                    from pybirdai.services.pipeline_repo_service import get_configured_pipeline_url
                    repository_url = get_configured_pipeline_url('main')
                if not repository_url:
                    results['error'] = "No repository URL configured. Please configure it in the dashboard settings."
                    return results

            owner, repo = self._parse_github_url(repository_url)
            if not owner or not repo:
                results['error'] = f"Invalid GitHub repository URL: {repository_url}"
                return results

            if not branch_name:
                timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')
                branch_name = f"csv-export-{timestamp}"

            logger.info(f"Starting export and push to {owner}/{repo} on branch {branch_name}")

            # Step 1: Export database to CSV
            logger.info("Exporting database to CSV...")
            zip_file_path, extract_dir = _export_database_to_csv_logic(framework_ids=framework_ids)
            results['csv_exported'] = True
            logger.info(f"CSV export completed: {extract_dir}")

            # Step 2: Create branch
            logger.info(f"Creating branch {branch_name}...")
            if self.create_branch(owner, repo, branch_name):
                results['branch_created'] = True
                logger.info(f"Branch {branch_name} created successfully")
            else:
                results['error'] = f"Failed to create branch {branch_name} :: " + traceback.format_exc()
                return results

            # Auto-detect pipeline from framework_ids if not provided
            if not pipeline and framework_ids:
                from pybirdai.context.framework_config import get_pipeline_for_frameworks
                pipeline = get_pipeline_for_frameworks(framework_ids)
                logger.info(f"Auto-detected pipeline: {pipeline} from frameworks: {framework_ids}")

            # Step 3: Push CSV files
            logger.info("Pushing CSV files to GitHub...")
            if self.push_csv_files(owner, repo, branch_name, extract_dir,
                                   pipeline=pipeline, framework_ids=framework_ids):
                results['files_pushed'] = True
                logger.info("CSV files pushed successfully")
            else:
                results['error'] = "Failed to push CSV files"
                return results

            # Step 4: Create pull request
            logger.info(f"Creating pull request for {owner}/{repo} on branch {branch_name}")
            pr_success, pr_url = self.create_pull_request(owner, repo, branch_name)
            if pr_success:
                results['pr_created'] = True
                results['pr_url'] = pr_url
                logger.info(f"Pull request created: {pr_url}")
            else:
                results['error'] = "Failed to create pull request"
                return results

            results['success'] = True
            logger.info("Export and push to GitHub completed successfully")

            # Clean up intermediate export folders
            _cleanup_export_intermediates()

        except Exception as e:
            traceback.print_exc()
            results['error'] = f"Unexpected error: {str(e)}"
            logger.error(f"Error in export_and_push_to_github: {e}")

        return results

    def fork_and_create_pr_workflow(self, source_repository_url: str,
                                    target_repository_url: str = "",
                                    organization: str = "",
                                    branch_name: str = "",
                                    csv_directory: str = "",
                                    pr_title: str = "",
                                    pr_body: str = "",
                                    target_branch: str = 'main',
                                    pipeline: str = None,
                                    framework_ids: list = None):
        """
        Complete workflow: Fork repo, create branch, push changes, create PR.

        Args:
            source_repository_url (str): Source GitHub repository to fork from
            target_repository_url (str, optional): Target repository for PR
            organization (str, optional): Organization to fork to
            branch_name (str, optional): Branch name for changes
            csv_directory (str, optional): Directory with files to push
            pr_title (str, optional): Pull request title
            pr_body (str, optional): Pull request body
            target_branch (str): Target branch for PR (default: 'main')
            pipeline (str, optional): Pipeline name to filter filter_code by type
            framework_ids (list, optional): List of framework IDs to filter filter_code

        Returns:
            dict: Results of the operation
        """
        from datetime import datetime

        results = {
            'success': False,
            'fork_created': False,
            'fork_data': None,
            'branch_created': False,
            'files_pushed': False,
            'pr_created': False,
            'pr_url': None,
            'error': None
        }

        try:
            source_owner, source_repo = self._parse_github_url(source_repository_url)
            if not source_owner or not source_repo:
                results['error'] = f"Invalid source repository URL: {source_repository_url}"
                return results

            if not target_repository_url:
                target_repository_url = source_repository_url

            target_owner, target_repo = self._parse_github_url(target_repository_url)
            if not target_owner or not target_repo:
                results['error'] = f"Invalid target repository URL: {target_repository_url}"
                return results

            if not branch_name:
                timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')
                branch_name = f"pybird-export-{timestamp}"

            logger.info(f"Starting fork and PR workflow from {source_owner}/{source_repo}")

            # Step 1: Fork the repository
            logger.info(f"Forking repository {source_owner}/{source_repo}...")
            fork_success, fork_data = self.fork_repository(source_owner, source_repo, organization)
            if not fork_success:
                results['error'] = "Failed to fork repository"
                return results

            results['fork_created'] = True
            results['fork_data'] = fork_data

            fork_owner = fork_data['owner']['login']
            head_repo = fork_data['name']
            logger.info(f"Fork created/found: {fork_owner}/{head_repo}")

            # Step 2: Wait for fork to be ready
            if not self.wait_for_fork_completion(fork_owner, head_repo):
                results['error'] = "Fork did not become ready in time"
                return results

            # Step 3: Create branch in fork
            logger.info(f"Creating branch {branch_name} in fork...")
            if self.create_branch(fork_owner, head_repo, branch_name):
                results['branch_created'] = True
                logger.info(f"Branch {branch_name} created successfully")
            else:
                results['error'] = f"Failed to create branch {branch_name}"
                return results

            # Step 4: Push files if directory provided
            if csv_directory:
                logger.info("Pushing files to fork...")
                if self.push_csv_files(fork_owner, head_repo, branch_name, csv_directory,
                                       pipeline=pipeline, framework_ids=framework_ids):
                    results['files_pushed'] = True
                    logger.info("Files pushed successfully")
                else:
                    results['error'] = "Failed to push files"
                    return results

            # Step 5: Create pull request
            logger.info(f"Creating pull request to {target_owner}/{target_repo}:{target_branch}")

            if not pr_body:
                pr_body = f"""## PyBIRD AI Export

This pull request was created automatically by PyBIRD AI's fork workflow.

### Details:
- Forked from: {source_owner}/{source_repo}
- Fork location: {fork_owner}/{head_repo}
- Branch: {branch_name}
- Target: {target_owner}/{target_repo}:{target_branch}
- Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

### Changes:
- Database export files in `export/database_export_ldm/`

This export was generated automatically by PyBIRD AI's database export functionality."""

            pr_success, pr_url = self.create_cross_fork_pull_request(
                source_owner=target_owner,
                source_repo=target_repo,
                fork_repo=head_repo,
                fork_owner=fork_owner,
                branch_name=branch_name,
                base_branch=target_branch,
                title=pr_title,
                body=pr_body
            )

            if pr_success:
                results['pr_created'] = True
                results['pr_url'] = pr_url
                logger.info(f"Pull request created: {pr_url}")
            else:
                logger.info(f"Pull request not created, target branch most likely not available. Creating PR on main for : {pr_url}")
                pr_success, pr_url = self.create_cross_fork_pull_request(
                    source_owner=target_owner,
                    source_repo=target_repo,
                    fork_repo=head_repo,
                    fork_owner=fork_owner,
                    branch_name=branch_name,
                    base_branch=DEFAULT_GITHUB_BRANCH,
                    title=pr_title,
                    body=pr_body
                )
                if pr_success:
                    results['pr_created'] = True
                    results['pr_url'] = pr_url
                    logger.info(f"Pull request created: {pr_url}")
                else:
                    results['error'] = "Failed to create pull request"
                    return results

            results['success'] = True
            logger.info("Fork and PR workflow completed successfully")

            # Clean up intermediate export folders
            _cleanup_export_intermediates()

        except Exception as e:
            traceback.print_exc()
            results['error'] = f"Unexpected error: {str(e)}"
            logger.error(f"Error in fork_and_create_pr_workflow: {e}")

        return results
