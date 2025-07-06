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

import requests
import json
import logging
import hashlib
import zipfile
import io
import os
from dataclasses import dataclass
from typing import List, Optional, Dict, Any
from collections import defaultdict

logger = logging.getLogger(__name__)

REPO_OWNER = "benjamin-arfa"
REPO_NAME = "database_generator_django_service"
BASELINK = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/actions/artifacts"


class ArtifactFetcherProcessStep:
    """
    Process step for fetching GitHub artifacts and databases.
    Refactored from utils.speed_improvements_initial_migration.artifact_fetcher to follow process step patterns.
    """
    
    def __init__(self, context=None):
        """
        Initialize the artifact fetcher process step.
        
        Args:
            context: The context object containing configuration settings.
        """
        self.context = context
        
    def execute(self, operation: str = "fetch_database", 
                token: str = None, **kwargs) -> Dict[str, Any]:
        """
        Execute artifact fetching operations.
        
        Args:
            operation (str): Operation type - "fetch_database", "get_artifacts", "download_artifact", "extract_zip"
            token (str): GitHub personal access token
            **kwargs: Additional parameters for specific operations
            
        Returns:
            dict: Result dictionary with success status and details
        """
        try:
            if not token:
                raise ValueError("GitHub token is required for artifact fetcher operations")
            
            if operation == "fetch_database":
                bird_data_model_path = kwargs.get('bird_data_model_path', f"pybirdai{os.sep}bird_data_model.py")
                bird_meta_data_model_path = kwargs.get('bird_meta_data_model_path', f"pybirdai{os.sep}bird_meta_data_model.py")
                repo_url = kwargs.get('repo_url', BASELINK)
                
                fetcher = PreconfiguredDatabaseFetcher(token, repo_url)
                result = fetcher.fetch(bird_data_model_path, bird_meta_data_model_path)
                
                return {
                    'success': result is not None,
                    'operation': 'fetch_database',
                    'database_found': result is not None,
                    'database_size': len(result) if result else 0,
                    'message': 'Database fetched successfully' if result else 'No matching database found'
                }
            
            elif operation == "get_artifacts":
                repo_url = kwargs.get('repo_url', BASELINK)
                
                fetcher = ArtifactFetcher(token)
                artifacts = fetcher.get_artifacts(repo_url)
                
                return {
                    'success': True,
                    'operation': 'get_artifacts',
                    'artifacts_count': len(artifacts),
                    'artifacts': [{'id': a.id, 'name': a.name, 'size': a.size_in_bytes} for a in artifacts],
                    'message': f'Retrieved {len(artifacts)} artifacts'
                }
            
            elif operation == "download_artifact":
                artifact_data = kwargs.get('artifact_data')
                if not artifact_data:
                    raise ValueError("artifact_data is required for download_artifact operation")
                
                fetcher = ArtifactFetcher(token)
                # Convert dict back to Artifact object if needed
                if isinstance(artifact_data, dict):
                    artifact = Artifact(**artifact_data)
                else:
                    artifact = artifact_data
                
                content = fetcher.download_artifact_zip(artifact)
                
                return {
                    'success': True,
                    'operation': 'download_artifact',
                    'artifact_name': artifact.name,
                    'content_size': len(content),
                    'content': content,
                    'message': f'Downloaded artifact {artifact.name}'
                }
            
            elif operation == "extract_zip":
                zip_content = kwargs.get('zip_content')
                if not zip_content:
                    raise ValueError("zip_content is required for extract_zip operation")
                
                fetcher = ArtifactFetcher(token)
                files = fetcher.extract_zip_files_in_memory(zip_content)
                
                return {
                    'success': True,
                    'operation': 'extract_zip',
                    'files_count': len(files),
                    'files': list(files.keys()),
                    'extracted_files': files,
                    'message': f'Extracted {len(files)} files from ZIP'
                }
            
            else:
                raise ValueError(f"Unknown operation: {operation}")
            
            if self.context:
                self.context.artifact_fetcher = fetcher
                
        except Exception as e:
            logger.error(f"Failed to execute artifact fetcher: {e}")
            return {
                'success': False,
                'error': str(e),
                'message': 'Artifact fetcher operation failed'
            }


@dataclass
class WorkflowRun:
    """Represents a GitHub workflow run."""
    id: int
    repository_id: int
    head_repository_id: int
    head_branch: str
    head_sha: str


@dataclass
class Artifact:
    """Represents a GitHub artifact."""
    id: int
    node_id: str
    name: str
    size_in_bytes: int
    url: str
    archive_download_url: str
    expired: bool
    digest: str
    created_at: str
    updated_at: str
    expires_at: str
    workflow_run: WorkflowRun


class ArtifactFetcher:
    """
    Enhanced GitHub artifact fetcher with process step integration.
    Fetches and processes GitHub Actions artifacts.
    """
    
    def __init__(self, token: str):
        """
        Initialize the artifact fetcher.
        
        Args:
            token (str): GitHub personal access token
        """
        self.token = token
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json"
        }
        logger.info("ArtifactFetcher initialized")

    def get_artifacts(self, repo_url: str) -> List[Artifact]:
        """
        Get all artifacts from a GitHub repository.
        
        Args:
            repo_url (str): GitHub API URL for artifacts
            
        Returns:
            list: List of Artifact objects
        """
        logger.info(f"Fetching artifacts from: {repo_url}")
        
        try:
            response = requests.get(repo_url, headers=self.headers)
            response.raise_for_status()
            logger.info(f"API response status: {response.status_code}")

            data = response.json()
            artifacts_count = len(data.get('artifacts', []))
            logger.info(f"Found {artifacts_count} artifacts")

            artifacts = []
            for artifact_data in data.get('artifacts', []):
                workflow_run = WorkflowRun(**artifact_data['workflow_run'])
                artifact = Artifact(
                    workflow_run=workflow_run,
                    **{k: v for k, v in artifact_data.items() if k != 'workflow_run'}
                )
                artifacts.append(artifact)
                logger.debug(f"Processed artifact: {artifact.name} (ID: {artifact.id})")

            return artifacts
            
        except requests.RequestException as e:
            logger.error(f"Failed to fetch artifacts: {e}")
            raise
        except Exception as e:
            logger.error(f"Error processing artifacts: {e}")
            raise

    def download_artifact_zip(self, artifact: Artifact) -> bytes:
        """
        Download artifact as zip using archive_download_url.
        
        Args:
            artifact (Artifact): The artifact to download
            
        Returns:
            bytes: The artifact content as bytes
        """
        logger.info(f"Downloading artifact zip: {artifact.name}")
        logger.debug(f"Download URL: {artifact.archive_download_url}")

        try:
            response = requests.get(artifact.archive_download_url, headers=self.headers)
            response.raise_for_status()
            logger.info(f"Download response status: {response.status_code}, size: {len(response.content)} bytes")
            return response.content
            
        except requests.RequestException as e:
            logger.error(f"Failed to download artifact: {e}")
            raise

    def extract_zip_files_in_memory(self, zip_content: bytes) -> Dict[str, str]:
        """
        Extract all text files from a zip archive in memory and return as dict.
        
        Args:
            zip_content (bytes): The ZIP file content
            
        Returns:
            dict: Dictionary mapping file names to their content
        """
        logger.info("Extracting ZIP files in memory")
        
        files_content = {}
        try:
            with zipfile.ZipFile(io.BytesIO(zip_content)) as zip_file:
                file_list = zip_file.namelist()
                logger.debug(f"Files in zip: {file_list}")

                for file_name in file_list:
                    if not file_name.endswith('/'):  # Skip directories
                        try:
                            with zip_file.open(file_name) as file:
                                content = file.read().decode('utf-8')
                                files_content[file_name] = content
                                logger.debug(f"Extracted file: {file_name} ({len(content)} chars)")
                        except UnicodeDecodeError:
                            # Skip binary files
                            logger.debug(f"Skipping binary file: {file_name}")
                            continue
                        except Exception as e:
                            logger.warning(f"Error extracting file {file_name}: {e}")
                            continue

        except Exception as e:
            logger.error(f"Error extracting zip: {e}")
            raise

        logger.info(f"Extracted {len(files_content)} text files from ZIP")
        return files_content


class PreconfiguredDatabaseFetcher(ArtifactFetcher):
    """
    Enhanced preconfigured database fetcher with process step integration.
    Fetches pre-built databases from GitHub artifacts that match specific model files.
    """
    
    def __init__(self, token: str, repo_url: str = BASELINK):
        """
        Initialize the preconfigured database fetcher.
        
        Args:
            token (str): GitHub personal access token
            repo_url (str): GitHub API URL for artifacts
        """
        super().__init__(token)
        self.repo_url = repo_url
        logger.info(f"PreconfiguredDatabaseFetcher initialized with repo: {repo_url}")

    def _compare_files(self, local_file_path: str, remote_file_content: str) -> bool:
        """
        Compare a local file with a file content from zip archive.
        
        Args:
            local_file_path (str): Path to the local file
            remote_file_content (str): Content of the remote file
            
        Returns:
            bool: True if files match, False otherwise
        """
        logger.debug(f"Comparing local file: {local_file_path}")
        
        try:
            # Read local file
            with open(local_file_path, 'r') as f:
                local_content = f.read()

            match = local_content == remote_file_content
            logger.debug(f"File comparison result for {local_file_path}: {match}")
            return match
        except Exception as e:
            logger.warning(f"Error comparing file {local_file_path}: {e}")
            return False

    def extract_zip_and_save(self, zip_content: bytes, file_path: str = ".") -> bool:
        """
        Extract all files from a zip archive to current directory.
        
        Args:
            zip_content (bytes): The ZIP file content
            file_path (str): Directory to extract to
            
        Returns:
            bool: True if extraction was successful
        """
        logger.info(f"Extracting zip content to: {file_path}")
        
        try:
            with zipfile.ZipFile(io.BytesIO(zip_content)) as zip_file:
                file_list = zip_file.namelist()
                logger.info(f"Extracting {len(file_list)} files: {file_list}")
                zip_file.extractall(file_path)
                logger.info(f"Zip extraction completed successfully to: {file_path} ({os.getcwd()})")
                return True
        except Exception as e:
            logger.error(f"Error extracting zip: {e}")
            return False

    def fetch(self, bird_data_model_path: str = f"pybirdai{os.sep}bird_data_model.py",
              bird_meta_data_model_path: str = f"pybirdai{os.sep}bird_meta_data_model.py") -> Optional[bytes]:
        """
        Fetch the db.sqlite3 artifact if the specified model files match those in the workflow run.
        
        Args:
            bird_data_model_path (str): Path to the bird data model file
            bird_meta_data_model_path (str): Path to the bird meta data model file
            
        Returns:
            bytes or None: Database content if found, None otherwise
        """
        logger.info("Starting database fetch process")
        logger.info(f"Looking for matching files: {bird_data_model_path}, {bird_meta_data_model_path}")

        try:
            artifacts = self.get_artifacts(self.repo_url)

            # Group artifacts by workflow run
            workflow_artifacts = defaultdict(list)
            for artifact in artifacts:
                workflow_artifacts[artifact.workflow_run.id].append(artifact)

            logger.info(f"Found {len(workflow_artifacts)} workflow runs with artifacts")

            # Check each workflow run
            for workflow_id, run_artifacts in workflow_artifacts.items():
                logger.info(f"Checking workflow run: {workflow_id} with {len(run_artifacts)} artifacts")

                # Find the required artifacts in this workflow run
                bird_data_artifact = None
                bird_meta_artifact = None
                db_artifact = None

                for artifact in run_artifacts:
                    if artifact.name == "bird_data_model.py":
                        bird_data_artifact = artifact
                        logger.debug(f"Found bird_data_model.py artifact: {artifact.id}")
                    elif artifact.name == "bird_meta_data_model.py":
                        bird_meta_artifact = artifact
                        logger.debug(f"Found bird_meta_data_model.py artifact: {artifact.id}")
                    elif artifact.name == "db.sqlite3":
                        db_artifact = artifact
                        logger.debug(f"Found db.sqlite3 artifact: {artifact.id}")

                # Skip if we don't have all required artifacts
                if not (bird_data_artifact and bird_meta_artifact and db_artifact):
                    logger.info(f"Workflow {workflow_id} missing required artifacts, skipping")
                    continue

                logger.info(f"Workflow {workflow_id} has all required artifacts, comparing files")

                # Download and compare the model files
                try:
                    # Download and extract bird_data_model.py
                    logger.debug(f"Downloading bird_data_model.py: {bird_data_artifact}")
                    bird_data_zip = self.download_artifact_zip(bird_data_artifact)
                    bird_data_files = self.extract_zip_files_in_memory(bird_data_zip)

                    # Find the actual file content (it might be nested in folders)
                    bird_data_content = None
                    for file_path, content in bird_data_files.items():
                        if file_path.endswith('bird_data_model.py'):
                            bird_data_content = content
                            break

                    if bird_data_content is None:
                        logger.warning(f"bird_data_model.py not found in artifact zip for workflow {workflow_id}")
                        continue

                    data_match = self._compare_files(bird_data_model_path, bird_data_content)

                    # Download and extract bird_meta_data_model.py
                    logger.debug(f"Downloading bird_meta_data_model.py: {bird_meta_artifact}")
                    bird_meta_zip = self.download_artifact_zip(bird_meta_artifact)
                    bird_meta_files = self.extract_zip_files_in_memory(bird_meta_zip)

                    # Find the actual file content (it might be nested in folders)
                    bird_meta_content = None
                    for file_path, content in bird_meta_files.items():
                        if file_path.endswith('bird_meta_data_model.py'):
                            bird_meta_content = content
                            break

                    if bird_meta_content is None:
                        logger.warning(f"bird_meta_data_model.py not found in artifact zip for workflow {workflow_id}")
                        continue

                    meta_match = self._compare_files(bird_meta_data_model_path, bird_meta_content)

                    logger.info(f"File comparison results - data_match: {data_match}, meta_match: {meta_match}")

                    if data_match and meta_match:
                        # Files match, download and return the database
                        logger.info("Files match! Downloading database artifact")
                        db_content = self.download_artifact_zip(db_artifact)
                        logger.info("Database artifact downloaded successfully")
                        return db_content

                except Exception as e:
                    logger.error(f"Error processing workflow {workflow_id}: {e}")
                    continue

            logger.warning("No matching workflow run found")
            return None
            
        except Exception as e:
            logger.error(f"Failed to fetch database: {e}")
            raise

    def get_matching_workflows(self, bird_data_model_path: str, bird_meta_data_model_path: str) -> List[Dict[str, Any]]:
        """
        Get information about workflows that have matching model files.
        
        Args:
            bird_data_model_path (str): Path to the bird data model file
            bird_meta_data_model_path (str): Path to the bird meta data model file
            
        Returns:
            list: List of matching workflow information
        """
        logger.info("Finding matching workflows")
        
        matching_workflows = []
        
        try:
            artifacts = self.get_artifacts(self.repo_url)
            workflow_artifacts = defaultdict(list)
            
            for artifact in artifacts:
                workflow_artifacts[artifact.workflow_run.id].append(artifact)

            for workflow_id, run_artifacts in workflow_artifacts.items():
                # Check if this workflow has all required artifacts
                has_bird_data = any(a.name == "bird_data_model.py" for a in run_artifacts)
                has_bird_meta = any(a.name == "bird_meta_data_model.py" for a in run_artifacts)
                has_database = any(a.name == "db.sqlite3" for a in run_artifacts)
                
                if has_bird_data and has_bird_meta and has_database:
                    workflow_info = {
                        'workflow_id': workflow_id,
                        'artifacts_count': len(run_artifacts),
                        'has_all_required': True,
                        'artifacts': [{'name': a.name, 'size': a.size_in_bytes} for a in run_artifacts]
                    }
                    matching_workflows.append(workflow_info)

            logger.info(f"Found {len(matching_workflows)} workflows with all required artifacts")
            return matching_workflows
            
        except Exception as e:
            logger.error(f"Failed to get matching workflows: {e}")
            raise