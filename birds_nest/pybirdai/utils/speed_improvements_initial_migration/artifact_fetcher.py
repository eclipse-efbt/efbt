import requests
import json
import logging
from dataclasses import dataclass
from typing import List, Optional, Dict
import hashlib
import zipfile
import io
from collections import defaultdict
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

REPO_OWNER = "benjamin-arfa"
REPO_NAME = "database_generator_django_service"
BASELINK = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/actions/artifacts"


@dataclass
class WorkflowRun:
    id: int
    repository_id: int
    head_repository_id: int
    head_branch: str
    head_sha: str

@dataclass
class Artifact:
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
    def __init__(self, token: str):
        self.token = token
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json"
        }
        logger.info("ArtifactFetcher initialized")

    def get_artifacts(self, repo_url: str) -> List[Artifact]:
        logger.info(f"Fetching artifacts from: {repo_url}")
        response = requests.get(repo_url, headers=self.headers)
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

    def download_artifact_zip(self, artifact: Artifact) -> bytes:
        """Download artifact as zip using archive_download_url."""
        logger.info(f"Downloading artifact zip: {artifact.name}")
        logger.debug(f"Download URL: {artifact.archive_download_url}")

        response = requests.get(artifact.archive_download_url, headers=self.headers)
        logger.info(f"Download response status: {response.status_code}, size: {len(response.content)} bytes")
        return response.content

    def extract_zip_files_in_memory(self, zip_content: bytes) -> Dict[str, str]:
        """Extract all text files from a zip archive in memory and return as dict."""
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

        return files_content

class PreconfiguredDatabaseFetcher(ArtifactFetcher):
    def __init__(self, token: str, repo_url: str = BASELINK):
        super().__init__(token)
        self.repo_url = repo_url
        logger.info(f"PreconfiguredDatabaseFetcher initialized with repo: {repo_url}")

    def _compare_files(self, local_file_path: str, remote_file_content: str) -> bool:
        """Compare a local file with a file content from zip archive."""
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
        """Extract all files from a zip archive to current directory."""
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
        """
        logger.info("Starting database fetch process")
        logger.info(f"Looking for matching files: {bird_data_model_path}, {bird_meta_data_model_path}")

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
