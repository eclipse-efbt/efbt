# coding=UTF-8
# Copyright (c) 2025 Arfa Digital Consulting
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDX-License-Identifier: EPL-2.0
#
# Contributors:
#    Benjamin Arfa - initial API and implementation
#

import subprocess
import os
import shutil
import logging
import json
import yaml
from pathlib import Path
from typing import Dict, List, Optional, Union
import tempfile
import re

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TestSuiteGitService:
    """
    Service for fetching and managing test suites via git clone without Django management commands.
    Provides pure Python git integration for test suite management.
    """

    def __init__(self, base_path: str = None):
        """
        Initialize the test suite git service.

        Args:
            base_path: Base path for the project (defaults to detect Django project root)
        """
        self.base_path = Path(base_path) if base_path else self._detect_project_root()
        self.tests_dir = self.base_path / "tests"
        self.tests_dir.mkdir(exist_ok=True)

        # Configuration paths
        self.test_repositories_config = self.tests_dir / "test_repositories.json"
        self.main_config_path = self.tests_dir / "configuration_file_tests.json"

    def _detect_project_root(self) -> Path:
        """
        Detect Django project root by looking for manage.py.

        Returns:
            Path to project root
        """
        current_dir = Path.cwd()

        # Look for manage.py in current directory and parents
        for parent in [current_dir] + list(current_dir.parents):
            if (parent / 'manage.py').exists():
                return parent

        # Fallback to current directory
        logger.warning("Could not detect Django project root, using current directory")
        return current_dir

    def _run_git_command(self, command: List[str], cwd: str = None, check: bool = True) -> subprocess.CompletedProcess:
        """
        Run a git command safely.

        Args:
            command: Git command as list of strings
            cwd: Working directory for command
            check: Whether to raise exception on error

        Returns:
            CompletedProcess result
        """
        try:
            result = subprocess.run(
                command,
                cwd=cwd,
                check=check,
                capture_output=True,
                text=True
            )
            if result.stdout:
                logger.debug(f"Git command output: {result.stdout.strip()}")
            return result
        except subprocess.CalledProcessError as e:
            logger.error(f"Git command failed: {' '.join(command)}")
            logger.error(f"Error: {e.stderr}")
            raise

    def clone_repository(self, repo_url: str, destination: str, branch: str = "main") -> bool:
        """
        Clone a git repository.

        Args:
            repo_url: Repository URL to clone
            destination: Destination directory
            branch: Branch to clone (default: main)

        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Cloning repository {repo_url} (branch: {branch}) to {destination}")

            # Remove destination if it exists
            if os.path.exists(destination):
                shutil.rmtree(destination)

            # Clone repository
            cmd = ["git", "clone", "-b", branch, repo_url, destination]
            self._run_git_command(cmd)

            logger.info(f"Successfully cloned repository to {destination}")
            return True

        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to clone repository {repo_url}: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error cloning repository: {e}")
            return False

    def _normalize_suite_name(self, repo_url: str, custom_name: str = None) -> str:
        """
        Generate a normalized suite name from repository URL or custom name.

        Args:
            repo_url: Repository URL
            custom_name: Custom name for the suite

        Returns:
            Normalized suite name
        """
        if custom_name:
            return re.sub(r'[^a-zA-Z0-9_-]', '_', custom_name.lower())

        # Extract repository name from URL
        repo_name = repo_url.rstrip('/').split('/')[-1]
        if repo_name.endswith('.git'):
            repo_name = repo_name[:-4]

        return re.sub(r'[^a-zA-Z0-9_-]', '_', repo_name.lower())

    def _load_suite_manifest(self, suite_path: Path) -> Dict:
        """
        Load suite manifest file (YAML or JSON).

        Args:
            suite_path: Path to test suite directory

        Returns:
            Manifest dictionary
        """
        manifest_files = [
            suite_path / "suite_manifest.yaml",
            suite_path / "suite_manifest.yml",
            suite_path / "suite_manifest.json",
            suite_path / "manifest.yaml",
            suite_path / "manifest.yml",
            suite_path / "manifest.json"
        ]

        for manifest_file in manifest_files:
            if manifest_file.exists():
                try:
                    with open(manifest_file, 'r') as f:
                        if manifest_file.suffix in ['.yaml', '.yml']:
                            return yaml.safe_load(f)
                        else:
                            return json.load(f)
                except Exception as e:
                    logger.warning(f"Failed to load manifest {manifest_file}: {e}")
                    continue

        logger.warning(f"No manifest file found in {suite_path}")
        return {}

    def _install_suite(self, temp_path: str, suite_name: str) -> bool:
        """
        Install a cloned test suite into the tests directory.

        Args:
            temp_path: Temporary path where suite was cloned
            suite_name: Name of the test suite

        Returns:
            True if successful, False otherwise
        """
        try:
            temp_suite_path = Path(temp_path)
            target_suite_path = self.tests_dir / suite_name

            # Remove target if it exists
            if target_suite_path.exists():
                logger.info(f"Removing existing suite at {target_suite_path}")
                shutil.rmtree(target_suite_path)

            # Copy suite to target location
            shutil.copytree(temp_suite_path, target_suite_path)
            logger.info(f"Installed test suite to {target_suite_path}")

            # Load and process suite configuration
            suite_config_path = target_suite_path / "configuration_file_tests.json"
            if suite_config_path.exists():
                self._merge_suite_configuration(suite_config_path, suite_name)
            else:
                logger.warning(f"No configuration file found at {suite_config_path}")

            # Install dependencies if requirements.txt exists
            requirements_file = target_suite_path / "requirements.txt"
            if requirements_file.exists():
                self._install_requirements(requirements_file)

            return True

        except Exception as e:
            logger.error(f"Failed to install suite {suite_name}: {e}")
            return False

    def _merge_suite_configuration(self, suite_config_path: Path, suite_name: str):
        """
        Merge suite configuration into main configuration.

        Args:
            suite_config_path: Path to suite configuration file
            suite_name: Name of the test suite
        """
        try:
            # Load suite configuration
            with open(suite_config_path, 'r') as f:
                suite_config = json.load(f)

            # Load main configuration
            if self.main_config_path.exists():
                with open(self.main_config_path, 'r') as f:
                    main_config = json.load(f)
            else:
                main_config = {'tests': []}

            # Remove existing tests from this suite
            main_config['tests'] = [
                test for test in main_config['tests']
                if test.get('_suite') != suite_name
            ]

            # Add suite tests with suite marker
            suite_tests_added = 0
            for test in suite_config.get('tests', []):
                if '_suite' not in test:
                    test['_suite'] = suite_name
                main_config['tests'].append(test)
                suite_tests_added += 1

            # Write updated main configuration
            with open(self.main_config_path, 'w') as f:
                json.dump(main_config, f, indent=2)

            logger.info(f"Merged {suite_tests_added} tests from {suite_name} into main configuration")

        except Exception as e:
            logger.error(f"Failed to merge configuration for {suite_name}: {e}")

    def _install_requirements(self, requirements_file: Path):
        """
        Install Python requirements for the test suite.

        Args:
            requirements_file: Path to requirements.txt
        """
        try:
            logger.info(f"Installing requirements from {requirements_file}")
            cmd = ["pip", "install", "-r", str(requirements_file)]
            subprocess.run(cmd, check=True, capture_output=True, text=True)
            logger.info("Successfully installed requirements")
        except subprocess.CalledProcessError as e:
            logger.warning(f"Failed to install requirements: {e}")
        except Exception as e:
            logger.warning(f"Unexpected error installing requirements: {e}")

    def fetch_test_suite(self, repo_url: str, suite_name: str = None, branch: str = "main") -> bool:
        """
        Fetch a test suite from a git repository.

        Args:
            repo_url: Repository URL to clone
            suite_name: Custom name for the suite (optional)
            branch: Git branch to clone

        Returns:
            True if successful, False otherwise
        """
        # Normalize suite name
        final_suite_name = self._normalize_suite_name(repo_url, suite_name)
        logger.info(f"Fetching test suite '{final_suite_name}' from {repo_url}")

        # Create temporary directory for cloning
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_suite_path = os.path.join(temp_dir, "suite")

            # Clone repository
            if not self.clone_repository(repo_url, temp_suite_path, branch):
                return False

            # Load manifest to validate suite
            manifest = self._load_suite_manifest(Path(temp_suite_path))
            if manifest:
                logger.info(f"Suite manifest: {manifest.get('name', final_suite_name)} v{manifest.get('version', 'unknown')}")

            # Install suite
            if self._install_suite(temp_suite_path, final_suite_name):
                logger.info(f"Successfully fetched test suite '{final_suite_name}'")
                return True
            else:
                return False

    def list_installed_suites(self) -> List[Dict]:
        """
        List all installed test suites.

        Returns:
            List of suite information dictionaries
        """
        suites = []

        if not self.tests_dir.exists():
            return suites

        for item in self.tests_dir.iterdir():
            if item.is_dir() and not item.name.startswith('.'):
                manifest = self._load_suite_manifest(item)
                suite_info = {
                    'name': item.name,
                    'path': str(item),
                    'manifest': manifest
                }
                suites.append(suite_info)

        return suites

    def remove_test_suite(self, suite_name: str) -> bool:
        """
        Remove an installed test suite.

        Args:
            suite_name: Name of the suite to remove

        Returns:
            True if successful, False otherwise
        """
        try:
            suite_path = self.tests_dir / suite_name

            if not suite_path.exists():
                logger.warning(f"Test suite '{suite_name}' not found")
                return False

            # Remove suite directory
            shutil.rmtree(suite_path)
            logger.info(f"Removed test suite directory: {suite_path}")

            # Clean up main configuration
            if self.main_config_path.exists():
                with open(self.main_config_path, 'r') as f:
                    main_config = json.load(f)

                # Remove tests from this suite
                original_count = len(main_config['tests'])
                main_config['tests'] = [
                    test for test in main_config['tests']
                    if test.get('_suite') != suite_name
                ]
                removed_count = original_count - len(main_config['tests'])

                # Write updated configuration
                with open(self.main_config_path, 'w') as f:
                    json.dump(main_config, f, indent=2)

                logger.info(f"Removed {removed_count} test entries from main configuration")

            logger.info(f"Successfully removed test suite '{suite_name}'")
            return True

        except Exception as e:
            logger.error(f"Failed to remove test suite '{suite_name}': {e}")
            return False

    def load_test_repositories_config(self) -> Dict:
        """
        Load test repositories configuration.

        Returns:
            Configuration dictionary
        """
        if not self.test_repositories_config.exists():
            return {'repositories': {}}

        try:
            with open(self.test_repositories_config, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load test repositories config: {e}")
            return {'repositories': {}}

    def save_test_repositories_config(self, config: Dict):
        """
        Save test repositories configuration.

        Args:
            config: Configuration dictionary
        """
        try:
            with open(self.test_repositories_config, 'w') as f:
                json.dump(config, f, indent=2)
            logger.info("Saved test repositories configuration")
        except Exception as e:
            logger.error(f"Failed to save test repositories config: {e}")

    def register_repository(self, name: str, url: str, branch: str = "main", description: str = ""):
        """
        Register a test repository in the configuration.

        Args:
            name: Repository name
            url: Repository URL
            branch: Git branch
            description: Repository description
        """
        config = self.load_test_repositories_config()

        config['repositories'][name] = {
            'url': url,
            'branch': branch,
            'description': description
        }

        self.save_test_repositories_config(config)
        logger.info(f"Registered repository '{name}': {url}")

    def fetch_from_registry(self, name: str) -> bool:
        """
        Fetch a test suite from the registered repositories.

        Args:
            name: Repository name in the registry

        Returns:
            True if successful, False otherwise
        """
        config = self.load_test_repositories_config()

        if name not in config['repositories']:
            logger.error(f"Repository '{name}' not found in registry")
            logger.info(f"Available repositories: {list(config['repositories'].keys())}")
            return False

        repo_info = config['repositories'][name]
        return self.fetch_test_suite(
            repo_url=repo_info['url'],
            suite_name=name,
            branch=repo_info.get('branch', 'main')
        )


def main():
    """Command line interface for the test suite git service."""
    import argparse

    parser = argparse.ArgumentParser(description='Fetch and manage test suites via git')
    parser.add_argument('--repo', help='Repository URL to fetch')
    parser.add_argument('--name', help='Custom name for the suite')
    parser.add_argument('--branch', default='main', help='Git branch to clone')
    parser.add_argument('--list', action='store_true', help='List installed suites')
    parser.add_argument('--list-repos', action='store_true', help='List registered repositories')
    parser.add_argument('--remove', help='Remove a test suite')
    parser.add_argument('--register', help='Register a repository (use with --repo)')
    parser.add_argument('--fetch-registry', help='Fetch from registered repository')

    args = parser.parse_args()

    service = TestSuiteGitService()

    if args.list:
        suites = service.list_installed_suites()
        print(f"Found {len(suites)} installed test suites:")
        for suite in suites:
            manifest = suite['manifest']
            print(f"  - {suite['name']}: {manifest.get('description', 'No description')}")
            if 'version' in manifest:
                print(f"    Version: {manifest['version']}")
        return

    if args.list_repos:
        config = service.load_test_repositories_config()
        repos = config.get('repositories', {})
        print(f"Found {len(repos)} registered repositories:")
        for name, info in repos.items():
            print(f"  - {name}: {info['url']} (branch: {info.get('branch', 'main')})")
            if info.get('description'):
                print(f"    {info['description']}")
        return

    if args.remove:
        if service.remove_test_suite(args.remove):
            print(f"Successfully removed test suite '{args.remove}'")
        else:
            print(f"Failed to remove test suite '{args.remove}'")
        return

    if args.register and args.repo:
        service.register_repository(
            name=args.register,
            url=args.repo,
            branch=args.branch,
            description=f"Repository at {args.repo}"
        )
        print(f"Registered repository '{args.register}'")
        return

    if args.fetch_registry:
        if service.fetch_from_registry(args.fetch_registry):
            print(f"Successfully fetched test suite '{args.fetch_registry}'")
        else:
            print(f"Failed to fetch test suite '{args.fetch_registry}'")
        return

    if args.repo:
        if service.fetch_test_suite(args.repo, args.name, args.branch):
            print(f"Successfully fetched test suite from {args.repo}")
        else:
            print(f"Failed to fetch test suite from {args.repo}")
        return

    parser.print_help()


if __name__ == '__main__':
    main()