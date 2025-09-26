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

import json
import os
import shutil
import subprocess
import logging
from pathlib import Path
from typing import Dict, List, Optional
import tempfile

logger = logging.getLogger(__name__)

class TestSuiteInstaller:
    """
    Utility class for installing test suites from various sources.
    Handles directory structure, configuration merging, and dependency installation.
    """

    def __init__(self, project_root: str = None):
        """
        Initialize the test suite installer.

        Args:
            project_root: Path to project root (auto-detected if not provided)
        """
        self.project_root = Path(project_root) if project_root else self._detect_project_root()
        self.tests_dir = self.project_root / "tests"
        self.tests_dir.mkdir(exist_ok=True)
        self.main_config_path = self.tests_dir / "configuration_file_tests.json"

    def _detect_project_root(self) -> Path:
        """
        Detect project root by looking for manage.py.

        Returns:
            Path to project root
        """
        current_dir = Path.cwd()

        for parent in [current_dir] + list(current_dir.parents):
            if (parent / 'manage.py').exists():
                return parent

        return current_dir

    def install_from_directory(self, source_path: str, suite_name: str = None) -> bool:
        """
        Install a test suite from a local directory.

        Args:
            source_path: Path to source test suite directory
            suite_name: Name for the installed suite

        Returns:
            True if successful, False otherwise
        """
        try:
            source_path = Path(source_path)
            if not source_path.exists() or not source_path.is_dir():
                logger.error(f"Source path does not exist or is not a directory: {source_path}")
                return False

            # Determine suite name
            if not suite_name:
                suite_name = source_path.name

            target_path = self.tests_dir / suite_name
            logger.info(f"Installing test suite '{suite_name}' from {source_path} to {target_path}")

            # Remove existing installation
            if target_path.exists():
                response = input(f"Test suite '{suite_name}' already exists. Overwrite? (y/N): ")
                if response.lower() != 'y':
                    logger.info("Installation cancelled")
                    return False
                shutil.rmtree(target_path)

            # Copy suite directory
            shutil.copytree(source_path, target_path)
            logger.info(f"Copied test suite to {target_path}")

            # Process installation
            return self._post_install_setup(target_path, suite_name)

        except Exception as e:
            logger.error(f"Failed to install test suite from directory: {e}")
            return False

    def install_from_archive(self, archive_path: str, suite_name: str = None) -> bool:
        """
        Install a test suite from a ZIP archive.

        Args:
            archive_path: Path to ZIP archive
            suite_name: Name for the installed suite

        Returns:
            True if successful, False otherwise
        """
        try:
            import zipfile

            archive_path = Path(archive_path)
            if not archive_path.exists():
                logger.error(f"Archive does not exist: {archive_path}")
                return False

            # Determine suite name
            if not suite_name:
                suite_name = archive_path.stem

            logger.info(f"Installing test suite '{suite_name}' from archive {archive_path}")

            with tempfile.TemporaryDirectory() as temp_dir:
                temp_path = Path(temp_dir) / "extracted"
                temp_path.mkdir()

                # Extract archive
                with zipfile.ZipFile(archive_path, 'r') as zip_ref:
                    zip_ref.extractall(temp_path)

                # Find the actual suite directory (may be nested)
                suite_dirs = [d for d in temp_path.iterdir() if d.is_dir()]
                if len(suite_dirs) == 1:
                    actual_suite_path = suite_dirs[0]
                else:
                    actual_suite_path = temp_path

                return self.install_from_directory(str(actual_suite_path), suite_name)

        except Exception as e:
            logger.error(f"Failed to install test suite from archive: {e}")
            return False

    def _post_install_setup(self, suite_path: Path, suite_name: str) -> bool:
        """
        Perform post-installation setup tasks.

        Args:
            suite_path: Path to installed suite
            suite_name: Name of the suite

        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Performing post-installation setup for '{suite_name}'")

            # Process configuration
            config_path = suite_path / "configuration_file_tests.json"
            if config_path.exists():
                self._merge_configuration(config_path, suite_name)
            else:
                logger.warning(f"No configuration file found at {config_path}")

            # Install dependencies
            requirements_path = suite_path / "requirements.txt"
            if requirements_path.exists():
                self._install_requirements(requirements_path)

            # Run suite installer script if it exists
            installer_script = suite_path / "install.py"
            if installer_script.exists():
                logger.info(f"Running suite installer script: {installer_script}")
                try:
                    result = subprocess.run(
                        ["python", str(installer_script)],
                        cwd=str(suite_path),
                        capture_output=True,
                        text=True,
                        check=True
                    )
                    logger.info("Suite installer script completed successfully")
                    if result.stdout:
                        logger.info(f"Installer output: {result.stdout}")
                except subprocess.CalledProcessError as e:
                    logger.warning(f"Suite installer script failed: {e}")
                    if e.stderr:
                        logger.warning(f"Installer error: {e.stderr}")

            # Make test scripts executable
            self._make_scripts_executable(suite_path)

            logger.info(f"Post-installation setup completed for '{suite_name}'")
            return True

        except Exception as e:
            logger.error(f"Post-installation setup failed for '{suite_name}': {e}")
            return False

    def _merge_configuration(self, suite_config_path: Path, suite_name: str):
        """
        Merge suite configuration into main configuration.

        Args:
            suite_config_path: Path to suite configuration file
            suite_name: Name of the test suite
        """
        try:
            logger.info(f"Merging configuration for suite '{suite_name}'")

            # Load suite configuration
            with open(suite_config_path, 'r') as f:
                suite_config = json.load(f)

            # Load main configuration
            if self.main_config_path.exists():
                with open(self.main_config_path, 'r') as f:
                    main_config = json.load(f)
            else:
                main_config = {'tests': []}
                self.main_config_path.parent.mkdir(exist_ok=True)

            # Remove existing tests from this suite
            original_count = len(main_config['tests'])
            main_config['tests'] = [
                test for test in main_config['tests']
                if test.get('_suite') != suite_name
            ]
            removed_count = original_count - len(main_config['tests'])

            # Add suite tests with suite marker
            suite_tests_added = 0
            for test in suite_config.get('tests', []):
                # Ensure suite is marked
                if '_suite' not in test:
                    test['_suite'] = suite_name
                main_config['tests'].append(test)
                suite_tests_added += 1

            # Write updated main configuration
            with open(self.main_config_path, 'w') as f:
                json.dump(main_config, f, indent=2)

            logger.info(f"Configuration merge completed: removed {removed_count}, added {suite_tests_added} tests")

        except Exception as e:
            logger.error(f"Failed to merge configuration for '{suite_name}': {e}")

    def _install_requirements(self, requirements_path: Path):
        """
        Install Python requirements for the test suite.

        Args:
            requirements_path: Path to requirements.txt
        """
        try:
            logger.info(f"Installing requirements from {requirements_path}")

            # Try pip install
            result = subprocess.run(
                ["pip", "install", "-r", str(requirements_path)],
                capture_output=True,
                text=True,
                check=True
            )

            logger.info("Successfully installed requirements")
            if result.stdout:
                logger.debug(f"pip output: {result.stdout}")

        except subprocess.CalledProcessError as e:
            logger.warning(f"Failed to install requirements: {e}")
            if e.stderr:
                logger.warning(f"pip error output: {e.stderr}")
        except FileNotFoundError:
            logger.warning("pip not found - skipping requirements installation")
        except Exception as e:
            logger.warning(f"Unexpected error installing requirements: {e}")

    def _make_scripts_executable(self, suite_path: Path):
        """
        Make test scripts executable (Unix/Linux systems).

        Args:
            suite_path: Path to test suite directory
        """
        try:
            import stat

            script_patterns = ["*.py", "*.sh", "*.bash"]
            scripts_made_executable = 0

            for pattern in script_patterns:
                for script_file in suite_path.rglob(pattern):
                    if script_file.is_file():
                        # Check if file needs to be made executable
                        current_mode = script_file.stat().st_mode
                        if not (current_mode & stat.S_IXUSR):
                            script_file.chmod(current_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
                            scripts_made_executable += 1

            if scripts_made_executable > 0:
                logger.info(f"Made {scripts_made_executable} scripts executable")

        except Exception as e:
            logger.debug(f"Could not make scripts executable: {e}")

    def uninstall_suite(self, suite_name: str) -> bool:
        """
        Uninstall a test suite.

        Args:
            suite_name: Name of the suite to uninstall

        Returns:
            True if successful, False otherwise
        """
        try:
            suite_path = self.tests_dir / suite_name

            if not suite_path.exists():
                logger.warning(f"Test suite '{suite_name}' not found")
                return False

            logger.info(f"Uninstalling test suite '{suite_name}'")

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

            logger.info(f"Successfully uninstalled test suite '{suite_name}'")
            return True

        except Exception as e:
            logger.error(f"Failed to uninstall test suite '{suite_name}': {e}")
            return False

    def list_installed_suites(self) -> List[Dict]:
        """
        List all installed test suites with their information.

        Returns:
            List of dictionaries with suite information
        """
        suites = []

        if not self.tests_dir.exists():
            return suites

        for item in self.tests_dir.iterdir():
            if item.is_dir() and not item.name.startswith('.'):
                suite_info = {
                    'name': item.name,
                    'path': str(item),
                    'config_exists': (item / "configuration_file_tests.json").exists(),
                    'requirements_exists': (item / "requirements.txt").exists(),
                    'installer_exists': (item / "install.py").exists(),
                }

                # Try to load manifest information
                manifest_files = [
                    item / "suite_manifest.yaml",
                    item / "suite_manifest.yml",
                    item / "manifest.yaml",
                    item / "manifest.yml"
                ]

                for manifest_file in manifest_files:
                    if manifest_file.exists():
                        try:
                            import yaml
                            with open(manifest_file, 'r') as f:
                                manifest = yaml.safe_load(f)
                                suite_info['manifest'] = manifest
                                break
                        except:
                            continue

                suites.append(suite_info)

        return suites

    def validate_suite(self, suite_path: str) -> Dict:
        """
        Validate a test suite structure and configuration.

        Args:
            suite_path: Path to test suite directory

        Returns:
            Dictionary with validation results
        """
        suite_path = Path(suite_path)
        results = {
            'valid': True,
            'errors': [],
            'warnings': [],
            'info': []
        }

        try:
            # Check if directory exists
            if not suite_path.exists():
                results['valid'] = False
                results['errors'].append(f"Suite directory does not exist: {suite_path}")
                return results

            if not suite_path.is_dir():
                results['valid'] = False
                results['errors'].append(f"Path is not a directory: {suite_path}")
                return results

            # Check for required files
            config_file = suite_path / "configuration_file_tests.json"
            if not config_file.exists():
                results['valid'] = False
                results['errors'].append("Missing required configuration_file_tests.json")
            else:
                # Validate configuration file
                try:
                    with open(config_file, 'r') as f:
                        config = json.load(f)

                    if 'tests' not in config:
                        results['warnings'].append("Configuration file missing 'tests' key")
                    elif not isinstance(config['tests'], list):
                        results['errors'].append("'tests' in configuration must be a list")
                    elif len(config['tests']) == 0:
                        results['warnings'].append("No tests defined in configuration")
                    else:
                        results['info'].append(f"Found {len(config['tests'])} test configurations")

                except json.JSONDecodeError as e:
                    results['valid'] = False
                    results['errors'].append(f"Invalid JSON in configuration file: {e}")
                except Exception as e:
                    results['warnings'].append(f"Could not read configuration file: {e}")

            # Check for optional files
            requirements_file = suite_path / "requirements.txt"
            if requirements_file.exists():
                results['info'].append("Found requirements.txt")
            else:
                results['info'].append("No requirements.txt found (optional)")

            installer_file = suite_path / "install.py"
            if installer_file.exists():
                results['info'].append("Found install.py script")

            # Check for manifest files
            manifest_files = [
                suite_path / "suite_manifest.yaml",
                suite_path / "suite_manifest.yml",
                suite_path / "manifest.yaml",
                suite_path / "manifest.yml"
            ]

            manifest_found = False
            for manifest_file in manifest_files:
                if manifest_file.exists():
                    manifest_found = True
                    results['info'].append(f"Found manifest: {manifest_file.name}")
                    break

            if not manifest_found:
                results['warnings'].append("No manifest file found (recommended)")

            # Check for test structure
            tests_dir = suite_path / "tests"
            if tests_dir.exists():
                results['info'].append("Found tests/ directory")

                fixtures_dir = tests_dir / "fixtures"
                if fixtures_dir.exists():
                    results['info'].append("Found tests/fixtures/ directory")
                else:
                    results['warnings'].append("No tests/fixtures/ directory found")

            else:
                results['warnings'].append("No tests/ directory found")

        except Exception as e:
            results['valid'] = False
            results['errors'].append(f"Validation error: {e}")

        return results


def main():
    """Command line interface for the test suite installer."""
    import argparse

    parser = argparse.ArgumentParser(description='Install and manage test suites')
    parser.add_argument('--install-dir', help='Install test suite from directory')
    parser.add_argument('--install-archive', help='Install test suite from ZIP archive')
    parser.add_argument('--name', help='Name for the installed suite')
    parser.add_argument('--list', action='store_true', help='List installed test suites')
    parser.add_argument('--uninstall', help='Uninstall a test suite')
    parser.add_argument('--validate', help='Validate a test suite directory')

    args = parser.parse_args()

    installer = TestSuiteInstaller()

    if args.list:
        suites = installer.list_installed_suites()
        print(f"Found {len(suites)} installed test suites:")
        for suite in suites:
            print(f"  - {suite['name']}: {suite['path']}")
            if 'manifest' in suite:
                manifest = suite['manifest']
                print(f"    Description: {manifest.get('description', 'No description')}")
                print(f"    Version: {manifest.get('version', 'Unknown')}")
        return

    if args.validate:
        results = installer.validate_suite(args.validate)
        print(f"Validation results for {args.validate}:")
        print(f"Valid: {results['valid']}")

        if results['errors']:
            print("Errors:")
            for error in results['errors']:
                print(f"  ❌ {error}")

        if results['warnings']:
            print("Warnings:")
            for warning in results['warnings']:
                print(f"  ⚠️  {warning}")

        if results['info']:
            print("Info:")
            for info in results['info']:
                print(f"  ℹ️  {info}")
        return

    if args.uninstall:
        if installer.uninstall_suite(args.uninstall):
            print(f"Successfully uninstalled test suite '{args.uninstall}'")
        else:
            print(f"Failed to uninstall test suite '{args.uninstall}'")
        return

    if args.install_dir:
        if installer.install_from_directory(args.install_dir, args.name):
            print(f"Successfully installed test suite from {args.install_dir}")
        else:
            print(f"Failed to install test suite from {args.install_dir}")
        return

    if args.install_archive:
        if installer.install_from_archive(args.install_archive, args.name):
            print(f"Successfully installed test suite from {args.install_archive}")
        else:
            print(f"Failed to install test suite from {args.install_archive}")
        return

    parser.print_help()


if __name__ == '__main__':
    main()