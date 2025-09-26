#!/usr/bin/env python3
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

"""
Standalone test fetcher script for managing BIRD test suites without Django dependencies.

This script provides a command-line interface for fetching, installing, and managing
test suites via git clone. It can be run independently of Django.

Usage:
    python fetch_tests.py --repo https://github.com/user/test-suite.git
    python fetch_tests.py --list
    python fetch_tests.py --list-repos
    python fetch_tests.py --fetch-registry bird_default_test_suite
"""

import argparse
import logging
import sys
import os
from pathlib import Path

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

def setup_path():
    """Add project paths to sys.path for imports."""
    script_dir = Path(__file__).parent
    project_root = script_dir.parent.parent  # Go up to birds_nest level

    paths_to_add = [
        str(project_root),
        str(script_dir),
    ]

    for path in paths_to_add:
        if path not in sys.path:
            sys.path.insert(0, path)

# Setup paths for imports
setup_path()

# Import our services
try:
    from test_suite_git_service import TestSuiteGitService
    from test_suite_installer import TestSuiteInstaller
except ImportError as e:
    logger.error(f"Could not import required modules: {e}")
    logger.error("Make sure you're running this script from the correct directory")
    sys.exit(1)

class TestFetcher:
    """
    Standalone test fetcher with comprehensive CLI interface.
    """

    def __init__(self):
        """Initialize the test fetcher."""
        self.git_service = TestSuiteGitService()
        self.installer = TestSuiteInstaller()

    def fetch_test_suite(self, repo_url: str, name: str = None, branch: str = "main") -> bool:
        """
        Fetch a test suite from a git repository.

        Args:
            repo_url: Repository URL to clone
            name: Custom name for the suite
            branch: Git branch to clone

        Returns:
            True if successful, False otherwise
        """
        logger.info(f"Fetching test suite from {repo_url}")
        return self.git_service.fetch_test_suite(repo_url, name, branch)

    def fetch_from_registry(self, name: str) -> bool:
        """
        Fetch a test suite from the repository registry.

        Args:
            name: Name of the suite in the registry

        Returns:
            True if successful, False otherwise
        """
        logger.info(f"Fetching test suite '{name}' from registry")
        return self.git_service.fetch_from_registry(name)

    def list_installed_suites(self):
        """List all installed test suites."""
        suites = self.installer.list_installed_suites()

        if not suites:
            print("No test suites installed.")
            return

        print(f"Found {len(suites)} installed test suites:\n")

        for suite in suites:
            print(f"📦 {suite['name']}")
            print(f"   Path: {suite['path']}")

            # Show configuration status
            config_status = "✅" if suite['config_exists'] else "❌"
            print(f"   Config: {config_status} configuration_file_tests.json")

            # Show requirements status
            req_status = "✅" if suite['requirements_exists'] else "➖"
            print(f"   Requirements: {req_status} requirements.txt")

            # Show installer status
            installer_status = "✅" if suite['installer_exists'] else "➖"
            print(f"   Installer: {installer_status} install.py")

            # Show manifest info if available
            if 'manifest' in suite:
                manifest = suite['manifest']
                print(f"   Description: {manifest.get('description', 'No description')}")
                print(f"   Version: {manifest.get('version', 'Unknown')}")
                if 'author' in manifest:
                    print(f"   Author: {manifest['author']}")

            print()  # Empty line between suites

    def list_registry_repositories(self):
        """List all registered repositories."""
        config = self.git_service.load_test_repositories_config()
        repos = config.get('repositories', {})

        if not repos:
            print("No repositories registered.")
            print(f"To register a repository, use:")
            print(f"  python fetch_tests.py --register my_suite --repo https://github.com/user/repo.git")
            return

        print(f"Found {len(repos)} registered repositories:\n")

        for name, info in repos.items():
            print(f"🗂️  {name}")
            print(f"   URL: {info['url']}")
            print(f"   Branch: {info.get('branch', 'main')}")
            if info.get('description'):
                print(f"   Description: {info['description']}")
            print()

    def register_repository(self, name: str, url: str, branch: str = "main", description: str = ""):
        """
        Register a repository in the configuration.

        Args:
            name: Repository name
            url: Repository URL
            branch: Git branch
            description: Repository description
        """
        self.git_service.register_repository(name, url, branch, description)
        print(f"✅ Registered repository '{name}': {url}")

    def uninstall_suite(self, name: str) -> bool:
        """
        Uninstall a test suite.

        Args:
            name: Name of the suite to uninstall

        Returns:
            True if successful, False otherwise
        """
        if self.installer.uninstall_suite(name):
            print(f"✅ Successfully uninstalled test suite '{name}'")
            return True
        else:
            print(f"❌ Failed to uninstall test suite '{name}'")
            return False

    def validate_suite(self, path: str):
        """
        Validate a test suite.

        Args:
            path: Path to the test suite directory
        """
        results = self.installer.validate_suite(path)

        print(f"Validation results for {path}:")
        status_icon = "✅" if results['valid'] else "❌"
        print(f"{status_icon} Valid: {results['valid']}\n")

        if results['errors']:
            print("❌ Errors:")
            for error in results['errors']:
                print(f"   • {error}")
            print()

        if results['warnings']:
            print("⚠️  Warnings:")
            for warning in results['warnings']:
                print(f"   • {warning}")
            print()

        if results['info']:
            print("ℹ️  Info:")
            for info in results['info']:
                print(f"   • {info}")

    def install_from_directory(self, path: str, name: str = None) -> bool:
        """
        Install a test suite from a local directory.

        Args:
            path: Path to directory containing the test suite
            name: Custom name for the suite

        Returns:
            True if successful, False otherwise
        """
        if self.installer.install_from_directory(path, name):
            suite_name = name or Path(path).name
            print(f"✅ Successfully installed test suite '{suite_name}' from {path}")
            return True
        else:
            print(f"❌ Failed to install test suite from {path}")
            return False

    def install_from_archive(self, path: str, name: str = None) -> bool:
        """
        Install a test suite from a ZIP archive.

        Args:
            path: Path to ZIP archive
            name: Custom name for the suite

        Returns:
            True if successful, False otherwise
        """
        if self.installer.install_from_archive(path, name):
            suite_name = name or Path(path).stem
            print(f"✅ Successfully installed test suite '{suite_name}' from {path}")
            return True
        else:
            print(f"❌ Failed to install test suite from {path}")
            return False

    def batch_fetch(self, suite_names: list):
        """
        Fetch multiple test suites from the registry.

        Args:
            suite_names: List of suite names to fetch
        """
        print(f"Fetching {len(suite_names)} test suites...")

        success_count = 0
        for suite_name in suite_names:
            print(f"\n📦 Fetching '{suite_name}'...")
            if self.fetch_from_registry(suite_name):
                success_count += 1
                print(f"✅ Successfully fetched '{suite_name}'")
            else:
                print(f"❌ Failed to fetch '{suite_name}'")

        print(f"\n📊 Batch fetch completed: {success_count}/{len(suite_names)} suites fetched successfully")


def create_parser():
    """Create the argument parser."""
    parser = argparse.ArgumentParser(
        description='Fetch and manage BIRD test suites',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Fetch from git URL
  python fetch_tests.py --repo https://github.com/user/test-suite.git

  # Fetch with custom name and branch
  python fetch_tests.py --repo https://github.com/user/test-suite.git --name my_suite --branch develop

  # Fetch from registry
  python fetch_tests.py --fetch-registry bird_default_test_suite

  # Install from local directory
  python fetch_tests.py --install-dir /path/to/suite --name local_suite

  # List operations
  python fetch_tests.py --list           # List installed suites
  python fetch_tests.py --list-repos     # List registered repositories

  # Management operations
  python fetch_tests.py --register my_suite --repo https://github.com/user/repo.git
  python fetch_tests.py --uninstall my_suite
  python fetch_tests.py --validate /path/to/suite

  # Batch operations
  python fetch_tests.py --batch bird_default_test_suite finrep_test_suite
        """
    )

    # Fetch operations
    fetch_group = parser.add_argument_group('Fetch Operations')
    fetch_group.add_argument('--repo', help='Git repository URL to fetch')
    fetch_group.add_argument('--fetch-registry', help='Fetch from registered repository')
    fetch_group.add_argument('--batch', nargs='+', help='Fetch multiple suites from registry')

    # Install operations
    install_group = parser.add_argument_group('Install Operations')
    install_group.add_argument('--install-dir', help='Install test suite from local directory')
    install_group.add_argument('--install-archive', help='Install test suite from ZIP archive')

    # List operations
    list_group = parser.add_argument_group('List Operations')
    list_group.add_argument('--list', action='store_true', help='List installed test suites')
    list_group.add_argument('--list-repos', action='store_true', help='List registered repositories')

    # Management operations
    mgmt_group = parser.add_argument_group('Management Operations')
    mgmt_group.add_argument('--register', help='Register a repository (use with --repo)')
    mgmt_group.add_argument('--uninstall', help='Uninstall a test suite')
    mgmt_group.add_argument('--validate', help='Validate a test suite directory')

    # Options
    options_group = parser.add_argument_group('Options')
    options_group.add_argument('--name', help='Custom name for the suite')
    options_group.add_argument('--branch', default='main', help='Git branch to clone (default: main)')
    options_group.add_argument('--description', help='Description for registered repository')
    options_group.add_argument('--verbose', '-v', action='store_true', help='Enable verbose output')

    return parser


def main():
    """Main entry point."""
    parser = create_parser()
    args = parser.parse_args()

    # Set logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    fetcher = TestFetcher()

    try:
        # List operations
        if args.list:
            fetcher.list_installed_suites()
            return

        if args.list_repos:
            fetcher.list_registry_repositories()
            return

        # Validation operation
        if args.validate:
            fetcher.validate_suite(args.validate)
            return

        # Uninstall operation
        if args.uninstall:
            fetcher.uninstall_suite(args.uninstall)
            return

        # Register operation
        if args.register and args.repo:
            description = args.description or f"Repository at {args.repo}"
            fetcher.register_repository(args.register, args.repo, args.branch, description)
            return

        # Batch fetch operation
        if args.batch:
            fetcher.batch_fetch(args.batch)
            return

        # Fetch operations
        if args.fetch_registry:
            success = fetcher.fetch_from_registry(args.fetch_registry)
            sys.exit(0 if success else 1)

        if args.repo:
            success = fetcher.fetch_test_suite(args.repo, args.name, args.branch)
            sys.exit(0 if success else 1)

        # Install operations
        if args.install_dir:
            success = fetcher.install_from_directory(args.install_dir, args.name)
            sys.exit(0 if success else 1)

        if args.install_archive:
            success = fetcher.install_from_archive(args.install_archive, args.name)
            sys.exit(0 if success else 1)

        # No action specified
        parser.print_help()

    except KeyboardInterrupt:
        print("\n🛑 Operation cancelled by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()