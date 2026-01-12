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
COREP GitHub Pipeline - 4-Step Process.

This standalone script runs the COREP 4-step workflow using a GitHub repository
as the data source. It imports pre-processed data from a GitHub package and
runs the transformation/code generation steps.

The 4 steps are:
1. Import Data - Download and import CSVs from GitHub package
2. Generate Structure Links - Create filters and joins metadata
3. Generate Executable Code - Generate Python filter/join files
4. Run Tests - Execute test suite (if available)

Usage:
    # Using default repository (FreeBIRD_IL_66_C07)
    uv run pybirdai/standalone/run_corep_github_pipeline.py

    # With custom repository URL
    uv run pybirdai/standalone/run_corep_github_pipeline.py \
        --repo-url https://github.com/regcommunity/FreeBIRD_IL_66_C07

    # With GitHub token (for private repos)
    uv run pybirdai/standalone/run_corep_github_pipeline.py --token ghp_xxx

    # Skip specific steps
    uv run pybirdai/standalone/run_corep_github_pipeline.py --skip-import
    uv run pybirdai/standalone/run_corep_github_pipeline.py --only-step 2

    # Force re-import even if code generation was already done
    uv run pybirdai/standalone/run_corep_github_pipeline.py --force
"""
import django
import os
import sys
import argparse
import logging

# Load token from .env file if it exists
def load_env_file():
    """Load environment variables from .env file."""
    env_path = os.path.join(os.path.dirname(__file__), '../../.env')
    if os.path.exists(env_path):
        with open(env_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ.setdefault(key.strip(), value.strip())

load_env_file()

# Parse arguments before Django setup
parser = argparse.ArgumentParser(description='Run COREP GitHub 4-Step Pipeline')
parser.add_argument(
    '--repo-url',
    default='https://github.com/regcommunity/FreeBIRD_IL_66_C07',
    help='GitHub repository URL (default: FreeBIRD_IL_66_C07)'
)
parser.add_argument(
    '--branch',
    default='main',
    help='Branch to use (default: main)'
)
parser.add_argument(
    '--token',
    default=None,
    help='GitHub token for private repositories (or set GITHUB_TOKEN env var, or use .pybird_github_token file)'
)
parser.add_argument(
    '--framework',
    default='COREP',
    help='Framework to process (default: COREP)'
)
parser.add_argument(
    '--skip-import',
    action='store_true',
    help='Skip Step 1 (import from GitHub)'
)
parser.add_argument(
    '--skip-structure-links',
    action='store_true',
    help='Skip Step 2 (generate structure links)'
)
parser.add_argument(
    '--skip-code-gen',
    action='store_true',
    help='Skip Step 3 (generate executable code)'
)
parser.add_argument(
    '--skip-tests',
    action='store_true',
    help='Skip Step 4 (run tests)'
)
parser.add_argument(
    '--only-step',
    type=int,
    choices=[1, 2, 3, 4],
    help='Run only a specific step (1-4)'
)
parser.add_argument(
    '--force',
    action='store_true',
    help='Force import even if metadata shows code generation was completed'
)
parser.add_argument(
    '--skip-cleanup',
    action='store_true',
    help='Do not delete existing database data before import'
)
parser.add_argument(
    '--verbose',
    '-v',
    action='store_true',
    help='Enable verbose output'
)

args = parser.parse_args()

# Resolve GitHub token from multiple sources: CLI arg → .pybird_github_token file → env var
def _get_standalone_github_token():
    """Get GitHub token for standalone scripts.

    Priority: CLI --token → .pybird_github_token file → GITHUB_TOKEN env var
    """
    # 1. CLI argument takes highest priority
    if args.token:
        return args.token

    # 2. Try to load from .pybird_github_token file (same as web interface)
    try:
        from pybirdai.views.workflow.github import _get_github_token
        token = _get_github_token(request=None)
        if token:
            return token
    except ImportError:
        pass  # Django not set up yet, will try after setup

    # 3. Fall back to environment variable
    return os.getenv('GITHUB_TOKEN')

# Configure logging
logging.basicConfig(
    level=logging.DEBUG if args.verbose else logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DjangoSetup:
    _initialized = False

    @classmethod
    def configure_django(cls):
        """Configure Django settings without starting the application"""
        if cls._initialized:
            return

        try:
            # Set up Django settings module for birds_nest in parent directory
            project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
            sys.path.insert(0, project_root)
            os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'birds_nest.settings')

            # This allows us to use Django models without running the server
            django.setup()

            logger.info("Django configured successfully with settings module: %s",
                       os.environ['DJANGO_SETTINGS_MODULE'])
            cls._initialized = True
        except Exception as e:
            logger.error(f"Django configuration failed: {str(e)}")
            raise


def print_banner(step_number, step_name):
    """Print a step banner."""
    banner = f"\n{'='*80}\nSTEP {step_number}: {step_name}\n{'='*80}"
    print(banner)
    logger.info(f"Starting Step {step_number}: {step_name}")


def step1_import_from_github(repo_url, branch, token, force=False, skip_cleanup=False):
    """
    Step 1: Import Data from GitHub package.

    Downloads and imports CSV files from the GitHub repository into the database.
    Handles repositories that don't have process_metadata.json by directly
    downloading and importing CSV files.
    """
    print_banner(1, "IMPORT DATA FROM GITHUB")

    import shutil
    from pybirdai.services.github_service import GitHubService
    from pybirdai.utils.clone_mode.import_from_metadata_export import CSVDataImporter

    print(f"  Repository: {repo_url}")
    print(f"  Branch: {branch}")
    print(f"  Token provided: {'Yes' if token else 'No'}")

    try:
        # Parse repository URL
        owner, repo = GitHubService.parse_url(repo_url)
        if not owner or not repo:
            raise ValueError(f"Invalid repository URL: {repo_url}")

        print(f"  Downloading {owner}/{repo}@{branch}...")

        # Download repository archive
        github_service = GitHubService(token=token)
        result = github_service.download_archive(owner, repo, branch)

        if not result['success']:
            raise Exception(result['error'])

        source_dir = result['path']
        print(f"  Downloaded to: {source_dir}")

        # Find CSV directory - prefer export/database_export_ldm over other directories
        csv_dir = None
        all_csv_dirs = []

        for root, dirs, files in os.walk(source_dir):
            csv_files = [f for f in files if f.endswith('.csv')]
            if csv_files and len(csv_files) > 5:
                all_csv_dirs.append((root, len(csv_files), csv_files))

        # Prefer database_export_ldm directory
        for dir_path, count, files in all_csv_dirs:
            if 'database_export_ldm' in dir_path:
                csv_dir = dir_path
                print(f"  Found {count} CSV files in: {dir_path}")
                break

        # Fall back to any directory with most CSV files
        if not csv_dir and all_csv_dirs:
            all_csv_dirs.sort(key=lambda x: x[1], reverse=True)
            csv_dir, count, _ = all_csv_dirs[0]
            print(f"  Found {count} CSV files in: {csv_dir}")

        if not csv_dir:
            raise Exception("No CSV directory found in repository")

        # Clean up existing database if not skipping
        if not skip_cleanup:
            print("  Cleaning up existing database...")
            try:
                from pybirdai.entry_points.delete_bird_metadata_database import RunDeleteBirdMetadataDatabase
                app_config = RunDeleteBirdMetadataDatabase("pybirdai", "birds_nest")
                app_config.run_delete_bird_metadata_database()
                print("  Database cleaned")
            except Exception as e:
                print(f"  Warning: Database cleanup failed: {e}")

        # Import CSV files
        print(f"  Importing CSV files from: {csv_dir}")
        importer = CSVDataImporter()
        import_results = importer.import_from_path_ordered(csv_dir, use_fast_import=False)

        # Count results
        total_records = sum(r.get('imported_count', 0) for r in import_results.values())
        print(f"  Imported {total_records} records from {len(import_results)} tables")

        # Also copy joins_configuration if it exists
        joins_config_src = None
        for root, dirs, files in os.walk(source_dir):
            if 'joins_configuration' in dirs:
                joins_config_src = os.path.join(root, 'joins_configuration')
                break

        if joins_config_src and os.path.exists(joins_config_src):
            from django.conf import settings
            joins_config_dst = os.path.join(settings.BASE_DIR, 'resources', 'joins_configuration')
            os.makedirs(joins_config_dst, exist_ok=True)
            for item in os.listdir(joins_config_src):
                src_path = os.path.join(joins_config_src, item)
                dst_path = os.path.join(joins_config_dst, item)
                if os.path.isfile(src_path):
                    shutil.copy2(src_path, dst_path)
            print(f"  Copied joins configuration files")

        # Cleanup downloaded files
        print("  Cleaning up temporary files...")
        shutil.rmtree(source_dir)

        logger.info("Step 1 completed: Data imported from GitHub")
        return {
            'success': True,
            'message': 'Data imported successfully',
            'records_imported': total_records,
            'tables_imported': len(import_results)
        }

    except Exception as e:
        logger.error(f"Step 1 failed: {e}")
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()
        return {'success': False, 'error': str(e)}


def step2_generate_structure_links(framework):
    """
    Step 2: Generate Structure Links (filters and joins metadata).

    Creates transformation rules including filters and joins metadata.
    """
    print_banner(2, "GENERATE STRUCTURE LINKS")

    from pybirdai.entry_points.create_filters import RunCreateFilters
    from pybirdai.entry_points.create_joins_metadata import RunCreateJoinsMetadata

    results = {
        'success': True,
        'filters_created': False,
        'joins_metadata_created': False,
        'errors': []
    }

    # Generate filters
    print(f"\n  Creating filters for framework: {framework}")
    try:
        framework_ref = f"{framework}_REF" if not framework.endswith('_REF') else framework
        version = "4.0"  # Default version

        logger.info(f"Generating filters for framework: {framework_ref}, version: {version}")
        RunCreateFilters.run_create_filters(framework=framework_ref, version=version)
        results['filters_created'] = True
        print(f"  Filters created successfully")
    except Exception as e:
        error_msg = f"Failed to create filters: {e}"
        logger.error(error_msg)
        print(f"  ERROR: {error_msg}")
        results['errors'].append(error_msg)
        results['success'] = False

    # Create joins metadata
    print(f"\n  Creating joins metadata...")
    try:
        logger.info("Creating joins metadata...")
        RunCreateJoinsMetadata.run_create_joins_meta_data()
        results['joins_metadata_created'] = True
        print(f"  Joins metadata created successfully")
    except Exception as e:
        error_msg = f"Failed to create joins metadata: {e}"
        logger.error(error_msg)
        print(f"  ERROR: {error_msg}")
        results['errors'].append(error_msg)
        results['success'] = False

    if results['success']:
        logger.info("Step 2 completed: Structure links generated")
    else:
        logger.error(f"Step 2 completed with errors: {results['errors']}")

    return results


def step3_generate_executable_code(framework):
    """
    Step 3: Generate Executable Code (Python).

    Generates executable Python code from metadata.
    """
    print_banner(3, "GENERATE EXECUTABLE CODE")

    from pybirdai.entry_points.run_create_executable_filters import RunCreateExecutableFilters
    from pybirdai.entry_points.create_executable_joins import RunCreateExecutableJoins

    results = {
        'success': True,
        'filter_code_generated': False,
        'join_code_generated': False,
        'errors': []
    }

    # Generate filter code
    print(f"\n  Generating executable filter code for framework: {framework}")
    try:
        logger.info(f"Generating filter code for framework: {framework}")
        RunCreateExecutableFilters.run_create_executable_filters_from_db(framework=framework)
        results['filter_code_generated'] = True
        print(f"  Filter code generated successfully")
    except Exception as e:
        error_msg = f"Failed to generate filter code: {e}"
        logger.error(error_msg)
        print(f"  ERROR: {error_msg}")
        results['errors'].append(error_msg)
        results['success'] = False

    # Generate join code
    print(f"\n  Generating executable join code for framework: {framework}")
    try:
        logger.info(f"Generating joins for framework: {framework}")
        RunCreateExecutableJoins.run_create_executable_joins(framework_id=framework)
        results['join_code_generated'] = True
        print(f"  Join code generated successfully")
    except Exception as e:
        error_msg = f"Failed to generate join code: {e}"
        logger.error(error_msg)
        print(f"  ERROR: {error_msg}")
        results['errors'].append(error_msg)
        results['success'] = False

    if results['success']:
        logger.info("Step 3 completed: Executable code generated")
    else:
        logger.error(f"Step 3 completed with errors: {results['errors']}")

    return results


def step4_run_tests(framework):
    """
    Step 4: Run Tests.

    Executes the test suite for the framework.
    """
    print_banner(4, "RUN TESTS")

    results = {
        'success': True,
        'tests_executed': False,
        'errors': []
    }

    # Check for test configuration
    tests_dir = 'tests/dpm'
    config_file_path = os.path.join(tests_dir, 'configuration_file_tests.json')

    if not os.path.exists(config_file_path):
        # Try alternate location
        config_file_path = os.path.join('tests', 'configuration_file_tests.json')

    if os.path.exists(config_file_path):
        print(f"\n  Found test suite configuration: {config_file_path}")

        try:
            from pybirdai.utils.datapoint_test_run.run_tests import RegulatoryTemplateTestRunner

            # Create test runner instance
            test_runner = RegulatoryTemplateTestRunner(False)

            # Configure test runner
            test_runner.args.uv = "False"
            test_runner.args.config_file = config_file_path
            test_runner.args.dp_value = None
            test_runner.args.reg_tid = None
            test_runner.args.dp_suffix = None
            test_runner.args.scenario = None
            test_runner.args.suite_name = 'dpm'
            test_runner.args.framework = framework

            # Execute tests
            print(f"  Executing tests for framework '{framework}'...")
            logger.info(f"Executing tests for framework '{framework}'")
            test_runner.main()

            results['tests_executed'] = True
            print(f"  Test suite completed")
            logger.info("Step 4 completed: Tests executed")

        except Exception as e:
            error_msg = f"Failed to run tests: {e}"
            logger.error(error_msg)
            print(f"  ERROR: {error_msg}")
            results['errors'].append(error_msg)
            results['success'] = False
    else:
        print(f"\n  No test configuration found at {config_file_path}")
        print(f"  Skipping test execution")
        logger.warning(f"No test configuration found at {config_file_path}")
        results['tests_executed'] = False
        results['errors'].append(f"No test suite found at {config_file_path}")

    return results


def main():
    """Main entry point for the COREP GitHub pipeline."""
    print("\n" + "=" * 80)
    print("COREP GitHub Pipeline - 4-Step Process")
    print("=" * 80)

    # Configure Django
    DjangoSetup.configure_django()

    # Determine which steps to run
    steps_to_run = [1, 2, 3, 4]

    if args.only_step:
        steps_to_run = [args.only_step]
    else:
        if args.skip_import:
            steps_to_run.remove(1)
        if args.skip_structure_links:
            steps_to_run.remove(2)
        if args.skip_code_gen:
            steps_to_run.remove(3)
        if args.skip_tests:
            steps_to_run.remove(4)

    # Resolve GitHub token (CLI → .pybird_github_token file → env var)
    github_token = _get_standalone_github_token()

    print(f"\nConfiguration:")
    print(f"  Repository: {args.repo_url}")
    print(f"  Branch: {args.branch}")
    print(f"  Framework: {args.framework}")
    print(f"  Steps to run: {steps_to_run}")
    print(f"  Force: {args.force}")
    print(f"  Token provided: {'Yes' if github_token else 'No'}")

    results = {}

    # Step 1: Import from GitHub
    if 1 in steps_to_run:
        results[1] = step1_import_from_github(
            args.repo_url,
            args.branch,
            github_token,
            force=args.force,
            skip_cleanup=args.skip_cleanup
        )
        if not results[1].get('success'):
            print("\n" + "=" * 80)
            print("PIPELINE STOPPED: Step 1 failed")
            print("=" * 80)
            return 1

    # Step 2: Generate Structure Links
    if 2 in steps_to_run:
        results[2] = step2_generate_structure_links(args.framework)
        if not results[2].get('success'):
            print("\n" + "=" * 80)
            print("PIPELINE STOPPED: Step 2 failed")
            print("=" * 80)
            return 1

    # Step 3: Generate Executable Code
    if 3 in steps_to_run:
        results[3] = step3_generate_executable_code(args.framework)
        if not results[3].get('success'):
            print("\n" + "=" * 80)
            print("PIPELINE STOPPED: Step 3 failed")
            print("=" * 80)
            return 1

    # Step 4: Run Tests
    if 4 in steps_to_run:
        results[4] = step4_run_tests(args.framework)
        # Don't stop on test failures, just report

    # Print summary
    print("\n" + "=" * 80)
    print("PIPELINE SUMMARY")
    print("=" * 80)

    all_success = True
    for step_num, step_result in results.items():
        status = "SUCCESS" if step_result.get('success') else "FAILED"
        if not step_result.get('success'):
            all_success = False
        print(f"  Step {step_num}: {status}")
        if step_result.get('errors'):
            for error in step_result['errors']:
                print(f"    - {error}")

    print("=" * 80)

    if all_success:
        print("\nPipeline completed successfully!")
        return 0
    else:
        print("\nPipeline completed with errors.")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
