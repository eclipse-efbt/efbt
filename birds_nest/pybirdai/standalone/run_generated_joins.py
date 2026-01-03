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
Standalone script to execute generated Python join transformations.

This script discovers and executes join table classes from the
results/generated_python_joins directory.
"""

import os
import sys
import django
import logging
import argparse
import importlib
import inspect
from datetime import datetime
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("generated_joins_execution.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


class DjangoSetup:
    """Setup Django environment for standalone execution"""
    _initialized = False

    @classmethod
    def configure_django(cls):
        """Configure Django settings without starting the application"""
        if cls._initialized:
            return

        try:
            # Set up Django settings module for birds_nest
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


class GeneratedJoinsRunner:
    """Execute generated join transformations from the results directory"""

    def __init__(self, output_dir=None):
        """
        Initialize the runner.

        Args:
            output_dir: Optional custom output directory for results
        """
        DjangoSetup.configure_django()

        from django.conf import settings
        self.base_dir = settings.BASE_DIR
        # Support both new unified structure and legacy location
        self.joins_dir_new = os.path.join(self.base_dir, 'results', 'generated_python')
        self.joins_dir_legacy = os.path.join(self.base_dir, 'results', 'generated_python_joins')
        # Use new structure if it exists, otherwise fall back to legacy
        self.joins_dir = self.joins_dir_new if os.path.exists(self.joins_dir_new) else self.joins_dir_legacy
        self.output_dir = output_dir or os.path.join(self.base_dir, 'results')

        # Import necessary modules
        self.output_tables_module = None
        self.ancrdt_output_tables_module = None

        # Ensure both results and filter_code directories are in Python path
        results_dir = os.path.join(self.base_dir, 'results')
        if results_dir not in sys.path:
            sys.path.insert(0, results_dir)

        # Also add pybirdai to path for filter_code access
        pybirdai_dir = os.path.join(self.base_dir, 'pybirdai')
        if pybirdai_dir not in sys.path:
            sys.path.insert(0, pybirdai_dir)

    def _import_module_with_fallback(self, module_name, source_preference='production', framework='ANCRDT'):
        """
        Try to import a module from filter_code (production) first, then fallback to generated_python (staging).
        This supports the lifecycle: Generate → Edit → Deploy.

        New structure (2025):
        - ANCRDT production: filter_code/datasets/ANCRDT/joins/{module}.py
        - ANCRDT staging: generated_python/datasets/ANCRDT/joins/{module}.py
        - FINREP/COREP production: filter_code/templates/{FRAMEWORK}/joins/{module}.py
        - FINREP/COREP staging: generated_python/templates/{FRAMEWORK}/joins/{module}.py
        - Legacy staging: generated_python_joins/{module}.py

        Args:
            module_name: Name of the module to import (e.g., 'ancrdt_output_tables', 'ANCRDT_INSTRMNT_C_1_logic')
            source_preference: 'production' (filter_code first) or 'staging' (generated_python first)
            framework: Framework name for path resolution (e.g., 'ANCRDT', 'FINREP', 'COREP')

        Returns:
            tuple: (module, source) where source is 'production' or 'staging'
        """
        from pybirdai.services.pipeline_repo_service import PipelineRepoService

        # Determine code type based on framework
        framework_upper = framework.upper() if framework else 'ANCRDT'
        if framework_upper in ['ANCRDT', 'ANACREDIT']:
            code_type = 'datasets'
            framework_upper = 'ANCRDT'
        else:
            code_type = 'templates'

        # Build production and staging paths based on new structure
        # Production: filter_code/{type}/{FRAMEWORK}/joins/{module}
        # Staging: generated_python/{type}/{FRAMEWORK}/joins/{module}
        production_new = f'pybirdai.process_steps.filter_code.{code_type}.{framework_upper}.joins.{module_name}'
        staging_new = f'results.generated_python.{code_type}.{framework_upper}.joins.{module_name}'

        # Legacy paths for backwards compatibility
        production_legacy = f'pybirdai.process_steps.filter_code.{module_name}'
        staging_legacy = f'generated_python_joins.{module_name}'

        # Special case for ancrdt_output_tables (main output tables module)
        if module_name == 'ancrdt_output_tables':
            production_new = f'pybirdai.process_steps.filter_code.datasets.ANCRDT.filter.ancrdt_output_tables'
            staging_new = f'results.generated_python.datasets.ANCRDT.filter.ancrdt_output_tables'

        sources = []
        if source_preference == 'production':
            sources = [
                (production_new, 'production (new)'),
                (production_legacy, 'production (legacy)'),
                (staging_new, 'staging (new)'),
                (staging_legacy, 'staging (legacy)')
            ]
        else:
            sources = [
                (staging_new, 'staging (new)'),
                (staging_legacy, 'staging (legacy)'),
                (production_new, 'production (new)'),
                (production_legacy, 'production (legacy)')
            ]

        for full_module_name, source_name in sources:
            try:
                module = importlib.import_module(full_module_name)
                logger.info(f"Imported {module_name} from {source_name} ({full_module_name})")
                return module, source_name
            except ImportError as e:
                logger.debug(f"Could not import {module_name} from {source_name}: {e}")
                continue
            except Exception as e:
                logger.warning(f"Error importing {module_name} from {source_name}: {e}")
                continue

        logger.error(f"Could not import {module_name} from any source")
        return None, None

    def discover_tables(self, framework=None):
        """
        Discover all *_Table classes in the generated_python directory.

        Args:
            framework: Optional filter for 'finrep', 'ancrdt', or None for all

        Returns:
            Dictionary mapping table names to their class objects
        """
        import glob
        tables = {}

        # Discover FINREP tables from individual logic files
        if framework in [None, 'finrep']:
            try:
                # Look in both new and legacy locations
                logic_patterns = [
                    # New structure: templates/FINREP/joins/
                    os.path.join(self.joins_dir_new, 'templates', 'FINREP', 'joins', 'F_*_logic.py'),
                    os.path.join(self.joins_dir_new, 'templates', 'FINREP', 'joins', '*_REF_FINREP_*_logic.py'),
                    # Legacy structure
                    os.path.join(self.joins_dir_legacy, 'F_*_REF_FINREP_*_logic.py'),
                ]
                logic_files = []
                for pattern in logic_patterns:
                    logic_files.extend(glob.glob(pattern))

                logger.info(f"Scanning {len(logic_files)} FINREP logic files...")

                for logic_file in logic_files:
                    filename = os.path.basename(logic_file)
                    module_name = filename[:-3]  # Remove .py extension

                    try:
                        # Import the logic module with fallback support
                        module, source = self._import_module_with_fallback(
                            module_name,
                            source_preference='production',
                            framework='FINREP'
                        )

                        if module:
                            # Find all classes ending with _Table
                            for name, obj in inspect.getmembers(module, inspect.isclass):
                                if name.endswith('_Table') and hasattr(obj, 'init'):
                                    # Only add if not already discovered
                                    if name not in tables:
                                        tables[name] = obj
                                        logger.debug(f"Discovered FINREP table: {name} from {module_name} ({source})")

                    except SyntaxError as e:
                        logger.warning(f"Syntax error in {module_name}: {e}")
                        continue
                    except Exception as e:
                        logger.warning(f"Error processing {module_name}: {e}")
                        continue

                finrep_count = len([t for t in tables.keys() if 'FINREP' in t])
                logger.info(f"Discovered {finrep_count} FINREP tables from logic files")

            except Exception as e:
                logger.error(f"Error discovering FINREP tables: {e}")

        # Try to import ancrdt_output_tables (AnaCredit)
        # New lifecycle: Try production (filter_code) first, then staging (generated_python)
        if framework in [None, 'ancrdt']:
            try:
                self.ancrdt_output_tables_module, source = self._import_module_with_fallback(
                    'ancrdt_output_tables',
                    source_preference='production',
                    framework='ANCRDT'
                )

                if self.ancrdt_output_tables_module:
                    logger.info(f"Imported ancrdt_output_tables module (AnaCredit) from {source}")

                    # Find all classes ending with _Table
                    for name, obj in inspect.getmembers(self.ancrdt_output_tables_module, inspect.isclass):
                        if name.endswith('_Table') and hasattr(obj, 'init'):
                            tables[name] = obj
                            logger.debug(f"Discovered AnaCredit table: {name} from {source}")
                else:
                    logger.warning("Could not import ancrdt_output_tables module from any source")

            except SyntaxError as e:
                logger.error(f"Syntax error in generated AnaCredit code: {e}")
                logger.error(f"Please regenerate the joins or fix the syntax error in {e.filename}:{e.lineno}")
            except Exception as e:
                logger.error(f"Unexpected error importing AnaCredit tables: {e}")

        logger.info(f"Discovered {len(tables)} table(s) total")
        return tables

    def execute_table(self, table_name, table_class):
        """
        Execute a single table join transformation.

        Args:
            table_name: Name of the table class
            table_class: The table class to execute

        Returns:
            Dictionary with execution results
        """
        start_time = datetime.now()
        result = {
            'table_name': table_name,
            'success': False,
            'start_time': start_time,
            'end_time': None,
            'duration': None,
            'error': None
        }

        try:
            logger.info(f"Executing table: {table_name}")

            # Create instance of the table class
            table_instance = table_class()

            # Execute the init() method which performs the join and saves CSV
            table_instance.init()

            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            result['success'] = True
            result['end_time'] = end_time
            result['duration'] = duration

            logger.info(f"Successfully executed {table_name} in {duration:.2f}s")

        except Exception as e:
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            result['end_time'] = end_time
            result['duration'] = duration
            result['error'] = str(e)

            logger.error(f"Failed to execute {table_name}: {e}")
            import traceback
            logger.debug(traceback.format_exc())

        return result

    def execute_tables(self, table_names=None, framework=None):
        """
        Execute multiple table transformations.

        Args:
            table_names: List of specific table names to execute, or None for all
            framework: Framework filter ('finrep', 'ancrdt', or None)

        Returns:
            List of execution results
        """
        # Discover available tables
        available_tables = self.discover_tables(framework=framework)

        if not available_tables:
            logger.error("No tables found to execute")
            return []

        # Determine which tables to execute
        if table_names:
            # Execute specific tables
            tables_to_execute = {}
            for name in table_names:
                # Support both with and without _Table suffix
                table_key = name if name.endswith('_Table') else f"{name}_Table"

                if table_key in available_tables:
                    tables_to_execute[table_key] = available_tables[table_key]
                else:
                    logger.warning(f"Table not found: {name}")
        else:
            # Execute all discovered tables
            tables_to_execute = available_tables

        if not tables_to_execute:
            logger.error("No tables selected for execution")
            return []

        logger.info(f"Executing {len(tables_to_execute)} table(s)...")

        # Execute each table
        results = []
        for table_name, table_class in tables_to_execute.items():
            result = self.execute_table(table_name, table_class)
            results.append(result)

        # Print summary
        self.print_summary(results)

        return results

    def print_summary(self, results):
        """Print execution summary"""
        logger.info("\n" + "="*80)
        logger.info("EXECUTION SUMMARY")
        logger.info("="*80)

        successful = [r for r in results if r['success']]
        failed = [r for r in results if not r['success']]

        logger.info(f"Total tables: {len(results)}")
        logger.info(f"Successful: {len(successful)}")
        logger.info(f"Failed: {len(failed)}")

        if successful:
            total_duration = sum(r['duration'] for r in successful)
            logger.info(f"Total execution time: {total_duration:.2f}s")
            logger.info(f"Average time per table: {total_duration/len(successful):.2f}s")

        if failed:
            logger.info("\nFailed tables:")
            for r in failed:
                logger.error(f"  - {r['table_name']}: {r['error']}")

        logger.info("="*80 + "\n")

    def list_tables(self, framework=None):
        """
        List all available table classes.

        Args:
            framework: Framework filter ('finrep', 'ancrdt', or None)
        """
        tables = self.discover_tables(framework=framework)

        if not tables:
            logger.info("No tables found")
            return

        logger.info("\nAvailable tables:")
        logger.info("-" * 80)

        # Group by framework
        finrep_tables = [name for name in tables.keys() if 'FINREP' in name or 'F_' in name]
        ancrdt_tables = [name for name in tables.keys() if 'ANCRDT' in name]

        if finrep_tables:
            logger.info(f"\nFINREP Tables ({len(finrep_tables)}):")
            for name in sorted(finrep_tables):
                logger.info(f"  - {name}")

        if ancrdt_tables:
            logger.info(f"\nAnaCredit Tables ({len(ancrdt_tables)}):")
            for name in sorted(ancrdt_tables):
                logger.info(f"  - {name}")

        logger.info("-" * 80)


def main():
    """Main entry point for the script"""
    parser = argparse.ArgumentParser(
        description='Execute generated Python join transformations',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # List all available tables
  python run_generated_joins.py --list

  # Run all FINREP tables
  python run_generated_joins.py --framework finrep --all

  # Run all AnaCredit tables
  python run_generated_joins.py --framework ancrdt --all

  # Run specific table
  python run_generated_joins.py --table F_01_01_REF_FINREP_3_0

  # Run multiple specific tables
  python run_generated_joins.py --table F_01_01_REF_FINREP_3_0 --table F_01_02_REF_FINREP_3_0
        """
    )

    parser.add_argument(
        '--list',
        action='store_true',
        help='List all available table classes'
    )

    parser.add_argument(
        '--all',
        action='store_true',
        help='Execute all discovered tables'
    )

    parser.add_argument(
        '--table',
        action='append',
        dest='tables',
        help='Execute specific table(s). Can be used multiple times. Table name with or without _Table suffix.'
    )

    parser.add_argument(
        '--framework',
        choices=['finrep', 'ancrdt'],
        help='Filter tables by framework (finrep or ancrdt)'
    )

    parser.add_argument(
        '--output-dir',
        help='Custom output directory for results (default: results/)'
    )

    args = parser.parse_args()

    # Create runner instance
    runner = GeneratedJoinsRunner(output_dir=args.output_dir)

    # Execute based on arguments
    if args.list:
        # List available tables
        runner.list_tables(framework=args.framework)
    elif args.all or args.tables:
        # Execute tables
        if args.all:
            runner.execute_tables(framework=args.framework)
        else:
            runner.execute_tables(table_names=args.tables, framework=args.framework)
    else:
        # No action specified, show help
        parser.print_help()
        logger.info("\nNo action specified. Use --list to see available tables or --all to run all tables.")


if __name__ == "__main__":
    main()
