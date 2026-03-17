"""
Django Management Command: load_test_data

This command provides utilities to populate the database with test data from test suites
and to clean BIRD database tables.

Usage:
    # Interactive mode (prompts for suite/template/scenario)
    python manage.py load_test_data

    # Clean database only
    python manage.py load_test_data --clean

    # Load specific scenario (non-interactive)
    python manage.py load_test_data --suite ancrdt-test-suite --template ANCRDT_INSTRMNT_C_1 --scenario 01_happy_path_single_instrument

    # Load with verbose output
    python manage.py load_test_data --suite bird-default-test-suite-eil-67 --template F_04_01 --scenario DP001 --verbose

Author: PyBIRD AI Team
Date: 2024
"""

import os
import sys
from pathlib import Path
from typing import List, Optional, Tuple
from django.core.management.base import BaseCommand, CommandError
from django.db import connection

# Import the database cleanup service
from pybirdai.utils.datapoint_test_run.database_cleanup_service import DatabaseCleanupService


class Command(BaseCommand):
    """
    Django management command to load test data into the database.

    This command supports:
    - Interactive mode: Prompts user to select suite, template, and scenario
    - Command-line mode: Accepts explicit parameters
    - Database cleanup: Cleans all BIRD data model tables
    - Verbose logging: Optional detailed output
    """

    help = 'Load test data from test suites or clean BIRD database tables'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.verbose = False
        self.tests_dir = None

    def add_arguments(self, parser):
        """
        Define command-line arguments.

        Args:
            parser: Django's argument parser
        """
        parser.add_argument(
            '--suite',
            type=str,
            help='Test suite name (e.g., bird-default-test-suite-eil-67, ancrdt-test-suite)',
        )
        parser.add_argument(
            '--template',
            type=str,
            help='Template name (e.g., ANCRDT_INSTRMNT_C_1, F_04_01)',
        )
        parser.add_argument(
            '--scenario',
            type=str,
            help='Scenario name (e.g., 01_happy_path_single_instrument, DP001)',
        )
        parser.add_argument(
            '--clean',
            action='store_true',
            help='Clean BIRD database tables only (no data loading)',
        )
        parser.add_argument(
            '--verbose',
            action='store_true',
            help='Enable verbose output (detailed logging)',
        )

    def handle(self, *args, **options):
        """
        Main entry point for the management command.

        Args:
            options: Command-line options dictionary
        """
        self.verbose = options.get('verbose', False)

        # Determine tests directory
        self.tests_dir = self._get_tests_directory()

        if not self.tests_dir.exists():
            raise CommandError(f"Tests directory not found: {self.tests_dir}")

        # Handle clean-only mode
        if options.get('clean'):
            self._clean_database()
            return

        # Determine if running in interactive or command-line mode
        suite = options.get('suite')
        template = options.get('template')
        scenario = options.get('scenario')

        if suite and template and scenario:
            # Command-line mode: all parameters provided
            self._load_data_cli_mode(suite, template, scenario)
        elif not suite and not template and not scenario:
            # Interactive mode: no parameters provided
            self._load_data_interactive_mode()
        else:
            # Partial parameters provided - error
            raise CommandError(
                "Either provide all parameters (--suite, --template, --scenario) "
                "or none for interactive mode."
            )

    def _get_tests_directory(self) -> Path:
        """
        Get the tests directory path.

        Returns:
            Path object pointing to the tests directory
        """
        # Get the project base directory (birds_nest)
        # Path: load_test_data.py -> commands -> management -> pybirdai -> birds_nest
        base_dir = Path(__file__).resolve().parent.parent.parent.parent
        tests_dir = base_dir / 'tests'
        return tests_dir

    # ============================================================================
    # DIRECTORY SCANNING UTILITIES
    # ============================================================================

    def _discover_test_suites(self) -> List[str]:
        """
        Discover available test suites in the tests directory.

        Returns:
            List of test suite names (directory names)
        """
        if not self.tests_dir.exists():
            return []

        suites = [
            d.name for d in self.tests_dir.iterdir()
            if d.is_dir() and not d.name.startswith('.') and not d.name.startswith('__')
        ]

        return sorted(suites)

    def _discover_templates(self, suite_name: str) -> List[str]:
        """
        Discover available templates in a test suite.

        Args:
            suite_name: Name of the test suite

        Returns:
            List of template names (directory names in fixtures/templates/)
        """
        templates_dir = self.tests_dir / suite_name / 'tests' / 'fixtures' / 'templates'

        if not templates_dir.exists():
            return []

        templates = [
            d.name for d in templates_dir.iterdir()
            if d.is_dir() and not d.name.startswith('.') and not d.name.startswith('__')
        ]

        return sorted(templates)

    def _discover_scenarios(self, suite_name: str, template_name: str) -> List[str]:
        """
        Discover available scenarios for a template.

        Args:
            suite_name: Name of the test suite
            template_name: Name of the template

        Returns:
            List of scenario names (directory names in template folder)
        """
        scenarios_dir = self.tests_dir / suite_name / 'tests' / 'fixtures' / 'templates' / template_name

        if not scenarios_dir.exists():
            return []

        scenarios = [
            d.name for d in scenarios_dir.iterdir()
            if d.is_dir() and not d.name.startswith('.') and not d.name.startswith('__')
        ]

        return sorted(scenarios)

    # ============================================================================
    # INTERACTIVE PROMPT SYSTEM
    # ============================================================================

    def _prompt_user_choice(self, prompt_message: str, choices: List[str]) -> str:
        """
        Prompt user to select from a list of choices.

        Args:
            prompt_message: Message to display to user
            choices: List of available choices

        Returns:
            Selected choice string

        Raises:
            CommandError: If user input is invalid
        """
        if not choices:
            raise CommandError("No choices available.")

        self.stdout.write(self.style.SUCCESS(f"\n{prompt_message}"))

        # Display numbered choices
        for idx, choice in enumerate(choices, 1):
            self.stdout.write(f"  [{idx}] {choice}")

        # Get user input
        while True:
            try:
                user_input = input(f"\nEnter your choice (1-{len(choices)}): ").strip()

                # Validate input
                choice_idx = int(user_input) - 1

                if 0 <= choice_idx < len(choices):
                    selected = choices[choice_idx]
                    self.stdout.write(self.style.SUCCESS(f"Selected: {selected}\n"))
                    return selected
                else:
                    self.stdout.write(self.style.ERROR(
                        f"Invalid choice. Please enter a number between 1 and {len(choices)}."
                    ))
            except ValueError:
                self.stdout.write(self.style.ERROR("Invalid input. Please enter a number."))
            except KeyboardInterrupt:
                self.stdout.write("\n")
                raise CommandError("Operation cancelled by user.")

    def _load_data_interactive_mode(self):
        """
        Run the command in interactive mode with step-by-step prompts.
        """
        self.stdout.write(self.style.SUCCESS("\n" + "="*70))
        self.stdout.write(self.style.SUCCESS("    BIRD TEST DATA LOADER - INTERACTIVE MODE"))
        self.stdout.write(self.style.SUCCESS("="*70 + "\n"))

        # Step 1: Select test suite
        suites = self._discover_test_suites()
        if not suites:
            raise CommandError(f"No test suites found in {self.tests_dir}")

        suite_name = self._prompt_user_choice(
            "STEP 1/3: Select a test suite:",
            suites
        )

        # Step 2: Select template
        templates = self._discover_templates(suite_name)
        if not templates:
            raise CommandError(f"No templates found in suite: {suite_name}")

        template_name = self._prompt_user_choice(
            "STEP 2/3: Select a template:",
            templates
        )

        # Step 3: Select scenario
        scenarios = self._discover_scenarios(suite_name, template_name)
        if not scenarios:
            raise CommandError(
                f"No scenarios found for template: {template_name} in suite: {suite_name}"
            )

        scenario_name = self._prompt_user_choice(
            "STEP 3/3: Select a scenario:",
            scenarios
        )

        # Load the data
        self.stdout.write(self.style.SUCCESS("\n" + "="*70))
        self.stdout.write(self.style.SUCCESS("    LOADING TEST DATA"))
        self.stdout.write(self.style.SUCCESS("="*70 + "\n"))

        self._load_test_data(suite_name, template_name, scenario_name)

    # ============================================================================
    # COMMAND-LINE MODE
    # ============================================================================

    def _load_data_cli_mode(self, suite_name: str, template_name: str, scenario_name: str):
        """
        Run the command in command-line mode with explicit parameters.

        Args:
            suite_name: Test suite name
            template_name: Template name
            scenario_name: Scenario name
        """
        self.stdout.write(self.style.SUCCESS("\n" + "="*70))
        self.stdout.write(self.style.SUCCESS("    BIRD TEST DATA LOADER - COMMAND-LINE MODE"))
        self.stdout.write(self.style.SUCCESS("="*70 + "\n"))

        # Validate parameters
        suites = self._discover_test_suites()
        if suite_name not in suites:
            raise CommandError(
                f"Suite '{suite_name}' not found. Available suites: {', '.join(suites)}"
            )

        templates = self._discover_templates(suite_name)
        if template_name not in templates:
            raise CommandError(
                f"Template '{template_name}' not found in suite '{suite_name}'. "
                f"Available templates: {', '.join(templates)}"
            )

        scenarios = self._discover_scenarios(suite_name, template_name)
        if scenario_name not in scenarios:
            raise CommandError(
                f"Scenario '{scenario_name}' not found for template '{template_name}'. "
                f"Available scenarios: {', '.join(scenarios)}"
            )

        # Load the data
        self._load_test_data(suite_name, template_name, scenario_name)

    # ============================================================================
    # DATABASE OPERATIONS
    # ============================================================================

    def _clean_database(self):
        """
        Clean all BIRD database tables using DatabaseCleanupService.

        This function:
        1. Uses DatabaseCleanupService to discover all BIRD data model tables
        2. Deletes data in the correct order to respect foreign key constraints
        3. Handles both assignment/relationship tables and regular tables
        """
        self.stdout.write(self.style.WARNING("\n" + "="*70))
        self.stdout.write(self.style.WARNING("    CLEANING BIRD DATABASE TABLES"))
        self.stdout.write(self.style.WARNING("="*70 + "\n"))

        try:
            if self.verbose:
                self.stdout.write("Discovering BIRD data model tables...")

            # Use DatabaseCleanupService to clean all BIRD tables
            cleanup_service = DatabaseCleanupService()
            deletion_results = cleanup_service.cleanup_bird_data_tables()

            # cleanup_bird_data_tables() returns a dict mapping table names to deleted counts
            # An empty dict means tables were already empty (success)
            # A dict with values means records were deleted (success)
            # Only None or exception indicates failure
            if deletion_results is not None:
                total_deleted = sum(count for count in deletion_results.values() if count > 0)
                if self.verbose and total_deleted > 0:
                    self.stdout.write(f"Deleted {total_deleted} records from {len([c for c in deletion_results.values() if c > 0])} tables")
                elif self.verbose:
                    self.stdout.write("All tables were already empty")

                self.stdout.write(self.style.SUCCESS(
                    "\nDatabase cleanup completed successfully!"
                ))
            else:
                raise CommandError("Database cleanup failed. Check logs for details.")

        except Exception as e:
            raise CommandError(f"Error during database cleanup: {str(e)}")

    def _load_sql_fixture(self, sql_file_path: Path):
        """
        Load SQL fixture file into the database.

        Args:
            sql_file_path: Path to the sql_inserts.sql file

        Raises:
            CommandError: If SQL execution fails
        """
        if not sql_file_path.exists():
            raise CommandError(f"SQL fixture file not found: {sql_file_path}")

        if self.verbose:
            self.stdout.write(f"Loading SQL fixture: {sql_file_path}")

        try:
            # Read SQL file
            with open(sql_file_path, 'r', encoding='utf-8') as f:
                sql_content = f.read()

            # Split into individual statements and filter
            raw_statements = sql_content.split(';')
            statements = []

            for stmt in raw_statements:
                # Remove comment lines (lines starting with --)
                lines = stmt.split('\n')
                non_comment_lines = [
                    line for line in lines
                    if line.strip() and not line.strip().startswith('--')
                ]

                # Join non-comment lines and check if there's actual SQL
                cleaned_stmt = '\n'.join(non_comment_lines).strip()
                if cleaned_stmt:
                    statements.append(cleaned_stmt)

            if self.verbose:
                self.stdout.write(f"Executing {len(statements)} SQL statements...")

            # Execute statements
            with connection.cursor() as cursor:
                for idx, statement in enumerate(statements, 1):
                    if self.verbose:
                        self.stdout.write(f"  [{idx}/{len(statements)}] Executing statement...")

                    try:
                        cursor.execute(statement)
                    except Exception as stmt_error:
                        raise CommandError(
                            f"Error executing SQL statement {idx}:\n"
                            f"Statement: {statement[:100]}...\n"
                            f"Error: {str(stmt_error)}"
                        )

            # Close Django connections to ensure data visibility
            connection.close()

            if self.verbose:
                self.stdout.write(self.style.SUCCESS(
                    f"Successfully loaded {len(statements)} SQL statements"
                ))

        except Exception as e:
            raise CommandError(f"Error loading SQL fixture: {str(e)}")

    def _load_test_data(self, suite_name: str, template_name: str, scenario_name: str):
        """
        Load test data for the specified suite, template, and scenario.

        Args:
            suite_name: Test suite name
            template_name: Template name
            scenario_name: Scenario name
        """
        self.stdout.write(f"Suite:    {suite_name}")
        self.stdout.write(f"Template: {template_name}")
        self.stdout.write(f"Scenario: {scenario_name}\n")

        # Construct path to SQL fixture file
        sql_file_path = (
            self.tests_dir / suite_name / 'tests' / 'fixtures' / 'templates' /
            template_name / scenario_name / 'sql_inserts.sql'
        )

        # Step 1: Clean database
        self.stdout.write(self.style.WARNING("Step 1/2: Cleaning database..."))
        self._clean_database()

        # Step 2: Load SQL fixture
        self.stdout.write(self.style.SUCCESS("\nStep 2/2: Loading test data..."))
        self._load_sql_fixture(sql_file_path)

        # Success message
        self.stdout.write(self.style.SUCCESS("\n" + "="*70))
        self.stdout.write(self.style.SUCCESS("    TEST DATA LOADED SUCCESSFULLY!"))
        self.stdout.write(self.style.SUCCESS("="*70))
        self.stdout.write(f"\nYou can now run tests or execute transformations for:")
        self.stdout.write(f"  Suite:    {suite_name}")
        self.stdout.write(f"  Template: {template_name}")
        self.stdout.write(f"  Scenario: {scenario_name}\n")
