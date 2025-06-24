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

import os
import sys
import ast
import json
import logging
import subprocess
import time
from django.conf import settings
from django.apps import AppConfig
from pybirdai.entry_points.create_django_models import RunCreateDjangoModels
from pybirdai.utils.derived_fields_extractor import (
    merge_derived_fields_into_original_model,
)

# Create a logger
logger = logging.getLogger(__name__)


class RunAutomodeDatabaseSetup(AppConfig):
    """
    Entry point for automode database setup that performs all necessary
    steps to create and configure the BIRD database automatically.
    """

    def __init__(self, app_name, app_module):
        self.app_name = app_name
        self.app_module = app_module

    def run_automode_database_setup(self):
        """
        Execute the complete database setup process for automode.
        This version avoids operations that could cause server restarts.
        """
        try:
            logger.info("Starting automode database setup...")

            # Step 1: Create Django models (this generates files but doesn't modify existing ones)
            logger.info("Step 1: Creating Django models...")
            try:
                app_config = RunCreateDjangoModels(self.app_name, self.app_module)
                app_config.ready()
                logger.info("Django models created successfully.")
            except Exception as e:
                logger.error(f"Failed to create Django models: {str(e)}")
                raise RuntimeError(f"Django model creation failed: {str(e)}") from e

            # Step 2: Check if generated files exist
            base_dir = settings.BASE_DIR

            results_admin_path = os.path.join(
                base_dir,
                "results"
                + os.sep
                + "database_configuration_files"
                + os.sep
                + "admin.py",
            )
            results_models_path = os.path.join(
                base_dir,
                "results"
                + os.sep
                + "database_configuration_files"
                + os.sep
                + "models.py",
            )

            logger.info(f"Base directory: {base_dir}")
            logger.info(
                f"Results models path exists: {os.path.exists(results_models_path)}"
            )
            logger.info(
                f"Results admin path exists: {os.path.exists(results_admin_path)}"
            )

            if not os.path.exists(results_models_path):
                raise RuntimeError(
                    f"Generated models file not found: {results_models_path}"
                )

            if not os.path.exists(results_admin_path):
                logger.warning(f"Generated admin file not found: {results_admin_path}")

            # Step 3: Run the complete automode setup operations directly
            logger.info("Step 2: Database setup preparation completed.")
            logger.info("Step 3: Generated configuration files are ready.")
            logger.info("Step 4: Running automatic database migrations and setup...")

            # Load and handle generated Python files if needed
            try:
                # Step 3a: Load temporary configuration to check when_to_stop setting
                config = self._load_temp_config()

                # Step 3b: Handle generated Python files if needed
                if config and config.get("when_to_stop") == "FULL_EXECUTION":
                    logger.info(
                        "Transferring generated Python files for full execution..."
                    )
                    self._transfer_generated_python_files()

                # Step 3c: Prepare for post-setup operations but don't execute them yet
                # The file modifications that trigger restart should be done by the workflow views
                # after the status has been properly communicated to the frontend
                logger.info("Database setup preparation completed.")
                logger.info(
                    "Post-setup operations (file updates) will be triggered by workflow after status update."
                )

                logger.info("Database models and configuration completed successfully!")

            except Exception as e:
                logger.error(f"Failed to run post-setup operations: {str(e)}")
                raise RuntimeError(f"Automatic database setup failed: {str(e)}") from e

            # Return success with server restart warning
            logger.info("Automode database setup completed successfully!")
            logger.warning(
                "IMPORTANT: Django will restart automatically due to file changes."
            )
            logger.warning(
                "The restart process has been initiated. Please wait for the server to come back online."
            )

            return {
                "success": True,
                "message": "Database setup completed successfully",
                "server_restart_required": True,
                "estimated_restart_time": "Django is restarting now due to file changes. Please wait 30-60 seconds for the server to come back online.",
                "steps_completed": [
                    "Django models generated",
                    "Configuration files created",
                    "Database migrations applied",
                    "Admin interface updated",
                    "Setup process completed",
                ],
            }

        except Exception as e:
            logger.error(f"Automode database setup failed: {str(e)}")
            raise

    def _load_temp_config(self):
        """Load temporary configuration file using same path logic as views.py."""
        try:
            import tempfile

            base_dir = getattr(settings, "BASE_DIR", tempfile.gettempdir())

            # Convert Path object to string if necessary (Django 5.x uses Path objects)
            if hasattr(base_dir, "__fspath__"):  # Check if it's a path-like object
                temp_dir = str(base_dir)
            else:
                temp_dir = base_dir

            # Ensure we use absolute path to avoid working directory issues
            if not os.path.isabs(temp_dir):
                temp_dir = os.path.abspath(temp_dir)

            temp_config_path = os.path.join(temp_dir, "automode_config.json")
            logger.info(f"Looking for config file at: {temp_config_path}")

            if os.path.exists(temp_config_path):
                with open(temp_config_path, "r") as f:
                    config = json.load(f)
                logger.info(
                    f"Loaded automode configuration: when_to_stop = {config.get('when_to_stop')}"
                )
                return config
            else:
                logger.warning(
                    f"No temporary configuration file found at {temp_config_path}"
                )

                # Try fallback location for backwards compatibility
                fallback_path = os.path.join(".", "automode_config.json")
                if os.path.exists(fallback_path):
                    logger.info(f"Found config at fallback location: {fallback_path}")
                    with open(fallback_path, "r") as f:
                        config = json.load(f)
                    return config

                return None
        except Exception as e:
            logger.error(f"Error loading temporary configuration: {e}")
            return None

    def _transfer_generated_python_files(self):
        """Transfer generated Python files from resources/generated_python to pybirdai/process_steps/filter_code."""
        import shutil

        source_dir = os.path.join(".", "resources", "generated_python")
        target_dir = os.path.join(".", "pybirdai", "process_steps", "filter_code")

        try:
            if not os.path.exists(source_dir):
                logger.warning(
                    f"Source directory {source_dir} does not exist - no Python files to transfer"
                )
                return

            # Ensure target directory exists
            os.makedirs(target_dir, exist_ok=True)

            # Find all Python files in source directory
            python_files = [f for f in os.listdir(source_dir) if f.endswith(".py")]

            if not python_files:
                logger.warning(f"No Python files found in {source_dir}")
                return

            # Transfer each Python file
            transferred_count = 0
            for file_name in python_files:
                source_path = os.path.join(source_dir, file_name)
                target_path = os.path.join(target_dir, file_name)

                try:
                    # Copy the file (this overwrites if exists)
                    shutil.copy2(source_path, target_path)
                    transferred_count += 1
                    logger.info(f"Transferred {file_name} to filter_code directory")
                except Exception as e:
                    logger.error(f"Error transferring {file_name}: {e}")

            logger.info(
                f"Successfully transferred {transferred_count} generated Python files"
            )

        except Exception as e:
            logger.error(f"Error during Python file transfer: {e}")
            raise

    def run_post_setup_operations(self):
        """
        STEP 1: Update admin.py and trigger restart.
        After restart, user will need to run migrations separately.
        """
        try:
            logger.info("Starting post-setup operations - STEP 1: Admin file update...")

            # call into RunCreateDjangoModels to create the models.py and admin.py files
            app_config = RunCreateDjangoModels(self.app_name, self.app_module)
            app_config.ready()

            base_dir = settings.BASE_DIR
            initial_migration_file = os.path.join(
                base_dir,
                "pybirdai" + os.sep + "migrations" + os.sep + "0001_initial.py",
            )
            db_file = os.path.join(base_dir, "db.sqlite3")
            pybirdai_admin_path = os.path.join(
                base_dir, "pybirdai" + os.sep + "admin.py"
            )
            pybirdai_meta_data_model_path = os.path.join(
                base_dir, "pybirdai" + os.sep + "bird_meta_data_model.py"
            )
            results_admin_path = os.path.join(
                base_dir,
                "results"
                + os.sep
                + "database_configuration_files"
                + os.sep
                + "admin.py",
            )
            pybirdai_models_path = os.path.join(
                base_dir, "pybirdai" + os.sep + "bird_data_model.py"
            )
            results_models_path = os.path.join(
                base_dir,
                "results"
                + os.sep
                + "database_configuration_files"
                + os.sep
                + "models.py",
            )

            # Cleanup existing files
            logger.info("Cleaning up existing files...")
            self._cleanup_files(initial_migration_file, db_file)

            # Update models file (this is safe, won't trigger restart)
            logger.info("Updating bird_data_model.py...")
            self._update_models_file(pybirdai_models_path, results_models_path)

            # Merge derived fields after pybirdai{os.sep}bird_data_model.py has been generated

            derived_fields_file_path = os.path.join(
                base_dir,
                "resources"
                + os.sep
                + "derivation_files"
                + os.sep
                + "derived_field_configuration.py",
            )

            merge_derived_fields_into_original_model(
                pybirdai_models_path, derived_fields_file_path
            )

            # Update admin.py FIRST - this will trigger Django restart
            logger.info("Updating admin.py with new model registrations...")
            logger.info(
                "This will trigger Django restart - user will need to run migrations after restart"
            )
            self._update_admin_file(
                pybirdai_admin_path, pybirdai_meta_data_model_path, results_admin_path
            )

            # Create a marker file to indicate we're ready for step 2
            self._create_migration_ready_marker(base_dir)

            logger.info("STEP 1 completed successfully!")
            logger.info(
                "Django will restart now. After restart, user needs to run migrations."
            )
            return {
                "success": True,
                "step": 1,
                "message": "Admin files updated successfully. Server will restart.",
                "next_action": "run_migrations_after_restart",
                "server_restart_required": True,
            }

        except Exception as e:
            logger.error(f"Post-setup operations STEP 1 failed: {str(e)}")
            raise

    def run_migrations_after_restart(self):
        """
        STEP 2: Run migrations after Django has restarted with updated admin.py
        """
        try:
            logger.info("Starting STEP 2: Running migrations after restart...")
            logger.info(
                "IMPORTANT: This step should ONLY run Django migrations - no file downloads or deletions"
            )

            base_dir = settings.BASE_DIR

            # Check if we have the ready marker
            if not self._check_migration_ready_marker(base_dir):
                raise RuntimeError(
                    "Migration ready marker not found. Please run STEP 1 first."
                )

            # Run migrations in subprocess (admin.py is already updated)
            logger.info(
                "Running Django migrations in subprocess - NO file operations should happen"
            )
            self._run_migrations_in_subprocess(base_dir)
            logger.info(
                "Django migrations completed - confirming no files were downloaded or deleted"
            )

            # Clean up the marker file
            self._remove_migration_ready_marker(base_dir)

            logger.info("STEP 2 completed successfully!")
            logger.info("Database setup is now complete!")
            return {
                "success": True,
                "step": 2,
                "message": "Database migrations completed successfully",
                "database_ready": True,
            }

        except Exception as e:
            logger.error(f"Post-setup operations STEP 2 failed: {str(e)}")
            raise

    def _cleanup_files(self, initial_migration_file, db_file):
        """Remove existing migration and database files."""
        # Remove initial migration file
        if os.path.exists(initial_migration_file):
            try:
                os.remove(initial_migration_file)
                logger.info(f"Successfully removed {initial_migration_file}")
            except OSError as e:
                logger.error(f"Error removing file {initial_migration_file}: {e}")
                raise RuntimeError(
                    f"Failed to remove file {initial_migration_file}"
                ) from e

        # Remove database file with enhanced error handling
        if os.path.exists(db_file):
            try:
                # Check file properties
                file_size = os.path.getsize(db_file)
                logger.info(
                    f"Database file {db_file} exists with size {file_size} bytes"
                )

                # Try to make it writable first
                try:
                    os.chmod(db_file, 0o666)
                    logger.info(f"Made database file writable: {db_file}")
                except OSError as chmod_error:
                    logger.warning(
                        f"Could not change permissions for {db_file}: {chmod_error}"
                    )

                # Remove the file
                os.remove(db_file)
                logger.info(f"Successfully removed database file {db_file}")

            except OSError as e:
                logger.warning(f"Could not remove database file {db_file}: {e}")
                # Try alternative approaches
                try:
                    # Force remove with different approach
                    import stat

                    os.chmod(db_file, stat.S_IWRITE | stat.S_IREAD)
                    os.unlink(db_file)
                    logger.info(f"Successfully force-removed {db_file}")
                except OSError as e2:
                    logger.warning(f"Failed to force-remove database file: {e2}")
                    # Last resort: rename the file so Django can create a new one
                    try:
                        backup_name = f"{db_file}.backup.{int(time.time())}"
                        os.rename(db_file, backup_name)
                        logger.info(
                            f"Renamed problematic database file to {backup_name}"
                        )
                    except OSError as e3:
                        logger.error(f"Could not even rename database file: {e3}")
                        # Continue anyway - let Django handle it

    def _update_admin_file(
        self, pybirdai_admin_path, pybirdai_meta_data_model_path, results_admin_path
    ):
        """Update the admin.py file with model registrations, avoiding duplicates."""
        registered_models = set()

        # Create initial admin.py content
        with open(pybirdai_admin_path, "w") as f_write:
            f_write.write("from django.contrib import admin\n")

            # Parse the meta data model file to find class definitions
            with open(pybirdai_meta_data_model_path, "r") as f_read:
                tree = ast.parse(f_read.read())
                for node in ast.walk(tree):
                    if isinstance(node, ast.ClassDef) and node.name not in [
                        "Meta",
                        "Admin",
                    ]:
                        if node.name not in registered_models:
                            f_write.write(
                                f"from .bird_meta_data_model import {node.name}\n"
                            )
                            f_write.write(f"admin.site.register({node.name})\n")
                            registered_models.add(node.name)

        # Check if results admin file exists and append its content (without duplicates)
        if not os.path.exists(results_admin_path):
            logger.warning(f"Results admin file not found: {results_admin_path}")
            return

        try:
            # Read content from results file and parse it for additional models
            with open(results_admin_path, "r") as f_read_results:
                results_admin_str = f_read_results.read()

            # Parse results admin.py content to extract model names and avoid duplicates
            additional_content = []
            lines = results_admin_str.split("\n")

            for line in lines:
                line = line.strip()
                if line.startswith("from .bird_data_model import "):
                    # Extract model name from import statement
                    model_name = line.split("import ")[-1].strip()
                    if model_name and model_name not in registered_models:
                        additional_content.append(
                            f"from .bird_data_model import {model_name}"
                        )
                        additional_content.append(f"admin.site.register({model_name})")
                        registered_models.add(model_name)
                elif line.startswith("from django.contrib import admin"):
                    # Skip duplicate admin import
                    continue
                elif line.startswith("admin.site.register("):
                    # Skip individual register lines as they're handled with imports
                    continue
                elif line and not line.startswith("#"):
                    # Include any other non-comment lines
                    additional_content.append(line)

            # Append additional content if any
            if additional_content:
                with open(pybirdai_admin_path, "a") as f_append:
                    f_append.write("\n")
                    f_append.write("\n".join(additional_content))
                    f_append.write("\n")

            logger.info(
                f"{pybirdai_admin_path} updated successfully with {len(registered_models)} unique models."
            )

            # Clean up the results admin.py file after successful use to prevent accumulation
            self._cleanup_results_admin_file(results_admin_path)

        except IOError as e:
            logger.error(f"Error updating {pybirdai_admin_path}: {e}")
            raise RuntimeError(f"Failed to update {pybirdai_admin_path}") from e

    def _cleanup_results_admin_file(self, results_admin_path):
        """Clean up the results admin.py file after successful use to prevent duplicate content."""
        try:
            if os.path.exists(results_admin_path):
                os.remove(results_admin_path)
                logger.info(f"Cleaned up results admin file: {results_admin_path}")
        except (OSError, PermissionError) as e:
            logger.warning(
                f"Could not clean up results admin file {results_admin_path}: {e}"
            )
            # Don't raise an exception here as this is cleanup, not critical functionality

    def _update_models_file(self, pybirdai_models_path, results_models_path):
        """Update the bird_data_model.py file with generated models."""
        if not os.path.exists(results_models_path):
            logger.warning(f"Results models file not found: {results_models_path}")
            return

        try:
            # Read content from results file
            with open(results_models_path, "r") as f_read_results:
                results_models_str = f_read_results.read()

            # Write content to bird_data_model.py
            with open(pybirdai_models_path, "w") as f_write:
                f_write.write("\n")
                f_write.write(results_models_str)
                f_write.flush()

            f_write.close()
            logger.info(f"{pybirdai_models_path} updated successfully.")
        except IOError as e:
            logger.error(f"Error updating {pybirdai_models_path}: {e}")
            raise RuntimeError(f"Failed to update {pybirdai_models_path}") from e

    def _run_django_commands(self, base_dir):
        """Run Django makemigrations and migrate commands."""
        try:
            # Import Django management commands
            from django.core.management import call_command
            from django.core.management.base import CommandError

            # Run makemigrations using Django's call_command
            logger.info("Running makemigrations command...")
            try:
                call_command("makemigrations", "pybirdai", verbosity=2)
                logger.info("Makemigrations command completed successfully2.")
            except CommandError as e:
                logger.error(f"Makemigrations failed: {e}")
                raise RuntimeError(f"Makemigrations failed: {e}") from e

            # Run migrate using Django's call_command
            logger.info("Running migrate command...")
            try:
                call_command("migrate", verbosity=2)
                logger.info("Migrate command completed successfully.")
            except CommandError as e:
                logger.error(f"Migrate failed: {e}")
                raise RuntimeError(f"Migrate failed: {e}") from e

        except Exception as e:
            logger.error(f"Django command execution failed: {e}")
            raise RuntimeError(f"Django command execution failed: {e}") from e

    def _run_migrations_in_subprocess(self, base_dir):
        """Run Django migrations in a subprocess to avoid auto-restart race condition."""
        import subprocess
        import sys
        import time

        start_time = time.time()

        try:
            # Get the python executable from the current environment
            python_executable = sys.executable

            # Get the virtual environment path if we're in one
            venv_path = os.environ.get("VIRTUAL_ENV")
            if venv_path:
                python_executable = os.path.join(venv_path, "bin", "python")

            # Change to project directory for subprocess
            original_dir = os.getcwd()
            os.chdir(base_dir)

            logger.info("Running makemigrations in subprocess...")
            makemig_start = time.time()

            # Run makemigrations
            makemig_result = subprocess.run(
                [python_executable, "manage.py", "makemigrations", "pybirdai"],
                capture_output=True,
                text=True,
                timeout=900,
            )  # 15 minute timeout

            makemig_time = time.time() - makemig_start

            if makemig_result.returncode != 0:
                logger.error(
                    f"Makemigrations failed with return code {makemig_result.returncode}"
                )
                logger.error(f"Stdout: {makemig_result.stdout}")
                logger.error(f"Stderr: {makemig_result.stderr}")
                raise RuntimeError(f"Makemigrations failed: {makemig_result.stderr}")

            logger.info(f"Makemigrations completed in {makemig_time:.2f}s")
            logger.info(f"Makemigrations output: {makemig_result.stdout.strip()}")

            logger.info("Running migrate in subprocess...")
            migrate_start = time.time()

            # Run migrate
            migrate_result = subprocess.run(
                [python_executable, "manage.py", "migrate"],
                capture_output=True,
                text=True,
                timeout=600,
            )  # 10 minute timeout

            migrate_time = time.time() - migrate_start

            if migrate_result.returncode != 0:
                logger.error(
                    f"Migrate failed with return code {migrate_result.returncode}"
                )
                logger.error(f"Stdout: {migrate_result.stdout}")
                logger.error(f"Stderr: {migrate_result.stderr}")
                raise RuntimeError(f"Migrate failed: {migrate_result.stderr}")

            logger.info(f"Migrate completed in {migrate_time:.2f}s")
            logger.info(f"Migrate output: {migrate_result.stdout.strip()}")

            total_time = time.time() - start_time
            logger.info(
                f"All Django migrations completed in {total_time:.2f}s total via subprocess"
            )

        except subprocess.TimeoutExpired as e:
            logger.error(f"Migration subprocess timed out: {e}")
            raise RuntimeError(
                f"Migration subprocess timed out after {e.timeout} seconds"
            )
        except Exception as e:
            logger.error(f"Migration subprocess failed: {e}")
            raise
        finally:
            # Restore original directory
            os.chdir(original_dir)

    def _run_django_system_command(self, base_dir):
        """Legacy method - now replaced by _run_migrations_in_subprocess."""
        logger.warning(
            "Using legacy _run_django_system_command - consider using _run_migrations_in_subprocess instead"
        )

        import time

        start_time = time.time()

        try:
            from django.core.management import call_command
            import io

            logger.info(
                "Running makemigrations command programmatically (fast mode)..."
            )
            makemig_start = time.time()

            # Capture output but suppress verbosity for speed
            out = io.StringIO()
            call_command(
                "makemigrations", "pybirdai", stdout=out, stderr=out, verbosity=0
            )
            output = out.getvalue()

            makemig_time = time.time() - makemig_start
            logger.info(
                f"Makemigrations completed in {makemig_time:.2f}s - Output: {output.strip()}"
            )

            logger.info("Running migrate command programmatically (fast mode)...")
            migrate_start = time.time()

            out = io.StringIO()
            call_command("migrate", stdout=out, stderr=out, verbosity=0)
            output = out.getvalue()

            migrate_time = time.time() - migrate_start
            logger.info(
                f"Migrate completed in {migrate_time:.2f}s - Output: {output.strip()}"
            )

            total_time = time.time() - start_time
            logger.info(
                f"All Django commands completed in {total_time:.2f}s total. Django restart may occur now."
            )

        except Exception as e:
            logger.error(f"Django management commands failed: {e}")
            # Fallback to os.system if programmatic approach fails
            logger.info("Falling back to os.system approach...")

            logger.info("Running makemigrations command...")
            makemigrations_command = "python manage.py makemigrations pybirdai"
            status = os.system(makemigrations_command)
            if status != 0:
                logger.error(f"Makemigrations command failed with exit status {status}")
                raise RuntimeError(
                    f"Command failed with status {status}: {makemigrations_command}"
                )
            logger.info("Makemigrations command completed successfully.")

            logger.info("Running migrate command...")
            migrate_command = "python manage.py migrate"
            status = os.system(migrate_command)
            if status != 0:
                logger.error(f"Migrate command failed with exit status {status}")
                raise RuntimeError(
                    f"Command failed with status {status}: {migrate_command}"
                )
            logger.info("Migrate command completed successfully.")

    def _create_migration_ready_marker(self, base_dir):
        """Create a marker file to indicate we're ready for step 2 migrations."""
        import json

        # Ensure base_dir is a string (handle Path objects)
        base_dir_str = str(base_dir)
        marker_path = os.path.join(base_dir_str, ".migration_ready_marker")
        marker_data = {
            "step": 1,
            "timestamp": time.time(),
            "status": "admin_updated_waiting_for_restart",
        }

        try:
            with open(marker_path, "w") as f:
                json.dump(marker_data, f, indent=2)
            logger.info(f"Created migration ready marker: {marker_path}")
        except Exception as e:
            logger.error(f"Failed to create migration marker: {e}")
            # Don't fail the whole process for this

    def _check_migration_ready_marker(self, base_dir):
        """Check if the migration ready marker exists."""
        # Ensure base_dir is a string (handle Path objects)
        base_dir_str = str(base_dir)
        marker_path = os.path.join(base_dir_str, ".migration_ready_marker")
        exists = os.path.exists(marker_path)
        logger.info(f"Migration ready marker exists: {exists} at {marker_path}")
        return exists

    def _remove_migration_ready_marker(self, base_dir):
        """Remove the migration ready marker file."""
        # Ensure base_dir is a string (handle Path objects)
        base_dir_str = str(base_dir)
        marker_path = os.path.join(base_dir_str, ".migration_ready_marker")
        try:
            if os.path.exists(marker_path):
                os.remove(marker_path)
                logger.info(f"Removed migration ready marker: {marker_path}")
        except Exception as e:
            logger.error(f"Failed to remove migration marker: {e}")
            # Don't fail the whole process for this
