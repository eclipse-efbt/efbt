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
from pybirdai.entry_points.generate_derived_fields import (
    run_generate_derivation_files,
    export_available_rules_to_config,
)
from pybirdai.utils.speed_improvements_initial_migration.derived_fields_extractor import (
    merge_derived_fields_into_original_model,
)
from pybirdai.utils.speed_improvements_initial_migration.artifact_fetcher import PreconfiguredDatabaseFetcher
from pybirdai.utils.speed_improvements_initial_migration.advanced_migration_generator import AdvancedMigrationGenerator
from django.conf import settings
import psutil

from importlib import metadata

# Create a logger
logger = logging.getLogger(__name__)

class RunAutomodeDatabaseSetup(AppConfig):
    """
    Entry point for automode database setup that performs all necessary
    steps to create and configure the BIRD database automatically.
    """

    def __init__(self, app_name, app_module, *args, **kwargs):
        self.app_name = app_name
        self.app_module = app_module
        self.token = ""
        for k,v in kwargs.items():
            if k == "token":
                self.token = v
                break

    def run_automode_database_setup(self):
        """
        Execute the complete database setup process for automode.
        This version avoids operations that could cause server restarts.
        """
        try:
            logger.info("Starting automode database setup...")

            base_dir = settings.BASE_DIR
            migration_file_path = os.path.join(
                base_dir,
                "pybirdai",
                "migrations"
            )
            for file in os.listdir(migration_file_path):
                if file.endswith(".py") and not file.startswith("__"):
                    logger.info(f"Processing file: {file}")
                    os.remove(os.path.join(migration_file_path, file))

            admin_file_path = os.path.join(
                base_dir,
                "pybirdai",
                "admin.py"
            )

            bird_data_model_path = os.path.join(
                base_dir,
                "pybirdai",
                "models",
                "bird_data_model.py"
            )

            if os.path.exists(admin_file_path):
                with open(admin_file_path) as rf:
                    with open(admin_file_path,"w") as wf:
                        wf.write(rf.read().split("\n\n")[0])

            if os.path.exists(bird_data_model_path):
                with open(bird_data_model_path,"w") as wf:
                    wf.write("")

            # Step 1a: Clean environment
            logger.info("Step 1a: Environment cleaned successfully.")

            # Step 1b: Generate derivation Python files from transformation rules CSV
            # This must happen BEFORE user can select which derived fields to include
            logger.info("Step 1b: Generating derivation Python files from transformation rules...")

            transformation_rules_csv = os.path.join(
                base_dir,
                'resources',
                'technical_export',
                'logical_transformation_rule.csv'
            )

            if os.path.exists(transformation_rules_csv):
                try:
                    # Generate Python derivation files for all available rules
                    generated_output_dir = os.path.join(
                        base_dir, 'resources', 'derivation_files', 'generated'
                    )
                    generated_files = run_generate_derivation_files(
                        transformation_rules_csv=transformation_rules_csv,
                        output_dir=generated_output_dir
                    )
                    logger.info(f"Generated {len(generated_files)} derivation file(s)")

                    # Export available rules to config CSV for the UI
                    config_csv = os.path.join(
                        base_dir, 'resources', 'derivation_files', 'derivation_config.csv'
                    )
                    export_available_rules_to_config(
                        transformation_rules_csv=transformation_rules_csv,
                        config_csv=config_csv,
                        enabled_by_default=False
                    )
                    logger.info(f"Exported available rules to config: {config_csv}")

                except Exception as e:
                    logger.warning(f"Could not generate derivation files: {e}")
                    logger.info("Derivation configuration will be limited without generated files.")
            else:
                logger.info(f"Transformation rules CSV not found: {transformation_rules_csv}")
                logger.info("Download technical exports from ECB to enable derivation configuration.")

            # Model files will be generated in run_post_setup_operations() after user configures derivations
            logger.info(f"Base directory: {base_dir}")
            logger.info("Environment cleaned. Ready for derivation configuration.")
            logger.info("User can now configure derived fields before model generation.")

            # Load temporary configuration if needed
            try:
                config = self._load_temp_config()

                # Handle generated Python files if needed (for full execution mode)
                if config and config.get("when_to_stop") == "FULL_EXECUTION":
                    logger.info(
                        "Transferring generated Python files for full execution..."
                    )
                    self._transfer_generated_python_files()

                logger.info("Artifact retrieval phase completed.")
                logger.info(
                    "Next step: Configure derivation rules, then run 'Setup Database' to generate models."
                )

            except Exception as e:
                logger.error(f"Failed to complete artifact retrieval: {str(e)}")
                raise RuntimeError(f"Artifact retrieval failed: {str(e)}") from e

            # Return success - no restart needed yet, models not generated
            logger.info("Artifact retrieval completed successfully!")
            logger.info(
                "User should now configure derivation rules before clicking 'Setup Database'."
            )

            # Create migration ready marker so "Setup Database" button becomes enabled
            self._create_migration_ready_marker(base_dir)

            return {
                "success": True,
                "message": "Artifacts retrieved and derivation files generated. Configure derivation rules, then click 'Setup Database'.",
                "server_restart_required": False,
                "next_step": "Configure derivation rules and click 'Setup Database'",
                "steps_completed": [
                    "Environment cleaned",
                    "Derivation Python files generated from transformation rules",
                    "Derivation config CSV exported for UI",
                    "Ready for derivation configuration",
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
        STEP 2: Generate models with derivation rules, update admin.py, and trigger restart.

        This step is called AFTER the user has configured derivation rules.
        Sequence:
        1. Generate Django models from LDM
        2. Copy models to bird_data_model.py
        3. Merge enabled derivation rules into model
        4. Update admin.py
        5. Create migration marker (triggers restart)

        After restart, user will need to run migrations separately.
        """
        try:
            logger.info("Starting post-setup operations - STEP 2: Model generation and admin update...")

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
                base_dir, "pybirdai" + os.sep + "models" + os.sep + "bird_meta_data_model.py"
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
                base_dir, "pybirdai" + os.sep + "models" + os.sep + "bird_data_model.py"
            )
            results_models_path = os.path.join(
                base_dir,
                "results"
                + os.sep
                + "database_configuration_files"
                + os.sep
                + "models.py",
            )

            # Step 2a: Generate Django models (moved from run_automode_database_setup)
            # This happens AFTER user has configured derivation rules
            logger.info("Step 2a: Generating Django models from LDM...")
            try:
                app_config = RunCreateDjangoModels(self.app_name, self.app_module)
                app_config.ready()
                logger.info("Django models generated successfully.")
            except Exception as e:
                logger.error(f"Failed to create Django models: {str(e)}")
                raise RuntimeError(f"Django model creation failed: {str(e)}") from e

            # Verify generated files exist
            if not os.path.exists(results_models_path):
                raise RuntimeError(
                    f"Generated models file not found: {results_models_path}"
                )
            logger.info(f"Generated models file found: {results_models_path}")

            # Step 2b: Update models file
            logger.info("Step 2b: Copying generated models to bird_data_model.py...")
            self._update_models_file(pybirdai_models_path, results_models_path)

            # Step 2c: Merge derived fields (based on user's derivation configuration)
            logger.info("Step 2c: Merging derived fields into model...")

            derived_fields_file_path = os.path.join(
                base_dir,
                "resources"
                + os.sep
                + "derivation_files"
                + os.sep
                + "derived_field_configuration.py",
            )

            os.makedirs(os.path.dirname(derived_fields_file_path), exist_ok=True)

            merge_derived_fields_into_original_model(
                pybirdai_models_path, derived_fields_file_path
            )
            logger.info("Derived fields merged successfully.")

            # Step 2d: Update admin.py
            logger.info("Step 2d: Updating admin.py...")
            self._update_admin_file(
                pybirdai_admin_path, pybirdai_meta_data_model_path, pybirdai_models_path
            )

            # Create a marker file to indicate we're ready for migrations
            self._create_migration_ready_marker(base_dir)

            logger.info("STEP 2 completed successfully!")
            logger.info(
                "Django will restart now. After restart, run migrations to complete database setup."
            )

            return {
                "success": True,
                "step": 2,
                "message": "Models generated and admin files updated. Server will restart.",
                "next_action": "run_migrations_after_restart",
                "server_restart_required": True,
                "steps_completed": [
                    "Django models generated from LDM",
                    "Models copied to bird_data_model.py",
                    "Derived fields merged",
                    "Admin.py updated",
                ],
            }

        except Exception as e:
            logger.error(f"Post-setup operations STEP 2 failed: {str(e)}")
            raise

    def run_migrations_after_restart(self):
        """
        STEP 3: Run migrations after Django has restarted with updated admin.py and models.
        """
        try:
            logger.info("Starting STEP 3: Running migrations after restart...")
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

            logger.info("STEP 3 completed successfully!")
            logger.info("Database setup is now complete!")

            return {
                "success": True,
                "step": 3,
                "message": "Database migrations completed successfully",
                "database_ready": True,
            }

        except Exception as e:
            logger.error(f"Post-setup operations STEP 3 failed: {str(e)}")
            raise

    def _cleanup_files(self, initial_migration_file, db_file):
        """Remove existing migration and database files."""
        # Remove initial migration file
        if os.path.exists(initial_migration_file):
            try:
                # os.remove(initial_migration_file)
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
        self, pybirdai_admin_path, pybirdai_meta_data_model_path, pybirdai_data_model_path
    ):
        """Update the admin.py file with model registrations, avoiding duplicates."""
        import glob

        registered_models = set()
        models_dir = os.path.join(os.path.dirname(pybirdai_admin_path), "models")

        # Create initial admin.py content
        with open(pybirdai_admin_path, "w") as f_write:
            f_write.write("from django.contrib import admin\n\n")

            # Process all Python files in the models directory
            for model_file_path in glob.glob(os.path.join(models_dir, "*.py")):
                if model_file_path.endswith("__init__.py"):
                    continue

                model_filename = os.path.basename(model_file_path)
                model_module_name = model_filename[:-3]  # Remove .py extension

                try:
                    with open(model_file_path, "r") as f_read:
                        file_content = f_read.read()
                        tree = ast.parse(file_content)

                        # Find all class definitions in the file
                        for node in ast.walk(tree):
                            if isinstance(node, ast.ClassDef) and node.name not in [
                                "Meta", "Admin"
                            ]:
                                # Check if this is an abstract model
                                is_abstract = self._is_abstract_model(node, file_content)

                                if not is_abstract and node.name not in registered_models:
                                    f_write.write(
                                        f"from .models.{model_module_name} import {node.name}\n"
                                    )
                                    f_write.write(f"admin.site.register({node.name})\n")
                                    registered_models.add(node.name)

                except Exception as e:
                    logger.warning(f"Error processing model file {model_file_path}: {e}")
                    continue

    def _is_abstract_model(self, class_node, file_content):
        """Check if a Django model class is abstract by examining its Meta class."""
        for node in class_node.body:
            if (isinstance(node, ast.ClassDef) and
                node.name == "Meta"):
                # Check if Meta class contains abstract = True
                for meta_node in node.body:
                    if (isinstance(meta_node, ast.Assign) and
                        any(isinstance(target, ast.Name) and target.id == "abstract"
                            for target in meta_node.targets)):
                        # Check if the value is True
                        if (isinstance(meta_node.value, ast.Constant) and
                            meta_node.value.value is True):
                            return True
                        elif (isinstance(meta_node.value, ast.NameConstant) and
                              meta_node.value.value is True):  # Python < 3.8 compatibility
                            return True
        return False







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

        # Read content from results file
        with open(results_models_path, "r") as f_read_results:
            results_models_str = f_read_results.read()

        # Write content to bird_data_model.py
        with open(pybirdai_models_path, "w") as f_write:
            f_write.write("\n")
            f_write.write(results_models_str)

        logger.info(f"{pybirdai_models_path} updated successfully.")

    def _fetch_preconfigured_database(self,python_executable):
        fetcher = PreconfiguredDatabaseFetcher(self.token)
        db_content = fetcher.fetch()
        db_file = "db.sqlite3"
        if os.path.exists(db_file):
            os.chmod(db_file, 0o666)

        if db_content:
            logger.info("Database content fetched, extracting...")
            success = fetcher.extract_zip_and_save(db_content)
            if success:
                logger.info("Process completed successfully")

        if not db_content or not success:
            logger.error("Failed to fetch database content")
            return 1, None

        os.listdir(f"pybirdai{os.sep}migrations")

        found_migration_files = []
        for file in os.listdir(f"pybirdai{os.sep}migrations"):
            if file.endswith(".py") and file != "__init__.py":
                logger.info(f"Found migration file: {file}")
                found_migration_files.append(file)

        found_migration_files = sorted(found_migration_files,
            key=lambda x: int(x.split("_")[0]))

        for file in found_migration_files:
            fake_migrate_cmd = [python_executable, "manage.py", "migrate", "--fake", "pybirdai", file.replace(".py", "")]
            migrate_result = subprocess.run(
                fake_migrate_cmd,
                capture_output=True,
                text=True,
                timeout=600,
                shell=(os.name == 'nt'),  # Use shell=True on Windows
            )

        return 0, migrate_result

    def _get_python_exc(self):
        # Get the python executable from the current environment
        python_executable = sys.executable

        # Get the virtual environment path if we're in one
        venv_path = os.environ.get("VIRTUAL_ENV")
        if venv_path:
            # Handle Windows vs Unix virtual environment structure
            if os.name == 'nt':  # Windows
                python_executable = os.path.join(venv_path, "Scripts", "python.exe")
            else:  # Unix/Linux/macOS
                python_executable = os.path.join(venv_path, "bin", "python")

        # Change to project directory for subprocess
        original_dir = os.getcwd()
        os.chdir(settings.BASE_DIR)
        return venv_path, original_dir, python_executable

    def _run_migrations_in_subprocess(self, base_dir):
        """Run Django migrations in a subprocess to avoid auto-restart race condition."""
        import subprocess
        import sys
        import time

        original_dir = os.getcwd()  # Ensure this is always set
        start_time = time.time()

        try:
            db_file = "db.sqlite3"
            if os.path.exists(db_file):
                os.chmod(db_file, 0o666)
                os.remove(db_file)

            venv_path, _, python_executable = self._get_python_exc()

            generator = AdvancedMigrationGenerator()
            models = generator.parse_files([f"pybirdai{os.sep}models{os.sep}bird_data_model.py", f"pybirdai{os.sep}models{os.sep}bird_meta_data_model.py"])
            _ = generator.generate_migration_code(models)
            generator.save_migration_file(models, f"pybirdai{os.sep}migrations{os.sep}0001_initial.py")

            logger.info("Running makemigrations in subprocess...")
            logger.info(f"Using Python executable: {python_executable}")
            logger.info(f"Current working directory: {os.getcwd()}")
            makemig_start = time.time()

            # Run makemigrations with proper Windows handling
            makemig_cmd = [python_executable, "manage.py", "makemigrations", "pybirdai"]
            logger.info(f"Running command: {' '.join(makemig_cmd)}")

            makemig_result = subprocess.run(
                makemig_cmd,
                capture_output=True,
                text=True,
                timeout=900,
                shell=(os.name == 'nt'),  # Use shell=True on Windows
            )  # 15 minute timeout

            makemig_time = time.time() - makemig_start

            if makemig_result.returncode != 0:
                logger.error(
                    f"Makemigrations failed with return code {makemig_result.returncode}"
                )
                logger.error(f"Python executable used: {python_executable}")
                logger.error(f"Python executable exists: {os.path.exists(python_executable)}")
                logger.error(f"manage.py exists: {os.path.exists('manage.py')}")
                logger.error(f"Current directory: {os.getcwd()}")
                logger.error(f"Stdout: {makemig_result.stdout}")
                logger.error(f"Stderr: {makemig_result.stderr}")
                raise RuntimeError(f"Makemigrations failed: {makemig_result.stderr}")

            logger.info(f"Makemigrations completed in {makemig_time:.2f}s")
            logger.info(f"Makemigrations output: {makemig_result.stdout.strip()}")

            migrate_start = time.time()

            return_code_preconfigured_migration, migrate_result = True, None

            if return_code_preconfigured_migration:
                logger.info("PreconfiguredDatabaseFetcher failed, running manual process")
                logger.info("Running migrate in subprocess...")

                db_file = "db.sqlite3"
                if os.path.exists(db_file):
                    os.chmod(db_file, 0o666)
                    os.remove(db_file)

                # Run migrate
                migrate_cmd = [python_executable, "manage.py", "migrate"]
                logger.info(f"Running migrate command: {' '.join(migrate_cmd)}")

                migrate_result = subprocess.run(
                    migrate_cmd,
                    capture_output=True,
                    text=True,
                    timeout=600,
                    shell=(os.name == 'nt'),  # Use shell=True on Windows
                )  # 10 minute timeout

                if migrate_result.returncode != 0:
                    logger.error(
                        f"Migrate failed with return code {migrate_result.returncode}"
                    )
                    logger.error(f"Python executable used: {python_executable}")
                    logger.error(f"Python executable exists: {os.path.exists(python_executable)}")
                    logger.error(f"manage.py exists: {os.path.exists('manage.py')}")
                    logger.error(f"Current directory: {os.getcwd()}")
                    logger.error(f"Stdout: {migrate_result.stdout}")
                    logger.error(f"Stderr: {migrate_result.stderr}")
                    raise RuntimeError(f"Migrate failed: {migrate_result.stderr}")

            migrate_time = time.time() - migrate_start
            logger.info(f"Migrate completed in {migrate_time:.2f}s")
            logger.info(f"Migrate output: {migrate_result.stdout.strip()}")

            total_time = time.time() - start_time
            logger.info(
                f"All Django migrations completed in {total_time:.2f}s total via subprocess"
            )

            self._create_setup_ready_marker(base_dir)

        except subprocess.TimeoutExpired as e:
            logger.error(f"Migration subprocess timed out: {e}")
            raise RuntimeError(
                f"Migration subprocess timed out after {e.timeout} seconds"
            )
        except Exception as e:
            logger.error(f"Migration subprocess failed: {e}")
            raise
        finally:
            os.chdir(original_dir)

    def _create_setup_ready_marker(self, base_dir):
        """Create a marker file to indicate we're ready for step 2 migrations."""
        import json

        # Ensure base_dir is a string (handle Path objects)
        base_dir_str = str(base_dir)
        marker_path = os.path.join(base_dir_str, ".setup_ready_marker")
        marker_data = {
            "step": 2,
            "timestamp": time.time(),
            "status": "setup completed - please use the application",
        }

        try:
            with open(marker_path, "w") as f:
                json.dump(marker_data, f, indent=2)
            logger.info(f"Created setup ready marker: {marker_path}")
        except Exception as e:
            logger.error(f"Failed to create setup ready marker: {e}")
            # Don't fail the whole process for this

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

    def _check_setup_ready_marker(self, base_dir):
        """Check if the setup ready marker exists."""
        # Ensure base_dir is a string (handle Path objects)
        base_dir_str = str(base_dir)
        marker_path = os.path.join(base_dir_str, ".setup_ready_marker")
        exists = os.path.exists(marker_path)
        logger.info(f"Setup ready marker exists: {exists} at {marker_path}")
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
