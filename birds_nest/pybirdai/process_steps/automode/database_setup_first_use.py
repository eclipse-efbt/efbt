# coding=UTF-8
# Copyright (c) 2025 Bird Software Solutions Ltd
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDX-License-Identifier: EPL-2.0
#
# Contributors:
#    Neil Mackenzie - initial API and implementation
#    Benjamin Arfa - improvements
#
import django
import os
import sys
from django.apps import AppConfig
from django.conf import settings
import logging
from importlib import metadata
import ast

# Create a logger
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

class RunDatabaseSetup(AppConfig):
    DjangoSetup.configure_django()
    from pybirdai.entry_points.create_django_models import RunCreateDjangoModels
    app_config = RunCreateDjangoModels('pybirdai', 'birds_nest')
    app_config.ready()

    # File paths - Define paths relative to where the script is executed
    initial_migration_file = "pybirdai/migrations/0001_initial.py"
    db_file = "db.sqlite3"
    pybirdai_admin_path = "pybirdai/admin.py"
    pybirdai_meta_data_model_path = "pybirdai/models/bird_meta_data_model.py"
    results_admin_path = "results/database_configuration_files/admin.py"
    pybirdai_models_path = "pybirdai/models/bird_data_model.py" # Target file
    results_models_path = "results/database_configuration_files/models.py" # Source file

    # --- Cleanup steps ---

    # Remove initial migration file - Strict interpretation: must exist to remove
    logger.info(f"Attempting to remove initial migration file: {initial_migration_file}")
    if os.path.exists(initial_migration_file):
        try:
            os.remove(initial_migration_file)
            logger.info(f"Successfully removed {initial_migration_file}")
        except OSError as e:
            logger.error(f"Error removing file {initial_migration_file}: {e}")
            # Raising specific error for removal failure
            raise RuntimeError(f"Failed to remove file {initial_migration_file}") from e


    # Remove database file - Strict interpretation: must exist to remove
    logger.info(f"Attempting to remove database file: {db_file}")
    if os.path.exists(db_file):
        try:
            os.remove(db_file)
            logger.info(f"Successfully removed {db_file}")
        except OSError as e:
            logger.error(f"Error removing file {db_file}: {e}")
            # Raising specific error for removal failure
            raise RuntimeError(f"Failed to remove file {db_file}") from e

    # --- Update admin.py ---

    logger.info(f"Updating {pybirdai_admin_path}...")
    # Check if source file for reading (pybirdai/admin.py) exists
    with open(pybirdai_admin_path, "w") as f_write:
        f_write.write("from django.contrib import admin\n")
        with open(pybirdai_meta_data_model_path, "r") as f_read:
            tree = ast.parse(f_read.read())
            for node in ast.walk(tree):
                if isinstance(node, ast.ClassDef) and node.name != "Meta" and node.name != "Admin":
                    f_write.write(f"from .bird_meta_data_model import {node.name}\n")
                    f_write.write(f"admin.site.register({node.name})\n")

    # Check if results file for reading (results/admin.py) exists
    if not os.path.exists(results_admin_path):
        logger.error(f"Results admin file not found: {results_admin_path}")
        raise FileNotFoundError(f"Source file '{results_admin_path}' not found. Cannot read results admin.py content.")

    try:
        # Read existing admin.py content
        with open(pybirdai_admin_path, "r") as f_read:
            admin_str = f_read.read()

        # Read content from results file
        with open(results_admin_path, "r") as f_read_results:
            results_admin_str = f_read_results.read()

        # Write combined content back to admin.py (overwriting existing)
        with open(pybirdai_admin_path, "w") as f_write:
            f_write.write(admin_str)
            f_write.write("\n")
            f_write.write(results_admin_str)
        logger.info(f"{pybirdai_admin_path} updated successfully.")
    except IOError as e:
        logger.error(f"Error reading or writing files for {pybirdai_admin_path} update: {e}")
        # Raising specific error for file operation failure
        raise RuntimeError(f"Failed to update {pybirdai_admin_path}") from e


    # --- Update bird_data_model.py (models.py) ---

    logger.info(f"Updating {pybirdai_models_path}...")
    # Check if results file for reading (results/models.py) exists
    if not os.path.exists(results_models_path):
        logger.error(f"Results models file not found: {results_models_path}")
        raise FileNotFoundError(f"Source file '{results_models_path}' not found. Cannot read results models.py content.")

    try:
        # Read content from results file
        with open(results_models_path, "r") as f_read_results:
            results_models_str = f_read_results.read()

        # Write content to bird_data_model.py (overwriting)
        with open(pybirdai_models_path, "w") as f_write:
            # Original code had an extra newline here, preserving that
            f_write.write("\n")
            f_write.write(results_models_str)
        logger.info(f"{pybirdai_models_path} updated successfully.")
    except IOError as e:
        logger.error(f"Error reading or writing files for {pybirdai_models_path} update: {e}")
        # Raising specific error for file operation failure
        raise RuntimeError(f"Failed to update {pybirdai_models_path}") from e

    # --- Run Django management commands ---

    logger.info("Running makemigrations command...")

    makemigrations_command = "uv run manage.py makemigrations pybirdai"

    # Note: os.system returns exit status, 0 usually means success
    status = os.system(makemigrations_command)
    if status != 0:
        logger.error(f"Makemigrations command failed with exit status {status}")
        # Raising error if command fails
        raise RuntimeError(f"Command failed with status {status}: {makemigrations_command}")
    logger.info("Makemigrations command completed successfully.")

    logger.info("Running migrate command...")
    migrate_command = "uv run manage.py migrate"
    status = os.system(migrate_command)
    if status != 0:
        logger.error(f"Migrate command failed with exit status {status}")
        # Raising error if command fails
        raise RuntimeError(f"Command failed with status {status}: {migrate_command}")
    logger.info("Migrate command completed successfully.")

    logger.info("Running runserver command...")
    runserver_command = "uv run manage.py runserver"
    # Note: runserver is a blocking command that starts the server.
    # The script will likely pause here until the server is stopped.
    # Added logging but not checking exit status as it's a server command
    # that isn't expected to exit with 0 in normal use.
    os.system(runserver_command)
    logger.info("Runserver command finished (server stopped or failed to start).")


if __name__ == "__main__":
    RunDatabaseSetup()
