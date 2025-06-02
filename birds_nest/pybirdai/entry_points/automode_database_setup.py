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
import logging
import subprocess
from django.conf import settings
from django.apps import AppConfig
from pybirdai.entry_points.create_django_models import RunCreateDjangoModels

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
            results_admin_path = os.path.join(base_dir, "results/database_configuration_files/admin.py")
            results_models_path = os.path.join(base_dir, "results/database_configuration_files/models.py")
            
            logger.info(f"Base directory: {base_dir}")
            logger.info(f"Results models path exists: {os.path.exists(results_models_path)}")
            logger.info(f"Results admin path exists: {os.path.exists(results_admin_path)}")
            
            if not os.path.exists(results_models_path):
                raise RuntimeError(f"Generated models file not found: {results_models_path}")
            
            if not os.path.exists(results_admin_path):
                logger.warning(f"Generated admin file not found: {results_admin_path}")
            
            # Step 3: Instead of modifying files during request, prepare instructions
            logger.info("Step 2: Database setup preparation completed.")
            logger.info("Step 3: Generated configuration files are ready.")
            logger.info("Step 4: Manual migration step required (see instructions).")
            
            # Return success with instructions for manual steps
            logger.info("Automode database setup preparation completed successfully!")
            logger.info("IMPORTANT: To complete the setup, please:")
            logger.info("1. Stop the Django server")
            logger.info("2. Run the post-setup script to apply file changes and migrations")
            logger.info("3. Restart the server")
            
            return True
            
        except Exception as e:
            logger.error(f"Automode database setup failed: {str(e)}")
            raise
    
    def run_post_setup_operations(self):
        """
        Run the operations that require server restart separately.
        This should be called when the server is not running.
        """
        try:
            logger.info("Starting post-setup operations...")
            
            base_dir = settings.BASE_DIR
            initial_migration_file = os.path.join(base_dir, "pybirdai/migrations/0001_initial.py")
            db_file = os.path.join(base_dir, "db.sqlite3")
            pybirdai_admin_path = os.path.join(base_dir, "pybirdai/admin.py")
            pybirdai_meta_data_model_path = os.path.join(base_dir, "pybirdai/bird_meta_data_model.py")
            results_admin_path = os.path.join(base_dir, "results/database_configuration_files/admin.py")
            pybirdai_models_path = os.path.join(base_dir, "pybirdai/bird_data_model.py")
            results_models_path = os.path.join(base_dir, "results/database_configuration_files/models.py")
            
            # Cleanup existing files
            logger.info("Cleaning up existing files...")
            self._cleanup_files(initial_migration_file, db_file)
            
            # Update admin.py
            logger.info("Updating admin.py...")
            self._update_admin_file(pybirdai_admin_path, pybirdai_meta_data_model_path, results_admin_path)
            
            # Update models
            logger.info("Updating bird_data_model.py...")
            self._update_models_file(pybirdai_models_path, results_models_path)
            
            # Run Django management commands
            logger.info("Running Django management commands...")
            self._run_django_commands(base_dir)
            
            logger.info("Post-setup operations completed successfully!")
            return True
            
        except Exception as e:
            logger.error(f"Post-setup operations failed: {str(e)}")
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
                raise RuntimeError(f"Failed to remove file {initial_migration_file}") from e
        
        # Remove database file
        if os.path.exists(db_file):
            try:
                os.remove(db_file)
                logger.info(f"Successfully removed {db_file}")
            except OSError as e:
                logger.error(f"Error removing file {db_file}: {e}")
                raise RuntimeError(f"Failed to remove file {db_file}") from e
    
    def _update_admin_file(self, pybirdai_admin_path, pybirdai_meta_data_model_path, results_admin_path):
        """Update the admin.py file with model registrations."""
        # Create initial admin.py content
        with open(pybirdai_admin_path, "w") as f_write:
            f_write.write("from django.contrib import admin\n")
            
            # Parse the meta data model file to find class definitions
            with open(pybirdai_meta_data_model_path, "r") as f_read:
                tree = ast.parse(f_read.read())
                for node in ast.walk(tree):
                    if isinstance(node, ast.ClassDef) and node.name not in ["Meta", "Admin"]:
                        f_write.write(f"from .bird_meta_data_model import {node.name}\n")
                        f_write.write(f"admin.site.register({node.name})\n")
        
        # Check if results admin file exists and append its content
        if not os.path.exists(results_admin_path):
            logger.warning(f"Results admin file not found: {results_admin_path}")
            return
        
        try:
            # Read existing admin.py content
            with open(pybirdai_admin_path, "r") as f_read:
                admin_str = f_read.read()
            
            # Read content from results file
            with open(results_admin_path, "r") as f_read_results:
                results_admin_str = f_read_results.read()
            
            # Write combined content back to admin.py
            with open(pybirdai_admin_path, "w") as f_write:
                f_write.write(admin_str)
                f_write.write("\n")
                f_write.write(results_admin_str)
            
            logger.info(f"{pybirdai_admin_path} updated successfully.")
        except IOError as e:
            logger.error(f"Error updating {pybirdai_admin_path}: {e}")
            raise RuntimeError(f"Failed to update {pybirdai_admin_path}") from e
    
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
                call_command('makemigrations', verbosity=2)
                logger.info("Makemigrations command completed successfully.")
            except CommandError as e:
                logger.error(f"Makemigrations failed: {e}")
                raise RuntimeError(f"Makemigrations failed: {e}") from e
            
            # Run migrate using Django's call_command
            logger.info("Running migrate command...")
            try:
                call_command('migrate', verbosity=2)
                logger.info("Migrate command completed successfully.")
            except CommandError as e:
                logger.error(f"Migrate failed: {e}")
                raise RuntimeError(f"Migrate failed: {e}") from e
                
        except Exception as e:
            logger.error(f"Django command execution failed: {e}")
            raise RuntimeError(f"Django command execution failed: {e}") from e 