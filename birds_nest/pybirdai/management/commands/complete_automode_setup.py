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

from django.core.management.base import BaseCommand
from pybirdai.entry_points.automode_database_setup import RunAutomodeDatabaseSetup
import logging
import os
import json
import shutil

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = 'Complete the automode database setup by applying file changes and running migrations'

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS('Starting automode database setup completion...'))
        
        try:
            # Step 1: Read configuration to check when_to_stop setting
            config = self._load_temp_config()
            
            # Step 2: Handle generated Python files if needed
            if config and config.get('when_to_stop') == 'FULL_EXECUTION':
                self.stdout.write(self.style.SUCCESS('Transferring generated Python files for full execution...'))
                self._transfer_generated_python_files()
            
            # Step 3: Run the standard database setup operations with subprocess approach
            app_config = RunAutomodeDatabaseSetup('pybirdai', 'birds_nest')
            app_config.run_post_setup_operations()
            
            self.stdout.write(
                self.style.SUCCESS(
                    'Automode database setup completed successfully!\n'
                    'You can now start the Django server and use the application.'
                )
            )
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f'Automode database setup failed: {str(e)}')
            )
            raise
    
    def _load_temp_config(self):
        """Load temporary configuration file using same path logic as views.py."""
        try:
            # Use the same path resolution logic as views.py to ensure consistency
            from django.conf import settings
            import tempfile
            
            base_dir = getattr(settings, 'BASE_DIR', tempfile.gettempdir())
            
            # Convert Path object to string if necessary (Django 5.x uses Path objects)
            if hasattr(base_dir, '__fspath__'):  # Check if it's a path-like object
                temp_dir = str(base_dir)
            else:
                temp_dir = base_dir
            
            # Ensure we use absolute path to avoid working directory issues
            if not os.path.isabs(temp_dir):
                temp_dir = os.path.abspath(temp_dir)
            
            temp_config_path = os.path.join(temp_dir, 'automode_config.json')
            logger.info(f"Looking for config file at: {temp_config_path}")
            
            if os.path.exists(temp_config_path):
                with open(temp_config_path, 'r') as f:
                    config = json.load(f)
                logger.info(f"Loaded automode configuration: when_to_stop = {config.get('when_to_stop')}")
                return config
            else:
                logger.warning(f"No temporary configuration file found at {temp_config_path}")
                
                # Try fallback location for backwards compatibility
                fallback_path = os.path.join('.', 'automode_config.json')
                if os.path.exists(fallback_path):
                    logger.info(f"Found config at fallback location: {fallback_path}")
                    with open(fallback_path, 'r') as f:
                        config = json.load(f)
                    return config
                
                return None
        except Exception as e:
            logger.error(f"Error loading temporary configuration: {e}")
            return None
    
    def _transfer_generated_python_files(self):
        """Transfer generated Python files from resources/generated_python to pybirdai/process_steps/filter_code."""
        source_dir = os.path.join('.', 'resources', 'generated_python')
        target_dir = os.path.join('.', 'pybirdai', 'process_steps', 'filter_code')
        
        try:
            if not os.path.exists(source_dir):
                logger.warning(f"Source directory {source_dir} does not exist - no Python files to transfer")
                self.stdout.write(self.style.WARNING(f'No generated Python files found in {source_dir}'))
                return
            
            # Ensure target directory exists
            os.makedirs(target_dir, exist_ok=True)
            
            # Find all Python files in source directory
            python_files = [f for f in os.listdir(source_dir) if f.endswith('.py')]
            
            if not python_files:
                logger.warning(f"No Python files found in {source_dir}")
                self.stdout.write(self.style.WARNING(f'No Python files found in {source_dir}'))
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
                    self.stdout.write(self.style.ERROR(f'Error transferring {file_name}: {e}'))
            
            self.stdout.write(
                self.style.SUCCESS(f'Successfully transferred {transferred_count} Python files to filter_code directory')
            )
            logger.info(f"Successfully transferred {transferred_count} generated Python files")
            
        except Exception as e:
            logger.error(f"Error during Python file transfer: {e}")
            self.stdout.write(self.style.ERROR(f'Error during Python file transfer: {e}'))
            raise 