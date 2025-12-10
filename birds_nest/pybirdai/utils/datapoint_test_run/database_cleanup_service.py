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
Database Cleanup Service

This service provides Django ORM-based cleanup functionality for the test system.
It handles proper deletion order based on foreign key relationships and provides
conditional cleanup with batch processing for large datasets.
"""

import ast
import logging
import os
import sys
import django
from typing import List, Tuple, Dict, Type, Set
from django.db import models, transaction
from django.conf import settings

# Configure logging
logger = logging.getLogger(__name__)

class DatabaseCleanupService:
    """
    Service for cleaning up BIRD data model tables using Django ORM.

    This class provides methods to delete all records from BIRD data model
    tables in the correct order to handle foreign key dependencies.
    """

    # Cache for dynamically discovered allowed tables
    _allowed_tables_cache = None

    def __init__(self):
        """Initialize the cleanup service and ensure Django is configured."""
        self._setup_django()
        self._bird_models = None
        self._deletion_order = None

    def _setup_django(self):
        """Ensure Django is properly configured."""
        if not settings.configured:
            os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'birds_nest.settings')
            django.setup()

    def _parse_bird_data_models(self) -> Set[str]:
        """
        Parse bird_data_model.py using AST to dynamically extract model class names.

        Returns:
            Set of model class names that inherit from models.Model
        """
        try:
            # Get the path to bird_data_model.py
            current_dir = os.path.dirname(os.path.abspath(__file__))
            models_dir = os.path.join(current_dir, '..', '..', 'models')
            bird_data_model_path = os.path.join(models_dir, 'bird_data_model.py')

            if not os.path.exists(bird_data_model_path):
                logger.error(f"bird_data_model.py not found at {bird_data_model_path}")
                return set()

            with open(bird_data_model_path, 'r', encoding='utf-8') as f:
                tree = ast.parse(f.read())

            model_names = set()
            for node in ast.walk(tree):
                if isinstance(node, ast.ClassDef):
                    # Check if class inherits from models.Model
                    for base in node.bases:
                        if (isinstance(base, ast.Attribute) and
                            isinstance(base.value, ast.Name) and
                            base.value.id == 'models' and
                            base.attr == 'Model'):
                            model_names.add(node.name)
                            break

            logger.debug(f"Found {len(model_names)} model classes in bird_data_model.py")
            return model_names

        except Exception as e:
            logger.error(f"Error parsing bird_data_model.py: {e}")
            return set()

    def _get_allowed_tables(self) -> Set[str]:
        """
        Get the allowed BIRD data tables using dynamic discovery with fallback.

        Returns:
            Set of allowed table names for cleanup operations
        """
        if self._allowed_tables_cache is not None:
            return self._allowed_tables_cache

        model_names = self._parse_bird_data_models()

        if model_names:
            # Convert model names to Django table names (app_name + model_name.lower())
            ast_table_names = {f'pybirdai_{name.lower()}' for name in model_names}
            logger.debug(f"AST discovered {len(ast_table_names)} table names")
        else:
            ast_table_names = set()

        self._allowed_tables_cache = ast_table_names
        return self._allowed_tables_cache

    def _get_bird_models(self) -> List[Type[models.Model]]:
        """
        Get all BIRD data model classes using dynamic discovery.

        Uses AST parsing to discover model class names and dynamically imports them.

        Returns:
            List of Django model classes from bird_data_model
        """
        if self._bird_models is None:
            import importlib

            try:
                # Get model names from AST parsing
                model_names = self._parse_bird_data_models()

                if not model_names:
                    logger.error("No model classes found via AST parsing")
                    self._bird_models = []
                    return self._bird_models

                # Dynamically import the bird_data_model module
                bird_module = importlib.import_module('pybirdai.models.bird_data_model')

                # Dynamically get model classes
                self._bird_models = []
                for model_name in model_names:
                    try:
                        model_class = getattr(bird_module, model_name)
                        # Verify it's actually a Django model
                        if issubclass(model_class, models.Model):
                            self._bird_models.append(model_class)
                        else:
                            logger.warning(f"Class {model_name} is not a Django model")
                    except AttributeError:
                        logger.warning(f"Model class {model_name} not found in bird_data_model module")
                    except TypeError:
                        logger.warning(f"Class {model_name} is not a valid class")

                logger.debug(f"Successfully imported {len(self._bird_models)} BIRD model classes dynamically")

            except ImportError as e:
                logger.error(f"Failed to import bird_data_model module: {e}")
                self._bird_models = []
            except Exception as e:
                logger.error(f"Error during dynamic model discovery: {e}")
                self._bird_models = []

        return self._bird_models

    def _get_deletion_order(self) -> List[Type[models.Model]]:
        """
        Determine the correct order for deleting models based on foreign key dependencies.

        For BIRD data models, we use a simplified approach that deletes assignment/relationship
        tables first, then main entity tables, ensuring foreign key constraints are respected.

        Returns:
            List of model classes in deletion order
        """
        if self._deletion_order is None:
            models = self._get_bird_models()

            # Create model name to model mapping for easier organization
            model_map = {model.__name__: model for model in models}

            # Organize models by dependency levels
            # Level 1: Assignment and relationship tables (highest dependencies)
            assignment_models = [
                model for model in models
                if 'ASSGNMNT' in model.__name__ or 'RL' in model.__name__
            ]

            # Level 2: Position and transaction tables
            position_models = [
                model for model in models
                if 'PSTN' in model.__name__ and model not in assignment_models
            ]

            # Level 3: Complex entity tables with dependencies
            complex_entity_models = [
                model for model in models
                if any(keyword in model.__name__ for keyword in ['HDG', 'CMPNNT', 'ENCMBRNC', 'PRTCTN'])
                and model not in assignment_models and model not in position_models
            ]

            # Level 4: Basic entity tables
            basic_entity_models = [
                model for model in models
                if model not in assignment_models
                and model not in position_models
                and model not in complex_entity_models
            ]

            # Combine in deletion order: most dependent first
            self._deletion_order = (
                assignment_models +
                position_models +
                complex_entity_models +
                basic_entity_models
            )

            # Ensure all models are included
            included_models = set(self._deletion_order)
            for model in models:
                if model not in included_models:
                    self._deletion_order.append(model)

        return self._deletion_order

    def check_table_emptiness(self) -> Dict[str, int]:
        """
        Check which BIRD tables have data.

        Returns:
            Dictionary mapping table names to record counts
        """
        table_counts = {}
        models = self._get_bird_models()

        for model in models:
            try:
                count = model.objects.count()
                table_name = model._meta.db_table
                table_counts[table_name] = count
                if count > 0:
                    logger.debug(f"Table {table_name} has {count} records")
            except Exception as e:
                logger.error(f"Error checking table {model.__name__}: {e}")
                table_counts[model._meta.db_table] = -1

        return table_counts

    def cleanup_bird_data_tables(self, force: bool = False, batch_size: int = 1000, use_sql_fallback: bool = True) -> Dict[str, int]:
        """
        Clean up all BIRD data model tables using Django ORM.

        Args:
            force: If True, delete all records regardless of table state.
                  If False, only delete if tables are not empty.
            batch_size: Number of records to delete in each batch (for large tables)
            use_sql_fallback: If True, use raw SQL DELETE as fallback for constraint errors

        Returns:
            Dictionary mapping table names to number of deleted records
        """
        logger.info("Starting BIRD data tables cleanup using Django ORM")

        # Check table state first
        table_counts = self.check_table_emptiness()
        total_records = sum(count for count in table_counts.values() if count > 0)

        if not force and total_records == 0:
            logger.info("All BIRD tables are already empty, skipping cleanup")
            return {}

        logger.info(f"Found {total_records} total records across {len([c for c in table_counts.values() if c > 0])} non-empty tables")

        deletion_results = {}
        models_in_order = self._get_deletion_order()

        # Process each table individually to avoid transaction conflicts
        for model in models_in_order:
            table_name = model._meta.db_table
            try:
                initial_count = model.objects.count()

                if initial_count > 0:
                    logger.debug(f"Deleting {initial_count} records from {table_name}")

                    # For large tables, delete in batches to avoid "too many SQL variables" error
                    if initial_count > batch_size:
                        total_deleted = 0
                        while model.objects.exists():
                            with transaction.atomic():
                                # Get primary keys for a batch
                                pks = list(model.objects.values_list('pk', flat=True)[:batch_size])
                                if not pks:
                                    break
                                deleted_count, _ = model.objects.filter(pk__in=pks).delete()
                                total_deleted += deleted_count
                                logger.debug(f"Deleted batch of {deleted_count} records from {table_name}")

                        deletion_results[table_name] = total_deleted
                        logger.info(f"Deleted {total_deleted} records from {table_name} in batches")
                    else:
                        # For smaller tables, delete all at once
                        with transaction.atomic():
                            deleted_count, _ = model.objects.all().delete()
                            deletion_results[table_name] = deleted_count
                            logger.debug(f"Deleted {deleted_count} records from {table_name}")
                else:
                    deletion_results[table_name] = 0

            except Exception as e:
                logger.error(f"Error deleting from {model.__name__} using ORM: {e}")

                # Try SQL fallback if enabled
                if use_sql_fallback:
                    try:
                        logger.info(f"Attempting SQL fallback cleanup for {table_name}")
                        deleted_count = self._sql_cleanup_table(table_name)
                        deletion_results[table_name] = deleted_count
                        logger.info(f"SQL fallback succeeded: deleted {deleted_count} records from {table_name}")
                    except Exception as sql_e:
                        logger.error(f"SQL fallback also failed for {table_name}: {sql_e}")
                        deletion_results[table_name] = -1
                else:
                    deletion_results[table_name] = -1

                # Continue with other models rather than failing completely
                continue

        total_deleted = sum(count for count in deletion_results.values() if count > 0)
        logger.info(f"Successfully deleted {total_deleted} records from BIRD data tables")

        return deletion_results

    def _sql_cleanup_table(self, table_name: str) -> int:
        """
        Clean up a specific table using raw SQL with database-specific optimizations.

        This method uses efficient deletion strategies based on the database vendor:
        - SQLite: Uses DELETE with foreign key constraints disabled
        - PostgreSQL: Uses TRUNCATE CASCADE for bulk deletion
        - MSSQL: Uses TRUNCATE TABLE for performance
        - Others: Uses standard DELETE

        Args:
            table_name: Name of the table to clean up

        Returns:
            Number of deleted records

        Raises:
            ValueError: If table_name is not in the allowed whitelist
        """
        from django.db import connection

        # Validate table name against dynamic whitelist to prevent SQL injection
        allowed_tables = self._get_allowed_tables()
        if table_name not in allowed_tables:
            raise ValueError(f"Table '{table_name}' not allowed for deletion")

        with connection.cursor() as cursor:
            # First get the count
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count_before = cursor.fetchone()[0]

            if count_before == 0:
                return 0

            # Database-specific optimized deletion
            if connection.vendor == 'sqlite':
                # SQLite: Disable foreign keys temporarily and use DELETE
                cursor.execute("PRAGMA foreign_keys = 0;")
                cursor.execute(f"DELETE FROM {table_name};")
                cursor.execute("PRAGMA foreign_keys = 1;")
                deleted_count = count_before  # SQLite doesn't reliably return rowcount

            elif connection.vendor == 'postgresql':
                # PostgreSQL: Use TRUNCATE CASCADE for efficient bulk deletion
                cursor.execute(f"TRUNCATE TABLE {table_name} CASCADE;")
                deleted_count = count_before  # TRUNCATE doesn't return rowcount

            elif connection.vendor in ['microsoft', 'mssql']:
                # MSSQL: Use TRUNCATE TABLE for performance
                cursor.execute(f"TRUNCATE TABLE {table_name};")
                deleted_count = count_before  # TRUNCATE doesn't return rowcount

            else:
                # Default: Use standard DELETE for other database vendors
                cursor.execute(f"DELETE FROM {table_name};")
                deleted_count = cursor.rowcount if cursor.rowcount != -1 else count_before

            logger.debug(f"SQL cleanup deleted {deleted_count} records from {table_name} using {connection.vendor}")
            return deleted_count

    def cleanup_specific_tables(self, table_names: List[str]) -> Dict[str, int]:
        """
        Clean up specific BIRD data tables by name.

        Args:
            table_names: List of table names to clean up

        Returns:
            Dictionary mapping table names to number of deleted records

        Raises:
            ValueError: If any table name is not in the allowed whitelist
        """
        logger.info(f"Starting cleanup of specific tables: {table_names}")

        # Validate all table names against dynamic whitelist first
        allowed_tables = self._get_allowed_tables()
        invalid_tables = [name for name in table_names if name not in allowed_tables]
        if invalid_tables:
            raise ValueError(f"Tables not allowed for deletion: {invalid_tables}")

        models = self._get_bird_models()
        model_map = {model._meta.db_table: model for model in models}

        deletion_results = {}

        try:
            with transaction.atomic():
                for table_name in table_names:
                    if table_name in model_map:
                        model = model_map[table_name]
                        try:
                            deleted_count, _ = model.objects.all().delete()
                            deletion_results[table_name] = deleted_count
                            logger.debug(f"Deleted {deleted_count} records from {table_name}")
                        except Exception as e:
                            logger.error(f"Error deleting from {table_name}: {e}")
                            # Try SQL fallback for this specific table
                            try:
                                deleted_count = self._sql_cleanup_table(table_name)
                                deletion_results[table_name] = deleted_count
                                logger.info(f"SQL fallback succeeded for {table_name}: deleted {deleted_count} records")
                            except Exception as sql_e:
                                logger.error(f"SQL fallback also failed for {table_name}: {sql_e}")
                                deletion_results[table_name] = -1
                    else:
                        logger.warning(f"Table {table_name} not found in BIRD models")
                        deletion_results[table_name] = -1

        except Exception as e:
            logger.error(f"Error during specific table cleanup: {e}")
            raise

        return deletion_results

def main():
    """
    Command line interface for the database cleanup service.
    """
    import argparse

    parser = argparse.ArgumentParser(description='Clean up BIRD data tables using Django ORM')
    parser.add_argument('--force', action='store_true',
                       help='Force cleanup even if tables appear empty')
    parser.add_argument('--check-only', action='store_true',
                       help='Only check table status, do not delete')
    parser.add_argument('--tables', nargs='+',
                       help='Specific table names to clean up')

    args = parser.parse_args()

    # Set up logging for command line use
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    service = DatabaseCleanupService()

    if args.check_only:
        table_counts = service.check_table_emptiness()
        print("Table Status:")
        for table_name, count in table_counts.items():
            if count > 0:
                print(f"  {table_name}: {count} records")
        total = sum(count for count in table_counts.values() if count > 0)
        print(f"\nTotal records across all tables: {total}")

    elif args.tables:
        results = service.cleanup_specific_tables(args.tables)
        print("Deletion Results:")
        for table_name, count in results.items():
            print(f"  {table_name}: {count} records deleted")

    else:
        results = service.cleanup_bird_data_tables(force=args.force)
        print("Cleanup Results:")
        for table_name, count in results.items():
            if count > 0:
                print(f"  {table_name}: {count} records deleted")
        total = sum(count for count in results.values() if count > 0)
        print(f"\nTotal records deleted: {total}")

if __name__ == "__main__":
    main()
