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

import logging
import os
import sys
import django
from typing import List, Tuple, Dict, Type
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

    def _get_bird_models(self) -> List[Type[models.Model]]:
        """
        Get all BIRD data model classes.

        Returns:
            List of Django model classes from bird_meta_data_model
        """
        if self._bird_models is None:
            try:
                from pybirdai.models.bird_meta_data_model import (
                    SUBDOMAIN, SUBDOMAIN_ENUMERATION, DOMAIN, FACET_COLLECTION,
                    MAINTENANCE_AGENCY, MEMBER, MEMBER_HIERARCHY, MEMBER_HIERARCHY_NODE,
                    VARIABLE, VARIABLE_SET, VARIABLE_SET_ENUMERATION, FRAMEWORK,
                    MEMBER_MAPPING, MEMBER_MAPPING_ITEM, VARIABLE_MAPPING_ITEM,
                    VARIABLE_MAPPING, MAPPING_TO_CUBE, MAPPING_DEFINITION,
                    AXIS, AXIS_ORDINATE, CELL_POSITION, ORDINATE_ITEM,
                    TABLE, TABLE_CELL, CUBE_STRUCTURE, CUBE_STRUCTURE_ITEM,
                    CUBE, CUBE_LINK, CUBE_STRUCTURE_ITEM_LINK, COMBINATION,
                    COMBINATION_ITEM, CUBE_TO_COMBINATION, MEMBER_LINK
                )

                self._bird_models = [
                    SUBDOMAIN, SUBDOMAIN_ENUMERATION, DOMAIN, FACET_COLLECTION,
                    MAINTENANCE_AGENCY, MEMBER, MEMBER_HIERARCHY, MEMBER_HIERARCHY_NODE,
                    VARIABLE, VARIABLE_SET, VARIABLE_SET_ENUMERATION, FRAMEWORK,
                    MEMBER_MAPPING, MEMBER_MAPPING_ITEM, VARIABLE_MAPPING_ITEM,
                    VARIABLE_MAPPING, MAPPING_TO_CUBE, MAPPING_DEFINITION,
                    AXIS, AXIS_ORDINATE, CELL_POSITION, ORDINATE_ITEM,
                    TABLE, TABLE_CELL, CUBE_STRUCTURE, CUBE_STRUCTURE_ITEM,
                    CUBE, CUBE_LINK, CUBE_STRUCTURE_ITEM_LINK, COMBINATION,
                    COMBINATION_ITEM, CUBE_TO_COMBINATION, MEMBER_LINK
                ]
            except ImportError as e:
                logger.error(f"Failed to import BIRD models: {e}")
                self._bird_models = []

        return self._bird_models

    def _get_deletion_order(self) -> List[Type[models.Model]]:
        """
        Determine the correct order for deleting models based on foreign key dependencies.

        Models with foreign key dependencies should be deleted before the models they reference.

        Returns:
            List of model classes in deletion order
        """
        if self._deletion_order is None:
            models = self._get_bird_models()

            # Manual ordering based on foreign key relationships
            # Delete dependent tables first, then parent tables
            self._deletion_order = [
                # Most dependent models first (have many foreign keys)
                models[28] if len(models) > 28 else None,  # COMBINATION_ITEM
                models[27] if len(models) > 27 else None,  # COMBINATION
                models[29] if len(models) > 29 else None,  # CUBE_TO_COMBINATION
                models[26] if len(models) > 26 else None,  # CUBE_STRUCTURE_ITEM_LINK
                models[25] if len(models) > 25 else None,  # CUBE_LINK
                models[23] if len(models) > 23 else None,  # CUBE_STRUCTURE_ITEM
                models[21] if len(models) > 21 else None,  # TABLE_CELL
                models[19] if len(models) > 19 else None,  # ORDINATE_ITEM
                models[18] if len(models) > 18 else None,  # CELL_POSITION
                models[17] if len(models) > 17 else None,  # AXIS_ORDINATE
                models[13] if len(models) > 13 else None,  # MEMBER_MAPPING_ITEM
                models[14] if len(models) > 14 else None,  # VARIABLE_MAPPING_ITEM
                models[10] if len(models) > 10 else None,  # VARIABLE_SET_ENUMERATION
                models[7] if len(models) > 7 else None,   # MEMBER_HIERARCHY_NODE
                models[1] if len(models) > 1 else None,   # SUBDOMAIN_ENUMERATION
                models[30] if len(models) > 30 else None,  # MEMBER_LINK

                # Mid-level dependency models
                models[24] if len(models) > 24 else None,  # CUBE
                models[22] if len(models) > 22 else None,  # CUBE_STRUCTURE
                models[20] if len(models) > 20 else None,  # TABLE
                models[16] if len(models) > 16 else None,  # AXIS
                models[17] if len(models) > 17 else None,  # MAPPING_DEFINITION
                models[16] if len(models) > 16 else None,  # MAPPING_TO_CUBE
                models[15] if len(models) > 15 else None,  # VARIABLE_MAPPING
                models[12] if len(models) > 12 else None,  # MEMBER_MAPPING
                models[9] if len(models) > 9 else None,   # VARIABLE_SET
                models[6] if len(models) > 6 else None,   # MEMBER_HIERARCHY

                # Lower dependency models
                models[8] if len(models) > 8 else None,   # VARIABLE
                models[5] if len(models) > 5 else None,   # MEMBER
                models[11] if len(models) > 11 else None,  # FRAMEWORK
                models[0] if len(models) > 0 else None,   # SUBDOMAIN
                models[2] if len(models) > 2 else None,   # DOMAIN

                # Base models with no/minimal dependencies
                models[3] if len(models) > 3 else None,   # FACET_COLLECTION
                models[4] if len(models) > 4 else None,   # MAINTENANCE_AGENCY
            ]

            # Filter out None values and duplicates
            self._deletion_order = list(filter(None, self._deletion_order))
            seen = set()
            unique_order = []
            for model in self._deletion_order:
                if model not in seen:
                    unique_order.append(model)
                    seen.add(model)
            self._deletion_order = unique_order

            # Add any missing models at the end
            for model in models:
                if model not in seen:
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

    def cleanup_bird_data_tables(self, force: bool = False, batch_size: int = 1000) -> Dict[str, int]:
        """
        Clean up all BIRD data model tables using Django ORM.

        Args:
            force: If True, delete all records regardless of table state.
                  If False, only delete if tables are not empty.
            batch_size: Number of records to delete in each batch (for large tables)

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
            try:
                table_name = model._meta.db_table
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
                logger.error(f"Error deleting from {model.__name__}: {e}")
                deletion_results[model._meta.db_table] = -1
                # Continue with other models rather than failing completely
                continue

        total_deleted = sum(count for count in deletion_results.values() if count > 0)
        logger.info(f"Successfully deleted {total_deleted} records from BIRD data tables")

        return deletion_results

    def cleanup_specific_tables(self, table_names: List[str]) -> Dict[str, int]:
        """
        Clean up specific BIRD data tables by name.

        Args:
            table_names: List of table names to clean up

        Returns:
            Dictionary mapping table names to number of deleted records
        """
        logger.info(f"Starting cleanup of specific tables: {table_names}")

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