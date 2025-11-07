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
#

"""CSV copy import functionality with backup/restore capabilities."""

import os
import subprocess
import platform
import time
from pathlib import Path
from django.db import connection
from pybirdai.process_steps.website_to_sddmodel.import_func.database_helpers import (
    backup_table_data,
    restore_backed_up_data_bulk,
    cleanup_backup_table
)

# Configuration parameters for CSV copy import optimization
CSV_COPY_CONFIG = {
    'BATCH_SIZE': int(os.environ.get('CSV_COPY_BATCH_SIZE', 10000)),
    'PARALLEL_WORKERS': int(os.environ.get('CSV_COPY_PARALLEL_WORKERS', max(1, os.cpu_count() - 1))),
    'ENABLE_PARALLEL': os.environ.get('CSV_COPY_ENABLE_PARALLEL', 'True').lower() == 'true',
    'ENABLE_INDEXING': os.environ.get('CSV_COPY_ENABLE_INDEXING', 'True').lower() == 'true',
    'ENABLE_DB_SIDE_DEDUP': os.environ.get('CSV_COPY_ENABLE_DB_SIDE_DEDUP', 'True').lower() == 'true',
    'STREAM_CHUNK_SIZE': int(os.environ.get('CSV_COPY_STREAM_CHUNK_SIZE', 5000)),
    'IN_MEMORY_THRESHOLD_MB': int(os.environ.get('CSV_COPY_IN_MEMORY_THRESHOLD_MB', 10)),
    'ENABLE_COMPRESSION': os.environ.get('CSV_COPY_ENABLE_COMPRESSION', 'False').lower() == 'true',
    'RETRY_ATTEMPTS': int(os.environ.get('CSV_COPY_RETRY_ATTEMPTS', 3)),
    'RETRY_BACKOFF_SECONDS': float(os.environ.get('CSV_COPY_RETRY_BACKOFF_SECONDS', 1.0)),
}


def retry_with_backoff(func, max_attempts=None, backoff_seconds=None):
    """
    Retry a function with exponential backoff on failure.

    Args:
        func: Function to retry (should be a callable with no arguments)
        max_attempts: Maximum number of retry attempts (default from config)
        backoff_seconds: Initial backoff time in seconds (default from config)

    Returns:
        Result of the function call

    Raises:
        Last exception if all retries fail
    """
    config = CSV_COPY_CONFIG
    max_attempts = max_attempts or config.get('RETRY_ATTEMPTS', 3)
    backoff_seconds = backoff_seconds or config.get('RETRY_BACKOFF_SECONDS', 1.0)

    last_exception = None
    for attempt in range(max_attempts):
        try:
            return func()
        except Exception as e:
            last_exception = e
            if attempt < max_attempts - 1:
                wait_time = backoff_seconds * (2 ** attempt)
                print(f"Attempt {attempt + 1}/{max_attempts} failed: {str(e)}")
                print(f"Retrying in {wait_time:.1f} seconds...")
                time.sleep(wait_time)
            else:
                print(f"All {max_attempts} attempts failed")
                raise last_exception


def bulk_import_with_fallback(context, cls, csv_file, delimiter, table_name):
    """
    Attempt bulk import with automatic fallback to smaller batches on failure.

    Args:
        context: SDDContext containing file paths
        cls: Django model class
        csv_file: Path to CSV file
        delimiter: CSV delimiter
        table_name: Name of the table

    Returns:
        Result of the import operation

    Raises:
        Exception if all fallback attempts fail
    """
    config = CSV_COPY_CONFIG

    # Try full bulk import first
    try:
        return perform_bulk_import(csv_file, delimiter, table_name)
    except Exception as bulk_error:
        print(f"Bulk import failed: {bulk_error}")

        # Fallback 1: Try with smaller batch size using executemany
        print("Attempting fallback with smaller batch size...")
        try:
            return perform_batch_import(csv_file, delimiter, table_name, batch_size=config.get('BATCH_SIZE', 10000))
        except Exception as batch_error:
            print(f"Batch import failed: {batch_error}")

            # Fallback 2: Use original row-by-row import
            print("Falling back to row-by-row import...")
            from pybirdai.process_steps.website_to_sddmodel.import_func.import_table_cells import import_table_cells
            from pybirdai.process_steps.website_to_sddmodel.import_func.import_ordinate_items import import_ordinate_items
            from pybirdai.process_steps.website_to_sddmodel.import_func.import_cell_positions import import_cell_positions
            from pybirdai.models.bird_meta_data_model import TABLE_CELL, ORDINATE_ITEM, CELL_POSITION

            fallback_func = {
                TABLE_CELL: import_table_cells,
                ORDINATE_ITEM: import_ordinate_items,
                CELL_POSITION: import_cell_positions
            }.get(cls)

            if fallback_func:
                return fallback_func(context)
            else:
                raise Exception(f"No fallback function available for {cls.__name__}")


def perform_bulk_import(csv_file, delimiter, table_name):
    """
    Perform database-native bulk import.

    Args:
        csv_file: Path to CSV file
        delimiter: CSV delimiter
        table_name: Name of the table

    Returns:
        Result of the import operation
    """
    if connection.vendor == 'sqlite':
        db_file = Path(connection.settings_dict['NAME']).absolute()
        commands = [
            ".mode csv",
            f".separator '{delimiter}'",
            f".import --skip 1 '{csv_file}' {table_name}"
        ]
        sqlite_script = '\n'.join(commands)
        sqlite_program = "sqlite3"
        if platform.system() == 'Windows':
            sqlite_program += ".exe"

        result = subprocess.run(
            [sqlite_program, str(db_file)],
            input=sqlite_script,
            text=True,
            capture_output=True,
            check=True
        )
        if result.stderr:
            raise Exception(f"SQLite import error: {result.stderr}")
        return result

    elif connection.vendor == 'postgresql':
        with connection.cursor() as cursor:
            with open(csv_file, 'r', encoding='utf-8') as f:
                next(f)  # Skip header
                cursor.copy_expert(
                    f"COPY {table_name} FROM STDIN WITH (FORMAT CSV, DELIMITER '{delimiter}')",
                    f
                )
        return None

    elif connection.vendor in ['microsoft', 'mssql']:
        with connection.cursor() as cursor:
            cursor.execute(f"""
                BULK INSERT {table_name}
                FROM '{csv_file}'
                WITH (
                    FORMAT = 'CSV',
                    FIRSTROW = 2,
                    FIELDTERMINATOR = '{delimiter}',
                    ROWTERMINATOR = '\\n'
                )
            """)
        return None
    else:
        raise Exception(f"Unsupported database vendor: {connection.vendor}")


def perform_batch_import(csv_file, delimiter, table_name, batch_size=10000):
    """
    Import CSV data using executemany with configurable batch size.

    Args:
        csv_file: Path to CSV file
        delimiter: CSV delimiter
        table_name: Name of the table
        batch_size: Number of rows to insert per batch

    Returns:
        Number of rows imported
    """
    import csv as csv_module

    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv_module.reader(f, delimiter=delimiter)
        headers = next(reader)  # Skip header

        batch = []
        total_rows = 0

        with connection.cursor() as cursor:
            placeholders = ', '.join(['%s'] * len(headers))
            insert_sql = f"INSERT INTO {table_name} ({', '.join(headers)}) VALUES ({placeholders})"

            for row in reader:
                batch.append(row)
                if len(batch) >= batch_size:
                    cursor.executemany(insert_sql, batch)
                    total_rows += len(batch)
                    print(f"Imported {total_rows} rows...")
                    batch = []

            # Insert remaining rows
            if batch:
                cursor.executemany(insert_sql, batch)
                total_rows += len(batch)

        print(f"Batch import complete: {total_rows} rows imported")
        return total_rows


def create_instances_from_csv_copy(context, cls):
    """
    Import CSV data using database-native bulk import with backup/restore.

    This function:
    1. Backs up existing table data
    2. Truncates the table
    3. Imports new CSV data using database-native commands
    4. Restores backed-up data (skipping duplicates)
    5. Cleans up temporary backup

    Args:
        context: SDDContext containing file paths
        cls: Django model class (TABLE_CELL, ORDINATE_ITEM, or CELL_POSITION)
    """
    sdd_table_name = cls.__name__.lower()
    table_name = f"pybirdai_{sdd_table_name}"

    csv_file = context.file_directory + os.sep + "technical_export" + os.sep + f"{sdd_table_name}.csv"
    csv_file = Path(csv_file).absolute()
    delimiter = ","

    # Check if CSV file exists
    if not csv_file.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_file}")

    try:
        # Define allowed table names to prevent SQL injection
        ALLOWED_TABLES = {
            'pybirdai_table_cell',
            'pybirdai_ordinate_item',
            'pybirdai_cell_position'
        }

        if table_name not in ALLOWED_TABLES:
            raise ValueError(f"Table '{table_name}' not allowed for deletion")

        # PHASE 1: Backup existing data before truncation
        backup_table_name = f"{table_name}_backup_temp"
        print(f"Backing up existing data from {table_name}...")
        backup_table_data(table_name, backup_table_name)

        # PHASE 2: Truncate table
        with connection.cursor() as cursor:
            if connection.vendor == 'sqlite':
                cursor.execute("PRAGMA foreign_keys = 0;")
                cursor.execute(f"DELETE FROM {table_name};")
                cursor.execute("PRAGMA foreign_keys = 1;")
            elif connection.vendor == 'postgresql':
                cursor.execute(f"TRUNCATE TABLE {table_name} CASCADE;")
            elif connection.vendor in ['microsoft', 'mssql']:
                cursor.execute(f"TRUNCATE TABLE {table_name};")
            else:
                cursor.execute(f"DELETE FROM {table_name};")

        # PHASE 3: Import CSV data with retry logic and automatic fallback
        print(f"Importing CSV data into {table_name}...")
        start_time = time.time()

        def import_with_retry():
            return bulk_import_with_fallback(context, cls, str(csv_file), delimiter, table_name)

        result = retry_with_backoff(import_with_retry)

        elapsed = time.time() - start_time
        print(f"Import completed in {elapsed:.2f} seconds")

        # PHASE 4: Restore backed-up data with key regeneration
        print(f"Restoring backed-up data to {table_name} with key regeneration...")
        restore_backed_up_data_bulk(table_name, backup_table_name, str(csv_file))

        # PHASE 5: Cleanup backup table
        print(f"Cleaning up backup table {backup_table_name}...")
        cleanup_backup_table(backup_table_name)

        return result

    except Exception as e:
        print(f"Error importing CSV for {table_name}: {str(e)}")
        # Try to cleanup backup table even on error
        try:
            backup_table_name = f"{table_name}_backup_temp"
            cleanup_backup_table(backup_table_name)
        except:
            pass
        raise
