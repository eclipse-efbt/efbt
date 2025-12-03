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
    'CLEANUP_TEMP_INDEXES': os.environ.get('CSV_COPY_CLEANUP_TEMP_INDEXES', 'True').lower() == 'true',
    'ENABLE_DB_SIDE_DEDUP': os.environ.get('CSV_COPY_ENABLE_DB_SIDE_DEDUP', 'True').lower() == 'true',
    'STREAM_CHUNK_SIZE': int(os.environ.get('CSV_COPY_STREAM_CHUNK_SIZE', 5000)),
    'IN_MEMORY_THRESHOLD_MB': int(os.environ.get('CSV_COPY_IN_MEMORY_THRESHOLD_MB', 10)),
    'ENABLE_COMPRESSION': os.environ.get('CSV_COPY_ENABLE_COMPRESSION', 'False').lower() == 'true',
    'RETRY_ATTEMPTS': int(os.environ.get('CSV_COPY_RETRY_ATTEMPTS', 3)),
    'RETRY_BACKOFF_SECONDS': float(os.environ.get('CSV_COPY_RETRY_BACKOFF_SECONDS', 1.0)),
}

# Column name mapping from CSV headers to database columns
# Django ForeignKey fields have _id suffix in the database
# Tables listed here use batch import (executemany) instead of SQLite bulk import
# This is required for tables with auto-increment id columns
CSV_TO_DB_COLUMN_MAPPING = {
    'pybirdai_table_cell': {
        'CELL_ID': 'cell_id',
        'TABLE_ID': 'table_id_id',
        'COMBINATION_ID': 'combination_id',
        'IS_SHADED': 'is_shaded',
    },
    'pybirdai_cell_position': {
        # ID excluded - let SQLite auto-generate (DPM duplication creates duplicate IDs)
        'CELL_ID': 'cell_id_id',
        'AXIS_ORDINATE_ID': 'axis_ordinate_id_id',
    },
    'pybirdai_ordinate_item': {
        'member_hierarchy_valid_from': 'member_hierarchy_valid_from',
        'is_starting_member_included': 'is_starting_member_included',
        'axis_ordinate_id_id': 'axis_ordinate_id_id',
        'variable_id_id': 'variable_id_id',
        'member_id_id': 'member_id_id',
        'member_hierarchy_id_id': 'member_hierarchy_id_id',
        'starting_member_id_id': 'starting_member_id_id',
    },
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


def bulk_import_with_fallback(context, cls, csv_file, delimiter, table_name, config=None):
    """
    Attempt bulk import with automatic fallback to smaller batches on failure.

    Args:
        context: SDDContext containing file paths
        cls: Django model class
        csv_file: Path to CSV file
        delimiter: CSV delimiter
        table_name: Name of the table
        config: DatasetConfig object for configurable import paths

    Returns:
        Result of the import operation

    Raises:
        Exception if all fallback attempts fail
    """
    csv_copy_config = CSV_COPY_CONFIG
    batch_error = None

    # Try fast bulk import first (uses temp table approach for SQLite)
    try:
        return perform_bulk_import(csv_file, delimiter, table_name)
    except Exception as bulk_error:
        print(f"Bulk import failed: {bulk_error}")

    # Fallback 1: Try with smaller batch size using executemany
    print("Attempting fallback with smaller batch size...")
    try:
        return perform_batch_import(csv_file, delimiter, table_name, batch_size=csv_copy_config.get('BATCH_SIZE', 10000))
    except Exception as e:
        batch_error = e
        print(f"Batch import failed: {batch_error}")

    # Fallback 2: Use original row-by-row import (for both paths)
    print("Falling back to row-by-row import...")
    from pybirdai.process_steps.website_to_sddmodel.import_func.import_table_cells import import_table_cells
    from pybirdai.process_steps.website_to_sddmodel.import_func.import_ordinate_items import import_ordinate_items
    from pybirdai.process_steps.website_to_sddmodel.import_func.import_cell_positions import import_cell_positions
    from pybirdai.models.bird_meta_data_model import TABLE_CELL, ORDINATE_ITEM, CELL_POSITION

    fallback_func = {
        TABLE_CELL: lambda ctx: import_table_cells(ctx, config=config),
        ORDINATE_ITEM: lambda ctx: import_ordinate_items(ctx, config),
        CELL_POSITION: lambda ctx: import_cell_positions(ctx, config=config)
    }.get(cls)

    if fallback_func:
        return fallback_func(context)
    else:
        raise Exception(f"No fallback function available for {cls.__name__}. Last error: {batch_error}")


def perform_bulk_import(csv_file, delimiter, table_name):
    """
    Perform database-native bulk import using temp table approach.

    For SQLite, uses temp table to handle auto-increment id:
    1. Read CSV header to get column names
    2. Create temp table with those columns (no id)
    3. Fast .import into temp table
    4. INSERT SELECT into real table (id auto-generates)
    5. Drop temp table

    Args:
        csv_file: Path to CSV file
        delimiter: CSV delimiter
        table_name: Name of the table

    Returns:
        Result of the import operation
    """
    if connection.vendor == 'sqlite':
        import csv as csv_module

        # Read CSV header to get column names
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv_module.reader(f, delimiter=delimiter)
            csv_headers = next(reader)

        # Map CSV headers to database column names
        column_mapping = CSV_TO_DB_COLUMN_MAPPING.get(table_name, {})
        db_columns = []
        for header in csv_headers:
            if header in column_mapping:
                db_columns.append(column_mapping[header])
            else:
                db_columns.append(header.lower())

        db_file = Path(connection.settings_dict['NAME']).absolute()
        temp_table = f"temp_{table_name.replace('pybirdai_', '')}_import"

        # Build SQLite script with temp table approach
        commands = [
            ".mode csv",
            f".separator '{delimiter}'",
            "PRAGMA foreign_keys = 0;",
            f"DROP TABLE IF EXISTS {temp_table};",
            f"CREATE TEMP TABLE {temp_table} ({', '.join(db_columns)});",
            f".import --skip 1 '{csv_file}' {temp_table}",
            f"INSERT INTO {table_name} ({', '.join(db_columns)}) SELECT * FROM {temp_table};",
            f"DROP TABLE {temp_table};",
            "PRAGMA foreign_keys = 1;",
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
            check=False  # Don't raise, check manually for better error message
        )
        if result.returncode != 0:
            error_msg = result.stderr or result.stdout or "Unknown error"
            raise Exception(f"SQLite import error (exit {result.returncode}): {error_msg}")
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
        csv_headers = next(reader)  # Get CSV headers

        # Map CSV headers to database column names
        column_mapping = CSV_TO_DB_COLUMN_MAPPING.get(table_name, {})
        db_columns = []
        for header in csv_headers:
            if header in column_mapping:
                db_columns.append(column_mapping[header])
            else:
                # Fallback: convert to lowercase
                db_columns.append(header.lower())

        batch = []
        total_rows = 0

        with connection.cursor() as cursor:
            # Disable foreign key checks for SQLite during batch import
            # This handles cases where referenced records may not exist yet
            if connection.vendor == 'sqlite':
                cursor.execute("PRAGMA foreign_keys = 0;")

            try:
                placeholders = ', '.join(['%s'] * len(db_columns))
                insert_sql = f"INSERT INTO {table_name} ({', '.join(db_columns)}) VALUES ({placeholders})"

                for row in reader:
                    # Convert empty strings to None for nullable fields
                    cleaned_row = [None if val == '' else val for val in row]
                    batch.append(cleaned_row)
                    if len(batch) >= batch_size:
                        cursor.executemany(insert_sql, batch)
                        total_rows += len(batch)
                        print(f"Imported {total_rows} rows...")
                        batch = []

                # Insert remaining rows
                if batch:
                    cursor.executemany(insert_sql, batch)
                    total_rows += len(batch)
            finally:
                # Re-enable foreign key checks
                if connection.vendor == 'sqlite':
                    cursor.execute("PRAGMA foreign_keys = 1;")

        print(f"Batch import complete: {total_rows} rows imported")
        return total_rows


def create_instances_from_csv_copy(context, cls, config=None):
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
        config: DatasetConfig object specifying file_directory subdirectory (optional, defaults to "technical_export")
    """
    sdd_table_name = cls.__name__.lower()
    table_name = f"pybirdai_{sdd_table_name}"

    subdir = config.file_directory if config else "technical_export"
    csv_file = context.file_directory + os.sep + subdir + os.sep + f"{sdd_table_name}.csv"
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
            return bulk_import_with_fallback(context, cls, str(csv_file), delimiter, table_name, config)

        result = retry_with_backoff(import_with_retry)

        elapsed = time.time() - start_time
        print(f"Import completed in {elapsed:.2f} seconds")

        # PHASE 4: Restore backed-up data with key regeneration
        print(f"Restoring backed-up data to {table_name} with key regeneration...")
        restore_backed_up_data_bulk(table_name, backup_table_name, str(csv_file))

        # PHASE 4.5: Clean up invalid FK strings for ORDINATE_ITEM table
        if table_name == 'pybirdai_ordinate_item':
            print(f"Cleaning up invalid FK strings in {table_name}...")
            with connection.cursor() as cursor:
                # Fix empty strings, whitespace, and 'None' string to NULL
                cursor.execute("UPDATE pybirdai_ordinate_item SET member_id_id = NULL WHERE member_id_id = '' OR TRIM(member_id_id) = '' OR member_id_id = 'None'")
                member_fixes = cursor.rowcount
                cursor.execute("UPDATE pybirdai_ordinate_item SET member_hierarchy_id_id = NULL WHERE member_hierarchy_id_id = '' OR TRIM(member_hierarchy_id_id) = '' OR member_hierarchy_id_id = 'None'")
                hierarchy_fixes = cursor.rowcount
                cursor.execute("UPDATE pybirdai_ordinate_item SET starting_member_id_id = NULL WHERE starting_member_id_id = '' OR TRIM(starting_member_id_id) = '' OR starting_member_id_id = 'None'")
                starting_fixes = cursor.rowcount
                total_fixes = member_fixes + hierarchy_fixes + starting_fixes
                if total_fixes > 0:
                    print(f"Fixed {total_fixes} invalid FK strings (member={member_fixes}, hierarchy={hierarchy_fixes}, starting={starting_fixes})")
                else:
                    print(f"FK cleanup completed - no invalid strings found")

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
