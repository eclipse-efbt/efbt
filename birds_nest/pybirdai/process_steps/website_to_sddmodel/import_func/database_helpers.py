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

"""Database helper functions for CSV import operations with backup/restore capabilities."""

import os
import subprocess
import platform
import time
import tempfile
import csv as csv_module
import io
import multiprocessing
from pathlib import Path
from django.db import connection

# Import configuration from csv_copy_importer
# Note: We'll access this via the global config when needed
def get_config():
    """Get the CSV copy configuration from csv_copy_importer."""
    from pybirdai.process_steps.website_to_sddmodel.import_func.csv_copy_importer import CSV_COPY_CONFIG
    return CSV_COPY_CONFIG


def get_primary_key_column(table_name):
    """
    Get the primary key column name for a given table.

    Args:
        table_name: Name of the database table

    Returns:
        Primary key column name
    """
    # Map table names to their primary key columns
    pk_mapping = {
        'pybirdai_table_cell': 'cell_id',
        'pybirdai_ordinate_item': 'id',  # Auto-increment ID
        'pybirdai_cell_position': 'id'   # Auto-increment ID
    }
    return pk_mapping.get(table_name, 'id')


def backup_table_data(table_name, backup_table_name):
    """
    Create a temporary backup table and copy all existing data.
    Phase 1 of the backup-restore approach.

    Args:
        table_name: Name of the table to backup
        backup_table_name: Name for the temporary backup table
    """
    with connection.cursor() as cursor:
        if connection.vendor == 'sqlite':
            cursor.execute(f"CREATE TEMP TABLE {backup_table_name} AS SELECT * FROM {table_name}")
        elif connection.vendor == 'postgresql':
            cursor.execute(f"CREATE TEMP TABLE {backup_table_name} AS SELECT * FROM {table_name}")
        else:
            # For other databases, use similar syntax
            cursor.execute(f"CREATE TABLE {backup_table_name} AS SELECT * FROM {table_name}")


def is_duplicate_content(table_name, backed_up_row, columns, existing_rows_cache):
    """
    Check if backed-up row content is identical to any existing row.

    For tables with string PKs (TABLE_CELL):
        - Compare by PK first, then all content fields

    For tables with auto-increment PKs (ORDINATE_ITEM, CELL_POSITION):
        - Ignore PK, compare all content fields against all existing rows

    Args:
        table_name: Name of the table being restored
        backed_up_row: Row from backup (list/tuple)
        columns: Column names in order
        existing_rows_cache: Dict (for TABLE_CELL) or set (for others) of existing rows

    Returns:
        True if duplicate found, False otherwise
    """
    pk_column = get_primary_key_column(table_name)

    if table_name == 'pybirdai_table_cell':
        # String PK - compare by key first
        pk_index = columns.index('cell_id')
        pk_value = str(backed_up_row[pk_index]) if backed_up_row[pk_index] else None

        if not pk_value:
            return False  # NULL PK, can't be duplicate

        # Check if this PK exists in newly imported data
        if pk_value not in existing_rows_cache:
            return False  # Not a duplicate, new record

        existing_row = existing_rows_cache[pk_value]

        # Compare content fields (exclude PK and 'name' if exists)
        content_fields = ['table_cell_combination_id', 'table_id_id', 'is_shaded', 'system_data_code']

        for field in content_fields:
            if field not in columns:
                continue  # Skip if field doesn't exist

            backed_up_value = backed_up_row[columns.index(field)]
            existing_value = existing_row[columns.index(field)]

            # Normalize None and empty string for comparison
            backed_up_value = None if backed_up_value in (None, '', 'None') else backed_up_value
            existing_value = None if existing_value in (None, '', 'None') else existing_value

            if backed_up_value != existing_value:
                return False  # Different content

        return True  # Duplicate - same key, same content

    elif table_name == 'pybirdai_ordinate_item':
        # Auto-increment PK - compare all content fields using hash lookup (O(1))
        content_fields = ['axis_ordinate_id_id', 'variable_id_id', 'member_id_id',
                        'member_hierarchy_id_id', 'starting_member_id_id', 'is_starting_member_included']

        # Build tuple of backed-up row's content field values (normalized)
        backed_up_values = []
        for field in content_fields:
            if field not in columns:
                continue
            value = backed_up_row[columns.index(field)]
            # Normalize None values
            value = None if value in (None, '', 'None') else value
            backed_up_values.append(value)

        backed_up_hash = tuple(backed_up_values)

        # Check if this content tuple exists in the hash set (O(1) lookup)
        return backed_up_hash in existing_rows_cache

    elif table_name == 'pybirdai_cell_position':
        # Auto-increment PK - compare all content fields using hash lookup (O(1))
        content_fields = ['cell_id_id', 'axis_ordinate_id_id']

        # Build tuple of backed-up row's content field values (normalized)
        backed_up_values = []
        for field in content_fields:
            if field not in columns:
                continue
            value = backed_up_row[columns.index(field)]
            # Normalize None values
            value = None if value in (None, '', 'None') else value
            backed_up_values.append(value)

        backed_up_hash = tuple(backed_up_values)

        # Check if this content tuple exists in the hash set (O(1) lookup)
        return backed_up_hash in existing_rows_cache

    # Unknown table - don't skip
    return False


def process_chunk_for_duplicates(args):
    """
    Process a chunk of backed-up rows to check for duplicates.
    Used for parallel processing.

    Args:
        args: Tuple of (table_name, chunk, columns, existing_rows_cache, pk_column, pk_index)

    Returns:
        Tuple of (modified_rows, skipped_count)
    """
    table_name, chunk, columns, existing_rows_cache, pk_column, pk_index = args
    modified_rows = []
    skipped_duplicates = 0

    for row in chunk:
        row_list = list(row)

        # Check for duplicate content
        if is_duplicate_content(table_name, row_list, columns, existing_rows_cache):
            skipped_duplicates += 1
            continue  # Skip restoration of this duplicate row

        # NOT a duplicate → restore with NEW auto-generated ID
        # For auto-increment tables, set PK to NULL so SQLite generates new sequential IDs
        if table_name in ['pybirdai_ordinate_item', 'pybirdai_cell_position']:
            row_list[pk_index] = None
        modified_rows.append(row_list)

    return modified_rows, skipped_duplicates


def process_backed_up_rows_parallel(table_name, backed_up_rows, columns, existing_rows_cache, pk_column, pk_index):
    """
    Process backed-up rows in parallel to check for duplicates.

    Args:
        table_name: Name of the table
        backed_up_rows: List of rows from backup table
        columns: Column names
        existing_rows_cache: Cache of existing rows (dict or set)
        pk_column: Primary key column name
        pk_index: Primary key column index

    Returns:
        Tuple of (modified_rows, total_skipped_duplicates)
    """
    config = get_config()
    num_workers = config.get('PARALLEL_WORKERS', max(1, multiprocessing.cpu_count() - 1))

    # Split backed_up_rows into chunks
    total_rows = len(backed_up_rows)
    chunk_size = max(1, total_rows // num_workers)
    chunks = [backed_up_rows[i:i + chunk_size] for i in range(0, total_rows, chunk_size)]

    print(f"Processing {total_rows} rows using {num_workers} parallel workers ({len(chunks)} chunks)...")

    # Prepare arguments for each chunk
    chunk_args = [
        (table_name, chunk, columns, existing_rows_cache, pk_column, pk_index)
        for chunk in chunks
    ]

    # Process chunks in parallel
    with multiprocessing.Pool(processes=num_workers) as pool:
        results = pool.map(process_chunk_for_duplicates, chunk_args)

    # Combine results
    all_modified_rows = []
    total_skipped = 0
    for modified_rows, skipped_count in results:
        all_modified_rows.extend(modified_rows)
        total_skipped += skipped_count

    print(f"Parallel processing complete: {len(all_modified_rows)} rows to restore, {total_skipped} duplicates skipped")

    return all_modified_rows, total_skipped


def restore_backed_up_data_bulk(table_name, backup_table_name, csv_file_path):
    """
    Restore backed-up data using bulk insert with key regeneration.
    Phases 3-4 of the backup-restore approach:
    - Use database-side deduplication with SQL queries
    - Bulk insert restored data that doesn't duplicate existing content

    Args:
        table_name: Name of the table to restore to
        backup_table_name: Name of the backup table
        csv_file_path: Path to original CSV file (for reference)
    """
    config = get_config()

    # Use database-side deduplication if enabled, otherwise fall back to in-memory
    if config.get('ENABLE_DB_SIDE_DEDUP', True):
        return restore_backed_up_data_db_side(table_name, backup_table_name, csv_file_path)
    else:
        return restore_backed_up_data_in_memory(table_name, backup_table_name, csv_file_path)


def restore_backed_up_data_db_side(table_name, backup_table_name, csv_file_path):
    """
    Restore backed-up data using database-side deduplication.
    Uses SQL NOT EXISTS queries to avoid loading data into Python memory.

    Args:
        table_name: Name of the table to restore to
        backup_table_name: Name of the backup table
        csv_file_path: Path to original CSV file (for reference)
    """
    pk_column = get_primary_key_column(table_name)
    config = get_config()

    with connection.cursor() as cursor:
        # Get column information
        cursor.execute(f"SELECT * FROM {backup_table_name} LIMIT 0")
        columns = [desc[0] for desc in cursor.description]

        # Count backed-up rows
        cursor.execute(f"SELECT COUNT(*) FROM {backup_table_name}")
        total_backed_up = cursor.fetchone()[0]

        if total_backed_up == 0:
            print(f"No data to restore for {table_name}")
            return

        print(f"Restoring {total_backed_up} backed-up rows for {table_name} using database-side deduplication...")

        # Build WHERE clause for duplicate detection based on content fields
        content_fields_map = {
            'pybirdai_table_cell': ['table_cell_combination_id', 'table_id_id', 'is_shaded', 'system_data_code'],
            'pybirdai_ordinate_item': ['axis_ordinate_id_id', 'variable_id_id', 'member_id_id',
                                        'member_hierarchy_id_id', 'starting_member_id_id', 'is_starting_member_included'],
            'pybirdai_cell_position': ['cell_id_id', 'axis_ordinate_id_id']
        }

        content_fields = content_fields_map.get(table_name, [])

        if not content_fields:
            print(f"Warning: No content fields defined for {table_name}, restoring all backed-up rows")
            # Simple restore without deduplication
            if table_name in ['pybirdai_ordinate_item', 'pybirdai_cell_position']:
                # For auto-increment tables, exclude PK column
                non_pk_columns = [col for col in columns if col != pk_column]
                columns_str = ', '.join(non_pk_columns)
                cursor.execute(f"""
                    INSERT INTO {table_name} ({columns_str})
                    SELECT {columns_str} FROM {backup_table_name}
                """)
            else:
                columns_str = ', '.join(columns)
                cursor.execute(f"""
                    INSERT INTO {table_name} ({columns_str})
                    SELECT {columns_str} FROM {backup_table_name}
                """)
            rows_restored = cursor.rowcount
            print(f"Restored {rows_restored} rows to {table_name}")
            return

        # Build NOT EXISTS clause for deduplication
        # For each content field, we need: (backup.field = main.field OR (backup.field IS NULL AND main.field IS NULL))
        dedup_conditions = []
        for field in content_fields:
            if field in columns:
                dedup_conditions.append(
                    f"(backup.{field} = main.{field} OR (backup.{field} IS NULL AND main.{field} IS NULL))"
                )

        dedup_where = " AND ".join(dedup_conditions)

        # Create temporary indexes on content fields for faster lookups (if enabled)
        if config.get('ENABLE_INDEXING', True):
            print(f"Creating temporary indexes on {table_name} for optimization...")
            for i, field in enumerate(content_fields):
                if field in columns:
                    try:
                        idx_name = f"tmp_idx_{table_name}_{field}_{int(time.time())}"
                        cursor.execute(f"CREATE INDEX {idx_name} ON {table_name}({field})")
                    except Exception as e:
                        print(f"Warning: Could not create index on {field}: {e}")

        # Insert non-duplicate rows from backup
        # For auto-increment tables, exclude the PK column so DB can generate new IDs
        if table_name in ['pybirdai_ordinate_item', 'pybirdai_cell_position']:
            non_pk_columns = [col for col in columns if col != pk_column]
            columns_str = ', '.join(non_pk_columns)
            select_columns = ', '.join([f"backup.{col}" for col in non_pk_columns])

            insert_sql = f"""
                INSERT INTO {table_name} ({columns_str})
                SELECT {select_columns}
                FROM {backup_table_name} AS backup
                WHERE NOT EXISTS (
                    SELECT 1 FROM {table_name} AS main
                    WHERE {dedup_where}
                )
            """
        else:
            # For string PK tables, include all columns
            columns_str = ', '.join(columns)
            select_columns = ', '.join([f"backup.{col}" for col in columns])

            insert_sql = f"""
                INSERT INTO {table_name} ({columns_str})
                SELECT {select_columns}
                FROM {backup_table_name} AS backup
                WHERE NOT EXISTS (
                    SELECT 1 FROM {table_name} AS main
                    WHERE {dedup_where}
                )
            """

        try:
            cursor.execute(insert_sql)
            rows_restored = cursor.rowcount

            # Count duplicates skipped
            skipped_duplicates = total_backed_up - rows_restored

            print(f"\nRestoration Statistics for {table_name}:")
            print(f"  Total backed-up rows: {total_backed_up}")
            print(f"  Skipped duplicates: {skipped_duplicates}")
            print(f"  Rows restored: {rows_restored}")
            if skipped_duplicates > 0:
                print(f"  → {skipped_duplicates}/{total_backed_up} ({skipped_duplicates*100/total_backed_up:.1f}%) were duplicates and not restored")

            print(f"Successfully restored {rows_restored} rows to {table_name}")
        except Exception as e:
            print(f"ERROR: Failed to restore backed-up data to {table_name}")
            print(f"  SQL: {insert_sql}")
            print(f"  Error: {str(e)}")
            raise
        finally:
            # Clean up temporary indexes created for this operation
            if config.get('ENABLE_INDEXING', True) and config.get('CLEANUP_TEMP_INDEXES', True):
                print(f"Cleaning up temporary indexes on {table_name}...")
                for field in content_fields:
                    if field in columns:
                        try:
                            # Find and drop all tmp_idx indexes for this table and field
                            cursor.execute(f"""
                                SELECT name FROM sqlite_master
                                WHERE type='index'
                                AND name LIKE 'tmp_idx_{table_name}_{field}%'
                            """)
                            temp_indexes = cursor.fetchall()
                            for (idx_name,) in temp_indexes:
                                cursor.execute(f"DROP INDEX IF EXISTS {idx_name}")
                                print(f"  Dropped temporary index: {idx_name}")
                        except Exception as e:
                            print(f"Warning: Could not drop temporary indexes for {field}: {e}")


def restore_backed_up_data_in_memory(table_name, backup_table_name, csv_file_path):
    """
    Restore backed-up data using in-memory duplicate detection (legacy approach).
    Loads all data into Python memory for duplicate checking.

    Args:
        table_name: Name of the table to restore to
        backup_table_name: Name of the backup table
        csv_file_path: Path to original CSV file (for reference)
    """
    pk_column = get_primary_key_column(table_name)
    timestamp = int(time.time())

    with connection.cursor() as cursor:
        # Get all existing rows (not just keys) for duplicate detection
        config = get_config()
        stream_chunk_size = config.get('STREAM_CHUNK_SIZE', 5000)

        # Use streaming approach to build cache in chunks
        cursor.execute(f"SELECT * FROM {table_name}")
        existing_columns = [desc[0] for desc in cursor.description]

        # Build cache incrementally instead of fetchall()
        existing_rows = []
        chunk_count = 0
        while True:
            chunk = cursor.fetchmany(stream_chunk_size)
            if not chunk:
                break
            existing_rows.extend(chunk)
            chunk_count += 1
            if chunk_count % 10 == 0:
                print(f"  Loaded {len(existing_rows)} existing rows so far...")

        print(f"Loaded {len(existing_rows)} existing rows in {chunk_count} chunks")

        # Build cache structure for duplicate detection
        # For TABLE_CELL (string PK): dict {cell_id: row}
        # For others (auto-increment PK): set of content field tuples for O(1) lookup
        if table_name == 'pybirdai_table_cell':
            pk_idx = existing_columns.index(pk_column)
            existing_rows_cache = {str(row[pk_idx]): row for row in existing_rows}
            existing_keys = set(existing_rows_cache.keys())
        else:
            # Build hash set of content tuples for fast duplicate detection
            # Define content fields for each table (exclude auto-increment PK)
            content_fields_map = {
                'pybirdai_ordinate_item': ['axis_ordinate_id_id', 'variable_id_id', 'member_id_id',
                                           'member_hierarchy_id_id', 'starting_member_id_id', 'is_starting_member_included'],
                'pybirdai_cell_position': ['cell_id_id', 'axis_ordinate_id_id']
            }

            content_fields = content_fields_map.get(table_name, [])
            existing_rows_cache = set()

            for row in existing_rows:
                # Build tuple of content field values (normalized)
                values = []
                for field in content_fields:
                    if field in existing_columns:
                        value = row[existing_columns.index(field)]
                        # Normalize None values
                        value = None if value in (None, '', 'None') else value
                        values.append(value)
                existing_rows_cache.add(tuple(values))

            existing_keys = {str(row[existing_columns.index(pk_column)]) for row in existing_rows}

        # Get all backed-up data
        cursor.execute(f"SELECT * FROM {backup_table_name}")
        columns = [desc[0] for desc in cursor.description]
        backed_up_rows = cursor.fetchall()

        if not backed_up_rows:
            return  # No data to restore

        print(f"Checking {len(backed_up_rows)} backed-up rows for duplicates...")

        # Find primary key column index
        pk_index = columns.index(pk_column)

        # Use parallel processing if enabled and dataset is large enough
        if config.get('ENABLE_PARALLEL', True) and len(backed_up_rows) > 10000:
            modified_rows, skipped_duplicates = process_backed_up_rows_parallel(
                table_name, backed_up_rows, columns, existing_rows_cache, pk_column, pk_index
            )
        else:
            # Sequential processing (original approach)
            modified_rows = []
            skipped_duplicates = 0

            for row in backed_up_rows:
                row_list = list(row)

                # Check for duplicate content
                if is_duplicate_content(table_name, row_list, columns, existing_rows_cache):
                    skipped_duplicates += 1
                    continue  # Skip restoration of this duplicate row

                # NOT a duplicate → restore with NEW auto-generated ID
                # For auto-increment tables, set PK to NULL so SQLite generates new sequential IDs
                # This prevents UNIQUE constraint violations with newly imported data
                if table_name in ['pybirdai_ordinate_item', 'pybirdai_cell_position']:
                    row_list[pk_index] = None
                modified_rows.append(row_list)

        # Print statistics before bulk insert
        total_backed_up = len(backed_up_rows)
        total_restored = len(modified_rows)
        print(f"\nRestoration Statistics for {table_name}:")
        print(f"  Total backed-up rows: {total_backed_up}")
        print(f"  Skipped duplicates: {skipped_duplicates}")
        print(f"  Rows to restore: {total_restored}")
        if skipped_duplicates > 0:
            print(f"  → {skipped_duplicates}/{total_backed_up} ({skipped_duplicates*100/total_backed_up:.1f}%) were duplicates and not restored")

        # Bulk insert restored data
        if total_restored > 0:
            print(f"\nBulk inserting {len(modified_rows)} rows into {table_name}...")
            print(f"Columns: {columns}")
            print(f"Database vendor: {connection.vendor}")
        else:
            print(f"No rows to restore for {table_name} - all backed-up data was duplicates")
            return

        try:
            if connection.vendor == 'sqlite':
                bulk_insert_sqlite(table_name, columns, modified_rows, csv_file_path)
            elif connection.vendor == 'postgresql':
                bulk_insert_postgresql(table_name, columns, modified_rows)
            else:
                bulk_insert_generic(table_name, columns, modified_rows)
            print(f"Successfully inserted {len(modified_rows)} rows into {table_name}")
        except Exception as e:
            print(f"ERROR: Failed to bulk insert into {table_name}")
            print(f"  Table: {table_name}")
            print(f"  Rows to insert: {len(modified_rows)}")
            print(f"  Columns: {columns}")
            print(f"  CSV file: {csv_file_path}")
            print(f"  Error: {str(e)}")
            raise


def bulk_insert_sqlite(table_name, columns, rows, original_csv_path):
    """
    Bulk insert for SQLite using temporary CSV file.
    Handles None values properly for auto-increment columns.

    Args:
        table_name: Name of the table to insert into
        columns: List of column names
        rows: List of row tuples/lists
        original_csv_path: Path to original CSV (for reference)
    """
    # Find columns and rows where primary key is None (for auto-increment)
    pk_column = get_primary_key_column(table_name)

    # Check if any rows have None in primary key position
    pk_index = columns.index(pk_column) if pk_column in columns else -1
    has_null_pks = pk_index >= 0 and any(row[pk_index] is None for row in rows)

    # If there are None primary keys, we need to exclude that column from import
    # so SQLite can auto-generate the IDs
    if has_null_pks:
        # Filter out the primary key column
        filtered_columns = [col for i, col in enumerate(columns) if i != pk_index]
        filtered_rows = [[val for i, val in enumerate(row) if i != pk_index] for row in rows]
        print(f"Excluding primary key column '{pk_column}' from import (contains NULL values for auto-increment)")
    else:
        filtered_columns = columns
        filtered_rows = rows

    # Estimate data size for optimization decision
    config = get_config()
    estimated_size_mb = (len(filtered_rows) * len(filtered_columns) * 50) / (1024 * 1024)  # Rough estimate
    in_memory_threshold = config.get('IN_MEMORY_THRESHOLD_MB', 10)
    use_compression = config.get('ENABLE_COMPRESSION', False) and estimated_size_mb > 50

    # Create temporary CSV file for restored data
    # Use in-memory file for small datasets, disk file for large ones
    if estimated_size_mb < in_memory_threshold:
        print(f"Using in-memory temporary file (estimated size: {estimated_size_mb:.2f} MB)")
        # Use StringIO for in-memory CSV
        from io import StringIO
        temp_buffer = StringIO()
        writer = csv_module.writer(temp_buffer)
        writer.writerow(filtered_columns)
        for row in filtered_rows:
            cleaned_row = ['' if val is None else val for val in row]
            writer.writerow(cleaned_row)

        # Write to temporary file (required for sqlite3 command-line tool)
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, newline='', encoding='utf-8') as temp_file:
            temp_csv_path = temp_file.name
            temp_file.write(temp_buffer.getvalue())
    else:
        print(f"Using disk-based temporary file (estimated size: {estimated_size_mb:.2f} MB, compression: {use_compression})")
        suffix = '.csv.gz' if use_compression else '.csv'

        if use_compression:
            import gzip
            with tempfile.NamedTemporaryFile(mode='wb', suffix=suffix, delete=False) as temp_file:
                temp_csv_path = temp_file.name
                with gzip.open(temp_file.name, 'wt', newline='', encoding='utf-8') as gz_file:
                    writer = csv_module.writer(gz_file)
                    writer.writerow(filtered_columns)
                    for row in filtered_rows:
                        cleaned_row = ['' if val is None else val for val in row]
                        writer.writerow(cleaned_row)

            # Decompress for sqlite import (sqlite3 doesn't support gzip directly)
            decompressed_path = temp_csv_path.replace('.gz', '')
            with gzip.open(temp_csv_path, 'rb') as gz_file:
                with open(decompressed_path, 'wb') as out_file:
                    out_file.write(gz_file.read())
            os.remove(temp_csv_path)  # Remove compressed file
            temp_csv_path = decompressed_path
        else:
            with tempfile.NamedTemporaryFile(mode='w', suffix=suffix, delete=False, newline='', encoding='utf-8') as temp_file:
                temp_csv_path = temp_file.name
                writer = csv_module.writer(temp_file)
                writer.writerow(filtered_columns)
                for row in filtered_rows:
                    cleaned_row = ['' if val is None else val for val in row]
                    writer.writerow(cleaned_row)

    try:
        # If there are NULL primary keys, we can't use CSV import
        # Fall back to direct SQL INSERTs with proper column specification
        if has_null_pks and pk_index >= 0:
            print(f"Using SQL INSERT method for {len(filtered_rows)} rows (has NULL primary keys)")
            with connection.cursor() as cursor:
                # Temporarily disable FK constraints for restoration
                # This is necessary because backed-up data may reference entities
                # that were replaced during the new import
                print(f"Temporarily disabling foreign key constraints for {table_name} restoration...")
                cursor.execute("PRAGMA foreign_keys = OFF")

                try:
                    column_list = ', '.join(filtered_columns)
                    placeholders = ', '.join(['?' for _ in filtered_columns])
                    insert_sql = f"INSERT INTO {table_name} ({column_list}) VALUES ({placeholders})"

                    # Convert None to NULL explicitly for SQL
                    cleaned_rows = []
                    for row in filtered_rows:
                        cleaned_row = [None if val == '' or val is None else val for val in row]
                        cleaned_rows.append(cleaned_row)

                    # Bulk insert using executemany
                    cursor.executemany(insert_sql, cleaned_rows)
                    print(f"Successfully inserted {len(cleaned_rows)} rows into {table_name}")
                finally:
                    # Re-enable FK constraints
                    cursor.execute("PRAGMA foreign_keys = ON")
                    print(f"Re-enabled foreign key constraints")
            return  # Skip CSV import

        # Standard CSV import for rows without NULL primary keys
        # Get database file path
        db_file = Path(connection.settings_dict['NAME']).absolute()

        # Create SQLite commands to import the restored data
        commands = [
            ".mode csv",
            ".separator ','",
            f".import --skip 1 '{temp_csv_path}' {table_name}"
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
            check=False  # Don't raise exception, we'll check returncode manually
        )

        if result.returncode != 0:
            # Display the actual error from sqlite3
            error_msg = f"SQLite import failed with exit code {result.returncode}"
            if result.stderr:
                error_msg += f"\nSTDERR: {result.stderr}"
            if result.stdout:
                error_msg += f"\nSTDOUT: {result.stdout}"
            error_msg += f"\nCommand: {' '.join([sqlite_program, str(db_file)])}"
            error_msg += f"\nDatabase: {db_file}"
            error_msg += f"\nCSV file: {temp_csv_path}"
            error_msg += f"\nSQL script:\n{sqlite_script}"
            print(error_msg)
            raise subprocess.CalledProcessError(result.returncode, [sqlite_program, str(db_file)], result.stdout, result.stderr)

        if result.stderr:
            print(f"SQLite restore warning: {result.stderr}")
    finally:
        # Clean up temporary file
        Path(temp_csv_path).unlink(missing_ok=True)


def bulk_insert_postgresql(table_name, columns, rows):
    """
    Bulk insert for PostgreSQL using COPY.

    Args:
        table_name: Name of the table to insert into
        columns: List of column names
        rows: List of row tuples/lists
    """
    # Create CSV string in memory
    output = io.StringIO()
    writer = csv_module.writer(output)
    for row in rows:
        writer.writerow(row)

    output.seek(0)

    with connection.cursor() as cursor:
        columns_str = ', '.join(columns)
        cursor.copy_expert(
            f"COPY {table_name} ({columns_str}) FROM STDIN WITH (FORMAT CSV, DELIMITER ',')",
            output
        )


def bulk_insert_generic(table_name, columns, rows):
    """
    Bulk insert for other databases using executemany.

    Args:
        table_name: Name of the table to insert into
        columns: List of column names
        rows: List of row tuples/lists
    """
    if not rows:
        return

    columns_str = ', '.join(columns)
    placeholders = ', '.join(['%s'] * len(columns))

    with connection.cursor() as cursor:
        cursor.executemany(
            f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})",
            rows
        )


def cleanup_backup_table(backup_table_name):
    """
    Drop the temporary backup table.
    Phase 5 of the backup-restore approach.

    Args:
        backup_table_name: Name of the backup table to drop
    """
    with connection.cursor() as cursor:
        if connection.vendor == 'sqlite':
            cursor.execute(f"DROP TABLE IF EXISTS {backup_table_name}")
        elif connection.vendor == 'postgresql':
            cursor.execute(f"DROP TABLE IF EXISTS {backup_table_name}")
        else:
            cursor.execute(f"DROP TABLE IF EXISTS {backup_table_name}")
