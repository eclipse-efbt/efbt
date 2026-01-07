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
"""
Bulk import operations for high-performance data import.

Provides SQLite-specific bulk import functionality for large datasets.
"""
import csv
import itertools
import logging
import os
import subprocess
import tempfile
from pathlib import Path
from typing import List, Set, Dict, Any

from django.db import models, connection

from .csv_utils import parse_csv_content, get_model_fields, convert_value
from .model_utils import is_safe_table_name, calculate_optimal_batch_size

logger = logging.getLogger(__name__)


def bulk_sqlite_import_with_index(
    csv_content: str,
    model_class,
    table_name: str,
    column_mappings: Dict[str, Dict[int, str]],
    allowed_table_names: Set[str],
    model_map: Dict[str, Any]
) -> List:
    """
    High-performance bulk import for large tables using SQLite3 directly.
    Auto-generates sequential indices for models using Django's auto-generated primary keys.

    Args:
        csv_content: CSV file content as string
        model_class: Django model class
        table_name: Database table name
        column_mappings: Column index to field name mappings
        allowed_table_names: Set of allowed table names for validation
        model_map: Mapping of table names to model classes

    Returns:
        List of imported model instances (limited sample)
    """
    logger.debug(f"Starting bulk SQLite3 import for {table_name}")

    # Validate table name strictly before any SQL execution
    if not is_safe_table_name(table_name, allowed_table_names) or table_name not in model_map:
        logger.error(f"Unsafe or unknown table name detected: {table_name}")
        raise Exception(f"Unsafe or unknown table name detected: {table_name}")

    # Parse CSV content
    headers, rows = parse_csv_content(csv_content)

    if not rows:
        logger.warning(f"No data rows found in CSV for {table_name}")
        return []

    # Check if model uses auto-generated primary key
    pk_fields = [field for field in model_class._meta.fields if field.primary_key]
    has_auto_pk = len(pk_fields) == 1 and pk_fields[0].name == 'id'

    if not has_auto_pk:
        raise ValueError(f"Bulk import with auto-generated index only supports models with auto-generated 'id' primary key. {model_class.__name__} doesn't qualify.")

    # Get database file path
    db_file = Path(connection.settings_dict['NAME']).absolute()
    if not db_file.exists():
        raise FileNotFoundError(f"Database file not found: {db_file}")

    # Create temporary CSV file with auto-generated indices
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as temp_file:
        csv_writer = csv.writer(temp_file)

        # Write headers with 'id' column first
        # For foreign key fields, we need to use the Django field names (with _id suffix)
        django_headers = ['id']
        model_fields = get_model_fields(model_class)

        if table_name in column_mappings:
            column_mapping = column_mappings[table_name]
            for i, header in enumerate(headers):
                if i in column_mapping:
                    field_name = column_mapping[i]
                    # Check if this is a foreign key field
                    if field_name in model_fields and isinstance(model_fields[field_name], models.ForeignKey):
                        django_headers.append(f"{field_name}_id")
                    else:
                        django_headers.append(field_name)
                else:
                    django_headers.append(header)
        else:
            django_headers.extend(headers)

        csv_writer.writerow(django_headers)

        # Write rows with auto-generated sequential IDs
        id_generator = itertools.count(1)  # Start from 1
        for row in rows:
            # Generate sequential ID
            row_id = next(id_generator)
            modified_row = [row_id] + list(row)
            csv_writer.writerow(modified_row)

        temp_csv_path = temp_file.name

    try:
        # Clear existing data first
        _clear_table_data(table_name, allowed_table_names, model_map)

        # Prepare SQLite import commands
        sqlite_commands = [
            ".mode csv",
            f".separator ','",
            f".import --skip 1 '{temp_csv_path}' {table_name}"
        ]

        sqlite_script = '\n'.join(sqlite_commands)

        # Execute SQLite import
        logger.debug(f"Executing bulk SQLite import for {table_name}")
        result = subprocess.run(
            ['sqlite3', str(db_file)],
            input=sqlite_script,
            text=True,
            capture_output=True,
            check=False
        )

        if result.returncode != 0:
            error_msg = f"SQLite bulk import failed: {result.stderr}"
            logger.error(error_msg)
            raise Exception(error_msg)

        # Verify import success
        with connection.cursor() as cursor:
            if not is_safe_table_name(table_name, allowed_table_names) or table_name not in model_map:
                logger.error(f"Unsafe or unknown table name detected (count): {table_name}")
                raise Exception(f"Unsafe or unknown table name detected (count): {table_name}")
            cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
            imported_count = cursor.fetchone()[0]

        logger.debug(f"Bulk SQLite import completed: {imported_count} records imported to {table_name}")

        # Return mock objects list (limited for memory efficiency)
        imported_objects = list(model_class.objects.all()[:min(100, imported_count)])
        return imported_objects

    except Exception as e:
        logger.error(f"Bulk SQLite import failed for {table_name}: {e}")
        raise
    finally:
        # Clean up temporary file
        try:
            os.unlink(temp_csv_path)
        except Exception as cleanup_error:
            logger.warning(f"Failed to clean up temporary file {temp_csv_path}: {cleanup_error}")


def _clear_table_data(table_name: str, allowed_table_names: Set[str], model_map: Dict[str, Any]) -> None:
    """Clear all data from a table safely."""
    if not is_safe_table_name(table_name, allowed_table_names):
        raise ValueError(f"Unsafe table name detected: {table_name}")

    with connection.cursor() as cursor:
        if connection.vendor == 'sqlite':
            cursor.execute("PRAGMA foreign_keys = 0;")

        cursor.execute(f"DELETE FROM {table_name};")

        if connection.vendor == 'sqlite':
            cursor.execute(f"DELETE FROM sqlite_sequence WHERE name='{table_name}';")
            cursor.execute("PRAGMA foreign_keys = 1;")

    logger.debug(f"Cleared existing data from {table_name}")


def resolve_foreign_keys_post_bulk_import(
    model_class,
    table_name: str,
    csv_headers: List[str],
    column_mappings: Dict[str, Dict[int, str]],
    allowed_table_names: Set[str]
) -> None:
    """
    Resolve foreign key relationships after bulk SQLite import.
    The bulk import stores string values, we need to convert them to proper FK references.

    Args:
        model_class: Django model class
        table_name: Database table name
        csv_headers: List of CSV column headers
        column_mappings: Column index to field name mappings
        allowed_table_names: Set of allowed table names
    """
    logger.debug(f"Resolving foreign keys for {table_name}")

    model_fields = get_model_fields(model_class)
    fk_fields = {name: field for name, field in model_fields.items() if isinstance(field, models.ForeignKey)}

    if not fk_fields:
        logger.debug(f"No foreign keys to resolve for {table_name}")
        return

    # Get column mappings for this table
    if table_name not in column_mappings:
        logger.warning(f"No column mappings found for {table_name}, skipping FK resolution")
        return

    column_mapping = column_mappings[table_name]

    # Build mapping of CSV column index to FK field name
    csv_to_fk_mapping = {}
    for col_idx, field_name in column_mapping.items():
        if field_name in fk_fields and col_idx < len(csv_headers):
            csv_to_fk_mapping[col_idx] = field_name

    if not csv_to_fk_mapping:
        logger.debug(f"No FK mappings found for {table_name}")
        return

    logger.debug(f"Resolving {len(csv_to_fk_mapping)} foreign key fields: {list(csv_to_fk_mapping.values())}")

    # Process FK resolution in batches to avoid memory issues
    batch_size = 1000

    if not is_safe_table_name(table_name, allowed_table_names):
        raise ValueError(f"Unsafe table name detected in FK resolution: {table_name}")

    with connection.cursor() as cursor:
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        total_records = cursor.fetchone()[0]

        for offset in range(0, total_records, batch_size):
            cursor.execute(f"SELECT id, {', '.join(csv_to_fk_mapping.values())} FROM {table_name} LIMIT {batch_size} OFFSET {offset}")
            records = cursor.fetchall()

            if not records:
                break

            # Process each record in the batch
            updates = []
            for record in records:
                record_id = record[0]
                fk_updates = {}

                # Process each FK field
                for i, (col_idx, field_name) in enumerate(csv_to_fk_mapping.items(), 1):
                    fk_string_value = record[i]

                    if fk_string_value and str(fk_string_value).strip():
                        # Get related model and try to find the object
                        fk_field = fk_fields[field_name]
                        related_model = fk_field.related_model

                        try:
                            related_obj = related_model.objects.get(pk=fk_string_value.strip())
                            fk_updates[f"{field_name}_id"] = related_obj.pk
                        except related_model.DoesNotExist:
                            logger.warning(f"Foreign key object not found: {related_model.__name__} with pk '{fk_string_value}'")
                            try:
                                related_obj = related_model.objects.create(pk=fk_string_value.strip())
                                fk_updates[f"{field_name}_id"] = related_obj.pk
                                logger.debug(f"Created missing {related_model.__name__} object with pk '{fk_string_value}'")
                            except Exception as create_error:
                                logger.error(f"Failed to create missing FK object: {create_error}")
                        except Exception as lookup_error:
                            logger.error(f"Error looking up FK object: {lookup_error}")

                if fk_updates:
                    updates.append((record_id, fk_updates))

            # Execute batch updates
            for record_id, fk_updates in updates:
                if fk_updates:
                    set_clause = ', '.join([f"{field} = ?" for field in fk_updates.keys()])
                    values = list(fk_updates.values()) + [record_id]
                    cursor.execute(f"UPDATE {table_name} SET {set_clause} WHERE id = ?", values)

            logger.debug(f"Processed FK resolution for batch {offset}-{offset + len(records)} of {total_records}")

    logger.debug(f"Completed foreign key resolution for {table_name}")


def fallback_csv_import(
    csv_file: str,
    table_name: str,
    delimiter: str,
    model_map: Dict[str, Any]
) -> None:
    """
    Fallback CSV import for databases that don't support native CSV import.

    Args:
        csv_file: Path to CSV file
        table_name: Database table name
        delimiter: CSV delimiter character
        model_map: Mapping of table names to model classes
    """
    logger.debug(f"Using fallback CSV import for {table_name}")

    with open(csv_file, 'r', encoding='utf-8') as f:
        csv_reader = csv.reader(f, delimiter=delimiter)
        headers = next(csv_reader)  # Skip header

        # Get model class
        if table_name not in model_map:
            raise ValueError(f"No model found for table: {table_name}")

        model_class = model_map[table_name]
        model_fields = get_model_fields(model_class)

        # Prepare objects for bulk create
        objects_to_create = []
        batch_size = calculate_optimal_batch_size(model_class)

        for row in csv_reader:
            if not any(row):  # Skip empty rows
                continue

            obj_data = {}
            for i, value in enumerate(row):
                if i < len(headers) and headers[i] in model_fields:
                    field = model_fields[headers[i]]
                    converted_value = convert_value(field, value, defer_foreign_keys=True)
                    if converted_value is not None:
                        obj_data[headers[i]] = converted_value

            if obj_data:
                obj = model_class(**obj_data)
                objects_to_create.append(obj)

                if len(objects_to_create) >= batch_size:
                    model_class.objects.bulk_create(objects_to_create, batch_size=batch_size)
                    objects_to_create = []
                    logger.debug(f"Bulk created {batch_size} objects for {table_name}")

        # Create remaining objects
        if objects_to_create:
            model_class.objects.bulk_create(objects_to_create, batch_size=batch_size)
            logger.debug(f"Bulk created final {len(objects_to_create)} objects for {table_name}")


def create_instances_from_csv_copy(
    csv_file_path: str,
    model_class,
    allowed_table_names: Set[str]
) -> Any:
    """
    Fast CSV import using SQLite3 command line tool consistently for all databases.

    Args:
        csv_file_path: Path to CSV file
        model_class: Django model class
        allowed_table_names: Set of allowed table names

    Returns:
        subprocess result object
    """
    table_name = model_class._meta.db_table
    csv_file = Path(csv_file_path).absolute()
    delimiter = ","

    # Check if CSV file exists
    if not csv_file.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_file}")

    # Validate CSV file
    try:
        with open(csv_file, 'r', encoding='utf-8') as f:
            first_line = f.readline().strip()
            if not first_line:
                raise ValueError(f"CSV file {csv_file} is empty")

            headers = first_line.split(delimiter)
            if len(headers) < 1:
                raise ValueError(f"CSV file {csv_file} has no columns")

            logger.debug(f"CSV validation passed: {len(headers)} columns found in {csv_file}")

    except Exception as e:
        logger.error(f"CSV validation failed for {csv_file}: {e}")
        raise

    logger.debug(f"Starting fast CSV import for {table_name} from {csv_file}")

    try:
        # Clear the table first with proper foreign key handling
        with connection.cursor() as cursor:
            if connection.vendor == 'sqlite':
                cursor.execute("PRAGMA foreign_keys = 0;")

            cursor.execute(f"DELETE FROM {table_name};")

            if connection.vendor == 'sqlite':
                cursor.execute(f"DELETE FROM sqlite_sequence WHERE name='{table_name}';")
                cursor.execute("PRAGMA foreign_keys = 1;")

            cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
            count = cursor.fetchone()[0]
            if count > 0:
                raise Exception(f"Failed to clear table {table_name}. Still has {count} records.")

            logger.debug(f"Successfully cleared table {table_name}")

        # Get database file path
        db_file = Path(connection.settings_dict['NAME']).absolute()

        if not db_file.exists():
            raise FileNotFoundError(f"Database file not found: {db_file}")

        # Create the SQLite commands
        commands = [
            ".mode csv",
            f".separator '{delimiter}'",
            f".import --skip 1 '{csv_file}' {table_name}"
        ]

        sqlite_script = '\n'.join(commands)

        # Execute the SQLite import
        logger.debug(f"Executing SQLite import: sqlite3 {db_file}")
        result = subprocess.run(
            ['sqlite3', str(db_file)],
            input=sqlite_script,
            text=True,
            capture_output=True,
            check=False
        )

        if result.returncode != 0:
            error_msg = f"SQLite import failed with return code {result.returncode}"
            if result.stderr:
                error_msg += f": {result.stderr}"
            if result.stdout:
                error_msg += f" (stdout: {result.stdout})"
            raise Exception(error_msg)

        if result.stderr and result.stderr.strip():
            logger.warning(f"SQLite import warnings: {result.stderr}")

        # Verify import success
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
            imported_count = cursor.fetchone()[0]
            logger.debug(f"SQLite import completed successfully for {table_name}: {imported_count} records imported")

            if imported_count == 0:
                logger.warning(f"No records were imported into {table_name}. Check CSV file format.")

        return result

    except Exception as e:
        logger.error(f"Error importing CSV for {table_name}: {str(e)}")
        if hasattr(e, 'stderr') and e.stderr:
            logger.error(f"SQLite stderr: {e.stderr}")
        if hasattr(e, 'stdout') and e.stdout:
            logger.error(f"SQLite stdout: {e.stdout}")
        raise
