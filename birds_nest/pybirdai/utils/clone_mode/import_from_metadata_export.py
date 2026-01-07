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
CSV Data Import from Metadata Export for Clone Mode.

This is a thin orchestration layer that uses the importer/ submodule
for the actual import logic.
"""
import csv
import zipfile
import io
import os
import glob
import logging
import json
import tempfile
from datetime import datetime

# Django setup
import django
from django.conf import settings
if not settings.configured:
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'birds_nest.settings')
    django.setup()

from django.db import transaction, models, connection

# Import from submodules
from .importer import (
    build_column_mappings,
    build_model_map,
    get_import_order,
    get_table_name_from_csv_filename,
    calculate_optimal_batch_size,
    is_high_volume_table,
    is_safe_table_name,
    should_import_table,
    parse_csv_content,
    convert_value,
    get_model_fields,
    bulk_sqlite_import_with_index,
)

# Set up logger for this module
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Add console handler if not already present
if not logger.handlers:
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)


class CSVDataImporter:
    """
    CSV Data Importer for BIRD metadata exports.

    This class orchestrates the import of CSV files exported from clone mode.
    It uses utility functions from the importer/ submodule for the actual
    parsing, conversion, and bulk import operations.
    """

    def __init__(self, results_dir="import_results", framework_ids=None):
        """
        Initialize the CSV data importer.

        Args:
            results_dir: Directory to save import results
            framework_ids: Optional list of framework IDs to filter imports.
                          If provided, only tables relevant to these frameworks will be imported.
        """
        self.results_dir = results_dir
        self.id_mappings = {}  # Track ID mappings for models with auto-generated IDs
        self.framework_ids = framework_ids  # Framework filter for import

        # Build model and column mappings using submodule functions
        self.model_map, self.allowed_table_names = build_model_map()
        self.column_mappings = build_column_mappings()

        self._ensure_results_directory()

        if framework_ids:
            logger.debug(f"CSVDataImporter initialized with framework filter: {framework_ids}")
        else:
            logger.debug("CSVDataImporter initialized (no framework filter)")

    def _is_safe_table_name(self, table_name):
        """Validate table name against whitelist and pattern."""
        return is_safe_table_name(table_name, self.allowed_table_names)

    def _should_import_table(self, table_name):
        """Check if a table should be imported based on framework filter."""
        return should_import_table(table_name, self.framework_ids)

    def _ensure_results_directory(self):
        """Ensure the results directory exists."""
        if not os.path.exists(self.results_dir):
            os.makedirs(self.results_dir)
            logger.debug(f"Created results directory: {self.results_dir}")

    def _save_results(self, results, operation_type="import"):
        """Save import results to a JSON file."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{operation_type}_results_{timestamp}.json"
        filepath = os.path.join(self.results_dir, filename)

        # Prepare results for JSON serialization
        serializable_results = {}
        for key, value in results.items():
            if isinstance(value, dict):
                serializable_value = {
                    'success': value.get('success', False),
                    'imported_count': value.get('imported_count', 0)
                }
                if 'error' in value:
                    serializable_value['error'] = value['error']
                serializable_results[key] = serializable_value
            else:
                serializable_results[key] = value

        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(serializable_results, f, indent=2, ensure_ascii=False)
            logger.debug(f"Results saved to: {filepath}")
            return filepath
        except Exception as e:
            logger.error(f"Failed to save results to {filepath}: {e}")
            return None

    def _get_import_order(self):
        """Define the order in which tables should be imported."""
        return get_import_order()

    def _get_table_name_from_csv_filename(self, filename):
        """Convert CSV filename back to table name."""
        return get_table_name_from_csv_filename(filename)

    def _parse_csv_content(self, csv_content):
        """Parse CSV content and return headers and rows."""
        return parse_csv_content(csv_content)

    def _convert_value(self, field, value, defer_foreign_keys=False):
        """Convert CSV string value to appropriate Python type."""
        return convert_value(field, value, defer_foreign_keys)

    def _get_model_fields(self, model_class):
        """Get model fields as a dictionary."""
        return get_model_fields(model_class)

    def _calculate_optimal_batch_size(self, model_class, base_batch_size=250):
        """Calculate optimal batch size based on model field count."""
        return calculate_optimal_batch_size(model_class, base_batch_size)

    def _is_high_volume_table(self, table_name, row_count):
        """Determine if a table should use bulk SQLite import."""
        return is_high_volume_table(table_name, row_count)

    def _bulk_sqlite_import_with_index(self, csv_content, model_class, table_name):
        """High-performance bulk import using SQLite3 directly."""
        return bulk_sqlite_import_with_index(
            csv_content,
            model_class,
            table_name,
            self.column_mappings,
            self.allowed_table_names,
            self.model_map
        )

    def import_csv_file(self, csv_filename, csv_content, use_fast_import=False):
        """
        Import a single CSV file using column index mappings.

        Args:
            csv_filename: Name of the CSV file
            csv_content: Content of the CSV file as string
            use_fast_import: If True, use fast SQL-based import method

        Returns:
            List of imported model instances
        """
        logger.debug(f"Starting import of CSV file: {csv_filename} (fast_import={use_fast_import})")

        table_name = self._get_table_name_from_csv_filename(csv_filename)
        logger.debug(f"Mapped CSV file '{csv_filename}' to table '{table_name}'")

        if table_name not in self.model_map:
            logger.warning(f"No model found for table: {table_name}. Skipping file {csv_filename}")
            return []

        if table_name not in self.column_mappings:
            logger.warning(f"No column mapping found for table: {table_name}. Skipping file {csv_filename}")
            return []

        model_class = self.model_map[table_name]
        column_mapping = self.column_mappings[table_name]
        model_fields = self._get_model_fields(model_class)

        # Parse CSV to get row count for high-volume detection
        headers, rows = self._parse_csv_content(csv_content)
        row_count = len(rows)

        # Check if this should use bulk SQLite import for high-volume data
        if self._is_high_volume_table(table_name, row_count):
            logger.debug(f"High-volume table detected ({row_count} rows). Using bulk SQLite3 import for {table_name}")

            # Check if model is compatible with bulk import
            pk_fields = [field for field in model_fields.values() if field.primary_key]
            has_auto_pk = len(pk_fields) == 1 and pk_fields[0].name == 'id'

            if has_auto_pk:
                try:
                    return self._bulk_sqlite_import_with_index(csv_content, model_class, table_name)
                except Exception as e:
                    logger.debug(f"Bulk import failed for {table_name}, falling back to standard import: {e}")

        # Standard import path
        return self._standard_import(
            csv_filename, csv_content, table_name, model_class,
            column_mapping, model_fields, headers, rows, use_fast_import
        )

    def _standard_import(self, csv_filename, csv_content, table_name, model_class,
                        column_mapping, model_fields, headers, rows, use_fast_import):
        """
        Standard row-by-row import with batching.

        This handles the complex import logic for most tables.
        """
        optimal_batch_size = self._calculate_optimal_batch_size(model_class)
        logger.debug(f"Using model {model_class.__name__} for table {table_name}")
        logger.debug(f"Optimal batch size: {optimal_batch_size}")

        if not rows:
            logger.warning(f"No data rows found in CSV for {table_name}")
            return []

        imported_objects = []
        batch = []
        errors = []

        # Determine if model has auto-generated primary key
        pk_field = model_class._meta.pk
        has_auto_pk = pk_field.name == 'id' and pk_field.auto_created

        for row_idx, row in enumerate(rows):
            if not any(row):  # Skip empty rows
                continue

            try:
                obj_data = {}

                # Map CSV columns to model fields
                for col_idx, field_name in column_mapping.items():
                    if col_idx < len(row):
                        value = row[col_idx]
                        if field_name in model_fields:
                            field = model_fields[field_name]
                            converted_value = self._convert_value(field, value, defer_foreign_keys=True)
                            if converted_value is not None:
                                # For FK fields, use the _id suffix for raw IDs
                                if isinstance(field, models.ForeignKey):
                                    obj_data[f"{field_name}_id"] = converted_value
                                else:
                                    obj_data[field_name] = converted_value

                if obj_data:
                    obj = model_class(**obj_data)
                    batch.append(obj)

                    if len(batch) >= optimal_batch_size:
                        try:
                            model_class.objects.bulk_create(batch, batch_size=optimal_batch_size)
                            imported_objects.extend(batch)
                            logger.debug(f"Bulk created {len(batch)} objects for {table_name}")
                        except Exception as batch_error:
                            logger.debug(f"Batch create failed, trying individual inserts: {batch_error}")
                            for single_obj in batch:
                                try:
                                    single_obj.save()
                                    imported_objects.append(single_obj)
                                except Exception as single_error:
                                    errors.append(str(single_error))
                        batch = []

            except Exception as row_error:
                errors.append(f"Row {row_idx}: {str(row_error)}")
                continue

        # Process remaining batch
        if batch:
            try:
                model_class.objects.bulk_create(batch, batch_size=optimal_batch_size)
                imported_objects.extend(batch)
                logger.debug(f"Bulk created final {len(batch)} objects for {table_name}")
            except Exception as batch_error:
                logger.debug(f"Final batch create failed, trying individual inserts: {batch_error}")
                for single_obj in batch:
                    try:
                        single_obj.save()
                        imported_objects.append(single_obj)
                    except Exception as single_error:
                        errors.append(str(single_error))

        if errors:
            logger.warning(f"Import completed with {len(errors)} errors for {table_name}")
            logger.debug(f"First 5 errors: {errors[:5]}")

        logger.debug(f"Imported {len(imported_objects)} objects for {table_name}")
        return imported_objects

    def import_from_csv_string(self, csv_string, filename="data.csv", use_fast_import=False):
        """Import CSV data from a string."""
        logger.debug(f"Importing CSV data from string for filename: {filename}")
        try:
            with transaction.atomic():
                imported_objects = self.import_csv_file(filename, csv_string, use_fast_import=use_fast_import)
                result = {
                    filename: {
                        'success': True,
                        'imported_count': len(imported_objects),
                        'objects': imported_objects
                    }
                }
                self._save_results(result, "csv_string_import")
                return result
        except Exception as e:
            logger.error(f"Failed to import CSV string: {e}")
            result = {
                filename: {
                    'success': False,
                    'error': str(e)
                }
            }
            self._save_results(result, "csv_string_import")
            return result

    def import_from_path(self, path):
        """Import CSV files from either a zip file or a directory."""
        logger.debug(f"Starting import from path: {path}")

        if os.path.isfile(path):
            if path.endswith('.zip'):
                logger.debug(f"Processing zip file: {path}")
                result = self.import_zip_file(path)
                self._save_results(result, "zip_import")
                return result
            elif path.endswith('.csv'):
                logger.debug(f"Processing single CSV file: {path}")
                with open(path, 'r', encoding='utf-8') as f:
                    csv_content = f.read()
                filename = os.path.basename(path)
                imported_objects = self.import_csv_file(filename, csv_content)
                result = {
                    filename: {
                        'success': True,
                        'imported_count': len(imported_objects),
                        'objects': imported_objects
                    }
                }
                self._save_results(result, "single_csv_import")
                return result
            else:
                error_msg = f"Unsupported file type: {path}"
                logger.error(error_msg)
                raise ValueError(error_msg)
        elif os.path.isdir(path):
            logger.debug(f"Processing directory: {path}")
            result = self.import_folder(path)
            self._save_results(result, "folder_import")
            return result
        else:
            error_msg = f"Path does not exist: {path}"
            logger.error(error_msg)
            raise ValueError(error_msg)

    def import_folder(self, folder_path):
        """Import all CSV files from a folder."""
        logger.debug(f"Importing CSV files from folder: {folder_path}")
        results = {}

        csv_files = glob.glob(os.path.join(folder_path, "*.csv"))
        logger.debug(f"Found {len(csv_files)} CSV files in folder")

        for csv_file_path in csv_files:
            filename = os.path.basename(csv_file_path)
            logger.debug(f"Processing file: {filename}")
            try:
                with open(csv_file_path, 'r', encoding='utf-8') as f:
                    csv_content = f.read()
                imported_objects = self.import_csv_file(filename, csv_content)
                results[filename] = {
                    'success': True,
                    'imported_count': len(imported_objects),
                    'objects': imported_objects
                }
            except Exception as e:
                logger.error(f"Failed to process {filename}: {e}")
                results[filename] = {
                    'success': False,
                    'error': str(e)
                }

        logger.debug(f"Completed folder import. Processed {len(csv_files)} files")
        return results

    def import_zip_file(self, zip_file_path_or_content):
        """Import CSV files from a zip archive."""
        logger.debug("Starting zip file import")
        if isinstance(zip_file_path_or_content, str):
            logger.debug(f"Processing zip file from path: {zip_file_path_or_content}")
            with zipfile.ZipFile(zip_file_path_or_content, 'r') as zip_file:
                return self._process_zip_contents(zip_file)
        else:
            logger.debug("Processing zip file from bytes content")
            with zipfile.ZipFile(io.BytesIO(zip_file_path_or_content), 'r') as zip_file:
                return self._process_zip_contents(zip_file)

    def _process_zip_contents(self, zip_file):
        """Process contents of an opened zip file."""
        logger.debug("Processing zip file contents")
        results = {}
        csv_files = [f for f in zip_file.namelist() if f.endswith('.csv')]
        logger.debug(f"Found {len(csv_files)} CSV files in zip archive")

        for csv_filename in csv_files:
            logger.debug(f"Processing CSV file from zip: {csv_filename}")
            try:
                csv_content = zip_file.read(csv_filename).decode('utf-8')
                imported_objects = self.import_csv_file(csv_filename, csv_content)
                results[csv_filename] = {
                    'success': True,
                    'imported_count': len(imported_objects),
                    'objects': imported_objects
                }
            except Exception as e:
                logger.error(f"Failed to process {csv_filename} from zip: {e}")
                results[csv_filename] = {
                    'success': False,
                    'error': str(e)
                }

        logger.debug(f"Completed zip file processing. Processed {len(csv_files)} CSV files")
        return results

    def import_from_csv_strings(self, csv_strings_list):
        """Import CSV data from a dictionary of CSV strings."""
        logger.debug(f"Importing CSV data from {len(csv_strings_list)} CSV strings")
        results = {}

        for filename, csv_string in csv_strings_list.items():
            logger.debug(f"Processing CSV string for filename: {filename}")
            try:
                imported_objects = self.import_csv_file(filename, csv_string)
                results[filename] = {
                    'success': True,
                    'imported_count': len(imported_objects),
                    'objects': imported_objects
                }
            except Exception as e:
                logger.error(f"Failed to process CSV string for {filename}: {e}")
                results[filename] = {
                    'success': False,
                    'error': str(e)
                }

        logger.debug(f"Completed CSV strings import. Processed {len(csv_strings_list)} files")
        self._save_results(results, "csv_strings_import")
        return results

    def import_from_csv_strings_ordered(self, csv_strings_list, use_fast_import=False):
        """Import CSV data from a dictionary of CSV strings in dependency order."""
        logger.debug(f"Starting ordered import from {len(csv_strings_list)} CSV strings")
        if self.framework_ids:
            logger.debug(f"Framework filter active: {self.framework_ids}")

        import_order = self._get_import_order()
        results = {}
        skipped_tables = []

        # Import files in dependency order
        for table_name in import_order:
            if not self._should_import_table(table_name):
                skipped_tables.append(table_name)
                continue

            # Find CSV file for this table
            csv_filename = None
            for filename in csv_strings_list.keys():
                if self._get_table_name_from_csv_filename(filename) == table_name:
                    csv_filename = filename
                    break

            if csv_filename and csv_filename in csv_strings_list:
                logger.debug(f"Importing {csv_filename} for table {table_name}")
                try:
                    csv_content = csv_strings_list[csv_filename]
                    imported_objects = self.import_csv_file(csv_filename, csv_content, use_fast_import=use_fast_import)
                    results[csv_filename] = {
                        'success': True,
                        'imported_count': len(imported_objects),
                        'objects': imported_objects
                    }
                except Exception as e:
                    logger.error(f"Failed to import {csv_filename}: {e}")
                    results[csv_filename] = {
                        'success': False,
                        'error': str(e)
                    }

        if skipped_tables:
            logger.debug(f"Skipped {len(skipped_tables)} tables due to framework filter")

        # Import any remaining CSV files not in the ordered list
        for filename, csv_content in csv_strings_list.items():
            if filename not in results:
                table_name = self._get_table_name_from_csv_filename(filename)
                if not self._should_import_table(table_name):
                    continue

                logger.debug(f"Importing remaining file: {filename}")
                try:
                    imported_objects = self.import_csv_file(filename, csv_content, use_fast_import=use_fast_import)
                    results[filename] = {
                        'success': True,
                        'imported_count': len(imported_objects),
                        'objects': imported_objects
                    }
                except Exception as e:
                    logger.error(f"Failed to import {filename}: {e}")
                    results[filename] = {
                        'success': False,
                        'error': str(e)
                    }

        self._save_results(results, "csv_strings_ordered_import")
        logger.debug(f"Completed ordered CSV strings import. Processed {len(results)} files")
        return results

    def import_from_path_ordered(self, path, use_fast_import=False):
        """Import CSV files from a path in dependency order."""
        logger.debug(f"Starting ordered import from path: {path}")
        if self.framework_ids:
            logger.debug(f"Framework filter active: {self.framework_ids}")

        # Collect all available CSV files
        csv_files_data = {}

        if os.path.isfile(path):
            if path.endswith('.zip'):
                with zipfile.ZipFile(path, 'r') as zip_file:
                    csv_files = [f for f in zip_file.namelist() if f.endswith('.csv')]
                    for csv_filename in csv_files:
                        csv_content = zip_file.read(csv_filename).decode('utf-8')
                        csv_files_data[csv_filename] = csv_content
            elif path.endswith('.csv'):
                filename = os.path.basename(path)
                with open(path, 'r', encoding='utf-8') as f:
                    csv_files_data[filename] = f.read()
        elif os.path.isdir(path):
            csv_files = glob.glob(os.path.join(path, "*.csv"))
            for csv_file_path in csv_files:
                filename = os.path.basename(csv_file_path)
                with open(csv_file_path, 'r', encoding='utf-8') as f:
                    csv_files_data[filename] = f.read()

        # Use the ordered import method
        return self.import_from_csv_strings_ordered(csv_files_data, use_fast_import=use_fast_import)


# Convenience functions
def import_bird_data_from_csv_export(path_or_content, use_fast_import=False):
    """
    Convenience function to import bird data from a CSV export.

    Args:
        path_or_content: Either a file path to a zip/folder/CSV, or file content (bytes) for zip
        use_fast_import: If True, use fast SQL-based import method

    Returns:
        Dictionary with import results for each CSV file
    """
    logger.debug("Starting bird data import from CSV export")
    importer = CSVDataImporter()

    if isinstance(path_or_content, bytes):
        logger.debug("Processing as zip file content (bytes)")
        result = importer.import_zip_file(path_or_content)
        importer._save_results(result, "bird_data_import_bytes")
    else:
        logger.debug(f"Processing as file path: {path_or_content}")
        result = importer.import_from_path(path_or_content)
        importer._save_results(result, "bird_data_import_path")

    logger.debug("Completed bird data import from CSV export")
    return result


def import_bird_data_from_csv_export_ordered(path_or_content, use_fast_import=False):
    """
    Convenience function to import bird data from a CSV export in dependency order.

    Args:
        path_or_content: Either a file path to a zip/folder/CSV, or file content (bytes) for zip
        use_fast_import: If True, use fast SQL-based import method

    Returns:
        Dictionary with import results for each CSV file
    """
    logger.debug("Starting ordered bird data import from CSV export")
    importer = CSVDataImporter()

    if isinstance(path_or_content, bytes):
        logger.debug("Processing as zip file content (bytes)")
        with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as temp_file:
            temp_file.write(path_or_content)
            temp_file_path = temp_file.name

        try:
            result = importer.import_from_path_ordered(temp_file_path, use_fast_import=use_fast_import)
            importer._save_results(result, "bird_data_import_ordered_bytes")
        finally:
            os.unlink(temp_file_path)
    else:
        logger.debug(f"Processing as file path: {path_or_content}")
        result = importer.import_from_path_ordered(path_or_content, use_fast_import=use_fast_import)
        importer._save_results(result, "bird_data_import_ordered_path")

    logger.debug("Completed ordered bird data import from CSV export")
    return result


# Backward compatibility exports
__all__ = [
    'CSVDataImporter',
    'import_bird_data_from_csv_export',
    'import_bird_data_from_csv_export_ordered',
]
