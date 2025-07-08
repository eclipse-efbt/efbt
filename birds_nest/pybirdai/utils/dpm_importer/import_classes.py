import csv
import os
import logging
from datetime import datetime
import django
from django.db import models
from django.conf import settings
import sys
from django.db import transaction, models
from django.apps import apps
from django.core.exceptions import ObjectDoesNotExist, ValidationError

from model_mapper import ModelMapper
from constants import PRIORITY_ORDER

import warnings
warnings.filterwarnings("ignore")

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DjangoSetup:
    @staticmethod
    def configure_django():
        """Configure Django settings without starting the application"""
        if not settings.configured:
            # Set up Django settings module for birds_nest in parent directory
            project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..'))
            sys.path.insert(0, project_root)
            os.environ['DJANGO_SETTINGS_MODULE'] = 'birds_nest.settings'
            django.setup()

class CSVExporter:
    """
    CSVExporter class that exports mapped DPM data to CSV files for debugging
    and validation purposes without importing to the database.
    """

    def __init__(self, csv_directory="target", export_directory="export_debug", batch_size=1000):
        """
        Initialize the CSVExporter with ModelMapper integration.

        Args:
            csv_directory (str): Directory containing source CSV files
            export_directory (str): Directory for exported mapped data
            batch_size (int): Number of records to process in each batch
        """
        os.makedirs(csv_directory, exist_ok=True)
        os.makedirs(export_directory, exist_ok=True)
        os.makedirs(os.path.join(export_directory, "mapped"), exist_ok=True)
        os.makedirs(os.path.join(export_directory, "reports"), exist_ok=True)

        self.csv_directory = csv_directory
        self.export_directory = export_directory
        self.batch_size = batch_size
        self.model_mapper = ModelMapper()
        self.export_stats = {
            'total_files': 0,
            'successful_exports': 0,
            'failed_exports': 0,
            'total_records_processed': 0,
            'total_records_exported': 0,
            'errors': [],
            'warnings': []
        }
        # Add caching for performance
        self._transformation_cache = {}

    def export_mapped_data(self):
        """
        Export all mapped data to CSV files for debugging and validation.

        Returns:
            dict: Export statistics and results
        """
        # logger.debug("Starting export of mapped DPM data...")

        # Clear existing export files to avoid conflicts
        mapped_dir = os.path.join(self.export_directory, "mapped")
        if os.path.exists(mapped_dir):
            import shutil
            shutil.rmtree(mapped_dir)
        os.makedirs(mapped_dir, exist_ok=True)

        # Initialize lookup tables for accurate ID mapping
        # logger.debug("Populating lookup tables for ID resolution...")
        from constants import auto_populate_lookup_tables
        lookup_summary = auto_populate_lookup_tables(self.csv_directory)
        # logger.debug(f"Loaded {lookup_summary['loaded']} lookup records")
        if lookup_summary['errors']:
            logger.warning(f"Lookup table errors: {lookup_summary['errors'][:5]}")

        # Validate function definitions before export
        validation_errors = self._validate_function_definitions()
        if validation_errors:
            logger.error("Missing function definitions found:")
            for error in validation_errors:
                logger.error(f"  - {error}")
            self.export_stats['errors'].extend(validation_errors)

        # Get list of CSV files
        csv_files = [f for f in os.listdir(self.csv_directory) if f.endswith('.csv')]
        self.export_stats['total_files'] = len(csv_files)

        # Process each CSV file
        for csv_file in csv_files:
            try:
                file_stats = self.export_csv_file(csv_file)
                if file_stats.get('success', False):
                    self.export_stats['successful_exports'] += 1
                    self.export_stats['total_records_processed'] += file_stats.get('records_processed', 0)
                    self.export_stats['total_records_exported'] += file_stats.get('records_exported', 0)
                else:
                    self.export_stats['failed_exports'] += 1
                    if 'errors' in file_stats:
                        self.export_stats['errors'].extend(file_stats['errors'])

            except Exception as e:
                self.export_stats['failed_exports'] += 1
                error_msg = f"Failed to export {csv_file}: {str(e)}"
                self.export_stats['errors'].append(error_msg)
                logger.error(error_msg)

        # Generate summary report
        self._generate_export_summary()

        # logger.debug(f"Export completed: {self.export_stats['successful_exports']} successful, "
        #           f"{self.export_stats['failed_exports']} failed")

        return self.export_stats

    def export_csv_file(self, csv_filename):
        """
        Export a single CSV file's mapped data.

        Args:
            csv_filename (str): Name of the CSV file

        Returns:
            dict: Export statistics for this file
        """
        source_table = csv_filename.replace('.csv', '')
        csv_path = os.path.join(self.csv_directory, csv_filename)

        if not os.path.exists(csv_path):
            return {'success': False, 'error': f"File not found: {csv_path}"}

        # Check if we have mapping for this table
        mapping = self.model_mapper.get_mapping(source_table)
        if not mapping:
            # logger.debug(f"No mapping defined for table: {source_table}, skipping...")
            return {'success': True, 'skipped': True, 'reason': 'No mapping defined'}

        file_stats = {
            'filename': csv_filename,
            'source_table': source_table,
            'target_table': mapping['target_table'],
            'records_processed': 0,
            'records_exported': 0,
            'transformation_issues': [],
            'validation_warnings': [],
            'success': True
        }

        # logger.debug(f"Starting export of {csv_filename} -> {mapping['target_table']}")

        # Prepare export files - use descriptive naming for debug purposes
        # Format: target_table_from_source_table.csv
        export_filename = f"{mapping['target_table']}_from_{source_table}.csv"
        export_path = os.path.join(self.export_directory, "mapped", export_filename)
        file_exists = os.path.exists(export_path)
        validation_report_path = os.path.join(self.export_directory, "reports", f"{source_table}_validation.txt")

        exported_data = []
        validation_issues = []

        try:
            with open(csv_path, 'r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                headers_written = False

                # Open file in write mode (each source table gets its own file)
                with open(export_path, 'w', newline='', encoding='utf-8') as export_file:
                    writer = None

                    for row_num, row in enumerate(reader, 1):
                        file_stats['records_processed'] += 1

                        # Transform the row data
                        transformed_result = self._transform_row_for_export(source_table, row, row_num)

                        if transformed_result and 'data' in transformed_result:
                            # Initialize CSV writer with headers
                            if not headers_written:
                                fieldnames = list(transformed_result['data'].keys())
                                writer = csv.DictWriter(export_file, fieldnames=fieldnames)
                                writer.writeheader()
                                headers_written = True

                            # Write transformed data
                            if writer:
                                writer.writerow(transformed_result['data'])
                                file_stats['records_exported'] += 1

                            # Collect validation issues
                            if 'issues' in transformed_result:
                                validation_issues.extend(transformed_result['issues'])

                        # Process in batches for memory efficiency
                        if row_num % self.batch_size == 0:
                            pass
                            # logger.debug(f"Exported {row_num} records from {csv_filename}")

        except Exception as e:
            error_msg = f"Error processing CSV file {csv_filename}: {str(e)}"
            logger.error(error_msg)
            file_stats['success'] = False
            file_stats['transformation_issues'].append(error_msg)

        # Write validation report
        self._write_validation_report(validation_report_path, source_table, mapping, validation_issues, file_stats)

        # logger.debug(f"Export completed for {csv_filename}: {file_stats['records_exported']} records exported")
        return file_stats

    def _transform_row_for_export(self, source_table, row_data, row_num):
        """
        Transform a single row for export, capturing validation issues.

        Args:
            source_table (str): Source table name
            row_data (dict): Raw row data
            row_num (int): Row number for error reporting

        Returns:
            dict: Transformed data with validation issues
        """
        mapping = self.model_mapper.get_mapping(source_table)
        if not mapping:
            return None

        target_table = mapping['target_table']
        column_mappings = mapping['column_mappings']
        additional_columns = mapping.get('additional_columns', {})

        transformed_data = {}
        issues = []

        # Apply column mappings
        for source_col, target_col in column_mappings.items():
            value = row_data.get(source_col, "")
            # Apply basic transformations (you can enhance this)
            transformed_value = self._clean_value_for_export(value, target_col)
            transformed_data[target_col] = transformed_value

            # Validate transformation - special handling for boolean fields
            is_boolean_field = 'is_' in target_col.lower() or target_col.lower() in ['abstract', 'header']

        # Apply additional columns
        for target_col, expression in additional_columns.items():
            if callable(expression):
                value = expression(row_data)
            else:
                value = expression

            transformed_data[target_col] = self._clean_value_for_export(value, target_col)


        return {
            'data': transformed_data,
            'issues': issues
        }

    def _clean_value_for_export(self, value, field_name):
        """
        Clean value for export (simplified version of database cleaning).
        Uses caching for improved performance.

        Args:
            value: Input value
            field_name (str): Target field name

        Returns:
            Cleaned value suitable for CSV export
        """
        if value is None:
            return ""

        # Create cache key
        cache_key = (str(value), field_name)
        if cache_key in self._transformation_cache:
            return self._transformation_cache[cache_key]

        # Convert to string and clean
        str_value = str(value).strip()
        result = str_value

        # Handle boolean-like fields
        if 'is_' in field_name.lower() or field_name.lower() in ['abstract', 'header']:
            from constants import transform_boolean
            boolean_result = transform_boolean(str_value)
            result = str(boolean_result) if boolean_result is not None else ""

        # Handle date-like fields
        elif 'date' in field_name.lower() or 'valid' in field_name.lower():
            from constants import transform_date
            result = transform_date(str_value,field_name) or str_value

        # Cache the result
        self._transformation_cache[cache_key] = result
        return result

    def _write_validation_report(self, report_path, source_table, mapping, validation_issues, file_stats):
        """
        Write validation report for debugging.

        Args:
            report_path (str): Path to validation report file
            source_table (str): Source table name
            mapping (dict): Mapping configuration
            validation_issues (list): List of validation issues
            file_stats (dict): File processing statistics
        """
        with open(report_path, 'w', encoding='utf-8') as report_file:
            report_file.write(f"Validation Report for {source_table}\n")
            report_file.write(f"{'='*50}\n\n")
            report_file.write(f"Source Table: {source_table}\n")
            report_file.write(f"Target Table: {mapping['target_table']}\n")
            report_file.write(f"Records Processed: {file_stats['records_processed']}\n")
            report_file.write(f"Records Exported: {file_stats['records_exported']}\n\n")

            report_file.write("Column Mappings:\n")
            for source_col, target_col in mapping['column_mappings'].items():
                report_file.write(f"  {source_col} -> {target_col}\n")

            if mapping.get('additional_columns'):
                report_file.write("\nAdditional Columns:\n")
                for target_col, expression in mapping['additional_columns'].items():
                    if callable(expression):
                        report_file.write(f"  {target_col} -> <function>\n")
                    else:
                        report_file.write(f"  {target_col} -> {expression}\n")

            if validation_issues:
                report_file.write(f"\nValidation Issues ({len(validation_issues)}):\n")
                for issue in validation_issues:
                    report_file.write(f"  - {issue}\n")
            else:
                report_file.write("\nNo validation issues found.\n")

    def _generate_export_summary(self):
        """
        Generate overall export summary report.
        """
        summary_path = os.path.join(self.export_directory, "export_summary.txt")

        with open(summary_path, 'w', encoding='utf-8') as summary_file:
            summary_file.write("DPM Export Summary Report\n")
            summary_file.write(f"{'='*30}\n\n")
            summary_file.write(f"Total Files: {self.export_stats['total_files']}\n")
            summary_file.write(f"Successful Exports: {self.export_stats['successful_exports']}\n")
            summary_file.write(f"Failed Exports: {self.export_stats['failed_exports']}\n")
            summary_file.write(f"Total Records Processed: {self.export_stats['total_records_processed']}\n")
            summary_file.write(f"Total Records Exported: {self.export_stats['total_records_exported']}\n\n")

            if self.export_stats['errors']:
                summary_file.write("Errors:\n")
                for error in self.export_stats['errors']:
                    summary_file.write(f"  - {error}\n")

            if self.export_stats['warnings']:
                summary_file.write("\nWarnings:\n")
                for warning in self.export_stats['warnings']:
                    summary_file.write(f"  - {warning}\n")

        # logger.debug(f"Export summary written to: {summary_path}")

    def clear_cache(self):
        """Clear transformation cache to free memory."""
        self._transformation_cache.clear()

    def _validate_function_definitions(self):
        """
        Validate that all function references in mappings are properly defined.

        Returns:
            list: List of validation errors for missing functions
        """
        errors = []
        import constants

        # Get all mappings
        all_mappings = self.model_mapper.get_all_mappings()

        # Track referenced functions
        referenced_functions = set()

        for source_table, mapping in all_mappings.items():
            additional_columns = mapping.get('additional_columns', {})

            for target_col, expression in additional_columns.items():
                if callable(expression):
                    # Extract function name from lambda expression
                    func_str = str(expression)
                    if 'resolve_maintenance_agency' in func_str:
                        referenced_functions.add('resolve_maintenance_agency')
                    if 'transform_boolean' in func_str:
                        referenced_functions.add('transform_boolean')
                    if 'transform_date' in func_str:
                        referenced_functions.add('transform_date')

        # Check if functions exist in constants module
        for func_name in referenced_functions:
            if not hasattr(constants, func_name):
                errors.append(f"Function '{func_name}' referenced in mappings but not defined in constants module")

        return errors


class CSVImporter:
    """
    CSVImporter class that uses ModelMapper to import DPM CSV files
    into PyBIRD Django models with proper mapping and error handling.
    """

    def __init__(self, csv_directory="target", batch_size=1000):
        os.makedirs(csv_directory, exist_ok=True)
        DjangoSetup.configure_django()
        """
        Initialize the CSVImporter with ModelMapper integration.

        Args:
            csv_directory (str): Directory containing CSV files
            batch_size (int): Number of records to process in each batch
        """
        self.csv_directory = csv_directory
        self.batch_size = batch_size
        self.model_mapper = ModelMapper()
        self.import_stats = {
            'total_files': 0,
            'successful_imports': 0,
            'failed_imports': 0,
            'total_records_processed': 0,
            'total_records_created': 0,
            'total_records_updated': 0,
            'errors': []
        }
        self.cache = dict()
        # Enhanced caching for frequent lookups
        self._value_transformation_cache = {}
        self._model_field_cache = {}

        # Get Django models mapping for target tables
        self.django_models = self._get_django_models()

    def _get_django_models(self):
        """
        Get Django model classes for all target tables.

        Returns:
            dict: Mapping of table names to Django model classes
        """
        models_dict = {}
        target_tables = self.model_mapper.get_target_tables()

        for table_name in target_tables:
            # Convert table name to Django model name (uppercase)
            model_name = table_name.upper()

            try:
                model_class = apps.get_model('pybirdai', model_name)
            except LookupError:
                logger.warning(f"Django model not found for table: {table_name}")
                continue

            models_dict[table_name] = model_class
            # logger.debug(f"Found Django model for table: {table_name}")


        return models_dict

    def _parse_datetime(self, value):
        """
        Parse datetime string to datetime object.

        Args:
            value: String representation of datetime

        Returns:
            datetime object or None
        """
        if not value or value.lower() in ['', 'null', 'none']:
            return None

        # Handle special case for CURRENT_DATE
        if str(value).upper() == 'CURRENT_DATE':
            return datetime.now()

        datetime_formats = [
            '%Y-%m-%d %H:%M:%S',
            '%m/%d/%Y %H:%M:%S',
            '%m/%d/%y %H:%M:%S',  # Added 2-digit year format
            '%Y-%m-%d',
            '%d/%m/%Y',
            '%d-%m-%Y',
            '%Y/%m/%d',
            '%m/%d/%Y',
            '%m/%d/%y',  # Added 2-digit year format
        ]

        for fmt in datetime_formats:
            try:
                return datetime.strptime(str(value), fmt)
            except ValueError:
                continue

        # logger.warning(f"Could not parse datetime: {value}")
        return None

    def _clean_value(self, value, field_type='CharField'):
        """
        Clean and convert value based on field type.
        Uses memoization for performance improvement.

        Args:
            value: Raw value from CSV
            field_type: Django field type

        Returns:
            Cleaned value appropriate for the field type
        """
        if value is None or str(value).strip() == '' or str(value).lower() in ['null', 'none']:
            return None

        value_str = str(value).strip()

        # Create cache key
        cache_key = (value_str, field_type)
        if cache_key in self._value_transformation_cache:
            return self._value_transformation_cache[cache_key]

        result = None
        if field_type in ['BooleanField']:
            result = value_str.lower() in ['true', '1', 'yes', 'y', 't']
        elif field_type in ['BigIntegerField', 'IntegerField']:
            try:
                result = int(float(value_str))  # Handle decimal strings
            except (ValueError, TypeError):
                result = None
        elif field_type in ['FloatField', 'DecimalField']:
            try:
                result = float(value_str)
            except (ValueError, TypeError):
                result = None
        elif field_type in ['DateTimeField']:
            result = self._parse_datetime(value_str)
        else:
            # CharField, TextField, etc.
            result = value_str[:1000] if len(value_str) > 1000 else value_str

        # Cache the result
        self._value_transformation_cache[cache_key] = result
        return result

    def _get_foreign_key_object(self, model_class, field_name, value):
        """
        Get foreign key object for a given value.

        Args:
            model_class: Django model class
            field_name: Name of the foreign key field
            value: Value to look up

        Returns:
            Foreign key object or None
        """
        if not value:
            return None

        if (model_class,field_name,value) in self.cache:
            return self.cache[(model_class,field_name,value)]

        field = model_class._meta.get_field(field_name)
        if hasattr(field, 'related_model'):
            related_model = field.related_model
            fetched_object, is_created = related_model.objects.get_or_create(pk=value)
            self.cache[(model_class,field_name,value)] = fetched_object
            return fetched_object

    def _transform_row_data(self, source_table, row_data):
        """
        Transform row data using ModelMapper configuration.

        Args:
            source_table (str): Name of the source table
            row_data (dict): Raw row data from CSV

        Returns:
            dict: Transformed row data for Django model
        """
        mapping = self.model_mapper.get_mapping(source_table)
        if not mapping:
            logger.warning(f"No mapping found for source table: {source_table}")
            return None

        target_table = mapping['target_table']
        column_mappings = mapping['column_mappings']
        additional_columns = mapping.get('additional_columns', {})

        # Get the Django model class
        if target_table not in self.django_models:
            logger.warning(f"No Django model found for target table: {target_table}")
            return None

        model_class = self.django_models[target_table]
        transformed_data = {}

        # Apply column mappings
        for source_col, target_col in column_mappings.items():
            value = row_data.get(source_col,"")
            if value:
                # Get field information with caching
                try:
                    field_cache_key = (model_class, target_col.lower())
                    if field_cache_key in self._model_field_cache:
                        field, field_type = self._model_field_cache[field_cache_key]
                    else:
                        field = model_class._meta.get_field(target_col.lower())
                        field_type = field.__class__.__name__
                        self._model_field_cache[field_cache_key] = (field, field_type)

                    clean_value = self._clean_value(value, field_type)
                    fk=None
                    try:
                        fk = self._get_foreign_key_object(model_class, target_col.lower(), value)
                    except Exception as fk_error:
                        # logger.debug(f"No foreign key relationship for {target_col}: {fk_error}")
                        pass
                    transformed_data[target_col.lower()] = fk if fk else clean_value
                except Exception as e:
                    logger.error(f"Error processing column {source_col} -> {target_col} for table {source_table} -> {target_table}: {value}. Error: {type(e).__name__}: {str(e)}")
                    transformed_data[target_col.lower()] = None


        # Apply additional columns (lambda functions or static values)
        for target_col, expression in additional_columns.items():
            try:
                value = expression(row_data) if callable(expression) else expression

                # Get field information with caching
                field_cache_key = (model_class, target_col.lower())
                if field_cache_key in self._model_field_cache:
                    field, field_type = self._model_field_cache[field_cache_key]
                else:
                    field = model_class._meta.get_field(target_col.lower())
                    field_type = field.__class__.__name__
                    self._model_field_cache[field_cache_key] = (field, field_type)

                clean_value = self._clean_value(value, field_type)
                fk=None
                try:
                    fk = self._get_foreign_key_object(model_class, target_col.lower(), value)
                except Exception as fk_error:
                    # logger.debug(f"No foreign key relationship for additional column {target_col}: {fk_error}")
                    pass
                transformed_data[target_col.lower()] = fk if fk else clean_value
            except Exception as e:
                logger.error(f"Error processing additional column {target_col} for table {source_table} -> {target_table}: {value}. Error: {type(e).__name__}: {str(e)}")
                logger.error(f"Expression type: {type(expression)}, Callable: {callable(expression)}")
                if callable(expression):
                    logger.error(f"Function source: {expression}")
                transformed_data[target_col.lower()] = None

        return {
            'target_table': target_table,
            'model_class': model_class,
            'data': transformed_data
        }

    def import_csv_file(self, csv_filename):
        """
        Import a single CSV file using ModelMapper configuration.

        Args:
            csv_filename (str): Name of the CSV file (without path)

        Returns:
            dict: Import statistics for this file
        """
        # Extract table name from filename (remove .csv extension)
        source_table = csv_filename.replace('.csv', '')
        csv_path = os.path.join(self.csv_directory, csv_filename)

        if not os.path.exists(csv_path):
            logger.error(f"CSV file not found: {csv_path}")
            return {'success': False, 'error': f"File not found: {csv_path}"}

        # Check if we have mapping for this table
        mapping = self.model_mapper.get_mapping(source_table)
        if not mapping:
            # logger.debug(f"No mapping defined for table: {source_table}, skipping...")
            return {'success': True, 'skipped': True, 'reason': 'No mapping defined'}

        file_stats = {
            'filename': csv_filename,
            'source_table': source_table,
            'target_table': mapping['target_table'],
            'records_processed': 0,
            'records_created': 0,
            'records_updated': 0,
            'errors': [],
            'success': True
        }

        # logger.debug(f"Starting import of {csv_filename} -> {mapping['target_table']}")

        try:
            with open(csv_path, 'r', encoding='utf-8') as csvfile:
                # Detect delimiter
                sample = csvfile.read(1024)
                csvfile.seek(0)
                sniffer = csv.Sniffer()
                delimiter = sniffer.sniff(sample).delimiter

                reader = csv.DictReader(csvfile, delimiter=delimiter)
                batch_data = []

                with transaction.atomic():
                    for row_num, row in enumerate(reader, 1):
                        try:
                            # Transform the row data
                            transformed = self._transform_row_data(source_table, row)
                            if not transformed:
                                continue

                            model_class = transformed['model_class']
                            data = transformed['data']

                            # Filter out None values for the primary key
                            pk_field_name = model_class._meta.pk.name
                            if pk_field_name in data and data[pk_field_name] is None:
                                logger.warning(f"Skipping row {row_num}: Primary key is None")
                                continue

                            batch_data.append((model_class, data, row_num))
                            file_stats['records_processed'] += 1

                            # Process batch when it reaches batch_size
                            if len(batch_data) >= self.batch_size:
                                created, updated, errors = self._process_batch(batch_data)
                                file_stats['records_created'] += created
                                file_stats['records_updated'] += updated
                                file_stats['errors'].extend(errors)
                                batch_data = []

                        except Exception as e:
                            error_msg = f"Error processing {csv_path}  row {row_num} {row}: {str(e)}"
                            logger.error(error_msg)
                            file_stats['errors'].append(error_msg)

                    # Process remaining batch
                    if batch_data:
                        created, updated, errors = self._process_batch(batch_data)
                        file_stats['records_created'] += created
                        file_stats['records_updated'] += updated
                        file_stats['errors'].extend(errors)

        except Exception as e:
            error_msg = f"Error reading CSV file {csv_filename}: {str(e)}"
            logger.error(error_msg)
            file_stats['success'] = False
            file_stats['errors'].append(error_msg)

        # Log results
        # if file_stats['success']:
        #     # logger.debug(f"Successfully imported {csv_filename}: "
        #                f"{file_stats['records_processed']} processed, "
        #                f"{file_stats['records_created']} created, "
        #                f"{file_stats['records_updated']} updated, "
        #                f"{len(file_stats['errors'])} errors")
        else:
            logger.error(f"Failed to import {csv_filename}")

        return file_stats

    def _process_batch(self, batch_data):
        """
        Process a batch of records - create or update instances.

        Args:
            batch_data: List of (model_class, data, row_num) tuples

        Returns:
            tuple: (created_count, updated_count, errors_list)
        """
        created_count = 0
        updated_count = 0
        errors = []

        for model_class, data, row_num in batch_data:
            try:
                # Get primary key field name and value
                pk_field_name = model_class._meta.pk.name
                pk_value = data.get(pk_field_name,row_num)

                # Try to get existing instance
                try:
                    instance = model_class.objects.get(pk=pk_value)
                    # Update existing instance
                    updated = False
                    for field_name, value in data.items():
                        if hasattr(instance, field_name):
                            current_value = getattr(instance, field_name)
                            if current_value != value:
                                setattr(instance, field_name, value)
                                updated = True

                    if updated:
                        instance.full_clean()
                        instance.save()
                        updated_count += 1

                except ObjectDoesNotExist:
                    # Create new instance
                    instance = model_class(**data)
                    instance.full_clean()
                    instance.save()
                    created_count += 1

            except ValidationError as e:
                error_msg = f"Row {row_num}: Validation error - {str(e)}"
                errors.append(error_msg)
                logger.warning(error_msg)
            except Exception as e:
                error_msg = f"Row {row_num}: Unexpected error - {str(e)}"
                errors.append(error_msg)
                logger.error(error_msg)

        return created_count, updated_count, errors

    def import_all_csv_files(self):
        """
        Import all CSV files in the directory that have mappings defined.

        Returns:
            dict: Overall import statistics
        """
        if not os.path.exists(self.csv_directory):
            logger.error(f"CSV directory not found: {self.csv_directory}")
            return {'success': False, 'error': f"Directory not found: {self.csv_directory}"}

        # Get all CSV files
        csv_files = [f for f in os.listdir(self.csv_directory) if f.endswith('.csv')]

        if not csv_files:
            logger.warning(f"No CSV files found in directory: {self.csv_directory}")
            return {'success': True, 'files_processed': 0, 'message': 'No CSV files found'}

        # Sort files to process in a logical order (dependencies first)
        csv_files = self._sort_files_by_dependencies(csv_files)

        # logger.debug(f"Found {len(csv_files)} CSV files to process")

        # Initialize overall stats
        self.import_stats = {
            'total_files': len(csv_files),
            'successful_imports': 0,
            'failed_imports': 0,
            'skipped_imports': 0,
            'total_records_processed': 0,
            'total_records_created': 0,
            'total_records_updated': 0,
            'errors': [],
            'file_results': [],
            'start_time': datetime.now(),
            'end_time': None
        }

        # Process each file
        for csv_file in csv_files:
            # logger.debug(f"Processing file: {csv_file}")
            file_result = self.import_csv_file(csv_file)

            # Update overall stats
            if file_result.get('skipped'):
                self.import_stats['skipped_imports'] += 1
            elif file_result.get('success'):
                self.import_stats['successful_imports'] += 1
                self.import_stats['total_records_processed'] += file_result.get('records_processed', 0)
                self.import_stats['total_records_created'] += file_result.get('records_created', 0)
                self.import_stats['total_records_updated'] += file_result.get('records_updated', 0)
            else:
                self.import_stats['failed_imports'] += 1

            self.import_stats['errors'].extend(file_result.get('errors', []))
            self.import_stats['file_results'].append(file_result)

        self.import_stats['end_time'] = datetime.now()

        # Print summary
        self._print_summary()

        return self.import_stats

    def _sort_files_by_dependencies(self, csv_files):
        """
        Sort CSV files based on foreign key dependencies.

        Args:
            csv_files: List of CSV filenames

        Returns:
            list: Sorted list of CSV filenames
        """
        # Define dependency order based on foreign key relationships
        priority_order = PRIORITY_ORDER

        # Sort files based on priority order
        sorted_files = []
        remaining_files = csv_files.copy()

        # Add files in priority order
        for priority_file in priority_order:
            if priority_file in remaining_files:
                sorted_files.append(priority_file)
                remaining_files.remove(priority_file)

        # Add remaining files at the end
        sorted_files.extend(remaining_files)

        return sorted_files

    def _print_summary(self):
        """Print import summary statistics."""
        stats = self.import_stats
        duration = stats['end_time'] - stats['start_time'] if stats['end_time'] else None

        print("\n" + "="*60)
        print("DPM IMPORT SUMMARY")
        print("="*60)
        print(f"Total files found: {stats['total_files']}")
        print(f"Successful imports: {stats['successful_imports']}")
        print(f"Failed imports: {stats['failed_imports']}")
        print(f"Skipped imports: {stats['skipped_imports']}")
        print(f"Total records processed: {stats['total_records_processed']}")
        print(f"Total records created: {stats['total_records_created']}")
        print(f"Total records updated: {stats['total_records_updated']}")
        print(f"Total errors: {len(stats['errors'])}")

        if duration:
            print(f"Duration: {duration}")

        # print("\nFILE DETAILS:")
        # print("-" * 60)
        # for result in stats['file_results']:
        #     status = "SKIPPED" if result.get('skipped') else ("SUCCESS" if result.get('success') else "FAILED")
        #     print(f"{result['filename']:30} | {status:8} | "
        #           f"Processed: {result.get('records_processed', 0):4} | "
        #           f"Created: {result.get('records_created', 0):4} | "
        #           f"Updated: {result.get('records_updated', 0):4} | "
        #           f"Errors: {len(result.get('errors', []))}")

        if stats['errors']:
            print(f"\nERROR DETAILS (first 10):")
            print("-" * 60)
            for error in stats['errors'][:10]:
                print(f"  • {error}")
            if len(stats['errors']) > 10:
                print(f"  ... and {len(stats['errors']) - 10} more errors")

        print("\n" + "="*60)

    def list_available_csv_files(self):
        """
        List all available CSV files and their mapping status.

        Returns:
            dict: Information about available files and mappings
        """
        if not os.path.exists(self.csv_directory):
            return {'error': f"Directory not found: {self.csv_directory}"}

        csv_files = [f for f in os.listdir(self.csv_directory) if f.endswith('.csv')]
        file_info = []

        for csv_file in csv_files:
            source_table = csv_file.replace('.csv', '')
            mapping = self.model_mapper.get_mapping(source_table)

            info = {
                'filename': csv_file,
                'source_table': source_table,
                'has_mapping': mapping is not None,
                'target_table': mapping['target_table'] if mapping else None,
                'django_model_exists': mapping['target_table'] in self.django_models if mapping else False
            }
            file_info.append(info)

        return {
            'total_files': len(csv_files),
            'files_with_mapping': len([f for f in file_info if f['has_mapping']]),
            'files_with_django_model': len([f for f in file_info if f['django_model_exists']]),
            'files': file_info
        }

    def validate_mappings(self):
        """
        Validate all mappings against available CSV files and Django models.

        Returns:
            dict: Validation results
        """
        validation_results = {
            'mapping_validation': self.model_mapper.validate_mappings(),
            'file_validation': self.list_available_csv_files(),
            'missing_models': [],
            'unused_mappings': []
        }

        # Check for missing Django models
        target_tables = self.model_mapper.get_target_tables()
        for table in target_tables:
            if table not in self.django_models:
                validation_results['missing_models'].append(table)

        # Check for unused mappings (mappings without corresponding CSV files)
        csv_files = [f.replace('.csv', '') for f in os.listdir(self.csv_directory) if f.endswith('.csv')] if os.path.exists(self.csv_directory) else []
        all_mappings = self.model_mapper.get_all_mappings()

        for source_table in all_mappings.keys():
            if source_table not in csv_files:
                validation_results['unused_mappings'].append(source_table)

        return validation_results

    def clear_cache(self):
        """Clear all caches to free memory."""
        self.cache.clear()
        self._value_transformation_cache.clear()
        self._model_field_cache.clear()


def main():
    """
    Main function to run the DPM CSV import process.
    """
    # logger.debug("Starting DPM CSV Import Process")

    # Initialize importer
    importer = CSVImporter()

    # Validate mappings first
    # logger.debug("Validating mappings...")
    validation_results = importer.validate_mappings()

    if validation_results['missing_models']:
        logger.warning(f"Missing Django models: {validation_results['missing_models']}")

    if validation_results['unused_mappings']:
        pass
        # logger.debug(f"Unused mappings (no CSV files): {validation_results['unused_mappings']}")

    # List available files
    # logger.debug("Checking available CSV files...")
    file_info = importer.list_available_csv_files()

    if 'error' in file_info:
        logger.error(file_info['error'])
        return

    # logger.debug(f"Found {file_info['total_files']} CSV files, "
               # f"{file_info['files_with_mapping']} have mappings, "
               # f"{file_info['files_with_django_model']} have Django models")

    # Start import process
    # logger.debug("Starting CSV import process...")
    results = importer.import_all_csv_files()

    if results.get('success', True):
        pass
        # logger.debug("DPM CSV import completed successfully")
    else:
        logger.error("DPM CSV import failed")
        if 'error' in results:
            logger.error(f"Error: {results['error']}")


if __name__ == "__main__":
    DjangoSetup.configure_django()
    main()
