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

import csv
import zipfile
import io
import os
import glob
import logging
import json
from datetime import datetime
from django.db import transaction
from django.db import models
from typing import Dict, Any, Optional, List
from pybirdai import bird_meta_data_model

logger = logging.getLogger(__name__)


class CSVDataImporterProcessStep:
    """
    Process step for importing CSV data with metadata export functionality.
    Refactored from utils.clone_mode.import_from_metadata_export to follow process step patterns.
    """
    
    def __init__(self, context=None):
        """
        Initialize the CSV data importer process step.
        
        Args:
            context: The context object containing configuration settings.
        """
        self.context = context
        
    def execute(self, operation: str = "import_csv", data_source: str = None, 
                results_dir: str = "import_results", **kwargs) -> Dict[str, Any]:
        """
        Execute CSV data import operations.
        
        Args:
            operation (str): Operation type - "import_csv", "export_csv", "process_zip"
            data_source (str): Path to data source (CSV file, ZIP file, or directory)
            results_dir (str): Directory to save results
            **kwargs: Additional parameters for specific operations
            
        Returns:
            dict: Result dictionary with success status and details
        """
        try:
            importer = CSVDataImporter(results_dir)
            
            if operation == "import_csv":
                if not data_source:
                    raise ValueError("data_source is required for import_csv operation")
                
                result = importer.import_from_csv(data_source, **kwargs)
                
                return {
                    'success': True,
                    'operation': 'import_csv',
                    'result': result,
                    'data_source': data_source,
                    'message': f'CSV data imported successfully from {data_source}'
                }
            
            elif operation == "export_csv":
                output_path = kwargs.get('output_path', 'export_data.csv')
                result = importer.export_to_csv(output_path, **kwargs)
                
                return {
                    'success': True,
                    'operation': 'export_csv',
                    'result': result,
                    'output_path': output_path,
                    'message': f'CSV data exported successfully to {output_path}'
                }
            
            elif operation == "process_zip":
                if not data_source:
                    raise ValueError("data_source is required for process_zip operation")
                
                result = importer.process_zip_file(data_source, **kwargs)
                
                return {
                    'success': True,
                    'operation': 'process_zip',
                    'result': result,
                    'data_source': data_source,
                    'message': f'ZIP file processed successfully: {data_source}'
                }
            
            else:
                raise ValueError(f"Unknown operation: {operation}")
            
            if self.context:
                self.context.csv_importer = importer
                
        except Exception as e:
            logger.error(f"Failed to execute CSV data importer: {e}")
            return {
                'success': False,
                'error': str(e),
                'message': 'CSV data import/export operation failed'
            }


class CSVDataImporter:
    """
    Enhanced CSV data importer with process step integration.
    Refactored from utils.clone_mode.import_from_metadata_export.
    """
    
    def __init__(self, results_dir="import_results"):
        """Initialize the CSV data importer."""
        self.model_map = {}
        self.column_mappings = {}
        self.results_dir = results_dir
        self.id_mappings = {}  # Track ID mappings for models with auto-generated IDs
        self._build_model_map()
        self._build_column_mappings()
        self._ensure_results_directory()
        logger.info("CSVDataImporter initialized")

    def _ensure_results_directory(self):
        """Ensure the results directory exists"""
        if not os.path.exists(self.results_dir):
            os.makedirs(self.results_dir)
            logger.info(f"Created results directory: {self.results_dir}")

    def _save_results(self, results, operation_type="import"):
        """Save import results to a JSON file"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{operation_type}_results_{timestamp}.json"
        filepath = os.path.join(self.results_dir, filename)

        # Prepare results for JSON serialization (remove non-serializable objects)
        serializable_results = self._prepare_for_json(results)
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(serializable_results, f, indent=2, ensure_ascii=False)
            logger.info(f"Results saved to: {filepath}")
        except Exception as e:
            logger.error(f"Failed to save results: {e}")

    def _prepare_for_json(self, data):
        """Prepare data for JSON serialization by converting non-serializable objects"""
        if isinstance(data, dict):
            return {key: self._prepare_for_json(value) for key, value in data.items()}
        elif isinstance(data, list):
            return [self._prepare_for_json(item) for item in data]
        elif hasattr(data, '__dict__'):
            return str(data)  # Convert objects to string representation
        else:
            return data

    def _build_model_map(self):
        """Build a mapping of table names to Django model classes"""
        for name in dir(bird_meta_data_model):
            obj = getattr(bird_meta_data_model, name)
            if (isinstance(obj, type) and 
                issubclass(obj, models.Model) and 
                obj != models.Model):
                
                table_name = obj._meta.db_table
                self.model_map[table_name.upper()] = obj
                self.model_map[name.upper()] = obj
                
        logger.info(f"Built model map with {len(self.model_map)} models")

    def _build_column_mappings(self):
        """Build column mappings for each model"""
        from .column_index_manager import ColumnIndexes
        
        # Use the column indexes if available
        try:
            column_indexes = ColumnIndexes()
            self.column_mappings = column_indexes.get_all_mappings()
        except Exception as e:
            logger.warning(f"Could not load column indexes: {e}")
            self.column_mappings = {}

    def import_from_csv(self, csv_file_path: str, model_name: str = None, **kwargs) -> Dict[str, Any]:
        """
        Import data from a CSV file into Django models.
        
        Args:
            csv_file_path (str): Path to the CSV file
            model_name (str): Optional specific model name to import to
            **kwargs: Additional import parameters
            
        Returns:
            dict: Import results with statistics
        """
        logger.info(f"Starting CSV import from: {csv_file_path}")
        
        if not os.path.exists(csv_file_path):
            raise FileNotFoundError(f"CSV file not found: {csv_file_path}")
        
        # Determine model from filename or parameter
        if not model_name:
            filename = os.path.basename(csv_file_path)
            model_name = filename.replace('.csv', '').upper()
        
        model_class = self.model_map.get(model_name.upper())
        if not model_class:
            raise ValueError(f"Model not found for: {model_name}")
        
        results = {
            'file_path': csv_file_path,
            'model_name': model_name,
            'model_class': str(model_class),
            'records_processed': 0,
            'records_created': 0,
            'records_updated': 0,
            'errors': []
        }
        
        try:
            with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
                # Detect delimiter
                sample = csvfile.read(1024)
                csvfile.seek(0)
                delimiter = ','
                if ';' in sample and sample.count(';') > sample.count(','):
                    delimiter = ';'
                
                reader = csv.DictReader(csvfile, delimiter=delimiter)
                
                with transaction.atomic():
                    for row_num, row in enumerate(reader, 1):
                        try:
                            self._process_csv_row(model_class, row, results)
                            results['records_processed'] += 1
                            
                            # Log progress every 1000 records
                            if row_num % 1000 == 0:
                                logger.info(f"Processed {row_num} records")
                                
                        except Exception as e:
                            error_msg = f"Row {row_num}: {str(e)}"
                            results['errors'].append(error_msg)
                            logger.warning(error_msg)
                            
                            # Stop if too many errors
                            if len(results['errors']) > 100:
                                logger.error("Too many errors, stopping import")
                                break
            
            self._save_results(results, "import")
            logger.info(f"CSV import completed: {results['records_created']} created, {results['records_updated']} updated")
            
        except Exception as e:
            logger.error(f"CSV import failed: {e}")
            results['errors'].append(f"Import failed: {str(e)}")
            
        return results

    def _process_csv_row(self, model_class, row_data: Dict[str, str], results: Dict[str, Any]):
        """Process a single CSV row and create/update model instance"""
        # Clean and prepare data
        cleaned_data = {}
        for field in model_class._meta.fields:
            field_name = field.name
            csv_value = row_data.get(field_name, row_data.get(field_name.upper(), ''))
            
            if csv_value:
                # Convert based on field type
                try:
                    if isinstance(field, models.CharField):
                        cleaned_data[field_name] = str(csv_value).strip()
                    elif isinstance(field, models.IntegerField):
                        cleaned_data[field_name] = int(csv_value) if csv_value else None
                    elif isinstance(field, models.BooleanField):
                        cleaned_data[field_name] = csv_value.lower() in ('true', '1', 'yes', 'on')
                    elif isinstance(field, models.DateTimeField):
                        # Handle datetime parsing
                        if csv_value:
                            cleaned_data[field_name] = self._parse_datetime(csv_value)
                    else:
                        cleaned_data[field_name] = csv_value
                except (ValueError, TypeError) as e:
                    logger.warning(f"Field conversion error for {field_name}: {e}")
        
        # Create or update instance
        if cleaned_data:
            try:
                # Check if instance exists (try to find by primary key or unique fields)
                pk_field = model_class._meta.pk
                pk_value = cleaned_data.get(pk_field.name)
                
                if pk_value:
                    instance, created = model_class.objects.get_or_create(
                        **{pk_field.name: pk_value},
                        defaults=cleaned_data
                    )
                    if created:
                        results['records_created'] += 1
                    else:
                        # Update existing instance
                        for field_name, value in cleaned_data.items():
                            setattr(instance, field_name, value)
                        instance.save()
                        results['records_updated'] += 1
                else:
                    # Create new instance
                    instance = model_class.objects.create(**cleaned_data)
                    results['records_created'] += 1
                    
            except Exception as e:
                raise Exception(f"Failed to create/update {model_class.__name__}: {e}")

    def _parse_datetime(self, datetime_str: str):
        """Parse datetime string with multiple format attempts"""
        formats = [
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%d',
            '%d/%m/%Y %H:%M:%S',
            '%d/%m/%Y',
            '%Y-%m-%dT%H:%M:%S',
            '%Y-%m-%dT%H:%M:%SZ'
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(datetime_str, fmt)
            except ValueError:
                continue
        
        raise ValueError(f"Unable to parse datetime: {datetime_str}")

    def export_to_csv(self, output_path: str, model_name: str = None, **kwargs) -> Dict[str, Any]:
        """
        Export model data to CSV file.
        
        Args:
            output_path (str): Path for output CSV file
            model_name (str): Model to export
            **kwargs: Additional export parameters
            
        Returns:
            dict: Export results
        """
        if not model_name:
            raise ValueError("model_name is required for export")
        
        model_class = self.model_map.get(model_name.upper())
        if not model_class:
            raise ValueError(f"Model not found: {model_name}")
        
        logger.info(f"Exporting {model_name} to {output_path}")
        
        results = {
            'output_path': output_path,
            'model_name': model_name,
            'records_exported': 0
        }
        
        try:
            with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
                # Get field names
                field_names = [field.name for field in model_class._meta.fields]
                writer = csv.DictWriter(csvfile, fieldnames=field_names)
                writer.writeheader()
                
                # Export all instances
                for instance in model_class.objects.all():
                    row_data = {}
                    for field_name in field_names:
                        value = getattr(instance, field_name)
                        row_data[field_name] = str(value) if value is not None else ''
                    
                    writer.writerow(row_data)
                    results['records_exported'] += 1
            
            self._save_results(results, "export")
            logger.info(f"Export completed: {results['records_exported']} records")
            
        except Exception as e:
            logger.error(f"Export failed: {e}")
            raise
        
        return results

    def process_zip_file(self, zip_file_path: str, **kwargs) -> Dict[str, Any]:
        """
        Process a ZIP file containing multiple CSV files.
        
        Args:
            zip_file_path (str): Path to ZIP file
            **kwargs: Additional processing parameters
            
        Returns:
            dict: Processing results
        """
        logger.info(f"Processing ZIP file: {zip_file_path}")
        
        if not os.path.exists(zip_file_path):
            raise FileNotFoundError(f"ZIP file not found: {zip_file_path}")
        
        results = {
            'zip_file': zip_file_path,
            'files_processed': 0,
            'total_records': 0,
            'file_results': []
        }
        
        try:
            with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                csv_files = [f for f in zip_ref.namelist() if f.endswith('.csv')]
                
                for csv_file in csv_files:
                    try:
                        # Extract CSV content
                        csv_content = zip_ref.read(csv_file)
                        csv_string = csv_content.decode('utf-8')
                        
                        # Process CSV content
                        file_result = self._process_csv_content(csv_string, csv_file)
                        results['file_results'].append(file_result)
                        results['files_processed'] += 1
                        results['total_records'] += file_result.get('records_processed', 0)
                        
                    except Exception as e:
                        error_msg = f"Failed to process {csv_file}: {e}"
                        logger.error(error_msg)
                        results['file_results'].append({
                            'file_name': csv_file,
                            'error': error_msg
                        })
            
            self._save_results(results, "zip_import")
            logger.info(f"ZIP processing completed: {results['files_processed']} files, {results['total_records']} total records")
            
        except Exception as e:
            logger.error(f"ZIP processing failed: {e}")
            raise
        
        return results

    def _process_csv_content(self, csv_content: str, filename: str) -> Dict[str, Any]:
        """Process CSV content from string"""
        model_name = os.path.basename(filename).replace('.csv', '').upper()
        model_class = self.model_map.get(model_name)
        
        if not model_class:
            return {
                'file_name': filename,
                'error': f'Model not found for {model_name}'
            }
        
        results = {
            'file_name': filename,
            'model_name': model_name,
            'records_processed': 0,
            'records_created': 0,
            'errors': []
        }
        
        try:
            csv_file = io.StringIO(csv_content)
            reader = csv.DictReader(csv_file)
            
            with transaction.atomic():
                for row in reader:
                    try:
                        self._process_csv_row(model_class, row, results)
                        results['records_processed'] += 1
                    except Exception as e:
                        results['errors'].append(str(e))
                        
        except Exception as e:
            results['error'] = str(e)
        
        return results