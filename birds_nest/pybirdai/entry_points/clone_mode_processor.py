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

from django.apps import AppConfig
import logging

logger = logging.getLogger(__name__)


class CloneModeProcessorConfig(AppConfig):
    """
    Django AppConfig for Clone Mode Processing operations.
    Provides CSV data import/export and column index management functionality.
    """
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'pybirdai.entry_points.clone_mode_processor'
    verbose_name = 'Clone Mode Processor'

    def ready(self):
        """Initialize the clone mode processor when Django starts."""
        logger.info("Clone Mode Processor initialized")


def get_csv_data_importer():
    """
    Get CSV data importer process step for clone mode operations.
    
    Returns:
        CSVDataImporterProcessStep: Configured CSV data importer
    """
    from pybirdai.process_steps.utils_integration.clone_mode.csv_data_importer import CSVDataImporterProcessStep
    return CSVDataImporterProcessStep()


def get_column_index_manager():
    """
    Get column index manager process step for clone mode operations.
    
    Returns:
        ColumnIndexManagerProcessStep: Configured column index manager
    """
    from pybirdai.process_steps.utils_integration.clone_mode.column_index_manager import ColumnIndexManagerProcessStep
    return ColumnIndexManagerProcessStep()


def import_csv_data(csv_file_path, model_name=None, results_dir="import_results", **kwargs):
    """
    Import CSV data using the clone mode CSV data importer.
    
    Args:
        csv_file_path (str): Path to the CSV file to import
        model_name (str): Optional model name to import to
        results_dir (str): Directory to save import results
        **kwargs: Additional import parameters
        
    Returns:
        dict: Import results with success status and details
    """
    logger.info(f"Importing CSV data from {csv_file_path}")
    
    importer = get_csv_data_importer()
    result = importer.execute(
        operation="import_csv",
        data_source=csv_file_path,
        results_dir=results_dir,
        model_name=model_name,
        **kwargs
    )
    
    if result.get('success'):
        logger.info(f"CSV import completed successfully: {result.get('message')}")
    else:
        logger.error(f"CSV import failed: {result.get('error')}")
    
    return result


def export_csv_data(output_path, model_name, results_dir="export_results", **kwargs):
    """
    Export model data to CSV using the clone mode CSV data importer.
    
    Args:
        output_path (str): Path for the output CSV file
        model_name (str): Model to export
        results_dir (str): Directory to save export results
        **kwargs: Additional export parameters
        
    Returns:
        dict: Export results with success status and details
    """
    logger.info(f"Exporting CSV data to {output_path}")
    
    importer = get_csv_data_importer()
    result = importer.execute(
        operation="export_csv",
        output_path=output_path,
        model_name=model_name,
        results_dir=results_dir,
        **kwargs
    )
    
    if result.get('success'):
        logger.info(f"CSV export completed successfully: {result.get('message')}")
    else:
        logger.error(f"CSV export failed: {result.get('error')}")
    
    return result


def process_zip_file(zip_file_path, results_dir="zip_results", **kwargs):
    """
    Process ZIP file containing multiple CSV files.
    
    Args:
        zip_file_path (str): Path to the ZIP file
        results_dir (str): Directory to save processing results
        **kwargs: Additional processing parameters
        
    Returns:
        dict: Processing results with success status and details
    """
    logger.info(f"Processing ZIP file {zip_file_path}")
    
    importer = get_csv_data_importer()
    result = importer.execute(
        operation="process_zip",
        data_source=zip_file_path,
        results_dir=results_dir,
        **kwargs
    )
    
    if result.get('success'):
        logger.info(f"ZIP processing completed successfully: {result.get('message')}")
    else:
        logger.error(f"ZIP processing failed: {result.get('error')}")
    
    return result


def get_column_indexes(table_name):
    """
    Get column index mappings for a specific table.
    
    Args:
        table_name (str): Name of the table
        
    Returns:
        dict: Result with column index mappings
    """
    logger.debug(f"Getting column indexes for table {table_name}")
    
    manager = get_column_index_manager()
    result = manager.execute(
        operation="get_indexes",
        table_name=table_name
    )
    
    return result


def set_column_indexes(table_name, column_mappings):
    """
    Set column index mappings for a specific table.
    
    Args:
        table_name (str): Name of the table
        column_mappings (dict): Column name to index mapping
        
    Returns:
        dict: Result with success status
    """
    logger.debug(f"Setting column indexes for table {table_name}")
    
    manager = get_column_index_manager()
    result = manager.execute(
        operation="set_indexes",
        table_name=table_name,
        column_mappings=column_mappings
    )
    
    return result


def get_all_column_mappings():
    """
    Get all column mappings for all tables.
    
    Returns:
        dict: Result with all column mappings
    """
    logger.debug("Getting all column mappings")
    
    manager = get_column_index_manager()
    result = manager.execute(operation="get_all_mappings")
    
    return result


class CloneModeProcessor:
    """
    Main clone mode processor class providing high-level interface.
    Combines CSV data import/export with column index management.
    """
    
    def __init__(self):
        """Initialize the clone mode processor."""
        self.csv_importer = get_csv_data_importer()
        self.column_manager = get_column_index_manager()
        logger.info("CloneModeProcessor initialized")
    
    def import_metadata_export(self, source_path, **kwargs):
        """
        Import data from metadata export (CSV or ZIP).
        
        Args:
            source_path (str): Path to CSV file or ZIP file
            **kwargs: Additional import parameters
            
        Returns:
            dict: Import results
        """
        if source_path.lower().endswith('.zip'):
            return process_zip_file(source_path, **kwargs)
        else:
            return import_csv_data(source_path, **kwargs)
    
    def export_with_metadata(self, output_path, model_name, **kwargs):
        """
        Export model data with metadata information.
        
        Args:
            output_path (str): Path for output file
            model_name (str): Model to export
            **kwargs: Additional export parameters
            
        Returns:
            dict: Export results
        """
        return export_csv_data(output_path, model_name, **kwargs)
    
    def configure_table_mappings(self, table_configs):
        """
        Configure column mappings for multiple tables.
        
        Args:
            table_configs (dict): Table name to column mapping configuration
            
        Returns:
            dict: Configuration results
        """
        results = {
            'success': True,
            'tables_configured': 0,
            'errors': []
        }
        
        for table_name, column_mappings in table_configs.items():
            result = set_column_indexes(table_name, column_mappings)
            if result.get('success'):
                results['tables_configured'] += 1
            else:
                results['errors'].append(f"Failed to configure {table_name}: {result.get('error')}")
        
        if results['errors']:
            results['success'] = False
        
        return results


# Convenience function for backwards compatibility
def run_clone_mode_operations():
    """Get a configured clone mode processor instance."""
    return CloneModeProcessor()


# Export main functions for easy access
__all__ = [
    'CloneModeProcessorConfig',
    'get_csv_data_importer',
    'get_column_index_manager', 
    'import_csv_data',
    'export_csv_data',
    'process_zip_file',
    'get_column_indexes',
    'set_column_indexes',
    'get_all_column_mappings',
    'CloneModeProcessor',
    'run_clone_mode_operations'
]