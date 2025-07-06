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


class ExternalDataProcessorConfig(AppConfig):
    """
    Django AppConfig for External Data Processing operations.
    Provides ECB website fetching and external data source integration functionality.
    """
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'pybirdai.entry_points.external_data_processor'
    verbose_name = 'External Data Processor'

    def ready(self):
        """Initialize the external data processor when Django starts."""
        logger.info("External Data Processor initialized")


def get_ecb_website_fetcher():
    """
    Get ECB website fetcher process step for external data operations.
    
    Returns:
        ECBWebsiteFetcherProcessStep: Configured ECB website fetcher
    """
    from pybirdai.process_steps.utils_integration.external_data_fetching.bird_ecb_website_fetcher import ECBWebsiteFetcherProcessStep
    return ECBWebsiteFetcherProcessStep()


def fetch_ecb_data(operation="fetch_latest", **kwargs):
    """
    Fetch data from ECB website.
    
    Args:
        operation (str): Operation type - "fetch_latest", "fetch_specific", "list_available"
        **kwargs: Additional parameters for specific operations
        
    Returns:
        dict: Fetch results with success status and details
    """
    logger.info(f"Fetching ECB data with operation: {operation}")
    
    fetcher = get_ecb_website_fetcher()
    result = fetcher.execute(operation=operation, **kwargs)
    
    if result.get('success'):
        logger.info(f"ECB data fetch completed: {result.get('message')}")
    else:
        logger.error(f"ECB data fetch failed: {result.get('error')}")
    
    return result


def fetch_ecb_report_templates(save_to_database=True, **kwargs):
    """
    Fetch report templates from ECB website.
    
    Args:
        save_to_database (bool): Whether to save fetched data to database
        **kwargs: Additional fetch parameters
        
    Returns:
        dict: Fetch results with success status and details
    """
    logger.info("Fetching ECB report templates")
    
    result = fetch_ecb_data(
        operation="fetch_report_templates",
        save_to_database=save_to_database,
        **kwargs
    )
    
    if result.get('success'):
        logger.info(f"Report templates fetched: {result.get('templates_count', 0)} templates")
    else:
        logger.error(f"Report template fetch failed: {result.get('error')}")
    
    return result


def fetch_ecb_semantic_integrations(save_to_database=True, **kwargs):
    """
    Fetch semantic integrations from ECB website.
    
    Args:
        save_to_database (bool): Whether to save fetched data to database
        **kwargs: Additional fetch parameters
        
    Returns:
        dict: Fetch results with success status and details
    """
    logger.info("Fetching ECB semantic integrations")
    
    result = fetch_ecb_data(
        operation="fetch_semantic_integrations", 
        save_to_database=save_to_database,
        **kwargs
    )
    
    if result.get('success'):
        logger.info(f"Semantic integrations fetched: {result.get('integrations_count', 0)} integrations")
    else:
        logger.error(f"Semantic integration fetch failed: {result.get('error')}")
    
    return result


def fetch_ecb_hierarchy_analysis(save_to_database=True, **kwargs):
    """
    Fetch hierarchy analysis from ECB website.
    
    Args:
        save_to_database (bool): Whether to save fetched data to database
        **kwargs: Additional fetch parameters
        
    Returns:
        dict: Fetch results with success status and details
    """
    logger.info("Fetching ECB hierarchy analysis")
    
    result = fetch_ecb_data(
        operation="fetch_hierarchy_analysis",
        save_to_database=save_to_database,
        **kwargs
    )
    
    if result.get('success'):
        logger.info(f"Hierarchy analysis fetched: {result.get('hierarchies_count', 0)} hierarchies")
    else:
        logger.error(f"Hierarchy analysis fetch failed: {result.get('error')}")
    
    return result


def get_ecb_data_status(**kwargs):
    """
    Get status of available ECB data sources.
    
    Args:
        **kwargs: Additional status parameters
        
    Returns:
        dict: Status results with availability information
    """
    logger.debug("Getting ECB data status")
    
    result = fetch_ecb_data(operation="get_status", **kwargs)
    
    return result


def download_ecb_file(url, destination_path=None, **kwargs):
    """
    Download a specific file from ECB website.
    
    Args:
        url (str): URL of the file to download
        destination_path (str): Local path to save the file
        **kwargs: Additional download parameters
        
    Returns:
        dict: Download results with success status and details
    """
    logger.info(f"Downloading ECB file from: {url}")
    
    result = fetch_ecb_data(
        operation="download_file",
        url=url,
        destination_path=destination_path,
        **kwargs
    )
    
    if result.get('success'):
        logger.info(f"File download completed: {result.get('message')}")
    else:
        logger.error(f"File download failed: {result.get('error')}")
    
    return result


def parse_ecb_data_format(data_content, format_type="xml", **kwargs):
    """
    Parse ECB data in various formats.
    
    Args:
        data_content (str): Content to parse
        format_type (str): Format type - "xml", "json", "csv"
        **kwargs: Additional parsing parameters
        
    Returns:
        dict: Parsing results with extracted data
    """
    logger.info(f"Parsing ECB data in {format_type} format")
    
    result = fetch_ecb_data(
        operation="parse_data",
        data_content=data_content,
        format_type=format_type,
        **kwargs
    )
    
    if result.get('success'):
        logger.info(f"Data parsing completed: {result.get('message')}")
    else:
        logger.error(f"Data parsing failed: {result.get('error')}")
    
    return result


def validate_ecb_data_integrity(data, validation_rules=None, **kwargs):
    """
    Validate integrity of fetched ECB data.
    
    Args:
        data: Data to validate
        validation_rules (dict): Custom validation rules
        **kwargs: Additional validation parameters
        
    Returns:
        dict: Validation results with success status and issues
    """
    logger.info("Validating ECB data integrity")
    
    result = fetch_ecb_data(
        operation="validate_data",
        data=data,
        validation_rules=validation_rules,
        **kwargs
    )
    
    if result.get('success'):
        is_valid = result.get('is_valid', False)
        logger.info(f"Data validation completed: {'Valid' if is_valid else 'Invalid'}")
    else:
        logger.error(f"Data validation failed: {result.get('error')}")
    
    return result


class ExternalDataProcessor:
    """
    Main external data processor class providing high-level interface.
    Handles ECB website integration and external data source management.
    """
    
    def __init__(self):
        """Initialize the external data processor."""
        self.ecb_fetcher = get_ecb_website_fetcher()
        logger.info("ExternalDataProcessor initialized")
    
    def process_ecb_data_workflow(self, data_types=None, save_to_database=True, validate_data=True):
        """
        Process complete ECB data fetching workflow.
        
        Args:
            data_types (list): List of data types to fetch - ["templates", "integrations", "hierarchies"]
            save_to_database (bool): Whether to save fetched data to database
            validate_data (bool): Whether to validate fetched data
            
        Returns:
            dict: Complete workflow results
        """
        if data_types is None:
            data_types = ["templates", "integrations", "hierarchies"]
        
        workflow_results = {
            'success': True,
            'steps_completed': [],
            'data_fetched': {},
            'errors': []
        }
        
        # Step 1: Check ECB data availability
        status_result = get_ecb_data_status()
        if status_result.get('success'):
            workflow_results['steps_completed'].append('status_check')
            workflow_results['ecb_status'] = status_result.get('status')
        
        # Step 2: Fetch requested data types
        for data_type in data_types:
            try:
                if data_type == "templates":
                    result = fetch_ecb_report_templates(save_to_database=save_to_database)
                elif data_type == "integrations":
                    result = fetch_ecb_semantic_integrations(save_to_database=save_to_database)
                elif data_type == "hierarchies":
                    result = fetch_ecb_hierarchy_analysis(save_to_database=save_to_database)
                else:
                    logger.warning(f"Unknown data type: {data_type}")
                    continue
                
                if result.get('success'):
                    workflow_results['steps_completed'].append(f'fetch_{data_type}')
                    workflow_results['data_fetched'][data_type] = result
                else:
                    workflow_results['errors'].append(f"Failed to fetch {data_type}: {result.get('error')}")
                    
            except Exception as e:
                workflow_results['errors'].append(f"Error fetching {data_type}: {str(e)}")
                logger.error(f"Error in ECB workflow for {data_type}: {e}")
        
        # Step 3: Validate data if requested
        if validate_data and workflow_results['data_fetched']:
            validation_results = {}
            for data_type, data_result in workflow_results['data_fetched'].items():
                if data_result.get('success') and 'data' in data_result:
                    validation = validate_ecb_data_integrity(data_result['data'])
                    validation_results[data_type] = validation
            
            if validation_results:
                workflow_results['steps_completed'].append('data_validation')
                workflow_results['validation_results'] = validation_results
        
        # Determine overall success
        if workflow_results['errors']:
            workflow_results['success'] = len(workflow_results['data_fetched']) > 0
        
        return workflow_results
    
    def sync_with_ecb_updates(self, last_sync_timestamp=None, **kwargs):
        """
        Synchronize with ECB website updates since last sync.
        
        Args:
            last_sync_timestamp (str): Timestamp of last synchronization
            **kwargs: Additional sync parameters
            
        Returns:
            dict: Synchronization results
        """
        logger.info("Synchronizing with ECB updates")
        
        sync_results = {
            'success': True,
            'updates_found': 0,
            'data_updated': {},
            'errors': []
        }
        
        try:
            # Check for updates since last sync
            updates_result = fetch_ecb_data(
                operation="check_updates",
                last_sync_timestamp=last_sync_timestamp,
                **kwargs
            )
            
            if updates_result.get('success'):
                updates = updates_result.get('updates', [])
                sync_results['updates_found'] = len(updates)
                
                # Process each update
                for update in updates:
                    update_type = update.get('type')
                    if update_type in ['templates', 'integrations', 'hierarchies']:
                        fetch_result = self.process_ecb_data_workflow(
                            data_types=[update_type],
                            save_to_database=True,
                            validate_data=True
                        )
                        
                        if fetch_result.get('success'):
                            sync_results['data_updated'][update_type] = fetch_result
                        else:
                            sync_results['errors'].extend(fetch_result.get('errors', []))
            
        except Exception as e:
            sync_results['success'] = False
            sync_results['errors'].append(f"Sync error: {str(e)}")
            logger.error(f"ECB sync error: {e}")
        
        return sync_results
    
    def get_external_data_summary(self):
        """
        Get summary of all external data sources and their status.
        
        Returns:
            dict: Summary of external data sources
        """
        summary = {
            'ecb_status': {},
            'last_updates': {},
            'data_counts': {},
            'health_status': 'unknown'
        }
        
        try:
            # Get ECB status
            status_result = get_ecb_data_status()
            if status_result.get('success'):
                summary['ecb_status'] = status_result.get('status', {})
                summary['health_status'] = 'healthy' if summary['ecb_status'] else 'unavailable'
            
            logger.info("External data summary generated")
            
        except Exception as e:
            summary['health_status'] = 'error'
            logger.error(f"Error generating external data summary: {e}")
        
        return summary


# Convenience function for backwards compatibility
def run_external_data_operations():
    """Get a configured external data processor instance."""
    return ExternalDataProcessor()


# Export main functions for easy access
__all__ = [
    'ExternalDataProcessorConfig',
    'get_ecb_website_fetcher',
    'fetch_ecb_data',
    'fetch_ecb_report_templates',
    'fetch_ecb_semantic_integrations',
    'fetch_ecb_hierarchy_analysis',
    'get_ecb_data_status',
    'download_ecb_file',
    'parse_ecb_data_format',
    'validate_ecb_data_integrity',
    'ExternalDataProcessor',
    'run_external_data_operations'
]