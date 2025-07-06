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


class HierarchyProcessorConfig(AppConfig):
    """
    Django AppConfig for Member Hierarchy Processing operations.
    Provides hierarchy visualization, model conversion, and Django integration functionality.
    """
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'pybirdai.entry_points.hierarchy_processor'
    verbose_name = 'Hierarchy Processor'

    def ready(self):
        """Initialize the hierarchy processor when Django starts."""
        logger.info("Hierarchy Processor initialized")


def get_hierarchy_integration():
    """
    Get hierarchy integration process step for Django model operations.
    
    Returns:
        HierarchyIntegrationProcessStep: Configured hierarchy integration
    """
    from pybirdai.process_steps.utils_integration.member_hierarchy.hierarchy_integration import HierarchyIntegrationProcessStep
    return HierarchyIntegrationProcessStep()


def get_model_converter():
    """
    Get model converter process step for Django model conversion.
    
    Returns:
        ModelConverterProcessStep: Configured model converter
    """
    from pybirdai.process_steps.utils_integration.member_hierarchy.model_converter import ModelConverterProcessStep
    return ModelConverterProcessStep()


def get_visualization_converter():
    """
    Get visualization converter process step for format conversion.
    
    Returns:
        VisualizationConverterProcessStep: Configured visualization converter
    """
    from pybirdai.process_steps.utils_integration.member_hierarchy.visualization_converter import VisualizationConverterProcessStep
    return VisualizationConverterProcessStep()


def convert_hierarchy_to_visualization(hierarchy_id):
    """
    Convert Django hierarchy models to visualization format.
    
    Args:
        hierarchy_id (str): The hierarchy ID to convert
        
    Returns:
        dict: Result with visualization data
    """
    logger.info(f"Converting hierarchy {hierarchy_id} to visualization format")
    
    integration = get_hierarchy_integration()
    result = integration.execute(
        operation="convert_to_visualization",
        hierarchy_id=hierarchy_id
    )
    
    if result.get('success'):
        logger.info(f"Hierarchy conversion completed: {result.get('message')}")
    else:
        logger.error(f"Hierarchy conversion failed: {result.get('error')}")
    
    return result


def convert_visualization_to_hierarchy(hierarchy_id, visualization_data):
    """
    Convert visualization format back to Django hierarchy models.
    
    Args:
        hierarchy_id (str): The hierarchy ID
        visualization_data (dict): Visualization data with boxes and arrows
        
    Returns:
        dict: Result with conversion status
    """
    logger.info(f"Converting visualization data to hierarchy {hierarchy_id}")
    
    integration = get_hierarchy_integration()
    result = integration.execute(
        operation="convert_from_visualization",
        hierarchy_id=hierarchy_id,
        visualization_data=visualization_data
    )
    
    if result.get('success'):
        logger.info(f"Visualization conversion completed: {result.get('message')}")
    else:
        logger.error(f"Visualization conversion failed: {result.get('error')}")
    
    return result


def get_hierarchy_data(hierarchy_id):
    """
    Get complete hierarchy data including nodes and relationships.
    
    Args:
        hierarchy_id (str): The hierarchy ID
        
    Returns:
        dict: Result with hierarchy data
    """
    logger.debug(f"Getting hierarchy data for {hierarchy_id}")
    
    integration = get_hierarchy_integration()
    result = integration.execute(
        operation="get_hierarchy",
        hierarchy_id=hierarchy_id
    )
    
    return result


def convert_django_to_visualization(hierarchy_id):
    """
    Convert Django MEMBER_HIERARCHY_NODE instances to visualization format.
    
    Args:
        hierarchy_id (str): The hierarchy ID to convert
        
    Returns:
        dict: Result with visualization data
    """
    logger.info(f"Converting Django nodes to visualization for hierarchy {hierarchy_id}")
    
    converter = get_model_converter()
    result = converter.execute(
        operation="convert_to_visualization",
        hierarchy_id=hierarchy_id
    )
    
    if result.get('success'):
        logger.info(f"Django to visualization conversion completed: {result.get('message')}")
    else:
        logger.error(f"Django to visualization conversion failed: {result.get('error')}")
    
    return result


def convert_visualization_to_django(hierarchy_id, visualization_data):
    """
    Convert visualization data to Django MEMBER_HIERARCHY_NODE instances.
    
    Args:
        hierarchy_id (str): The hierarchy ID
        visualization_data (dict): Visualization data with boxes and arrows
        
    Returns:
        dict: Result with conversion status
    """
    logger.info(f"Converting visualization to Django nodes for hierarchy {hierarchy_id}")
    
    converter = get_model_converter()
    result = converter.execute(
        operation="convert_to_django",
        hierarchy_id=hierarchy_id,
        visualization_data=visualization_data
    )
    
    if result.get('success'):
        logger.info(f"Visualization to Django conversion completed: {result.get('message')}")
    else:
        logger.error(f"Visualization to Django conversion failed: {result.get('error')}")
    
    return result


def validate_hierarchy_structure(visualization_data):
    """
    Validate the hierarchy structure for logical consistency.
    
    Args:
        visualization_data (dict): Visualization data to validate
        
    Returns:
        dict: Result with validation status and issues
    """
    logger.info("Validating hierarchy structure")
    
    converter = get_model_converter()
    result = converter.execute(
        operation="validate_structure",
        visualization_data=visualization_data
    )
    
    if result.get('success'):
        is_valid = result.get('is_valid', False)
        logger.info(f"Hierarchy validation completed: {'Valid' if is_valid else 'Invalid'}")
    else:
        logger.error(f"Hierarchy validation failed: {result.get('error')}")
    
    return result


def get_hierarchy_statistics(hierarchy_id):
    """
    Get statistics about a hierarchy.
    
    Args:
        hierarchy_id (str): The hierarchy ID
        
    Returns:
        dict: Result with hierarchy statistics
    """
    logger.debug(f"Getting statistics for hierarchy {hierarchy_id}")
    
    converter = get_model_converter()
    result = converter.execute(
        operation="get_statistics",
        hierarchy_id=hierarchy_id
    )
    
    return result


def convert_pandas_to_visualization(hierarchy_id, members_df, hierarchies_df, hierarchy_nodes_df):
    """
    Convert pandas DataFrames to visualization format.
    
    Args:
        hierarchy_id (str): The hierarchy ID
        members_df: DataFrame with member data
        hierarchies_df: DataFrame with hierarchy data
        hierarchy_nodes_df: DataFrame with hierarchy node data
        
    Returns:
        dict: Result with visualization data
    """
    logger.info(f"Converting pandas data to visualization for hierarchy {hierarchy_id}")
    
    converter = get_visualization_converter()
    result = converter.execute(
        operation="pandas_to_visualization",
        hierarchy_id=hierarchy_id,
        members_df=members_df,
        hierarchies_df=hierarchies_df,
        hierarchy_nodes_df=hierarchy_nodes_df
    )
    
    if result.get('success'):
        logger.info(f"Pandas to visualization conversion completed: {result.get('message')}")
    else:
        logger.error(f"Pandas to visualization conversion failed: {result.get('error')}")
    
    return result


def convert_json_to_hierarchy_nodes(json_data):
    """
    Convert JSON visualization data to hierarchy nodes format.
    
    Args:
        json_data: JSON visualization data (file path or dict)
        
    Returns:
        dict: Result with hierarchy nodes
    """
    logger.info("Converting JSON to hierarchy nodes format")
    
    converter = get_visualization_converter()
    result = converter.execute(
        operation="json_to_nodes",
        data_source=json_data
    )
    
    if result.get('success'):
        logger.info(f"JSON to nodes conversion completed: {result.get('message')}")
    else:
        logger.error(f"JSON to nodes conversion failed: {result.get('error')}")
    
    return result


class HierarchyProcessor:
    """
    Main hierarchy processor class providing high-level interface.
    Combines hierarchy integration, model conversion, and visualization.
    """
    
    def __init__(self):
        """Initialize the hierarchy processor."""
        self.integration = get_hierarchy_integration()
        self.model_converter = get_model_converter()
        self.visualization_converter = get_visualization_converter()
        logger.info("HierarchyProcessor initialized")
    
    def process_hierarchy_visualization(self, hierarchy_id, operation="get"):
        """
        Process hierarchy visualization operations.
        
        Args:
            hierarchy_id (str): The hierarchy ID
            operation (str): Operation type - "get", "validate", "statistics"
            
        Returns:
            dict: Operation results
        """
        if operation == "get":
            return convert_hierarchy_to_visualization(hierarchy_id)
        elif operation == "validate":
            # First get the visualization data, then validate
            viz_result = convert_hierarchy_to_visualization(hierarchy_id)
            if viz_result.get('success'):
                return validate_hierarchy_structure(viz_result.get('visualization_data'))
            return viz_result
        elif operation == "statistics":
            return get_hierarchy_statistics(hierarchy_id)
        else:
            return {
                'success': False,
                'error': f"Unknown operation: {operation}",
                'message': 'Invalid operation specified'
            }
    
    def convert_between_formats(self, source_format, target_format, **kwargs):
        """
        Convert between different hierarchy formats.
        
        Args:
            source_format (str): Source format - "django", "visualization", "pandas", "json"
            target_format (str): Target format - "django", "visualization", "nodes"
            **kwargs: Additional parameters for conversion
            
        Returns:
            dict: Conversion results
        """
        if source_format == "django" and target_format == "visualization":
            hierarchy_id = kwargs.get('hierarchy_id')
            return convert_django_to_visualization(hierarchy_id)
        
        elif source_format == "visualization" and target_format == "django":
            hierarchy_id = kwargs.get('hierarchy_id')
            visualization_data = kwargs.get('visualization_data')
            return convert_visualization_to_django(hierarchy_id, visualization_data)
        
        elif source_format == "pandas" and target_format == "visualization":
            return convert_pandas_to_visualization(
                kwargs.get('hierarchy_id'),
                kwargs.get('members_df'),
                kwargs.get('hierarchies_df'),
                kwargs.get('hierarchy_nodes_df')
            )
        
        elif source_format == "json" and target_format == "nodes":
            return convert_json_to_hierarchy_nodes(kwargs.get('json_data'))
        
        else:
            return {
                'success': False,
                'error': f"Unsupported conversion: {source_format} to {target_format}",
                'message': 'Invalid format combination'
            }
    
    def validate_and_process(self, hierarchy_id, visualization_data=None):
        """
        Validate hierarchy structure and process if valid.
        
        Args:
            hierarchy_id (str): The hierarchy ID
            visualization_data (dict): Optional visualization data to validate
            
        Returns:
            dict: Validation and processing results
        """
        # If no visualization data provided, get from Django models
        if not visualization_data:
            viz_result = convert_django_to_visualization(hierarchy_id)
            if not viz_result.get('success'):
                return viz_result
            visualization_data = viz_result.get('visualization_data')
        
        # Validate the structure
        validation_result = validate_hierarchy_structure(visualization_data)
        
        if validation_result.get('success') and validation_result.get('is_valid'):
            # If valid, also get statistics
            stats_result = get_hierarchy_statistics(hierarchy_id)
            return {
                'success': True,
                'validation': validation_result,
                'statistics': stats_result.get('statistics') if stats_result.get('success') else None,
                'message': 'Hierarchy validated and processed successfully'
            }
        else:
            return {
                'success': False,
                'validation': validation_result,
                'message': 'Hierarchy validation failed'
            }


# Convenience function for backwards compatibility
def run_hierarchy_operations():
    """Get a configured hierarchy processor instance."""
    return HierarchyProcessor()


# Export main functions for easy access
__all__ = [
    'HierarchyProcessorConfig',
    'get_hierarchy_integration',
    'get_model_converter',
    'get_visualization_converter',
    'convert_hierarchy_to_visualization',
    'convert_visualization_to_hierarchy',
    'get_hierarchy_data',
    'convert_django_to_visualization',
    'convert_visualization_to_django',
    'validate_hierarchy_structure',
    'get_hierarchy_statistics',
    'convert_pandas_to_visualization',
    'convert_json_to_hierarchy_nodes',
    'HierarchyProcessor',
    'run_hierarchy_operations'
]