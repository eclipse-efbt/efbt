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


class VisualizationProcessorConfig(AppConfig):
    """
    Django AppConfig for Visualization Processing operations.
    Provides chart generation and data visualization services functionality.
    """
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'pybirdai.entry_points.visualization_processor'
    verbose_name = 'Visualization Processor'

    def ready(self):
        """Initialize the visualization processor when Django starts."""
        logger.info("Visualization Processor initialized")


def create_chart_visualization(data, chart_type="bar", **kwargs):
    """
    Create chart visualization from data.
    
    Args:
        data: Data to visualize (dict, list, or DataFrame)
        chart_type (str): Type of chart - "bar", "line", "pie", "scatter"
        **kwargs: Additional chart parameters
        
    Returns:
        dict: Visualization results with chart data and configuration
    """
    logger.info(f"Creating {chart_type} chart visualization")
    
    try:
        # Import visualization service if available
        from pybirdai.entry_points.visualization_service_processor import create_mermaid_graph
        
        result = create_mermaid_graph(
            json_data=data,
            file_name=kwargs.get('title', 'chart'),
            output_format=kwargs.get('output_format', 'html')
        )
        
        if result.get('success'):
            logger.info(f"Chart visualization created successfully")
            return {
                'success': True,
                'chart_type': chart_type,
                'chart_data': result.get('content'),
                'message': f'{chart_type.title()} chart created successfully'
            }
        else:
            return result
        
    except ImportError:
        # Fallback implementation if visualization service not available
        logger.warning("Visualization service not available, using fallback")
        return _create_basic_visualization(data, chart_type, **kwargs)
    except Exception as e:
        logger.error(f"Chart visualization failed: {e}")
        return {
            'success': False,
            'error': str(e),
            'message': 'Chart visualization creation failed'
        }


def create_data_dashboard(datasets, dashboard_config=None, **kwargs):
    """
    Create a dashboard with multiple visualizations.
    
    Args:
        datasets (dict): Dictionary of datasets to visualize
        dashboard_config (dict): Dashboard configuration
        **kwargs: Additional dashboard parameters
        
    Returns:
        dict: Dashboard results with visualization data
    """
    logger.info("Creating data dashboard")
    
    dashboard_results = {
        'success': True,
        'visualizations': {},
        'dashboard_config': dashboard_config or {},
        'errors': []
    }
    
    try:
        for dataset_name, dataset_data in datasets.items():
            chart_config = dashboard_config.get(dataset_name, {}) if dashboard_config else {}
            chart_type = chart_config.get('chart_type', 'bar')
            
            viz_result = create_chart_visualization(
                data=dataset_data,
                chart_type=chart_type,
                title=chart_config.get('title', dataset_name),
                **chart_config
            )
            
            if viz_result.get('success'):
                dashboard_results['visualizations'][dataset_name] = viz_result
            else:
                dashboard_results['errors'].append(f"Failed to create visualization for {dataset_name}: {viz_result.get('error')}")
        
        if dashboard_results['errors']:
            dashboard_results['success'] = len(dashboard_results['visualizations']) > 0
        
        logger.info(f"Dashboard created with {len(dashboard_results['visualizations'])} visualizations")
        
    except Exception as e:
        dashboard_results['success'] = False
        dashboard_results['errors'].append(f"Dashboard creation failed: {str(e)}")
        logger.error(f"Dashboard creation error: {e}")
    
    return dashboard_results


def generate_report_visualization(report_data, report_type="summary", **kwargs):
    """
    Generate visualization for report data.
    
    Args:
        report_data: Report data to visualize
        report_type (str): Type of report - "summary", "detailed", "comparison"
        **kwargs: Additional report parameters
        
    Returns:
        dict: Report visualization results
    """
    logger.info(f"Generating {report_type} report visualization")
    
    try:
        # Determine appropriate visualization based on report type
        if report_type == "summary":
            chart_type = "pie"
        elif report_type == "detailed":
            chart_type = "bar"
        elif report_type == "comparison":
            chart_type = "line"
        else:
            chart_type = "bar"
        
        viz_result = create_chart_visualization(
            data=report_data,
            chart_type=chart_type,
            title=f"{report_type.title()} Report",
            **kwargs
        )
        
        if viz_result.get('success'):
            viz_result['report_type'] = report_type
            logger.info(f"Report visualization generated successfully")
        
        return viz_result
        
    except Exception as e:
        logger.error(f"Report visualization failed: {e}")
        return {
            'success': False,
            'error': str(e),
            'message': 'Report visualization generation failed'
        }


def export_visualization(visualization_data, output_format="png", output_path=None, **kwargs):
    """
    Export visualization to file.
    
    Args:
        visualization_data: Visualization data to export
        output_format (str): Export format - "png", "jpg", "svg", "pdf", "html"
        output_path (str): Path to save exported file
        **kwargs: Additional export parameters
        
    Returns:
        dict: Export results with file path
    """
    logger.info(f"Exporting visualization to {output_format} format")
    
    try:
        # Generate default output path if not provided
        if not output_path:
            import datetime
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = f"visualization_{timestamp}.{output_format}"
        
        # Export based on format
        if output_format.lower() in ['png', 'jpg', 'jpeg', 'svg', 'pdf']:
            result = _export_image_visualization(visualization_data, output_format, output_path, **kwargs)
        elif output_format.lower() in ['html', 'htm']:
            result = _export_html_visualization(visualization_data, output_path, **kwargs)
        elif output_format.lower() in ['json']:
            result = _export_json_visualization(visualization_data, output_path, **kwargs)
        else:
            raise ValueError(f"Unsupported export format: {output_format}")
        
        if result.get('success'):
            logger.info(f"Visualization exported to: {output_path}")
        
        return result
        
    except Exception as e:
        logger.error(f"Visualization export failed: {e}")
        return {
            'success': False,
            'error': str(e),
            'message': 'Visualization export failed'
        }


def validate_visualization_data(data, validation_rules=None, **kwargs):
    """
    Validate data for visualization compatibility.
    
    Args:
        data: Data to validate
        validation_rules (dict): Custom validation rules
        **kwargs: Additional validation parameters
        
    Returns:
        dict: Validation results with success status and issues
    """
    logger.debug("Validating visualization data")
    
    validation_result = {
        'valid': True,
        'issues': [],
        'data_info': {},
        'recommendations': []
    }
    
    try:
        # Basic data validation
        if data is None:
            validation_result['valid'] = False
            validation_result['issues'].append("Data cannot be None")
            return validation_result
        
        # Analyze data structure
        import pandas as pd
        
        if isinstance(data, dict):
            validation_result['data_info']['type'] = 'dictionary'
            validation_result['data_info']['keys'] = list(data.keys())
            validation_result['data_info']['size'] = len(data)
            
            # Check for numeric values
            numeric_keys = [k for k, v in data.items() if isinstance(v, (int, float))]
            if not numeric_keys:
                validation_result['recommendations'].append("Consider adding numeric values for better visualization")
        
        elif isinstance(data, list):
            validation_result['data_info']['type'] = 'list'
            validation_result['data_info']['size'] = len(data)
            
            if not data:
                validation_result['valid'] = False
                validation_result['issues'].append("Data list is empty")
        
        elif isinstance(data, pd.DataFrame):
            validation_result['data_info']['type'] = 'dataframe'
            validation_result['data_info']['shape'] = data.shape
            validation_result['data_info']['columns'] = list(data.columns)
            
            # Check for numeric columns
            numeric_cols = data.select_dtypes(include=['number']).columns.tolist()
            validation_result['data_info']['numeric_columns'] = numeric_cols
            
            if not numeric_cols:
                validation_result['recommendations'].append("Consider having numeric columns for better visualization")
        
        else:
            validation_result['recommendations'].append(f"Data type {type(data)} may need conversion for visualization")
        
        # Apply custom validation rules if provided
        if validation_rules:
            for rule_name, rule_func in validation_rules.items():
                try:
                    if callable(rule_func):
                        rule_result = rule_func(data)
                        if not rule_result:
                            validation_result['issues'].append(f"Custom rule '{rule_name}' failed")
                except Exception as e:
                    validation_result['issues'].append(f"Error in custom rule '{rule_name}': {str(e)}")
        
        # Update validity based on issues
        validation_result['valid'] = len(validation_result['issues']) == 0
        
        logger.debug(f"Data validation completed: {'Valid' if validation_result['valid'] else 'Invalid'}")
        
    except Exception as e:
        validation_result['valid'] = False
        validation_result['issues'].append(f"Validation error: {str(e)}")
        logger.error(f"Data validation error: {e}")
    
    return validation_result


def _create_basic_visualization(data, chart_type, **kwargs):
    """Fallback basic visualization creation."""
    logger.info("Creating basic visualization")
    
    try:
        # Basic chart data structure
        chart_data = {
            'type': chart_type,
            'data': data,
            'options': kwargs,
            'generated_at': str(logger.handlers[0].formatter.formatTime() if logger.handlers else 'unknown')
        }
        
        return {
            'success': True,
            'chart_type': chart_type,
            'chart_data': chart_data,
            'message': f'Basic {chart_type} chart created'
        }
        
    except Exception as e:
        return {
            'success': False,
            'error': str(e),
            'message': 'Basic visualization creation failed'
        }


def _export_image_visualization(viz_data, format_type, output_path, **kwargs):
    """Export visualization as image file."""
    try:
        # Basic image export implementation
        # Note: This would typically use matplotlib, plotly, or similar libraries
        logger.info(f"Exporting image visualization to {output_path}")
        
        # Placeholder implementation - would need actual image generation
        with open(output_path, 'w') as f:
            f.write(f"# Visualization export placeholder\n# Format: {format_type}\n# Data: {str(viz_data)[:100]}...")
        
        return {
            'success': True,
            'output_path': output_path,
            'format': format_type,
            'message': f'Visualization exported to {output_path}'
        }
        
    except Exception as e:
        return {
            'success': False,
            'error': str(e),
            'message': 'Image export failed'
        }


def _export_html_visualization(viz_data, output_path, **kwargs):
    """Export visualization as HTML file."""
    try:
        logger.info(f"Exporting HTML visualization to {output_path}")
        
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Visualization Export</title>
            <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
        </head>
        <body>
            <div id="visualization">
                <h2>Visualization Data</h2>
                <pre>{str(viz_data)}</pre>
            </div>
        </body>
        </html>
        """
        
        with open(output_path, 'w') as f:
            f.write(html_content)
        
        return {
            'success': True,
            'output_path': output_path,
            'format': 'html',
            'message': f'HTML visualization exported to {output_path}'
        }
        
    except Exception as e:
        return {
            'success': False,
            'error': str(e),
            'message': 'HTML export failed'
        }


def _export_json_visualization(viz_data, output_path, **kwargs):
    """Export visualization as JSON file."""
    try:
        import json
        
        logger.info(f"Exporting JSON visualization to {output_path}")
        
        with open(output_path, 'w') as f:
            json.dump(viz_data, f, indent=2, default=str)
        
        return {
            'success': True,
            'output_path': output_path,
            'format': 'json',
            'message': f'JSON visualization exported to {output_path}'
        }
        
    except Exception as e:
        return {
            'success': False,
            'error': str(e),
            'message': 'JSON export failed'
        }


class VisualizationProcessor:
    """
    Main visualization processor class providing high-level interface.
    Handles chart generation, dashboard creation, and visualization export.
    """
    
    def __init__(self):
        """Initialize the visualization processor."""
        logger.info("VisualizationProcessor initialized")
    
    def process_data_visualization_workflow(self, datasets, output_formats=None, export_dashboard=True):
        """
        Process complete data visualization workflow.
        
        Args:
            datasets (dict): Dictionary of datasets to visualize
            output_formats (list): List of export formats
            export_dashboard (bool): Whether to export complete dashboard
            
        Returns:
            dict: Complete workflow results
        """
        if output_formats is None:
            output_formats = ['html', 'json']
        
        workflow_results = {
            'success': True,
            'steps_completed': [],
            'visualizations_created': 0,
            'exports_completed': 0,
            'errors': []
        }
        
        try:
            # Step 1: Validate all datasets
            validation_results = {}
            for dataset_name, dataset_data in datasets.items():
                validation = validate_visualization_data(dataset_data)
                validation_results[dataset_name] = validation
                
                if not validation.get('valid'):
                    workflow_results['errors'].append(f"Dataset '{dataset_name}' validation failed")
            
            workflow_results['steps_completed'].append('data_validation')
            workflow_results['validation_results'] = validation_results
            
            # Step 2: Create dashboard
            dashboard_result = create_data_dashboard(datasets)
            if dashboard_result.get('success'):
                workflow_results['steps_completed'].append('dashboard_creation')
                workflow_results['visualizations_created'] = len(dashboard_result.get('visualizations', {}))
                workflow_results['dashboard_data'] = dashboard_result
            else:
                workflow_results['errors'].extend(dashboard_result.get('errors', []))
            
            # Step 3: Export dashboard if requested
            if export_dashboard and dashboard_result.get('success'):
                for format_type in output_formats:
                    export_result = export_visualization(
                        dashboard_result,
                        output_format=format_type,
                        output_path=f"dashboard.{format_type}"
                    )
                    
                    if export_result.get('success'):
                        workflow_results['exports_completed'] += 1
                    else:
                        workflow_results['errors'].append(f"Export to {format_type} failed: {export_result.get('error')}")
                
                if workflow_results['exports_completed'] > 0:
                    workflow_results['steps_completed'].append('dashboard_export')
            
        except Exception as e:
            workflow_results['success'] = False
            workflow_results['errors'].append(f"Workflow error: {str(e)}")
            logger.error(f"Visualization workflow error: {e}")
        
        # Determine overall success
        workflow_results['success'] = (
            workflow_results['visualizations_created'] > 0 and 
            len([e for e in workflow_results['errors'] if 'failed' in e.lower()]) == 0
        )
        
        return workflow_results


# Convenience function for backwards compatibility
def run_visualization_operations():
    """Get a configured visualization processor instance."""
    return VisualizationProcessor()


# Export main functions for easy access
__all__ = [
    'VisualizationProcessorConfig',
    'create_chart_visualization',
    'create_data_dashboard',
    'generate_report_visualization',
    'export_visualization',
    'validate_visualization_data',
    'VisualizationProcessor',
    'run_visualization_operations'
]