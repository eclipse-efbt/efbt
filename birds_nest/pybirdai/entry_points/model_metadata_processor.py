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

from django.apps import AppConfig, apps
from django.db.models.fields.related import ForeignKey
import logging

logger = logging.getLogger(__name__)


class ModelMetadataProcessorConfig(AppConfig):
    """
    Django AppConfig for Model Metadata Processing operations.
    Provides model metadata extraction and analysis functionality.
    """
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'pybirdai.entry_points.model_metadata_processor'
    verbose_name = 'Model Metadata Processor'

    def ready(self):
        """Initialize the model metadata processor when Django starts."""
        logger.info("Model Metadata Processor initialized")


def get_all_models_metadata(app_label=None, include_django_models=False):
    """
    Get metadata for all Django models.
    
    Args:
        app_label (str): Optional app label to filter models
        include_django_models (bool): Whether to include Django built-in models
        
    Returns:
        dict: Comprehensive model metadata
    """
    logger.info(f"Getting model metadata for app: {app_label or 'all apps'}")
    
    try:
        model_list = apps.get_models(include_auto_created=True)
        
        models_metadata = {
            'total_models': 0,
            'models': {},
            'app_summary': {},
            'relationship_summary': {}
        }
        
        for model in model_list:
            # Filter by app label if specified
            if app_label and model._meta.app_label != app_label:
                continue
            
            # Skip Django models if not requested
            if not include_django_models and model._meta.app_label in ['auth', 'admin', 'contenttypes', 'sessions']:
                continue
            
            model_metadata = extract_model_metadata(model)
            model_key = f"{model._meta.app_label}.{model.__name__}"
            models_metadata['models'][model_key] = model_metadata
            models_metadata['total_models'] += 1
            
            # Update app summary
            app_name = model._meta.app_label
            if app_name not in models_metadata['app_summary']:
                models_metadata['app_summary'][app_name] = {
                    'model_count': 0,
                    'field_count': 0,
                    'relationship_count': 0
                }
            
            models_metadata['app_summary'][app_name]['model_count'] += 1
            models_metadata['app_summary'][app_name]['field_count'] += len(model_metadata['fields'])
            models_metadata['app_summary'][app_name]['relationship_count'] += len(model_metadata['relationships'])
        
        logger.info(f"Extracted metadata for {models_metadata['total_models']} models")
        
        return {
            'success': True,
            'metadata': models_metadata,
            'message': f'Metadata extracted for {models_metadata["total_models"]} models'
        }
        
    except Exception as e:
        logger.error(f"Failed to get models metadata: {e}")
        return {
            'success': False,
            'error': str(e),
            'message': 'Failed to extract models metadata'
        }


def extract_model_metadata(model):
    """
    Extract detailed metadata for a single Django model.
    
    Args:
        model: Django model class
        
    Returns:
        dict: Detailed model metadata
    """
    metadata = {
        'model_name': model.__name__,
        'app_label': model._meta.app_label,
        'db_table': model._meta.db_table,
        'verbose_name': str(model._meta.verbose_name),
        'verbose_name_plural': str(model._meta.verbose_name_plural),
        'abstract': model._meta.abstract,
        'proxy': model._meta.proxy,
        'parent_models': [str(parent) for parent in model._meta.get_parent_list()],
        'fields': {},
        'relationships': {},
        'indexes': [],
        'constraints': [],
        'managers': []
    }
    
    try:
        # Extract field information
        field_list = model._meta.get_fields()
        for field in field_list:
            field_metadata = {
                'name': field.name,
                'class': field.__class__.__name__,
                'verbose_name': getattr(field, 'verbose_name', None),
                'help_text': getattr(field, 'help_text', None),
                'null': getattr(field, 'null', False),
                'blank': getattr(field, 'blank', False),
                'primary_key': getattr(field, 'primary_key', False),
                'unique': getattr(field, 'unique', False),
                'db_index': getattr(field, 'db_index', False),
                'editable': getattr(field, 'editable', True)
            }
            
            # Add database-specific attributes if available
            try:
                field_metadata['db_column'] = getattr(field, 'db_column', None)
                field_metadata['db_comment'] = getattr(field, 'db_comment', None)
            except AttributeError:
                pass
            
            # Add field-specific attributes
            if hasattr(field, 'max_length'):
                field_metadata['max_length'] = field.max_length
            if hasattr(field, 'choices') and field.choices:
                field_metadata['choices'] = list(field.choices)
            if hasattr(field, 'default'):
                field_metadata['default'] = str(field.default) if field.default is not None else None
            
            # Handle relationship fields
            if isinstance(field, ForeignKey):
                field_metadata['related_model'] = f"{field.related_model._meta.app_label}.{field.related_model.__name__}"
                field_metadata['on_delete'] = str(field.on_delete) if hasattr(field, 'on_delete') else None
                field_metadata['related_name'] = getattr(field, 'related_name', None)
                
                metadata['relationships'][field.name] = {
                    'type': 'ForeignKey',
                    'target_model': field_metadata['related_model'],
                    'on_delete': field_metadata['on_delete'],
                    'related_name': field_metadata['related_name']
                }
            elif hasattr(field, 'related_model') and field.related_model:
                field_metadata['related_model'] = f"{field.related_model._meta.app_label}.{field.related_model.__name__}"
                
                metadata['relationships'][field.name] = {
                    'type': field.__class__.__name__,
                    'target_model': field_metadata['related_model']
                }
            
            metadata['fields'][field.name] = field_metadata
        
        # Extract indexes
        for index in model._meta.indexes:
            metadata['indexes'].append({
                'name': index.name,
                'fields': index.fields,
                'condition': str(index.condition) if hasattr(index, 'condition') and index.condition else None
            })
        
        # Extract constraints
        for constraint in model._meta.constraints:
            metadata['constraints'].append({
                'name': constraint.name,
                'type': constraint.__class__.__name__
            })
        
        # Extract managers
        for manager_name, manager in model._meta.managers_map.items():
            metadata['managers'].append({
                'name': manager_name,
                'class': manager.__class__.__name__
            })
        
    except Exception as e:
        logger.error(f"Error extracting metadata for {model.__name__}: {e}")
        metadata['extraction_error'] = str(e)
    
    return metadata


def get_model_relationships(model_name=None, app_label=None):
    """
    Get relationship information between models.
    
    Args:
        model_name (str): Optional specific model name
        app_label (str): Optional app label to filter
        
    Returns:
        dict: Model relationship information
    """
    logger.info(f"Getting model relationships for {model_name or 'all models'}")
    
    try:
        relationships = {
            'foreign_keys': {},
            'one_to_one': {},
            'many_to_many': {},
            'reverse_relationships': {},
            'relationship_graph': {}
        }
        
        model_list = apps.get_models()
        
        for model in model_list:
            # Filter by app and model if specified
            if app_label and model._meta.app_label != app_label:
                continue
            if model_name and model.__name__ != model_name:
                continue
            
            model_key = f"{model._meta.app_label}.{model.__name__}"
            relationships['relationship_graph'][model_key] = {
                'outgoing': [],
                'incoming': []
            }
            
            # Analyze fields for relationships
            for field in model._meta.get_fields():
                if hasattr(field, 'related_model') and field.related_model:
                    target_model = f"{field.related_model._meta.app_label}.{field.related_model.__name__}"
                    
                    relationship_info = {
                        'source_model': model_key,
                        'target_model': target_model,
                        'field_name': field.name,
                        'field_type': field.__class__.__name__
                    }
                    
                    if isinstance(field, ForeignKey):
                        relationships['foreign_keys'][f"{model_key}.{field.name}"] = relationship_info
                        relationships['relationship_graph'][model_key]['outgoing'].append(target_model)
                    elif field.__class__.__name__ == 'OneToOneField':
                        relationships['one_to_one'][f"{model_key}.{field.name}"] = relationship_info
                        relationships['relationship_graph'][model_key]['outgoing'].append(target_model)
                    elif field.__class__.__name__ == 'ManyToManyField':
                        relationships['many_to_many'][f"{model_key}.{field.name}"] = relationship_info
                        relationships['relationship_graph'][model_key]['outgoing'].append(target_model)
                
                # Handle reverse relationships
                if hasattr(field, 'remote_field') and field.remote_field:
                    reverse_info = {
                        'source_model': model_key,
                        'field_name': field.name,
                        'reverse_field_name': getattr(field, 'related_name', None),
                        'field_type': field.__class__.__name__
                    }
                    relationships['reverse_relationships'][f"{model_key}.{field.name}"] = reverse_info
        
        return {
            'success': True,
            'relationships': relationships,
            'total_foreign_keys': len(relationships['foreign_keys']),
            'total_one_to_one': len(relationships['one_to_one']),
            'total_many_to_many': len(relationships['many_to_many']),
            'message': 'Model relationships extracted successfully'
        }
        
    except Exception as e:
        logger.error(f"Failed to get model relationships: {e}")
        return {
            'success': False,
            'error': str(e),
            'message': 'Failed to extract model relationships'
        }


def analyze_model_dependencies(model_name, app_label):
    """
    Analyze dependencies for a specific model.
    
    Args:
        model_name (str): Name of the model to analyze
        app_label (str): App label containing the model
        
    Returns:
        dict: Model dependency analysis
    """
    logger.info(f"Analyzing dependencies for {app_label}.{model_name}")
    
    try:
        model = apps.get_model(app_label, model_name)
        
        dependencies = {
            'direct_dependencies': [],
            'reverse_dependencies': [],
            'dependency_depth': 0,
            'circular_dependencies': []
        }
        
        # Find direct dependencies (models this model references)
        for field in model._meta.get_fields():
            if hasattr(field, 'related_model') and field.related_model:
                target_model = f"{field.related_model._meta.app_label}.{field.related_model.__name__}"
                if target_model not in dependencies['direct_dependencies']:
                    dependencies['direct_dependencies'].append(target_model)
        
        # Find reverse dependencies (models that reference this model)
        model_key = f"{app_label}.{model_name}"
        for other_model in apps.get_models():
            for field in other_model._meta.get_fields():
                if hasattr(field, 'related_model') and field.related_model == model:
                    source_model = f"{other_model._meta.app_label}.{other_model.__name__}"
                    if source_model not in dependencies['reverse_dependencies']:
                        dependencies['reverse_dependencies'].append(source_model)
        
        # Check for circular dependencies
        dependencies['circular_dependencies'] = _detect_circular_dependencies(model_key, dependencies['direct_dependencies'])
        
        # Calculate dependency depth
        dependencies['dependency_depth'] = _calculate_dependency_depth(model_key, set())
        
        return {
            'success': True,
            'model': model_key,
            'dependencies': dependencies,
            'message': f'Dependencies analyzed for {model_key}'
        }
        
    except Exception as e:
        logger.error(f"Failed to analyze dependencies for {app_label}.{model_name}: {e}")
        return {
            'success': False,
            'error': str(e),
            'message': 'Failed to analyze model dependencies'
        }


def _detect_circular_dependencies(model_key, direct_deps, visited=None, path=None):
    """Detect circular dependencies in model relationships."""
    if visited is None:
        visited = set()
    if path is None:
        path = []
    
    if model_key in path:
        return [path[path.index(model_key):] + [model_key]]
    
    if model_key in visited:
        return []
    
    visited.add(model_key)
    path.append(model_key)
    
    circular_deps = []
    for dep in direct_deps:
        # Get dependencies for the dependency (recursive)
        try:
            app_label, model_name = dep.split('.')
            dep_model = apps.get_model(app_label, model_name)
            dep_direct_deps = []
            for field in dep_model._meta.get_fields():
                if hasattr(field, 'related_model') and field.related_model:
                    target = f"{field.related_model._meta.app_label}.{field.related_model.__name__}"
                    dep_direct_deps.append(target)
            
            circular_deps.extend(_detect_circular_dependencies(dep, dep_direct_deps, visited.copy(), path.copy()))
        except Exception:
            continue
    
    return circular_deps


def _calculate_dependency_depth(model_key, visited):
    """Calculate the maximum dependency depth for a model."""
    if model_key in visited:
        return 0
    
    visited.add(model_key)
    
    try:
        app_label, model_name = model_key.split('.')
        model = apps.get_model(app_label, model_name)
        
        max_depth = 0
        for field in model._meta.get_fields():
            if hasattr(field, 'related_model') and field.related_model:
                target = f"{field.related_model._meta.app_label}.{field.related_model.__name__}"
                depth = 1 + _calculate_dependency_depth(target, visited.copy())
                max_depth = max(max_depth, depth)
        
        return max_depth
    except Exception:
        return 0


def export_model_metadata(output_format="json", output_path=None, app_label=None):
    """
    Export model metadata to file.
    
    Args:
        output_format (str): Export format - "json", "csv", "yaml"
        output_path (str): Path to save exported file
        app_label (str): Optional app label to filter
        
    Returns:
        dict: Export results
    """
    logger.info(f"Exporting model metadata to {output_format} format")
    
    try:
        # Get metadata
        metadata_result = get_all_models_metadata(app_label=app_label)
        if not metadata_result.get('success'):
            return metadata_result
        
        metadata = metadata_result['metadata']
        
        # Generate output path if not provided
        if not output_path:
            import datetime
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            app_suffix = f"_{app_label}" if app_label else ""
            output_path = f"model_metadata{app_suffix}_{timestamp}.{output_format}"
        
        # Export based on format
        if output_format.lower() == "json":
            import json
            with open(output_path, 'w') as f:
                json.dump(metadata, f, indent=2, default=str)
        elif output_format.lower() == "yaml":
            import yaml
            with open(output_path, 'w') as f:
                yaml.dump(metadata, f, default_flow_style=False)
        elif output_format.lower() == "csv":
            import csv
            with open(output_path, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['Model', 'App', 'Table', 'Fields', 'Relationships'])
                for model_key, model_data in metadata['models'].items():
                    writer.writerow([
                        model_data['model_name'],
                        model_data['app_label'],
                        model_data['db_table'],
                        len(model_data['fields']),
                        len(model_data['relationships'])
                    ])
        else:
            raise ValueError(f"Unsupported export format: {output_format}")
        
        logger.info(f"Model metadata exported to: {output_path}")
        
        return {
            'success': True,
            'output_path': output_path,
            'format': output_format,
            'models_exported': metadata['total_models'],
            'message': f'Metadata exported successfully to {output_path}'
        }
        
    except Exception as e:
        logger.error(f"Failed to export model metadata: {e}")
        return {
            'success': False,
            'error': str(e),
            'message': 'Failed to export model metadata'
        }


class ModelMetadataProcessor:
    """
    Main model metadata processor class providing high-level interface.
    Enhanced version of the original ModelMetaDataUtils functionality.
    """
    
    def __init__(self):
        """Initialize the model metadata processor."""
        logger.info("ModelMetadataProcessor initialized")
    
    def print_meta_data(self, app_label=None):
        """
        Print comprehensive metadata for all models.
        Enhanced version of the original print_meta_data method.
        
        Args:
            app_label (str): Optional app label to filter models
        """
        logger.info("Printing model metadata")
        
        metadata_result = get_all_models_metadata(app_label=app_label)
        if not metadata_result.get('success'):
            print(f"Error getting metadata: {metadata_result.get('error')}")
            return
        
        metadata = metadata_result['metadata']
        
        print(f"\n=== MODEL METADATA REPORT ===")
        print(f"Total Models: {metadata['total_models']}")
        print(f"Apps: {', '.join(metadata['app_summary'].keys())}")
        
        for model_key, model_data in metadata['models'].items():
            print(f"\n{model_data['app_label']} -> {model_data['model_name']}")
            print(f"  DB Table: {model_data['db_table']}")
            print(f"  Superclasses: {model_data['parent_models']}")
            
            print("  Fields:")
            for field_name, field_data in model_data['fields'].items():
                print(f"    {field_name} ({field_data['class']})")
                if field_data.get('db_column'):
                    print(f"      DB Column: {field_data['db_column']}")
                if field_data.get('db_comment'):
                    print(f"      DB Comment: {field_data['db_comment']}")
            
            if model_data['relationships']:
                print("  Relationships:")
                for rel_name, rel_data in model_data['relationships'].items():
                    print(f"    {rel_name} -> {rel_data['target_model']} ({rel_data['type']})")
    
    def print_table_meta_data(self, table_name=None):
        """
        Print metadata for specific database tables.
        Enhanced version of the original print_table_meta_data method.
        
        Args:
            table_name (str): Optional specific table name to filter
        """
        logger.info(f"Printing table metadata for: {table_name or 'all tables'}")
        
        metadata_result = get_all_models_metadata()
        if not metadata_result.get('success'):
            print(f"Error getting metadata: {metadata_result.get('error')}")
            return
        
        metadata = metadata_result['metadata']
        
        print(f"\n=== TABLE METADATA REPORT ===")
        
        for model_key, model_data in metadata['models'].items():
            if table_name and model_data['db_table'] != table_name:
                continue
            
            print(f"\nTable: {model_data['db_table']}")
            print(f"  Model: {model_data['model_name']} ({model_data['app_label']})")
            print(f"  Verbose Name: {model_data['verbose_name']}")
            
            print("  Columns:")
            for field_name, field_data in model_data['fields'].items():
                db_column = field_data.get('db_column') or field_name
                print(f"    {db_column} ({field_data['class']})")
                
                attributes = []
                if field_data.get('primary_key'):
                    attributes.append("PK")
                if field_data.get('null'):
                    attributes.append("NULL")
                if field_data.get('unique'):
                    attributes.append("UNIQUE")
                if field_data.get('db_index'):
                    attributes.append("INDEX")
                
                if attributes:
                    print(f"      Attributes: {', '.join(attributes)}")
                
                if field_data.get('max_length'):
                    print(f"      Max Length: {field_data['max_length']}")
                if field_data.get('db_comment'):
                    print(f"      Comment: {field_data['db_comment']}")


# Convenience function for backwards compatibility
def run_model_metadata_operations():
    """Get a configured model metadata processor instance."""
    return ModelMetadataProcessor()


# Export main functions for easy access
__all__ = [
    'ModelMetadataProcessorConfig',
    'get_all_models_metadata',
    'extract_model_metadata',
    'get_model_relationships',
    'analyze_model_dependencies',
    'export_model_metadata',
    'ModelMetadataProcessor',
    'run_model_metadata_operations'
]