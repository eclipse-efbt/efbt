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


class MigrationProcessorConfig(AppConfig):
    """
    Django AppConfig for Migration Processing operations.
    Provides advanced migration generation, artifact fetching, and derived fields functionality.
    """
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'pybirdai.entry_points.migration_processor'
    verbose_name = 'Migration Processor'

    def ready(self):
        """Initialize the migration processor when Django starts."""
        logger.info("Migration Processor initialized")


def get_migration_generator():
    """
    Get migration generator process step for Django migration generation.
    
    Returns:
        MigrationGeneratorProcessStep: Configured migration generator
    """
    from pybirdai.process_steps.utils_integration.speed_improvements.migration_generator import MigrationGeneratorProcessStep
    return MigrationGeneratorProcessStep()


def get_artifact_fetcher():
    """
    Get artifact fetcher process step for GitHub artifact operations.
    
    Returns:
        ArtifactFetcherProcessStep: Configured artifact fetcher
    """
    from pybirdai.process_steps.utils_integration.speed_improvements.artifact_fetcher import ArtifactFetcherProcessStep
    return ArtifactFetcherProcessStep()


def get_derived_fields_extractor():
    """
    Get derived fields extractor process step for lineage property extraction.
    
    Returns:
        DerivedFieldsExtractorProcessStep: Configured derived fields extractor
    """
    from pybirdai.process_steps.utils_integration.speed_improvements.derived_fields_extractor import DerivedFieldsExtractorProcessStep
    return DerivedFieldsExtractorProcessStep()


def generate_migration_from_file(source_file, output_file=None):
    """
    Generate Django migration from a single source file.
    
    Args:
        source_file (str): Path to the Python file containing model definitions
        output_file (str): Path for the output migration file
        
    Returns:
        dict: Generation results with success status and details
    """
    logger.info(f"Generating migration from file: {source_file}")
    
    generator = get_migration_generator()
    result = generator.execute(
        operation="generate_from_file",
        source=source_file,
        output_file=output_file or "0001_initial.py"
    )
    
    if result.get('success'):
        logger.info(f"Migration generation completed: {result.get('message')}")
    else:
        logger.error(f"Migration generation failed: {result.get('error')}")
    
    return result


def generate_migration_from_files(source_files, output_file=None):
    """
    Generate Django migration from multiple source files.
    
    Args:
        source_files (list): List of paths to Python files containing model definitions
        output_file (str): Path for the output migration file
        
    Returns:
        dict: Generation results with success status and details
    """
    logger.info(f"Generating migration from {len(source_files)} files")
    
    generator = get_migration_generator()
    result = generator.execute(
        operation="generate_from_files",
        source=source_files,
        output_file=output_file or "0001_initial.py"
    )
    
    if result.get('success'):
        logger.info(f"Migration generation completed: {result.get('message')}")
    else:
        logger.error(f"Migration generation failed: {result.get('error')}")
    
    return result


def generate_migration_from_directory(source_dir, output_file=None, pattern="*.py"):
    """
    Generate Django migration from all Python files in a directory.
    
    Args:
        source_dir (str): Path to the directory containing Python files
        output_file (str): Path for the output migration file
        pattern (str): File pattern to match
        
    Returns:
        dict: Generation results with success status and details
    """
    logger.info(f"Generating migration from directory: {source_dir}")
    
    generator = get_migration_generator()
    result = generator.execute(
        operation="generate_from_directory",
        source=source_dir,
        output_file=output_file or "0001_initial.py",
        pattern=pattern
    )
    
    if result.get('success'):
        logger.info(f"Migration generation completed: {result.get('message')}")
    else:
        logger.error(f"Migration generation failed: {result.get('error')}")
    
    return result


def parse_models_from_source(source):
    """
    Parse Django models from source without generating migration.
    
    Args:
        source: Source file path, directory path, or list of file paths
        
    Returns:
        dict: Parsing results with model information
    """
    logger.info(f"Parsing models from source: {source}")
    
    generator = get_migration_generator()
    result = generator.execute(
        operation="parse_models",
        source=source
    )
    
    if result.get('success'):
        logger.info(f"Model parsing completed: {result.get('message')}")
    else:
        logger.error(f"Model parsing failed: {result.get('error')}")
    
    return result


def fetch_database_artifact(token, bird_data_model_path=None, bird_meta_data_model_path=None):
    """
    Fetch pre-built database artifact from GitHub that matches model files.
    
    Args:
        token (str): GitHub personal access token
        bird_data_model_path (str): Path to the bird data model file
        bird_meta_data_model_path (str): Path to the bird meta data model file
        
    Returns:
        dict: Fetch results with success status and details
    """
    logger.info("Fetching database artifact from GitHub")
    
    fetcher = get_artifact_fetcher()
    result = fetcher.execute(
        operation="fetch_database",
        token=token,
        bird_data_model_path=bird_data_model_path,
        bird_meta_data_model_path=bird_meta_data_model_path
    )
    
    if result.get('success'):
        logger.info(f"Database fetch completed: {result.get('message')}")
    else:
        logger.error(f"Database fetch failed: {result.get('error')}")
    
    return result


def get_github_artifacts(token, repo_url=None):
    """
    Get all artifacts from a GitHub repository.
    
    Args:
        token (str): GitHub personal access token
        repo_url (str): GitHub API URL for artifacts
        
    Returns:
        dict: Results with artifact list
    """
    logger.info("Getting GitHub artifacts")
    
    fetcher = get_artifact_fetcher()
    result = fetcher.execute(
        operation="get_artifacts",
        token=token,
        repo_url=repo_url
    )
    
    if result.get('success'):
        logger.info(f"Artifacts retrieved: {result.get('message')}")
    else:
        logger.error(f"Artifact retrieval failed: {result.get('error')}")
    
    return result


def download_github_artifact(token, artifact_data):
    """
    Download a specific GitHub artifact.
    
    Args:
        token (str): GitHub personal access token
        artifact_data: Artifact information or data
        
    Returns:
        dict: Results with artifact content
    """
    logger.info("Downloading GitHub artifact")
    
    fetcher = get_artifact_fetcher()
    result = fetcher.execute(
        operation="download_artifact",
        token=token,
        artifact_data=artifact_data
    )
    
    if result.get('success'):
        logger.info(f"Artifact download completed: {result.get('message')}")
    else:
        logger.error(f"Artifact download failed: {result.get('error')}")
    
    return result


def extract_lineage_classes(file_path):
    """
    Extract classes with lineage properties from a Python file.
    
    Args:
        file_path (str): Path to the Python file to analyze
        
    Returns:
        dict: Results with extracted classes information
    """
    logger.info(f"Extracting lineage classes from: {file_path}")
    
    extractor = get_derived_fields_extractor()
    result = extractor.execute(
        operation="extract_lineage_classes",
        file_path=file_path
    )
    
    if result.get('success'):
        logger.info(f"Lineage extraction completed: {result.get('message')}")
    else:
        logger.error(f"Lineage extraction failed: {result.get('error')}")
    
    return result


def generate_derived_fields_ast(classes_info, output_path=None):
    """
    Generate AST representation of extracted classes.
    
    Args:
        classes_info (list): List of class information dictionaries
        output_path (str): Path for output file
        
    Returns:
        dict: Results with AST generation status
    """
    logger.info("Generating derived fields AST")
    
    extractor = get_derived_fields_extractor()
    result = extractor.execute(
        operation="generate_ast",
        classes_info=classes_info,
        output_path=output_path or 'derived_field_configuration.py'
    )
    
    if result.get('success'):
        logger.info(f"AST generation completed: {result.get('message')}")
    else:
        logger.error(f"AST generation failed: {result.get('error')}")
    
    return result


def merge_derived_fields(bird_data_model_path, lineage_classes_ast_path):
    """
    Merge derived fields into the original bird data model file.
    
    Args:
        bird_data_model_path (str): Path to the original bird_data_model.py file
        lineage_classes_ast_path (str): Path to the derived_field_configuration.py file
        
    Returns:
        dict: Results with merge status
    """
    logger.info("Merging derived fields into original model")
    
    extractor = get_derived_fields_extractor()
    result = extractor.execute(
        operation="merge_fields",
        bird_data_model_path=bird_data_model_path,
        lineage_classes_ast_path=lineage_classes_ast_path
    )
    
    if result.get('success'):
        logger.info(f"Field merge completed: {result.get('message')}")
    else:
        logger.error(f"Field merge failed: {result.get('error')}")
    
    return result


def check_file_modification_status(file_path):
    """
    Check if a file already has lineage modifications.
    
    Args:
        file_path (str): Path to the file to check
        
    Returns:
        dict: Results with modification status
    """
    logger.debug(f"Checking modification status of: {file_path}")
    
    extractor = get_derived_fields_extractor()
    result = extractor.execute(
        operation="check_modified",
        file_path=file_path
    )
    
    return result


class MigrationProcessor:
    """
    Main migration processor class providing high-level interface.
    Combines migration generation, artifact fetching, and derived fields processing.
    """
    
    def __init__(self):
        """Initialize the migration processor."""
        self.migration_generator = get_migration_generator()
        self.artifact_fetcher = get_artifact_fetcher()
        self.derived_fields_extractor = get_derived_fields_extractor()
        logger.info("MigrationProcessor initialized")
    
    def process_complete_migration_workflow(self, source, output_file=None, include_derived_fields=False):
        """
        Process complete migration workflow from source to final migration.
        
        Args:
            source: Source file, files, or directory
            output_file (str): Output migration file path
            include_derived_fields (bool): Whether to include derived fields processing
            
        Returns:
            dict: Complete workflow results
        """
        workflow_results = {
            'success': True,
            'steps_completed': [],
            'errors': []
        }
        
        # Step 1: Parse models
        parse_result = parse_models_from_source(source)
        if parse_result.get('success'):
            workflow_results['steps_completed'].append('model_parsing')
            workflow_results['models_found'] = parse_result.get('models_found', 0)
        else:
            workflow_results['success'] = False
            workflow_results['errors'].append(f"Model parsing failed: {parse_result.get('error')}")
            return workflow_results
        
        # Step 2: Generate migration
        if isinstance(source, str):
            if source.endswith('.py'):
                gen_result = generate_migration_from_file(source, output_file)
            else:
                gen_result = generate_migration_from_directory(source, output_file)
        elif isinstance(source, list):
            gen_result = generate_migration_from_files(source, output_file)
        else:
            workflow_results['success'] = False
            workflow_results['errors'].append("Invalid source type")
            return workflow_results
        
        if gen_result.get('success'):
            workflow_results['steps_completed'].append('migration_generation')
            workflow_results['output_file'] = gen_result.get('output_file')
        else:
            workflow_results['success'] = False
            workflow_results['errors'].append(f"Migration generation failed: {gen_result.get('error')}")
            return workflow_results
        
        # Step 3: Process derived fields if requested
        if include_derived_fields and isinstance(source, str) and source.endswith('.py'):
            lineage_result = extract_lineage_classes(source)
            if lineage_result.get('success') and lineage_result.get('classes_found', 0) > 0:
                workflow_results['steps_completed'].append('derived_fields_extraction')
                workflow_results['lineage_classes'] = lineage_result.get('classes_found')
            
        return workflow_results
    
    def process_speed_improvement_workflow(self, token, source_models, output_migration=None):
        """
        Process speed improvement workflow with artifact fetching.
        
        Args:
            token (str): GitHub personal access token
            source_models (dict): Dictionary with model file paths
            output_migration (str): Output migration file path
            
        Returns:
            dict: Speed improvement workflow results
        """
        workflow_results = {
            'success': True,
            'steps_completed': [],
            'database_fetched': False,
            'migration_generated': False
        }
        
        # Step 1: Try to fetch pre-built database
        if token:
            fetch_result = fetch_database_artifact(
                token,
                source_models.get('bird_data_model_path'),
                source_models.get('bird_meta_data_model_path')
            )
            
            if fetch_result.get('success') and fetch_result.get('database_found'):
                workflow_results['steps_completed'].append('database_fetch')
                workflow_results['database_fetched'] = True
                workflow_results['database_size'] = fetch_result.get('database_size', 0)
                return workflow_results  # Skip migration generation if database found
        
        # Step 2: Generate migration if no pre-built database found
        model_files = [path for path in source_models.values() if path]
        if model_files:
            gen_result = generate_migration_from_files(model_files, output_migration)
            if gen_result.get('success'):
                workflow_results['steps_completed'].append('migration_generation')
                workflow_results['migration_generated'] = True
                workflow_results['output_file'] = gen_result.get('output_file')
        
        return workflow_results


# Convenience function for backwards compatibility
def merge_derived_fields_into_model(*args, **kwargs):
    """
    Merge derived fields into model using the process step architecture.
    Backward compatibility wrapper for merge_derived_fields.
    """
    try:
        return merge_derived_fields(*args, **kwargs)
    except Exception as e:
        logger.warning(f"merge_derived_fields_into_model failed: {e}")
        return {'success': False, 'error': str(e)}


def get_preconfigured_database_fetcher():
    """
    Get preconfigured database fetcher using the process step architecture.
    Backward compatibility wrapper for get_artifact_fetcher.
    """
    try:
        return get_artifact_fetcher()
    except Exception as e:
        logger.warning(f"get_preconfigured_database_fetcher failed: {e}")
        return None


def generate_advanced_migration(*args, **kwargs):
    """
    Generate advanced migration using the process step architecture.
    Backward compatibility wrapper for generate_migration_from_files.
    """
    try:
        return generate_migration_from_files(*args, **kwargs)
    except Exception as e:
        logger.warning(f"generate_advanced_migration failed: {e}")
        return {'success': False, 'error': str(e)}


def run_migration_operations():
    """Get a configured migration processor instance."""
    return MigrationProcessor()


# Export main functions for easy access
__all__ = [
    'MigrationProcessorConfig',
    'get_migration_generator',
    'get_artifact_fetcher',
    'get_derived_fields_extractor',
    'generate_migration_from_file',
    'generate_migration_from_files',
    'generate_migration_from_directory',
    'parse_models_from_source',
    'fetch_database_artifact',
    'get_github_artifacts',
    'download_github_artifact',
    'extract_lineage_classes',
    'generate_derived_fields_ast',
    'merge_derived_fields',
    'check_file_modification_status',
    'merge_derived_fields_into_model',
    'get_preconfigured_database_fetcher',
    'generate_advanced_migration',
    'MigrationProcessor',
    'run_migration_operations'
]