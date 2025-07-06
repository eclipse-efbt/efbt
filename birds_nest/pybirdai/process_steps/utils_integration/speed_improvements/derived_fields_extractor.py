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

import ast
import os
import logging
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)


class DerivedFieldsExtractorProcessStep:
    """
    Process step for extracting and merging derived fields with lineage properties.
    Refactored from utils.speed_improvements_initial_migration.derived_fields_extractor to follow process step patterns.
    """
    
    def __init__(self, context=None):
        """
        Initialize the derived fields extractor process step.
        
        Args:
            context: The context object containing configuration settings.
        """
        self.context = context
        
    def execute(self, operation: str = "extract_lineage_classes", 
                file_path: str = None, **kwargs) -> Dict[str, Any]:
        """
        Execute derived fields extraction operations.
        
        Args:
            operation (str): Operation type - "extract_lineage_classes", "generate_ast", "merge_fields", "check_modified"
            file_path (str): Path to the Python file to process
            **kwargs: Additional parameters for specific operations
            
        Returns:
            dict: Result dictionary with success status and details
        """
        try:
            extractor = DerivedFieldsExtractor()
            
            if operation == "extract_lineage_classes":
                if not file_path:
                    raise ValueError("file_path is required for extract_lineage_classes operation")
                
                classes = extractor.extract_classes_with_lineage_properties(file_path)
                
                return {
                    'success': True,
                    'operation': 'extract_lineage_classes',
                    'file_path': file_path,
                    'classes_found': len(classes),
                    'classes': [{'name': c['class_name'], 'properties': c['property_names']} for c in classes],
                    'message': f'Extracted {len(classes)} classes with lineage properties'
                }
            
            elif operation == "generate_ast":
                classes_info = kwargs.get('classes_info')
                if not classes_info:
                    raise ValueError("classes_info is required for generate_ast operation")
                
                ast_module = extractor.generate_ast_output(classes_info)
                output_path = kwargs.get('output_path', 'derived_field_configuration.py')
                
                # Write to file
                with open(output_path, 'w') as f:
                    f.write(ast.unparse(ast_module))
                
                return {
                    'success': True,
                    'operation': 'generate_ast',
                    'output_path': output_path,
                    'classes_processed': len(classes_info),
                    'message': f'Generated AST output for {len(classes_info)} classes'
                }
            
            elif operation == "merge_fields":
                bird_data_model_path = kwargs.get('bird_data_model_path')
                lineage_classes_ast_path = kwargs.get('lineage_classes_ast_path')
                
                if not bird_data_model_path or not lineage_classes_ast_path:
                    raise ValueError("bird_data_model_path and lineage_classes_ast_path are required for merge_fields operation")
                
                result = extractor.merge_derived_fields_into_original_model(
                    bird_data_model_path, lineage_classes_ast_path
                )
                
                return {
                    'success': True,
                    'operation': 'merge_fields',
                    'bird_data_model_path': bird_data_model_path,
                    'lineage_classes_ast_path': lineage_classes_ast_path,
                    'modifications_made': result,
                    'message': 'Merged derived fields successfully' if result else 'File already modified, no changes made'
                }
            
            elif operation == "check_modified":
                if not file_path:
                    raise ValueError("file_path is required for check_modified operation")
                
                is_modified = extractor.check_if_file_already_modified(file_path)
                
                return {
                    'success': True,
                    'operation': 'check_modified',
                    'file_path': file_path,
                    'is_modified': is_modified,
                    'message': f'File {"has" if is_modified else "does not have"} lineage modifications'
                }
            
            else:
                raise ValueError(f"Unknown operation: {operation}")
            
            if self.context:
                self.context.derived_fields_extractor = extractor
                
        except Exception as e:
            logger.error(f"Failed to execute derived fields extractor: {e}")
            return {
                'success': False,
                'error': str(e),
                'message': 'Derived fields extractor operation failed'
            }


class DerivedFieldsExtractor:
    """
    Enhanced derived fields extractor with process step integration.
    Extracts classes with lineage properties and merges them into model files.
    """
    
    def __init__(self):
        """Initialize the derived fields extractor."""
        logger.info("DerivedFieldsExtractor initialized")

    def extract_classes_with_lineage_properties(self, path: str) -> List[Dict[str, Any]]:
        """
        Extract classes that have properties with 'lineage' decorator.
        
        Args:
            path (str): Path to the Python file to analyze
            
        Returns:
            list: List of class information dictionaries
        """
        logger.info(f"Extracting classes with lineage properties from: {path}")
        
        classes_with_lineage = []

        try:
            # Parse the file using AST
            with open(path, "r") as f:
                source_code = f.read()

            tree = ast.parse(source_code)

            # Walk through the AST to find classes with lineage decorators
            for node in ast.walk(tree):
                if isinstance(node, ast.ClassDef):
                    class_name = node.name
                    lineage_properties = []

                    # Check each method/property in the class
                    for item in node.body:
                        if isinstance(item, ast.FunctionDef):
                            # Check if it has decorators
                            for decorator in item.decorator_list:
                                # Check for lineage decorator
                                if self._is_lineage_decorator(decorator):
                                    lineage_properties.append(item.name)
                                    break

                    if lineage_properties:
                        class_info = {
                            "class_name": class_name,
                            "property_names": lineage_properties,
                            "class_node": node,
                        }
                        classes_with_lineage.append(class_info)

            logger.info(f"Found {len(classes_with_lineage)} classes with lineage properties")
            return classes_with_lineage
            
        except Exception as e:
            logger.error(f"Failed to extract classes from {path}: {e}")
            raise

    def _is_lineage_decorator(self, decorator: ast.AST) -> bool:
        """Check if a decorator is a lineage decorator."""
        return (
            (isinstance(decorator, ast.Name) and decorator.id == "lineage") or
            (isinstance(decorator, ast.Attribute) and decorator.attr == "lineage") or
            (isinstance(decorator, ast.Call) and (
                (isinstance(decorator.func, ast.Name) and decorator.func.id == "lineage") or
                (isinstance(decorator.func, ast.Attribute) and decorator.func.attr == "lineage")
            ))
        )

    def generate_ast_output(self, classes_info: List[Dict[str, Any]]) -> ast.Module:
        """
        Generate AST representation of the extracted classes.
        
        Args:
            classes_info (list): List of class information dictionaries
            
        Returns:
            ast.Module: AST module containing the extracted classes
        """
        logger.info(f"Generating AST output for {len(classes_info)} classes")

        # Create module node
        module = ast.Module(body=[], type_ignores=[])
        
        # Add imports
        import_sttmt_1 = ast.ImportFrom(
            module="django.db", names=[ast.alias(name="models")], level=0
        )
        import_sttmt_2 = ast.ImportFrom(
            module="pybirdai.annotations.decorators",
            names=[ast.alias(name="lineage")],
            level=0,
        )

        module.body.append(import_sttmt_1)
        module.body.append(import_sttmt_2)

        for class_info in classes_info:
            class_node = class_info["class_node"]
            class_name = class_info["class_name"]

            # Create class definition
            class_def = ast.ClassDef(
                name=class_name,
                bases=class_node.bases,
                keywords=class_node.keywords,
                decorator_list=class_node.decorator_list,
                body=[],
            )

            # Add class body items
            for item in class_node.body:
                if isinstance(item, ast.FunctionDef):
                    class_def.body.append(item)
                elif isinstance(item, ast.ClassDef) and item.name == "Meta":
                    # Add Meta class
                    meta_class = ast.ClassDef(
                        name="Meta",
                        bases=item.bases,
                        keywords=item.keywords,
                        decorator_list=item.decorator_list,
                        body=[ast.Pass()],
                    )
                    class_def.body.append(meta_class)

            if not class_def.body:
                class_def.body.append(ast.Pass())

            module.body.append(class_def)

        logger.info(f"Generated AST module with {len(classes_info)} classes")
        return module

    def check_if_file_already_modified(self, file_path: str) -> bool:
        """
        Check if the file already has lineage imports and decorators.
        
        Args:
            file_path (str): Path to the file to check
            
        Returns:
            bool: True if file already has lineage modifications
        """
        logger.debug(f"Checking if file is already modified: {file_path}")
        
        try:
            with open(file_path, "r") as f:
                content = f.read()

            # Check for lineage import and @lineage decorator
            has_lineage_import = (
                "from pybirdai.annotations.decorators import lineage" in content
            )
            has_lineage_decorator = "@lineage" in content

            is_modified = has_lineage_import and has_lineage_decorator
            logger.debug(f"File modification check result: {is_modified}")
            return is_modified
            
        except Exception as e:
            logger.error(f"Failed to check file modification status: {e}")
            return False

    def merge_derived_fields_into_original_model(
        self, bird_data_model_path: str, lineage_classes_ast_path: str
    ) -> bool:
        """
        Merge derived fields from derived_field_configuration.py into the original bird_data_model.py.

        This function:
        1. Checks if the original file has already been modified (has @lineage imports/decorators)
        2. If not modified, processes each class to:
           - Remove existing fields that are overwritten by derived properties
           - Add derived properties before the Meta class and after all other fields
        3. Saves the modified file back to the same location

        Args:
            bird_data_model_path (str): Path to the original bird_data_model.py file
            lineage_classes_ast_path (str): Path to the derived_field_configuration.py file with derived fields

        Returns:
            bool: True if modifications were made, False if file was already modified
        """
        logger.info(f"Merging derived fields from {lineage_classes_ast_path} into {bird_data_model_path}")

        # Check if file has already been modified
        if self.check_if_file_already_modified(bird_data_model_path):
            logger.info(
                "File already contains @lineage decorators and imports, skipping modification"
            )
            return False

        try:
            # Parse both files
            with open(bird_data_model_path, "r") as f:
                original_content = f.read()

            with open(lineage_classes_ast_path, "r") as f:
                lineage_content = f.read()

            original_tree = ast.parse(original_content)
            lineage_tree = ast.parse(lineage_content)

            # Extract derived classes and their properties from lineage file
            derived_classes = {}
            for node in ast.walk(lineage_tree):
                if isinstance(node, ast.ClassDef):
                    class_name = node.name
                    derived_properties = []
                    # Find all properties with @lineage decorator
                    for item in node.body:
                        if isinstance(item, ast.FunctionDef):
                            for decorator in item.decorator_list:
                                if self._is_lineage_decorator(decorator):
                                    derived_properties.append(item)
                                    break
                    if derived_properties:
                        derived_classes[class_name] = derived_properties

            # Add lineage import to original file
            lineage_import = ast.ImportFrom(
                module="pybirdai.annotations.decorators",
                names=[ast.alias(name="lineage")],
                level=0,
            )
            original_tree.body.insert(0, lineage_import)

            # Process each class in the original file
            for node in ast.walk(original_tree):
                if isinstance(node, ast.ClassDef) and node.name in derived_classes:
                    class_name = node.name
                    logger.info(f"Processing class {class_name}")

                    # Get the derived properties to know which fields to remove
                    derived_properties = derived_classes[class_name]
                    derived_property_names = [prop.name for prop in derived_properties]

                    # Remove existing fields that are overwritten by derived properties
                    new_body = []
                    meta_class = None

                    for item in node.body:
                        if isinstance(item, ast.ClassDef) and item.name == "Meta":
                            # Store Meta class to add at the end
                            meta_class = item
                            continue
                        elif isinstance(item, ast.Assign):
                            # Check if this is a field assignment that should be removed
                            if len(item.targets) == 1 and isinstance(item.targets[0], ast.Name):
                                field_name = item.targets[0].id
                                if field_name in derived_property_names:
                                    # Skip this field as it will be replaced by a derived property
                                    continue
                        new_body.append(item)

                    # Add the derived properties before the Meta class
                    if class_name in derived_classes:
                        for derived_property in derived_classes[class_name]:
                            new_body.append(derived_property)

                    # Add Meta class at the end if it exists
                    if meta_class:
                        new_body.append(meta_class)

                    node.body = new_body

            # Write the modified content back to the original file
            modified_content = ast.unparse(original_tree)
            with open(bird_data_model_path, "w") as f:
                f.write(modified_content)

            logger.info(f"Successfully modified {bird_data_model_path} with derived fields")
            return True
            
        except Exception as e:
            logger.error(f"Failed to merge derived fields: {e}")
            raise

    def process_complete_workflow(
        self, 
        source_file: str, 
        output_dir: str = "results/derivation_files/",
        bird_data_model_path: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Execute the complete workflow for extracting and merging derived fields.
        
        Args:
            source_file (str): Path to the source file with lineage properties
            output_dir (str): Directory to save intermediate files
            bird_data_model_path (str): Path to the bird data model file to modify
            
        Returns:
            dict: Workflow results
        """
        logger.info("Starting complete derived fields workflow")
        
        results = {
            'extraction_successful': False,
            'ast_generation_successful': False,
            'merge_successful': False,
            'classes_extracted': 0,
            'files_modified': False
        }
        
        try:
            # Step 1: Extract classes with lineage properties
            lineage_classes = self.extract_classes_with_lineage_properties(source_file)
            results['extraction_successful'] = True
            results['classes_extracted'] = len(lineage_classes)
            
            if not lineage_classes:
                logger.warning("No classes with lineage properties found")
                return results
            
            # Step 2: Generate AST output
            ast_module = self.generate_ast_output(lineage_classes)
            
            # Ensure output directory exists
            os.makedirs(output_dir, exist_ok=True)
            derived_config_path = os.path.join(output_dir, 'derived_field_configuration.py')
            
            with open(derived_config_path, 'w') as f:
                f.write(ast.unparse(ast_module))
            
            results['ast_generation_successful'] = True
            results['derived_config_path'] = derived_config_path
            
            # Step 3: Merge fields if bird_data_model_path is provided
            if bird_data_model_path:
                modifications_made = self.merge_derived_fields_into_original_model(
                    bird_data_model_path, derived_config_path
                )
                results['merge_successful'] = True
                results['files_modified'] = modifications_made
            
            logger.info("Complete derived fields workflow finished successfully")
            return results
            
        except Exception as e:
            logger.error(f"Workflow failed: {e}")
            results['error'] = str(e)
            return results

    def validate_lineage_syntax(self, file_path: str) -> Dict[str, Any]:
        """
        Validate the syntax of lineage decorators in a file.
        
        Args:
            file_path (str): Path to the file to validate
            
        Returns:
            dict: Validation results
        """
        logger.info(f"Validating lineage syntax in: {file_path}")
        
        validation_result = {
            'valid': True,
            'issues': [],
            'lineage_decorators_found': 0,
            'classes_with_lineage': 0
        }
        
        try:
            with open(file_path, "r") as f:
                source_code = f.read()

            tree = ast.parse(source_code)
            classes_with_lineage = 0
            total_lineage_decorators = 0

            for node in ast.walk(tree):
                if isinstance(node, ast.ClassDef):
                    class_lineage_count = 0
                    
                    for item in node.body:
                        if isinstance(item, ast.FunctionDef):
                            for decorator in item.decorator_list:
                                if self._is_lineage_decorator(decorator):
                                    total_lineage_decorators += 1
                                    class_lineage_count += 1
                    
                    if class_lineage_count > 0:
                        classes_with_lineage += 1

            validation_result['lineage_decorators_found'] = total_lineage_decorators
            validation_result['classes_with_lineage'] = classes_with_lineage
            
            logger.info(f"Validation completed: {total_lineage_decorators} decorators in {classes_with_lineage} classes")
            return validation_result
            
        except SyntaxError as e:
            validation_result['valid'] = False
            validation_result['issues'].append(f"Syntax error: {e}")
            logger.error(f"Syntax error in file: {e}")
            return validation_result
        except Exception as e:
            validation_result['valid'] = False
            validation_result['issues'].append(f"Validation error: {e}")
            logger.error(f"Validation failed: {e}")
            return validation_result