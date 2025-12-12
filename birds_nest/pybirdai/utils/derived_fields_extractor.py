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
import ast
import inspect
import argparse
import glob
import csv

import os
import django
from django.db import models
from django.conf import settings
import sys

import logging

# Constants for file paths
MANUAL_DERIVATION_FILE = "resources/derivation_files/derived_field_configuration.py"
GENERATED_DERIVATION_DIR = "resources/derivation_files/generated"
DERIVATION_CONFIG_FILE = "resources/derivation_files/derivation_config.csv"


class DjangoSetup:
    @staticmethod
    def configure_django():
        """Configure Django settings without starting the application"""
        if not settings.configured:
            # Set up Django settings module for birds_nest in parent directory
            project_root = os.path.abspath(
                os.path.join(os.path.dirname(__file__), "../..")
            )
            sys.path.insert(0, project_root)
            os.environ["DJANGO_SETTINGS_MODULE"] = "birds_nest.settings"
            django.setup()


def extract_classes_with_lineage_properties(path: str):
    """Extract classes that have properties with 'lineage' decorator"""
    DjangoSetup.configure_django()

    classes_with_lineage = []

    # Parse the bird_data_model.py file using AST
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
                        if (
                            (
                                isinstance(decorator, ast.Name)
                                and decorator.id == "lineage"
                            )
                            or (
                                isinstance(decorator, ast.Attribute)
                                and decorator.attr == "lineage"
                            )
                            or (
                                isinstance(decorator, ast.Call)
                                and (
                                    (
                                        isinstance(decorator.func, ast.Name)
                                        and decorator.func.id == "lineage"
                                    )
                                    or (
                                        isinstance(decorator.func, ast.Attribute)
                                        and decorator.func.attr == "lineage"
                                    )
                                )
                            )
                        ):
                            lineage_properties.append(item.name)
                            break

            if lineage_properties:
                class_info = {
                    "class_name": class_name,
                    "property_names": lineage_properties,
                    "class_node": node,
                }
                classes_with_lineage.append(class_info)

    return classes_with_lineage


def generate_ast_output(classes_info):
    """Generate AST representation of the extracted classes"""
    DjangoSetup.configure_django()

    # Create module node
    module = ast.Module(body=[], type_ignores=[])
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
                # Create function def
                func_def = ast.FunctionDef(
                    name=item.name,
                    args=item.args,
                    body=item.body,
                    decorator_list=item.decorator_list,
                    returns=item.returns,
                )
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

    return module


def check_if_file_already_modified(file_path):
    """Check if the file already has lineage imports and decorators"""
    with open(file_path, "r") as f:
        content = f.read()

    # Check for lineage import and @lineage decorator
    has_lineage_import = (
        "from pybirdai.annotations.decorators import lineage" in content
    )
    has_lineage_decorator = "@lineage" in content

    return has_lineage_import and has_lineage_decorator


def merge_derived_fields_into_original_model(
    bird_data_model_path, lineage_classes_ast_path
):
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
    logger = logging.getLogger(__name__)

    # Check if file has already been modified
    if check_if_file_already_modified(bird_data_model_path):
        logger.info(
            "File already contains @lineage decorators and imports, skipping modification"
        )
        return False

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
                        if (
                            (
                                isinstance(decorator, ast.Name)
                                and decorator.id == "lineage"
                            )
                            or (
                                isinstance(decorator, ast.Attribute)
                                and decorator.attr == "lineage"
                            )
                            or (
                                isinstance(decorator, ast.Call)
                                and (
                                    (
                                        isinstance(decorator.func, ast.Name)
                                        and decorator.func.id == "lineage"
                                    )
                                    or (
                                        isinstance(decorator.func, ast.Attribute)
                                        and decorator.func.attr == "lineage"
                                    )
                                )
                            )
                        ):
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


def load_enabled_fields_from_config(config_path: str = DERIVATION_CONFIG_FILE) -> dict:
    """
    Load enabled fields from the derivation config CSV.

    Args:
        config_path: Path to the derivation_config.csv file.

    Returns:
        Dictionary mapping class names to sets of enabled field names.
    """
    logger = logging.getLogger(__name__)
    enabled_fields = {}

    if not os.path.exists(config_path):
        logger.warning(f"Derivation config not found: {config_path}")
        return enabled_fields

    with open(config_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            class_name = row.get('class_name', '').strip()
            field_name = row.get('field_name', '').strip()
            enabled = row.get('enabled', '').strip().lower()

            # Skip comments and empty rows
            if class_name.startswith('#') or not class_name:
                continue

            if enabled == 'true':
                if class_name not in enabled_fields:
                    enabled_fields[class_name] = set()
                enabled_fields[class_name].add(field_name)

    logger.info(f"Loaded config: {sum(len(f) for f in enabled_fields.values())} enabled fields across {len(enabled_fields)} classes")
    return enabled_fields


def collect_all_derivation_files(
    manual_file: str = MANUAL_DERIVATION_FILE,
    generated_dir: str = GENERATED_DERIVATION_DIR
) -> list[str]:
    """
    Collect all derivation files from both manual and generated sources.

    Args:
        manual_file: Path to the manual derived_field_configuration.py file.
        generated_dir: Path to the directory containing generated *_derived.py files.

    Returns:
        List of paths to all derivation files (manual first, then generated).
    """
    files = []

    # Add manual file if it exists
    if os.path.exists(manual_file):
        files.append(manual_file)

    # Add generated files if directory exists
    if os.path.exists(generated_dir):
        generated_pattern = os.path.join(generated_dir, "*_derived.py")
        generated_files = sorted(glob.glob(generated_pattern))
        files.extend(generated_files)

    return files


def extract_derived_classes_from_files(
    file_paths: list[str],
    manual_takes_precedence: bool = True,
    enabled_fields: dict = None
) -> dict:
    """
    Extract derived classes from multiple files.

    Manual implementations are always included.
    Generated implementations are only included if they are enabled in the config.
    Manual implementations take precedence over auto-generated ones
    when there are conflicts (same class + same property name).

    Args:
        file_paths: List of paths to derivation files.
        manual_takes_precedence: If True, manual implementations override generated ones.
        enabled_fields: Dict mapping class_name -> set of enabled field names.
                       If None, all fields are included. Only applies to generated files.

    Returns:
        Dictionary mapping class names to lists of property AST nodes.
    """
    logger = logging.getLogger(__name__)
    derived_classes = {}
    seen_properties = {}  # class_name -> {property_name: source_file}

    for file_path in file_paths:
        if not os.path.exists(file_path):
            continue

        is_manual = MANUAL_DERIVATION_FILE in file_path

        with open(file_path, "r") as f:
            content = f.read()

        try:
            tree = ast.parse(content)
        except SyntaxError as e:
            logger.warning(f"Skipping {file_path} due to syntax error: {e}")
            continue

        # Extract classes and their derived properties
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                class_name = node.name
                derived_properties = []

                # Find all properties with @lineage decorator
                for item in node.body:
                    if isinstance(item, ast.FunctionDef):
                        for decorator in item.decorator_list:
                            if (
                                (isinstance(decorator, ast.Name) and decorator.id == "lineage")
                                or (isinstance(decorator, ast.Attribute) and decorator.attr == "lineage")
                                or (
                                    isinstance(decorator, ast.Call)
                                    and (
                                        (isinstance(decorator.func, ast.Name) and decorator.func.id == "lineage")
                                        or (isinstance(decorator.func, ast.Attribute) and decorator.func.attr == "lineage")
                                    )
                                )
                            ):
                                prop_name = item.name

                                # For generated files, check if field is enabled in config
                                if not is_manual and enabled_fields is not None:
                                    if class_name not in enabled_fields:
                                        logger.debug(
                                            f"Skipping {class_name}.{prop_name} "
                                            f"(class not enabled in config)"
                                        )
                                        continue
                                    if prop_name not in enabled_fields[class_name]:
                                        logger.debug(
                                            f"Skipping {class_name}.{prop_name} "
                                            f"(field not enabled in config)"
                                        )
                                        continue

                                # Check for conflicts
                                if class_name in seen_properties and prop_name in seen_properties[class_name]:
                                    prev_source = seen_properties[class_name][prop_name]
                                    prev_is_manual = MANUAL_DERIVATION_FILE in prev_source

                                    if manual_takes_precedence:
                                        if prev_is_manual and not is_manual:
                                            # Skip: manual takes precedence
                                            logger.debug(
                                                f"Skipping generated {class_name}.{prop_name} "
                                                f"(manual implementation takes precedence)"
                                            )
                                            continue
                                        elif not prev_is_manual and is_manual:
                                            # Replace: new manual implementation
                                            logger.debug(
                                                f"Replacing generated {class_name}.{prop_name} "
                                                f"with manual implementation"
                                            )
                                            # Remove the old property
                                            if class_name in derived_classes:
                                                derived_classes[class_name] = [
                                                    p for p in derived_classes[class_name]
                                                    if p.name != prop_name
                                                ]

                                derived_properties.append(item)
                                logger.info(f"Including {class_name}.{prop_name} ({'manual' if is_manual else 'generated'})")

                                # Track seen properties
                                if class_name not in seen_properties:
                                    seen_properties[class_name] = {}
                                seen_properties[class_name][prop_name] = file_path
                                break

                if derived_properties:
                    if class_name not in derived_classes:
                        derived_classes[class_name] = []
                    derived_classes[class_name].extend(derived_properties)

    return derived_classes


def merge_all_derived_fields_into_model(
    bird_data_model_path: str,
    manual_file: str = MANUAL_DERIVATION_FILE,
    generated_dir: str = GENERATED_DERIVATION_DIR,
    config_file: str = DERIVATION_CONFIG_FILE
) -> bool:
    """
    Merge derived fields from both manual and generated sources into the model.

    This function:
    1. Loads the derivation config to determine which generated fields to include
    2. Collects all derivation files (manual + generated)
    3. Extracts derived classes (manual always included, generated filtered by config)
    4. Merges them into the bird_data_model.py file

    Args:
        bird_data_model_path: Path to the original bird_data_model.py file.
        manual_file: Path to manual derivation file.
        generated_dir: Path to generated derivation files directory.
        config_file: Path to derivation_config.csv for filtering generated fields.

    Returns:
        bool: True if modifications were made, False if file was already modified.
    """
    logger = logging.getLogger(__name__)

    # Check if file has already been modified
    if check_if_file_already_modified(bird_data_model_path):
        logger.info(
            "File already contains @lineage decorators and imports, skipping modification"
        )
        return False

    # Load enabled fields from config (for filtering generated files)
    enabled_fields = load_enabled_fields_from_config(config_file)

    # Collect all derivation files
    derivation_files = collect_all_derivation_files(manual_file, generated_dir)

    if not derivation_files:
        logger.warning("No derivation files found")
        return False

    logger.info(f"Found {len(derivation_files)} derivation file(s)")
    for f in derivation_files:
        logger.debug(f"  - {f}")

    # Extract derived classes from all files (filtered by config for generated files)
    derived_classes = extract_derived_classes_from_files(
        derivation_files,
        enabled_fields=enabled_fields
    )

    if not derived_classes:
        logger.warning("No derived classes found in derivation files")
        return False

    logger.info(f"Found derived fields for {len(derived_classes)} class(es)")

    # Parse original file
    with open(bird_data_model_path, "r") as f:
        original_content = f.read()

    original_tree = ast.parse(original_content)

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

            # Get the derived properties
            derived_properties = derived_classes[class_name]
            derived_property_names = [prop.name for prop in derived_properties]

            # Remove existing fields that are overwritten by derived properties
            new_body = []
            meta_class = None

            for item in node.body:
                if isinstance(item, ast.ClassDef) and item.name == "Meta":
                    meta_class = item
                    continue
                elif isinstance(item, ast.Assign):
                    if len(item.targets) == 1 and isinstance(item.targets[0], ast.Name):
                        field_name = item.targets[0].id
                        if field_name in derived_property_names:
                            continue
                new_body.append(item)

            # Add the derived properties before the Meta class
            for derived_property in derived_properties:
                new_body.append(derived_property)

            # Add Meta class at the end if it exists
            if meta_class:
                new_body.append(meta_class)

            node.body = new_body

    # Write the modified content back
    modified_content = ast.unparse(original_tree)
    with open(bird_data_model_path, "w") as f:
        f.write(modified_content)

    logger.info(f"Successfully modified {bird_data_model_path} with derived fields from all sources")
    return True


def main():
    """
    Main command-line interface for the derived fields extractor.

    Usage examples:
        # Extract and generate AST only
        python derived_fields_extractor.py path/to/bird_meta_data_model.py

        # Extract and save to database
        python derived_fields_extractor.py path/to/bird_meta_data_model.py --save-to-db
    """
    DjangoSetup.configure_django()

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logger = logging.getLogger(__name__)

    #     parser = argparse.ArgumentParser(
    #         description='Extract classes with lineage properties from a Python file and optionally save as cube structure items',
    #         epilog="""
    # Examples:
    #   %(prog)s bird_meta_data_model.py
    #   %(prog)s bird_meta_data_model.py --save-to-db
    #   %(prog)s bird_meta_data_model.py --save-to-db --cube-structure-id my_cube
    #         """,
    #         formatter_class=argparse.RawDescriptionHelpFormatter
    #     )
    #     parser.add_argument('file_path', help='Path to the Python file to analyze')
    #     parser.add_argument('--save-to-db', action='store_true', help='Save derived fields to database as cube structure items')
    #     args = parser.parse_args()

    # Extract classes with lineage properties
    # lineage_classes = extract_classes_with_lineage_properties(args.file_path)

    # # Generate AST
    # ast_module = generate_ast_output(lineage_classes)

    # # Write to file
    # os.makedirs("results/derivation_files/", exist_ok=True)
    # with open('results/derivation_files/derived_field_configuration.py', 'w') as f:
    #     f.write(ast.unparse(ast_module))

    # print(f"Extracted {len(lineage_classes)} classes with lineage properties")
    print("Output written to derived_field_configuration.py")
    model_file_path = f"pybirdai{os.sep}models{os.sep}bird_data_model.py"
    derived_fields_file_path = (
        f"resources{os.sep}derivation_files{os.sep}derived_field_configuration.py"
    )

    merge_all_derived_fields_into_model(model_file_path)


if __name__ == "__main__":
    main()
