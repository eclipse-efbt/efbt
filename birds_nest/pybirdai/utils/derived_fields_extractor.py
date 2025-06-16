import ast
import inspect
import argparse

import os
import django
from django.db import models
from django.conf import settings
import sys
import numpy as np
import logging


class DjangoSetup:
    @staticmethod
    def configure_django():
        """Configure Django settings without starting the application"""
        if not settings.configured:
            # Set up Django settings module for birds_nest in parent directory
            project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
            sys.path.insert(0, project_root)
            os.environ['DJANGO_SETTINGS_MODULE'] = 'birds_nest.settings'
            django.setup()

def extract_classes_with_lineage_properties(path:str):
    """Extract classes that have properties with 'lineage' decorator"""
    DjangoSetup.configure_django()

    classes_with_lineage = []

    # Parse the bird_data_model.py file using AST
    with open(path, 'r') as f:
        source_code = f.read()

    tree = ast.parse(source_code)

    # Walk through the AST to find classes with lineage decorators
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef):
            class_name = node.name
            has_lineage = False
            lineage_property = None

            # Check each method/property in the class
            for item in node.body:
                if isinstance(item, ast.FunctionDef):
                    # Check if it has decorators
                    for decorator in item.decorator_list:
                        # Check for lineage decorator
                        if (isinstance(decorator, ast.Name) and decorator.id == 'lineage') or \
                           (isinstance(decorator, ast.Attribute) and decorator.attr == 'lineage') or \
                           (isinstance(decorator, ast.Call) and
                            ((isinstance(decorator.func, ast.Name) and decorator.func.id == 'lineage') or
                             (isinstance(decorator.func, ast.Attribute) and decorator.func.attr == 'lineage'))):
                            has_lineage = True
                            lineage_property = item.name
                            break

                    if has_lineage:
                        break

            if has_lineage:
                class_info = {
                    'class_name': class_name,
                    'property_name': lineage_property,
                    'class_node': node
                }
                classes_with_lineage.append(class_info)

    return classes_with_lineage

def generate_ast_output(classes_info):
    """Generate AST representation of the extracted classes"""
    DjangoSetup.configure_django()

    # Create module node
    module = ast.Module(body=[], type_ignores=[])
    import_sttmt_1 = ast.ImportFrom(module='django.db',names=[ast.alias(name='models')],level=0)
    import_sttmt_2 = ast.ImportFrom(module='pybirdai.annotations.decorators',names=[ast.alias(name='lineage')],level=0)

    module.body.append(import_sttmt_1)
    module.body.append(import_sttmt_2)

    for class_info in classes_info:
        class_node = class_info['class_node']
        class_name = class_info['class_name']

        # Create class definition
        class_def = ast.ClassDef(
            name=class_name,
            bases=class_node.bases,
            keywords=class_node.keywords,
            decorator_list=class_node.decorator_list,
            body=[]
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
                    returns=item.returns
                )
                class_def.body.append(item)
            elif isinstance(item, ast.ClassDef) and item.name == 'Meta':
                # Add Meta class
                meta_class = ast.ClassDef(
                    name='Meta',
                    bases=item.bases,
                    keywords=item.keywords,
                    decorator_list=item.decorator_list,
                    body=[ast.Pass()]
                )
                class_def.body.append(meta_class)

        if not class_def.body:
            class_def.body.append(ast.Pass())

        module.body.append(class_def)

    return module

def save_derived_fields_as_cube_structure_items(classes_info, cube_structure_id=None):
    """
    Save derived fields as cube structure items with Integer subdomain.

    This function takes classes that have properties with 'lineage' decorators and creates
    corresponding cube structure items in the database. Each derived field is saved as:
    - A VARIABLE with Integer domain
    - A CUBE_STRUCTURE_ITEM linked to the cube structure with Integer subdomain

    Args:
        classes_info (list): List of dictionaries containing class information with keys:
            - 'class_name': Name of the class
            - 'property_name': Name of the property with lineage decorator
            - 'class_node': AST node of the class
        cube_structure_id (str, optional): ID of the cube structure to save items to.
                                         If None, class_name will be used as cube_structure_id

    Returns:
        list: List of dictionaries containing saved items with keys:
            - 'cube_structure_item': The created CUBE_STRUCTURE_ITEM instance
            - 'variable': The created VARIABLE instance
            - 'class_name': Name of the source class
            - 'property_name': Name of the source property
            - 'created': Boolean indicating if the item was newly created

    Raises:
        Exception: If there's an error during database operations
    """
    from pybirdai.bird_meta_data_model import CUBE_STRUCTURE, CUBE_STRUCTURE_ITEM, SUBDOMAIN, VARIABLE, DOMAIN
    from django.db import transaction

    DjangoSetup.configure_django()

    logger = logging.getLogger(__name__)
    saved_items = []

    if not classes_info:
        logger.warning("No classes with lineage properties found to save")
        return saved_items

    try:
        with transaction.atomic():
            # Get or create Integer subdomain
            integer_subdomain, subdomain_created = SUBDOMAIN.objects.get_or_create(
                subdomain_id="Integer",
                defaults={
                    'name': 'Integer',
                    'description': 'Integer subdomain for derived fields'
                }
            )
            if subdomain_created:
                logger.info("Created Integer subdomain")

            # Get or create Integer domain if it doesn't exist
            integer_domain, domain_created = DOMAIN.objects.get_or_create(
                domain_id="Integer",
                defaults={
                    'name': 'Integer',
                    'description': 'Integer domain',
                    'data_type': 'Integer'
                }
            )
            if domain_created:
                logger.info("Created Integer domain")

            # Link subdomain to domain if not already linked
            if not integer_subdomain.domain_id:
                integer_subdomain.domain_id = integer_domain
                integer_subdomain.save()
                logger.info("Linked Integer subdomain to Integer domain")

            # Process each class with lineage properties
            for class_info in classes_info:
                try:
                    class_name = class_info['class_name']
                    property_name = class_info['property_name']

                    # Use class_name as cube_structure_id if not provided
                    current_cube_structure_id = cube_structure_id if cube_structure_id else class_name

                    # Get or create the cube structure for this class
                    try:
                        cube_structure = CUBE_STRUCTURE.objects.get(cube_structure_id=current_cube_structure_id)
                        logger.info(f"Using existing cube structure: {current_cube_structure_id}")
                    except CUBE_STRUCTURE.DoesNotExist:
                        cube_structure = CUBE_STRUCTURE.objects.create(
                            cube_structure_id=current_cube_structure_id,
                            name=f"Cube Structure for {current_cube_structure_id}",
                            description=f"Auto-generated cube structure for derived fields from {class_name}"
                        )
                        logger.info(f"Created new cube structure: {current_cube_structure_id}")

                    # Create or get variable for the derived field
                    variable_id = f"{property_name}"
                    variable, variable_created = VARIABLE.objects.get_or_create(
                        variable_id=variable_id,
                        defaults={
                            'name': variable_id,
                            'description': f"Derived field {property_name} from class {class_name}",
                            'domain_id': integer_domain
                        }
                    )

                    # Create cube structure item
                    cube_item, item_created = CUBE_STRUCTURE_ITEM.objects.get_or_create(
                        cube_structure_id=cube_structure,
                        cube_variable_code=f"{class_name}_{property_name}",
                        defaults={
                            'variable_id': variable,
                            'cube_variable_code': f"{class_name}__{property_name}",
                        }
                    )

                    saved_items.append({
                        'cube_structure_item': cube_item,
                        'variable': variable,
                        'class_name': class_name,
                        'property_name': property_name,
                        'created': item_created
                    })

                    action = "created" if item_created else "found existing"
                    logger.info(f"Successfully {action} cube structure item for {class_name}.{property_name} in cube structure {current_cube_structure_id}")

                except Exception as e:
                    logger.error(f"Error processing {class_info.get('class_name', 'unknown')}.{class_info.get('property_name', 'unknown')}: {str(e)}")
                    raise

    except Exception as e:
        logger.error(f"Error saving derived fields as cube structure items: {str(e)}")
        raise

    logger.info(f"Successfully processed {len(saved_items)} derived fields as cube structure items")
    return saved_items


def extract_and_save_derived_fields(file_path, cube_structure_id='derived_fields_cube'):
    """
    Convenience function to extract classes with lineage properties and save them as cube structure items.

    This is a high-level function that combines the extraction and saving process in one call.
    It will:
    1. Parse the Python file to find classes with lineage-decorated properties
    2. Create cube structure items for each derived field
    3. Set up the necessary database relationships (domains, subdomains, variables)

    Args:
        file_path (str): Path to the Python file to analyze (usually bird_meta_data_model.py)
        cube_structure_id (str): ID of the cube structure to save items to.
                                Defaults to 'derived_fields_cube'

    Returns:
        tuple: (lineage_classes, saved_items) where:
            - lineage_classes: List of extracted class information
            - saved_items: List of database items that were created/updated

    Example:
        >>> classes, items = extract_and_save_derived_fields('bird_meta_data_model.py')
        >>> print(f"Found {len(classes)} classes with {len(items)} derived fields")
    """
    DjangoSetup.configure_django()

    # Extract classes with lineage properties
    lineage_classes = extract_classes_with_lineage_properties(file_path)

    # Save to database
    saved_items = save_derived_fields_as_cube_structure_items(lineage_classes, cube_structure_id)

    return lineage_classes, saved_items

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
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)

    parser = argparse.ArgumentParser(
        description='Extract classes with lineage properties from a Python file and optionally save as cube structure items',
        epilog="""
Examples:
  %(prog)s bird_meta_data_model.py
  %(prog)s bird_meta_data_model.py --save-to-db
  %(prog)s bird_meta_data_model.py --save-to-db --cube-structure-id my_cube
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('file_path', help='Path to the Python file to analyze')
    parser.add_argument('--save-to-db', action='store_true', help='Save derived fields to database as cube structure items')
    args = parser.parse_args()

    # Extract classes with lineage properties
    lineage_classes = extract_classes_with_lineage_properties(args.file_path)

    # Generate AST
    ast_module = generate_ast_output(lineage_classes)

    # Write to file
    os.makedirs("results/derivation_files/", exist_ok=True)
    with open('results/derivation_files/lineage_classes_ast.py', 'w') as f:
        f.write(ast.unparse(ast_module))

    print(f"Extracted {len(lineage_classes)} classes with lineage properties")
    print("Output written to lineage_classes_ast.py")

    # Save to database if requested
    if args.save_to_db:
        saved_items = save_derived_fields_as_cube_structure_items(lineage_classes, "")
        print(f"Saved {len(saved_items)} derived fields as cube structure items:")
        for item in saved_items:
            status = "created" if item['created'] else "updated"
            print(f"  - {item['class_name']}.{item['property_name']} ({status})")

if __name__ == "__main__":
    main()
