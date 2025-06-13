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

def main():
    DjangoSetup.configure_django()
    parser = argparse.ArgumentParser(description='Extract classes with lineage properties from a Python file')
    parser.add_argument('file_path', help='Path to the Python file to analyze')
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

if __name__ == "__main__":
    main()
