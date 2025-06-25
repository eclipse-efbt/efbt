"""
Advanced Django Migration Generator using AST Only

This module provides enhanced functionality to generate Django migration files from model definitions
by parsing Python source code and extracting model information using AST. All code generation is done
using AST nodes without any string concatenation.
"""

import ast
import os
import re
from typing import Dict, List, Any, Optional, Tuple, Union
from dataclasses import dataclass
from pathlib import Path


@dataclass
class FieldInfo:
    """Represents a Django model field."""
    name: str
    field_type: str
    verbose_name: Optional[str] = None
    max_length: Optional[int] = None
    blank: bool = False
    null: bool = False
    default: Any = None
    primary_key: bool = False
    choices: Optional[List[Tuple[str, str]]] = None
    db_comment: Optional[str] = None
    foreign_key_to: Optional[str] = None
    on_delete: Optional[str] = None
    related_name: Optional[str] = None
    auto_created: bool = False
    serialize: bool = True
    unique: bool = False
    db_index: bool = False
    editable: bool = True
    help_text: Optional[str] = None
    parent_link: bool = False


@dataclass
class ModelInfo:
    """Represents a Django model."""
    name: str
    fields: List[FieldInfo]
    verbose_name: Optional[str] = None
    verbose_name_plural: Optional[str] = None
    db_table: Optional[str] = None
    ordering: Optional[List[str]] = None
    abstract: bool = False
    parent_model: Optional[str] = None
    bases: List[str] = None

    def __post_init__(self):
        if self.bases is None:
            self.bases = []


class ModelParser(ast.NodeVisitor):
    """AST visitor to parse Django model definitions from source code."""

    def __init__(self):
        self.models = []
        self.current_model = None
        self.current_class_name = None
        self.choice_domains = {}
        self.all_classes = {}  # Track all classes for inheritance detection
        self.potential_models = {}  # Store potential models for second pass

    def visit_ClassDef(self, node: ast.ClassDef):
        """Visit class definitions to find Django models."""
        self.current_class_name = node.name

        # Store all classes for inheritance detection
        self.all_classes[node.name] = node

        # Store potential models for two-pass processing
        self.potential_models[node.name] = node

        # Check for choice domains (dictionaries with domain suffix)
        if node.name.endswith('_domain'):
            self._parse_choice_domain(node)

        self.generic_visit(node)

    def process_models(self):
        """Process all potential models in two passes to handle inheritance."""
        # First pass: identify direct models.Model inheritors
        direct_models = set()

        for class_name, node in self.potential_models.items():
            for base in node.bases:
                if (isinstance(base, ast.Attribute) and isinstance(base.value, ast.Name) and
                    base.value.id == 'models' and base.attr == 'Model'):
                    direct_models.add(class_name)
                    self._parse_model(node, parent_model=None)
                    break

        # Second pass: identify model inheritors (subclasses of existing models)
        for class_name, node in self.potential_models.items():
            if class_name in direct_models:
                continue  # Already processed

            for base in node.bases:
                parent_model = None
                if isinstance(base, ast.Name):
                    # Direct class name (e.g., ADVNC)
                    if base.id in direct_models or base.id.isupper():
                        parent_model = base.id
                        self._parse_model(node, parent_model=parent_model)
                        break
                elif isinstance(base, ast.Attribute) and isinstance(base.value, ast.Name):
                    # Qualified name (e.g., app.Model)
                    base_name = base.attr
                    if base_name in direct_models or base_name.isupper():
                        parent_model = base_name
                        self._parse_model(node, parent_model=parent_model)
                        break

    def _parse_model(self, node: ast.ClassDef, parent_model: Optional[str] = None):
        """Parse a single model class definition."""
        bases = []
        for base in node.bases:
            if isinstance(base, ast.Attribute) and isinstance(base.value, ast.Name):
                bases.append(f'{base.value.id}.{base.attr}')
            elif isinstance(base, ast.Name):
                bases.append(base.id)

        self.current_model = ModelInfo(name=node.name, fields=[], parent_model=parent_model, bases=bases)

        # Parse class body
        for item in node.body:
            if isinstance(item, ast.Assign):
                self._parse_field_assignment(item)
            elif isinstance(item, ast.ClassDef) and item.name == 'Meta':
                self._parse_meta_class(item)

        self.models.append(self.current_model)
        self.current_model = None

    def visit_Assign(self, node: ast.Assign):
        """Visit assignments to find choice domains."""
        for target in node.targets:
            if isinstance(target, ast.Name) and target.id.endswith('_domain'):
                if isinstance(node.value, ast.Dict):
                    choices = []
                    for key, value in zip(node.value.keys, node.value.values):
                        if isinstance(key, ast.Constant) and isinstance(value, ast.Constant):
                            choices.append((str(key.value), str(value.value)))
                    self.choice_domains[target.id] = choices

        self.generic_visit(node)

    def _parse_choice_domain(self, node: ast.ClassDef):
        """Parse choice domain from class definition."""
        # This is for when domains are defined as class attributes
        pass

    def _parse_field_assignment(self, node: ast.Assign):
        """Parse field assignments in model classes."""
        if not self.current_model:
            return

        for target in node.targets:
            if isinstance(target, ast.Name):
                field_name = target.id

                # Skip non-field attributes
                if field_name.startswith('_') or field_name in ['DoesNotExist', 'MultipleObjectsReturned']:
                    continue

                field_info = self._parse_field_call(field_name, node.value)
                if field_info:
                    # Check for duplicate field names
                    existing_field_names = [f.name for f in self.current_model.fields]
                    if field_info.name not in existing_field_names:
                        self.current_model.fields.append(field_info)

    def _parse_field_call(self, field_name: str, node: ast.AST) -> Optional[FieldInfo]:
        """Parse field call to extract field information."""
        if not isinstance(node, ast.Call):
            return None

        field_info = FieldInfo(name=field_name, field_type='CharField')

        # Determine field type
        if isinstance(node.func, ast.Attribute):
            if isinstance(node.func.value, ast.Name) and node.func.value.id == 'models':
                field_info.field_type = node.func.attr

        # Parse arguments
        for i, arg in enumerate(node.args):
            if i == 0 and isinstance(arg, ast.Constant):
                # For ForeignKey/OneToOneField, first positional arg is 'to'
                if field_info.field_type in ['ForeignKey', 'OneToOneField']:
                    field_info.foreign_key_to = str(arg.value)
                else:
                    field_info.verbose_name = str(arg.value)

        # Parse keyword arguments
        for keyword in node.keywords:
            if keyword.arg == 'max_length' and isinstance(keyword.value, ast.Constant):
                field_info.max_length = int(keyword.value.value)
            elif keyword.arg == 'blank' and isinstance(keyword.value, ast.Constant):
                field_info.blank = bool(keyword.value.value)
            elif keyword.arg == 'null' and isinstance(keyword.value, ast.Constant):
                field_info.null = bool(keyword.value.value)
            elif keyword.arg == 'default':
                if isinstance(keyword.value, ast.Constant):
                    field_info.default = keyword.value.value
                elif isinstance(keyword.value, ast.Name) and keyword.value.id == 'None':
                    field_info.default = None
            elif keyword.arg == 'primary_key' and isinstance(keyword.value, ast.Constant):
                field_info.primary_key = bool(keyword.value.value)
            elif keyword.arg == 'choices':
                field_info.choices = self._parse_choices(keyword.value)
            elif keyword.arg == 'db_comment' and isinstance(keyword.value, ast.Constant):
                field_info.db_comment = str(keyword.value.value)
            elif keyword.arg == 'to' and isinstance(keyword.value, ast.Constant):
                field_info.foreign_key_to = str(keyword.value.value)
            elif keyword.arg == 'on_delete':
                field_info.on_delete = self._parse_on_delete(keyword.value)
            elif keyword.arg == 'related_name' and isinstance(keyword.value, ast.Constant):
                field_info.related_name = str(keyword.value.value)
            elif keyword.arg == 'auto_created' and isinstance(keyword.value, ast.Constant):
                field_info.auto_created = bool(keyword.value.value)
            elif keyword.arg == 'serialize' and isinstance(keyword.value, ast.Constant):
                field_info.serialize = bool(keyword.value.value)
            elif keyword.arg == 'unique' and isinstance(keyword.value, ast.Constant):
                field_info.unique = bool(keyword.value.value)
            elif keyword.arg == 'db_index' and isinstance(keyword.value, ast.Constant):
                field_info.db_index = bool(keyword.value.value)
            elif keyword.arg == 'help_text' and isinstance(keyword.value, ast.Constant):
                field_info.help_text = str(keyword.value.value)
            elif keyword.arg == 'parent_link' and isinstance(keyword.value, ast.Constant):
                field_info.parent_link = bool(keyword.value.value)

        return field_info

    def _parse_choices(self, node: ast.AST) -> Optional[List[Tuple[str, str]]]:
        """Parse choices field."""
        if isinstance(node, ast.Name):
            # Reference to a domain variable
            domain_name = node.id
            if domain_name in self.choice_domains:
                return self.choice_domains[domain_name]
        elif isinstance(node, ast.List):
            # Inline choices list
            choices = []
            for item in node.elts:
                if isinstance(item, ast.Tuple) and len(item.elts) == 2:
                    key_node, value_node = item.elts
                    if isinstance(key_node, ast.Constant) and isinstance(value_node, ast.Constant):
                        choices.append((str(key_node.value), str(value_node.value)))
            return choices

        return None

    def _parse_on_delete(self, node: ast.AST) -> Optional[str]:
        """Parse on_delete parameter."""
        if isinstance(node, ast.Attribute):
            parts = []
            current = node
            while isinstance(current, ast.Attribute):
                parts.append(current.attr)
                current = current.value
            if isinstance(current, ast.Name):
                parts.append(current.id)
            # Return the full path to ensure complete on_delete reference
            full_path = '.'.join(reversed(parts))
            # Ensure we have the complete django.db.models.deletion.X format
            if 'django' in full_path and 'deletion' in full_path:
                return full_path
            elif full_path.startswith('models.deletion'):
                return f'django.db.{full_path}'
            elif full_path.endswith('CASCADE') or full_path.endswith('SET_NULL') or full_path.endswith('PROTECT'):
                if not full_path.startswith('django.db.models.deletion'):
                    return f'django.db.models.deletion.{full_path.split(".")[-1]}'
            return full_path
        return None

    def _parse_meta_class(self, node: ast.ClassDef):
        """Parse Meta class in Django models."""
        if not self.current_model:
            return

        for item in node.body:
            if isinstance(item, ast.Assign):
                for target in item.targets:
                    if isinstance(target, ast.Name):
                        if target.id == 'verbose_name' and isinstance(item.value, ast.Constant):
                            self.current_model.verbose_name = str(item.value.value)
                        elif target.id == 'verbose_name_plural' and isinstance(item.value, ast.Constant):
                            self.current_model.verbose_name_plural = str(item.value.value)
                        elif target.id == 'db_table' and isinstance(item.value, ast.Constant):
                            self.current_model.db_table = str(item.value.value)
                        elif target.id == 'ordering' and isinstance(item.value, ast.List):
                            ordering = []
                            for elt in item.value.elts:
                                if isinstance(elt, ast.Constant):
                                    ordering.append(str(elt.value))
                            self.current_model.ordering = ordering
                        elif target.id == 'abstract' and isinstance(item.value, ast.Constant):
                            self.current_model.abstract = bool(item.value.value)


class AdvancedMigrationGenerator:
    """Generates Django migration files from parsed model information using only AST."""

    def __init__(self):
        self.models = []

    def parse_file(self, file_path: str) -> List[ModelInfo]:
        """Parse a Python file to extract model definitions."""
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()

        # Parse the file
        tree = ast.parse(content)
        parser = ModelParser()
        parser.visit(tree)
        parser.process_models()  # Two-pass processing for inheritance

        return parser.models

    def parse_files(self, file_paths: List[str]) -> List[ModelInfo]:
        """Parse multiple Python files to extract model definitions."""
        all_models = []

        # Create a single parser for all files to handle cross-file inheritance
        parser = ModelParser()

        # First pass: collect all classes across all files
        for file_path in file_paths:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                tree = ast.parse(content)
                parser.visit(tree)
            except Exception as e:
                print(f"Error parsing {file_path}: {e}")

        # Process models with inheritance awareness
        parser.process_models()
        return parser.models

    def parse_directory(self, directory_path: str, pattern: str = "*.py") -> List[ModelInfo]:
        """Parse all Python files in a directory to extract model definitions."""
        directory = Path(directory_path)
        file_paths = [str(f) for f in directory.glob(pattern) if f.is_file()]
        return self.parse_files(file_paths)

    def generate_migration_ast(self, models: List[ModelInfo], migration_name: str = "0001_initial") -> ast.Module:
        """Generate migration AST from model information."""
        # Import statements
        import_nodes = [
            ast.Import(names=[ast.alias(name='django.db.models.deletion', asname=None)]),
            ast.Import(names=[ast.alias(name='django.utils.timezone', asname=None)]),
            ast.ImportFrom(module='django.db', names=[
                ast.alias(name='migrations', asname=None),
                ast.alias(name='models', asname=None)
            ], level=0)
        ]

        # Create operations list
        operations = []
        add_field_operations = []

        for model in models:
            if model.abstract:
                continue

            # Separate fields: non-ForeignKey fields for CreateModel, ForeignKey fields for AddField
            create_model_fields = []
            foreign_key_fields = []
            reverse_fields = []

            for field in model.fields:
                if self._is_reverse_field(field):
                    reverse_fields.append(field)
                elif field.field_type in ['ForeignKey', 'OneToOneField']:
                    foreign_key_fields.append(field)
                else:
                    create_model_fields.append(field)

            # Build fields list for CreateModel (only non-ForeignKey fields)
            fields_list = []

            # Add ID field if no primary key exists and this is not an inherited model
            has_primary_key = any(field.primary_key for field in create_model_fields + foreign_key_fields)
            if not has_primary_key and not model.parent_model:
                id_field = ast.Tuple(elts=[
                    ast.Constant(value='id'),
                    ast.Call(
                        func=ast.Attribute(value=ast.Name(id='models', ctx=ast.Load()), attr='BigAutoField', ctx=ast.Load()),
                        args=[],
                        keywords=[
                            ast.keyword(arg='auto_created', value=ast.Constant(value=True)),
                            ast.keyword(arg='default', value=ast.Constant(value=None)),
                            ast.keyword(arg='primary_key', value=ast.Constant(value=True)),
                            ast.keyword(arg='serialize', value=ast.Constant(value=False)),
                            ast.keyword(arg='verbose_name', value=ast.Constant(value='ID'))
                        ]
                    )
                ], ctx=ast.Load())
                fields_list.append(id_field)

            # Add non-ForeignKey model fields (check for duplicates)
            seen_field_names = set()
            for field in create_model_fields:
                if field.name not in seen_field_names:
                    field_tuple = self._generate_field_ast(field)
                    fields_list.append(field_tuple)
                    seen_field_names.add(field.name)

            # Create CreateModel call
            create_model_keywords = [
                ast.keyword(arg='name', value=ast.Constant(value=model.name)),
                ast.keyword(arg='fields', value=ast.List(elts=fields_list, ctx=ast.Load()))
            ]

            # Add options if present
            options = self._generate_options_dict(model)
            if options.keys or options.values:
                create_model_keywords.append(ast.keyword(arg='options', value=options))

            # Add bases if this is a model inheritance (subclass)
            if model.parent_model:
                # For inherited models, add parent_link field if not already present
                has_parent_link = any(field.name.endswith('_ptr') for field in create_model_fields + foreign_key_fields)
                if not has_parent_link:
                    parent_link_name = f"{model.parent_model.lower()}_ptr"
                    parent_link_field = ast.Tuple(elts=[
                        ast.Constant(value=parent_link_name),
                        ast.Call(
                            func=ast.Attribute(value=ast.Name(id='models', ctx=ast.Load()), attr='OneToOneField', ctx=ast.Load()),
                            args=[],
                            keywords=[
                                ast.keyword(arg='auto_created', value=ast.Constant(value=True)),
                                ast.keyword(arg='on_delete', value=ast.Attribute(
                                    value=ast.Attribute(
                                        value=ast.Attribute(
                                            value=ast.Attribute(
                                                value=ast.Name(id='django', ctx=ast.Load()),
                                                attr='db', ctx=ast.Load()),
                                            attr='models', ctx=ast.Load()),
                                        attr='deletion', ctx=ast.Load()),
                                    attr='CASCADE', ctx=ast.Load())),
                                ast.keyword(arg='parent_link', value=ast.Constant(value=True)),
                                ast.keyword(arg='primary_key', value=ast.Constant(value=True)),
                                ast.keyword(arg='serialize', value=ast.Constant(value=False)),
                                ast.keyword(arg='to', value=ast.Constant(value=f'pybirdai.{model.parent_model.lower()}'))
                            ]
                        )
                    ], ctx=ast.Load())
                    fields_list.insert(0, parent_link_field)

                # Add bases parameter as tuple (Django convention)
                bases_tuple = ast.Tuple(elts=[ast.Constant(value=f'pybirdai.{model.parent_model.lower()}')], ctx=ast.Load())
                create_model_keywords.append(ast.keyword(arg='bases', value=bases_tuple))

            create_model_call = ast.Call(
                func=ast.Attribute(value=ast.Name(id='migrations', ctx=ast.Load()), attr='CreateModel', ctx=ast.Load()),
                args=[],
                keywords=create_model_keywords
            )
            operations.append(create_model_call)

            # Create AddField operations for ForeignKey fields
            for field in foreign_key_fields:
                add_field_call = self._generate_add_field_ast(model.name, field)
                add_field_operations.append(add_field_call)

            # Create AddField operations for reverse relationship fields
            for field in reverse_fields:
                add_field_call = self._generate_add_field_ast(model.name, field)
                add_field_operations.append(add_field_call)

        # Add all operations (CreateModel first, then AddField)
        all_operations = operations + add_field_operations

        # Create Migration class
        migration_class = ast.ClassDef(
            name='Migration',
            bases=[ast.Attribute(value=ast.Name(id='migrations', ctx=ast.Load()), attr='Migration', ctx=ast.Load())],
            keywords=[],
            body=[
                ast.Assign(
                    targets=[ast.Name(id='initial', ctx=ast.Store())],
                    value=ast.Constant(value=True)
                ),
                ast.Assign(
                    targets=[ast.Name(id='dependencies', ctx=ast.Store())],
                    value=ast.List(elts=[], ctx=ast.Load())
                ),
                ast.Assign(
                    targets=[ast.Name(id='operations', ctx=ast.Store())],
                    value=ast.List(elts=all_operations, ctx=ast.Load())
                )
            ],
            decorator_list=[]
        )

        # Create module
        module = ast.Module(body=import_nodes + [migration_class], type_ignores=[])
        return module

    def _is_reverse_field(self, field: FieldInfo) -> bool:
        """Check if field should be added as a separate AddField operation (typically reverse relationships)."""
        # A field is a reverse field if it has CASCADE deletion and contains '_to_' in the field name
        # has_cascade_deletion = (field.on_delete and ('CASCADE' in field.on_delete or 'models.CASCADE' in field.on_delete))
        return False

    def _generate_field_ast(self, field: FieldInfo) -> ast.Tuple:
        """Generate AST node for field definition."""
        keywords = []

        # Parameters in logical order: auto_created, blank, default, max_length, null, primary_key, serialize, verbose_name, choices, etc.

        # Add auto_created first if applicable
        if field.auto_created:
            keywords.append(ast.keyword(arg='auto_created', value=ast.Constant(value=True)))

        # Add blank parameter
        if field.blank:
            keywords.append(ast.keyword(arg='blank', value=ast.Constant(value=True)))

        # Only add default for non-ForeignKey fields in CreateModel (avoid clutter)
        if field.field_type not in ['ForeignKey', 'OneToOneField']:
            default_value = field.default if field.default is not None else None
            if default_value is not None or field.null:
                keywords.append(ast.keyword(arg='default', value=ast.Constant(value=default_value)))

        # Add field-specific arguments like max_length
        if field.field_type == 'CharField' and field.max_length:
            keywords.append(ast.keyword(arg='max_length', value=ast.Constant(value=field.max_length)))

        # Add null parameter
        if field.null:
            keywords.append(ast.keyword(arg='null', value=ast.Constant(value=True)))

        # For ForeignKey fields, ensure required parameters are present
        if field.field_type in ['ForeignKey', 'OneToOneField']:
            # Add on_delete parameter (required for ForeignKey/OneToOneField)
            if field.on_delete:
                on_delete_node = self._create_on_delete_ast(field.on_delete)
                keywords.append(ast.keyword(arg='on_delete', value=on_delete_node))
            else:
                # Choose appropriate default based on field characteristics
                # Use SET_NULL for nullable fields, CASCADE for required fields
                if field.null and not field.primary_key:
                    default_behavior = 'SET_NULL'
                else:
                    default_behavior = 'CASCADE'

                default_on_delete = ast.Attribute(
                    value=ast.Attribute(
                        value=ast.Attribute(
                            value=ast.Attribute(
                                value=ast.Name(id='django', ctx=ast.Load()),
                                attr='db', ctx=ast.Load()),
                            attr='models', ctx=ast.Load()),
                        attr='deletion', ctx=ast.Load()),
                    attr=default_behavior, ctx=ast.Load())
                keywords.append(ast.keyword(arg='on_delete', value=default_on_delete))

            # Add to parameter (required for ForeignKey/OneToOneField)
            if field.foreign_key_to:
                keywords.append(ast.keyword(arg='to', value=ast.Constant(value="pybirdai."+field.foreign_key_to.lower())))
            else:
                # Try to infer from field name or use a placeholder
                inferred_to = f'pybirdai.{field.name.lower().replace("_", "")}'
                keywords.append(ast.keyword(arg='to', value=ast.Constant(value="pybirdai."+inferred_to.lower())))
        else:
            # Add on_delete for other field types if present
            if field.on_delete:
                on_delete_node = self._create_on_delete_ast(field.on_delete)
                keywords.append(ast.keyword(arg='on_delete', value=on_delete_node))

        # Add primary_key parameter
        if field.primary_key:
            keywords.append(ast.keyword(arg='primary_key', value=ast.Constant(value=True)))

        # Always explicitly set serialize=False for primary keys
        if field.primary_key or not field.serialize:
            keywords.append(ast.keyword(arg='serialize', value=ast.Constant(value=False)))

        # Add to parameter for non-ForeignKey fields if present
        if field.foreign_key_to and field.field_type not in ['ForeignKey', 'OneToOneField']:
            keywords.append(ast.keyword(arg='to', value=ast.Constant(value="pybirdai."+field.foreign_key_to.lower())))

        # Add other special parameters
        if field.parent_link:
            keywords.append(ast.keyword(arg='parent_link', value=ast.Constant(value=True)))

        # Add verbose_name as named parameter
        if field.verbose_name:
            keywords.append(ast.keyword(arg='verbose_name', value=ast.Constant(value=field.verbose_name)))

        # Add other parameters
        if field.choices:
            choices_list = []
            for k, v in field.choices:
                choice_tuple = ast.Tuple(elts=[ast.Constant(value=k), ast.Constant(value=v)], ctx=ast.Load())
                choices_list.append(choice_tuple)
            keywords.append(ast.keyword(arg='choices', value=ast.List(elts=choices_list, ctx=ast.Load())))

        if field.db_comment:
            keywords.append(ast.keyword(arg='db_comment', value=ast.Constant(value=field.db_comment)))

        if field.related_name:
            keywords.append(ast.keyword(arg='related_name', value=ast.Constant(value=field.related_name)))

        if field.unique:
            keywords.append(ast.keyword(arg='unique', value=ast.Constant(value=True)))

        if field.db_index:
            keywords.append(ast.keyword(arg='db_index', value=ast.Constant(value=True)))

        if field.help_text:
            keywords.append(ast.keyword(arg='help_text', value=ast.Constant(value=field.help_text)))

        # Create field call with no positional arguments, only named parameters
        field_call = ast.Call(
            func=ast.Attribute(value=ast.Name(id='models', ctx=ast.Load()), attr=field.field_type, ctx=ast.Load()),
            args=[],  # No positional arguments
            keywords=keywords
        )

        return ast.Tuple(elts=[ast.Constant(value=field.name), field_call], ctx=ast.Load())

    def _generate_add_field_ast(self, model_name: str, field: FieldInfo) -> ast.Call:
        """Generate AddField operation AST node for reverse relationship fields."""
        # Generate field definition
        field_call = self._generate_field_call_ast(field)

        add_field_call = ast.Call(
            func=ast.Attribute(value=ast.Name(id='migrations', ctx=ast.Load()), attr='AddField', ctx=ast.Load()),
            args=[],
            keywords=[
                ast.keyword(arg='model_name', value=ast.Constant(value=model_name.lower())),
                ast.keyword(arg='name', value=ast.Constant(value=field.name)),
                ast.keyword(arg='field', value=field_call)
            ]
        )

        return add_field_call

    def _generate_field_call_ast(self, field: FieldInfo) -> ast.Call:
        """Generate field call AST node for use in AddField operations."""
        keywords = []

        # Parameters in logical order: auto_created, blank, default, max_length, null, primary_key, serialize, verbose_name, choices, etc.

        # Add auto_created first if applicable
        if field.auto_created:
            keywords.append(ast.keyword(arg='auto_created', value=ast.Constant(value=True)))

        # Add blank parameter
        if field.blank:
            keywords.append(ast.keyword(arg='blank', value=ast.Constant(value=True)))

        # Add field-specific arguments like max_length
        if field.field_type == 'CharField' and field.max_length:
            keywords.append(ast.keyword(arg='max_length', value=ast.Constant(value=field.max_length)))

        # Add null parameter
        if field.null:
            keywords.append(ast.keyword(arg='null', value=ast.Constant(value=True)))

        # For ForeignKey fields, ensure required parameters are present
        if field.field_type in ['ForeignKey', 'OneToOneField']:
            # Add on_delete parameter (required for ForeignKey/OneToOneField)
            if field.on_delete:
                on_delete_node = self._create_on_delete_ast(field.on_delete)
                keywords.append(ast.keyword(arg='on_delete', value=on_delete_node))
            else:
                # Choose appropriate default based on field characteristics
                # Use SET_NULL for nullable fields, CASCADE for required fields
                if field.null and not field.primary_key:
                    default_behavior = 'SET_NULL'
                else:
                    default_behavior = 'CASCADE'

                default_on_delete = ast.Attribute(
                    value=ast.Attribute(
                        value=ast.Attribute(
                            value=ast.Attribute(
                                value=ast.Name(id='django', ctx=ast.Load()),
                                attr='db', ctx=ast.Load()),
                            attr='models', ctx=ast.Load()),
                        attr='deletion', ctx=ast.Load()),
                    attr=default_behavior, ctx=ast.Load())
                keywords.append(ast.keyword(arg='on_delete', value=default_on_delete))

            # Add to parameter (required for ForeignKey/OneToOneField)
            if field.foreign_key_to:
                keywords.append(ast.keyword(arg='to', value=ast.Constant(value="pybirdai."+field.foreign_key_to.lower())))
            else:
                # Try to infer from field name or use a placeholder
                inferred_to = f'pybirdai.{field.name.lower().replace("_", "")}'
                keywords.append(ast.keyword(arg='to', value=ast.Constant(value="pybirdai."+inferred_to.lower())))
        else:
            # Add on_delete for other field types if present
            if field.on_delete:
                on_delete_node = self._create_on_delete_ast(field.on_delete)
                keywords.append(ast.keyword(arg='on_delete', value=on_delete_node))

        # Add primary_key parameter
        if field.primary_key:
            keywords.append(ast.keyword(arg='primary_key', value=ast.Constant(value=True)))

        # Always explicitly set serialize=False for primary keys
        if field.primary_key or not field.serialize:
            keywords.append(ast.keyword(arg='serialize', value=ast.Constant(value=False)))

        # Add to parameter for non-ForeignKey fields if present
        if field.foreign_key_to and field.field_type not in ['ForeignKey', 'OneToOneField']:
            keywords.append(ast.keyword(arg='to', value=ast.Constant(value="pybirdai."+field.foreign_key_to.lower())))

        # Add verbose_name as named parameter
        if field.verbose_name:
            keywords.append(ast.keyword(arg='verbose_name', value=ast.Constant(value=field.verbose_name)))

        # Add other parameters
        if field.choices:
            choices_list = []
            for k, v in field.choices:
                choice_tuple = ast.Tuple(elts=[ast.Constant(value=k), ast.Constant(value=v)], ctx=ast.Load())
                choices_list.append(choice_tuple)
            keywords.append(ast.keyword(arg='choices', value=ast.List(elts=choices_list, ctx=ast.Load())))

        if field.db_comment:
            keywords.append(ast.keyword(arg='db_comment', value=ast.Constant(value=field.db_comment)))

        if field.related_name:
            keywords.append(ast.keyword(arg='related_name', value=ast.Constant(value=field.related_name)))

        if field.unique:
            keywords.append(ast.keyword(arg='unique', value=ast.Constant(value=True)))

        if field.db_index:
            keywords.append(ast.keyword(arg='db_index', value=ast.Constant(value=True)))

        if field.help_text:
            keywords.append(ast.keyword(arg='help_text', value=ast.Constant(value=field.help_text)))

        # Create field call with no positional arguments, only named parameters
        field_call = ast.Call(
            func=ast.Attribute(value=ast.Name(id='models', ctx=ast.Load()), attr=field.field_type, ctx=ast.Load()),
            args=[],  # No positional arguments
            keywords=keywords
        )

        return field_call

    def _create_on_delete_ast(self, on_delete_str: str) -> ast.AST:
        """Create AST node for on_delete parameter."""
        # Handle complete on_delete paths like 'django.db.models.deletion.CASCADE'
        if 'django.db.models.deletion' in on_delete_str:
            # Extract the specific deletion behavior (CASCADE, SET_NULL, etc.)
            parts = on_delete_str.split('.')
            deletion_behavior = parts[-1] if len(parts) > 1 else 'CASCADE'

            # Create the full AST: django.db.models.deletion.CASCADE
            return ast.Attribute(
                value=ast.Attribute(
                    value=ast.Attribute(
                        value=ast.Attribute(
                            value=ast.Name(id='django', ctx=ast.Load()),
                            attr='db', ctx=ast.Load()),
                        attr='models', ctx=ast.Load()),
                    attr='deletion', ctx=ast.Load()),
                attr=deletion_behavior, ctx=ast.Load())
        elif on_delete_str in ['CASCADE', 'SET_NULL', 'PROTECT', 'SET_DEFAULT', 'DO_NOTHING']:
            # Handle bare deletion behaviors - add full path
            return ast.Attribute(
                value=ast.Attribute(
                    value=ast.Attribute(
                        value=ast.Attribute(
                            value=ast.Name(id='django', ctx=ast.Load()),
                            attr='db', ctx=ast.Load()),
                        attr='models', ctx=ast.Load()),
                    attr='deletion', ctx=ast.Load()),
                attr=on_delete_str, ctx=ast.Load())
        else:
            # Try to parse the existing format
            parts = on_delete_str.split('.')
            if len(parts) == 1:
                # Single part - assume it's a deletion behavior
                return ast.Attribute(
                    value=ast.Attribute(
                        value=ast.Attribute(
                            value=ast.Attribute(
                                value=ast.Name(id='django', ctx=ast.Load()),
                                attr='db', ctx=ast.Load()),
                            attr='models', ctx=ast.Load()),
                        attr='deletion', ctx=ast.Load()),
                    attr=parts[0], ctx=ast.Load())
            else:
                # Multiple parts - reconstruct as attribute chain
                node = ast.Name(id=parts[0], ctx=ast.Load())
                for part in parts[1:]:
                    node = ast.Attribute(value=node, attr=part, ctx=ast.Load())
                return node

    def _generate_options_dict(self, model: ModelInfo) -> ast.Dict:
        """Generate model options dictionary as AST."""
        keys = []
        values = []

        if model.verbose_name:
            keys.append(ast.Constant(value='verbose_name'))
            values.append(ast.Constant(value=model.verbose_name))

        if model.verbose_name_plural:
            keys.append(ast.Constant(value='verbose_name_plural'))
            values.append(ast.Constant(value=model.verbose_name_plural))

        if model.db_table:
            keys.append(ast.Constant(value='db_table'))
            values.append(ast.Constant(value=model.db_table))

        if model.ordering:
            keys.append(ast.Constant(value='ordering'))
            ordering_list = ast.List(elts=[ast.Constant(value=item) for item in model.ordering], ctx=ast.Load())
            values.append(ordering_list)

        return ast.Dict(keys=keys, values=values)

    def generate_migration_code(self, models: List[ModelInfo], migration_name: str = "0001_initial") -> str:
        """Generate migration code from model information."""
        migration_ast = self.generate_migration_ast(models, migration_name)
        return ast.unparse(ast.fix_missing_locations(migration_ast))

    def save_migration_file(self, models: List[ModelInfo], output_path: str, migration_name: str = "0001_initial"):
        """Save migration file to disk."""
        migration_code = self.generate_migration_code(models, migration_name)

        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(migration_code)

        print(f"Migration saved to: {output_path}")


def generate_migration_from_file(source_file: str, output_file: str = "0001_initial.py"):
    """
    Convenience function to generate migration from a single source file.

    Args:
        source_file: Path to the Python file containing model definitions
        output_file: Path for the output migration file
    """
    generator = AdvancedMigrationGenerator()
    models = generator.parse_file(source_file)
    generator.save_migration_file(models, output_file)


def generate_migration_from_files(source_files: List[str], output_file: str = "0001_initial.py"):
    """
    Convenience function to generate migration from multiple source files.

    Args:
        source_files: List of paths to Python files containing model definitions
        output_file: Path for the output migration file
    """
    generator = AdvancedMigrationGenerator()
    models = generator.parse_files(source_files)
    generator.save_migration_file(models, output_file)


def generate_migration_from_directory(source_dir: str, output_file: str = "0001_initial.py"):
    """
    Convenience function to generate migration from all Python files in a directory.

    Args:
        source_dir: Path to the directory containing Python files with model definitions
        output_file: Path for the output migration file
    """
    generator = AdvancedMigrationGenerator()
    models = generator.parse_directory(source_dir)
    generator.save_migration_file(models, output_file)


# Example usage
if __name__ == '__main__':
    # Example: Generate migration from the initial_migration_generation_script.py
    import sys

    if len(sys.argv) > 1:
        source_path = sys.argv[1]
        output_path = sys.argv[2] if len(sys.argv) > 2 else "0001_initial.py"

        print(f"Generating migration from: {source_path}")
        print(f"Output file: {output_path}")

        generator = AdvancedMigrationGenerator()

        if os.path.isfile(source_path):
            models = generator.parse_file(source_path)
        elif os.path.isdir(source_path):
            models = generator.parse_directory(source_path)
        else:
            print(f"Invalid source path: {source_path}")
            sys.exit(1)

        print(f"Found {len(models)} models:")
        for model in models:
            parent_info = f" (inherits from {model.parent_model})" if model.parent_model else ""
            print(f"  - {model.name}{parent_info} with {len(model.fields)} fields")

        generator.save_migration_file(models, output_path)
    else:
        print("Usage: python advanced_migration_generator.py <source_file_or_directory> [output_file]")
        print("Example: python advanced_migration_generator.py models.py 0001_initial.py")
    generator = AdvancedMigrationGenerator()

    models = generator.parse_files(["pybirdai/bird_data_model.py", "pybirdai/bird_meta_data_model.py"])

    # Generate migration code
    migration_code = generator.generate_migration_code(models)
    generator.save_migration_file(models, "pybirdai/migrations/0001_initial.py")
