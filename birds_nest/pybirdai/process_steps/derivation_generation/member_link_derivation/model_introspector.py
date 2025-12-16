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
"""
Django Model Introspector using AST.

This module parses Django model files using Python's AST module to extract:
- Table/class names
- Fields per table (CharField, BigIntegerField, etc.)
- ForeignKey relationships with their related_names

This enables dynamic generation of:
- TABLE.FIELD lineage dependencies
- Django ORM navigation paths for cross-table field access
"""

import ast
import os
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple


@dataclass
class ForeignKeyInfo:
    """Information about a ForeignKey relationship.

    Attributes:
        field_name: The name of the FK field (e.g., 'theINSTRMNT')
        target_table: The target table this FK points to (e.g., 'INSTRMNT')
        related_name: The reverse relation name (e.g., 'INSTRMNT_RL_to_theINSTRMNTs')
    """
    field_name: str
    target_table: str
    related_name: str


@dataclass
class TableInfo:
    """Information about a Django model/table.

    Attributes:
        name: The table/class name
        fields: Set of field names (non-FK fields)
        foreign_keys: List of ForeignKeyInfo objects
    """
    name: str
    fields: Set[str] = field(default_factory=set)
    foreign_keys: List[ForeignKeyInfo] = field(default_factory=list)


class ModelIntrospector:
    """Parses Django models.py files using AST to extract table structure.

    This class builds a complete picture of the data model including:
    - All tables and their fields
    - Forward FK relationships (table -> target via FK field)
    - Reverse FK relationships (table <- source via related_name)

    It can then compute navigation paths between tables to access
    fields from related tables.
    """

    def __init__(self, models_path: str):
        """Initialize the introspector with a models.py file path.

        Args:
            models_path: Path to the Django models.py file to parse
        """
        self.models_path = models_path

        # Tables indexed by name
        self.tables: Dict[str, TableInfo] = {}

        # Maps variable name -> set of tables that have that variable
        self.variable_to_tables: Dict[str, Set[str]] = defaultdict(set)

        # Forward relations: source_table -> {fk_field: (target_table, related_name)}
        self.forward_relations: Dict[str, Dict[str, Tuple[str, str]]] = defaultdict(dict)

        # Reverse relations: target_table -> {related_name: (source_table, fk_field)}
        self.reverse_relations: Dict[str, Dict[str, Tuple[str, str]]] = defaultdict(dict)

        # Parse the models file
        self._parse_models()

    def _parse_models(self):
        """Parse the models.py file using AST."""
        if not os.path.exists(self.models_path):
            raise FileNotFoundError(f"Models file not found: {self.models_path}")

        with open(self.models_path, 'r', encoding='utf-8') as f:
            source = f.read()

        tree = ast.parse(source)

        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                # Check if it's a Django model (inherits from models.Model)
                if self._is_django_model(node):
                    self._process_model_class(node)

    def _is_django_model(self, class_node: ast.ClassDef) -> bool:
        """Check if a class inherits from models.Model."""
        for base in class_node.bases:
            if isinstance(base, ast.Attribute):
                if base.attr == 'Model':
                    return True
            elif isinstance(base, ast.Name):
                if base.id == 'Model':
                    return True
        return False

    def _process_model_class(self, class_node: ast.ClassDef):
        """Process a Django model class to extract fields and relationships."""
        table_name = class_node.name
        table_info = TableInfo(name=table_name)

        for item in class_node.body:
            if isinstance(item, ast.Assign):
                # Handle field assignments like: field_name = models.CharField(...)
                for target in item.targets:
                    if isinstance(target, ast.Name):
                        field_name = target.id

                        # Skip domain dictionaries and Meta class attributes
                        if field_name.endswith('_domain'):
                            continue

                        # Check if it's a ForeignKey
                        fk_info = self._extract_foreign_key(item.value, field_name)
                        if fk_info:
                            table_info.foreign_keys.append(fk_info)
                            # Register forward and reverse relations
                            self.forward_relations[table_name][fk_info.field_name] = (
                                fk_info.target_table, fk_info.related_name
                            )
                            self.reverse_relations[fk_info.target_table][fk_info.related_name] = (
                                table_name, fk_info.field_name
                            )
                        else:
                            # Regular field
                            if self._is_model_field(item.value):
                                table_info.fields.add(field_name)
                                self.variable_to_tables[field_name].add(table_name)

        self.tables[table_name] = table_info

    def _extract_foreign_key(self, value_node: ast.AST, field_name: str) -> Optional[ForeignKeyInfo]:
        """Extract ForeignKey information from an assignment value."""
        if not isinstance(value_node, ast.Call):
            return None

        # Check if it's models.ForeignKey(...)
        func = value_node.func
        if isinstance(func, ast.Attribute):
            if func.attr != 'ForeignKey':
                return None
        else:
            return None

        # Extract target table from first positional argument
        target_table = None
        if value_node.args:
            first_arg = value_node.args[0]
            if isinstance(first_arg, ast.Constant):
                target_table = first_arg.value
            elif isinstance(first_arg, ast.Str):  # Python 3.7 compatibility
                target_table = first_arg.s

        if not target_table:
            return None

        # Extract related_name from keyword arguments
        related_name = None
        for keyword in value_node.keywords:
            if keyword.arg == 'related_name':
                if isinstance(keyword.value, ast.Constant):
                    related_name = keyword.value.value
                elif isinstance(keyword.value, ast.Str):  # Python 3.7 compatibility
                    related_name = keyword.value.s

        if not related_name:
            # Generate default related_name if not specified
            related_name = f"{field_name}_set"

        return ForeignKeyInfo(
            field_name=field_name,
            target_table=target_table,
            related_name=related_name
        )

    def _is_model_field(self, value_node: ast.AST) -> bool:
        """Check if a value is a Django model field (CharField, BigIntegerField, etc.)."""
        if not isinstance(value_node, ast.Call):
            return False

        func = value_node.func
        if isinstance(func, ast.Attribute):
            # Check for models.CharField, models.BigIntegerField, etc.
            field_types = {
                'CharField', 'BigIntegerField', 'IntegerField', 'FloatField',
                'DateTimeField', 'DateField', 'BooleanField', 'TextField',
                'DecimalField', 'SmallIntegerField', 'PositiveIntegerField'
            }
            return func.attr in field_types

        return False

    def find_variable_table(self, variable_name: str) -> Optional[str]:
        """Find the primary table that contains a variable.

        If the variable exists in multiple tables, this returns the first
        match (alphabetically). Use find_all_variable_tables() for all matches.

        Args:
            variable_name: The variable/field name to look up

        Returns:
            The table name, or None if not found
        """
        tables = self.variable_to_tables.get(variable_name)
        if tables:
            return sorted(tables)[0]
        return None

    def find_all_variable_tables(self, variable_name: str) -> Set[str]:
        """Find all tables that contain a variable.

        Args:
            variable_name: The variable/field name to look up

        Returns:
            Set of table names that contain this variable
        """
        return self.variable_to_tables.get(variable_name, set())

    def find_path(self, from_table: str, to_table: str) -> Optional[List[Tuple[str, str, str]]]:
        """Find a navigation path from one table to another using BFS.

        The path is returned as a list of (relation_type, relation_name, table) tuples:
        - relation_type: 'forward' (FK field) or 'reverse' (related_name)
        - relation_name: The FK field name or related_name
        - table: The table reached via this relation

        Args:
            from_table: Starting table name
            to_table: Target table name

        Returns:
            List of navigation steps, or None if no path exists
        """
        if from_table == to_table:
            return []

        if from_table not in self.tables or to_table not in self.tables:
            return None

        # BFS to find shortest path
        from collections import deque

        # Queue entries: (current_table, path_so_far)
        queue = deque([(from_table, [])])
        visited = {from_table}

        while queue:
            current, path = queue.popleft()

            # Try forward relations (FK fields)
            for fk_field, (target, related_name) in self.forward_relations.get(current, {}).items():
                if target not in visited:
                    new_path = path + [('forward', fk_field, target)]
                    if target == to_table:
                        return new_path
                    visited.add(target)
                    queue.append((target, new_path))

            # Try reverse relations (related_names)
            for related_name, (source, fk_field) in self.reverse_relations.get(current, {}).items():
                if source not in visited:
                    new_path = path + [('reverse', related_name, source)]
                    if source == to_table:
                        return new_path
                    visited.add(source)
                    queue.append((source, new_path))

        return None

    def generate_accessor(
        self,
        from_table: str,
        variable_name: str,
        variable_table: Optional[str] = None
    ) -> Tuple[str, str]:
        """Generate Django ORM accessor code for a variable.

        This generates the Python code needed to access a variable from
        a starting table, navigating through relationships as needed.

        Args:
            from_table: The starting table (where the derivation is defined)
            variable_name: The variable/field to access
            variable_table: The table containing the variable (auto-detected if None)

        Returns:
            Tuple of (accessor_code, lineage_dependency):
            - accessor_code: Python code to access the variable (e.g., 'self.FIELD')
            - lineage_dependency: TABLE.FIELD format for @lineage decorator
        """
        # Find the table containing the variable
        if variable_table is None:
            variable_table = self.find_variable_table(variable_name)

        if variable_table is None:
            # Variable not found - return default accessor
            return f"self.{variable_name}", variable_name

        # Generate lineage dependency in TABLE.FIELD format
        lineage_dep = f"{variable_table}.{variable_name}"

        # If same table, direct access
        if from_table == variable_table:
            return f"self.{variable_name}", lineage_dep

        # Find path from from_table to variable_table
        path = self.find_path(from_table, variable_table)

        if path is None:
            # No path found - return accessor with comment
            return f"self.{variable_name}  # WARNING: No path found to {variable_table}", lineage_dep

        # Build accessor code
        accessor = "self"
        for relation_type, relation_name, _ in path:
            if relation_type == 'forward':
                # Forward FK: use the FK field directly
                accessor = f"{accessor}.{relation_name}"
            else:
                # Reverse relation: use related_name with .all() or .first()
                accessor = f"{accessor}.{relation_name}.first()"

        # Add final field access
        accessor = f"{accessor}.{variable_name} if {accessor.rsplit('.', 1)[0]} else None"

        return accessor, lineage_dep

    def generate_loop_accessor(
        self,
        from_table: str,
        variable_name: str,
        variable_table: Optional[str] = None
    ) -> Tuple[str, str, str]:
        """Generate accessor code that iterates over related records.

        This is used when checking multiple related records for a matching value.

        Args:
            from_table: The starting table
            variable_name: The variable to access
            variable_table: The table containing the variable

        Returns:
            Tuple of (loop_code, accessor_in_loop, lineage_dependency):
            - loop_code: The for loop header (e.g., 'for item in self.RELATION.all():')
            - accessor_in_loop: How to access the variable inside the loop
            - lineage_dependency: TABLE.FIELD format
        """
        if variable_table is None:
            variable_table = self.find_variable_table(variable_name)

        if variable_table is None:
            return "", f"self.{variable_name}", variable_name

        lineage_dep = f"{variable_table}.{variable_name}"

        if from_table == variable_table:
            return "", f"self.{variable_name}", lineage_dep

        path = self.find_path(from_table, variable_table)

        if path is None or len(path) == 0:
            return "", f"self.{variable_name}", lineage_dep

        # For single-step reverse relations, generate a loop
        if len(path) == 1 and path[0][0] == 'reverse':
            related_name = path[0][1]
            loop_var = related_name.split('_')[0].lower()
            loop_code = f"for {loop_var} in self.{related_name}.all():"
            accessor = f"{loop_var}.{variable_name}"
            return loop_code, accessor, lineage_dep

        # For multi-step paths, generate nested loops or use first()
        # For simplicity, use first() for intermediate steps
        accessor = "self"
        for i, (relation_type, relation_name, target) in enumerate(path):
            if relation_type == 'reverse':
                if i == len(path) - 1:
                    # Last step - generate loop
                    loop_var = relation_name.split('_')[0].lower()
                    loop_code = f"for {loop_var} in {accessor}.{relation_name}.all():"
                    final_accessor = f"{loop_var}.{variable_name}"
                    return loop_code, final_accessor, lineage_dep
                else:
                    accessor = f"{accessor}.{relation_name}.first()"
            else:
                accessor = f"{accessor}.{relation_name}"

        # If we get here, the last step was forward
        return "", f"{accessor}.{variable_name} if {accessor} else None", lineage_dep

    def get_table_fields(self, table_name: str) -> Set[str]:
        """Get all non-FK fields for a table.

        Args:
            table_name: The table name

        Returns:
            Set of field names
        """
        table = self.tables.get(table_name)
        return table.fields if table else set()

    def get_table_foreign_keys(self, table_name: str) -> List[ForeignKeyInfo]:
        """Get all ForeignKey relationships for a table.

        Args:
            table_name: The table name

        Returns:
            List of ForeignKeyInfo objects
        """
        table = self.tables.get(table_name)
        return table.foreign_keys if table else []

    def get_reverse_relations(self, table_name: str) -> Dict[str, Tuple[str, str]]:
        """Get all reverse relations pointing to a table.

        Args:
            table_name: The table name

        Returns:
            Dict mapping related_name to (source_table, fk_field)
        """
        return dict(self.reverse_relations.get(table_name, {}))


def create_introspector(models_path: str = None) -> ModelIntrospector:
    """Create a ModelIntrospector with default or specified path.

    Args:
        models_path: Path to models.py, or None for default location

    Returns:
        Initialized ModelIntrospector
    """
    if models_path is None:
        # Default to database_configuration_files/models.py
        models_path = "results/database_configuration_files/models.py"

    return ModelIntrospector(models_path)
