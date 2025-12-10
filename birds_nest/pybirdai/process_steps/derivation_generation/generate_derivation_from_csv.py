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
Derivation Code Generator

This module generates Python derivation files from ECB logical transformation rules.
It parses the sddlogicaltransformationrule CSV, filters for DER-type rules,
and generates Python files with @lineage decorated properties for each class.
"""

import csv
import os
import re
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class TransformationRule:
    """Represents a single logical transformation rule from the ECB CSV."""
    rule_id: str
    semantic_id: str
    algorithm: str
    additional_filters: str
    source_layer: str
    destination_layer: str
    transformation_type: str
    valid_from: str
    valid_to: str

    # Parsed fields
    target_class: str = ""
    target_field: str = ""
    dependencies: set = field(default_factory=set)

    def __post_init__(self):
        """Parse the rule to extract target class, field, and dependencies."""
        self._parse_rule_id()
        self._parse_algorithm_dependencies()

    def _parse_rule_id(self):
        """Extract target class and field from the rule ID.

        Rule ID format: DER__BIRD_<CLASS>_<LAYER>_1__<FIELD>__<DATE>
        Example: DER__BIRD_INSTRMNT_RL_EIL_1__ACCMLTD_TTL_WRTFFS__01_01_1990
        """
        if not self.rule_id.startswith("DER__"):
            return

        # Remove DER__ prefix
        parts = self.rule_id[5:].split("__")
        if len(parts) >= 2:
            # First part contains class info with BIRD_ prefix and layer suffix
            class_part = parts[0]
            self.target_field = parts[1] if len(parts) > 1 else ""

            # Extract class name: remove BIRD_ prefix and _1 suffix, keep layer
            self.target_class = self._normalize_class_name(class_part)

    def _normalize_class_name(self, class_name: str) -> str:
        """Normalize class name by removing BIRD_ prefix and version suffix."""
        # Remove BIRD_ prefix
        if class_name.startswith("BIRD_"):
            class_name = class_name[5:]

        # Remove trailing _1, _2, etc. version numbers
        if re.match(r'.*_\d+$', class_name):
            class_name = re.sub(r'_\d+$', '', class_name)

        return class_name

    def _parse_algorithm_dependencies(self):
        """Extract field dependencies from the algorithm."""
        if not self.algorithm:
            return

        # Find all field references in the form BIRD_TABLE.FIELD or TABLE.FIELD
        pattern = r'BIRD_([A-Z_]+)\.([A-Z_]+)|([A-Z_]+)\.([A-Z_]+)'
        matches = re.findall(pattern, self.algorithm)

        for match in matches:
            if match[0] and match[1]:
                # BIRD_TABLE.FIELD format
                table = self._normalize_class_name(f"BIRD_{match[0]}")
                field_name = match[1]
            elif match[2] and match[3]:
                # TABLE.FIELD format
                table = self._normalize_class_name(match[2])
                field_name = match[3]
            else:
                continue

            # Add as dependency
            self.dependencies.add(f"{table}.{field_name}")


@dataclass
class DerivationConfig:
    """Configuration for which derived fields to generate for which classes."""
    class_name: str
    field_name: str
    enabled: bool
    notes: str = ""


def load_transformation_rules_csv(csv_path: str) -> list[TransformationRule]:
    """Load and parse the logical transformation rules CSV.

    Args:
        csv_path: Path to the sddlogicaltransformationrule.csv file.

    Returns:
        List of TransformationRule objects, filtered for DER type only.
    """
    rules = []

    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Only process DER (derived) type rules
            if row.get('TRANSFORMATION_TYPE') != 'DER':
                continue

            rule = TransformationRule(
                rule_id=row.get('LOGICAL_TRANSFORMATION_RULE_ID', ''),
                semantic_id=row.get('SEMANTIC_TRANSFORMATION_RULE_ID', ''),
                algorithm=row.get('ALGORITHM', ''),
                additional_filters=row.get('ADDITIONAL_FILTERS', ''),
                source_layer=row.get('SOURCE_LAYER', ''),
                destination_layer=row.get('DESTINATION_LAYER', ''),
                transformation_type=row.get('TRANSFORMATION_TYPE', ''),
                valid_from=row.get('VALID_FROM', ''),
                valid_to=row.get('VALID_TO', ''),
            )
            rules.append(rule)

    return rules


def load_derivation_config(config_path: str) -> list[DerivationConfig]:
    """Load the derivation configuration CSV.

    Args:
        config_path: Path to the derivation_config.csv file.

    Returns:
        List of DerivationConfig objects.
    """
    configs = []

    with open(config_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Skip comment rows (starting with #)
            class_name = row.get('class_name', '').strip()
            if class_name.startswith('#') or not class_name:
                continue

            config = DerivationConfig(
                class_name=class_name,
                field_name=row.get('field_name', '').strip(),
                enabled=row.get('enabled', '').strip().lower() == 'true',
                notes=row.get('notes', '').strip(),
            )
            configs.append(config)

    return configs


class DerivationCodeGenerator:
    """Generates Python derivation code from transformation rules."""

    # Template for the file header
    FILE_HEADER = '''# coding=UTF-8
# Copyright (c) 2025 Arfa Digital Consulting
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDX-License-Identifier: EPL-2.0
#
# Contributors:
#    Auto-generated from ECB logical transformation rules
#
"""
Auto-generated derivation fields for {class_name}.

This file was generated from ECB sddlogicaltransformationrule data.
Manual edits may be overwritten when regenerating.
"""

from django.db import models
from pybirdai.annotations.decorators import lineage

'''

    def __init__(self, output_dir: str = "resources/derivation_files/generated"):
        """Initialize the generator.

        Args:
            output_dir: Directory where generated Python files will be written.
        """
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)

    def generate_for_class(self, class_name: str, rules: list[TransformationRule]) -> str:
        """Generate Python code for a single class.

        Args:
            class_name: The name of the class.
            rules: List of transformation rules for this class.

        Returns:
            The generated Python code as a string.
        """
        code_lines = [self.FILE_HEADER.format(class_name=class_name)]

        # Start class definition
        code_lines.append(f"class {class_name}(models.Model):\n")
        code_lines.append('    """Auto-generated derived fields."""\n\n')

        for rule in rules:
            # Generate property for each rule
            property_code = self._generate_property(rule)
            code_lines.append(property_code)

        # Add Meta class
        code_lines.append("    class Meta:\n")
        code_lines.append("        pass\n")

        return "".join(code_lines)

    def _generate_property(self, rule: TransformationRule) -> str:
        """Generate a single property method from a transformation rule.

        Args:
            rule: The transformation rule.

        Returns:
            Python code for the property method.
        """
        lines = []

        # Format dependencies for decorator
        deps_str = ", ".join(f"'{dep}'" for dep in sorted(rule.dependencies))

        # Add decorators
        lines.append("    @property\n")
        lines.append(f"    @lineage(dependencies={{{deps_str}}})\n")

        # Add method definition
        lines.append(f"    def {rule.target_field}(self):\n")

        # Add docstring with original algorithm
        algorithm_escaped = rule.algorithm.replace('"""', "'''")
        lines.append(f'        """\n')
        lines.append(f'        Auto-generated from rule: {rule.semantic_id}\n')
        lines.append(f'        Source: {rule.source_layer} -> {rule.destination_layer}\n')
        lines.append(f'        \n')
        lines.append(f'        Original algorithm:\n')
        for alg_line in algorithm_escaped.split('\n'):
            lines.append(f'        {alg_line}\n')
        lines.append(f'        """\n')

        # Generate Python code from algorithm
        python_code = self._algorithm_to_python(rule)
        for code_line in python_code.split('\n'):
            if code_line.strip():
                lines.append(f"        {code_line}\n")

        lines.append("\n")
        return "".join(lines)

    def _algorithm_to_python(self, rule: TransformationRule) -> str:
        """Convert SQL-like algorithm to Python code.

        This method parses CASE WHEN statements and converts them to Python match/case
        or if/elif/else statements.

        Args:
            rule: The transformation rule containing the algorithm.

        Returns:
            Python code implementing the algorithm.
        """
        algorithm = rule.algorithm.strip()

        if not algorithm:
            return "return None  # No algorithm defined"

        # Parse SET statement
        set_match = re.match(r'SET\s+[\w\.]+\s*=\s*(.+)', algorithm, re.DOTALL | re.IGNORECASE)
        if not set_match:
            # Can't parse, return placeholder
            return self._generate_placeholder(rule)

        expression = set_match.group(1).strip()

        # Remove trailing semicolon
        expression = expression.rstrip(';').strip()

        # Try to parse CASE expression
        if 'CASE' in expression.upper():
            return self._parse_case_expression(expression, rule)
        else:
            # Simple expression
            return self._convert_expression(expression, rule)

    def _parse_case_expression(self, expression: str, rule: TransformationRule) -> str:
        """Parse a CASE WHEN expression and convert to Python.

        Args:
            expression: The CASE WHEN expression.
            rule: The transformation rule for context.

        Returns:
            Python code implementing the CASE expression.
        """
        lines = []

        # Simple tokenization approach for CASE WHEN THEN ELSE END
        # This handles the basic structure but may need enhancement for complex cases

        # Extract WHEN clauses
        when_pattern = r'WHEN\s+(.+?)\s+THEN\s+(.+?)(?=\s+WHEN|\s+ELSE|\s+END)'
        else_pattern = r'ELSE\s+(.+?)\s+END'

        when_matches = re.findall(when_pattern, expression, re.DOTALL | re.IGNORECASE)
        else_match = re.search(else_pattern, expression, re.DOTALL | re.IGNORECASE)

        if not when_matches:
            # Can't parse, return placeholder
            return self._generate_placeholder(rule)

        # Generate if/elif/else structure
        for i, (condition, result) in enumerate(when_matches):
            condition_py = self._convert_condition(condition.strip(), rule)
            result_py = self._convert_expression(result.strip(), rule)

            if i == 0:
                lines.append(f"if {condition_py}:")
            else:
                lines.append(f"elif {condition_py}:")
            lines.append(f"    return {result_py}")

        # Handle ELSE clause
        if else_match:
            else_value = else_match.group(1).strip()
            else_py = self._convert_expression(else_value, rule)
            lines.append(f"else:")
            lines.append(f"    return {else_py}")
        else:
            lines.append(f"else:")
            lines.append(f"    return None")

        return "\n".join(lines)

    def _convert_condition(self, condition: str, rule: TransformationRule) -> str:
        """Convert an SQL condition to Python.

        Args:
            condition: The SQL condition.
            rule: The transformation rule for context.

        Returns:
            Python condition expression.
        """
        py_condition = condition

        # Replace SQL operators with Python equivalents
        py_condition = re.sub(r'\bAND\b', 'and', py_condition, flags=re.IGNORECASE)
        py_condition = re.sub(r'\bOR\b', 'or', py_condition, flags=re.IGNORECASE)
        py_condition = re.sub(r'\bNOT\b', 'not', py_condition, flags=re.IGNORECASE)
        py_condition = re.sub(r'\bIS\s+NULL\b', '== None', py_condition, flags=re.IGNORECASE)
        py_condition = re.sub(r'\bIS\s+NOT\s+NULL\b', '!= None', py_condition, flags=re.IGNORECASE)
        py_condition = py_condition.replace('<>', '!=')

        # Handle IN clause
        in_match = re.search(r'(\S+)\s+IN\s*\(([^)]+)\)', py_condition, re.IGNORECASE)
        if in_match:
            field = in_match.group(1)
            values = in_match.group(2)
            py_condition = re.sub(
                r'(\S+)\s+IN\s*\([^)]+\)',
                f'{field} in ({values})',
                py_condition,
                flags=re.IGNORECASE
            )

        # Convert field references
        py_condition = self._convert_field_references(py_condition, rule)

        return py_condition

    def _convert_expression(self, expression: str, rule: TransformationRule) -> str:
        """Convert an SQL expression to Python.

        Args:
            expression: The SQL expression.
            rule: The transformation rule for context.

        Returns:
            Python expression.
        """
        py_expr = expression.strip()

        # Handle N/A or NULL
        if py_expr.upper() in ('N/A', 'NULL', "'N/A'"):
            return "None"

        # Handle nested CASE (simplified - may need enhancement)
        if 'CASE' in py_expr.upper():
            # For nested CASE, generate a helper comment
            return "None  # TODO: Nested CASE needs manual implementation"

        # Convert field references
        py_expr = self._convert_field_references(py_expr, rule)

        # Handle date functions (simplified)
        py_expr = re.sub(
            r'DATEADD\s*\(\s*(\w+)\s*,\s*(-?\d+)\s*,\s*([^)]+)\)',
            r'\3  # TODO: Implement DATEADD(\1, \2)',
            py_expr,
            flags=re.IGNORECASE
        )

        return py_expr

    def _convert_field_references(self, text: str, rule: TransformationRule) -> str:
        """Convert BIRD_TABLE.FIELD references to self.TABLE.FIELD.

        Args:
            text: Text containing field references.
            rule: The transformation rule for context.

        Returns:
            Text with converted field references.
        """
        # Replace BIRD_TABLE_IL.FIELD with self.TABLE.FIELD
        # and BIRD_TABLE_EIL.FIELD with self.TABLE.FIELD

        def replace_ref(match):
            full_table = match.group(1)
            field_name = match.group(2)

            # Normalize table name
            table = full_table
            if table.startswith("BIRD_"):
                table = table[5:]

            # Remove layer suffix for property access
            table = re.sub(r'_IL$|_EIL$', '', table)

            return f"self.{table}.{field_name}"

        result = re.sub(
            r'(BIRD_[A-Z_]+)\s*\.\s*([A-Z_]+)',
            replace_ref,
            text
        )

        return result

    def _generate_placeholder(self, rule: TransformationRule) -> str:
        """Generate placeholder code when algorithm can't be parsed.

        Args:
            rule: The transformation rule.

        Returns:
            Placeholder Python code with TODO comment.
        """
        return f"# TODO: Implement derivation logic from rule {rule.semantic_id}\nreturn None"

    def write_class_file(self, class_name: str, code: str) -> str:
        """Write generated code to a file.

        Args:
            class_name: The name of the class.
            code: The generated Python code.

        Returns:
            Path to the written file.
        """
        file_name = f"{class_name}_derived.py"
        file_path = os.path.join(self.output_dir, file_name)

        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(code)

        return file_path


def generate_all_derivation_files(
    transformation_rules_csv: str,
    output_dir: str = "resources/derivation_files/generated"
) -> dict[str, str]:
    """Generate all derivation files from transformation rules.

    This generates Python files for ALL DER-type rules in the CSV,
    regardless of the config. The config is used later during merge
    to determine which derived fields to include in the bird data model.

    Args:
        transformation_rules_csv: Path to logical_transformation_rule.csv.
        output_dir: Directory for generated files.

    Returns:
        Dictionary mapping class names to generated file paths.
    """
    # Load all rules
    rules = load_transformation_rules_csv(transformation_rules_csv)

    # Group rules by class (generate ALL, not filtered by config)
    rules_by_class = {}
    for rule in rules:
        if not rule.target_class or not rule.target_field:
            continue

        # Normalize class name (remove _EIL/_IL suffix)
        normalized_class = re.sub(r'_IL$|_EIL$', '', rule.target_class)

        if normalized_class not in rules_by_class:
            rules_by_class[normalized_class] = []
        rules_by_class[normalized_class].append(rule)

    # Generate files for ALL classes
    generator = DerivationCodeGenerator(output_dir)
    generated_files = {}

    for class_name, class_rules in rules_by_class.items():
        code = generator.generate_for_class(class_name, class_rules)
        file_path = generator.write_class_file(class_name, code)
        generated_files[class_name] = file_path
        print(f"Generated: {file_path}")

    return generated_files


def get_available_derivation_rules(transformation_rules_csv: str) -> dict[str, list[str]]:
    """Get all available derivation rules grouped by class.

    This is useful for discovering what derived fields are available
    to configure in derivation_config.csv.

    Args:
        transformation_rules_csv: Path to sddlogicaltransformationrule.csv.

    Returns:
        Dictionary mapping class names to lists of available field names.
    """
    rules = load_transformation_rules_csv(transformation_rules_csv)

    available = {}
    for rule in rules:
        if not rule.target_class or not rule.target_field:
            continue

        # Normalize class name
        normalized_class = re.sub(r'_IL$|_EIL$', '', rule.target_class)

        if normalized_class not in available:
            available[normalized_class] = []

        if rule.target_field not in available[normalized_class]:
            available[normalized_class].append(rule.target_field)

    # Sort for consistent output
    for class_name in available:
        available[class_name].sort()

    return available
