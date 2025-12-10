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
from dateutil.relativedelta import relativedelta

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

        # Generate initialization for related table fields
        related_init_lines, field_mappings = self._generate_related_table_inits(rule)

        # Check for multi-statement algorithms with intermediate variables
        # Format: SET VAR = expr, BIRD_TARGET.FIELD = CASE WHEN VAR ...
        # Pass field_mappings so local var expressions use the local variables
        local_vars = self._extract_local_variables(algorithm, rule, field_mappings)

        # For multi-statement algorithms, find the target expression (after local vars)
        # Pattern: ..., BIRD_TABLE.FIELD = CASE WHEN ...
        if local_vars:
            # Find the last BIRD_TABLE.FIELD = expression (the target assignment)
            target_match = re.search(
                r',\s*BIRD_[A-Z_]+\.[A-Z_]+\s*=\s*(.+)',
                algorithm,
                re.DOTALL | re.IGNORECASE
            )
            if target_match:
                expression = target_match.group(1).strip().rstrip(';').strip()
            else:
                # Fallback to original parsing
                set_match = re.match(r'SET\s+[\w\.]+\s*=\s*(.+)', algorithm, re.DOTALL | re.IGNORECASE)
                if not set_match:
                    return self._generate_placeholder(rule)
                expression = set_match.group(1).strip().rstrip(';').strip()
        else:
            # Simple single-statement algorithm
            set_match = re.match(r'SET\s+[\w\.]+\s*=\s*(.+)', algorithm, re.DOTALL | re.IGNORECASE)
            if not set_match:
                return self._generate_placeholder(rule)
            expression = set_match.group(1).strip().rstrip(';').strip()

        # Build output: first related table inits, then local variables
        output_lines = []

        # Add related table field initializations
        if related_init_lines:
            output_lines.append("# Initialize fields from related tables")
            output_lines.extend(related_init_lines)
            output_lines.append("")  # Empty line for readability

        # Add extracted local variables (like TM_PST_DU) with null-safety
        for var_name, var_expr in local_vars.items():
            # Check if expression uses any potentially-None field mappings
            # Use set to avoid duplicates when a variable appears multiple times
            used_null_vars = list(set(v for v in field_mappings.values() if v in var_expr))
            if used_null_vars:
                # Add null check before arithmetic with potentially-None values
                null_checks = " and ".join([f"{v} is not None" for v in sorted(used_null_vars)])
                output_lines.append(f"if {null_checks}:")
                output_lines.append(f"    {var_name} = {var_expr}")
                output_lines.append("else:")
                output_lines.append("    return None")
            else:
                output_lines.append(f"{var_name} = {var_expr}")

        # Combine local var names with field mappings for conversion
        all_local_names = set(local_vars.keys())
        # Add the local variable names from field_mappings (e.g., 'prty_dflt_stts')
        all_local_names.update(field_mappings.values())

        # Try to parse CASE expression
        if 'CASE' in expression.upper():
            # Pass local variable names and field mappings
            case_code = self._parse_case_expression(
                expression, rule,
                local_var_names=all_local_names,
                field_mappings=field_mappings
            )
            if output_lines:
                return "\n".join(output_lines) + "\n" + case_code
            return case_code
        else:
            # Simple expression - ensure we have a return statement
            py_expr = self._convert_expression(expression, rule, set(), field_mappings)
            # Apply division safety for expressions with division
            py_expr = self._make_division_safe(py_expr)
            if output_lines:
                output_lines.append(f"return {py_expr}")
                return "\n".join(output_lines)
            return f"return {py_expr}"

    def _extract_local_variables(self, algorithm: str, rule: TransformationRule,
                                  field_mappings: dict = None) -> dict:
        """Extract intermediate variable assignments from multi-statement algorithms.

        Handles patterns like:
        SET TM_PST_DU = BIRD_X.A - BIRD_Y.B,
        BIRD_TARGET.FIELD = CASE WHEN TM_PST_DU ...

        Args:
            algorithm: The full algorithm text.
            rule: The transformation rule for context.
            field_mappings: Dict mapping 'TABLE.FIELD' to local variable names.

        Returns:
            Dictionary mapping variable names to their Python expressions.
        """
        local_vars = {}
        field_mappings = field_mappings or {}

        # Look for comma-separated SET statements with intermediate variables
        # Pattern: SET VARNAME = expression, followed by more content
        var_pattern = r'SET\s+([A-Z_]+)\s*=\s*([^,]+),\s*(?:BIRD_|SET)'

        matches = re.findall(var_pattern, algorithm, re.IGNORECASE)
        for var_name, var_expr in matches:
            # Only extract if var_name is NOT a BIRD table reference
            if not var_name.startswith('BIRD_') and '.' not in var_name:
                py_expr = self._convert_expression(var_expr.strip(), rule, set(), field_mappings)
                local_vars[var_name] = py_expr

        return local_vars

    def _parse_case_expression(self, expression: str, rule: TransformationRule, indent: int = 0,
                                local_var_names: set = None, field_mappings: dict = None) -> str:
        """Parse a CASE WHEN expression and convert to Python.

        Supports nested CASE statements by recursively parsing THEN clauses
        that contain CASE expressions.

        Args:
            expression: The CASE WHEN expression.
            rule: The transformation rule for context.
            indent: Current indentation level for nested cases.
            local_var_names: Set of local variable names to preserve (not convert to self.table.field).
            field_mappings: Dict mapping 'TABLE.FIELD' to local variable names.

        Returns:
            Python code implementing the CASE expression.
        """
        lines = []
        indent_str = "    " * indent
        local_var_names = local_var_names or set()
        field_mappings = field_mappings or {}

        # Extract WHEN clauses - improved pattern to handle nested CASE
        when_clauses = self._extract_when_clauses(expression)
        else_value = self._extract_else_clause(expression)

        if not when_clauses:
            # Can't parse, return placeholder
            return self._generate_placeholder(rule)

        # Generate if/elif/else structure
        for i, (condition, result) in enumerate(when_clauses):
            condition_py = self._convert_condition(condition.strip(), rule, local_var_names, field_mappings)

            # Check if the result contains a nested CASE
            result_stripped = result.strip()
            if 'CASE' in result_stripped.upper() and result_stripped.upper().startswith('CASE'):
                # Recursively parse nested CASE
                nested_code = self._parse_case_expression(result_stripped, rule, indent + 1, local_var_names, field_mappings)
                if i == 0:
                    lines.append(f"{indent_str}if {condition_py}:")
                else:
                    lines.append(f"{indent_str}elif {condition_py}:")
                # Indent the nested case code
                for nested_line in nested_code.split('\n'):
                    lines.append(f"{indent_str}    {nested_line}")
            else:
                result_py = self._convert_expression(result_stripped, rule, local_var_names, field_mappings)
                # Apply division safety for expressions with division
                result_py = self._make_division_safe(result_py)
                if i == 0:
                    lines.append(f"{indent_str}if {condition_py}:")
                else:
                    lines.append(f"{indent_str}elif {condition_py}:")
                lines.append(f"{indent_str}    return {result_py}")

        # Handle ELSE clause
        if else_value:
            else_stripped = else_value.strip()
            if 'CASE' in else_stripped.upper() and else_stripped.upper().startswith('CASE'):
                # Nested CASE in ELSE
                nested_code = self._parse_case_expression(else_stripped, rule, indent + 1, local_var_names, field_mappings)
                lines.append(f"{indent_str}else:")
                for nested_line in nested_code.split('\n'):
                    lines.append(f"{indent_str}    {nested_line}")
            else:
                else_py = self._convert_expression(else_stripped, rule, local_var_names, field_mappings)
                # Apply division safety for expressions with division
                else_py = self._make_division_safe(else_py)
                lines.append(f"{indent_str}else:")
                lines.append(f"{indent_str}    return {else_py}")
        else:
            lines.append(f"{indent_str}else:")
            lines.append(f"{indent_str}    return None")

        return "\n".join(lines)

    def _extract_when_clauses(self, expression: str) -> list:
        """Extract WHEN...THEN clauses from a CASE expression.

        Handles nested CASE by tracking parenthesis/CASE...END depth.

        Args:
            expression: The CASE expression.

        Returns:
            List of (condition, result) tuples.
        """
        clauses = []

        # Find the content between CASE and final END
        case_match = re.match(r'CASE\s+(.+)', expression, re.DOTALL | re.IGNORECASE)
        if not case_match:
            return clauses

        content = case_match.group(1)

        # Use a state machine to extract WHEN clauses
        # This handles nested CASE statements properly
        i = 0
        while i < len(content):
            # Find WHEN
            when_match = re.match(r'\s*WHEN\s+', content[i:], re.IGNORECASE)
            if not when_match:
                break

            i += when_match.end()

            # Find THEN, accounting for possible nested expressions
            condition_start = i
            depth = 0
            while i < len(content):
                upper_content = content[i:].upper()
                if upper_content.startswith('CASE'):
                    depth += 1
                    i += 4
                elif upper_content.startswith('END'):
                    if depth > 0:
                        depth -= 1
                        i += 3
                    else:
                        break
                elif depth == 0 and upper_content.startswith('THEN'):
                    break
                else:
                    i += 1

            condition = content[condition_start:i].strip()

            # Skip THEN
            then_match = re.match(r'\s*THEN\s+', content[i:], re.IGNORECASE)
            if then_match:
                i += then_match.end()

            # Find the result (until next WHEN, ELSE, or END at depth 0)
            result_start = i
            depth = 0
            while i < len(content):
                upper_content = content[i:].upper()
                if upper_content.startswith('CASE'):
                    depth += 1
                    i += 4
                elif upper_content.startswith('END'):
                    if depth > 0:
                        depth -= 1
                        i += 3
                    else:
                        break
                elif depth == 0 and (upper_content.startswith('WHEN') or upper_content.startswith('ELSE')):
                    break
                else:
                    i += 1

            result = content[result_start:i].strip()
            clauses.append((condition, result))

        return clauses

    def _extract_else_clause(self, expression: str) -> str:
        """Extract the ELSE clause from a CASE expression.

        Args:
            expression: The CASE expression.

        Returns:
            The ELSE value, or empty string if no ELSE clause.
        """
        content = expression.upper()

        # Skip the initial CASE keyword
        case_start = content.find('CASE')
        if case_start == -1:
            return ""

        # Start after the initial CASE, depth 0 means we're at the top level
        i = case_start + 4
        depth = 0  # 0 = top level of the outer CASE

        while i < len(content):
            if content[i:].startswith('CASE'):
                depth += 1  # Entering a nested CASE
                i += 4
            elif content[i:].startswith('END'):
                if depth > 0:
                    depth -= 1  # Exiting a nested CASE
                    i += 3
                else:
                    # This is the END of the outer CASE
                    break
            elif depth == 0 and content[i:].startswith('ELSE'):
                # Found ELSE at the top level
                else_start = i + 4
                # Find the END at depth 0
                j = else_start
                inner_depth = 0
                while j < len(content):
                    if content[j:].startswith('CASE'):
                        inner_depth += 1
                        j += 4
                    elif content[j:].startswith('END'):
                        if inner_depth > 0:
                            inner_depth -= 1
                            j += 3
                        else:
                            # Extract the ELSE value (strip semicolons and whitespace)
                            else_value = expression[else_start:j].strip().rstrip(';').strip()
                            return else_value
                    else:
                        j += 1
                # If we didn't find END, return what we have
                else_value = expression[else_start:].strip().rstrip(';').strip()
                return else_value
            else:
                i += 1

        return ""

    def _convert_condition(self, condition: str, rule: TransformationRule,
                           local_var_names: set = None, field_mappings: dict = None) -> str:
        """Convert an SQL condition to Python.

        Args:
            condition: The SQL condition.
            rule: The transformation rule for context.
<<<<<<< HEAD
            local_var_names: Set of local variable names to preserve (not convert to self.table.field).
            field_mappings: Dict mapping 'TABLE.FIELD' to local variable names.
=======
>>>>>>> 1e2a4393 (>feat: add ancrdt model vizualisation, and generation of derived fields)

        Returns:
            Python condition expression.
        """
        py_condition = condition
<<<<<<< HEAD
        local_var_names = local_var_names or set()
        field_mappings = field_mappings or {}
=======
>>>>>>> 1e2a4393 (>feat: add ancrdt model vizualisation, and generation of derived fields)

        # Replace SQL operators with Python equivalents
        py_condition = re.sub(r'\bAND\b', 'and', py_condition, flags=re.IGNORECASE)
        py_condition = re.sub(r'\bOR\b', 'or', py_condition, flags=re.IGNORECASE)
        py_condition = re.sub(r'\bNOT\b', 'not', py_condition, flags=re.IGNORECASE)
        py_condition = re.sub(r'\bIS\s+NULL\b', '== None', py_condition, flags=re.IGNORECASE)
        py_condition = re.sub(r'\bIS\s+NOT\s+NULL\b', '!= None', py_condition, flags=re.IGNORECASE)
        py_condition = py_condition.replace('<>', '!=')

<<<<<<< HEAD
        # Convert SQL = (assignment/comparison) to Python == (comparison)
        # Must be careful not to affect <=, >=, !=, or already converted ==
        py_condition = re.sub(r'(?<![<>!=])=(?!=)', '==', py_condition)

        # Handle IN clause - use a function to preserve each field reference
        def replace_in_clause(match):
            field = match.group(1)
            values = match.group(2)
            return f'{field} in ({values})'

        py_condition = re.sub(
            r'(\S+)\s+IN\s*\(([^)]+)\)',
            replace_in_clause,
            py_condition,
            flags=re.IGNORECASE
        )

        # Handle DATEADD function - convert to Python relativedelta
        # DATEADD(month, -3, date) -> (date - relativedelta(months=3))
        def replace_dateadd_in_condition(match):
            unit = match.group(1).lower()
            offset = int(match.group(2))
            date_expr = match.group(3).strip()

            # Map SQL units to relativedelta kwargs
            unit_map = {
                'month': 'months',
                'months': 'months',
                'day': 'days',
                'days': 'days',
                'year': 'years',
                'years': 'years',
                'week': 'weeks',
                'weeks': 'weeks',
            }
            py_unit = unit_map.get(unit, 'days')

            # Convert date expression field references
            date_py = self._convert_field_references(date_expr, rule, local_var_names, field_mappings)

            # Generate the relativedelta expression
            if offset < 0:
                return f"({date_py} - relativedelta({py_unit}={abs(offset)}))"
            else:
                return f"({date_py} + relativedelta({py_unit}={offset}))"

        # Try proper format first: DATEADD(month, -3, date_field)
        py_condition = re.sub(
            r'DATEADD\s*\(\s*(\w+)\s*,\s*(-?\d+)\s*,\s*([^)]+)\)',
            replace_dateadd_in_condition,
            py_condition,
            flags=re.IGNORECASE
        )

        # Handle malformed format (missing comma after number): DATEADD(month,-3BIRD_TABLE.FIELD)
        def replace_dateadd_malformed(match):
            unit = match.group(1).lower()
            offset = int(match.group(2))
            date_expr = match.group(3).strip()

            unit_map = {
                'month': 'months',
                'months': 'months',
                'day': 'days',
                'days': 'days',
                'year': 'years',
                'years': 'years',
            }
            py_unit = unit_map.get(unit, 'days')
            date_py = self._convert_field_references(date_expr, rule, local_var_names, field_mappings)

            if offset < 0:
                return f"({date_py} - relativedelta({py_unit}={abs(offset)}))"
            else:
                return f"({date_py} + relativedelta({py_unit}={offset}))"

        py_condition = re.sub(
            r'DATEADD\s*\(\s*(\w+)\s*,\s*(-?\d+)([A-Z_][^)]*)\)',
            replace_dateadd_malformed,
            py_condition,
            flags=re.IGNORECASE
        )

        # Convert field references, using field_mappings for related tables
        py_condition = self._convert_field_references(py_condition, rule, local_var_names, field_mappings)

        # Clean up whitespace and normalize multi-line conditions
        py_condition = ' '.join(py_condition.split())

        # Wrap conditions with 'and' or 'or' in parentheses for Python multi-line support
        if ' and ' in py_condition or ' or ' in py_condition:
            py_condition = f'({py_condition})'

        return py_condition

    def _convert_expression(self, expression: str, rule: TransformationRule,
                            local_var_names: set = None, field_mappings: dict = None) -> str:
=======
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
>>>>>>> 1e2a4393 (>feat: add ancrdt model vizualisation, and generation of derived fields)
        """Convert an SQL expression to Python.

        Args:
            expression: The SQL expression.
            rule: The transformation rule for context.
<<<<<<< HEAD
            local_var_names: Set of local variable names to preserve (not convert to self.table.field).
            field_mappings: Dict mapping 'TABLE.FIELD' to local variable names.
=======
>>>>>>> 1e2a4393 (>feat: add ancrdt model vizualisation, and generation of derived fields)

        Returns:
            Python expression.
        """
        py_expr = expression.strip()
<<<<<<< HEAD
        local_var_names = local_var_names or set()
        field_mappings = field_mappings or {}
=======
>>>>>>> 1e2a4393 (>feat: add ancrdt model vizualisation, and generation of derived fields)

        # Handle N/A or NULL
        if py_expr.upper() in ('N/A', 'NULL', "'N/A'"):
            return "None"

<<<<<<< HEAD
        # Handle nested CASE - recursively parse it
        if 'CASE' in py_expr.upper():
            return self._parse_nested_case_inline(py_expr, rule, local_var_names, field_mappings)

        # Strip assignment operators from expressions like "TABLE.FIELD = value"
        # In THEN clauses, we only want the right-hand side value
        if '=' in py_expr and 'BIRD_' in py_expr:
            # Check if this is an assignment (TABLE.FIELD = expression)
            assignment_match = re.match(
                r'BIRD_[A-Z_]+\.[A-Z_]+\s*=\s*(.+)',
                py_expr,
                re.DOTALL | re.IGNORECASE
            )
            if assignment_match:
                py_expr = assignment_match.group(1).strip()

        # Handle DATEADD function - convert to Python relativedelta
        # DATEADD(month, -3, date) -> (date - relativedelta(months=3))
        def replace_dateadd(match):
            unit = match.group(1).lower()
            offset = int(match.group(2))
            date_expr = match.group(3).strip()

            # Map SQL units to relativedelta kwargs
            unit_map = {
                'month': 'months',
                'months': 'months',
                'day': 'days',
                'days': 'days',
                'year': 'years',
                'years': 'years',
                'week': 'weeks',
                'weeks': 'weeks',
            }
            py_unit = unit_map.get(unit, 'days')

            # Convert date expression field references
            date_py = self._convert_field_references(date_expr, rule, local_var_names, field_mappings)

            # Generate the relativedelta expression
            if offset < 0:
                return f"({date_py} - relativedelta({py_unit}={abs(offset)}))"
            else:
                return f"({date_py} + relativedelta({py_unit}={offset}))"

        # Try proper format first: DATEADD(month, -3, date_field)
        py_expr = re.sub(
            r'DATEADD\s*\(\s*(\w+)\s*,\s*(-?\d+)\s*,\s*([^)]+)\)',
            replace_dateadd,
=======
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
>>>>>>> 1e2a4393 (>feat: add ancrdt model vizualisation, and generation of derived fields)
            py_expr,
            flags=re.IGNORECASE
        )

<<<<<<< HEAD
        # Handle malformed format (missing comma after number): DATEADD(month,-3BIRD_TABLE.FIELD)
        def replace_dateadd_malformed(match):
            unit = match.group(1).lower()
            offset = int(match.group(2))
            date_expr = match.group(3).strip()

            unit_map = {
                'month': 'months',
                'months': 'months',
                'day': 'days',
                'days': 'days',
                'year': 'years',
                'years': 'years',
            }
            py_unit = unit_map.get(unit, 'days')
            date_py = self._convert_field_references(date_expr, rule, local_var_names, field_mappings)

            if offset < 0:
                return f"({date_py} - relativedelta({py_unit}={abs(offset)}))"
            else:
                return f"({date_py} + relativedelta({py_unit}={offset}))"

        py_expr = re.sub(
            r'DATEADD\s*\(\s*(\w+)\s*,\s*(-?\d+)([A-Z_][^)]*)\)',
            replace_dateadd_malformed,
            py_expr,
            flags=re.IGNORECASE
        )

        # Convert remaining field references, using field_mappings for related tables
        py_expr = self._convert_field_references(py_expr, rule, local_var_names, field_mappings)

        return py_expr

    def _parse_nested_case_inline(self, expression: str, rule: TransformationRule,
                                   local_var_names: set = None, field_mappings: dict = None) -> str:
        """Parse a nested CASE expression and return inline Python code.

        For nested CASE in THEN clauses, we generate a helper method call.

        Args:
            expression: The CASE expression.
            rule: The transformation rule for context.
            local_var_names: Set of local variable names to preserve.
            field_mappings: Dict mapping 'TABLE.FIELD' to local variable names.

        Returns:
            Python expression or method call.
        """
        local_var_names = local_var_names or set()
        field_mappings = field_mappings or {}

        # Try to parse the nested CASE into a simple conditional expression
        # Extract WHEN clauses
        when_pattern = r'WHEN\s+(.+?)\s+THEN\s+(.+?)(?=\s+WHEN|\s+ELSE|\s+END)'
        else_pattern = r'ELSE\s+(.+?)\s+END'

        when_matches = re.findall(when_pattern, expression, re.DOTALL | re.IGNORECASE)
        else_match = re.search(else_pattern, expression, re.DOTALL | re.IGNORECASE)

        if not when_matches:
            return "None  # TODO: Complex nested CASE needs manual implementation"

        # For simple two-branch cases, use ternary expression
        if len(when_matches) == 1 and else_match:
            condition = when_matches[0][0].strip()
            then_value = when_matches[0][1].strip()
            else_value = else_match.group(1).strip()

            # Check if then/else values contain nested CASE
            if 'CASE' in then_value.upper() or 'CASE' in else_value.upper():
                return "None  # TODO: Deeply nested CASE needs manual implementation"

            cond_py = self._convert_condition(condition, rule, local_var_names, field_mappings)
            then_py = self._convert_expression(then_value, rule, local_var_names, field_mappings)
            else_py = self._convert_expression(else_value, rule, local_var_names, field_mappings)

            return f"({then_py} if {cond_py} else {else_py})"

        # For more complex cases, generate a comment
        return "None  # TODO: Multi-branch nested CASE needs manual implementation"

    def _convert_field_references(self, text: str, rule: TransformationRule,
                                   local_var_names: set = None, field_mappings: dict = None) -> str:
        """Convert BIRD_TABLE.FIELD references to self.field or local variable names.

        When the table matches the target class, uses self.FIELD directly.
        For other tables, if field_mappings is provided, uses the local variable name.
        Otherwise falls back to self.table.field for relationship access.

        Examples for INSTRMNT_RL class with field_mappings:
        - BIRD_INSTRMNT_RL_IL.DFLT_STTS -> self.DFLT_STTS (same class)
        - BIRD_PRTY_IL.DFLT_STTS -> prty_dflt_stts (from field_mappings)

        Examples without field_mappings:
        - BIRD_PRTY_IL.DFLT_STTS -> self.prty.dflt_stts

        Local variable names (like TM_PST_DU) are preserved and not converted.
=======
        return py_expr

    def _convert_field_references(self, text: str, rule: TransformationRule) -> str:
        """Convert BIRD_TABLE.FIELD references to self.TABLE.FIELD.
>>>>>>> 1e2a4393 (>feat: add ancrdt model vizualisation, and generation of derived fields)

        Args:
            text: Text containing field references.
            rule: The transformation rule for context.
<<<<<<< HEAD
            local_var_names: Set of local variable names to preserve (not convert).
            field_mappings: Dict mapping 'TABLE.FIELD' to local variable names.
=======
>>>>>>> 1e2a4393 (>feat: add ancrdt model vizualisation, and generation of derived fields)

        Returns:
            Text with converted field references.
        """
<<<<<<< HEAD
        local_var_names = local_var_names or set()
        field_mappings = field_mappings or {}

        # Get the target class name (normalized, without layer suffix)
        target_class = rule.target_class
        target_class_normalized = re.sub(r'_IL$|_EIL$', '', target_class).upper()
=======
        # Replace BIRD_TABLE_IL.FIELD with self.TABLE.FIELD
        # and BIRD_TABLE_EIL.FIELD with self.TABLE.FIELD
>>>>>>> 1e2a4393 (>feat: add ancrdt model vizualisation, and generation of derived fields)

        def replace_ref(match):
            full_table = match.group(1)
            field_name = match.group(2)

            # Normalize table name
            table = full_table
            if table.startswith("BIRD_"):
                table = table[5:]

<<<<<<< HEAD
            # Remove layer suffix (_IL, _EIL) for comparison
            base_table = re.sub(r'_IL$|_EIL$', '', table)
            base_table_upper = base_table.upper()

            # Check if this table matches the target class
            if base_table_upper == target_class_normalized:
                # Same class - use self.FIELD directly (keep original case)
                return f"self.{field_name}"
            else:
                # Different class - check if we have a field mapping
                # Try various key formats to find the mapping
                mapping_keys = [
                    f"{base_table_upper}.{field_name}",  # PRTY.DFLT_STTS
                    f"{table}.{field_name}",  # PRTY_IL.DFLT_STTS
                ]

                for key in mapping_keys:
                    if key in field_mappings:
                        # Use the local variable name from mappings
                        return field_mappings[key]

                # Fallback to self.table.field for relationship access
                base_table_lower = base_table.lower()
                field_name_lower = field_name.lower()
                return f"self.{base_table_lower}.{field_name_lower}"

        # Match BIRD_TABLE_LAYER.FIELD patterns (local variables without BIRD_ prefix are preserved)
=======
            # Remove layer suffix for property access
            table = re.sub(r'_IL$|_EIL$', '', table)

            return f"self.{table}.{field_name}"

>>>>>>> 1e2a4393 (>feat: add ancrdt model vizualisation, and generation of derived fields)
        result = re.sub(
            r'(BIRD_[A-Z_]+)\s*\.\s*([A-Z_]+)',
            replace_ref,
            text
        )

        return result

<<<<<<< HEAD
    def _make_division_safe(self, expression: str) -> str:
        """Wrap division expressions with null/zero safety checks.

        Transforms expressions like:
            a / b -> (a / b) if b else None
            a + b / c -> (a + b / c) if c else None

        Args:
            expression: The Python expression that may contain division.

        Returns:
            Expression with safe division wrapping if needed.
        """
        # Check if expression contains division
        if '/' not in expression:
            return expression

        # Find the denominator (everything after the last /)
        # This is a simplified approach - handles simple cases like "a / b" and "a + b / c"
        # For complex nested cases, we'd need a proper parser

        # Pattern: find "expr / denominator" where denominator is a variable or expression
        div_match = re.search(r'/\s*([a-zA-Z_][a-zA-Z0-9_\.]*)', expression)
        if div_match:
            denominator = div_match.group(1)
            # Wrap with safety check
            return f"({expression}) if {denominator} else None"

        return expression

=======
>>>>>>> 1e2a4393 (>feat: add ancrdt model vizualisation, and generation of derived fields)
    def _generate_placeholder(self, rule: TransformationRule) -> str:
        """Generate placeholder code when algorithm can't be parsed.

        Args:
            rule: The transformation rule.

        Returns:
            Placeholder Python code with TODO comment.
        """
        return f"# TODO: Implement derivation logic from rule {rule.semantic_id}\nreturn None"

<<<<<<< HEAD
    def _get_related_tables_and_fields(self, rule: TransformationRule) -> dict:
        """Extract related tables and their fields from rule dependencies.

        Returns a dictionary mapping table names to their referenced fields,
        excluding the target class itself.

        Args:
            rule: The transformation rule with dependencies.

        Returns:
            Dict mapping table names to list of field names.
            Example: {'PRTY': ['DFLT_STTS', 'PRFRMNG_STTS'], 'CLLTRL': ['CLLTRL_TYP']}
        """
        related_tables = {}

        # Get target class normalized (without layer suffix)
        target_class = rule.target_class
        target_class_normalized = re.sub(r'_IL$|_EIL$', '', target_class).upper()

        for dep in rule.dependencies:
            # Parse dependency format: TABLE_LAYER.FIELD or TABLE.FIELD
            if '.' in dep:
                parts = dep.split('.')
                table_part = parts[0]
                field_name = parts[1]

                # Normalize table name (remove layer suffix)
                table_normalized = re.sub(r'_IL$|_EIL$', '', table_part).upper()

                # Only include if it's a different table than target
                if table_normalized != target_class_normalized:
                    if table_normalized not in related_tables:
                        related_tables[table_normalized] = []
                    if field_name not in related_tables[table_normalized]:
                        related_tables[table_normalized].append(field_name)

        return related_tables

    def _abbreviate_table_name(self, table_name: str, max_len: int = 20) -> str:
        """Abbreviate long table names for use in variable names.

        For short names (< max_len), returns lowercase as-is.
        For long names, creates abbreviation using first letters of words.

        Examples:
            PRTY -> prty
            INSTRMNT_RL -> instrmnt_rl
            LNG_SCRTY_PSTN_PRDNTL_PRTFL_ASSGNMNT_... -> lsp_ppa_acfaa

        Args:
            table_name: The full table name (uppercase).
            max_len: Maximum length before abbreviating.

        Returns:
            Abbreviated or lowercase table name.
        """
        table_lower = table_name.lower()

        if len(table_lower) <= max_len:
            return table_lower

        # Split by underscore and take first 3 letters of each part
        parts = table_name.split('_')
        abbreviated_parts = []
        for part in parts:
            if len(part) <= 3:
                abbreviated_parts.append(part.lower())
            else:
                # Take first 3 letters for readability
                abbreviated_parts.append(part[:3].lower())

        abbreviated = '_'.join(abbreviated_parts)

        # If still too long, use just first letter of each part
        if len(abbreviated) > max_len:
            abbreviated = ''.join(p[0].lower() for p in parts if p)

        return abbreviated

    def _generate_related_table_inits(self, rule: TransformationRule) -> tuple:
        """Generate initialization code for related table fields.

        Args:
            rule: The transformation rule.

        Returns:
            Tuple of (init_lines: list[str], field_mappings: dict)
            - init_lines: Python code lines for initialization
            - field_mappings: Dict mapping 'TABLE.FIELD' to local variable name
        """
        related_tables = self._get_related_tables_and_fields(rule)
        init_lines = []
        field_mappings = {}  # Maps 'PRTY.DFLT_STTS' -> 'prty_dflt_stts'

        for table_name, fields in related_tables.items():
            # Use abbreviated name for variable prefix
            table_abbrev = self._abbreviate_table_name(table_name)
            # ForeignKey field name pattern: the<TABLE>
            fk_name = f"the{table_name}"

            for field in fields:
                field_lower = field.lower()
                local_var = f"{table_abbrev}_{field_lower}"

                # Generate safe initialization
                init_lines.append(
                    f"{local_var} = self.{fk_name}.{field} if self.{fk_name} else None"
                )

                # Store mapping for later use in condition/expression conversion
                field_mappings[f"{table_name}.{field}"] = local_var
                # Also store with layer suffixes for matching
                field_mappings[f"{table_name}_IL.{field}"] = local_var
                field_mappings[f"{table_name}_EIL.{field}"] = local_var

        return init_lines, field_mappings

=======
>>>>>>> 1e2a4393 (>feat: add ancrdt model vizualisation, and generation of derived fields)
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
