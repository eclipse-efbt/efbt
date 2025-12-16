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
Python Code Generator for member link derivations.

This module generates Python code for derivation properties based on
member link mappings. The generated code uses @lineage decorators and
creates @property methods that can be added to data model classes.
"""

import logging
import os
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from .dataclasses import DerivationConfig, DerivationMapping
from .model_introspector import ModelIntrospector

logger = logging.getLogger(__name__)


class DerivationCodeGenerator:
    """Generates Python derivation code from member link mappings.

    This class creates Python source code for @property methods that
    implement derivation logic based on member link mappings.

    It uses ModelIntrospector to:
    - Find which table contains each source variable
    - Generate TABLE.FIELD format for @lineage dependencies
    - Generate correct Django ORM navigation paths for cross-table access
    """

    # Template for the generated file header
    FILE_HEADER_TEMPLATE = '''# coding=UTF-8
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
Auto-generated derivation code for {output_variable}.

Generated on: {timestamp}
Source: {csv_path}
Target cube: {target_cube}
Target variable: {target_variable}

This file contains derivation logic generated from member link mappings.
DO NOT EDIT MANUALLY - regenerate using the member_link_derivation module.
"""

from django.db import models
from pybirdai.annotations.decorators import lineage

'''

    # Template for a property method
    PROPERTY_TEMPLATE = '''
    @property
    @lineage(dependencies={{{dependencies}}})
    def {property_name}(self):
        """Derive {property_name} from member link mappings.

        Source variables: {source_vars}
        Generated from member_link.csv mappings.

        Returns:
            The derived {target_variable} value based on input variables.
        """
{mapping_code}
{resolution_code}
'''

    def __init__(self, config: DerivationConfig):
        """Initialize the generator with configuration.

        Args:
            config: DerivationConfig with generation settings
        """
        self.config = config
        self._introspector: Optional[ModelIntrospector] = None
        self._variable_tables: Dict[str, str] = {}  # Cache: variable -> table

    @property
    def introspector(self) -> Optional[ModelIntrospector]:
        """Lazy-load the model introspector."""
        if self._introspector is None and self.config.models_path:
            try:
                if os.path.exists(self.config.models_path):
                    self._introspector = ModelIntrospector(self.config.models_path)
                    logger.info(f"Loaded model introspector from {self.config.models_path}")
                else:
                    logger.warning(f"Models file not found: {self.config.models_path}")
            except Exception as e:
                logger.warning(f"Failed to load model introspector: {e}")
        return self._introspector

    def _get_variable_table(self, var_name: str) -> str:
        """Get the table name that contains a variable.

        Uses the model introspector to find the table, with caching.

        Args:
            var_name: The variable name

        Returns:
            The table name, or the config class_name as fallback
        """
        if var_name in self._variable_tables:
            return self._variable_tables[var_name]

        table = self.config.class_name  # Default fallback

        if self.introspector:
            found_table = self.introspector.find_variable_table(var_name)
            if found_table:
                table = found_table
            else:
                logger.debug(f"Variable {var_name} not found in models, using default {table}")

        self._variable_tables[var_name] = table
        return table

    def generate_file(
        self,
        mappings: Dict[str, DerivationMapping],
        output_path: Optional[str] = None
    ) -> str:
        """Generate a complete Python file with derivation properties.

        Args:
            mappings: Dictionary of source variable names to DerivationMapping
            output_path: Optional path to write the file (uses config if not provided)

        Returns:
            The generated Python code as a string
        """
        output_path = output_path or os.path.join(
            self.config.output_dir,
            f"derived_{self.config.output_variable_name.lower()}.py"
        )

        # Generate file header
        code = self.FILE_HEADER_TEMPLATE.format(
            output_variable=self.config.output_variable_name,
            timestamp=datetime.now().isoformat(),
            csv_path=self.config.csv_path,
            target_cube=self.config.target_cube,
            target_variable=self.config.target_variable
        )

        # Generate the class (same format as bird_data_model.py)
        code += f"\nclass {self.config.class_name}(models.Model):\n"
        code += f'    """Auto-generated derived fields."""\n'

        # Generate the property
        property_code = self.generate_property(
            self.config.output_variable_name,
            mappings
        )
        code += property_code

        # Write to file if output path provided
        if output_path:
            self._ensure_directory(output_path)
            self.write_output_file(code, output_path)

        return code

    def generate_property(
        self,
        property_name: str,
        mappings: Dict[str, DerivationMapping]
    ) -> str:
        """Generate a @property method for the derivation.

        Args:
            property_name: Name of the property to generate
            mappings: Dictionary of source variable names to DerivationMapping

        Returns:
            Python code for the property method
        """
        # Collect dependencies in TABLE.FIELD format
        dependencies = []
        for var_name in sorted(mappings.keys()):
            table = self._get_variable_table(var_name)
            dependencies.append(f"{table}.{var_name}")

        dep_str = ", ".join(f"'{d}'" for d in dependencies)

        # Generate mapping dictionaries
        mapping_code = self._generate_mapping_dicts(mappings)

        # Generate resolution logic
        resolution_code = self._generate_resolution_logic(mappings)

        # Get source variable list for docstring (TABLE.FIELD format)
        source_vars = ", ".join(dependencies)

        return self.PROPERTY_TEMPLATE.format(
            property_name=property_name,
            dependencies=dep_str,
            source_vars=source_vars,
            target_variable=self.config.target_variable,
            mapping_code=mapping_code,
            resolution_code=resolution_code
        )

    def _generate_mapping_dicts(self, mappings: Dict[str, DerivationMapping]) -> str:
        """Generate Python dictionary literals for the mappings.

        Args:
            mappings: Dictionary of source variable names to DerivationMapping

        Returns:
            Python code defining the mapping dictionaries
        """
        lines = []

        # Sort mappings by specificity (highest first) for priority ordering
        sorted_mappings = sorted(
            mappings.items(),
            key=lambda x: x[1].specificity_score,
            reverse=True
        )

        for var_name, mapping in sorted_mappings:
            # Generate the mapping dict
            dict_name = f"_mapping_{var_name.lower()}"
            lines.append(f"        # Mappings for {var_name} (specificity: {mapping.specificity_score})")
            lines.append(f"        {dict_name} = {{")

            # Find the common domain prefix for input members
            input_domain = self._find_common_domain(list(mapping.mappings.keys()))

            # Sort mapping items for consistent output
            for input_member, output_member in sorted(mapping.mappings.items()):
                # Strip the domain prefix from input member
                # e.g., INSTTTNL_SCTR_S122_A_1 -> S122_A_1
                clean_input = self._strip_domain_prefix(input_member, input_domain)
                # Strip the target variable prefix from output member
                # e.g., TYP_INSTRMNT_71 -> 71
                clean_output = self._strip_variable_prefix(output_member, self.config.target_variable)
                lines.append(f'            "{clean_input}": "{clean_output}",')

            lines.append("        }")
            lines.append("")

        return "\n".join(lines)

    def _strip_variable_prefix(self, member_value: str, variable_name: str) -> str:
        """Strip the variable name prefix from a member value.

        Args:
            member_value: The full member value (e.g., TYP_INSTRMNT_71)
            variable_name: The variable name prefix to strip (e.g., TYP_INSTRMNT)

        Returns:
            The member value without the prefix (e.g., 71)
        """
        prefix = f"{variable_name}_"
        if member_value.startswith(prefix):
            return member_value[len(prefix):]
        return member_value

    def _find_common_domain(self, member_ids: List[str]) -> str:
        """Find the common domain prefix from a list of member IDs.

        The domain is the common prefix before the member code.
        E.g., for ['INSTTTNL_SCTR_S122_A_1', 'INSTTTNL_SCTR_S123'], domain is 'INSTTTNL_SCTR'.

        Args:
            member_ids: List of member ID strings

        Returns:
            The common domain prefix, or empty string if none found
        """
        if not member_ids:
            return ""

        # Split each member by underscore and find common prefix parts
        split_members = [m.split("_") for m in member_ids]

        # Find common prefix parts
        common_parts = []
        min_len = min(len(parts) for parts in split_members)

        for i in range(min_len - 1):  # -1 to leave at least one part as the code
            part = split_members[0][i]
            if all(parts[i] == part for parts in split_members):
                common_parts.append(part)
            else:
                break

        return "_".join(common_parts) if common_parts else ""

    def _strip_domain_prefix(self, member_value: str, domain: str) -> str:
        """Strip the domain prefix from a member value.

        Args:
            member_value: The full member value (e.g., INSTTTNL_SCTR_S122_A_1)
            domain: The domain prefix to strip (e.g., INSTTTNL_SCTR)

        Returns:
            The member value without the domain prefix (e.g., S122_A_1)
        """
        if domain:
            prefix = f"{domain}_"
            if member_value.startswith(prefix):
                return member_value[len(prefix):]
        return member_value

    def _generate_resolution_logic(self, mappings: Dict[str, DerivationMapping]) -> str:
        """Generate the resolution logic that determines the output value.

        The resolution logic checks each source variable in priority order
        (by specificity) and returns the first matching output value.

        For cross-table variables accessed via reverse relations, it generates
        loop-based code to iterate over related records.

        Args:
            mappings: Dictionary of source variable names to DerivationMapping

        Returns:
            Python code for the resolution logic
        """
        lines = []
        lines.append("        # Resolution logic - check variables in specificity order")

        # Sort by specificity (highest first)
        sorted_mappings = sorted(
            mappings.items(),
            key=lambda x: x[1].specificity_score,
            reverse=True
        )

        for i, (var_name, mapping) in enumerate(sorted_mappings):
            dict_name = f"_mapping_{var_name.lower()}"
            var_table = self._get_variable_table(var_name)
            from_table = self.config.class_name

            # Add comment showing TABLE.FIELD
            table_field = f"{var_table}.{var_name}"

            if i > 0:
                lines.append("")

            if i == 0:
                lines.append(f"        # Check {table_field} (highest specificity)")
            else:
                lines.append(f"        # Check {table_field}")

            # Check if we need loop-based access
            loop_code, accessor = self._get_variable_loop_accessor(var_name)

            if loop_code:
                # Need to iterate over related records
                lines.append(f"        {loop_code}")
                lines.append(f"            {var_name.lower()}_value = {accessor}")
                lines.append(f"            if {var_name.lower()}_value in {dict_name}:")
                lines.append(f"                return {dict_name}[{var_name.lower()}_value]")
            else:
                # Direct or single-navigation access
                accessor = self._get_variable_accessor(var_name)
                lines.append(f"        {var_name.lower()}_value = {accessor}")
                lines.append(f"        if {var_name.lower()}_value in {dict_name}:")
                lines.append(f"            return {dict_name}[{var_name.lower()}_value]")

        # Default return
        lines.append("")
        lines.append("        # No matching mapping found")
        lines.append("        return None")

        return "\n".join(lines)

    def _get_variable_accessor(self, var_name: str) -> str:
        """Get the Python accessor for a source variable.

        Uses the model introspector to determine the correct navigation path
        when the variable is in a different table than the derivation class.

        Args:
            var_name: The variable name

        Returns:
            Python expression to access the variable
        """
        var_table = self._get_variable_table(var_name)
        from_table = self.config.class_name

        # If variable is in the same table as the derivation, direct access
        if var_table == from_table:
            return f"self.{var_name}"

        # Use introspector to generate navigation path
        if self.introspector:
            accessor, _ = self.introspector.generate_accessor(from_table, var_name, var_table)
            return accessor

        # Fallback: direct access (may not work for cross-table)
        logger.warning(f"No introspector available, using direct access for {var_name}")
        return f"self.{var_name}"

    def _get_variable_loop_accessor(self, var_name: str) -> Tuple[str, str]:
        """Get loop-based accessor for iterating over related records.

        This is used when we need to check multiple related records for a match.

        Args:
            var_name: The variable name

        Returns:
            Tuple of (loop_code, accessor_in_loop)
        """
        var_table = self._get_variable_table(var_name)
        from_table = self.config.class_name

        if var_table == from_table:
            return "", f"self.{var_name}"

        if self.introspector:
            loop_code, accessor, _ = self.introspector.generate_loop_accessor(
                from_table, var_name, var_table
            )
            return loop_code, accessor

        return "", f"self.{var_name}"

    def _ensure_directory(self, file_path: str):
        """Ensure the directory for the output file exists.

        Args:
            file_path: Path to the output file
        """
        directory = os.path.dirname(file_path)
        if directory and not os.path.exists(directory):
            os.makedirs(directory, exist_ok=True)

    def write_output_file(self, code: str, output_path: str):
        """Write generated code to a file.

        Args:
            code: The Python code to write
            output_path: Path to the output file
        """
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(code)


def generate_standalone_derivation(
    mappings: Dict[str, DerivationMapping],
    output_variable: str,
    target_variable: str
) -> str:
    """Generate standalone derivation code without file writing.

    This is a convenience function for generating derivation code
    that can be embedded into other modules.

    Args:
        mappings: Dictionary of source variable names to DerivationMapping
        output_variable: Name for the generated property
        target_variable: The target variable being derived

    Returns:
        Python code for the derivation property
    """
    config = DerivationConfig(
        output_variable_name=output_variable,
        target_variable=target_variable
    )
    generator = DerivationCodeGenerator(config)
    return generator.generate_property(output_variable, mappings)
