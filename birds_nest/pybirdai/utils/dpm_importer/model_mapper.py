"""
Data Model Mapper: DPM Input Model to Standardized Output Model

This module provides mapping definitions between the input DPM (Data Point Model) schema
and the output standardized schema. Each mapping defines how columns from input tables
map to columns and tables in the output model.

Author: Generated Mapper
Date: 2024
"""

from constants import MAPPING

class ModelMapper:
    """
    Comprehensive mapper for transforming DPM input model to standardized output model.

    The mapper is organized by functional areas and provides both table-level and
    column-level mappings with transformation logic where applicable.
    """

    def __init__(self):
        self.mappings = MAPPING

    def get_mapping(self, source_table):
        """
        Get mapping configuration for a source table.

        Args:
            source_table (str): Name of the source table

        Returns:
            dict: Mapping configuration or None if not found
        """
        return self.mappings.get(source_table)

    def get_all_mappings(self):
        """
        Get all mapping configurations.

        Returns:
            dict: All mapping configurations
        """
        return self.mappings

    def get_target_tables(self):
        """
        Get list of all target tables.

        Returns:
            list: Unique list of target tables
        """
        target_tables = set()
        for mapping in self.mappings.values():
            target_tables.add(mapping['target_table'])
        return sorted(list(target_tables))

    def get_source_tables_for_target(self, target_table):
        """
        Get source tables that map to a specific target table.

        Args:
            target_table (str): Name of the target table

        Returns:
            list: List of source tables that map to the target
        """
        source_tables = []
        for source_table, mapping in self.mappings.items():
            if mapping['target_table'] == target_table:
                source_tables.append(source_table)
        return source_tables

    def get_unmapped_input_tables(self, input_tables):
        """
        Get list of input tables that don't have mappings defined.

        Args:
            input_tables (list): List of input table names

        Returns:
            list: List of unmapped tables
        """
        mapped_tables = set(self.mappings.keys())
        return [table for table in input_tables if table not in mapped_tables]

    def validate_mappings(self):
        """
        Validate mapping configurations for completeness and consistency.

        Returns:
            dict: Validation results with any issues found
        """
        issues = {
            'missing_target_tables': [],
            'invalid_column_mappings': [],
            'duplicate_mappings': []
        }

        # Check for duplicate target mappings
        target_usage = {}
        for source_table, mapping in self.mappings.items():
            target = mapping['target_table']
            if target not in target_usage:
                target_usage[target] = []
            target_usage[target].append(source_table)

        # Report tables with multiple source mappings (may need union logic)
        for target, sources in target_usage.items():
            if len(sources) > 1:
                issues['duplicate_mappings'].append({
                    'target_table': target,
                    'source_tables': sources
                })

        return issues
