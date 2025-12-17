# coding=UTF-8
# Copyright (c) 2025 Bird Software Solutions Ltd
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDX-License-Identifier: EPL-2.0
#
# Contributors:
#    Neil Mackenzie - initial API and implementation
#

"""
Configuration Resolver for Flexible Gap Analysis.

This module provides the JoinsConfigurationResolver class for resolving
which configuration files to use for a given framework and cube.

It supports:
- Framework-wide configuration files (e.g., join_for_product_to_reference_category_FINREP_REF.csv)
- Per-cube configuration files (e.g., join_for_product_to_reference_category_ANACREDIT_CNTRPRTY.csv)
"""

import os
from typing import List, Optional, Dict, Any
import csv
import logging

from pybirdai.process_steps.joins_meta_data.condition_parser import BreakdownCondition

logger = logging.getLogger(__name__)


class JoinsConfigurationResolver:
    """
    Resolves which configuration file(s) to use for a given context.

    This class handles the discovery and loading of joins configuration files,
    supporting both framework-wide and per-cube configurations.

    File Naming Convention:
        - Framework-wide: join_for_product_to_reference_category_{FRAMEWORK}_REF.csv
        - Per-cube: join_for_product_to_reference_category_{FRAMEWORK}_{CUBE_CODE}.csv

    Priority:
        1. Per-cube file (if exists) takes precedence
        2. Framework-wide file (fallback)
    """

    def __init__(self, config_directory: str):
        """
        Initialize the resolver.

        Args:
            config_directory: Base directory for configuration files.
        """
        self.config_directory = config_directory

    def get_configuration_files(
        self, framework: str, cube_code: Optional[str] = None
    ) -> List[str]:
        """
        Get configuration file(s) for the given framework and optional cube.

        Args:
            framework: Framework name (e.g., "FINREP_REF", "ANACREDIT")
            cube_code: Optional cube code for per-cube configuration

        Returns:
            List of file paths (may be empty if no files found).
        """
        files = []

        # Check for per-cube configuration first (higher priority)
        if cube_code:
            per_cube_file = self._get_per_cube_filename(framework, cube_code)
            per_cube_path = os.path.join(self.config_directory, per_cube_file)
            if os.path.exists(per_cube_path):
                logger.debug(f"Found per-cube config: {per_cube_path}")
                files.append(per_cube_path)
                return files  # Per-cube takes precedence

        # Check for framework-wide configuration
        framework_file = self._get_framework_filename(framework)
        framework_path = os.path.join(self.config_directory, framework_file)
        if os.path.exists(framework_path):
            logger.debug(f"Found framework config: {framework_path}")
            files.append(framework_path)

        return files

    def _get_per_cube_filename(self, framework: str, cube_code: str) -> str:
        """Generate filename for per-cube configuration."""
        return f"join_for_product_to_reference_category_{framework}_{cube_code}.csv"

    def _get_framework_filename(self, framework: str) -> str:
        """Generate filename for framework-wide configuration."""
        return f"join_for_product_to_reference_category_{framework}.csv"

    def has_per_cube_config(self, framework: str, cube_code: str) -> bool:
        """
        Check if a per-cube configuration exists.

        Args:
            framework: Framework name
            cube_code: Cube code

        Returns:
            True if per-cube config exists.
        """
        per_cube_file = self._get_per_cube_filename(framework, cube_code)
        per_cube_path = os.path.join(self.config_directory, per_cube_file)
        return os.path.exists(per_cube_path)

    def has_framework_config(self, framework: str) -> bool:
        """
        Check if a framework-wide configuration exists.

        Args:
            framework: Framework name

        Returns:
            True if framework config exists.
        """
        framework_file = self._get_framework_filename(framework)
        framework_path = os.path.join(self.config_directory, framework_file)
        return os.path.exists(framework_path)

    def discover_all_cube_configs(self, framework: str) -> List[str]:
        """
        Discover all per-cube configuration files for a framework.

        Args:
            framework: Framework name

        Returns:
            List of cube codes that have per-cube configurations.
        """
        cube_codes = []
        prefix = f"join_for_product_to_reference_category_{framework}_"
        suffix = ".csv"

        if not os.path.exists(self.config_directory):
            return cube_codes

        for filename in os.listdir(self.config_directory):
            if filename.startswith(prefix) and filename.endswith(suffix):
                # Extract cube code from filename
                # Skip if it's the framework-wide file (ends with _REF.csv but prefix already includes framework)
                cube_code = filename[len(prefix):-len(suffix)]
                if cube_code and cube_code != "REF":
                    cube_codes.append(cube_code)

        return cube_codes

    def load_configuration(
        self, framework: str, cube_code: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Load configuration data from the appropriate file(s).

        Args:
            framework: Framework name
            cube_code: Optional cube code

        Returns:
            List of configuration rows with parsed BreakdownCondition.
        """
        files = self.get_configuration_files(framework, cube_code)
        if not files:
            logger.warning(
                f"No configuration files found for framework={framework}, cube={cube_code}"
            )
            return []

        all_data = []
        for file_path in files:
            data = self._load_csv_file(file_path, framework)
            all_data.extend(data)

        return all_data

    def _load_csv_file(self, file_path: str, framework: str) -> List[Dict[str, Any]]:
        """
        Load and parse a single CSV configuration file.

        Args:
            file_path: Path to the CSV file
            framework: Framework name (for column detection)

        Returns:
            List of parsed configuration rows.
        """
        rows = []

        try:
            with open(file_path, encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                headers = reader.fieldnames or []

                # Detect format based on headers
                is_ancrdt_format = 'rolc' in headers and 'join_identifier' in headers
                is_finrep_format = 'Main Category' in headers

                for row in reader:
                    parsed_row = self._parse_row(row, is_ancrdt_format, is_finrep_format)
                    if parsed_row:
                        rows.append(parsed_row)

        except FileNotFoundError:
            logger.error(f"Configuration file not found: {file_path}")
        except Exception as e:
            logger.error(f"Error reading configuration file {file_path}: {e}")

        return rows

    def _parse_row(
        self, row: Dict[str, str], is_ancrdt_format: bool, is_finrep_format: bool
    ) -> Optional[Dict[str, Any]]:
        """
        Parse a single row from the configuration file.

        Args:
            row: Dictionary of column values
            is_ancrdt_format: True if ANACREDIT format
            is_finrep_format: True if FINREP format

        Returns:
            Parsed row dictionary with BreakdownCondition, or None if invalid.
        """
        if is_ancrdt_format:
            # ANACREDIT format: rolc, join_identifier
            main_category = row.get('rolc', '').strip()
            name = row.get('join_identifier', '').strip()
            slice_name = name  # Use same as name for ANACREDIT
        elif is_finrep_format:
            # FINREP format: Main Category, Name, slice_name
            main_category = row.get('Main Category', '').strip()
            name = row.get('Name', '').strip()
            slice_name = row.get('slice_name', '').strip()
        else:
            logger.warning(f"Unknown row format: {row}")
            return None

        # Parse the main category using BreakdownCondition
        try:
            condition = BreakdownCondition(main_category)
        except ValueError as e:
            logger.warning(f"Failed to parse condition '{main_category}': {e}")
            # Return with None condition for invalid entries
            condition = None

        return {
            'main_category': main_category,
            'name': name,
            'slice_name': slice_name,
            'condition': condition,
            'raw_row': row
        }

    def get_products_for_framework(
        self, framework: str, cube_code: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get all product configurations for a framework.

        This is a convenience method that loads and returns product configurations
        suitable for use in joins metadata creation.

        Args:
            framework: Framework name
            cube_code: Optional cube code

        Returns:
            List of product configurations.
        """
        return self.load_configuration(framework, cube_code)

    def get_unique_main_categories(
        self, framework: str, cube_code: Optional[str] = None
    ) -> List[str]:
        """
        Get unique main category values from configuration.

        Args:
            framework: Framework name
            cube_code: Optional cube code

        Returns:
            List of unique main category strings.
        """
        configs = self.load_configuration(framework, cube_code)
        seen = set()
        result = []
        for config in configs:
            mc = config['main_category']
            if mc and mc not in seen:
                seen.add(mc)
                result.append(mc)
        return result

    def get_unique_slice_names(
        self, framework: str, cube_code: Optional[str] = None
    ) -> List[str]:
        """
        Get unique slice names from configuration.

        Args:
            framework: Framework name
            cube_code: Optional cube code

        Returns:
            List of unique slice names.
        """
        configs = self.load_configuration(framework, cube_code)
        seen = set()
        result = []
        for config in configs:
            sn = config['slice_name']
            if sn and sn not in seen:
                seen.add(sn)
                result.append(sn)
        return result
