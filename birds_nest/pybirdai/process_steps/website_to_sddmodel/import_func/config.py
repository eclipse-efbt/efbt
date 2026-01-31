# coding=UTF-8
# Copyright (c) 2024 Bird Software Solutions Ltd
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDX-License-Identifier: EPL-2.0
#
# Contributors:
#    Neil Mackenzie - initial API and implementation
#    Benjamin Arfa - dataset configuration system

"""
Dataset Configuration for Import Operations

This module provides configuration classes that control import behavior
based on the dataset type (FINREP, ANCRDT, DPM, etc.).
"""

import os
from django.conf import settings


def get_csv_file_path(context, filename, config=None):
    """
    Get the correct path for a CSV file based on configuration.

    SMCubes artefacts are stored in artefacts/smcubes_artefacts/
    Other datasets (ANCRDT, etc.) use context.file_directory + subdirectory

    Args:
        context: SDDContext containing file_directory
        filename: Name of the CSV file (e.g., "member.csv")
        config: Optional DatasetConfig for dynamic file paths

    Returns:
        Full path to the CSV file
    """
    if config and config.file_directory != "smcubes_artefacts":
        # Use context-based path for non-standard datasets (e.g., ANCRDT)
        return os.path.join(context.file_directory, config.file_directory, filename)
    else:
        # SMCubes artefacts are in the artefacts directory
        return os.path.join(settings.BASE_DIR, "artefacts", "smcubes_artefacts", filename)


class DatasetConfig:
    """
    Configuration class for dataset-specific import behavior.

    Attributes:
        dataset_type: Type of dataset ("finrep", "ancrdt", "dpm")
        bypass_ecb_filter: Whether to bypass ECB maintenance agency filtering
        includes_cubes: Whether dataset includes cube structures
        includes_subdomains: Whether dataset includes subdomains
        includes_table_cells: Whether to import table cells
        includes_ordinate_items: Whether to import ordinate items
        includes_cell_positions: Whether to import cell positions
        use_csv_copy: Whether to use optimized csv_copy imports for large datasets
        file_directory: Subdirectory containing CSV files (e.g., "smcubes_artefacts", "ancrdt_csv")
    """

    # Supported dataset types
    FINREP = "finrep"
    ANCRDT = "ancrdt"
    DPM = "dpm"

    def __init__(self, dataset_type="finrep", file_directory="smcubes_artefacts"):
        """
        Initialize dataset configuration.

        Args:
            dataset_type: Type of dataset ("finrep", "ancrdt", "dpm")
            file_directory: Subdirectory containing CSV files
        """
        self.dataset_type = dataset_type.lower()
        self.file_directory = file_directory

        # Configure behavior based on dataset type
        if self.dataset_type == self.ANCRDT:
            self.bypass_ecb_filter = True
            self.includes_cubes = True
            self.includes_subdomains = True
            self.use_csv_copy = False
            self.column_index_class_name = "ancrdt"
            # ANCRDT does not include rendering detail entities
            self.includes_rendering_package = False
        elif self.dataset_type == self.DPM:
            self.bypass_ecb_filter = False
            self.includes_cubes = False
            self.includes_subdomains = False
            self.use_csv_copy = True  # Use optimized imports for large DPM datasets
            self.column_index_class_name = "standard"
            # DPM includes rendering detail entities
            self.includes_rendering_package = True
        else:  # FINREP or default
            self.bypass_ecb_filter = False
            self.includes_cubes = False
            self.includes_subdomains = False
            self.use_csv_copy = False
            self.column_index_class_name = "standard"
            # FINREP includes rendering detail entities
            self.includes_rendering_package = True

    def get_column_indexes(self):
        """
        Get the appropriate ColumnIndexes class for this dataset type.

        Returns:
            ColumnIndexes instance
        """
        if self.column_index_class_name == "ancrdt":
            from pybirdai.process_steps.ancrdt_transformation.csv_column_index_context_ancrdt import ColumnIndexes
        else:
            from pybirdai.context.csv_column_index_context import ColumnIndexes

        return ColumnIndexes()

    def should_include_entity(self, maintenance_agency, is_reference_data):
        """
        Determine if an entity should be included based on filtering rules.

        Args:
            maintenance_agency: Maintenance agency ID
            is_reference_data: Whether this is reference data import

        Returns:
            bool: True if entity should be included
        """
        if self.bypass_ecb_filter:
            # ANCRDT: Include everything
            return True

        # Standard filtering logic
        if is_reference_data and maintenance_agency == "ECB":
            return True
        if not is_reference_data and maintenance_agency != "ECB":
            return True

        return False

    def __repr__(self):
        return f"DatasetConfig(type='{self.dataset_type}', dir='{self.file_directory}')"
