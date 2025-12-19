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
#    Benjamin Arfa - unified import with dataset routing
#

"""Orchestrator function for importing report templates from SDD."""

import os
from .config import DatasetConfig
from .import_maintenance_agencies import import_maintenance_agencies
from .import_frameworks import import_frameworks
from .import_domains import import_domains
from .import_members import import_members
from .import_variables import import_variables
from .import_report_tables import import_report_tables
from .import_axis import import_axis
from .import_axis_ordinates import import_axis_ordinates
from .import_table_cells import import_table_cells
from .import_table_cells_csv_copy import import_table_cells_csv_copy
from .import_ordinate_items import import_ordinate_items
from .import_ordinate_items_csv_copy import import_ordinate_items_csv_copy
from .import_cell_positions import import_cell_positions
from .import_cell_positions_csv_copy import import_cell_positions_csv_copy
# ANCRDT-specific imports
from .import_subdomains import import_subdomains
from .import_subdomain_enumerations import import_subdomain_enumerations
from .import_cube_structures import import_cube_structures
from .import_cube_structure_items import import_cube_structure_items
from .import_cubes import import_cubes


def import_report_templates_from_sdd(sdd_context, dataset_type="finrep", file_dir=None, dpm=False, framework=None, frameworks=None):
    """
    Orchestrate the import of report templates from SDD CSV files.

    This function coordinates the import of all entities required for
    report templates, including metadata, tables, axes, and cells.
    Supports multiple dataset types (FINREP, ANCRDT, DPM).

    Framework Isolation:
    - When framework(s) specified, only deletes/replaces data for those frameworks
    - Other frameworks' data is preserved (e.g., importing FINREP won't delete COREP)
    - Uses FRAMEWORK_TABLE junction to determine framework ownership

    Args:
        sdd_context: SDDContext containing file paths and dictionaries
        dataset_type: Type of dataset ("finrep", "ancrdt", "dpm"). Default: "finrep"
        file_dir: Subdirectory containing CSV files (e.g., "technical_export", "ancrdt_csv")
                  If None, uses "technical_export" for finrep/dpm, "ancrdt_csv" for ancrdt
        dpm: Boolean indicating if importing DPM data (default False, kept for backward compatibility)
        framework: Single framework ID for framework-isolated imports (e.g., "EBA_FINREP").
                   If None, falls back to sdd_context.current_framework.
        frameworks: List of framework IDs for multi-framework imports (e.g., ["BIRD", "EBA_FINREP"]).
                    If None, falls back to sdd_context.current_frameworks.
                    Takes precedence over `framework` parameter.
    """
    # Handle backward compatibility: dpm=True maps to dataset_type="dpm"
    if dpm and dataset_type == "finrep":
        dataset_type = "dpm"

    # Set default file_dir based on dataset_type
    if file_dir is None:
        file_dir = "ancrdt_csv" if dataset_type == "ancrdt" else "technical_export"

    # Get frameworks from parameter or context (list takes precedence)
    effective_frameworks = frameworks
    effective_framework = framework

    # Check context for frameworks (list first, then single)
    if not effective_frameworks and hasattr(sdd_context, 'current_frameworks') and sdd_context.current_frameworks:
        effective_frameworks = sdd_context.current_frameworks
    if not effective_framework and not effective_frameworks and hasattr(sdd_context, 'current_framework'):
        effective_framework = sdd_context.current_framework

    # Create configuration with framework(s) for isolation
    config = DatasetConfig(
        dataset_type=dataset_type,
        file_directory=file_dir,
        framework=effective_framework,
        frameworks=effective_frameworks
    )

    # Build base_path
    base_path = os.path.join(sdd_context.file_directory, config.file_directory)

    # Import basic entities (always needed)
    import_maintenance_agencies(sdd_context, config)
    import_frameworks(sdd_context, config)
    import_domains(sdd_context, False, config)
    import_members(sdd_context, False, config)
    import_variables(sdd_context, False, config)

    # Import ANCRDT-specific entities if applicable
    if config.includes_subdomains:
        import_subdomains(base_path, sdd_context)

    if config.includes_cubes:
        if config.includes_subdomains:
            # subdomains must be imported before cube structures
            import_subdomain_enumerations(base_path, sdd_context)
        import_cube_structures(base_path, sdd_context)
        import_cube_structure_items(base_path, sdd_context)
        import_cubes(base_path, sdd_context)


    if config.includes_rendering_package:
        # Import rendering entities (tables, axes, cells)
        import_report_tables(sdd_context, config)
        import_axis(sdd_context, config)
        import_axis_ordinates(sdd_context, config)

        # Import table cells and related entities (conditional based on dataset type)
        if config.use_csv_copy:
            # Use optimized CSV copy imports for large datasets
            import_table_cells_csv_copy(sdd_context, config)
            import_ordinate_items_csv_copy(sdd_context, config)
            import_cell_positions_csv_copy(sdd_context, config)
        else:
            # Use standard imports
            import_table_cells(sdd_context, config=config)
            import_ordinate_items(sdd_context, config)
            import_cell_positions(sdd_context, config=config)
