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
#

"""Orchestrator function for importing report templates from SDD."""

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


def import_report_templates_from_sdd(sdd_context, dpm=False):
    """
    Orchestrate the import of report templates from SDD CSV files.

    This function coordinates the import of all entities required for
    report templates, including metadata, tables, axes, and cells.

    Args:
        sdd_context: SDDContext containing file paths and dictionaries
        dpm: Boolean indicating if importing DPM data (default False)
    """
    import_maintenance_agencies(sdd_context)
    import_frameworks(sdd_context)
    import_domains(sdd_context, False)
    import_members(sdd_context, False)
    import_variables(sdd_context, False)

    import_report_tables(sdd_context)
    import_axis(sdd_context)
    import_axis_ordinates(sdd_context)

    if dpm:
        import_table_cells_csv_copy(sdd_context)
        import_ordinate_items_csv_copy(sdd_context)
        import_cell_positions_csv_copy(sdd_context)
    else:
        import_table_cells(sdd_context)
        import_ordinate_items(sdd_context)
        import_cell_positions(sdd_context)
