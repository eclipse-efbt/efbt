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
#    Benjamin Arfa - Improvements and Refactoring

"""
ImportWebsiteToSDDModel class - orchestrator for SDD import operations.

This class serves as a thin wrapper around the import_func module,
maintaining backward compatibility with existing code while delegating
all actual import work to standalone functions in import_func/.
"""

from pybirdai.process_steps.website_to_sddmodel.import_func import (
    import_report_templates_from_sdd as _import_report_templates,
    import_semantic_integrations_from_sdd as _import_semantic_integrations,
    import_hierarchies_from_sdd as _import_hierarchies
)


class ImportWebsiteToSDDModel:
    """
    Orchestrator class for importing SDD CSV files into the analysis model.

    This class maintains the existing API for backward compatibility while
    delegating all import operations to the import_func module.

    All import logic has been refactored into standalone functions in the
    import_func/ submodule for better organization, testability, and
    maintainability.
    """

    def import_report_templates_from_sdd(self, sdd_context, dpm=False, framework=None, frameworks=None):
        """
        Import SDD CSV files for report templates into the analysis model.

        This method orchestrates the import of:
        - Maintenance agencies and frameworks
        - Domains, members, and variables
        - Report tables, axes, and axis ordinates
        - Table cells, ordinate items, and cell positions

        Framework Isolation:
        - When framework(s) specified, only deletes/replaces data for those frameworks
        - Other frameworks' data is preserved (e.g., importing FINREP won't delete COREP)

        Args:
            sdd_context: SDDContext containing file paths and configuration
            dpm: Boolean indicating if importing DPM data (uses CSV copy
                 method for better performance with large datasets)
            framework: Single framework ID for framework-isolated imports (e.g., "EBA_FINREP").
                       If None, falls back to sdd_context.current_framework.
            frameworks: List of framework IDs for multi-framework imports (e.g., ["BIRD", "EBA_FINREP"]).
                        If None, falls back to sdd_context.current_frameworks.
        """
        _import_report_templates(sdd_context, dpm=dpm, framework=framework, frameworks=frameworks)

    def import_semantic_integrations_from_sdd(self, sdd_context):
        """
        Import SDD CSV files for semantic integrations into the analysis model.

        This method orchestrates the import of:
        - Variable mappings and their items
        - Member mappings and their items
        - Mapping definitions
        - Mapping to cube relationships

        Args:
            sdd_context: SDDContext containing file paths and configuration
        """
        _import_semantic_integrations(sdd_context)

    def import_hierarchies_from_sdd(self, sdd_context):
        """
        Import hierarchies from CSV files.

        This method orchestrates the import of:
        - Member hierarchies
        - Parent-child member relationships
        - Member hierarchy nodes

        Args:
            sdd_context: SDDContext containing file paths and configuration
        """
        _import_hierarchies(sdd_context)
