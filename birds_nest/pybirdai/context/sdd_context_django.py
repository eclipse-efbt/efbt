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
#    Benjamin Arfa - pipeline context integration
#

import os
from typing import Optional, List


class SDDContext:
    '''
    Documentation for Context
    '''
    # variables to configure the behaviour

    use_codes = True

    # Framework tracking - identifies which framework (FINREP, COREP, ANCRDT) is being processed
    current_framework = None

    # the directory where we get our input files
    file_directory = ""
    # the directory where we save our outputs.
    output_directory = ""

    subdomain_to_domain_map = {}
    subdomain_enumeration_dictionary = {}
    members_that_are_nodes = {}
    member_plus_hierarchy_to_child_literals = {}
    domain_to_hierarchy_dictionary = {}
    combinations_dictionary = {}
    member_dictionary = {}
    domain_dictionary = {}
    variable_dictionary= {}
    member_hierarchy_dictionary = {}
    member_hierarchy_node_dictionary = {}
    bird_cube_structure_dictionary = {}
    bird_cube_dictionary = {}
    bird_cube_structure_item_dictionary = {}
    bird_cube_structure_dictionary = {}

    combination_dictionary = {}
    combination_item_dictionary = {}
    combination_to_rol_cube_map = {}


    axis_ordinate_dictionary= {}
    table_cell_dictionary= {}
    table_to_table_cell_dictionary= {}
    member_mapping_dictionary = {}
    member_mapping_items_dictionary = {}
    cell_positions_dictionary = {}
    variable_set_enumeration_dictionary = {}
    report_tables_dictionary = {}
    axis_dictionary = {}
    variable_set_dictionary = {}
    mapping_definition_dictionary = {}
    mapping_to_cube_dictionary = {}
    variable_mapping_dictionary = {}
    variable_mapping_item_dictionary = {}
    variable_set_mappings = []
    agency_dictionary = {}
    framework_dictionary = {}
    subdomain_to_items_map = {}
    subdomain_dictionary = {}
    # For the reference output layers we record a map between variables
    # and domains
    variable_to_domain_map = {}
    variable_to_long_names_map = {}
    variable_to_primary_concept_map = {}

    combination_to_typ_instrmnt_map = {}
    table_to_combination_dictionary = {}



     # For the reference output layers we record a map between members ids
    # andtheir containing domains
    member_id_to_domain_map = {}

    # For the reference output layers we record a map between members ids
    # and their codes
    member_id_to_member_code_map = {}

    variable_set_to_variable_map = {}

    axis_ordinate_to_ordinate_items_map = {}

    finrep_output_cubes = {}
    ae_output_cubes = {}

    cube_link_dictionary = {}
    cube_link_to_foreign_cube_map = {}
    cube_structure_item_links_dictionary = {}
    cube_structure_item_link_to_cube_link_map = {}
    cube_link_to_join_identifier_map = {}
    cube_link_to_join_for_report_id_map = {}

    save_sdd_to_db = True

    exclude_reference_info_from_website = False

    # Pipeline tracking
    _current_pipeline: Optional[str] = None
    _current_frameworks: List[str] = []

    def __init__(self):
        pass

    # ========================================================================
    # Pipeline Context Methods
    # ========================================================================

    @property
    def current_pipeline(self) -> str:
        """
        Get the current pipeline.

        If not explicitly set, auto-detects from current_frameworks.

        Returns:
            Pipeline name ('main', 'ancrdt', or 'dpm')
        """
        if self._current_pipeline:
            return self._current_pipeline

        # Auto-detect from frameworks
        return self._detect_pipeline_from_frameworks()

    @current_pipeline.setter
    def current_pipeline(self, value: str):
        """Set the current pipeline explicitly."""
        from pybirdai.services.pipeline_repo_service import PIPELINES
        if value and value not in PIPELINES:
            raise ValueError(f"Invalid pipeline: {value}. Must be one of {PIPELINES}")
        self._current_pipeline = value

    @property
    def current_frameworks(self) -> List[str]:
        """Get the list of current frameworks being processed."""
        return self._current_frameworks

    @current_frameworks.setter
    def current_frameworks(self, value: List[str]):
        """Set the current frameworks and clear pipeline cache."""
        self._current_frameworks = value or []
        # Clear cached pipeline so it's re-detected
        self._current_pipeline = None

    def _detect_pipeline_from_frameworks(self) -> str:
        """
        Auto-detect pipeline from current frameworks.

        Returns:
            Pipeline name based on frameworks
        """
        from pybirdai.services.pipeline_repo_service import FRAMEWORK_PIPELINE_MAP

        if not self._current_frameworks:
            # Check the current_framework class variable as fallback
            if self.current_framework:
                framework_upper = self.current_framework.upper()
                return FRAMEWORK_PIPELINE_MAP.get(framework_upper, 'main')
            return 'main'

        # Check each framework in order
        for framework in self._current_frameworks:
            framework_upper = framework.upper()
            if framework_upper in FRAMEWORK_PIPELINE_MAP:
                return FRAMEWORK_PIPELINE_MAP[framework_upper]

        return 'main'

    def get_joins_configuration_path(self) -> str:
        """
        Get the joins_configuration path for the current pipeline.

        Returns:
            Absolute path to joins_configuration directory
        """
        from pybirdai.services.pipeline_repo_service import PipelineRepoService

        service = PipelineRepoService()
        return service.get_joins_path(self.current_pipeline)

    def get_database_export_path(self) -> str:
        """
        Get the database_export path for the current pipeline.

        Returns:
            Absolute path to database_export directory
        """
        from pybirdai.services.pipeline_repo_service import PipelineRepoService

        service = PipelineRepoService()
        return service.get_export_path(self.current_pipeline)

    def get_backup_path(self) -> str:
        """
        Get the backup directory path for the current pipeline.

        Returns:
            Absolute path to backup directory
        """
        from pybirdai.services.pipeline_repo_service import PipelineRepoService

        service = PipelineRepoService()
        return service.get_backup_path(self.current_pipeline)

    def set_pipeline_context(self, pipeline: str, frameworks: Optional[List[str]] = None):
        """
        Set both pipeline and frameworks context.

        Args:
            pipeline: Pipeline name ('main', 'ancrdt', or 'dpm')
            frameworks: Optional list of framework codes
        """
        self.current_pipeline = pipeline
        if frameworks:
            self._current_frameworks = frameworks
