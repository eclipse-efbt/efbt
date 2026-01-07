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
"""
Process Metadata submodule for clone mode.

This module provides functionality for generating, saving, loading, and validating
process_metadata.json files that track workflow execution state.
"""
from .constants import (
    SCHEMA_VERSION,
    WORKFLOW_STEP_COUNTS,
    COMPLETION_STEPS,
    VALID_RESTART_POINTS,
    CODE_GENERATION_STEPS,
)
from .helpers import utc_timestamp, build_step_status
from .workflow_status import (
    get_main_workflow_status,
    get_dpm_workflow_status,
    get_anacredit_workflow_status,
    empty_main_workflow,
    empty_dpm_workflow,
    empty_anacredit_workflow,
    determine_primary_framework,
    calculate_export_status,
    determine_last_step,
    get_user_selections,
    get_configuration,
)
from .validation import validate_metadata_parsing_only, verify_environment_state
from .restoration import restore_workflow_states

__all__ = [
    # Constants
    'SCHEMA_VERSION',
    'WORKFLOW_STEP_COUNTS',
    'COMPLETION_STEPS',
    'VALID_RESTART_POINTS',
    'CODE_GENERATION_STEPS',
    # Helpers
    'utc_timestamp',
    'build_step_status',
    # Workflow status
    'get_main_workflow_status',
    'get_dpm_workflow_status',
    'get_anacredit_workflow_status',
    'empty_main_workflow',
    'empty_dpm_workflow',
    'empty_anacredit_workflow',
    'determine_primary_framework',
    'calculate_export_status',
    'determine_last_step',
    'get_user_selections',
    'get_configuration',
    # Validation
    'validate_metadata_parsing_only',
    'verify_environment_state',
    # Restoration
    'restore_workflow_states',
]
