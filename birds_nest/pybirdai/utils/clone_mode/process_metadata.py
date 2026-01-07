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
Process Metadata Management for Clone Mode.

This module provides functionality for generating, saving, loading, and validating
process_metadata.json files that track workflow execution state for clone mode.

The process_metadata.json file captures:
- Workflow step statuses (completed, pending, in_progress)
- User selections (tables, frameworks, etc.)
- Configuration settings
- Timestamps for tracking progress

This enables clone mode to restore the exact state of a workflow and continue
from where it left off.

This is a thin orchestration layer - implementation details are in the
metadata/ submodule.
"""
import json
import os
import logging
from typing import Optional

# Import constants for backward compatibility
from .metadata.constants import (
    SCHEMA_VERSION,
    WORKFLOW_STEP_COUNTS,
    COMPLETION_STEPS,
    VALID_RESTART_POINTS,
    CODE_GENERATION_STEPS,
)

# Import helpers
from .metadata.helpers import utc_timestamp

# Import workflow status functions
from .metadata.workflow_status import (
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

# Import validation functions
from .metadata.validation import (
    validate_metadata_parsing_only,
    verify_environment_state,
)

# Import restoration
from .metadata.restoration import restore_workflow_states

logger = logging.getLogger(__name__)

# Re-export private functions with underscore prefix for backward compatibility
_get_main_workflow_status = get_main_workflow_status
_get_dpm_workflow_status = get_dpm_workflow_status
_get_anacredit_workflow_status = get_anacredit_workflow_status
_empty_main_workflow = empty_main_workflow
_empty_dpm_workflow = empty_dpm_workflow
_empty_anacredit_workflow = empty_anacredit_workflow
_determine_primary_framework = determine_primary_framework
_calculate_export_status = calculate_export_status
_determine_last_step = determine_last_step
_get_user_selections = get_user_selections
_get_configuration = get_configuration


def generate_process_metadata() -> dict:
    """
    Generate process_metadata.json from current database state.

    Queries Django models to determine:
    - Which workflow steps have been completed
    - User selections (tables, frameworks)
    - Configuration settings
    - Primary framework being used
    - Export completeness status

    Returns:
        dict: Process metadata dictionary conforming to the schema
    """
    # Get workflow statuses first to determine primary framework
    main_workflow = get_main_workflow_status()
    dpm_workflow = get_dpm_workflow_status()
    anacredit_workflow = get_anacredit_workflow_status()

    # Determine primary framework from active workflow
    primary_framework = determine_primary_framework(main_workflow, dpm_workflow, anacredit_workflow)

    # Build workflows dict
    workflows = {
        "main": main_workflow,
        "dpm": dpm_workflow,
        "anacredit": anacredit_workflow,
    }

    # Calculate export status
    export_status = calculate_export_status(workflows)

    metadata = {
        "version": SCHEMA_VERSION,
        "created_at": utc_timestamp(),
        "updated_at": utc_timestamp(),
        "primary_framework": primary_framework,
        "last_step_completed": None,
        "export_status": export_status,
        "workflows": workflows,
        "user_selections": get_user_selections(),
        "configuration": get_configuration(),
    }

    # Determine last completed step
    metadata["last_step_completed"] = determine_last_step(metadata["workflows"])

    return metadata


def save_process_metadata(output_path: str, metadata: Optional[dict] = None) -> str:
    """
    Save metadata to JSON file.

    Args:
        output_path: Directory path where to save the file
        metadata: Optional metadata dict; if None, generates from current state

    Returns:
        str: Full path to the saved file
    """
    if metadata is None:
        metadata = generate_process_metadata()

    # Update timestamp
    metadata["updated_at"] = utc_timestamp()

    # Ensure output directory exists
    os.makedirs(output_path, exist_ok=True)

    file_path = os.path.join(output_path, 'process_metadata.json')

    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)

    logger.info(f"Process metadata saved to: {file_path}")
    return file_path


def load_process_metadata(file_path: str) -> dict:
    """
    Load and validate process_metadata.json.

    Args:
        file_path: Path to the JSON file

    Returns:
        dict: Loaded and validated metadata

    Raises:
        FileNotFoundError: If file doesn't exist
        ValueError: If file is invalid or incompatible version
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Process metadata file not found: {file_path}")

    with open(file_path, 'r', encoding='utf-8') as f:
        metadata = json.load(f)

    # Validate version
    version = metadata.get('version')
    if not version:
        raise ValueError("Invalid process_metadata.json: missing version field")

    # Check for required fields
    required_fields = ['workflows', 'user_selections', 'configuration']
    for field in required_fields:
        if field not in metadata:
            raise ValueError(f"Invalid process_metadata.json: missing {field} field")

    logger.info(f"Loaded process metadata version {version} from: {file_path}")
    return metadata


def get_restart_point(metadata: dict) -> str:
    """
    Determine valid restart point from metadata.

    Args:
        metadata: Loaded process metadata

    Returns:
        str: The step/task name to restart from
    """
    return metadata.get('last_step_completed', 'MAIN_TASK1_DATABASE_SETUP')


# Export all public names
__all__ = [
    # Constants
    'SCHEMA_VERSION',
    'WORKFLOW_STEP_COUNTS',
    'COMPLETION_STEPS',
    'VALID_RESTART_POINTS',
    'CODE_GENERATION_STEPS',
    # Main functions
    'generate_process_metadata',
    'save_process_metadata',
    'load_process_metadata',
    'get_restart_point',
    'validate_metadata_parsing_only',
    'restore_workflow_states',
    'verify_environment_state',
    # Private functions (for backward compatibility)
    '_get_main_workflow_status',
    '_get_dpm_workflow_status',
    '_get_anacredit_workflow_status',
    '_empty_main_workflow',
    '_empty_dpm_workflow',
    '_empty_anacredit_workflow',
    '_determine_primary_framework',
    '_calculate_export_status',
    '_determine_last_step',
    '_get_user_selections',
    '_get_configuration',
]
