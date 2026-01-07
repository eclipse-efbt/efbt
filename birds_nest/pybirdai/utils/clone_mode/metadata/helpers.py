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
Helper functions for process metadata management.

Provides common utilities for timestamp formatting and status building
to reduce code duplication across workflow status functions.
"""
from datetime import datetime
from typing import Optional, Dict, Any


def utc_timestamp() -> str:
    """
    Generate an ISO-8601 UTC timestamp string.

    Returns:
        str: Current UTC time in ISO format with 'Z' suffix (e.g., '2025-01-07T12:00:00Z')
    """
    return datetime.utcnow().isoformat() + "Z"


def build_step_status(is_complete: bool) -> Dict[str, Any]:
    """
    Build a step status dictionary with completion info.

    Args:
        is_complete: Whether the step is completed

    Returns:
        dict: Status dict with 'status' and 'completed_at' keys
    """
    return {
        "status": "completed" if is_complete else "pending",
        "completed_at": utc_timestamp() if is_complete else None
    }


def build_steps_status_dict(step_completion_map: Dict[str, bool]) -> Dict[str, str]:
    """
    Build a steps_status dictionary from a map of step names to completion status.

    Args:
        step_completion_map: Dictionary mapping step names to boolean completion status

    Returns:
        dict: Dictionary mapping step names to status strings ('completed' or 'pending')
    """
    return {
        step_name: "completed" if is_complete else "pending"
        for step_name, is_complete in step_completion_map.items()
    }


def determine_workflow_progress(step_completions: list, step_names: list) -> tuple:
    """
    Determine the last completed step and overall status from a list of step completions.

    Args:
        step_completions: List of booleans indicating if each step is complete (in order)
        step_names: List of step name suffixes (e.g., ['task1', 'task2', 'task3', 'task4'])

    Returns:
        tuple: (last_step_number, overall_status_string)
    """
    last_step = 0
    overall_status = 'pending'

    for i, (is_complete, step_name) in enumerate(zip(step_completions, step_names), 1):
        if is_complete:
            last_step = i
            overall_status = f'{step_name}_complete'

    return last_step, overall_status


def parse_timestamp(timestamp_str: Optional[str]) -> Optional[datetime]:
    """
    Parse an ISO-8601 timestamp string to a datetime object.

    Handles the 'Z' suffix by converting to '+00:00' for Python's fromisoformat.

    Args:
        timestamp_str: ISO-8601 timestamp string, or None

    Returns:
        datetime object or None if parsing fails or input is None
    """
    if not timestamp_str:
        return None
    try:
        return datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
    except (ValueError, AttributeError):
        return None
