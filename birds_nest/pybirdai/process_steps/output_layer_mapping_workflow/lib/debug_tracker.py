"""
Debug tracking utilities for output layer mapping workflow.

Provides a centralized way to track created objects during workflow execution
for debugging and validation purposes.
"""

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


def track_object(
    debug_data: Optional[Dict[str, List]],
    model_name: str,
    obj: Any,
    allow_duplicates: bool = False
) -> None:
    """
    Track a created object in debug_data dictionary.

    This utility eliminates boilerplate code like:
        if debug_data is not None:
            if 'MODEL' not in debug_data:
                debug_data['MODEL'] = []
            debug_data['MODEL'].append(obj)

    Args:
        debug_data: Dict to track objects (may be None to skip tracking)
        model_name: Key name for the model type (e.g., 'VARIABLE_MAPPING')
        obj: The object to track
        allow_duplicates: If False, skips adding if obj already in list

    Example:
        track_object(debug_data, 'CUBE', cube)
        track_object(debug_data, 'COMBINATION', combo, allow_duplicates=True)
    """
    if debug_data is None:
        return

    if model_name not in debug_data:
        debug_data[model_name] = []

    if allow_duplicates or obj not in debug_data[model_name]:
        debug_data[model_name].append(obj)


def track_objects(
    debug_data: Optional[Dict[str, List]],
    model_name: str,
    objects: List[Any],
    allow_duplicates: bool = False
) -> None:
    """
    Track multiple objects in debug_data dictionary.

    Args:
        debug_data: Dict to track objects (may be None to skip tracking)
        model_name: Key name for the model type
        objects: List of objects to track
        allow_duplicates: If False, skips adding duplicates

    Example:
        track_objects(debug_data, 'MEMBER', created_members)
    """
    if debug_data is None or not objects:
        return

    for obj in objects:
        track_object(debug_data, model_name, obj, allow_duplicates)


def track_message(
    debug_data: Optional[Dict[str, List]],
    key: str,
    message: str
) -> None:
    """
    Track a debug message in debug_data dictionary.

    Useful for tracking counts, status messages, or other non-object data.

    Args:
        debug_data: Dict to track messages (may be None to skip)
        key: Key name for the message category
        message: The message to track

    Example:
        track_message(debug_data, 'MAPPING_ORDINATE_LINK', '15 links created')
    """
    if debug_data is None:
        return

    if key not in debug_data:
        debug_data[key] = []

    debug_data[key].append(message)


def get_tracked_count(
    debug_data: Optional[Dict[str, List]],
    model_name: str
) -> int:
    """
    Get the count of tracked objects for a model type.

    Args:
        debug_data: Dict containing tracked objects
        model_name: Key name for the model type

    Returns:
        Number of tracked objects, or 0 if not tracked
    """
    if debug_data is None or model_name not in debug_data:
        return 0
    return len(debug_data[model_name])


def initialize_debug_data(model_names: List[str]) -> Dict[str, List]:
    """
    Initialize a debug_data dictionary with empty lists for specified models.

    Args:
        model_names: List of model names to initialize

    Returns:
        Dict with empty lists for each model name

    Example:
        debug_data = initialize_debug_data([
            'CUBE', 'CUBE_STRUCTURE', 'COMBINATION'
        ])
    """
    return {name: [] for name in model_names}


def summarize_debug_data(debug_data: Optional[Dict[str, List]]) -> str:
    """
    Create a summary string of tracked objects.

    Args:
        debug_data: Dict containing tracked objects

    Returns:
        Human-readable summary string

    Example output:
        "Created: 5 CUBE, 10 CUBE_STRUCTURE, 25 COMBINATION"
    """
    if debug_data is None:
        return "Debug tracking disabled"

    parts = []
    for key, items in debug_data.items():
        count = len(items)
        parts.append(f"{count} {key}")

    if not parts:
        return "No objects tracked"

    return "Created: " + ", ".join(parts)
