"""Utilities for safely including user-controlled values in logs."""

from typing import Any


def sanitize_log_value(value: Any) -> str:
    """Collapse line breaks so user-controlled values cannot forge log entries."""
    if value is None:
        return "[none]"

    return (
        str(value)
        .replace("\r\n", "\\r\\n")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
    )
