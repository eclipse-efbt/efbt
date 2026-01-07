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
CSV parsing and value conversion utilities for data import.
"""
import csv
import io
import logging
from typing import Tuple, List, Any, Optional

from django.db import models

logger = logging.getLogger(__name__)


def parse_csv_content(csv_content: str) -> Tuple[List[str], List[List[str]]]:
    """
    Parse CSV content and return headers and rows.

    Args:
        csv_content: CSV file content as a string

    Returns:
        Tuple of (headers list, rows list)
    """
    logger.debug("Parsing CSV content")
    csv_reader = csv.reader(io.StringIO(csv_content))
    headers = next(csv_reader, [])  # First row is headers (if present)
    rows = list(csv_reader)
    logger.debug(f"Parsed CSV with {len(headers)} headers and {len(rows)} rows")
    return headers, rows


def convert_value(field, value: str, defer_foreign_keys: bool = False) -> Any:
    """
    Convert CSV string value to appropriate Python type for the field.

    Args:
        field: Django model field instance
        value: String value from CSV
        defer_foreign_keys: If True, return raw FK values for later resolution

    Returns:
        Converted value appropriate for the field type
    """
    if not value or value == '' or value == 'None':
        return None

    if isinstance(field, models.CharField):
        return value
    if isinstance(field, models.IntegerField):
        return int(float(value))  # Handle cases where int comes as float string
    elif isinstance(field, models.FloatField):
        return float(value)
    elif isinstance(field, models.DecimalField):
        from decimal import Decimal
        return Decimal(str(value))
    elif isinstance(field, models.BooleanField):
        return value.lower() in ('true', '1', 'yes', 't')
    elif isinstance(field, models.DateField) or isinstance(field, models.DateTimeField):
        if value and value.strip():
            from django.utils.dateparse import parse_date, parse_datetime
            from django.utils import timezone
            from datetime import datetime

            parsed = parse_datetime(value) or parse_date(value)

            # For DateTimeField, ensure timezone-aware datetime
            if isinstance(field, models.DateTimeField) and parsed:
                if isinstance(parsed, datetime) and timezone.is_naive(parsed):
                    parsed = timezone.make_aware(parsed)
                elif not isinstance(parsed, datetime):
                    # parse_date returns date, convert to datetime for DateTimeField
                    parsed = timezone.make_aware(datetime.combine(parsed, datetime.min.time()))

            return parsed
        return None
    elif isinstance(field, models.ForeignKey):
        if defer_foreign_keys:
            # Return the raw ID value for later processing
            return value if value and str(value).strip() not in ('', 'None', 'NULL') else None
        else:
            # For foreign keys, we need to return the related object, not just the ID
            if value and str(value).strip() not in ('', 'None', 'NULL'):
                try:
                    fk_id = value
                    # Get the related model
                    related_model = field.related_model
                    # Try to get the object by its primary key
                    return related_model.objects.get(pk=fk_id)
                except (related_model.DoesNotExist, ValueError):
                    logger.warning(f"Foreign key object not found for {field.name} with value {value}")
                    return None
            return None
    else:
        return str(value) if value else None


def get_model_fields(model_class) -> dict:
    """
    Get model fields as a dictionary.

    Args:
        model_class: Django model class

    Returns:
        Dictionary mapping field names to field objects
    """
    return {field.name: field for field in model_class._meta.fields}


def validate_csv_file(csv_file_path: str, delimiter: str = ',') -> Tuple[bool, Optional[str], int]:
    """
    Validate that a CSV file exists and has proper content.

    Args:
        csv_file_path: Path to the CSV file
        delimiter: CSV delimiter character

    Returns:
        Tuple of (is_valid, error_message, column_count)
    """
    try:
        with open(csv_file_path, 'r', encoding='utf-8') as f:
            first_line = f.readline().strip()
            if not first_line:
                return False, f"CSV file {csv_file_path} is empty", 0

            # Check if file has reasonable number of columns
            headers = first_line.split(delimiter)
            if len(headers) < 1:
                return False, f"CSV file {csv_file_path} has no columns", 0

            logger.debug(f"CSV validation passed: {len(headers)} columns found in {csv_file_path}")
            return True, None, len(headers)

    except Exception as e:
        logger.error(f"CSV validation failed for {csv_file_path}: {e}")
        return False, str(e), 0
