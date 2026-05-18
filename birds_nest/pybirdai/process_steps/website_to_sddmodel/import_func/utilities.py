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

"""Utility functions for SDD model import operations."""

import os
from django.conf import settings
from pathlib import Path


def replace_dots(text):
    """
    Replace dots with underscores in the given text.

    Args:
        text: String to process

    Returns:
        String with dots replaced by underscores
    """
    return text.replace('.', '_')


def optional_datetime(value):
    """Return None for blank optional datetime CSV values."""
    if value is None:
        return None

    value = str(value).strip()
    return value or None


def delete_hierarchy_warnings_files(context):
    """
    Delete warning files more efficiently using pathlib.

    Args:
        context: SDDContext containing output directory information
    """
    warnings_dir = Path(settings.BASE_DIR) / 'results' / 'generated_hierarchy_warnings'
    for file in warnings_dir.glob('*'):
        file.unlink()


def delete_mapping_warnings_files(context):
    """
    Delete mapping warning files.

    Args:
        context: SDDContext containing output directory information
    """
    base_dir = settings.BASE_DIR
    mapping_warnings_dir = os.path.join(base_dir, 'results', 'generated_mapping_warnings')
    for file in os.listdir(mapping_warnings_dir):
        os.remove(os.path.join(mapping_warnings_dir, file))
