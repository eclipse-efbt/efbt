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
#
"""
ANCRDT Transformation Utilities Package.

This package provides shared utility functions and classes for ANCRDT
transformation processes, eliminating code duplication across the module.

Modules:
    - constants: Shared constants and mappings
    - django_setup: Django environment configuration
    - logging_config: Standard logging configuration
    - file_lifecycle: Generated file lifecycle management
    - cube_utils: Cube and structure iteration utilities
    - database_utils: Common database query patterns
"""

# Export commonly used utilities for easy importing
from .constants import (
    DOMAIN_TYPE_MAP,
    ANCRDT_TABLE_PREFIX,
    ANCRDT_FRAMEWORK_ID,
    get_python_type
)
from .django_setup import DjangoSetup
from .logging_config import setup_ancrdt_logger, get_logger_for_module
from .file_lifecycle import GeneratedFileLifecycle
from .cube_utils import (
    iterate_cube_structure_items,
    filter_ancrdt_cube_links,
    filter_ancrdt_tables,
    get_ancrdt_cubes,
    is_ancrdt_cube
)

__all__ = [
    # Constants
    'DOMAIN_TYPE_MAP',
    'ANCRDT_TABLE_PREFIX',
    'ANCRDT_FRAMEWORK_ID',
    'get_python_type',
    # Django setup
    'DjangoSetup',
    # Logging
    'setup_ancrdt_logger',
    'get_logger_for_module',
    # File lifecycle
    'GeneratedFileLifecycle',
    # Cube utilities
    'iterate_cube_structure_items',
    'filter_ancrdt_cube_links',
    'filter_ancrdt_tables',
    'get_ancrdt_cubes',
    'is_ancrdt_cube',
]
