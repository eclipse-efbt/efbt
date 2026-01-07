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
CSV Data Importer submodule for clone mode.

This module provides functionality for importing BIRD data from CSV exports.
"""
from .column_mappings import build_column_mappings
from .bulk_operations import (
    bulk_sqlite_import_with_index,
    resolve_foreign_keys_post_bulk_import,
    fallback_csv_import,
    create_instances_from_csv_copy,
)
from .csv_utils import (
    parse_csv_content,
    convert_value,
    get_model_fields,
    validate_csv_file,
)
from .model_utils import (
    build_model_map,
    get_import_order,
    get_table_name_from_csv_filename,
    calculate_optimal_batch_size,
    is_high_volume_table,
    is_safe_table_name,
    should_import_table,
)

__all__ = [
    # Column mappings
    'build_column_mappings',
    # Bulk operations
    'bulk_sqlite_import_with_index',
    'resolve_foreign_keys_post_bulk_import',
    'fallback_csv_import',
    'create_instances_from_csv_copy',
    # CSV utilities
    'parse_csv_content',
    'convert_value',
    'get_model_fields',
    'validate_csv_file',
    # Model utilities
    'build_model_map',
    'get_import_order',
    'get_table_name_from_csv_filename',
    'calculate_optimal_batch_size',
    'is_high_volume_table',
    'is_safe_table_name',
    'should_import_table',
]
