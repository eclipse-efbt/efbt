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
ANCRDT Transformation Functions

This package contains modular functions for generating ANCRDT transformation code.
Each module focuses on a single responsibility (locality of behavior).

All code generation uses Python's AST module for syntactically guaranteed correctness.
"""

# Import all builder functions
from .module_builder import build_complete_module
from .create_output_class import create_output_class
from .create_output_table_class import create_output_table_class
from .create_union_item_class import create_union_item_class
from .create_base_class import create_base_class
from .create_union_table_class import create_union_table_class
from .create_join_class import create_join_class
from .create_join_table_class import create_join_table_class
from .create_filtered_class import create_filtered_class_pair

__all__ = [
    'build_complete_module',
    'create_output_class',
    'create_output_table_class',
    'create_union_item_class',
    'create_base_class',
    'create_union_table_class',
    'create_join_class',
    'create_join_table_class',
    'create_filtered_class_pair',
]
