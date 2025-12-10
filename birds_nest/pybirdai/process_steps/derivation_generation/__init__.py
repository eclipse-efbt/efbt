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
Derivation Generation Module

This module provides functionality for generating Python derivation files
from ECB logical transformation rules.
"""

from .generate_derivation_from_csv import (
    DerivationCodeGenerator,
    load_transformation_rules_csv,
    load_derivation_config,
    generate_all_derivation_files,
    get_available_derivation_rules,
)

__all__ = [
    'DerivationCodeGenerator',
    'load_transformation_rules_csv',
    'load_derivation_config',
    'generate_all_derivation_files',
    'get_available_derivation_rules',
]
