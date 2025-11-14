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
ANCRDT Testing Module

This module provides reusable test utilities and helpers for ANCRDT
transformation testing. It can be imported by any test suite that needs to test
ANCRDT tables.

Components:
    - test_helpers.py: Reusable validation, assertion, and configuration loading functions

Note: Tests are self-contained and don't require pytest fixtures.
"""

# Make test helpers easily importable
from . import test_helpers

__all__ = ['test_helpers']
