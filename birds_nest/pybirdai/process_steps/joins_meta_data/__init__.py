# coding=UTF-8
# Copyright (c) 2025 Bird Software Solutions Ltd
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDX-License-Identifier: EPL-2.0
#
# Contributors:
#    Neil Mackenzie - initial API and implementation
#    Benjamin Arfa - improvements
#

"""
Joins Meta Data module for creating generation rules for reports and tables.

This module provides:
- BreakdownCondition: Parser for flexible product breakdown conditions
- JoinsConfigurationResolver: Resolver for configuration file discovery
- MainCategoryFinder: Maps of information related to EBA main categories
- JoinsMetaDataCreator: Generation rules for reports and tables
"""

from pybirdai.process_steps.joins_meta_data.condition_parser import BreakdownCondition
from pybirdai.process_steps.joins_meta_data.config_resolver import JoinsConfigurationResolver

__all__ = [
    'BreakdownCondition',
    'JoinsConfigurationResolver',
]
