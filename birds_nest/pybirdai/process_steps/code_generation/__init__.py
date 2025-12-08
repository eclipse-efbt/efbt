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
Code Generation Package for DPM Template Execution Code.

This package provides generators for creating executable Python code
from DPM (Data Point Model) metadata, following the FINREP pattern
of report_cells.py and *_logic.py files.

Main components:
- DPMLogicGenerator: Generates *_logic.py files with inheritance
- DPMReportCellsGenerator: Generates dpm_{framework}_report_cells.py files
"""

from .dpm_logic_generator import DPMLogicGenerator
from .dpm_report_cells_generator import DPMReportCellsGenerator

__all__ = ['DPMLogicGenerator', 'DPMReportCellsGenerator']
