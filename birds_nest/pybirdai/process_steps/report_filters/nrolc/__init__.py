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
NROLC (Non-Reference Output Layer Creator) package.
Creates output layer objects from DPM table renderings.
"""

from pybirdai.process_steps.report_filters.nrolc.nrolc_from_dpm import NonReferenceOutputLayerCreator

__all__ = ['NonReferenceOutputLayerCreator']
