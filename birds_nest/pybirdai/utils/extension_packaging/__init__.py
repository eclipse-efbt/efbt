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

"""
Extension Packaging Toolkit for BIRD Bench

This package provides utilities for packaging, versioning, and deploying
BIRD extensions as standalone Git repositories.
"""

from .extension_packager import ExtensionPackager
from .git_manager import GitManager
from .settings_extractor import SettingsExtractor
from .manifest_generator import ManifestGenerator
from .installer_generator import InstallerGenerator
from .readme_generator import ReadmeGenerator
from .dependency_analyzer import DependencyAnalyzer
from .pyproject_generator import PyProjectGenerator

__all__ = [
    'ExtensionPackager',
    'GitManager',
    'SettingsExtractor',
    'ManifestGenerator',
    'InstallerGenerator',
    'ReadmeGenerator',
    'DependencyAnalyzer',
    'PyProjectGenerator',
]