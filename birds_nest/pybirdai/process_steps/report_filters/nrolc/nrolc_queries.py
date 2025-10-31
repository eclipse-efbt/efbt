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
Database query operations for NROLC (Non-Reference Output Layer Creator).
Handles all TABLE and FRAMEWORK queries.
"""

from pybirdai.models.bird_meta_data_model import TABLE, FRAMEWORK
from django.db.models import Q


class TableFrameworkQueries:
    """Handles database queries for tables and frameworks."""

    @staticmethod
    def get_framework_object(framework_id):
        """
        Get FRAMEWORK object from database.

        Args:
            framework_id: Framework identifier string

        Returns:
            FRAMEWORK instance or None if not found
        """
        try:
            return FRAMEWORK.objects.get(framework_id=framework_id)
        except FRAMEWORK.DoesNotExist:
            return None

    @staticmethod
    def get_tables_by_framework(framework):
        """
        Query tables by framework name.

        Args:
            framework: Framework identifier (e.g., 'EBA_FINREP')

        Returns:
            QuerySet of TABLE instances
        """
        framework_upper = framework.upper()
        tables = TABLE.objects.filter(table_id__contains=framework_upper)
        return tables

    @staticmethod
    def get_tables_by_framework_version(framework, version):
        """
        Query tables by framework name and version.

        Args:
            framework: Framework identifier (e.g., 'EBA_FINREP')
            version: Version string (e.g., '3.0.0')

        Returns:
            QuerySet of TABLE instances
        """
        framework_upper = framework.upper()
        tables = TABLE.objects.filter(
            table_id__contains=framework_upper).filter(
            table_id__contains="_" + version.replace(".", "_"))
        return tables

    @staticmethod
    def get_tables_by_code(table_code):
        """
        Get a specific table by its code.

        Args:
            table_code: Table code to search for

        Returns:
            TABLE instance or None if not found
        """
        table = TABLE.objects.filter(
            Q(table_id=table_code) |
            Q(name=table_code) |
            Q(code=table_code)
        ).first()
        return table

    @staticmethod
    def get_table_by_code_version(table_code, version):
        """
        Get a specific table by its code and version.

        Args:
            table_code: Table code to search for
            version: Version string

        Returns:
            TABLE instance or None if not found
        """
        table = TABLE.objects.filter(table_id__contains=version).filter(
            Q(table_id=table_code) |
            Q(name=table_code) |
            Q(code=table_code)
        ).first()
        return table
