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
#
# Entry point for framework-specific data deletion with orphan cleanup

import os
import logging

from django.conf import settings

from pybirdai.services.framework_selection import FrameworkSelectionService

logger = logging.getLogger(__name__)


class RunDeleteFrameworkData:
    """
    Entry point for deleting framework-specific data.

    This preserves the input model (DOMAIN, VARIABLE, MEMBER, etc.) and only
    deletes framework-specific output (CUBE, CUBE_LINK, MAPPING, etc.) plus any
    orphaned records that are no longer referenced.
    """

    path = os.path.join(settings.BASE_DIR, 'birds_nest')

    # Framework ID mappings for common frameworks
    FRAMEWORK_IDS = {
        'FINREP': 'FINREP_REF',
        'COREP': 'COREP_REF',
        'ANCRDT': 'ANCRDT',
        'AE': 'AE_REF',
        'FP': 'FP_REF',
        'SBP': 'SBP_REF',
        'PILLAR3': 'PILLAR3_REF',
    }

    @staticmethod
    def is_dataset_framework(framework_id: str) -> bool:
        """
        Check if a framework is a dataset framework (no rendering package).

        Uses the unified FrameworkSelectionService.

        Args:
            framework_id: The framework ID (e.g., 'ANCRDT')

        Returns:
            True if dataset framework, False otherwise
        """
        return FrameworkSelectionService.is_dataset_framework(framework_id)

    @staticmethod
    def get_tables_to_delete(framework_id: str) -> set:
        """
        Get the set of tables that should be considered for deletion.

        Uses the unified FrameworkSelectionService to determine which
        SMCubes categories apply to the framework.

        Args:
            framework_id: The framework ID

        Returns:
            Set of table names that can be deleted for this framework
        """
        return FrameworkSelectionService.get_allowed_tables_for_framework(framework_id)

    @staticmethod
    def run_delete_framework_data(framework_id: str):
        """
        Delete framework-specific output data and clean up orphaned records.

        Args:
            framework_id: The framework ID to delete (e.g., 'FINREP_REF', 'ANCRDT')

        Returns:
            dict: Summary of deleted records
        """
        # Log framework type for debugging
        is_dataset = RunDeleteFrameworkData.is_dataset_framework(framework_id)
        framework_type = "dataset" if is_dataset else "reporting"
        logger.info(f"Starting framework-specific deletion for: {framework_id} (type: {framework_type})")

        from pybirdai.context.sdd_context_django import SDDContext
        from pybirdai.context.context import Context

        from pybirdai.process_steps.joins_meta_data.delete_joins_meta_data import (
            TransformationMetaDataDestroyer
        )

        sdd_context = SDDContext()
        context = Context()
        context.input_directory = sdd_context.input_directory
        context.output_directory = sdd_context.output_directory

        destroyer = TransformationMetaDataDestroyer()
        result = destroyer.delete_framework_with_orphan_cleanup(
            context,
            sdd_context,
            framework_id
        )

        logger.info(f"Framework deletion complete for: {framework_id}")
        logger.info(f"Deletion summary: {result}")

        return result

    @staticmethod
    def run_delete_finrep():
        """Delete FINREP framework data."""
        return RunDeleteFrameworkData.run_delete_framework_data('FINREP_REF')

    @staticmethod
    def run_delete_corep():
        """Delete COREP framework data."""
        return RunDeleteFrameworkData.run_delete_framework_data('COREP_REF')

    @staticmethod
    def run_delete_ancrdt():
        """Delete AnaCredit framework data."""
        return RunDeleteFrameworkData.run_delete_framework_data('ANCRDT')

    @staticmethod
    def run_delete_dpm_frameworks(frameworks: list = None):
        """
        Delete DPM framework data for specified frameworks.

        Args:
            frameworks: List of framework names (e.g., ['FINREP', 'COREP'])
                       If None, deletes all common DPM frameworks.
        """
        if frameworks is None:
            frameworks = ['FINREP', 'COREP']

        results = {}
        for framework_name in frameworks:
            framework_id = RunDeleteFrameworkData.FRAMEWORK_IDS.get(
                framework_name,
                f'{framework_name}_REF'
            )
            try:
                result = RunDeleteFrameworkData.run_delete_framework_data(framework_id)
                results[framework_name] = {'success': True, 'result': result}
            except Exception as e:
                logger.error(f"Failed to delete framework {framework_name}: {e}")
                results[framework_name] = {'success': False, 'error': str(e)}

        return results
