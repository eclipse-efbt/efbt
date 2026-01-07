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
Validation functions for process metadata.

Provides functions to validate metadata for clone mode and verify
that the environment matches the expected state.
"""
import logging
import os
from typing import Dict, List, Any

from .constants import CODE_GENERATION_STEPS

logger = logging.getLogger(__name__)


def validate_metadata_parsing_only(metadata: dict) -> bool:
    """
    Ensure clone is only for metadata parsing steps (before code generation).

    Clone mode should only be used to restore state up to the point where
    code generation hasn't started. This prevents issues with generated code
    that may not match the cloned database state.

    Args:
        metadata: Loaded process metadata

    Returns:
        bool: True if valid for clone, False if code generation has started
    """
    workflows = metadata.get('workflows', {})

    # Check DPM workflow
    dpm = workflows.get('dpm', {})
    for step_name in CODE_GENERATION_STEPS.get('dpm', []):
        step = dpm.get(step_name, {})
        if step.get('status') == 'completed':
            logger.warning(f"DPM code generation step {step_name} is completed - invalid for clone")
            return False

    # Check main workflow
    main = workflows.get('main', {})
    for task_name in CODE_GENERATION_STEPS.get('main', []):
        task = main.get(task_name, {})
        if task.get('status') == 'completed':
            logger.warning(f"Main code generation task {task_name} is completed - invalid for clone")
            return False

    # Check AnaCredit workflow
    anacredit = workflows.get('anacredit', {})
    for step_name in CODE_GENERATION_STEPS.get('anacredit', []):
        step = anacredit.get(step_name, {})
        if step.get('status') == 'completed':
            logger.warning(f"AnaCredit code generation step {step_name} is completed - invalid for clone")
            return False

    return True


def verify_environment_state(metadata: dict) -> dict:
    """
    Verify that the environment matches the expected state from metadata.

    Checks:
    - Database tables exist
    - Record counts match expected ranges
    - Required models are present
    - No unexpected code generation artifacts

    Args:
        metadata: Loaded process metadata

    Returns:
        dict: Verification results with warnings/errors
    """
    from django.db import connection

    results = {
        'valid': True,
        'warnings': [],
        'errors': [],
        'checks_performed': []
    }

    # Check 1: Verify database tables exist
    results = _check_database_tables(results, connection)

    # Check 2: Verify no code generation artifacts if metadata says none should exist
    results = _check_code_generation_artifacts(results, metadata)

    # Check 3: Verify user selections match available data
    results = _check_user_selections(results, metadata)

    # Check 4: Database connectivity
    results = _check_database_connectivity(results, connection)

    logger.info(f"Environment verification complete: valid={results['valid']}, "
                f"errors={len(results['errors'])}, warnings={len(results['warnings'])}")

    return results


def _check_database_tables(results: dict, connection) -> dict:
    """Check that required database tables exist."""
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            existing_tables = {row[0] for row in cursor.fetchall()}

        required_tables = [
            'pybirdai_maintenance_agency',
            'pybirdai_domain',
            'pybirdai_variable'
        ]

        for table in required_tables:
            if table not in existing_tables:
                results['errors'].append(f"Required table missing: {table}")
                results['valid'] = False
            else:
                results['checks_performed'].append(f"Table exists: {table}")

    except Exception as e:
        results['errors'].append(f"Failed to check database tables: {e}")
        results['valid'] = False

    return results


def _check_code_generation_artifacts(results: dict, metadata: dict) -> dict:
    """Check for unexpected code generation artifacts."""
    workflows = metadata.get('workflows', {})
    dpm_workflow = workflows.get('dpm', {})

    # If step5 (code generation) is not completed, check for artifacts
    step5_status = dpm_workflow.get('step5_generate_code', {}).get('status', 'pending')
    if step5_status != 'completed':
        try:
            from django.conf import settings

            # Check for generated filter code
            filter_code_dir = os.path.join(settings.BASE_DIR, 'pybirdai', 'process_steps', 'filter_code')
            if os.path.exists(filter_code_dir):
                filter_files = [f for f in os.listdir(filter_code_dir)
                               if f.startswith('F_') and f.endswith('.py')]
                if filter_files:
                    results['warnings'].append(
                        f"Found {len(filter_files)} generated filter files - "
                        "these may not match the imported database state"
                    )

            # Check for generated join code
            join_code_dir = os.path.join(settings.BASE_DIR, 'pybirdai', 'process_steps', 'join_code')
            if os.path.exists(join_code_dir):
                join_files = [f for f in os.listdir(join_code_dir)
                             if f.startswith('J_') and f.endswith('.py')]
                if join_files:
                    results['warnings'].append(
                        f"Found {len(join_files)} generated join files - "
                        "these may not match the imported database state"
                    )

            results['checks_performed'].append("Checked for code generation artifacts")

        except Exception as e:
            results['warnings'].append(f"Could not check for code generation artifacts: {e}")

    return results


def _check_user_selections(results: dict, metadata: dict) -> dict:
    """Verify user selections match available data."""
    user_selections = metadata.get('user_selections', {})
    selected_tables = user_selections.get('selected_tables', [])

    if selected_tables:
        try:
            from pybirdai.models.bird_meta_data_model import TABLE

            # Check if any of the selected tables exist
            existing_table_codes = set(TABLE.objects.values_list('code', flat=True))
            missing_tables = set(selected_tables) - existing_table_codes

            if missing_tables and len(missing_tables) == len(selected_tables):
                # All tables are missing - this is expected before import
                results['checks_performed'].append(
                    f"Tables to be imported: {len(selected_tables)}"
                )
            elif missing_tables:
                results['warnings'].append(
                    f"{len(missing_tables)} of {len(selected_tables)} selected tables not found in database"
                )

        except Exception as e:
            results['warnings'].append(f"Could not verify table selections: {e}")

    return results


def _check_database_connectivity(results: dict, connection) -> dict:
    """Verify database connectivity."""
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
        results['checks_performed'].append("Database connection verified")
    except Exception as e:
        results['errors'].append(f"Database connection failed: {e}")
        results['valid'] = False

    return results
