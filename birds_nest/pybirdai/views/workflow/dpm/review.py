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
#
# Extracted from workflow_views.py

import logging
import json
import os

from django.shortcuts import render, redirect, get_object_or_404
from django.contrib import messages
from django.conf import settings

from pybirdai.models.workflow_model import WorkflowSession, DPMProcessExecution

logger = logging.getLogger(__name__)

def workflow_dpm_review(request, step_number):
    """Review page for DPM step execution results"""
    logger = logging.getLogger(__name__)

    try:
        # Get workflow session (optional for viewing artifacts)
        session_id = request.session.get('workflow_session_id')
        if session_id:
            try:
                workflow_session = WorkflowSession.objects.get(session_id=session_id)
            except WorkflowSession.DoesNotExist:
                workflow_session = None
        else:
            workflow_session = None

        # Show info message if no active session
        if not workflow_session:
            messages.info(request, 'No active workflow session. Showing available artifacts.')

        # Get DPM execution record (if it exists and we have a session)
        if workflow_session:
            try:
                dpm_execution = DPMProcessExecution.objects.get(
                    session=workflow_session,
                    step_number=step_number
                )
                execution_exists = True
            except DPMProcessExecution.DoesNotExist:
                dpm_execution = None
                execution_exists = False
                messages.info(request, f'DPM Step {step_number} has not been executed. Showing available artifacts.')
        else:
            # No workflow session, so no execution record
            dpm_execution = None
            execution_exists = False

        # Define step names for when execution doesn't exist
        STEP_NAMES = {
            1: 'Prepare DPM Data',
            2: 'Import DPM Data',
            3: 'Create Output Layers',
            4: 'Create Transformation Rules',
            5: 'Generate Python Code',
            6: 'Execute DPM Tests',
        }

        # Gather generated files based on step number
        import glob
        generated_files = []

        if step_number == 1:
            # Prepare DPM Data - check for CSV files in results/technical_export/
            csv_pattern = "results/technical_export/*.csv"
            generated_files = glob.glob(csv_pattern)
        elif step_number == 2:
            # Import DPM Data - check database records
            from pybirdai.models.bird_meta_data_model import FRAMEWORK, DOMAIN, MEMBER
            generated_files = [
                f"Frameworks: {FRAMEWORK.objects.count()} records",
                f"Domains: {DOMAIN.objects.count()} records",
                f"Members: {MEMBER.objects.count()} records",
            ]
        elif step_number == 3:
            # Create Output Layers - check for cubes and related structures
            from pybirdai.models.bird_meta_data_model import (
                CUBE, CUBE_STRUCTURE, CUBE_STRUCTURE_ITEM,
                COMBINATION, COMBINATION_ITEM
            )
            generated_files = [
                f"Cubes: {CUBE.objects.count()} records",
                f"Cube Structures: {CUBE_STRUCTURE.objects.count()} records",
                f"Cube Structure Items: {CUBE_STRUCTURE_ITEM.objects.count()} records",
                f"Combinations: {COMBINATION.objects.count()} records",
                f"Combination Items: {COMBINATION_ITEM.objects.count()} records",
            ]
        elif step_number == 4:
            # Create Transformation Rules - check for filters and joins metadata
            from pybirdai.models.bird_meta_data_model import (
                CUBE_LINK, CUBE_STRUCTURE_ITEM_LINK, MEMBER_LINK, TABLE_CELL
            )
            generated_files = [
                f"Cube Links: {CUBE_LINK.objects.count()} records",
                f"Cube Structure Item Links: {CUBE_STRUCTURE_ITEM_LINK.objects.count()} records",
                f"Member Links: {MEMBER_LINK.objects.count()} records",
                f"Table Cells: {TABLE_CELL.objects.count()} records",
            ]
        elif step_number == 5:
            # Generate Python Code - check for generated Python files
            filter_code_dir = os.path.join(settings.BASE_DIR, 'pybirdai', 'process_steps', 'filter_code')
            join_code_dir = os.path.join(settings.BASE_DIR, 'pybirdai', 'process_steps', 'join_code')

            filter_files = [os.path.basename(f) for f in glob.glob(os.path.join(filter_code_dir, 'F_*.py'))]
            join_files = [os.path.basename(f) for f in glob.glob(os.path.join(join_code_dir, 'J_*.py'))]

            filter_files.sort()
            join_files.sort()

            # Encode file lists for code editor
            encoded_filter_files = encode_file_list(filter_files)
            encoded_join_files = encode_file_list(join_files)

            generated_files = [
                f"Generated Filter Files: {len(filter_files)} Python files",
                f"Generated Join Files: {len(join_files)} Python files",
            ]

            # Add sample file names if they exist
            if filter_files:
                generated_files.append(f"Sample Filter: {filter_files[0]}")
            if join_files:
                generated_files.append(f"Sample Join: {join_files[0]}")
        elif step_number == 6:
            # Execute DPM Tests - check for test results and reports
            test_config_path = "tests/dpm/configuration_file_tests.json"
            test_results_pattern = "results/test_reports/dpm_*.html"

            test_config_exists = os.path.exists(test_config_path)
            test_reports = glob.glob(test_results_pattern)

            generated_files = [
                f"DPM Test Configuration: {'Found' if test_config_exists else 'Not Found'}",
                f"Test Reports Generated: {len(test_reports)} reports",
            ]

            # Add test execution summary from execution data if available
            if execution_exists and dpm_execution and dpm_execution.execution_data:
                tests_executed = dpm_execution.execution_data.get('tests_executed', False)
                generated_files.append(f"Tests Executed: {'Yes' if tests_executed else 'No'}")

                steps_completed = dpm_execution.execution_data.get('steps_completed', [])
                if steps_completed:
                    generated_files.append(f"Steps Completed: {len(steps_completed)}")

            # Add report file names if they exist
            if test_reports:
                for report in test_reports[:3]:  # Show first 3 reports
                    generated_files.append(f"Report: {os.path.basename(report)}")

        # Step 6: Load test results
        test_results_list = []
        total_tests = 0
        passed_tests = 0
        failed_tests = 0

        if step_number == 6:
            # Load test results from JSON files
            all_test_results = load_test_results()

            # Filter for DPM suite tests only
            test_results_list = [r for r in all_test_results if r.get('suite_name') == 'dpm']

            # Calculate statistics
            total_tests = len(test_results_list)
            for result in test_results_list:
                test_data = result.get('test_results', {})
                passed_list = test_data.get('passed', [])
                failed_list = test_data.get('failed', [])

                if passed_list:
                    passed_tests += len(passed_list) if isinstance(passed_list, list) else 1
                if failed_list:
                    failed_tests += len(failed_list) if isinstance(failed_list, list) else 1

        # Calculate duration if available
        duration = None
        if execution_exists and dpm_execution and dpm_execution.started_at and dpm_execution.completed_at:
            duration = dpm_execution.completed_at - dpm_execution.started_at

        context = {
            'step_number': step_number,
            'step_name': dpm_execution.step_name if (execution_exists and dpm_execution) else STEP_NAMES.get(step_number, f'Step {step_number}'),
            'execution': dpm_execution,
            'execution_exists': execution_exists,
            'status': dpm_execution.status if (execution_exists and dpm_execution) else 'not_executed',
            'started_at': dpm_execution.started_at if (execution_exists and dpm_execution) else None,
            'completed_at': dpm_execution.completed_at if (execution_exists and dpm_execution) else None,
            'duration': duration,
            'error_message': dpm_execution.error_message if (execution_exists and dpm_execution) else None,
            'execution_data': dpm_execution.execution_data if (execution_exists and dpm_execution) else None,
            'generated_files': generated_files,
            'workflow_type': 'dpm',
            # Step 5 specific context
            'encoded_filter_files': encoded_filter_files if step_number == 5 else None,
            'encoded_join_files': encoded_join_files if step_number == 5 else None,
            'filter_files_count': len(filter_files) if step_number == 5 else 0,
            'join_files_count': len(join_files) if step_number == 5 else 0,
            # Step 6 specific context
            'test_results': test_results_list,
            'total_tests': total_tests,
            'passed_tests': passed_tests,
            'failed_tests': failed_tests,
        }

        return render(request, 'pybirdai/workflow/dpm_review.html', context)

    except Exception as e:
        logger.error(f"Error in DPM review page: {e}")
        messages.error(request, f'Error loading review page: {str(e)}')
        return redirect('pybirdai:workflow_dashboard')
