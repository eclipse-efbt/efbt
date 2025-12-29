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

import os
import json
import logging
import traceback

from django.shortcuts import get_object_or_404
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from django.conf import settings
from django.utils import timezone

from pybirdai.models.workflow_model import WorkflowSession, DPMProcessExecution
from pybirdai.entry_points import (
    import_dpm_data,
    dpm_output_layer_creation,
    create_filters,
    create_joins_metadata,
)
from pybirdai.utils.datapoint_test_run.run_tests import RegulatoryTemplateTestRunner

logger = logging.getLogger(__name__)

def execute_dpm_step(request, step_number):
    """Execute a DPM process step"""
    logger = logging.getLogger(__name__)

    try:
        # Get workflow session
        session_id = request.session.get('workflow_session_id')
        if not session_id:
            return JsonResponse({
                'success': False,
                'error': 'No active workflow session found'
            })

        workflow_session = get_object_or_404(WorkflowSession, session_id=session_id)

        # Get or create DPM execution record
        dpm_execution, created = DPMProcessExecution.objects.get_or_create(
            session=workflow_session,
            step_number=step_number,
            defaults={
                'step_name': dict(DPMProcessExecution.STEP_CHOICES).get(step_number, f'Step {step_number}'),
                'status': 'pending'
            }
        )

        # Extract selected frameworks from request
        selected_frameworks = request.POST.getlist('frameworks')

        # Validate selected frameworks
        VALID_FRAMEWORKS = [
            'FINREP', 'COREP', 'AE', 'FP', 'SBP', 'REM', 'RES', 'PAY',
            'COVID19', 'IF', 'GSII', 'MREL', 'IMPRAC', 'ESG',
            'IPU', 'PILLAR3', 'IRRBB', 'DORA', 'FC', 'MICA'
        ]

        if selected_frameworks:
            # Check for invalid frameworks
            invalid_frameworks = [fw for fw in selected_frameworks if fw not in VALID_FRAMEWORKS]
            if invalid_frameworks:
                return JsonResponse({
                    'success': False,
                    'error': f'Invalid frameworks selected: {", ".join(invalid_frameworks)}. Valid frameworks are: {", ".join(VALID_FRAMEWORKS)}'
                })

            dpm_execution.selected_frameworks = selected_frameworks
            dpm_execution.save()

        # Mark as running
        dpm_execution.start_execution()

        # Execute the appropriate entry point based on step number
        # Wrap in try-finally to ensure status is ALWAYS updated, even if error handler fails
        try:
            if step_number == 1:
                # Step 1: Extract DPM Metadata (formerly Phase A)
                from pybirdai.entry_points.import_dpm_data import RunImportDPMData

                logger.info("Running DPM Step 1: Extracting metadata")

                result = RunImportDPMData.run_import_phase_a(frameworks=selected_frameworks or None)

                logger.info(f"Step 1 complete - {result.get('table_count', 0)} tables available for selection")

            elif step_number == 2:
                # Step 2: Process Selected Tables & Import (formerly Phase B + Step 2)
                # This step combines table processing (ordinate explosion) with database import
                from pybirdai.entry_points.import_dpm_data import RunImportDPMData
                import json

                # Check if selected_tables provided
                selected_tables_json = request.POST.get('selected_tables')
                selected_tables = json.loads(selected_tables_json) if selected_tables_json else None

                if not selected_tables:
                    # No tables selected - this should be caught by modal, but handle gracefully
                    return JsonResponse({
                        'success': False,
                        'error': 'No tables selected. Please select tables to process.'
                    })

                logger.info(f"Running DPM Step 2 with {len(selected_tables)} selected tables")
                dpm_execution.selected_tables = selected_tables
                dpm_execution.save()

                # Run Phase B: Process selected tables with ordinate explosion
                RunImportDPMData.run_import_phase_b(
                    selected_tables=selected_tables,
                    enable_table_duplication=True  # Enable Z-axis table duplication
                )

                logger.info("Table processing completed, database import already done in Phase B")

                # After importing DPM data, ensure Float subdomain exists for MTRC variable
                try:
                    from pybirdai.process_steps.output_layer_mapping_workflow.create_float_subdomain import (
                        ensure_float_subdomain_for_mtrc
                    )
                    float_result = ensure_float_subdomain_for_mtrc()
                    if float_result['success']:
                        logger.info(f"Float subdomain setup: {float_result['message']}")
                    else:
                        logger.warning(f"Float subdomain setup warning: {float_result['message']}")
                except Exception as e:
                    logger.warning(f"Float subdomain setup encountered an issue (non-critical): {str(e)}")

            elif step_number == 3:
                # Create Output Layers from DPM tables
                from pybirdai.entry_points.dpm_output_layer_creation import RunDPMOutputLayerCreation

                # Get optional parameters from request
                framework = request.POST.get('framework', '')
                version = request.POST.get('version', '')
                table_code = request.POST.get('table_code', '')
                table_codes = request.POST.get('table_codes', '')

                logger.info(f"Running output layer creation with params: framework={framework}, version={version}, table_code={table_code}, table_codes={table_codes}")

                # Call run_creation with parameters
                result = RunDPMOutputLayerCreation.run_creation(
                    framework=framework,
                    version=version,
                    table_code=table_code,
                    table_codes=table_codes
                )

                # Log results for multi-table processing
                if table_codes and isinstance(result, dict):
                    logger.info(f"Output layer creation result: {result.get('status')} - Processed: {len(result.get('processed', []))}, Errors: {len(result.get('errors', []))}")

                    # Check if there were errors
                    if result.get('status') == 'error' or (result.get('errors') and not result.get('processed')):
                        # All tables failed
                        error_details = result.get('errors', [])
                        error_msg = f"Failed to process {len(error_details)} table(s). "
                        if error_details:
                            first_error = error_details[0].get('error', 'Unknown error')
                            error_msg += f"First error: {first_error}"
                        elif result.get('message'):
                            error_msg = result.get('message')

                        logger.error(f"Output layer creation failed: {error_msg}")
                        dpm_execution.handle_error(error_msg)
                        return JsonResponse({
                            'success': False,
                            'status': 'failed',
                            'error': error_msg,
                            'details': error_details
                        })

                    elif result.get('status') == 'partial':
                        # Some tables succeeded, some failed
                        processed_count = len(result.get('processed', []))
                        error_count = len(result.get('errors', []))

                        logger.warning(f"Partial success: {processed_count} processed, {error_count} failed")
                        dpm_execution.complete_execution({
                            'completed_at': timezone.now().isoformat(),
                            'processed': processed_count,
                            'errors': error_count,
                            'details': result.get('errors', [])
                        })

                        return JsonResponse({
                            'success': True,
                            'status': 'partial',
                            'message': f'Processed {processed_count} table(s) successfully, {error_count} failed',
                            'details': {
                                'processed': processed_count,
                                'errors': error_count,
                                'error_list': result.get('errors', [])
                            }
                        })

            elif step_number == 4:
                # Create Transformation Rules - Generate filters and joins metadata for DPM output layers
                logger.info("DPM Step 4: Creating transformation rules (filters and joins metadata)...")

                from pybirdai.entry_points.create_filters import RunCreateFilters
                from pybirdai.entry_points.create_joins_metadata import RunCreateJoinsMetadata

                execution_data = {
                    'filters_created': False,
                    'joins_metadata_created': False,
                    'steps_completed': []
                }
                
                # Create joins metadata
                logger.info("Creating joins metadata for DPM output layer cubes...")
                RunCreateJoinsMetadata.run_create_joins_meta_data_DPM()
                execution_data['joins_metadata_created'] = True
                execution_data['steps_completed'].append('Joins metadata creation')

                logger.info("DPM Step 4 completed: Transformation rules created successfully")

                # Store execution data before completing
                dpm_execution.execution_data = execution_data
                dpm_execution.save()

            elif step_number == 5:
                # Generate Python Code - Create executable Python transformations for DPM
                logger.info("DPM Step 5: Generating Python code (executable filters and joins)...")

                from pybirdai.entry_points.run_create_executable_filters import RunCreateExecutableFilters
                from pybirdai.entry_points.create_executable_joins import RunCreateExecutableJoins

                execution_data = {
                    'filter_code_generated': False,
                    'join_code_generated': False,
                    'steps_completed': []
                }

                # Generate filter code
                logger.info("Generating executable filter Python code...")
                RunCreateExecutableFilters.run_create_executable_filters_from_db()
                execution_data['filter_code_generated'] = True
                execution_data['steps_completed'].append('Executable filter code generation')

                # Generate join code
                logger.info("Generating executable join Python code...")
                RunCreateExecutableJoins.create_python_joins_from_db()
                execution_data['join_code_generated'] = True
                execution_data['steps_completed'].append('Executable join code generation')

                logger.info("DPM Step 5 completed: Python code generated successfully")

                # Store execution data before completing
                dpm_execution.execution_data = execution_data
                dpm_execution.save()

            elif step_number == 6:
                # Execute DPM Tests - Run DPM test suite and validate results
                logger.info("DPM Step 6: Executing DPM test suite...")

                from pybirdai.utils.datapoint_test_run.run_tests import RegulatoryTemplateTestRunner

                execution_data = {
                    'tests_executed': False,
                    'steps_completed': []
                }

                # Look for DPM-specific test suite in tests/dpm/ directory
                tests_dir = 'tests/dpm'
                config_file_path = os.path.join(tests_dir, 'configuration_file_tests.json')

                if os.path.exists(config_file_path):
                    logger.info(f"Found DPM test suite configuration: {config_file_path}")

                    # Create test runner instance
                    test_runner = RegulatoryTemplateTestRunner(False)

                    # Configure test runner
                    test_runner.args.uv = "False"
                    test_runner.args.config_file = config_file_path
                    test_runner.args.dp_value = None
                    test_runner.args.reg_tid = None
                    test_runner.args.dp_suffix = None
                    test_runner.args.scenario = None
                    test_runner.args.suite_name = 'dpm'
                    test_runner.args.framework = "FINREP"

                    # Execute tests
                    logger.info(f"Executing DPM tests from config: {config_file_path}")
                    test_runner.main()
                    logger.info("Completed DPM test suite")

                    execution_data['tests_executed'] = True
                    execution_data['steps_completed'].append('DPM test suite executed')
                else:
                    logger.warning(f"No DPM test configuration found at {config_file_path}")
                    logger.info("Step 6 completed without test execution (no test suite configured)")
                    execution_data['tests_executed'] = False
                    execution_data['steps_completed'].append('No test suite found - skipped')

                # Store execution data before completing
                dpm_execution.execution_data = execution_data
                dpm_execution.save()

                logger.info("DPM Step 6 completed")

            else:
                raise ValueError(f'Invalid DPM step number: {step_number}')

            # Mark as completed (only if no errors or not multi-table)
            dpm_execution.complete_execution({
                'completed_at': timezone.now().isoformat()
            })

            return JsonResponse({
                'success': True,
                'status': 'completed',
                'message': f'DPM Step {step_number} completed successfully'
            })

        except Exception as e:
            logger.error(f"DPM Step {step_number} execution failed: {e}")
            dpm_execution.handle_error(str(e))
            return JsonResponse({
                'success': False,
                'error': str(e),
                'status': 'failed'
            })
        finally:
            # Safety net: If task is still 'running' after all error handling,
            # force it to 'failed' to prevent stuck status
            # This ensures tasks can NEVER remain stuck in 'running' status
            dpm_execution.refresh_from_db()
            if dpm_execution.status == 'running':
                logger.error(f"DPM Step {step_number} was still 'running' after execution - forcing to 'failed'")
                dpm_execution.status = 'failed'
                if not dpm_execution.error_message:
                    dpm_execution.error_message = 'Execution interrupted unexpectedly'
                dpm_execution.completed_at = timezone.now()
                dpm_execution.save(update_fields=['status', 'error_message', 'completed_at'])

    except Exception as e:
        logger.error(f"Error executing DPM step: {e}")
        return JsonResponse({
            'success': False,
            'error': str(e)
        })


def get_dpm_status(request):
    """Get DPM execution status"""
    try:
        session_id = request.session.get('workflow_session_id')
        if not session_id:
            return JsonResponse({
                'success': False,
                'error': 'No active workflow session found'
            })

        workflow_session = get_object_or_404(WorkflowSession, session_id=session_id)
        dpm_grid = get_dpm_task_grid(workflow_session)

        return JsonResponse({
            'success': True,
            'dpm_status': dpm_grid
        })

    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        })
