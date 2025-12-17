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

from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from django.conf import settings

from pybirdai.models.workflow_model import WorkflowSession, DPMProcessExecution

from .execution import execute_dpm_step

logger = logging.getLogger(__name__)

def get_available_tables_for_selection(request):
    """
    API endpoint to get available tables for selection after Phase A.
    Reads table.csv and returns list of tables with metadata.
    """
    logger = logging.getLogger(__name__)

    try:
        import pandas as pd
        import os
        from django.conf import settings

        base_dir = settings.BASE_DIR
        table_csv_path = os.path.join(base_dir, 'results', 'technical_export', 'table.csv')

        if not os.path.exists(table_csv_path):
            return JsonResponse({
                'success': False,
                'error': 'table.csv not found. Please run Phase A first.'
            }, status=404)

        # Read table.csv
        tables_df = pd.read_csv(table_csv_path)

        # Convert to list of dictionaries
        tables = []
        for _, row in tables_df.iterrows():
            table_id = str(row.get('TABLE_ID', ''))
            # Use correct column names from table.csv: NAME and CODE (not TABLE_NAME and TABLE_CODE)
            table_name = str(row.get('NAME', ''))
            table_code = str(row.get('CODE', ''))
            version = str(row.get('VERSION', ''))
            description = str(row.get('DESCRIPTION', ''))

            # Extract framework from table_id (format: EBA_FRAMEWORK_CODE_VERSION)
            framework = ''
            if table_id.startswith('EBA_'):
                parts = table_id.split('_')
                if len(parts) >= 2:
                    framework = parts[1]

            tables.append({
                'table_id': table_id,
                'table_name': table_name,
                'table_code': table_code,
                'framework': framework,
                'version': version,
                'description': description
                # Note: TABLE_VID is intentionally excluded as it's an internal ID
            })

        logger.info(f"Returning {len(tables)} available tables for selection")

        return JsonResponse({
            'success': True,
            'tables': tables,
            'count': len(tables)
        })

    except Exception as e:
        logger.error(f"Error getting available tables: {e}", exc_info=True)
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)


def save_table_selection(request):
    """
    API endpoint to save table selection and continue with Phase B.
    Receives selected_tables and triggers Phase B execution.
    """
    logger = logging.getLogger(__name__)

    if request.method != 'POST':
        return JsonResponse({'error': 'POST required'}, status=405)

    try:
        import json

        # Get selected tables from JSON body
        data = json.loads(request.body)
        selected_tables = data.get('selected_tables', [])

        if not selected_tables:
            return JsonResponse({
                'success': False,
                'error': 'No tables selected'
            }, status=400)

        logger.info(f"Received table selection: {len(selected_tables)} tables")

        # Call execute_dpm_step with selected_tables to trigger Phase B
        # We need to create a new request object with the selected_tables
        from django.test import RequestFactory
        factory = RequestFactory()

        # Create new POST request with selected_tables
        new_request = factory.post('/workflow/dpm/execute/1/', {
            'selected_tables': json.dumps(selected_tables)
        })

        # Copy session from original request
        new_request.session = request.session

        # Execute Phase B
        response = execute_dpm_step(new_request, step_number=1)

        return response

    except Exception as e:
        logger.error(f"Error saving table selection: {e}", exc_info=True)
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)


def manage_table_presets(request):
    """
    API endpoint to manage table selection presets (get/save/delete).
    GET: Returns all saved presets
    POST: Saves a new preset
    DELETE: Deletes a preset
    """
    logger = logging.getLogger(__name__)

    try:
        # Get workflow session
        session_id = request.session.get('workflow_session_id')
        if not session_id:
            return JsonResponse({
                'success': False,
                'error': 'No active workflow session'
            }, status=400)

        workflow_session = WorkflowSession.objects.get(session_id=session_id)

        # Get DPM execution record (for storing presets)
        dpm_execution = DPMProcessExecution.objects.filter(
            session=workflow_session,
            step_number=1
        ).first()

        if not dpm_execution:
            return JsonResponse({
                'success': False,
                'error': 'DPM Step 1 execution not found'
            }, status=404)

        if request.method == 'GET':
            # Return all presets
            presets = dpm_execution.table_selection_presets or {}
            return JsonResponse({
                'success': True,
                'presets': presets
            })

        elif request.method == 'POST':
            # Save a new preset
            import json
            data = json.loads(request.body)
            preset_name = data.get('preset_name')
            selected_tables = data.get('table_ids', [])

            if not preset_name:
                return JsonResponse({
                    'success': False,
                    'error': 'Preset name required'
                }, status=400)

            # Get existing presets
            presets = dpm_execution.table_selection_presets or {}
            presets[preset_name] = selected_tables

            # Save to database
            dpm_execution.table_selection_presets = presets
            dpm_execution.save()

            logger.info(f"Saved preset '{preset_name}' with {len(selected_tables)} tables")

            return JsonResponse({
                'success': True,
                'message': f"Preset '{preset_name}' saved successfully"
            })

        elif request.method == 'DELETE':
            # Delete a preset
            import json
            data = json.loads(request.body)
            preset_name = data.get('preset_name')

            if not preset_name:
                return JsonResponse({
                    'success': False,
                    'error': 'Preset name required'
                }, status=400)

            presets = dpm_execution.table_selection_presets or {}
            if preset_name in presets:
                del presets[preset_name]
                dpm_execution.table_selection_presets = presets
                dpm_execution.save()

                logger.info(f"Deleted preset '{preset_name}'")

                return JsonResponse({
                    'success': True,
                    'message': f"Preset '{preset_name}' deleted successfully"
                })
            else:
                return JsonResponse({
                    'success': False,
                    'error': f"Preset '{preset_name}' not found"
                }, status=404)

        else:
            return JsonResponse({'error': 'Method not allowed'}, status=405)

    except Exception as e:
        logger.error(f"Error managing presets: {e}", exc_info=True)
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)
