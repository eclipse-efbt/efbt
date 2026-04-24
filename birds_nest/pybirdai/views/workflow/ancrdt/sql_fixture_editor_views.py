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
Views for editing ANCRDT SQL fixtures.

Provides a code editor interface for creating and editing SQL fixture files
used to populate the database before executing ANCRDT table transformations.
"""

import os
import json
import re
import shutil
from datetime import datetime
from django.shortcuts import render
from django.http import JsonResponse, HttpResponse, HttpResponseBadRequest
from django.conf import settings
from django.views.decorators.http import require_http_methods

from pybirdai.utils.secure_error_handling import SecureErrorHandler


# Base directory for SQL fixtures
SQL_FIXTURES_DIR = os.path.join(
    settings.BASE_DIR,
    'pybirdai',
    'process_steps',
    'ancrdt_transformation',
    'ancrdt_sql_fixtures'
)
TABLE_NAME_PATTERN = re.compile(r'^ANCRDT_[A-Z0-9_]+$')
FIXTURE_NAME_PATTERN = re.compile(r'^[a-zA-Z0-9_-]+$')


def _validate_table_name(table_name):
    """Allow only expected ANCRDT table identifiers in fixture paths."""
    if not isinstance(table_name, str) or not TABLE_NAME_PATTERN.fullmatch(table_name):
        raise ValueError('Invalid table name')

    return table_name


def _validate_fixture_name(fixture_name):
    """Allow only simple fixture slugs in fixture paths."""
    if not isinstance(fixture_name, str) or not FIXTURE_NAME_PATTERN.fullmatch(fixture_name):
        raise ValueError('Invalid fixture name. Use only letters, numbers, underscores, and hyphens.')

    return fixture_name


def _resolve_table_dir(table_name):
    """Resolve a table fixture directory and ensure it stays inside the fixtures root."""
    safe_table_name = _validate_table_name(table_name)
    fixtures_root = os.path.abspath(SQL_FIXTURES_DIR)
    table_dir = os.path.abspath(os.path.join(fixtures_root, safe_table_name))

    if os.path.commonpath([fixtures_root, table_dir]) != fixtures_root:
        raise ValueError('Invalid file path')

    return table_dir


def _resolve_fixture_path(table_name, fixture_name):
    """Resolve a fixture file path and ensure it stays inside the fixtures root."""
    safe_fixture_name = _validate_fixture_name(fixture_name)
    return os.path.join(_resolve_table_dir(table_name), f'{safe_fixture_name}.sql')


def _validation_error_response(message, status=400):
    """Return a consistent validation error payload."""
    return JsonResponse({'success': False, 'error': message}, status=status)


def _validation_exception_response(exception):
    """Return a generic validation error without echoing exception details."""
    SecureErrorHandler.handle_exception(exception, 'validating SQL fixture request')
    return _validation_error_response('Invalid request parameters.')


def _internal_error_response(exception, context, request):
    """Hide internal exception details from API consumers."""
    error_data = SecureErrorHandler.handle_exception(exception, context, request)
    return JsonResponse({'success': False, 'error': error_data['message']}, status=500)


def sql_fixtures_editor(request, table_name=None):
    """
    Main SQL fixtures editor view.

    Displays a unified editor interface with:
    - Sidebar showing tables and their fixtures
    - Code editor for editing SQL
    - Save/Revert/New fixture controls

    Args:
        request: Django HttpRequest
        table_name (str, optional): Pre-select a specific table

    Returns:
        HttpResponse: Rendered editor template
    """
    if table_name:
        try:
            table_name = _validate_table_name(table_name)
        except ValueError:
            return HttpResponseBadRequest('Invalid table name')

    # Get all ANCRDT tables (directories in fixtures folder)
    tables = []
    if os.path.exists(SQL_FIXTURES_DIR):
        for item in os.listdir(SQL_FIXTURES_DIR):
            item_path = os.path.join(SQL_FIXTURES_DIR, item)
            if os.path.isdir(item_path) and item.startswith('ANCRDT_'):
                # Get fixtures for this table
                fixtures = []
                for file in os.listdir(item_path):
                    if file.endswith('.sql'):
                        fixtures.append({
                            'name': file[:-4],  # Remove .sql extension
                            'filename': file
                        })

                tables.append({
                    'name': item,
                    'fixtures': sorted(fixtures, key=lambda x: x['name'])
                })

    context = {
        'tables': sorted(tables, key=lambda x: x['name']),
        'selected_table': table_name,
    }

    return render(request, 'pybirdai/workflow/ancrdt_workflow/sql_fixtures_editor.html', context)


@require_http_methods(["POST"])
def load_sql_fixture(request):
    """
    Load a SQL fixture file for editing.

    Request body:
    {
        "table_name": "ANCRDT_INSTRMNT_C_1",
        "fixture_name": "base_scenario"
    }

    Returns:
        JsonResponse: {
            "success": true,
            "content": "SQL file content",
            "file_path": "relative/path/to/file.sql"
        }
    """
    try:
        data = json.loads(request.body)
        table_name = data.get('table_name')
        fixture_name = data.get('fixture_name')

        if not table_name or not fixture_name:
            return JsonResponse({
                'success': False,
                'error': 'Missing table_name or fixture_name'
            }, status=400)

        file_path = _resolve_fixture_path(table_name, fixture_name)

        # Read file
        if not os.path.exists(file_path):
            return JsonResponse({
                'success': False,
                'error': f'Fixture file not found: {fixture_name}.sql'
            }, status=404)

        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()

        # Get relative path for display
        rel_path = os.path.relpath(file_path, SQL_FIXTURES_DIR)

        return JsonResponse({
            'success': True,
            'content': content,
            'file_path': rel_path,
            'table_name': table_name,
            'fixture_name': fixture_name
        })

    except json.JSONDecodeError:
        return _validation_error_response('Invalid JSON')
    except ValueError as e:
        return _validation_exception_response(e)
    except Exception as e:
        return _internal_error_response(e, 'loading SQL fixture', request)


@require_http_methods(["POST"])
def save_sql_fixture(request):
    """
    Save a SQL fixture file.

    Request body:
    {
        "table_name": "ANCRDT_INSTRMNT_C_1",
        "fixture_name": "base_scenario",
        "content": "SQL content to save"
    }

    Returns:
        JsonResponse: {
            "success": true,
            "message": "File saved successfully",
            "backup_created": true
        }
    """
    try:
        data = json.loads(request.body)
        table_name = data.get('table_name')
        fixture_name = data.get('fixture_name')
        content = data.get('content')

        if not table_name or not fixture_name or content is None:
            return JsonResponse({
                'success': False,
                'error': 'Missing required fields'
            }, status=400)

        file_path = _resolve_fixture_path(table_name, fixture_name)

        # Create table directory if it doesn't exist
        table_dir = _resolve_table_dir(table_name)
        os.makedirs(table_dir, exist_ok=True)

        # Create backup if file exists
        backup_created = False
        if os.path.exists(file_path):
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_path = f'{file_path}.backup_{timestamp}'
            shutil.copy2(file_path, backup_path)
            backup_created = True

        # Write file
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)

        return JsonResponse({
            'success': True,
            'message': f'Saved {fixture_name}.sql successfully',
            'backup_created': backup_created
        })

    except json.JSONDecodeError:
        return _validation_error_response('Invalid JSON')
    except ValueError as e:
        return _validation_exception_response(e)
    except Exception as e:
        return _internal_error_response(e, 'saving SQL fixture', request)


@require_http_methods(["POST"])
def create_sql_fixture(request):
    """
    Create a new SQL fixture file.

    Request body:
    {
        "table_name": "ANCRDT_INSTRMNT_C_1",
        "fixture_name": "new_scenario"
    }

    Returns:
        JsonResponse: {
            "success": true,
            "message": "Fixture created successfully",
            "content": "-- New fixture template"
        }
    """
    try:
        data = json.loads(request.body)
        table_name = data.get('table_name')
        fixture_name = data.get('fixture_name')

        if not table_name or not fixture_name:
            return JsonResponse({
                'success': False,
                'error': 'Missing table_name or fixture_name'
            }, status=400)

        file_path = _resolve_fixture_path(table_name, fixture_name)

        # Check if file already exists
        if os.path.exists(file_path):
            return JsonResponse({
                'success': False,
                'error': f'Fixture {fixture_name}.sql already exists'
            }, status=409)

        # Create table directory if needed
        table_dir = _resolve_table_dir(table_name)
        os.makedirs(table_dir, exist_ok=True)

        # Create template content
        template_content = f"""-- {table_name} - {fixture_name}
-- Created: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
--
-- This fixture populates test data for {table_name} table execution.
-- All records use test_id = '1' for easy cleanup.

-- Insert party (required for foreign keys)
INSERT INTO pybirdai_prty (
    test_id, PRTY_uniqueID, PRTY_ID, DT_RFRNC, RPRTNG_AGNT_ID
) VALUES (
    '1', 'PARTY_1_2024-01-01_AGENT1', 'PARTY_1',
    CAST('2024-01-01' AS DATETIME), 'AGENT1'
);

-- TODO: Add your SQL INSERT statements here
"""

        # Write file
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(template_content)

        return JsonResponse({
            'success': True,
            'message': f'Created {fixture_name}.sql successfully',
            'content': template_content,
            'table_name': table_name,
            'fixture_name': fixture_name
        })

    except json.JSONDecodeError:
        return _validation_error_response('Invalid JSON')
    except ValueError as e:
        return _validation_exception_response(e)
    except Exception as e:
        return _internal_error_response(e, 'creating SQL fixture', request)


@require_http_methods(["POST"])
def delete_sql_fixture(request):
    """
    Delete a SQL fixture file.

    Request body:
    {
        "table_name": "ANCRDT_INSTRMNT_C_1",
        "fixture_name": "old_scenario"
    }

    Returns:
        JsonResponse: {
            "success": true,
            "message": "Fixture deleted successfully"
        }
    """
    try:
        data = json.loads(request.body)
        table_name = data.get('table_name')
        fixture_name = data.get('fixture_name')

        if not table_name or not fixture_name:
            return JsonResponse({
                'success': False,
                'error': 'Missing table_name or fixture_name'
            }, status=400)

        file_path = _resolve_fixture_path(table_name, fixture_name)

        # Delete file
        if os.path.exists(file_path):
            # Create backup before deleting
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_path = f'{file_path}.deleted_{timestamp}'
            shutil.move(file_path, backup_path)

            return JsonResponse({
                'success': True,
                'message': f'Deleted {fixture_name}.sql (backup created)',
                'backup_path': os.path.relpath(backup_path, SQL_FIXTURES_DIR)
            })
        else:
            return JsonResponse({
                'success': False,
                'error': 'Fixture file not found'
            }, status=404)

    except json.JSONDecodeError:
        return _validation_error_response('Invalid JSON')
    except ValueError as e:
        return _validation_exception_response(e)
    except Exception as e:
        return _internal_error_response(e, 'deleting SQL fixture', request)


def list_sql_fixtures(request, table_name):
    """
    List all SQL fixtures for a given table.

    Args:
        request: Django HttpRequest
        table_name (str): ANCRDT table name

    Returns:
        JsonResponse: {
            "success": true,
            "fixtures": [{"name": "...", "size": ..., "modified": "..."}]
        }
    """
    try:
        table_dir = _resolve_table_dir(table_name)

        if not os.path.exists(table_dir):
            return JsonResponse({
                'success': True,
                'fixtures': []
            })

        fixtures = []
        for file in os.listdir(table_dir):
            if file.endswith('.sql') and not file.startswith('.'):
                file_path = os.path.join(table_dir, file)
                stat = os.stat(file_path)

                fixtures.append({
                    'name': file[:-4],  # Remove .sql
                    'filename': file,
                    'size': stat.st_size,
                    'modified': datetime.fromtimestamp(stat.st_mtime).strftime('%Y-%m-%d %H:%M:%S')
                })

        return JsonResponse({
            'success': True,
            'fixtures': sorted(fixtures, key=lambda x: x['name'])
        })

    except ValueError as e:
        return _validation_exception_response(e)
    except Exception as e:
        return _internal_error_response(e, 'listing SQL fixtures', request)
