"""
Join Configuration Views - AJAX API for Modal Editor
Handles loading, saving, and creating join configuration CSV files.
"""

import os
import json
import glob
import logging
from django.db.models import Q
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from pybirdai.process_steps.joins_configuration.joins_configuration_manager import JoinsConfigurationManager
from pybirdai.models.bird_meta_data_model import CUBE
from pybirdai.utils.secure_error_handling import SecureErrorHandler

logger = logging.getLogger(__name__)


def _framework_from_config_filename(filename: str, manager: JoinsConfigurationManager):
    """Extract the framework from a known joins-configuration CSV filename."""
    for file_def in manager.CSV_FILES.values():
        patterns = [file_def.get('filename_pattern')]
        patterns.extend(file_def.get('filename_pattern_by_model', {}).values())

        for pattern in [p for p in patterns if p]:
            prefix, marker, suffix = pattern.partition('{framework}')
            if not marker:
                continue
            if filename.startswith(prefix) and filename.endswith(suffix):
                framework = filename[len(prefix):len(filename) - len(suffix)]
                if framework:
                    return framework

    return None


def _tables_from_definition_csv(manager: JoinsConfigurationManager, framework: str,
                                data_model_type: str, search: str = ''):
    """Build available-table entries from the active definitions CSV."""
    is_ldm = data_model_type == 'ldm'
    main_table_column = 'LDM_ENTITY_CODE' if is_ldm else 'Main Table'
    display_name_column = 'LDM_ENTITY_NAME' if is_ldm else 'Main Table'
    related_tables_column = 'LINKED_ITEMS' if is_ldm else 'Related Tables'
    search_text = search.lower().strip()
    table_map = {}

    for row in manager.read_csv('product_il_definitions', framework):
        table_id = row.get(main_table_column, '').strip()
        table_name = row.get(display_name_column, '').strip() or table_id
        if table_id:
            table_map[table_id] = table_name

        for related_table in row.get(related_tables_column, '').split(':'):
            related_table = related_table.strip()
            if related_table and related_table not in table_map:
                table_map[related_table] = related_table

    tables = []
    for table_id, table_name in sorted(table_map.items()):
        if search_text and search_text not in table_id.lower() and search_text not in table_name.lower():
            continue
        tables.append({
            'id': table_id,
            'name': table_name,
            'code': table_id,
            'type': 'LDM' if is_ldm else 'EIL',
            'framework': None
        })

    return tables[:200]


def _json_error_response(message: str, status: int = 400):
    """Return a consistent JSON error payload."""
    return JsonResponse({
        "success": False,
        "error": message,
    }, status=status)


def _validation_error_response(message: str = "Invalid joins configuration request."):
    """Return a stable validation error without echoing exception details."""
    return _json_error_response(message, status=400)


def _internal_error_response(exception: Exception, context: str, request):
    """Hide implementation details from client-visible errors."""
    error_data = SecureErrorHandler.handle_exception(exception, context, request)
    return _json_error_response(error_data['message'], status=500)


@require_http_methods(["GET"])
def list_frameworks(request):
    """
    List all available frameworks (detected from existing CSV files).

    Returns:
        JSON: {
            "frameworks": ["FINREP_REF", "ANCRDT_REF", ...],
            "default": "FINREP_REF"
        }
    """
    manager = JoinsConfigurationManager()

    # Get frameworks from manager
    frameworks = manager.get_available_frameworks()

    # Also scan directory for any additional framework files
    base_path = manager.base_path
    if os.path.exists(base_path):
        csv_files = glob.glob(os.path.join(base_path, "*.csv"))
        for csv_file in csv_files:
            filename = os.path.basename(csv_file)
            framework = _framework_from_config_filename(filename, manager)
            if framework and framework not in frameworks:
                frameworks.append(framework)

    return JsonResponse({
        "success": True,
        "frameworks": sorted(frameworks),
        "default": "FINREP_REF",
        "data_model_type": manager.get_current_data_model_type()
    })


@require_http_methods(["POST"])
def load_csv(request):
    """
    Load CSV file content for editing.

    POST params:
        file_type: 'in_scope_reports', 'product_to_category', or 'product_il_definitions'
        framework: 'FINREP_REF', 'ANCRDT_REF', etc.

    Returns:
        JSON: {
            "success": true,
            "content": "CSV file content as string",
            "file_name": "filename.csv",
            "exists": true/false,
            "columns": ["col1", "col2", ...],
            "row_count": 123
        }
    """
    try:
        data = json.loads(request.body)
        file_type = data.get('file_type')
        framework = data.get('framework', 'FINREP_REF')

        if not file_type:
            return JsonResponse({
                "success": False,
                "error": "file_type is required"
            }, status=400)

        manager = JoinsConfigurationManager()
        file_def = manager.get_file_definition(file_type)

        # Check if file exists
        file_path = manager.get_file_path(file_type, framework)
        exists = os.path.exists(file_path)

        # Read content
        if exists:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
        else:
            # Return empty content with headers
            columns = file_def['columns']
            content = ','.join(columns) + '\n'

        # Get file info
        file_info = manager.get_file_info(file_type, framework)
        row_count = file_info['row_count'] if file_info else 0

        return JsonResponse({
            "success": True,
            "content": content,
            "file_name": os.path.basename(file_path),
            "exists": exists,
            "columns": file_def['columns'],
            "row_count": row_count,
            "description": file_def['description'],
            "data_model_type": file_def['data_model_type']
        })

    except ValueError as e:
        logger.info("Rejected joins configuration CSV load request: %s", e)
        return _validation_error_response("Invalid joins configuration file request.")
    except Exception as e:
        return _internal_error_response(e, "loading joins configuration CSV", request)


@require_http_methods(["POST"])
def save_csv(request):
    """
    Save CSV file content.

    POST params:
        file_type: 'in_scope_reports', 'product_to_category', or 'product_il_definitions'
        framework: 'FINREP_REF', 'ANCRDT_REF', etc.
        content: CSV file content as string

    Returns:
        JSON: {
            "success": true,
            "message": "File saved successfully",
            "backup_path": "/path/to/backup.bak"
        }
    """
    try:
        data = json.loads(request.body)
        file_type = data.get('file_type')
        framework = data.get('framework', 'FINREP_REF')
        content = data.get('content', '')

        if not file_type:
            return JsonResponse({
                "success": False,
                "error": "file_type is required"
            }, status=400)

        manager = JoinsConfigurationManager()
        file_path = manager.get_file_path(file_type, framework)

        # Create backup if file exists
        backup_path = None
        if os.path.exists(file_path):
            backup_path = manager.create_backup(file_type, framework)

        # Ensure directory exists
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        # Write content
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)

        return JsonResponse({
            "success": True,
            "message": f"File saved successfully: {os.path.basename(file_path)}",
            "backup_path": os.path.relpath(backup_path, manager.base_path) if backup_path else None
        })

    except ValueError as e:
        logger.info("Rejected joins configuration CSV save request: %s", e)
        return _validation_error_response("Invalid joins configuration save request.")
    except Exception as e:
        return _internal_error_response(e, "saving joins configuration CSV", request)


@require_http_methods(["POST"])
def create_framework(request):
    """
    Create CSV files for a new framework based on a template.

    POST params:
        framework_name: Name of new framework (e.g., "EBA_REF")
        template: "FINREP_REF" or "ANCRDT_REF" (which framework to copy from)

    Returns:
        JSON: {
            "success": true,
            "message": "Framework created successfully",
            "files_created": ["file1.csv", "file2.csv", ...]
        }
    """
    try:
        data = json.loads(request.body)
        framework_name = data.get('framework_name', '').strip()
        template = data.get('template', 'FINREP_REF')

        if not framework_name:
            return JsonResponse({
                "success": False,
                "error": "framework_name is required"
            }, status=400)

        # Validate framework name (alphanumeric and underscore only)
        if not framework_name.replace('_', '').isalnum():
            return JsonResponse({
                "success": False,
                "error": "Framework name must contain only letters, numbers, and underscores"
            }, status=400)

        manager = JoinsConfigurationManager()

        # Check if framework already exists
        existing_file = manager.get_file_path('in_scope_reports', framework_name)
        if os.path.exists(existing_file):
            return JsonResponse({
                "success": False,
                "error": f"Framework '{framework_name}' already exists"
            }, status=400)

        # Copy files from template
        file_types = ['in_scope_reports', 'product_to_category', 'product_il_definitions']
        files_created = []

        for file_type in file_types:
            # Read template file
            template_path = manager.get_file_path(file_type, template)

            if os.path.exists(template_path):
                with open(template_path, 'r', encoding='utf-8') as f:
                    content = f.read()

                # Write to new framework file
                new_path = manager.get_file_path(file_type, framework_name)
                os.makedirs(os.path.dirname(new_path), exist_ok=True)

                with open(new_path, 'w', encoding='utf-8') as f:
                    f.write(content)

                files_created.append(os.path.basename(new_path))

        return JsonResponse({
            "success": True,
            "message": f"Framework '{framework_name}' created successfully from {template} template",
            "files_created": files_created,
            "framework_name": framework_name
        })

    except ValueError as e:
        logger.info("Rejected joins configuration framework creation request: %s", e)
        return _validation_error_response("Invalid framework configuration request.")
    except Exception as e:
        return _internal_error_response(e, "creating joins configuration framework", request)


@require_http_methods(["GET"])
def get_file_info(request):
    """
    Get information about all CSV files for a framework.

    GET params:
        framework: Framework name

    Returns:
        JSON: {
            "success": true,
            "files": {
                "in_scope_reports": {"exists": true, "row_count": 48, "size": 1234, ...},
                ...
            }
        }
    """
    try:
        framework = request.GET.get('framework', 'FINREP_REF')
        manager = JoinsConfigurationManager()

        file_types = ['in_scope_reports', 'product_to_category', 'product_il_definitions']
        file_info = {}

        for file_type in file_types:
            file_def = manager.get_file_definition(file_type)
            info = manager.get_file_info(file_type, framework)
            file_info[file_type] = {
                'exists': info is not None,
                'row_count': info['row_count'] if info else 0,
                'size': info['size'] if info else 0,
                'modified': info['modified'].isoformat() if info and info.get('modified') else None,
                'description': file_def['description'],
                'file_name': os.path.basename(manager.get_file_path(file_type, framework)),
                'columns': file_def['columns'],
                'data_model_type': file_def['data_model_type']
            }

        return JsonResponse({
            "success": True,
            "framework": framework,
            "files": file_info
        })

    except ValueError as e:
        logger.info("Rejected joins configuration file info request: %s", e)
        return _validation_error_response("Invalid joins configuration file request.")
    except Exception as e:
        return _internal_error_response(e, "getting joins configuration file info", request)


@require_http_methods(["GET"])
def get_il_tables(request):
    """
    Get list of available IL (Input Layer) tables for the visual join editor.

    Returns all CUBE records that can be used as tables in join definitions.

    GET params:
        framework: Optional framework filter (e.g., 'ANCRDT', 'FINREP')
        search: Optional search term to filter by name/code

    Returns:
        JSON: {
            "success": true,
            "tables": [
                {"id": "INSTRMNT", "name": "Instrument", "code": "INSTRMNT", "type": "IL"},
                ...
            ]
        }
    """
    try:
        framework = request.GET.get('framework', '')
        search = request.GET.get('search', '').strip()
        manager = JoinsConfigurationManager()
        data_model_type = manager.get_current_data_model_type()
        input_model_framework = 'BIRD_ELDM' if data_model_type == 'ldm' else 'BIRD_EIL'

        # Query CUBE records
        queryset = CUBE.objects.filter(framework_id__framework_id=input_model_framework)

        # If the active input-model framework is not loaded yet, derive the
        # sidebar list from the active definitions CSV before trying broader
        # database fallbacks.
        if not queryset.exists():
            csv_tables = _tables_from_definition_csv(
                manager,
                framework or 'FINREP_REF',
                data_model_type,
                search
            )
            if csv_tables:
                return JsonResponse({
                    "success": True,
                    "tables": csv_tables,
                    "count": len(csv_tables),
                    "data_model_type": data_model_type,
                    "input_model_framework": input_model_framework,
                    "source": "definition_csv"
                })

        if not queryset.exists() and framework:
            queryset = CUBE.objects.filter(framework_id__framework_id__icontains=framework)
        if not queryset.exists():
            queryset = CUBE.objects.all()

        # Filter by search term if provided
        if search:
            queryset = queryset.filter(
                Q(cube_id__icontains=search) |
                Q(name__icontains=search) |
                Q(code__icontains=search)
            )

        # Order by cube_id and limit results
        queryset = queryset.order_by('cube_id')[:200]

        # Format results
        tables = []
        for cube in queryset:
            tables.append({
                'id': cube.cube_id,
                'name': cube.name or cube.cube_id,
                'code': cube.code or cube.cube_id,
                'type': cube.cube_type or 'CUBE',
                'framework': cube.framework_id.framework_id if cube.framework_id else None
            })

        return JsonResponse({
            "success": True,
            "tables": tables,
            "count": len(tables),
            "data_model_type": data_model_type,
            "input_model_framework": input_model_framework,
            "source": "database"
        })

    except ValueError as e:
        logger.info("Rejected joins configuration table request: %s", e)
        return _validation_error_response("Invalid joins configuration table request.")
    except Exception as e:
        return _internal_error_response(e, "listing joins configuration tables", request)


@require_http_methods(["GET"])
def get_filters_list(request):
    """
    Get list of available filters for join definitions.

    Returns filter identifiers that can be used in the Filter field.

    GET params:
        framework: Optional framework filter

    Returns:
        JSON: {
            "success": true,
            "filters": ["ADVNC", "OTHR_LN", "SCRTY_PSTN", ...]
        }
    """
    try:
        framework = request.GET.get('framework', 'FINREP_REF')
        manager = JoinsConfigurationManager()

        # Read existing IL definitions to extract filters
        data_model_type = manager.get_current_data_model_type()
        filter_column = 'FILTER' if data_model_type == 'ldm' else 'Filter'
        filters = set()

        for row in manager.read_csv('product_il_definitions', framework):
            filter_value = row.get(filter_column, '').strip()
            if filter_value:
                filters.add(filter_value)

        return JsonResponse({
            "success": True,
            "filters": sorted(list(filters)),
            "data_model_type": data_model_type
        })

    except Exception as e:
        return _internal_error_response(e, "listing joins configuration filters", request)
