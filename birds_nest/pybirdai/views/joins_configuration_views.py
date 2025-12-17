"""
Join Configuration Views - AJAX API for Modal Editor
Handles loading, saving, and creating join configuration CSV files.
"""

import os
import json
import glob
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from django.views.decorators.csrf import csrf_exempt
from pybirdai.process_steps.joins_configuration.joins_configuration_manager import JoinsConfigurationManager
from pybirdai.models.bird_meta_data_model import CUBE


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
            # Extract framework from filename pattern: *_{FRAMEWORK}.csv
            if '_' in filename:
                framework = filename.rsplit('_', 1)[-1].replace('.csv', '')
                if framework not in frameworks:
                    frameworks.append(framework)

    return JsonResponse({
        "success": True,
        "frameworks": sorted(frameworks),
        "default": "FINREP_REF"
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

        # Check if file exists
        file_path = manager.get_file_path(file_type, framework)
        exists = os.path.exists(file_path)

        # Read content
        if exists:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
        else:
            # Return empty content with headers
            columns = manager.get_columns(file_type, framework)
            content = ','.join(columns) + '\n'

        # Get file info
        file_info = manager.get_file_info(file_type, framework)
        row_count = file_info['row_count'] if file_info else 0

        return JsonResponse({
            "success": True,
            "content": content,
            "file_name": os.path.basename(file_path),
            "exists": exists,
            "columns": manager.get_columns(file_type, framework),
            "row_count": row_count
        })

    except Exception as e:
        return JsonResponse({
            "success": False,
            "error": str(e)
        }, status=500)


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
            "backup_path": backup_path
        })

    except Exception as e:
        return JsonResponse({
            "success": False,
            "error": f"Error saving file: {str(e)}"
        }, status=500)


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

    except Exception as e:
        return JsonResponse({
            "success": False,
            "error": f"Error creating framework: {str(e)}"
        }, status=500)


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
            info = manager.get_file_info(file_type, framework)
            file_info[file_type] = {
                'exists': info is not None,
                'row_count': info['row_count'] if info else 0,
                'size': info['size'] if info else 0,
                'modified': info['modified'].isoformat() if info and info.get('modified') else None,
                'description': manager.CSV_FILES[file_type]['description']
            }

        return JsonResponse({
            "success": True,
            "framework": framework,
            "files": file_info
        })

    except Exception as e:
        return JsonResponse({
            "success": False,
            "error": str(e)
        }, status=500)


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

        # Query CUBE records
        queryset = CUBE.objects.all()

        # Filter by framework if provided
        if framework:
            queryset = queryset.filter(
                framework_id__framework_id__icontains=framework
            )

        # Filter by search term if provided
        if search:
            queryset = queryset.filter(
                cube_id__icontains=search
            ) | queryset.filter(
                name__icontains=search
            ) | queryset.filter(
                code__icontains=search
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
            "count": len(tables)
        })

    except Exception as e:
        return JsonResponse({
            "success": False,
            "error": str(e)
        }, status=500)


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
        file_path = manager.get_file_path('product_il_definitions', framework)
        filters = set()

        if os.path.exists(file_path):
            with open(file_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
                for line in lines[1:]:  # Skip header
                    parts = line.strip().split(',')
                    if len(parts) >= 3 and parts[2]:
                        filters.add(parts[2])

        return JsonResponse({
            "success": True,
            "filters": sorted(list(filters))
        })

    except Exception as e:
        return JsonResponse({
            "success": False,
            "error": str(e)
        }, status=500)
