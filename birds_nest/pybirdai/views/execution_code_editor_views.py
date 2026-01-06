"""
Views for execution code editing workflow.
Provides functionality to review and edit generated execution code (_logic.py files)
with a feedback loop for iterative refinement.
"""

import ast
import json
import os
import zlib
import binascii
import logging
from pathlib import Path
from django.shortcuts import render, redirect
from django.http import JsonResponse, HttpResponse
from django.views.decorators.http import require_http_methods
from django.contrib import messages
from django.conf import settings
from django.utils import timezone
from pybirdai.models import AnaCreditProcessExecution, CUBE_LINK, MEMBER_MAPPING, CUBE_STRUCTURE_ITEM_LINK
from pybirdai.entry_points.create_executable_joins import RunCreateExecutableJoins
from pybirdai.entry_points.create_joins_metadata import RunCreateJoinsMetadata
from pybirdai.views.workflow.code_sync import CodeSyncManager

logger = logging.getLogger(__name__)


def _decode_file_list(hex_string):
    """
    Decode and decompress a hex-encoded file list from URL parameter.

    Args:
        hex_string: Hex-encoded compressed file list

    Returns:
        List of filenames, or None if decoding fails
    """
    if not hex_string:
        return None

    try:
        # Convert from hex to bytes
        compressed = binascii.unhexlify(hex_string)

        # Decompress
        file_string = zlib.decompress(compressed).decode('utf-8')

        # Split by pipe separator
        file_list = file_string.split("|")

        return file_list
    except Exception as e:
        logger.error(f"Error decoding file list: {e}")
        return None


def _get_source_directory(source='joins', framework=None):
    """
    Get the directory path based on the source type.

    Args:
        source: 'joins' for generated joins, 'filters' for filter_code, 'report_cells' for report cells
        framework: Optional framework name (e.g., 'COREP', 'FINREP', 'ANCRDT') for unified structure

    Returns:
        Absolute path to the source directory
    """
    if source == 'filters':
        return os.path.join(settings.BASE_DIR, 'pybirdai', 'process_steps', 'filter_code')
    elif framework:
        # Use unified folder structure based on framework type
        # New structure (2025): results/generated_python/{type}/{FRAMEWORK}/{subdir}/
        framework_upper = framework.upper().replace('_REF', '')

        # Determine code type based on framework
        if framework_upper in ['ANCRDT', 'ANACREDIT']:
            code_type = 'datasets'
            framework_upper = 'ANCRDT'
        else:
            code_type = 'templates'

        # Map source to subdirectory
        subdir_map = {'joins': 'joins', 'filter': 'filter', 'report_cells': 'filter'}
        subdir = subdir_map.get(source, source)

        new_path = os.path.join(settings.BASE_DIR, 'results', 'generated_python', code_type, framework_upper, subdir)

        # Check if new structure exists, otherwise fall back to legacy
        if os.path.exists(new_path):
            return new_path

        # Try legacy unified structure: results/generated_python/{FRAMEWORK}/{type}/
        legacy_unified = os.path.join(settings.BASE_DIR, 'results', 'generated_python', framework_upper, source)
        if os.path.exists(legacy_unified):
            return legacy_unified

        # Fall back to legacy flat structure
        legacy_flat = os.path.join(settings.BASE_DIR, 'results', 'generated_python_joins')
        if os.path.exists(legacy_flat):
            return legacy_flat

        # Return new path even if it doesn't exist (it will be created)
        return new_path
    else:
        # No framework specified - check for new structure first, then legacy
        new_path = os.path.join(settings.BASE_DIR, 'results', 'generated_python')
        legacy_path = os.path.join(settings.BASE_DIR, 'results', 'generated_python_joins')

        # Prefer new if exists, otherwise legacy
        if os.path.exists(new_path):
            return new_path
        return legacy_path


def review_joins_metadata(request, step=2):
    """
    Review the generated joins metadata after Step 2 of ANCRDT workflow.
    Provides links to existing editors for cube links, member mappings, etc.
    """
    context = {
        'step': step,
        'cube_links_count': CUBE_LINK.objects.count(),
        'member_mappings_count': MEMBER_MAPPING.objects.count(),
        'cube_structure_links_count': CUBE_STRUCTURE_ITEM_LINK.objects.count(),
    }

    # Get the latest ANCRDT execution
    try:
        latest_execution = AnaCreditProcessExecution.objects.filter(
            step_number=2
        ).latest('started_at')
        context['execution'] = latest_execution
        context['joins_metadata_approved'] = getattr(latest_execution, 'joins_metadata_approved', False)
    except AnaCreditProcessExecution.DoesNotExist:
        context['execution'] = None
        context['joins_metadata_approved'] = False

    return render(request, 'pybirdai/workflow/shared/execution_code_editing/review_joins_metadata.html', context)


def regenerate_execution_code(request, step=3):
    """
    Regenerate execution code after editing joins metadata.
    """
    if request.method == 'POST':
        try:
            # Mark joins metadata as approved
            latest_execution = AnaCreditProcessExecution.objects.filter(
                step_number=2
            ).latest('started_at')
            if hasattr(latest_execution, 'joins_metadata_approved'):
                latest_execution.joins_metadata_approved = True
                latest_execution.save()

            # Regenerate execution code
            RunCreateExecutableJoins.create_python_joins_from_db()

            messages.success(request, "Execution code regenerated successfully!")
            return redirect('pybirdai:review_execution_code', step=step)
        except Exception as e:
            messages.error(request, f"Error regenerating execution code: {str(e)}")
            return redirect('pybirdai:review_joins_metadata', step=2)

    return redirect('pybirdai:review_joins_metadata', step=2)


def edit_execution_code(request, file_name, source='joins'):
    """
    Main execution code editor interface.
    Displays a tree view of classes/methods and allows editing of the selected code.

    Args:
        source: 'joins' for generated_python_joins or 'filters' for filter_code
    """
    # Get the file path based on source
    results_dir = _get_source_directory(source)
    file_path = os.path.join(results_dir, file_name)

    if not os.path.exists(file_path):
        messages.error(request, f"File {file_name} not found")
        return redirect('pybirdai:review_execution_code', source=source)

    # Check file size before loading (max 300KB)
    MAX_FILE_SIZE = 300 * 1024  # 300KB in bytes
    file_size = os.path.getsize(file_path)
    file_size_mb = file_size / (1024 * 1024)

    if file_size > MAX_FILE_SIZE:
        messages.error(
            request,
            f"File {file_name} is too large to edit in the browser ({file_size_mb:.2f} MB). "
            f"Maximum allowed size is 300 KB. Please edit this file locally or split it into smaller files."
        )
        return redirect('pybirdai:review_execution_code', source=source)

    # Parse the file to get structure
    with open(file_path, 'r') as f:
        code_content = f.read()

    try:
        tree = ast.parse(code_content)
        code_structure = _extract_code_structure(tree)
    except SyntaxError as e:
        messages.error(request, f"Syntax error in file: {str(e)}")
        code_structure = []

    context = {
        'file_name': file_name,
        'file_path': file_path,
        'code_content': code_content,
        'code_structure': json.dumps(code_structure),
        'source': source,
    }

    return render(request, 'pybirdai/workflow/shared/execution_code_editing/code_editor.html', context)


def review_execution_code(request, source='joins', step=3):
    """
    Review generated execution code files.
    Shows list of generated _logic.py files with options to edit.

    Args:
        source: 'joins' for generated_python_joins or 'filters' for filter_code
        step: Workflow step number
    """
    results_dir = _get_source_directory(source)

    # Get list of generated logic files
    logic_files = []
    if os.path.exists(results_dir):
        for file_name in os.listdir(results_dir):
            if file_name.endswith('_logic.py'):
                file_path = os.path.join(results_dir, file_name)
                file_size = os.path.getsize(file_path)
                logic_files.append({
                    'name': file_name,
                    'size': file_size,
                    'size_kb': round(file_size / 1024, 2),
                })

    # Get execution status
    try:
        latest_execution = AnaCreditProcessExecution.objects.filter(
            step_number=3
        ).latest('started_at')
        execution_code_approved = getattr(latest_execution, 'execution_code_approved', False)
    except AnaCreditProcessExecution.DoesNotExist:
        latest_execution = None
        execution_code_approved = False

    context = {
        'step': step,
        'source': source,
        'logic_files': logic_files,
        'execution': latest_execution,
        'execution_code_approved': execution_code_approved,
    }

    # Check if this is an embedded view request (for iframe)
    # If source is provided via GET parameter, it's likely from an iframe embed
    is_embed = 'source' in request.GET or request.GET.get('embed') == 'true'
    template_name = 'pybirdai/workflow/shared/execution_code_editing/code_review_embed.html' if is_embed else 'pybirdai/workflow/shared/execution_code_editing/code_review.html'

    return render(request, template_name, context)


@require_http_methods(["POST"])
def save_code_modifications(request):
    """
    Save edited code back to the file.
    Validates Python syntax before saving.
    """
    try:
        data = json.loads(request.body)
        file_name = data.get('file_name')
        code_content = data.get('code_content')
        source = data.get('source', 'joins')

        if not file_name or not code_content:
            return JsonResponse({'error': 'Missing file_name or code_content'}, status=400)

        # Validate Python syntax
        try:
            ast.parse(code_content)
        except SyntaxError as e:
            return JsonResponse({
                'error': f'Syntax error: {str(e)}',
                'line': e.lineno,
                'offset': e.offset
            }, status=400)

        # Save the file
        results_dir = _get_source_directory(source)
        file_path = os.path.join(results_dir, file_name)

        # Create backup
        backup_path = file_path + '.backup'
        if os.path.exists(file_path):
            with open(file_path, 'r') as f:
                backup_content = f.read()
            with open(backup_path, 'w') as f:
                f.write(backup_content)

        # Write new content
        with open(file_path, 'w') as f:
            f.write(code_content)

        # Track modifications
        try:
            execution = AnaCreditProcessExecution.objects.filter(
                step_number=3
            ).latest('started_at')
            if hasattr(execution, 'code_modifications'):
                if not execution.code_modifications:
                    execution.code_modifications = {}
                execution.code_modifications[file_name] = {
                    'modified': True,
                    'timestamp': str(timezone.now()),
                }
                execution.save()
        except:
            pass  # Continue even if tracking fails

        return JsonResponse({
            'success': True,
            'message': f'File {file_name} saved successfully'
        })

    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)


@require_http_methods(["GET"])
def get_code_structure(request, file_name, source='joins'):
    """
    API endpoint to get the structure of a Python file.
    Returns JSON with classes, methods, and their signatures.

    Args:
        source: 'joins' for generated_python_joins or 'filters' for filter_code
    """
    results_dir = _get_source_directory(source)
    file_path = os.path.join(results_dir, file_name)

    if not os.path.exists(file_path):
        return JsonResponse({'error': 'File not found'}, status=404)

    try:
        with open(file_path, 'r') as f:
            code_content = f.read()

        tree = ast.parse(code_content)
        structure = _extract_code_structure(tree)

        return JsonResponse({'structure': structure})
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)


@require_http_methods(["POST"])
def validate_python_code(request):
    """
    Validate Python code syntax without saving.
    """
    try:
        data = json.loads(request.body)
        code_content = data.get('code_content', '')

        try:
            ast.parse(code_content)
            return JsonResponse({'valid': True})
        except SyntaxError as e:
            return JsonResponse({
                'valid': False,
                'error': str(e),
                'line': e.lineno,
                'offset': e.offset
            })
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)


@require_http_methods(["POST"])
def duplicate_class_node(request):
    """
    Duplicate a class in the execution code file.
    Creates a copy with a new name that can be customized.
    """
    try:
        data = json.loads(request.body)
        file_name = data.get('file_name')
        class_name = data.get('class_name')
        new_class_name = data.get('new_class_name')
        source = data.get('source', 'joins')

        if not all([file_name, class_name, new_class_name]):
            return JsonResponse({'error': 'Missing required parameters'}, status=400)

        results_dir = _get_source_directory(source)
        file_path = os.path.join(results_dir, file_name)

        if not os.path.exists(file_path):
            return JsonResponse({'error': 'File not found'}, status=404)

        with open(file_path, 'r') as f:
            code_content = f.read()

        # Parse and duplicate the class
        tree = ast.parse(code_content)
        new_tree = _duplicate_class(tree, class_name, new_class_name)

        # Convert back to code
        import astor
        new_code = astor.to_source(new_tree)

        # Save the modified code
        with open(file_path, 'w') as f:
            f.write(new_code)

        return JsonResponse({
            'success': True,
            'message': f'Class {class_name} duplicated as {new_class_name}'
        })

    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)


@require_http_methods(["POST"])
def approve_execution_code(request, step=3):
    """
    Mark execution code as approved and ready for use.
    """
    try:
        execution = AnaCreditProcessExecution.objects.filter(
            step_number=3
        ).latest('started_at')

        if hasattr(execution, 'execution_code_approved'):
            execution.execution_code_approved = True
            execution.save()

        messages.success(request, "Execution code approved successfully!")
        return redirect('pybirdai:ancrdt_step_3_review')

    except Exception as e:
        messages.error(request, f"Error approving execution code: {str(e)}")
        return redirect('pybirdai:review_execution_code', step=step)


def edit_ancrdt_output_tables(request):
    """
    Editor for ancrdt_output_tables.py file.
    Provides embedded code editor interface for the ANCRDT dashboard.
    """
    # Try new structure first, then legacy
    new_dir = os.path.join(settings.BASE_DIR, 'results', 'generated_python', 'datasets', 'ANCRDT', 'filter')
    legacy_dir = os.path.join(settings.BASE_DIR, 'results', 'generated_python_joins')

    # Check where file exists
    new_path = os.path.join(new_dir, 'ancrdt_output_tables.py')
    legacy_path = os.path.join(legacy_dir, 'ancrdt_output_tables.py')

    if os.path.exists(new_path):
        results_dir = new_dir
        file_path = new_path
    elif os.path.exists(legacy_path):
        results_dir = legacy_dir
        file_path = legacy_path
    else:
        # Default to new structure for new files
        results_dir = new_dir
        file_path = new_path

    # Initialize code content
    code_content = ""

    if os.path.exists(file_path):
        with open(file_path, 'r') as f:
            code_content = f.read()
    else:
        # If file doesn't exist, create with default content
        code_content = "# ANCRDT Output Tables\n# Define output table configurations here\n"

    context = {
        'code_content': code_content,
    }

    return render(request, 'pybirdai/workflow/shared/execution_code_editing/edit_ancrdt_output_tables_embed.html', context)


@require_http_methods(["POST"])
def save_ancrdt_output_tables(request):
    """
    Save edited ancrdt_output_tables.py file.
    Validates Python syntax before saving.
    """
    try:
        data = json.loads(request.body)
        code_content = data.get('code_content')

        if not code_content:
            return JsonResponse({'error': 'Missing code_content'}, status=400)

        # Validate Python syntax
        try:
            ast.parse(code_content)
        except SyntaxError as e:
            return JsonResponse({
                'success': False,
                'error': f'Syntax error: {str(e)}',
                'line': e.lineno,
                'offset': e.offset
            }, status=400)

        # Save the file - try new structure first, then legacy
        new_dir = os.path.join(settings.BASE_DIR, 'results', 'generated_python', 'datasets', 'ANCRDT', 'filter')
        legacy_dir = os.path.join(settings.BASE_DIR, 'results', 'generated_python_joins')

        # Check where file exists to determine save location
        new_path = os.path.join(new_dir, 'ancrdt_output_tables.py')
        legacy_path = os.path.join(legacy_dir, 'ancrdt_output_tables.py')

        if os.path.exists(new_path):
            results_dir = new_dir
            file_path = new_path
        elif os.path.exists(legacy_path):
            results_dir = legacy_dir
            file_path = legacy_path
        else:
            # Default to new structure for new files
            results_dir = new_dir
            file_path = new_path

        # Create directory if it doesn't exist
        os.makedirs(results_dir, exist_ok=True)

        # Create backup
        backup_path = file_path + '.backup'
        if os.path.exists(file_path):
            with open(file_path, 'r') as f:
                backup_content = f.read()
            with open(backup_path, 'w') as f:
                f.write(backup_content)

        # Write new content
        with open(file_path, 'w') as f:
            f.write(code_content)

        # Track modifications
        try:
            execution = AnaCreditProcessExecution.objects.filter(
                step_number=3
            ).latest('started_at')
            if hasattr(execution, 'code_modifications'):
                if not execution.code_modifications:
                    execution.code_modifications = {}
                execution.code_modifications['ancrdt_output_tables.py'] = {
                    'modified': True,
                    'timestamp': str(timezone.now()),
                }
                execution.save()
        except:
            pass  # Continue even if tracking fails

        return JsonResponse({
            'success': True,
            'message': 'File ancrdt_output_tables.py saved successfully'
        })

    except Exception as e:
        return JsonResponse({'success': False, 'error': str(e)}, status=500)


# Helper functions

def _extract_code_structure(tree):
    """
    Extract the structure of a Python AST tree.
    Returns a list of dictionaries representing classes and their methods.
    """
    structure = []

    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef):
            class_info = {
                'type': 'class',
                'name': node.name,
                'line': node.lineno,
                'methods': [],
                'base_classes': [base.id if isinstance(base, ast.Name) else str(base)
                               for base in node.bases],
            }

            for item in node.body:
                if isinstance(item, ast.FunctionDef):
                    method_info = {
                        'type': 'method',
                        'name': item.name,
                        'line': item.lineno,
                        'args': [arg.arg for arg in item.args.args],
                        'decorators': [_get_decorator_name(d) for d in item.decorator_list],
                    }
                    class_info['methods'].append(method_info)

            structure.append(class_info)

    return structure


def _get_decorator_name(decorator):
    """
    Extract the name of a decorator from an AST node.
    """
    if isinstance(decorator, ast.Name):
        return decorator.id
    elif isinstance(decorator, ast.Call):
        if isinstance(decorator.func, ast.Name):
            return decorator.func.id
    return str(decorator)


def _duplicate_class(tree, class_name, new_class_name):
    """
    Duplicate a class in the AST tree with a new name.
    """
    import copy

    for i, node in enumerate(tree.body):
        if isinstance(node, ast.ClassDef) and node.name == class_name:
            # Create a deep copy of the class
            new_class = copy.deepcopy(node)
            new_class.name = new_class_name

            # Insert the new class after the original
            tree.body.insert(i + 1, new_class)
            break

    return tree


def unified_filter_code_editor(request):
    """
    Unified code editor with sidebar for filter code files.
    Shows all files from pybirdai/process_steps/filter_code/ directory tree.
    Provides left sidebar for file selection and right panel with CodeMirror editor.

    Directory structure:
      filter_code/
        templates/{FRAMEWORK}/filter/  - Framework filter files (FINREP, COREP)
        templates/{FRAMEWORK}/joins/   - Framework join files
        datasets/{FRAMEWORK}/filter/   - Dataset filter files (ANCRDT)
        datasets/{FRAMEWORK}/joins/    - Dataset join files
        lib/                           - Shared utilities

    URL Parameters:
        f: Optional hex-encoded compressed whitelist of filenames
        default_file: Optional file to load initially (or "smallest" for smallest file)
        framework: Optional framework name (FINREP, COREP, ANCRDT) for framework-specific view
        subdir: Optional subdirectory within framework (filter, joins)
    """
    # Get optional framework parameter for DPM workflow
    framework = request.GET.get('framework', None)
    subdir = request.GET.get('subdir', None)

    # Determine base directory
    filter_code_base = os.path.join(settings.BASE_DIR, 'pybirdai', 'process_steps', 'filter_code')

    if framework:
        # Framework-specific directory: filter_code/{templates|datasets}/{FRAMEWORK}/{subdir}/
        framework_upper = framework.upper()
        framework_type = 'datasets' if framework_upper == 'ANCRDT' else 'templates'
        if subdir:
            filter_code_dir = os.path.join(filter_code_base, framework_type, framework_upper, subdir)
        else:
            filter_code_dir = os.path.join(filter_code_base, framework_type, framework_upper)
    else:
        filter_code_dir = filter_code_base

    # Get optional file whitelist parameter (hex-encoded)
    hex_param = request.GET.get('f', None)
    whitelist = None
    if hex_param:
        whitelist = _decode_file_list(hex_param)

    # Get list of all Python files in filter_code directory tree (recursive)
    files = []
    if os.path.exists(filter_code_dir):
        for root, dirs, filenames in os.walk(filter_code_dir):
            # Get relative path from filter_code_dir
            rel_root = os.path.relpath(root, filter_code_dir)
            if rel_root == '.':
                rel_root = ''

            for file_name in filenames:
                if file_name.endswith('.py') and file_name != '__init__.py':
                    # Build relative path for display
                    if rel_root:
                        display_name = f"{rel_root}/{file_name}"
                    else:
                        display_name = file_name

                    # Apply whitelist filter if provided (match on filename or full path)
                    if whitelist and file_name not in whitelist and display_name not in whitelist:
                        continue

                    # Exclude *_report_cells.py files due to large size (75MB+)
                    # But allow them in framework-specific views where they're smaller
                    if not framework and file_name.endswith('_report_cells.py'):
                        continue

                    file_path = os.path.join(root, file_name)
                    file_size = os.path.getsize(file_path)
                    files.append({
                        'name': display_name,  # Full relative path
                        'filename': file_name,  # Just the filename
                        'subdir': rel_root,     # Subdirectory path
                        'size': file_size,
                        'size_kb': round(file_size / 1024, 2),
                    })

    # Sort files alphabetically by full path
    files.sort(key=lambda x: x['name'])

    # Determine which file to load by default
    default_file_param = request.GET.get('default_file', None)
    selected_file = None

    if default_file_param and files:
        if default_file_param == 'smallest':
            # Find smallest file by size
            selected_file = min(files, key=lambda x: x['size'])
        else:
            # Try to find specified file by name (match on filename or full path)
            selected_file = next((f for f in files if f['name'] == default_file_param or f['filename'] == default_file_param), None)

    # If no selection made or file not found, use first file alphabetically (current behavior)
    if not selected_file and files:
        selected_file = files[0]

    # Get selected file content as default
    first_file_content = ""
    first_file_name = ""
    if selected_file:
        first_file_name = selected_file['name']
        first_file_path = os.path.join(filter_code_dir, first_file_name)
        try:
            with open(first_file_path, 'r', encoding='utf-8') as f:
                first_file_content = f.read()
        except Exception as e:
            first_file_content = f"# Error loading file: {str(e)}"

    context = {
        'files': files,
        'first_file_name': first_file_name,
        'first_file_content': first_file_content,
        'total_files': len(files),
        'framework': framework,
        'subdir': subdir,
    }

    return render(request, 'pybirdai/workflow/shared/execution_code_editing/unified_filter_code_editor.html', context)


def load_filter_code_file(request):
    """
    AJAX endpoint to load a filter code file.
    Returns file content as JSON.

    Now supports subdirectory paths (e.g., 'lib/automatic_tracking_wrapper.py')
    and framework-specific paths via framework/subdir parameters.
    """
    if request.method != 'GET':
        return JsonResponse({'success': False, 'error': 'Method not allowed'}, status=405)

    file_name = request.GET.get('file_name')
    if not file_name:
        return JsonResponse({'success': False, 'error': 'Missing file_name parameter'}, status=400)

    # Security check: ensure file name doesn't contain path traversal outside filter_code
    # Allow forward slashes for subdirectory paths, but reject '..' and backslashes
    if '..' in file_name or '\\' in file_name:
        return JsonResponse({'success': False, 'error': 'Invalid file name'}, status=400)

    # Ensure file has .py extension
    if not file_name.endswith('.py'):
        return JsonResponse({'success': False, 'error': 'Only Python files are allowed'}, status=400)

    # Get optional framework parameter for DPM workflow
    framework = request.GET.get('framework', None)
    subdir = request.GET.get('subdir', None)

    # Determine base directory
    filter_code_base = os.path.join(settings.BASE_DIR, 'pybirdai', 'process_steps', 'filter_code')

    if framework:
        # Framework-specific directory: filter_code/{templates|datasets}/{FRAMEWORK}/{subdir}/
        framework_upper = framework.upper()
        framework_type = 'datasets' if framework_upper == 'ANCRDT' else 'templates'
        if subdir:
            filter_code_dir = os.path.join(filter_code_base, framework_type, framework_upper, subdir)
        else:
            filter_code_dir = os.path.join(filter_code_base, framework_type, framework_upper)
    else:
        filter_code_dir = filter_code_base

    file_path = os.path.join(filter_code_dir, file_name)

    # Check if file exists
    if not os.path.exists(file_path):
        return JsonResponse({'success': False, 'error': 'File not found'}, status=404)

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()

        file_size = os.path.getsize(file_path)

        return JsonResponse({
            'success': True,
            'content': content,
            'file_name': file_name,
            'size': file_size,
            'size_kb': round(file_size / 1024, 2),
        })
    except Exception as e:
        return JsonResponse({'success': False, 'error': str(e)}, status=500)


@require_http_methods(["POST"])
def save_filter_code_file(request):
    """
    AJAX endpoint to save a filter code file.
    Validates Python syntax before saving.

    Now supports subdirectory paths (e.g., 'lib/automatic_tracking_wrapper.py')
    and framework-specific paths via framework/subdir parameters.
    """
    try:
        data = json.loads(request.body)
        file_name = data.get('file_name')
        content = data.get('content')
        framework = data.get('framework', None)
        subdir = data.get('subdir', None)

        if not file_name or content is None:
            return JsonResponse({'success': False, 'error': 'Missing file_name or content'}, status=400)

        # Security check: ensure file name doesn't contain path traversal outside filter_code
        # Allow forward slashes for subdirectory paths, but reject '..' and backslashes
        if '..' in file_name or '\\' in file_name:
            return JsonResponse({'success': False, 'error': 'Invalid file name'}, status=400)

        # Ensure file has .py extension
        if not file_name.endswith('.py'):
            return JsonResponse({'success': False, 'error': 'Only Python files are allowed'}, status=400)

        # Validate Python syntax
        try:
            ast.parse(content)
        except SyntaxError as e:
            return JsonResponse({
                'success': False,
                'error': f'Syntax error: {str(e)}',
                'line': e.lineno,
                'offset': e.offset
            }, status=400)

        # Determine base directory
        filter_code_base = os.path.join(settings.BASE_DIR, 'pybirdai', 'process_steps', 'filter_code')

        if framework:
            # Framework-specific directory: filter_code/{templates|datasets}/{FRAMEWORK}/{subdir}/
            framework_upper = framework.upper()
            framework_type = 'datasets' if framework_upper == 'ANCRDT' else 'templates'
            if subdir:
                filter_code_dir = os.path.join(filter_code_base, framework_type, framework_upper, subdir)
            else:
                filter_code_dir = os.path.join(filter_code_base, framework_type, framework_upper)
        else:
            filter_code_dir = filter_code_base

        file_path = os.path.join(filter_code_dir, file_name)

        # Create backup before saving
        if os.path.exists(file_path):
            backup_path = file_path + '.backup'
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    backup_content = f.read()
                with open(backup_path, 'w', encoding='utf-8') as f:
                    f.write(backup_content)
            except Exception:
                pass  # Backup is optional

        # Save the file
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)

        return JsonResponse({
            'success': True,
            'message': 'File saved successfully',
            'file_name': file_name
        })

    except json.JSONDecodeError:
        return JsonResponse({'success': False, 'error': 'Invalid JSON'}, status=400)
    except Exception as e:
        return JsonResponse({'success': False, 'error': str(e)}, status=500)


# ============================================================================
# Code Sync Views - ANCRDT Lifecycle Management
# ============================================================================

@require_http_methods(["POST"])
def sync_file_to_production(request):
    """
    Sync a single file from staging (generated_python_joins) to production (filter_code).
    Part of the ANCRDT code lifecycle: Generate → Edit → Deploy.

    Expected POST data:
        {
            "file_name": "ANCRDT_INSTRMNT_C_1_logic.py",
            "create_backup": true
        }
    """
    try:
        data = json.loads(request.body)
        file_name = data.get('file_name')
        create_backup = data.get('create_backup', True)

        if not file_name:
            return JsonResponse({'error': 'Missing file_name'}, status=400)

        # Initialize sync manager
        sync_manager = CodeSyncManager()

        # Perform sync
        result = sync_manager.sync_file(file_name, create_backup)

        if result['success']:
            # Track the sync in execution record
            try:
                execution = AnaCreditProcessExecution.objects.filter(
                    step_number=3
                ).latest('started_at')
                if hasattr(execution, 'code_modifications'):
                    if not execution.code_modifications:
                        execution.code_modifications = {}
                    if file_name not in execution.code_modifications:
                        execution.code_modifications[file_name] = {}
                    execution.code_modifications[file_name]['synced_to_production'] = True
                    execution.code_modifications[file_name]['sync_timestamp'] = result['timestamp']
                    execution.save()
            except:
                pass  # Continue even if tracking fails

            return JsonResponse({
                'success': True,
                'message': result['message'],
                'file_name': file_name,
                'timestamp': result['timestamp'],
                'backup_created': result.get('backup_created', False)
            })
        else:
            return JsonResponse({
                'success': False,
                'error': result['message']
            }, status=400)

    except json.JSONDecodeError:
        return JsonResponse({'error': 'Invalid JSON'}, status=400)
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)


@require_http_methods(["POST"])
def sync_all_ancrdt_files(request):
    """
    Sync all ANCRDT files from staging to production.
    """
    try:
        data = json.loads(request.body)
        create_backup = data.get('create_backup', True)

        # Initialize sync manager
        sync_manager = CodeSyncManager()

        # Perform sync
        results = sync_manager.sync_all_ancrdt_files(create_backup)

        # Count successes and failures
        successes = [r for r in results if r['success']]
        failures = [r for r in results if not r['success']]

        return JsonResponse({
            'success': True,
            'total': len(results),
            'successes': len(successes),
            'failures': len(failures),
            'results': results
        })

    except json.JSONDecodeError:
        return JsonResponse({'error': 'Invalid JSON'}, status=400)
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)


@require_http_methods(["GET"])
def get_sync_status(request, file_name=None):
    """
    Get sync status for ANCRDT files.

    If file_name is provided, returns status for that file.
    Otherwise, returns status for all ANCRDT files.
    """
    try:
        sync_manager = CodeSyncManager()

        if file_name:
            # Get status for specific file
            status = sync_manager._get_file_status(file_name)
            return JsonResponse({
                'success': True,
                'file_status': status
            })
        else:
            # Get status for all files
            status_map = sync_manager.get_sync_status()
            return JsonResponse({
                'success': True,
                'status_map': status_map,
                'total_files': len(status_map)
            })

    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)


@require_http_methods(["GET"])
def get_file_diff(request, file_name):
    """
    Get a diff summary between staging and production versions of a file.
    """
    try:
        sync_manager = CodeSyncManager()

        diff_summary = sync_manager.get_diff_summary(file_name)

        if diff_summary:
            return JsonResponse({
                'success': True,
                'diff': diff_summary
            })
        else:
            return JsonResponse({
                'success': False,
                'error': 'Unable to generate diff (one or both files may not exist)'
            }, status=404)

    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)


@require_http_methods(["GET"])
def check_manual_edits(request, file_name):
    """
    Check if a file has manual edits (differs from .generated base).
    """
    try:
        sync_manager = CodeSyncManager()

        has_edits = sync_manager.has_manual_edits(file_name)

        return JsonResponse({
            'success': True,
            'file_name': file_name,
            'has_manual_edits': has_edits
        })

    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)


@require_http_methods(["GET"])
def get_file_info(request, source='joins', file_name=None):
    """
    Get information about a file (size, line count, etc.).

    Args:
        source: 'joins' for generated_python_joins or 'filters' for filter_code
        file_name: Name of the file to get info for

    Returns:
        JSON with file information
    """
    if not file_name:
        return JsonResponse({'error': 'Missing file_name'}, status=400)

    try:
        results_dir = _get_source_directory(source)
        file_path = os.path.join(results_dir, file_name)

        if not os.path.exists(file_path):
            return JsonResponse({'error': 'File not found'}, status=404)

        # Get file size
        file_size = os.path.getsize(file_path)
        file_size_kb = file_size / 1024
        file_size_mb = file_size / (1024 * 1024)

        # Check if file can be edited (under 300KB)
        MAX_FILE_SIZE = 300 * 1024
        can_edit = file_size <= MAX_FILE_SIZE

        # Get line count
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                line_count = sum(1 for _ in f)
        except Exception:
            line_count = None

        # Format size string
        if file_size_mb >= 1:
            size_str = f"{file_size_mb:.2f} MB"
        else:
            size_str = f"{file_size_kb:.1f} KB"

        return JsonResponse({
            'success': True,
            'file_name': file_name,
            'file_size': file_size,
            'file_size_kb': round(file_size_kb, 1),
            'file_size_mb': round(file_size_mb, 2),
            'size_str': size_str,
            'line_count': line_count,
            'can_edit': can_edit,
            'max_size_kb': 300
        })

    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)


@require_http_methods(["POST"])
def save_and_deploy(request):
    """
    Combination endpoint: save code modifications and immediately deploy to production.
    This is a convenience endpoint that combines save_code_modifications and sync_file_to_production.

    Expected POST data:
        {
            "file_name": "ANCRDT_INSTRMNT_C_1_logic.py",
            "code_content": "... python code ...",
            "create_backup": true
        }
    """
    try:
        data = json.loads(request.body)
        file_name = data.get('file_name')
        code_content = data.get('code_content')
        create_backup = data.get('create_backup', True)

        if not file_name or not code_content:
            return JsonResponse({'error': 'Missing file_name or code_content'}, status=400)

        # Step 1: Validate Python syntax
        try:
            ast.parse(code_content)
        except SyntaxError as e:
            return JsonResponse({
                'error': f'Syntax error: {str(e)}',
                'line': e.lineno,
                'offset': e.offset
            }, status=400)

        # Step 2: Save to staging area
        results_dir = _get_source_directory('joins')
        file_path = os.path.join(results_dir, file_name)

        # Create backup in staging
        backup_path = file_path + '.backup'
        if os.path.exists(file_path):
            with open(file_path, 'r') as f:
                backup_content = f.read()
            with open(backup_path, 'w') as f:
                f.write(backup_content)

        # Write new content to staging
        with open(file_path, 'w') as f:
            f.write(code_content)

        # Step 3: Sync to production
        sync_manager = CodeSyncManager()
        sync_result = sync_manager.sync_file(file_name, create_backup)

        # Step 4: Track modifications
        try:
            execution = AnaCreditProcessExecution.objects.filter(
                step_number=3
            ).latest('started_at')
            if hasattr(execution, 'code_modifications'):
                if not execution.code_modifications:
                    execution.code_modifications = {}
                execution.code_modifications[file_name] = {
                    'modified': True,
                    'timestamp': str(timezone.now()),
                    'synced_to_production': sync_result['success'],
                    'sync_timestamp': sync_result.get('timestamp')
                }
                execution.save()
        except:
            pass  # Continue even if tracking fails

        if sync_result['success']:
            return JsonResponse({
                'success': True,
                'message': f'File {file_name} saved and deployed successfully',
                'saved': True,
                'deployed': True,
                'sync_result': sync_result
            })
        else:
            return JsonResponse({
                'success': True,
                'message': f'File {file_name} saved but deployment failed: {sync_result["message"]}',
                'saved': True,
                'deployed': False,
                'sync_result': sync_result
            })

    except json.JSONDecodeError:
        return JsonResponse({'error': 'Invalid JSON'}, status=400)
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)