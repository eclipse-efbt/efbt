"""
Views for execution code editing workflow.
Provides functionality to review and edit generated execution code (_logic.py files)
with a feedback loop for iterative refinement.
"""

import ast
import json
import os
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


def _get_source_directory(source='joins'):
    """
    Get the directory path based on the source type.

    Args:
        source: 'joins' for generated_python_joins or 'filters' for filter_code

    Returns:
        Absolute path to the source directory
    """
    if source == 'filters':
        return os.path.join(settings.BASE_DIR, 'pybirdai', 'process_steps', 'filter_code')
    else:  # default to 'joins'
        return os.path.join(settings.BASE_DIR, 'results', 'generated_python_joins')


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

    return render(request, 'pybirdai/execution_code_editing_workflow/review_joins_metadata.html', context)


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

    return render(request, 'pybirdai/execution_code_editing_workflow/code_editor.html', context)


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
    template_name = 'pybirdai/execution_code_editing_workflow/code_review_embed.html' if is_embed else 'pybirdai/execution_code_editing_workflow/code_review.html'

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
        return redirect('pybirdai:workflow_ancrdt_review', step_number=3)

    except Exception as e:
        messages.error(request, f"Error approving execution code: {str(e)}")
        return redirect('pybirdai:review_execution_code', step=step)


def edit_ancrdt_output_tables(request):
    """
    Editor for ancrdt_output_tables.py file.
    Provides embedded code editor interface for the ANCRDT dashboard.
    """
    results_dir = os.path.join(settings.BASE_DIR, 'results', 'generated_python_joins')
    file_path = os.path.join(results_dir, 'ancrdt_output_tables.py')

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

    return render(request, 'pybirdai/execution_code_editing_workflow/edit_ancrdt_output_tables_embed.html', context)


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

        # Save the file
        results_dir = os.path.join(settings.BASE_DIR, 'results', 'generated_python_joins')
        file_path = os.path.join(results_dir, 'ancrdt_output_tables.py')

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
    Shows all files from pybirdai/process_steps/filter_code/ directory.
    Provides left sidebar for file selection and right panel with CodeMirror editor.
    """
    filter_code_dir = os.path.join(settings.BASE_DIR, 'pybirdai', 'process_steps', 'filter_code')

    # Get list of all Python files in filter_code directory
    files = []
    if os.path.exists(filter_code_dir):
        for file_name in os.listdir(filter_code_dir):
            if file_name.endswith('.py'):
                # Exclude report_cells.py due to large size (75MB)
                if file_name == 'report_cells.py':
                    continue

                file_path = os.path.join(filter_code_dir, file_name)
                file_size = os.path.getsize(file_path)
                files.append({
                    'name': file_name,
                    'size': file_size,
                    'size_kb': round(file_size / 1024, 2),
                })

    # Sort files alphabetically
    files.sort(key=lambda x: x['name'])

    # Get first file content as default
    first_file_content = ""
    first_file_name = ""
    if files:
        first_file_name = files[0]['name']
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
    }

    return render(request, 'pybirdai/execution_code_editing_workflow/unified_filter_code_editor.html', context)


def load_filter_code_file(request):
    """
    AJAX endpoint to load a filter code file.
    Returns file content as JSON.
    """
    if request.method != 'GET':
        return JsonResponse({'success': False, 'error': 'Method not allowed'}, status=405)

    file_name = request.GET.get('file_name')
    if not file_name:
        return JsonResponse({'success': False, 'error': 'Missing file_name parameter'}, status=400)

    # Security check: ensure file name doesn't contain path traversal
    if '..' in file_name or '/' in file_name or '\\' in file_name:
        return JsonResponse({'success': False, 'error': 'Invalid file name'}, status=400)

    # Ensure file has .py extension
    if not file_name.endswith('.py'):
        return JsonResponse({'success': False, 'error': 'Only Python files are allowed'}, status=400)

    filter_code_dir = os.path.join(settings.BASE_DIR, 'pybirdai', 'process_steps', 'filter_code')
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
    """
    try:
        data = json.loads(request.body)
        file_name = data.get('file_name')
        content = data.get('content')

        if not file_name or content is None:
            return JsonResponse({'success': False, 'error': 'Missing file_name or content'}, status=400)

        # Security check: ensure file name doesn't contain path traversal
        if '..' in file_name or '/' in file_name or '\\' in file_name:
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

        filter_code_dir = os.path.join(settings.BASE_DIR, 'pybirdai', 'process_steps', 'filter_code')
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