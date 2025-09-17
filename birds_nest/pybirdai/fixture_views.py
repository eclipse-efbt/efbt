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

from django.shortcuts import render, redirect
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from django.views.decorators.csrf import csrf_exempt
from django.contrib import messages
from django.conf import settings
import json
import os
import subprocess
import logging
import shutil
from pathlib import Path
from typing import Dict, List, Any
from datetime import datetime

from .process_steps.test_fixture.test_fixture_service import (
    WebFixtureService, TemplateInfo, CellInfo, WebFixtureConfig,
    FixtureResult, EntityData, CellAnalysis
)
from .utils.datapoint_test_run.generator_delete_fixtures import process_sql_file

logger = logging.getLogger(__name__)

def sort_cells(cells, key_func=None):
    """
    Sort cells by numeric value extracted from cell suffix or ID.

    Args:
        cells: List of cells (can be cell objects with suffix, or strings)
        key_func: Optional function to extract the cell identifier from each cell
                 If None, assumes cells are strings or have a 'suffix' attribute

    Returns:
        Sorted list of cells
    """
    def extract_numeric_id(cell_id):
        """Extract numeric part from cell ID by removing _REF and converting to int"""
        try:
            # Remove _REF suffix if present
            numeric_part = cell_id.replace('_REF', '')
            return int(numeric_part)
        except (ValueError, AttributeError):
            # If conversion fails, return a large number to sort at the end
            return 999999

    def get_cell_key(cell):
        """Get the sortable key for a cell"""
        if key_func:
            cell_id = key_func(cell)
        elif isinstance(cell, str):
            cell_id = cell
        elif hasattr(cell, 'suffix'):
            cell_id = cell.suffix
        elif hasattr(cell, 'cell_suffix'):
            cell_id = cell.cell_suffix
        else:
            # Fallback - convert to string and hope for the best
            cell_id = str(cell)

        return extract_numeric_id(cell_id)

    return sorted(cells, key=get_cell_key)

@require_http_methods(["GET"])
def get_current_tests(request):
    """Get current test configuration from configuration_file_tests.json"""
    try:
        config_file_path = Path("tests/configuration_file_tests.json")

        if not config_file_path.exists():
            return JsonResponse({
                'success': True,
                'tests': [],
                'summary': {'total': 0, 'templates': {}}
            })

        with open(config_file_path, 'r') as f:
            config_data = json.load(f)

        tests = config_data.get('tests', [])

        # Create summary by template
        summary = {'total': len(tests), 'templates': {}}

        for test in tests:
            template_id = test.get('reg_tid', '')
            if template_id not in summary['templates']:
                summary['templates'][template_id] = {
                    'count': 0,
                    'cells': {},
                    'scenarios': set()
                }

            summary['templates'][template_id]['count'] += 1
            summary['templates'][template_id]['scenarios'].add(test.get('scenario', 'base'))

            cell_suffix = test.get('dp_suffix', '')
            if cell_suffix not in summary['templates'][template_id]['cells']:
                summary['templates'][template_id]['cells'][cell_suffix] = []
            summary['templates'][template_id]['cells'][cell_suffix].append(test.get('scenario', 'base'))

        # Convert sets to lists for JSON serialization and cells dict to list
        for template_data in summary['templates'].values():
            template_data['scenarios'] = list(template_data['scenarios'])
            # Convert cells dictionary to list of cell suffixes for frontend
            template_data['cells'] = list(template_data['cells'].keys())

        return JsonResponse({
            'success': True,
            'tests': tests,
            'summary': summary
        })

    except Exception as e:
        logger.error(f"Error getting current tests: {e}")
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)

def fixture_generator_dashboard(request):
    """Main fixture generator dashboard"""
    return render(request, 'pybirdai/fixture_generator_dashboard.html', context={"templateId":"F_05_01_REF_FINREP_3_0"})

@require_http_methods(["GET"])
def get_available_templates(request):
    """Get available regulatory templates"""
    try:
        service = WebFixtureService()
        templates = service.discover_available_templates()

        # Convert TemplateInfo objects to JSON-serializable format
        template_list = [
            {
                'id': template_info.id,
                'name': template_info.name,
                'description': template_info.description,
                'cell_count': template_info.cell_count,
                'categories': template_info.categories,
                'format_version': template_info.format_version
            }
            for template_info in templates.values()
        ]

        logger.info(f"Successfully discovered {len(template_list)} templates")
        return JsonResponse({
            'success': True,
            'templates': template_list
        })
    except Exception as e:
        logger.error(f"Error getting templates: {e}")
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)

@require_http_methods(["GET"])
def get_template_cells(request, template_id):
    """Get all cells for a specific template with test status"""
    try:
        service = WebFixtureService()
        cells = service.get_template_cells(template_id)

        # Get test status for this template
        test_status = service.get_test_status_for_template(template_id)

        # Sort cells numerically by suffix before converting
        sorted_cells = sort_cells(cells)

        # Convert CellInfo objects to JSON-serializable format with test status
        cell_list = [
            {
                'id': cell_info.id,
                'name': cell_info.name,
                'suffix': cell_info.suffix,
                'template_id': cell_info.template_id,
                'description': cell_info.description,
                'has_test': cell_info.suffix in test_status,
                'test_scenarios': test_status.get(cell_info.suffix, []),
                'test_count': len(test_status.get(cell_info.suffix, []))
            }
            for cell_info in sorted_cells
        ]

        return JsonResponse({
            'success': True,
            'cells': cell_list,
            'test_summary': {
                'total_cells': len(cell_list),
                'tested_cells': len([c for c in cell_list if c['has_test']]),
                'untested_cells': len([c for c in cell_list if not c['has_test']])
            }
        })
    except Exception as e:
        logger.error(f"Error getting cells for template {template_id}: {e}")
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)

@require_http_methods(["POST"])
@csrf_exempt
def analyze_cell(request):
    """Analyze a specific cell to extract filter conditions"""
    try:
        data = json.loads(request.body)
        template_id = data.get('template_id')
        cell_suffix = data.get('cell_suffix')

        if not template_id or not cell_suffix:
            return JsonResponse({
                'success': False,
                'error': 'Missing template_id or cell_suffix'
            }, status=400)

        service = WebFixtureService()
        cell_analysis = service.analyze_cell_requirements(template_id, cell_suffix)

        if not cell_analysis:
            return JsonResponse({
                'success': False,
                'error': f'Failed to analyze cell {template_id}_{cell_suffix}'
            }, status=404)

        # Convert to JSON-serializable format
        analysis_data = {
            'cell_name': cell_analysis.cell_name,
            'template_id': cell_analysis.template_id,
            'cell_suffix': cell_analysis.cell_suffix,
            'referenced_table': cell_analysis.referenced_table,
            'metric_field': cell_analysis.metric_field,
            'filter_count': cell_analysis.filter_count,
            'filters': [
                {
                    'field_name': f.field_name,
                    'values': f.values,
                    'operator': f.operator,
                    'description': f.description
                }
                for f in cell_analysis.filters
            ]
        }

        return JsonResponse({
            'success': True,
            'analysis': analysis_data
        })

    except Exception as e:
        logger.error(f"Error analyzing cell: {e}")
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)

@require_http_methods(["POST"])
@csrf_exempt
def generate_fixtures(request):
    """Generate fixtures for selected cells"""
    try:
        data = json.loads(request.body)
        template_id = data.get('template_id')
        cells = data.get('cells', [])  # List of cell suffixes

        # Handle both old single scenario format and new multiple scenarios format
        scenarios = data.get('scenarios', [])
        if not scenarios:
            # Fallback to old format for backward compatibility
            scenario_name = data.get('scenario_name', 'base')
            expected_value = data.get('expected_value', 83491250)
            scenarios = [{'name': scenario_name, 'value': expected_value}]

        batch_mode = data.get('batch_mode', False)
        database_prefill = data.get('database_prefill', False)

        if not template_id or not cells:
            return JsonResponse({
                'success': False,
                'error': 'Missing template_id or cells'
            }, status=400)

        if not scenarios:
            return JsonResponse({
                'success': False,
                'error': 'At least one scenario is required'
            }, status=400)

        service = WebFixtureService()
        fixture_results = []
        errors = []

        # Generate fixtures for each cell and each scenario combination
        for cell_suffix in cells:
            for scenario in scenarios:
                scenario_name = scenario.get('name', 'base')
                expected_value = scenario.get('value', 83491250)

                try:
                    # Generate fixture set using the web service
                    result = service.create_fixture_set(
                        template_id=template_id,
                        cell_suffix=cell_suffix,
                        scenario=scenario_name,
                        expected_value=expected_value,
                        custom_entities=[],  # Could be enhanced to accept custom entities
                        database_prefill=database_prefill
                    )

                    if result.success:
                        fixture_results.append(result)
                    else:
                        errors.extend(result.errors)

                except Exception as e:
                    logger.error(f"Error generating fixtures for {cell_suffix} scenario {scenario_name}: {e}")
                    errors.append(f"Error generating fixtures for {cell_suffix} scenario {scenario_name}: {str(e)}")

        # Update test configuration for successful results
        if fixture_results:
            config_update = service.update_test_configuration(fixture_results)
            if not config_update.success:
                errors.extend(config_update.errors)

        # Format results for frontend
        results = [
            {
                'cell_suffix': result.cell_suffix,
                'scenario': result.scenario,  # Include scenario information
                'status': 'success' if result.success else 'error',
                'fixture_path': result.fixture_path,
                'generated_files': result.generated_files,
                'test_file_path': result.test_file_path,
                'test_file_content': result.test_file_content,
                'errors': result.errors
            }
            for result in fixture_results
        ]

        return JsonResponse({
            'success': len(errors) == 0,
            'results': results,
            'errors': errors,
            'generated_count': len([r for r in results if r['status'] == 'success']),
            'error_count': len(errors),
            'total_files_generated': sum(len(r['generated_files']) for r in results)
        })

    except Exception as e:
        logger.error(f"Error in generate_fixtures: {e}")
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)



def edit_sql_fixtures(request, template_id, cell_suffix, scenario):
    """Edit SQL fixtures for a specific combination"""
    try:
        fixture_path = Path(f"tests/fixtures/templates/{template_id}/{cell_suffix}/{scenario}")
        sql_inserts_file = fixture_path / "sql_inserts.sql"
        sql_deletes_file = fixture_path / "sql_deletes.sql"

        sql_inserts_content = ""
        sql_deletes_content = ""

        if sql_inserts_file.exists():
            with open(sql_inserts_file, 'r') as f:
                sql_inserts_content = f.read()

        if sql_deletes_file.exists():
            with open(sql_deletes_file, 'r') as f:
                sql_deletes_content = f.read()

        context = {
            'template_id': template_id,
            'cell_suffix': cell_suffix,
            'scenario': scenario,
            'sql_inserts_content': sql_inserts_content,
            'sql_deletes_content': sql_deletes_content,
            'fixture_path': str(fixture_path)
        }

        return render(request, 'pybirdai/fixture_sql_editor.html', context)

    except Exception as e:
        logger.error(f"Error loading SQL fixtures: {e}")
        messages.error(request, f"Error loading SQL fixtures: {str(e)}")
        return redirect('pybirdai:fixture_generator_dashboard')

@require_http_methods(["POST"])
@csrf_exempt
def save_sql_fixtures(request):
    """Save edited SQL fixtures"""
    try:
        data = json.loads(request.body)
        template_id = data.get('template_id')
        cell_suffix = data.get('cell_suffix')
        scenario = data.get('scenario')
        sql_content = data.get('sql_content')

        if not all([template_id, cell_suffix, scenario, sql_content is not None]):
            return JsonResponse({
                'success': False,
                'error': 'Missing required fields'
            }, status=400)

        fixture_path = Path(f"tests/fixtures/templates/{template_id}/{cell_suffix}/{scenario}")
        sql_inserts_file = fixture_path / "sql_inserts.sql"

        # Ensure directory exists
        fixture_path.mkdir(parents=True, exist_ok=True)

        # Save SQL content
        with open(sql_inserts_file, 'w') as f:
            f.write(sql_content)

        return JsonResponse({
            'success': True,
            'message': 'SQL fixtures saved successfully'
        })

    except Exception as e:
        logger.error(f"Error saving SQL fixtures: {e}")
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)

@require_http_methods(["POST"])
@csrf_exempt
def regenerate_delete_fixtures(request):
    """Regenerate delete fixtures from insert fixtures"""
    try:
        data = json.loads(request.body)
        template_id = data.get('template_id')
        cell_suffix = data.get('cell_suffix')
        scenario = data.get('scenario')

        if not all([template_id, cell_suffix, scenario]):
            return JsonResponse({
                'success': False,
                'error': 'Missing required fields'
            }, status=400)

        fixture_path = Path(f"tests/fixtures/templates/{template_id}/{cell_suffix}/{scenario}")
        sql_inserts_file = fixture_path / "sql_inserts.sql"
        sql_deletes_file = fixture_path / "sql_deletes.sql"

        if not sql_inserts_file.exists():
            return JsonResponse({
                'success': False,
                'error': 'SQL inserts file not found'
            }, status=404)

        # Generate delete fixtures using existing generator
        process_sql_file(str(sql_inserts_file))

        # Read the generated delete content
        delete_content = ""
        if sql_deletes_file.exists():
            with open(sql_deletes_file, 'r') as f:
                delete_content = f.read()

        return JsonResponse({
            'success': True,
            'delete_content': delete_content,
            'message': 'Delete fixtures regenerated successfully'
        })

    except Exception as e:
        logger.error(f"Error regenerating delete fixtures: {e}")
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)

@require_http_methods(["GET"])
def search_fixtures(request):
    """Search through existing fixtures with advanced filtering"""
    try:
        search_term = request.GET.get('search', '').strip()
        template_filter = request.GET.get('template', '').strip()
        scenario_filter = request.GET.get('scenario', '').strip()

        fixtures_base_path = Path("tests/fixtures/templates")

        if not fixtures_base_path.exists():
            return JsonResponse({
                'success': True,
                'fixtures': [],
                'count': 0,
                'templates': [],
                'scenarios': []
            })

        fixtures = []
        templates = set()
        scenarios = set()

        # Scan fixture directories
        for template_dir in fixtures_base_path.iterdir():
            if not template_dir.is_dir():
                continue

            template_id = template_dir.name
            templates.add(template_id)

            for cell_dir in template_dir.iterdir():
                if not cell_dir.is_dir():
                    continue

                cell_suffix = cell_dir.name

                for scenario_dir in cell_dir.iterdir():
                    if not scenario_dir.is_dir():
                        continue

                    scenario = scenario_dir.name
                    scenarios.add(scenario)

                    # Check if fixture files exist
                    sql_inserts = scenario_dir / "sql_inserts.sql"
                    sql_deletes = scenario_dir / "sql_deletes.sql"

                    if sql_inserts.exists():
                        # Get file metadata
                        stat = sql_inserts.stat()
                        last_modified = datetime.fromtimestamp(stat.st_mtime)
                        file_size = stat.st_size

                        # Check test status
                        test_status = "unknown"
                        try:
                            config_file_path = Path("tests/configuration_file_tests.json")
                            if config_file_path.exists():
                                with open(config_file_path, 'r') as f:
                                    config_data = json.load(f)

                                # Check if this fixture has a corresponding test
                                for test in config_data.get('tests', []):
                                    if (test.get('reg_tid') == template_id and
                                        test.get('dp_suffix') == cell_suffix and
                                        test.get('scenario') == scenario):
                                        test_status = "configured"
                                        break
                                else:
                                    test_status = "not_configured"
                        except Exception:
                            test_status = "unknown"

                        fixture_info = {
                            'template_id': template_id,
                            'cell_suffix': cell_suffix,
                            'scenario': scenario,
                            'test_status': test_status,
                            'last_modified': last_modified.isoformat(),
                            'file_size': file_size,
                            'has_deletes': sql_deletes.exists(),
                            'fixture_path': str(scenario_dir)
                        }

                        # Apply filters
                        include_fixture = True

                        if search_term:
                            search_match = (
                                search_term.lower() in template_id.lower() or
                                search_term.lower() in cell_suffix.lower() or
                                search_term.lower() in scenario.lower()
                            )
                            if not search_match:
                                include_fixture = False

                        if template_filter and template_id != template_filter:
                            include_fixture = False

                        if scenario_filter and scenario != scenario_filter:
                            include_fixture = False

                        if include_fixture:
                            fixtures.append(fixture_info)

        # Sort fixtures by template, then cell numerically, then scenario
        def get_numeric_cell_id(cell_suffix):
            try:
                return int(cell_suffix.replace('_REF', ''))
            except ValueError:
                return 999999

        fixtures.sort(key=lambda x: (
            x['template_id'],
            get_numeric_cell_id(x['cell_suffix']),
            x['scenario']
        ))

        return JsonResponse({
            'success': True,
            'fixtures': fixtures,
            'count': len(fixtures),
            'templates': sorted(list(templates)),
            'scenarios': sorted(list(scenarios))
        })

    except Exception as e:
        logger.error(f"Error searching fixtures: {e}")
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)

@require_http_methods(["POST"])
@csrf_exempt
def create_github_pr(request):
    """Create GitHub pull request with fixture changes using existing GitHub integration"""
    try:
        data = json.loads(request.body)
        fixtures = data.get('fixtures', [])  # List of {template_id, cell_suffix, scenario}
        pr_title = data.get('pr_title', 'PyBIRD AI Test Fixtures - Regulatory Test Suite')
        pr_description = data.get('pr_description', 'Generated test fixture suite for regulatory template validation')
        branch_name = data.get('branch_name', f'fixture-updates-{int(__import__("time").time())}')
        github_token = data.get('github_token', '')
        repository_url = data.get('repository_url', 'https://github.com/regcommunity/FreeBIRD_EIL')
        organization = data.get('organization', '')
        target_branch = data.get('target_branch', 'develop')

        if not fixtures:
            return JsonResponse({
                'success': False,
                'error': 'No fixtures specified'
            }, status=400)

        if not github_token:
            return JsonResponse({
                'success': False,
                'error': 'GitHub token is required'
            }, status=400)

        # Validate repository URL
        if repository_url and not repository_url.startswith('https://github.com/'):
            return JsonResponse({
                'success': False,
                'error': 'Repository URL must be a valid GitHub URL'
            }, status=400)

        # Collect all changed files with their relative paths
        changed_files = []
        base_dir = Path(settings.BASE_DIR)

        # Add configuration file changes
        config_file = base_dir / "tests/configuration_file_tests.json"
        if config_file.exists():
            changed_files.append({
                'path': 'tests/configuration_file_tests.json',
                'full_path': str(config_file)
            })

        # Add fixture files
        for fixture in fixtures:
            template_id = fixture.get('template_id')
            cell_suffix = fixture.get('cell_suffix')
            scenario = fixture.get('scenario')

            fixture_path = base_dir / f"tests/fixtures/templates/{template_id}/{cell_suffix}/{scenario}"

            # Add all files in the fixture directory
            if fixture_path.exists():
                for file_path in fixture_path.rglob('*'):
                    if file_path.is_file():
                        relative_path = file_path.relative_to(base_dir)
                        changed_files.append({
                            'path': str(relative_path).replace('\\', '/'),  # Ensure forward slashes
                            'full_path': str(file_path)
                        })

        if not changed_files:
            return JsonResponse({
                'success': False,
                'error': 'No fixture files found to commit'
            }, status=400)

        # Create detailed PR description
        detailed_description = f"""{pr_description}

## 🧪 PyBIRD AI Test Fixture Suite

⚠️ **Content Type: Test Fixtures (Not Database Export)**

This PR contains **regulatory test fixtures** generated automatically by PyBIRD AI's Test Fixture Generator. It includes test files for:

"""
        for fixture in fixtures:
            detailed_description += f"- **{fixture['template_id']}_{fixture['cell_suffix']}** ({fixture['scenario']} scenario)\n"

        detailed_description += f"""

### 📁 Test Files Added/Modified:
"""
        for file_info in changed_files:
            file_type = "🐍 Python Test" if file_info['path'].endswith('.py') else \
                       "🗄️ SQL Fixture" if file_info['path'].endswith('.sql') else \
                       "⚙️ Configuration" if file_info['path'].endswith('.json') else \
                       "📦 Package Init" if file_info['path'].endswith('__init__.py') else "📄 File"
            detailed_description += f"- {file_type}: `{file_info['path']}`\n"

        detailed_description += f"""

### 🚀 Test Execution:
Execute these test fixtures using the PyBIRD AI test runner:

```bash
python pybirdai/utils/run_tests.py --uv "False" --config-file "tests/configuration_file_tests.json"
```

### 📋 Test Categories:
- **Python Tests**: Validate cell calculations and datapoint execution
- **SQL Fixtures**: Provide test data setup and cleanup
- **Configuration**: Links test scenarios to regulatory templates

🧪 **Generated by PyBIRD AI Test Fixture Generator** 🧪"""

        # Use existing GitHub integration service
        try:
            from .workflow_services import GitHubIntegrationService

            github_service = GitHubIntegrationService(github_token)

            # Create a temporary directory structure for the files
            import tempfile
            import shutil

            with tempfile.TemporaryDirectory() as temp_dir:
                temp_path = Path(temp_dir)

                # Copy files to temporary directory maintaining structure
                for file_info in changed_files:
                    src_file = Path(file_info['full_path'])
                    dest_file = temp_path / file_info['path']

                    # Create directory structure
                    dest_file.parent.mkdir(parents=True, exist_ok=True)

                    # Copy file
                    if src_file.exists():
                        shutil.copy2(src_file, dest_file)

                # Use test fixture fork workflow to create PR
                result = github_service.fork_and_create_test_pr_workflow(
                    source_repository_url=repository_url,
                    organization=organization,
                    test_directory=str(temp_path),
                    target_branch=target_branch,
                    pr_title=pr_title,
                    pr_body=detailed_description,
                    custom_branch_name=branch_name
                )

                if result.get('success'):
                    return JsonResponse({
                        'success': True,
                        'message': 'Pull request created successfully',
                        'pr_url': result.get('pr_url'),
                        'branch_name': result.get('branch_name'),
                        'files_changed': len(changed_files),
                        'repository': repository_url
                    })
                else:
                    return JsonResponse({
                        'success': False,
                        'error': result.get('error', 'Unknown error occurred')
                    }, status=500)

        except ImportError:
            # Fallback to basic git operations if GitHub service not available
            return JsonResponse({
                'success': False,
                'error': 'GitHub integration service not available. Please ensure all dependencies are installed.'
            }, status=500)

    except Exception as e:
        logger.error(f"Error creating GitHub PR: {e}")
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)

@require_http_methods(["POST"])
@csrf_exempt
def delete_fixtures(request):
    """Delete fixtures including scenario folders, test scripts, and configuration entries"""
    try:
        data = json.loads(request.body)
        fixtures = data.get('fixtures', [])  # List of {template_id, cell_suffix, scenario}

        if not fixtures:
            return JsonResponse({
                'success': False,
                'error': 'No fixtures specified for deletion'
            }, status=400)

        deleted_count = 0
        errors = []
        deletion_summary = {
            'scenario_folders': [],
            'test_scripts': [],
            'config_entries': []
        }

        base_dir = Path(settings.BASE_DIR)

        for fixture in fixtures:
            template_id = fixture.get('template_id')
            cell_suffix = fixture.get('cell_suffix')
            scenario = fixture.get('scenario')

            if not all([template_id, cell_suffix, scenario]):
                errors.append(f"Missing required fields for fixture: {fixture}")
                continue

            fixture_key = f"{template_id}_{cell_suffix}_{scenario}"
            logger.info(f"Processing deletion for fixture: {fixture_key}")

            try:
                # 1. Delete scenario folder and all its contents
                scenario_folder = base_dir / f"tests/fixtures/templates/{template_id}/{cell_suffix}/{scenario}"
                if scenario_folder.exists() and scenario_folder.is_dir():
                    # Log contents before deletion for verification
                    folder_contents = list(scenario_folder.rglob('*'))
                    logger.info(f"Deleting scenario folder with {len(folder_contents)} items: {scenario_folder}")

                    shutil.rmtree(scenario_folder)
                    deletion_summary['scenario_folders'].append({
                        'path': str(scenario_folder.relative_to(base_dir)),
                        'contents_deleted': len(folder_contents)
                    })
                    logger.info(f"Successfully deleted scenario folder: {scenario_folder}")
                else:
                    logger.warning(f"Scenario folder not found: {scenario_folder}")

                # 2. Delete test script file (convert to lowercase and use correct pattern)
                test_script_path = base_dir / f"tests/test_cell_{template_id.lower()}_{cell_suffix.lower()}__{scenario}.py"
                if test_script_path.exists() and test_script_path.is_file():
                    test_script_path.unlink()
                    deletion_summary['test_scripts'].append(str(test_script_path.relative_to(base_dir)))
                    logger.info(f"Deleted test script: {test_script_path}")
                else:
                    logger.warning(f"Test script not found: {test_script_path}")

                # 3. Remove configuration entries
                config_file_path = base_dir / "tests/configuration_file_tests.json"
                if config_file_path.exists():
                    with open(config_file_path, 'r') as f:
                        config_data = json.load(f)

                    original_count = len(config_data.get('tests', []))

                    # Remove matching test entries
                    config_data['tests'] = [
                        test for test in config_data.get('tests', [])
                        if not (test.get('reg_tid') == template_id and
                               test.get('dp_suffix') == cell_suffix and
                               test.get('scenario') == scenario)
                    ]

                    removed_entries = original_count - len(config_data.get('tests', []))
                    if removed_entries > 0:
                        with open(config_file_path, 'w') as f:
                            json.dump(config_data, f, indent=2)
                        deletion_summary['config_entries'].append(f"Removed {removed_entries} entries for {fixture_key}")
                        logger.info(f"Removed {removed_entries} config entries for {fixture_key}")

                # 4. Clean up empty parent directories hierarchically
                try:
                    # Clean up cell folder if empty
                    cell_folder = scenario_folder.parent  # tests/fixtures/templates/{template_id}/{cell_suffix}
                    if cell_folder.exists():
                        remaining_scenarios = list(cell_folder.iterdir())
                        if not remaining_scenarios:
                            cell_folder.rmdir()
                            deletion_summary['scenario_folders'].append({
                                'path': str(cell_folder.relative_to(base_dir)),
                                'type': 'empty_cell_folder',
                                'contents_deleted': 0
                            })
                            logger.info(f"Removed empty cell folder: {cell_folder}")

                            # Clean up template folder if empty
                            template_folder = cell_folder.parent  # tests/fixtures/templates/{template_id}
                            if template_folder.exists():
                                remaining_cells = list(template_folder.iterdir())
                                if not remaining_cells:
                                    template_folder.rmdir()
                                    deletion_summary['scenario_folders'].append({
                                        'path': str(template_folder.relative_to(base_dir)),
                                        'type': 'empty_template_folder',
                                        'contents_deleted': 0
                                    })
                                    logger.info(f"Removed empty template folder: {template_folder}")
                        else:
                            logger.info(f"Cell folder still has {len(remaining_scenarios)} scenarios, keeping: {cell_folder}")
                except (OSError, FileNotFoundError) as e:
                    logger.warning(f"Error during parent directory cleanup: {e}")
                    pass

                deleted_count += 1
                logger.info(f"Successfully deleted fixture: {fixture_key}")

            except Exception as e:
                error_msg = f"Error deleting fixture {fixture_key}: {str(e)}"
                errors.append(error_msg)
                logger.error(error_msg)

        # Prepare response
        success = deleted_count > 0
        response_data = {
            'success': success,
            'deleted_count': deleted_count,
            'total_requested': len(fixtures),
            'errors': errors,
            'deletion_summary': deletion_summary
        }

        if not success and errors:
            response_data['error'] = f"Failed to delete fixtures. Errors: {'; '.join(errors)}"
            return JsonResponse(response_data, status=500)
        elif errors:
            response_data['warning'] = f"Some fixtures could not be deleted: {'; '.join(errors)}"

        return JsonResponse(response_data)

    except json.JSONDecodeError:
        return JsonResponse({
            'success': False,
            'error': 'Invalid JSON in request body'
        }, status=400)
    except Exception as e:
        logger.error(f"Error deleting fixtures: {e}")
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)
