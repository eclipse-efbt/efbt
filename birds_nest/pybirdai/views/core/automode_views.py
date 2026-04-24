# coding=UTF-8
# Copyright (c) 2025 Bird Software Solutions Ltd
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDX-License-Identifier: EPL-2.0
#
# Contributors:
#    Neil Mackenzie - initial API and implementation
"""
Automode views for automated database setup and configuration.
"""
import os
import json
import logging

from django.http import JsonResponse
from django.conf import settings

from pybirdai.entry_points.database_setup import RunApplicationSetup
from pybirdai.entry_points.create_django_models import RunCreateDjangoModels
from pybirdai.utils.secure_error_handling import SecureErrorHandler

from .loading_helpers import create_response_with_loading, create_response_with_loading_extended

logger = logging.getLogger(__name__)

def _json_error_response(message: str, status: int = 400, **extra):
    """Return a consistent JSON error payload."""
    payload = {
        'success': False,
        'error': message,
    }
    payload.update({key: value for key, value in extra.items() if value is not None})
    return JsonResponse(payload, status=status)


def _internal_error_response(exception: Exception, context: str, request, *, message: str | None = None):
    """Hide implementation details from JSON error responses."""
    error_data = SecureErrorHandler.handle_exception(exception, context, request)
    return _json_error_response(message or error_data['message'], status=500)


def _public_automode_config(config_data):
    """Drop secrets from automode configuration payloads."""
    if not isinstance(config_data, dict):
        return config_data

    return {
        key: value
        for key, value in config_data.items()
        if key not in {'github_token'}
    }


def _public_automode_step_summary(step_results, allowed_keys):
    """Return a stable summary for a nested automode step result."""
    if not isinstance(step_results, dict):
        return {}

    summary = {}
    for key in allowed_keys:
        value = step_results.get(key)
        if value is not None:
            summary[key] = value

    errors = step_results.get('errors')
    if isinstance(errors, list) and errors:
        summary['error_count'] = len(errors)

    return summary


def _public_automode_files_fetched(files_fetched):
    """Return safe file-fetch counters for automode execution."""
    return _public_automode_step_summary(
        files_fetched,
        (
            'technical_export',
            'config_files',
            'test_suite',
            'generated_python',
            'filter_code',
            'test_fixtures',
            'report_templates',
        ),
    )


def _public_automode_execute_results(results):
    """Return a whitelisted summary for the execute endpoint."""
    if not isinstance(results, dict):
        return {}

    summary = {
        'files_fetched': _public_automode_files_fetched(results.get('files_fetched')),
        'database_created': bool(results.get('database_created', False)),
        'server_restart_required': bool(results.get('server_restart_required', False)),
        'setup_completed': bool(results.get('setup_completed', False)),
    }

    for key in ('stopped_at', 'next_steps', 'detailed_next_steps'):
        value = results.get(key)
        if value:
            summary[key] = value

    errors = results.get('errors')
    if isinstance(errors, list) and errors:
        summary['error_count'] = len(errors)

    return summary


def _public_automode_post_restart_results(results):
    """Return a whitelisted summary for the post-restart endpoint."""
    if not isinstance(results, dict):
        return {}

    summary = {
        'setup_completed': bool(results.get('setup_completed', False)),
    }

    for key in ('stopped_at', 'next_steps'):
        value = results.get(key)
        if value:
            summary[key] = value

    section_keys = {
        'smcubes_rules': (
            'database_setup',
            'metadata_population',
            'filters_creation',
            'joins_creation',
        ),
        'python_code': (
            'tests_executed',
            'suites_run',
        ),
        'full_execution': (
            'tests_executed',
            'suites_run',
        ),
    }

    for key, allowed_keys in section_keys.items():
        section_summary = _public_automode_step_summary(results.get(key), allowed_keys)
        if section_summary:
            summary[key] = section_summary

    errors = results.get('errors')
    if isinstance(errors, list) and errors:
        summary['error_count'] = len(errors)

    return summary


def automode_create_database(request):
    """Create database for automode setup."""
    if request.GET.get('execute') == 'true':
        try:
            app_config = RunApplicationSetup('pybirdai', 'birds_nest')
            app_config.run_automode_setup()
            return JsonResponse({
                'status': 'success',
                'message': 'Database preparation completed successfully!',
                'instructions': [
                    'The database configuration files have been generated.',
                    'To complete the setup:',
                    '1. Stop the Django server (Ctrl+C in the terminal)',
                    '2. Run: python manage.py complete_automode_setup',
                    '3. Restart the server using your preferred port (e.g., python manage.py runserver 0.0.0.0:8001)',
                    '4. Your database will be ready to use!'
                ]
            })
        except Exception as e:
            logger.error(f"Automode database setup failed: {str(e)}")
            return SecureErrorHandler.secure_json_response(e, 'automode database setup', request)

    return create_response_with_loading_extended(
        request,
        "Creating BIRD Database (Automode) - Preparing database configuration files (this won't restart the server)",
        "Database preparation completed successfully! Please follow the instructions to complete the setup.",
        '/pybirdai/automode',
        "Back to Automode"
    )


def automode_import_bird_metamodel_from_website(request):
    """Import BIRD metamodel from website for automode."""
    if request.GET.get('execute') == 'true':
        from pybirdai.utils import bird_ecb_website_fetcher
        client = bird_ecb_website_fetcher.BirdEcbWebsiteClient()
        print(client.request_and_save_all())

    return create_response_with_loading(
        request,
        "Importing BIRD Metamodel from Website (Automode)",
        "BIRD Metamodel import completed successfully!",
        '/pybirdai/automode',
        "Back to Automode"
    )


def test_automode_components(request):
    """Test view to verify automode components work individually."""
    if request.GET.get('execute') == 'true':
        try:
            # Test basic setup
            base_dir = settings.BASE_DIR
            logger.info(f"Base directory: {base_dir}")

            # Check if required directories exist
            resources_dir = os.path.join(base_dir, 'resources')
            results_dir = os.path.join(base_dir, 'results')
            ldm_dir = os.path.join(resources_dir, 'ldm')

            logger.info(f"Resources directory exists: {os.path.exists(resources_dir)}")
            logger.info(f"Results directory exists: {os.path.exists(results_dir)}")
            logger.info(f"LDM directory exists: {os.path.exists(ldm_dir)}")

            if os.path.exists(ldm_dir):
                ldm_files = os.listdir(ldm_dir)
                logger.info(f"LDM files: {ldm_files}")

            # Test creating a simple Django model instance
            app_config = RunCreateDjangoModels('pybirdai', 'birds_nest')
            logger.info("RunCreateDjangoModels instance created successfully")

            return JsonResponse({
                'status': 'success',
                'message': 'Basic components test passed',
                'base_dir': str(base_dir),
                'resources_exists': os.path.exists(resources_dir),
                'results_exists': os.path.exists(results_dir),
                'ldm_exists': os.path.exists(ldm_dir)
            })

        except Exception as e:
            logger.error(f"Test failed: {str(e)}")
            return SecureErrorHandler.secure_json_response(e, 'test execution', request)

    return create_response_with_loading_extended(
        request,
        "Testing Automode Components",
        "Component test completed successfully!",
        '/pybirdai/automode',
        "Back to Automode"
    )


def run_fetch_curated_resources(request):
    """Fetch curated resources from GitHub."""
    if request.GET.get('execute') == 'true':
        try:
            from pybirdai.utils import github_file_fetcher

            fetcher = github_file_fetcher.GitHubFileFetcher("https://github.com/regcommunity/FreeBIRD")

            logger.info("STEP 1: Fetching specific derivation model file")

            fetcher.fetch_derivation_model_file(
                "birds_nest/pybirdai",
                "bird_data_model.py",
                f"resources{os.sep}derivation_implementation",
                "bird_data_model_with_derivation.py"
            )

            logger.info("STEP 2: Fetching all artefacts (database, filter code, derivation, joins config)")
            fetcher.fetch_all_artefacts()

            logger.info("STEP 3: Fetching test fixtures and templates")
            fetcher.fetch_test_fixtures()

            logger.info("STEP 4: Fetching REF_FINREP report template HTML files")
            fetcher.fetch_report_template_htmls()

            logger.info("File fetching process completed successfully!")
            print("File fetching process completed!")

            base_dir = settings.BASE_DIR
            resources_dir = os.path.join(base_dir, 'resources')
            results_dir = os.path.join(base_dir, 'results')
            ldm_dir = os.path.join(resources_dir, 'ldm')

            return JsonResponse({
                'status': 'success',
                'message': 'Basic components test passed',
                'base_dir': str(base_dir),
                'resources_exists': os.path.exists(resources_dir),
                'results_exists': os.path.exists(results_dir),
                'ldm_exists': os.path.exists(ldm_dir)
            })

        except Exception as e:
            logger.error(f"Test failed: {str(e)}")
            return SecureErrorHandler.secure_json_response(e, 'test execution', request)

    return create_response_with_loading_extended(
        request,
        "Fetching Test Components and derived fields",
        "Test components and derived fields fetched successfully!",
        '/pybirdai/automode',
        "Back to Automode"
    )


def automode_configure(request):
    """Handle automode configuration form submission."""
    try:
        from pybirdai.views.forms import AutomodeConfigurationSessionForm
        from pybirdai.api.workflow_api import AutomodeConfigurationService
    except Exception as e:
        logger.error(f"Error importing modules in automode_configure: {str(e)}")
        return _internal_error_response(
            e,
            'loading automode configuration dependencies',
            request,
            message='Configuration error. Please contact the administrator.',
        )

    if request.method == 'POST':
        try:
            # Use session-based form that doesn't depend on database model
            form = AutomodeConfigurationSessionForm(request.POST)

            if form.is_valid():
                # Validate GitHub URLs if GitHub is selected
                service = AutomodeConfigurationService()

                if form.cleaned_data['technical_export_source'] == 'GITHUB':
                    url = form.cleaned_data['technical_export_github_url']
                    # Check environment variable first, then form input
                    token = os.environ.get('GITHUB_TOKEN', form.cleaned_data.get('github_token'))
                    if not service.validate_github_repository(url, token):
                        error_msg = f'Technical export GitHub repository is not accessible: {url}'
                        if not token:
                            error_msg += '. For private repositories, please provide a GitHub Personal Access Token.'
                        else:
                            error_msg += '. Please check your token has "repo" permissions and is valid.'
                        return JsonResponse({
                            'success': False,
                            'error': error_msg
                        })

                if form.cleaned_data['config_files_source'] == 'GITHUB':
                    url = form.cleaned_data['config_files_github_url']
                    # Check environment variable first, then form input
                    token = os.environ.get('GITHUB_TOKEN', form.cleaned_data.get('github_token'))
                    if not service.validate_github_repository(url, token):
                        error_msg = f'Configuration files GitHub repository is not accessible: {url}'
                        if not token:
                            error_msg += '. For private repositories, please provide a GitHub Personal Access Token.'
                        else:
                            error_msg += '. Please check your token has "repo" permissions and is valid.'
                        return JsonResponse({
                            'success': False,
                            'error': error_msg
                        })

                # Store configuration in a temporary file instead of database/session
                technical_export_github_url = form.cleaned_data.get('technical_export_github_url', '')
                bird_content_branch = request.POST.get('bird_content_branch', 'main')
                config_data = {
                    'data_model_type': form.cleaned_data['data_model_type'],
                    'technical_export_source': form.cleaned_data['technical_export_source'],
                    'technical_export_github_url': technical_export_github_url,
                    'config_files_source': 'GITHUB',  # Always use GitHub
                    'config_files_github_url': technical_export_github_url,  # Always use same URL as BIRD Content Repository
                    'test_suite_source': 'GITHUB',
                    'test_suite_github_url': request.POST.get('test_suite_github_url', ''),
                    'when_to_stop': form.cleaned_data['when_to_stop'],
                    'enable_lineage_tracking': form.cleaned_data.get('enable_lineage_tracking', True),
                    'bird_content_branch': bird_content_branch,
                    'test_suite_branch': request.POST.get('test_suite_branch', 'main'),
                    'github_branch': bird_content_branch,  # Keep for backwards compatibility
                }

                # Store GitHub token (temporarily, for execution)
                # Prioritize environment variable, then form input
                github_token = os.environ.get('GITHUB_TOKEN', form.cleaned_data.get('github_token', ''))
                if github_token:
                    config_data['github_token'] = github_token

                # Save to temporary file
                _save_temp_config(config_data)

                logger.info("Automode configuration saved to temporary file")
                return JsonResponse({
                    'success': True,
                    'message': 'Configuration saved successfully. Ready for execution.'
                })
            else:
                # Return form errors
                errors = []
                for field, field_errors in form.errors.items():
                    for error in field_errors:
                        errors.append(f"{field}: {error}")

                return JsonResponse({
                    'success': False,
                    'error': '; '.join(errors)
                })

        except Exception as e:
            logger.error(f"Error saving automode configuration: {str(e)}")
            return _internal_error_response(e, 'saving automode configuration', request)

    # GET request - return current configuration
    try:
        # First try to get configuration from temporary file
        temp_config = _load_temp_config()

        if temp_config:
            # Use temporary file configuration if available
            config_data = temp_config
        else:
            # Fall back to database configuration if temp file is empty
            try:
                from pybirdai.views.models.workflow_model import AutomodeConfiguration
                config = AutomodeConfiguration.get_active_configuration()
                config_data = {
                    'data_model_type': config.data_model_type if config else 'ELDM',
                    'technical_export_source': config.technical_export_source if config else 'BIRD_WEBSITE',
                    'technical_export_github_url': config.technical_export_github_url if config else '',
                    'config_files_source': config.config_files_source if config else 'MANUAL',
                    'config_files_github_url': config.config_files_github_url if config else '',
                    'when_to_stop': config.when_to_stop if config else 'RESOURCE_DOWNLOAD',
                    'enable_lineage_tracking': True
                }
            except Exception:
                # If database doesn't exist or model isn't available, use defaults
                config_data = {
                    'data_model_type': 'ELDM',
                    'technical_export_source': 'BIRD_WEBSITE',
                    'technical_export_github_url': '',
                    'config_files_source': 'MANUAL',
                    'config_files_github_url': '',
                    'when_to_stop': 'RESOURCE_DOWNLOAD',
                    'enable_lineage_tracking': True
                }

        return JsonResponse({
            'success': True,
            'config': _public_automode_config(config_data)
        })
    except Exception as e:
        logger.error(f"Error retrieving automode configuration: {str(e)}")
        return _internal_error_response(e, 'retrieving automode configuration', request)


def automode_execute(request):
    """Execute automode setup with current configuration."""
    from pybirdai.api.workflow_api import AutomodeConfigurationService

    if request.method == 'POST':
        try:
            # Get configuration from temporary file
            temp_config_data = _load_temp_config()
            if not temp_config_data:
                return JsonResponse({
                    'success': False,
                    'error': 'No configuration found. Please configure automode first.'
                })

            # Check confirmation
            confirm_execution = request.POST.get('confirm_execution') == 'on'
            if not confirm_execution:
                return JsonResponse({
                    'success': False,
                    'error': 'Execution must be confirmed.'
                })

            force_refresh = request.POST.get('force_refresh') == 'on'
            # Get GitHub token from environment, temp config, or POST data
            github_token = (os.environ.get('GITHUB_TOKEN') or
                          temp_config_data.get('github_token') or
                          request.POST.get('github_token', '')).strip() or None

            # Create a temporary configuration object from temp file data
            from pybirdai.models.workflow_model import AutomodeConfiguration
            temp_config = AutomodeConfiguration(
                data_model_type=temp_config_data['data_model_type'],
                technical_export_source=temp_config_data['technical_export_source'],
                technical_export_github_url=temp_config_data.get('technical_export_github_url', ''),
                config_files_source=temp_config_data['config_files_source'],
                config_files_github_url=temp_config_data.get('config_files_github_url', ''),
                test_suite_source=temp_config_data.get('test_suite_source', 'GITHUB'),
                test_suite_github_url=temp_config_data.get('test_suite_github_url', ''),
                when_to_stop=temp_config_data['when_to_stop']
            )
            temp_config.bird_content_branch = temp_config_data.get(
                'bird_content_branch',
                temp_config_data.get('github_branch', 'main'),
            )
            temp_config.test_suite_branch = temp_config_data.get('test_suite_branch', 'main')
            temp_config.github_branch = temp_config_data.get(
                'github_branch',
                temp_config_data.get('bird_content_branch', 'main'),
            )

            # Execute automode setup with session-based configuration
            service = AutomodeConfigurationService()
            results = service.execute_automode_setup_with_database_creation(temp_config, github_token, force_refresh)

            if results['errors']:
                public_results = _public_automode_execute_results(results)
                return JsonResponse({
                    'success': False,
                    'error': 'Execution completed with errors. Please review the logs and try again later.',
                    'results': public_results,
                })
            else:
                # Only clear temporary config file if setup is completely finished
                # If server restart is required, keep the temp file for continuation
                if results.get('setup_completed', False) and not results.get('server_restart_required', False):
                    _clear_temp_config()

                # Provide clear messaging about what happened
                message = 'Automode setup executed successfully'
                next_steps = []

                if results.get('server_restart_required', False):
                    message = 'Initial setup completed - database created successfully!'
                    next_steps = [
                        '1. Stop the Django server (Ctrl+C in the terminal)',
                        '2. Run: python manage.py complete_automode_setup  (this will take a while and will restart the server)',
                        '3. After the server restarts, press "Continue After Restart" button below'
                    ]
                elif results.get('stopped_at') == 'RESOURCE_DOWNLOAD':
                    message = 'Resource download completed - ready for step-by-step mode'

                # Add next steps to results if present
                if next_steps:
                    results['detailed_next_steps'] = next_steps

                public_results = _public_automode_execute_results(results)
                return JsonResponse({
                    'success': True,
                    'message': message,
                    'results': public_results,
                })

        except Exception as e:
            logger.error(f"Error executing automode setup: {str(e)}")
            return _internal_error_response(e, 'executing automode setup', request)

    # GET request not supported for execution
    return JsonResponse({
        'success': False,
        'error': 'GET method not supported for execution'
    })


def automode_continue_post_restart(request):
    """Handle continuing automode execution after server restart."""
    if request.method != 'POST':
        return JsonResponse({
            'success': False,
            'error': 'POST method required for continuation'
        })

    try:
        from pybirdai.api.workflow_api import AutomodeConfigurationService
    except Exception as e:
        logger.error(f"Error importing modules in automode_continue_post_restart: {str(e)}")
        return _internal_error_response(
            e,
            'loading automode post-restart dependencies',
            request,
            message='Configuration error. Please contact the administrator.',
        )

    try:
        # Load configuration from temporary file
        temp_config = _load_temp_config()

        if not temp_config:
            # Provide more detailed error information for debugging
            temp_path = _get_temp_config_path()
            fallback_path = os.path.join('.', 'automode_config.json')

            error_details = [
                f"Expected config at: {temp_path} (exists: {os.path.exists(temp_path)})",
                f"Fallback config at: {fallback_path} (exists: {os.path.exists(fallback_path)})",
                f"Current working directory: {os.getcwd()}",
                f"BASE_DIR: {getattr(settings, 'BASE_DIR', 'Not set')}"
            ]

            logger.error("Configuration not found. Debug details:")
            for detail in error_details:
                logger.error(f"  {detail}")

            return JsonResponse({
                'success': False,
                'error': 'No configuration found. Please configure and save settings first.',
                'debug_info': {
                    'temp_config_exists': os.path.exists(temp_path),
                    'fallback_exists': os.path.exists(fallback_path),
                } if hasattr(settings, 'DEBUG') and settings.DEBUG else None,
            })

        # Create a simple config object from the temp data
        class SimpleConfig:
            def __init__(self, data):
                self.data_model_type = data.get('data_model_type', 'ELDM')
                self.technical_export_source = data.get('technical_export_source', 'BIRD_WEBSITE')
                self.technical_export_github_url = data.get('technical_export_github_url', '')
                self.config_files_source = data.get('config_files_source', 'MANUAL')
                self.config_files_github_url = data.get('config_files_github_url', '')
                self.test_suite_source = data.get('test_suite_source', 'GITHUB')
                self.test_suite_github_url = data.get('test_suite_github_url', '')
                self.bird_content_branch = data.get('bird_content_branch', data.get('github_branch', 'main'))
                self.test_suite_branch = data.get('test_suite_branch', 'main')
                self.github_branch = data.get('github_branch', self.bird_content_branch)
                self.when_to_stop = data.get('when_to_stop', 'RESOURCE_DOWNLOAD')

        config = SimpleConfig(temp_config)

        # Execute post-restart steps
        service = AutomodeConfigurationService()
        results = service.execute_automode_post_restart(config)

        logger.info(
            "Automode post-restart execution completed: %s",
            _public_automode_post_restart_results(results),
        )

        if results['errors']:
            public_results = _public_automode_post_restart_results(results)
            return JsonResponse({
                'success': False,
                'error': 'Post-restart execution completed with errors. Please review the logs and try again later.',
                'results': public_results,
            })
        else:
            # Clear temporary config file after successful completion
            if results.get('setup_completed', False):
                _clear_temp_config()

            public_results = _public_automode_post_restart_results(results)
            return JsonResponse({
                'success': True,
                'message': 'Automode post-restart execution completed successfully',
                'results': public_results,
            })

    except Exception as e:
        logger.error(f"Error in automode post-restart execution: {str(e)}")
        return _internal_error_response(e, 'continuing automode after restart', request)


def automode_debug_config(request):
    """Debug endpoint to check configuration file status."""
    try:
        temp_path = _get_temp_config_path()
        fallback_path = os.path.join('.', 'automode_config.json')

        base_dir_raw = getattr(settings, 'BASE_DIR', 'Not set')

        debug_info = {
            'temp_config_exists': os.path.exists(temp_path),
            'fallback_exists': os.path.exists(fallback_path),
            'path_resolution_type': type(base_dir_raw).__name__,
        }

        # Try to read config if exists
        config_data = None
        if os.path.exists(temp_path):
            try:
                with open(temp_path, 'r') as f:
                    config_data = json.load(f)
                debug_info['config_data'] = _public_automode_config(config_data)
                debug_info['config_status'] = 'Successfully loaded from temp path'
            except Exception as e:
                SecureErrorHandler.handle_exception(e, 'reading automode debug temp config', request)
                debug_info['config_error'] = 'Unable to load configuration data.'
                debug_info['config_status'] = 'Error loading from temp path'
        elif os.path.exists(fallback_path):
            try:
                with open(fallback_path, 'r') as f:
                    config_data = json.load(f)
                debug_info['config_data'] = _public_automode_config(config_data)
                debug_info['config_status'] = 'Successfully loaded from fallback path'
            except Exception as e:
                SecureErrorHandler.handle_exception(e, 'reading automode debug fallback config', request)
                debug_info['config_error'] = 'Unable to load configuration data.'
                debug_info['config_status'] = 'Error loading from fallback path'
        else:
            debug_info['config_status'] = 'No configuration file found'

        return JsonResponse({
            'success': True,
            'debug_info': debug_info
        })
    except Exception as e:
        return _internal_error_response(e, 'debugging automode configuration', request)


def automode_status(request):
    """Get current automode configuration status and file information."""
    from pybirdai.views.models.workflow_model import AutomodeConfiguration

    try:
        config = AutomodeConfiguration.get_active_configuration()

        # Check file existence
        smcubes_dir = os.path.join(settings.BASE_DIR, 'artefacts', 'smcubes_artefacts')
        file_status = {
            'technical_export': {
                'directory': 'artefacts/smcubes_artefacts',
                'exists': os.path.exists(smcubes_dir),
                'file_count': len(os.listdir(smcubes_dir)) if os.path.exists(smcubes_dir) else 0
            },
            'joins_configuration': {
                'directory': 'artefacts/joins_configuration',
                'exists': os.path.exists(os.path.join(settings.BASE_DIR, 'artefacts', 'joins_configuration')),
                'file_count': len(os.listdir(os.path.join(settings.BASE_DIR, 'artefacts', 'joins_configuration'))) if os.path.exists(os.path.join(settings.BASE_DIR, 'artefacts', 'joins_configuration')) else 0
            },
            'extra_variables': {
                'directory': 'resources/extra_variables',
                'exists': os.path.exists('resources/extra_variables'),
                'file_count': len(os.listdir('resources/extra_variables')) if os.path.exists('resources/extra_variables') else 0
            },
            'ldm': {
                'directory': 'resources/ldm',
                'exists': os.path.exists('resources/ldm'),
                'file_count': len(os.listdir('resources/ldm')) if os.path.exists('resources/ldm') else 0
            }
        }

        return JsonResponse({
            'success': True,
            'configuration': {
                'exists': config is not None,
                'data_model_type': config.data_model_type if config else None,
                'technical_export_source': config.technical_export_source if config else None,
                'config_files_source': config.config_files_source if config else None,
                'last_updated': config.updated_at.isoformat() if config else None
            },
            'file_status': file_status
        })

    except Exception as e:
        logger.error(f"Error getting automode status: {str(e)}")
        return JsonResponse({
            'success': False,
            'error': 'An internal error occurred while getting status.'
        })


# Private helper functions for temp config management

def _get_temp_config_path():
    """Get the path for the temporary configuration file."""
    import tempfile

    # Use a persistent temp file in the project directory
    base_dir = getattr(settings, 'BASE_DIR', tempfile.gettempdir())

    # Convert Path object to string if necessary (Django 5.x uses Path objects)
    if hasattr(base_dir, '__fspath__'):  # Check if it's a path-like object
        temp_dir = str(base_dir)
    else:
        temp_dir = base_dir

    # Ensure we use absolute path to avoid working directory issues
    if not os.path.isabs(temp_dir):
        temp_dir = os.path.abspath(temp_dir)

    config_path = os.path.join(temp_dir, 'automode_config.json')
    logger.debug(f"Temp config path resolved to: {config_path}")
    return config_path


def _save_temp_config(config_data):
    """Save configuration data to a temporary file."""
    temp_path = _get_temp_config_path()

    try:
        with open(temp_path, 'w') as f:
            json.dump(config_data, f, indent=2)
        logger.info(f"Configuration saved to temporary file: {temp_path}")
    except Exception as e:
        logger.error(f"Error saving configuration to temporary file: {e}")
        raise


def _load_temp_config():
    """Load configuration data from temporary file."""
    temp_path = _get_temp_config_path()

    try:
        logger.info(f"Attempting to load configuration from: {temp_path}")
        if os.path.exists(temp_path):
            logger.info(f"Configuration file exists at: {temp_path}")
            with open(temp_path, 'r') as f:
                config_data = json.load(f)
            logger.info(f"Configuration loaded successfully from: {temp_path}")
            logger.debug("Loaded config data: %s", _public_automode_config(config_data))
            return config_data
        else:
            logger.warning(f"No temporary configuration file found at: {temp_path}")

            # Try fallback location for debugging
            fallback_path = os.path.join('.', 'automode_config.json')
            logger.info(f"Checking fallback location: {fallback_path}")
            if os.path.exists(fallback_path):
                logger.info(f"Found config at fallback location: {fallback_path}")
                try:
                    with open(fallback_path, 'r') as f:
                        config_data = json.load(f)
                    logger.info(
                        "Successfully loaded config from fallback: %s",
                        _public_automode_config(config_data),
                    )
                    return config_data
                except Exception as e:
                    logger.error(f"Error reading fallback config file: {e}")
            else:
                logger.warning(f"No configuration file found at fallback location either: {fallback_path}")

            return None
    except Exception as e:
        logger.error(f"Error loading configuration from temporary file {temp_path}: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return None


def _clear_temp_config():
    """Clear the temporary configuration file."""
    temp_path = _get_temp_config_path()

    try:
        if os.path.exists(temp_path):
            os.remove(temp_path)
            logger.info(f"Temporary configuration file cleared: {temp_path}")
    except Exception as e:
        logger.error(f"Error clearing temporary configuration file: {e}")
