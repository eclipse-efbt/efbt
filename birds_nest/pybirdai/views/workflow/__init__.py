# coding=UTF-8
# Copyright (c) 2024 Bird Software Solutions Ltd
# Workflow views module - extracted from workflow_views.py

from .helpers import encode_file_list, refresh_complete_status, load_test_results, _discover_test_suites
from .github import (
    _get_github_token, _set_github_token, _clear_github_token,
    export_database_to_github, _in_memory_github_token
)
from .status import (
    _migration_status, _database_setup_status, _automode_status, _setup_database_models_status,
    _reset_migration_status, _reset_database_setup_status, _reset_automode_status,
    _reset_setup_database_models_status, workflow_automode_status, workflow_task_status,
    workflow_migration_status, workflow_setup_database_models_status, workflow_database_setup_status
)
from .progress import get_dpm_task_grid, get_ancrdt_task_grid, get_workflow_progress_summary
from .async_operations import (
    _run_migrations_async, _run_setup_database_models_async,
    _load_task1_completion_from_marker, _run_database_setup_async, _run_automode_async
)
from .sync_operations import run_automode_sync
from .dashboard import workflow_dashboard
from .tasks import (
    workflow_task_router, task1_smcubes_core, task2_smcubes_rules,
    task3_python_rules, task4_full_execution
)
from .substeps import (
    workflow_task_substep, _execute_database_creation_substep, _execute_task1_substep,
    _execute_task2_substep, _execute_task3_substep, _execute_task4_substep,
    workflow_task_substep_with_loading
)
from .automode import workflow_automode, workflow_save_config
from .setup import workflow_run_migrations, workflow_setup_database_models, workflow_database_setup
from .session import (
    workflow_clone_import, workflow_session_check,
    workflow_reset_session_full, workflow_reset_session_partial
)

# ANCRDT submodule exports
from .ancrdt.execution import execute_ancrdt_step, get_ancrdt_status
from .ancrdt.views import ancrdt_dashboard, approve_joins_metadata

__all__ = [
    # Helpers
    'encode_file_list', 'refresh_complete_status', 'load_test_results',
    # GitHub
    '_get_github_token', '_set_github_token', '_clear_github_token',
    'export_database_to_github', '_in_memory_github_token',
    # Status
    '_migration_status', '_database_setup_status', '_automode_status', '_setup_database_models_status',
    '_reset_migration_status', '_reset_database_setup_status', '_reset_automode_status',
    '_reset_setup_database_models_status', 'workflow_automode_status', 'workflow_task_status',
    'workflow_migration_status', 'workflow_setup_database_models_status', 'workflow_database_setup_status',
    # Progress
    'get_dpm_task_grid', 'get_ancrdt_task_grid', 'get_workflow_progress_summary',
    # Async operations
    '_run_migrations_async', '_run_setup_database_models_async',
    '_load_task1_completion_from_marker', '_run_database_setup_async', '_run_automode_async',
    # Sync operations
    'run_automode_sync',
    # Dashboard
    'workflow_dashboard',
    # Tasks
    'workflow_task_router', 'task1_smcubes_core', 'task2_smcubes_rules',
    'task3_python_rules', 'task4_full_execution',
    # Substeps
    'workflow_task_substep', '_execute_database_creation_substep', '_execute_task1_substep',
    '_execute_task2_substep', '_execute_task3_substep', '_execute_task4_substep',
    '_discover_test_suites', 'workflow_task_substep_with_loading',
    # Automode
    'workflow_automode', 'workflow_save_config',
    # Setup
    'workflow_run_migrations', 'workflow_setup_database_models', 'workflow_database_setup',
    # Session
    'workflow_clone_import', 'workflow_session_check',
    'workflow_reset_session_full', 'workflow_reset_session_partial',
    # ANCRDT
    'execute_ancrdt_step', 'get_ancrdt_status',
    'ancrdt_dashboard', 'approve_joins_metadata',
]
