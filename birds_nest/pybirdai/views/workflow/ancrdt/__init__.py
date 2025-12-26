# coding=UTF-8
# ANCRDT workflow views

from .execution import execute_ancrdt_step, get_ancrdt_status
from .views import ancrdt_dashboard, approve_joins_metadata

# Transformation views (deprecated step-by-step)
from .transformation_views import (
    ancrdt_fetch_csv,
    ancrdt_import,
    ancrdt_create_joins_metadata,
    ancrdt_create_executable_joins,
)

# Workflow views (step-based workflow)
from .workflow_views import (
    ancrdt_step_0_view,
    ancrdt_step_1_view,
    ancrdt_step_2_view,
    ancrdt_step_3_view,
    ancrdt_step_1_review_view,
    ancrdt_step_2_review_view,
    ancrdt_step_3_review_view,
    ancrdt_step_4_run_tests_view,
    ancrdt_step_4_review_view,
    ancrdt_execute_tables_view,
    execute_ancrdt_table_with_fixture,
    download_ancrdt_csv,
    api_ancrdt_cubes,
    api_ancrdt_cube_structure,
)

# SQL fixture editor views
from .sql_fixture_editor_views import (
    sql_fixtures_editor,
    load_sql_fixture,
    save_sql_fixture,
    create_sql_fixture,
    delete_sql_fixture,
    list_sql_fixtures,
)

# Table views
from .table_views import execute_ancrdt_table

__all__ = [
    # Execution
    'execute_ancrdt_step', 'get_ancrdt_status',
    'ancrdt_dashboard', 'approve_joins_metadata',
    # Transformation views
    'ancrdt_fetch_csv', 'ancrdt_import',
    'ancrdt_create_joins_metadata', 'ancrdt_create_executable_joins',
    # Workflow views
    'ancrdt_step_0_view', 'ancrdt_step_1_view', 'ancrdt_step_2_view', 'ancrdt_step_3_view',
    'ancrdt_step_1_review_view', 'ancrdt_step_2_review_view', 'ancrdt_step_3_review_view',
    'ancrdt_step_4_run_tests_view', 'ancrdt_step_4_review_view', 'ancrdt_execute_tables_view',
    'execute_ancrdt_table_with_fixture', 'download_ancrdt_csv',
    'api_ancrdt_cubes', 'api_ancrdt_cube_structure',
    # SQL fixture editor
    'sql_fixtures_editor', 'load_sql_fixture', 'save_sql_fixture',
    'create_sql_fixture', 'delete_sql_fixture', 'list_sql_fixtures',
    # Table views
    'execute_ancrdt_table',
]
