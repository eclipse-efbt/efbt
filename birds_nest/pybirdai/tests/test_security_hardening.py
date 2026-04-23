import csv
import json
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import mock_open, patch

from django.test import Client, RequestFactory, SimpleTestCase

from pybirdai.api.ancrdt_tables_graph_api import get_ancrdt_tables_graph
from pybirdai.api.workflow_api import GitHubIntegrationService
from pybirdai.api.enhanced_lineage_api_v2 import get_enhanced_lineage
from pybirdai.process_steps.database_setup.automode_orchestrator import (
    _run_migrations_in_subprocess,
)
from pybirdai.views.core.process_execution_views import execute_data_point
from pybirdai.views.core.csv_views import (
    import_mapping_from_csv,
    view_csv_file,
)
from pybirdai.views.core.semantic_integration_views import get_domain_members
from pybirdai.views.core.visualisation_service import NetworkGraphGenerationService
from pybirdai.views.execution_code_editor_views import (
    get_file_diff,
    save_code_modifications,
    save_filter_code_file,
    sync_file_to_production,
)
from pybirdai.views.core.derivation_configuration_views import (
    get_derivation_file_content,
    get_derivation_files_sync_status,
    save_derivation_file,
)
from pybirdai.views.core.automode_views import (
    automode_configure,
    automode_continue_post_restart,
    automode_debug_config,
)
from pybirdai.views.joins_metadata_embed_views import add_cube_link_ajax
from pybirdai.views.member_link_views import get_member_links_json
from pybirdai.views.joins_configuration_views import load_csv
from pybirdai.views.test_data_template_views import (
    convert_sql_to_csv,
    export_bird_excel_template,
)
from pybirdai.views.workflow.ancrdt.execution import execute_ancrdt_step
from pybirdai.views.workflow.ancrdt.transformation_views import ancrdt_import
from pybirdai.views.workflow.ancrdt.table_views import execute_ancrdt_table
from pybirdai.views.workflow.ancrdt.workflow_views import (
    api_ancrdt_cube_structure,
    api_ancrdt_cubes,
    download_ancrdt_csv,
)
from pybirdai.views.workflow.async_operations import trigger_server_restart
from pybirdai.views.workflow.github import export_database_to_github
from pybirdai.views.workflow.session import workflow_session_check
from pybirdai.views.workflow.setup import workflow_run_migrations
from pybirdai.views.workflow.substeps import _execute_task1_substep
from pybirdai.views.workflow.tasks import task2_smcubes_rules
from pybirdai.process_steps.joins_configuration.joins_configuration_manager import (
    JoinsConfigurationManager,
)
from pybirdai.views.workflow.ancrdt.sql_fixture_editor_views import save_sql_fixture
from pybirdai.views.workflow.code_sync import CodeSyncManager


class SecurityHardeningTests(SimpleTestCase):
    def setUp(self):
        self.factory = RequestFactory()

    class _DummySession(dict):
        session_key = 'test-session-key'

    def test_joins_configuration_manager_rejects_path_traversal_framework_name(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = JoinsConfigurationManager(base_path=tmpdir)

            with self.assertRaises(ValueError):
                manager.get_file_path('in_scope_reports', '../../etc/passwd')

    def test_code_sync_manager_rejects_path_traversal_filename(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = CodeSyncManager(base_dir=tmpdir)

            with self.assertRaises(ValueError):
                manager.sync_file('../outside.py')

    def test_save_code_modifications_rejects_path_traversal_filename(self):
        request = self.factory.post(
            '/pybirdai/execution-code-editing/save/',
            data=json.dumps({
                'file_name': '../../settings.py',
                'code_content': 'print("safe")',
                'source': 'joins',
            }),
            content_type='application/json',
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            with self.settings(BASE_DIR=Path(tmpdir)):
                response = save_code_modifications(request)

        self.assertEqual(response.status_code, 400)

    def test_save_sql_fixture_rejects_invalid_table_name(self):
        request = self.factory.post(
            '/pybirdai/api/ancrdt/sql-fixtures/save/',
            data=json.dumps({
                'table_name': '/tmp/evil',
                'fixture_name': 'fixture_one',
                'content': '-- fixture',
            }),
            content_type='application/json',
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch(
                'pybirdai.views.workflow.ancrdt.sql_fixture_editor_views.SQL_FIXTURES_DIR',
                tmpdir,
            ):
                response = save_sql_fixture(request)

        self.assertEqual(response.status_code, 400)

    def test_derivation_config_requires_csrf_token(self):
        client = Client(enforce_csrf_checks=True)
        response = client.post(
            '/pybirdai/api/derivations/save/',
            data=json.dumps({
                'selections': [
                    {
                        'class_name': 'INSTRMNT',
                        'field_name': 'CRRNT_LTV_RT',
                        'enabled': True,
                    }
                ]
            }),
            content_type='application/json',
            secure=True,
        )

        self.assertEqual(response.status_code, 403)

    def test_execute_data_point_escapes_html_in_response(self):
        request = self.factory.get('/pybirdai/execute-data-point/test/')

        with patch(
            'pybirdai.views.core.process_execution_views.RunExecuteDataPoint'
        ) as mock_runner:
            mock_runner.return_value.run_execute_data_point.return_value = '<script>alert("xss")</script>'
            response = execute_data_point(request, '<b>dp-1</b>')

        self.assertEqual(response.status_code, 200)
        content = response.content.decode('utf-8')
        self.assertIn('&lt;b&gt;dp-1&lt;/b&gt;', content)
        self.assertIn('&lt;script&gt;alert(&quot;xss&quot;)&lt;/script&gt;', content)
        self.assertNotIn('<script>alert("xss")</script>', content)

    def test_execute_ancrdt_table_hides_internal_exception_details(self):
        request = self.factory.get('/pybirdai/execute-ancrdt-table/ANCRDT_TEST/?format=json')

        with patch(
            'pybirdai.views.workflow.ancrdt.table_views.RunANCRDTTable'
        ) as mock_runner:
            mock_runner.return_value.run_execute_ancrdt_table.side_effect = RuntimeError(
                'secret db path /tmp/private.sqlite3'
            )
            response = execute_ancrdt_table(request, 'ANCRDT_TEST')

        self.assertEqual(response.status_code, 500)
        payload = json.loads(response.content)
        self.assertFalse(payload['success'])
        self.assertNotIn('secret db path', payload['error'])

    def test_visualization_service_escapes_generated_html(self):
        json_data = {
            'nodes': [
                {
                    'name': '<script>alert(1)</script>',
                    'items': [{'code': 'SRC"}<img src=x onerror=1>'}],
                    'is_source': True,
                },
                {
                    'name': 'Target',
                    'items': [{'code': 'TGT'}],
                    'is_source': False,
                },
            ],
            'edges': [
                {
                    'source': '<script>alert(1)</script>',
                    'target': 'Target',
                    'sourceItem': 'SRC"}<img src=x onerror=1>',
                    'targetItem': 'TGT',
                }
            ],
        }

        with patch('pybirdai.views.core.visualisation_service.os.makedirs'), patch(
            'builtins.open',
            mock_open(),
        ):
            html_output = NetworkGraphGenerationService.create_graph(
                json_data,
                file_name='<img src=x onerror=1>.html',
            )

        self.assertIn('&lt;img src=x onerror=1&gt;', html_output)
        self.assertNotIn('<img src=x onerror=1>', html_output)
        self.assertNotIn('<script>alert(1)</script>', html_output)

    def test_save_sql_fixture_hides_internal_exception_details(self):
        request = self.factory.post(
            '/pybirdai/api/ancrdt/sql-fixtures/save/',
            data=json.dumps({
                'table_name': 'ANCRDT_TEST',
                'fixture_name': 'fixture_one',
                'content': '-- fixture',
            }),
            content_type='application/json',
        )

        with patch(
            'pybirdai.views.workflow.ancrdt.sql_fixture_editor_views._resolve_fixture_path',
            side_effect=RuntimeError('secret filesystem path /tmp/private.sql'),
        ):
            response = save_sql_fixture(request)

        self.assertEqual(response.status_code, 500)
        payload = json.loads(response.content)
        self.assertFalse(payload['success'])
        self.assertNotIn('secret filesystem path', payload['error'])

    def test_ancrdt_graph_api_hides_internal_exception_details(self):
        request = self.factory.get('/pybirdai/api/ancrdt/tables/graph/')

        with patch(
            'pybirdai.api.ancrdt_tables_graph_api._load_join_configurations',
            side_effect=RuntimeError('secret joins config path'),
        ):
            response = get_ancrdt_tables_graph(request)

        self.assertEqual(response.status_code, 500)
        payload = json.loads(response.content)
        self.assertEqual(payload['error'], 'Failed to load ANCRDT tables graph data')
        self.assertNotIn('secret joins config path', payload['message'])

    def test_download_ancrdt_csv_executes_directly_without_loopback_request(self):
        request = self.factory.get(
            '/pybirdai/download-ancrdt-csv/ANCRDT_TEST/?format=json&PRPS=7',
            HTTP_HOST='evil.example.com',
        )

        with patch('requests.get', side_effect=AssertionError('loopback HTTP should not be used')), patch(
            'pybirdai.process_steps.ancrdt_transformation.execute_ancrdt_table.ExecuteANCRDTTable.execute_table',
            return_value={'rows': [{'code': 'A1', 'value': 7}]},
        ) as mock_execute:
            response = download_ancrdt_csv(request, 'ANCRDT_TEST')

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response['Content-Type'], 'text/csv')
        self.assertIn('attachment; filename="ANCRDT_TEST_export.csv"', response['Content-Disposition'])
        self.assertIn('code,value', response.content.decode('utf-8'))
        mock_execute.assert_called_once_with(table_name='ANCRDT_TEST', filters={'PRPS': '7'})

    def test_save_filter_code_file_hides_internal_exception_details(self):
        request = self.factory.post(
            '/pybirdai/filter-code/save/',
            data=json.dumps({
                'file_name': 'safe_filter.py',
                'content': 'print("ok")',
            }),
            content_type='application/json',
        )

        with patch(
            'pybirdai.views.execution_code_editor_views._get_source_file_path',
            side_effect=RuntimeError('secret filesystem path /tmp/private.py'),
        ):
            response = save_filter_code_file(request)

        self.assertEqual(response.status_code, 500)
        payload = json.loads(response.content)
        self.assertFalse(payload['success'])
        self.assertNotIn('secret filesystem path', payload['error'])

    def test_run_migrations_in_subprocess_does_not_make_db_world_writable(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch(
                'pybirdai.process_steps.database_setup.automode_orchestrator.os.path.exists',
                side_effect=lambda path: path == 'db.sqlite3',
            ), patch(
                'pybirdai.process_steps.database_setup.automode_orchestrator.os.remove',
            ), patch(
                'pybirdai.process_steps.database_setup.automode_orchestrator.os.chmod',
            ) as mock_chmod, patch(
                'pybirdai.process_steps.database_setup.automode_orchestrator._get_python_executable',
                return_value='python',
            ), patch(
                'pybirdai.process_steps.database_setup.migration_generator.AdvancedMigrationGenerator',
            ) as mock_generator_class, patch(
                'pybirdai.process_steps.database_setup.automode_orchestrator.subprocess.run',
                return_value=SimpleNamespace(returncode=1, stderr='boom', stdout=''),
            ):
                mock_generator = mock_generator_class.return_value
                mock_generator.parse_files.return_value = []

                with self.assertRaises(RuntimeError):
                    _run_migrations_in_subprocess(tmpdir)

        mock_chmod.assert_not_called()

    def test_sync_file_to_production_hides_internal_sync_message_details(self):
        request = self.factory.post(
            '/pybirdai/execution-code-editing/sync/',
            data=json.dumps({
                'file_name': 'ANCRDT_TEST_logic.py',
                'create_backup': True,
            }),
            content_type='application/json',
        )

        with patch(
            'pybirdai.views.execution_code_editor_views.CodeSyncManager.sync_file',
            return_value={
                'success': False,
                'message': 'Failed to sync file: /tmp/private/filter_code.py',
                'timestamp': '2026-04-23T00:00:00',
                'filename': 'ANCRDT_TEST_logic.py',
            },
        ):
            response = sync_file_to_production(request)

        self.assertEqual(response.status_code, 400)
        payload = json.loads(response.content)
        self.assertEqual(payload['error'], 'File synchronization failed. Please try again later.')
        self.assertNotIn('/tmp/private', payload['error'])

    def test_get_file_diff_hides_internal_comparison_details(self):
        request = self.factory.get('/pybirdai/execution-code-editing/diff/ANCRDT_TEST_logic.py/')

        with patch(
            'pybirdai.views.execution_code_editor_views.CodeSyncManager.get_diff_summary',
            return_value={
                'filename': 'ANCRDT_TEST_logic.py',
                'error': 'secret filesystem path /tmp/private/filter_code.py',
            },
        ):
            response = get_file_diff(request, 'ANCRDT_TEST_logic.py')

        self.assertEqual(response.status_code, 500)
        payload = json.loads(response.content)
        self.assertFalse(payload['success'])
        self.assertEqual(payload['error'], 'Unable to compare file contents.')
        self.assertNotIn('/tmp/private', payload['error'])

    def test_code_sync_manager_get_diff_summary_hides_internal_exception_details(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = CodeSyncManager(base_dir=tmpdir)
            staging_file = Path(tmpdir) / 'results' / 'generated_python_joins' / 'safe.py'
            production_file = Path(tmpdir) / 'pybirdai' / 'process_steps' / 'filter_code' / 'safe.py'
            staging_file.write_text('print(\"staging\")\n', encoding='utf-8')
            production_file.write_text('print(\"prod\")\n', encoding='utf-8')

            with patch('builtins.open', side_effect=RuntimeError('secret diff path /tmp/private.py')):
                diff_summary = manager.get_diff_summary('safe.py')

        self.assertEqual(diff_summary, {
            'filename': 'safe.py',
            'comparison_failed': True,
        })

    def test_api_ancrdt_cubes_hides_internal_exception_details(self):
        request = self.factory.get('/pybirdai/api/ancrdt/cubes/')

        with patch(
            'pybirdai.views.workflow.ancrdt.workflow_views.CUBE.objects.filter',
            side_effect=RuntimeError('secret cube query path'),
        ):
            response = api_ancrdt_cubes(request)

        self.assertEqual(response.status_code, 500)
        payload = json.loads(response.content)
        self.assertNotIn('secret cube query path', payload['error'])

    def test_api_ancrdt_cube_structure_hides_internal_exception_details(self):
        request = self.factory.get('/pybirdai/api/ancrdt/cube-structure/CUBE_1/')

        with patch(
            'pybirdai.views.workflow.ancrdt.workflow_views.get_object_or_404',
            side_effect=RuntimeError('secret structure query path'),
        ):
            response = api_ancrdt_cube_structure(request, 'CUBE_1')

        self.assertEqual(response.status_code, 500)
        payload = json.loads(response.content)
        self.assertNotIn('secret structure query path', payload['error'])

    def test_github_integration_service_rejects_invalid_owner_without_network_call(self):
        service = GitHubIntegrationService(github_token='token')

        with patch('pybirdai.api.workflow_api.requests.get') as mock_get:
            success = service.create_branch('../evil', 'safe-repo', 'feature-safe')

        self.assertFalse(success)
        mock_get.assert_not_called()

    def test_save_derivation_file_hides_internal_manager_error_details(self):
        request = self.factory.post(
            '/pybirdai/api/derivations/file/save/',
            data=json.dumps({
                'path': 'generated_from_member_links/safe.py',
                'content': 'print("ok")',
            }),
            content_type='application/json',
        )

        with patch(
            'pybirdai.views.workflow.derivation_sync.DerivationSyncManager.save_file',
            return_value={
                'success': False,
                'error': 'secret filesystem path /tmp/private_derivation.py',
            },
        ):
            response = save_derivation_file(request)

        self.assertEqual(response.status_code, 400)
        payload = json.loads(response.content)
        self.assertEqual(payload['error'], 'Unable to save derivation file. Please try again later.')
        self.assertNotIn('/tmp/private_derivation.py', payload['error'])

    def test_derivation_sync_status_omits_full_paths(self):
        request = self.factory.get('/pybirdai/api/derivations/files/status/')

        with patch(
            'pybirdai.views.workflow.derivation_sync.DerivationSyncManager.get_all_derivation_files',
            return_value=[{
                'filename': 'safe.py',
                'relative_path': 'generated_from_member_links/safe.py',
                'full_path': '/tmp/private/generated_from_member_links/safe.py',
                'type': 'cube_link',
                'status': 'staging_modified',
                'is_modified': True,
                'mtime': '2026-04-23T00:00:00',
                'size': 123,
            }],
        ), patch(
            'pybirdai.views.workflow.derivation_sync.DerivationSyncManager.get_sync_status_summary',
            return_value={'total': 1},
        ), patch(
            'pybirdai.views.workflow.derivation_sync.is_cube_link_allowed',
            return_value=True,
        ):
            response = get_derivation_files_sync_status(request)

        self.assertEqual(response.status_code, 200)
        payload = json.loads(response.content)
        self.assertEqual(payload['files'][0]['filename'], 'safe.py')
        self.assertNotIn('full_path', payload['files'][0])

    def test_get_derivation_file_content_omits_full_path_metadata(self):
        request = self.factory.get('/pybirdai/api/derivations/file/content/?path=generated_from_member_links/safe.py')

        with patch(
            'pybirdai.views.workflow.derivation_sync.DerivationSyncManager.read_file',
            return_value='print("ok")',
        ), patch(
            'pybirdai.views.workflow.derivation_sync.DerivationSyncManager.get_file_info',
            return_value={
                'filename': 'safe.py',
                'relative_path': 'generated_from_member_links/safe.py',
                'full_path': '/tmp/private/generated_from_member_links/safe.py',
                'type': 'cube_link',
                'status': 'staging_modified',
                'is_modified': True,
                'mtime': '2026-04-23T00:00:00',
                'size': 123,
            },
        ):
            response = get_derivation_file_content(request)

        self.assertEqual(response.status_code, 200)
        payload = json.loads(response.content)
        self.assertEqual(payload['content'], 'print("ok")')
        self.assertNotIn('full_path', payload['file_info'])

    def test_load_csv_hides_internal_exception_details(self):
        request = self.factory.post(
            '/pybirdai/joins-configuration/load/',
            data=json.dumps({
                'file_type': 'in_scope_reports',
                'framework': 'FINREP_REF',
            }),
            content_type='application/json',
        )

        with patch(
            'pybirdai.views.joins_configuration_views.JoinsConfigurationManager.get_file_path',
            side_effect=RuntimeError('secret filesystem path /tmp/private.csv'),
        ):
            response = load_csv(request)

        self.assertEqual(response.status_code, 500)
        payload = json.loads(response.content)
        self.assertFalse(payload['success'])
        self.assertNotIn('/tmp/private.csv', payload['error'])

    def test_automode_configure_get_omits_github_token(self):
        request = self.factory.get('/pybirdai/automode/configure/')

        with patch.dict(
            'sys.modules',
            {'pybirdai.views.forms': SimpleNamespace(AutomodeConfigurationSessionForm=object)},
        ), patch(
            'pybirdai.views.core.automode_views._load_temp_config',
            return_value={
                'data_model_type': 'ELDM',
                'technical_export_source': 'GITHUB',
                'technical_export_github_url': 'https://github.com/regcommunity/FreeBIRD',
                'config_files_source': 'GITHUB',
                'config_files_github_url': 'https://github.com/regcommunity/FreeBIRD',
                'when_to_stop': 'RESOURCE_DOWNLOAD',
                'github_token': 'secret-token',
            },
        ):
            response = automode_configure(request)

        self.assertEqual(response.status_code, 200)
        payload = json.loads(response.content)
        self.assertTrue(payload['success'])
        self.assertNotIn('github_token', payload['config'])

    def test_automode_continue_post_restart_hides_internal_result_errors(self):
        request = self.factory.post('/pybirdai/automode/continue-post-restart/')

        with patch(
            'pybirdai.views.core.automode_views._load_temp_config',
            return_value={
                'data_model_type': 'ELDM',
                'technical_export_source': 'GITHUB',
                'technical_export_github_url': 'https://github.com/regcommunity/FreeBIRD',
                'config_files_source': 'GITHUB',
                'config_files_github_url': 'https://github.com/regcommunity/FreeBIRD',
                'test_suite_source': 'GITHUB',
                'test_suite_github_url': 'https://github.com/regcommunity/FreeBIRD',
                'when_to_stop': 'SMCUBES_RULES',
            },
        ), patch(
            'pybirdai.api.workflow_api.AutomodeConfigurationService.execute_automode_post_restart',
            return_value={
                'setup_completed': False,
                'errors': ['secret filesystem path /tmp/private.sqlite3'],
                'smcubes_rules': {
                    'joins_creation': False,
                    'errors': ['nested secret path /tmp/private_rules.py'],
                },
            },
        ):
            response = automode_continue_post_restart(request)

        self.assertEqual(response.status_code, 200)
        payload = json.loads(response.content)
        self.assertFalse(payload['success'])
        self.assertEqual(
            payload['error'],
            'Post-restart execution completed with errors. Please review the logs and try again later.',
        )
        self.assertEqual(payload['results']['error_count'], 1)
        self.assertEqual(payload['results']['smcubes_rules']['error_count'], 1)
        self.assertNotIn('errors', payload['results'])
        self.assertNotIn('errors', payload['results']['smcubes_rules'])
        self.assertNotIn('/tmp/private.sqlite3', json.dumps(payload))
        self.assertNotIn('/tmp/private_rules.py', json.dumps(payload))

    def test_automode_debug_config_hides_internal_exception_details(self):
        request = self.factory.get('/pybirdai/automode/debug-config/')

        with patch(
            'pybirdai.views.core.automode_views._get_temp_config_path',
            side_effect=RuntimeError('secret config path /tmp/private.json'),
        ):
            response = automode_debug_config(request)

        self.assertEqual(response.status_code, 500)
        payload = json.loads(response.content)
        self.assertFalse(payload['success'])
        self.assertNotIn('/tmp/private.json', payload['error'])

    def test_get_member_links_json_hides_internal_exception_details(self):
        request = self.factory.get('/pybirdai/member-links/json/')

        with patch(
            'pybirdai.views.member_link_views.MEMBER_LINK.objects.all',
            side_effect=RuntimeError('secret member link query path /tmp/private.sqlite3'),
        ):
            response = get_member_links_json(request)

        self.assertEqual(response.status_code, 500)
        payload = json.loads(response.content)
        self.assertEqual(payload['status'], 'error')
        self.assertNotIn('/tmp/private.sqlite3', payload['message'])

    def test_task1_substep_hides_internal_exception_details(self):
        request = self.factory.post('/pybirdai/workflow/task/1/substep/delete_database/')
        task_execution = SimpleNamespace(
            execution_data={},
            status='running',
            completed_at=None,
            save=lambda: None,
        )

        with patch(
            'pybirdai.entry_points.delete_bird_metadata_database.RunDeleteBirdMetadataDatabase'
        ) as mock_delete:
            mock_delete.return_value.run_delete_bird_metadata_database.side_effect = RuntimeError(
                'secret workflow deletion path /tmp/private.db'
            )
            response = _execute_task1_substep(
                request,
                'delete_database',
                task_execution,
                SimpleNamespace(),
            )

        self.assertEqual(response.status_code, 500)
        payload = json.loads(response.content)
        self.assertFalse(payload['success'])
        self.assertNotIn('/tmp/private.db', payload['message'])

    def test_ancrdt_import_hides_internal_exception_details(self):
        request = self.factory.get('/pybirdai/ancrdt/import/?execute=true')

        with patch(
            'pybirdai.views.workflow.ancrdt.transformation_views.RunANCRDTTransformation.run_step_1_import',
            side_effect=RuntimeError('secret ANCRDT import path /tmp/private.csv'),
        ):
            response = ancrdt_import(request)

        self.assertEqual(response.status_code, 500)
        payload = json.loads(response.content)
        self.assertEqual(payload['status'], 'error')
        self.assertNotIn('/tmp/private.csv', payload['message'])

    def test_export_bird_excel_template_hides_internal_exception_details(self):
        request = self.factory.get('/pybirdai/test-data-template/export/')

        with patch(
            'pybirdai.utils.datapoint_test_run.test_data_template_utils.get_bird_model_classes',
            side_effect=RuntimeError('secret template path /tmp/private_model.py'),
        ):
            response = export_bird_excel_template(request)

        self.assertEqual(response.status_code, 500)
        content = response.content.decode('utf-8')
        self.assertNotIn('/tmp/private_model.py', content)

    def test_convert_sql_to_csv_hides_internal_exception_details(self):
        request = self.factory.post(
            '/pybirdai/test-data-template/convert-sql-to-csv/',
            data=json.dumps({'scenario_path': '/tmp/scenario'}),
            content_type='application/json',
        )

        with patch(
            'pybirdai.utils.datapoint_test_run.sql_to_csv_converter.SQLToCSVConverter.convert_scenario_in_place',
            side_effect=RuntimeError('secret scenario path /tmp/private_scenario'),
        ):
            response = convert_sql_to_csv(request)

        self.assertEqual(response.status_code, 500)
        payload = json.loads(response.content)
        self.assertNotIn('/tmp/private_scenario', payload['error'])

    def test_workflow_run_migrations_hides_internal_exception_details(self):
        request = self.factory.post('/pybirdai/workflow/run-migrations/')

        with patch(
            'pybirdai.views.workflow.setup.threading.Thread',
            side_effect=RuntimeError('secret migration thread path /tmp/private.sock'),
        ):
            response = workflow_run_migrations(request)

        self.assertEqual(response.status_code, 500)
        payload = json.loads(response.content)
        self.assertFalse(payload['success'])
        self.assertNotIn('/tmp/private.sock', payload['message'])

    def test_workflow_session_check_hides_internal_exception_details(self):
        request = self.factory.get('/pybirdai/workflow/session-check/')
        request.session = self._DummySession({'workflow_session_id': 'wf-123'})

        with patch(
            'pybirdai.views.workflow.session.WorkflowSession.objects.get',
            side_effect=RuntimeError('secret workflow lookup path /tmp/private.sqlite3'),
        ):
            response = workflow_session_check(request)

        self.assertEqual(response.status_code, 500)
        payload = json.loads(response.content)
        self.assertFalse(payload['success'])
        self.assertNotIn('/tmp/private.sqlite3', payload['error'])

    def test_execute_ancrdt_step_hides_internal_exception_details(self):
        request = self.factory.post('/pybirdai/workflow/ancrdt/step/0/')
        request.session = self._DummySession({'workflow_session_id': 'wf-123'})

        execution_record = SimpleNamespace(
            status='pending',
            error_message='',
            start_execution=lambda: None,
            complete_execution=lambda data: None,
            refresh_from_db=lambda: None,
            save=lambda **kwargs: None,
        )

        def _handle_error(message):
            execution_record.error_message = message

        execution_record.handle_error = _handle_error

        with patch(
            'pybirdai.views.workflow.ancrdt.execution.get_object_or_404',
            return_value=SimpleNamespace(session_id='wf-123'),
        ), patch(
            'pybirdai.views.workflow.ancrdt.execution.AnaCreditProcessExecution.objects.get_or_create',
            return_value=(execution_record, False),
        ), patch(
            'pybirdai.entry_points.ancrdt_transformation.RunANCRDTTransformation.run_step_0_fetch_ancrdt_csv',
            side_effect=RuntimeError('secret ancrdt execution path /tmp/ancrdt.csv'),
        ):
            response = execute_ancrdt_step(request, 0)

        self.assertEqual(response.status_code, 500)
        payload = json.loads(response.content)
        self.assertFalse(payload['success'])
        self.assertNotIn('/tmp/ancrdt.csv', payload['error'])
        self.assertNotIn('/tmp/ancrdt.csv', execution_record.error_message)

    def test_task2_smcubes_rules_hides_internal_exception_details(self):
        request = self.factory.post(
            '/pybirdai/workflow/task/2/do/',
            data={},
            HTTP_X_REQUESTED_WITH='XMLHttpRequest',
        )

        task_execution = SimpleNamespace(
            status='pending',
            started_at=None,
            completed_at=None,
            execution_data={},
            error_message='',
            save=lambda: None,
        )

        with patch(
            'pybirdai.entry_points.create_filters.RunCreateFilters.run_create_filters',
            side_effect=RuntimeError('secret filter generation path /tmp/filters.py'),
        ):
            response = task2_smcubes_rules(
                request,
                'do',
                task_execution,
                SimpleNamespace(),
            )

        self.assertEqual(response.status_code, 500)
        payload = json.loads(response.content)
        self.assertFalse(payload['success'])
        self.assertNotIn('/tmp/filters.py', payload['message'])
        self.assertNotIn('/tmp/filters.py', task_execution.error_message)

    def test_add_cube_link_ajax_hides_internal_exception_details(self):
        request = self.factory.post(
            '/pybirdai/joins-metadata/add-cube-link/',
            data={
                'primary_cube_id': 'PRIMARY',
                'foreign_cube_id': 'FOREIGN',
                'join_identifier': 'JOIN_1',
            },
        )

        with patch(
            'pybirdai.views.joins_metadata_embed_views.CUBE.objects.get',
            side_effect=RuntimeError('secret cube lookup path /tmp/cubes.sqlite3'),
        ):
            response = add_cube_link_ajax(request)

        self.assertEqual(response.status_code, 500)
        payload = json.loads(response.content)
        self.assertEqual(payload['status'], 'error')
        self.assertNotIn('/tmp/cubes.sqlite3', payload['message'])

    def test_export_database_to_github_hides_internal_exception_details(self):
        request = self.factory.post(
            '/pybirdai/workflow/export-database-to-github/',
            data={
                'github_token': 'ghp_test_token',
                'repository_url': 'https://github.com/example/repo',
            },
        )

        with patch(
            'pybirdai.views.core.export_db._export_database_to_csv_enhanced',
            side_effect=RuntimeError('secret github export path /tmp/export.zip'),
        ):
            response = export_database_to_github(request)

        self.assertEqual(response.status_code, 500)
        payload = json.loads(response.content)
        self.assertFalse(payload['success'])
        self.assertNotIn('/tmp/export.zip', payload['error'])

    def test_trigger_server_restart_hides_internal_exception_details(self):
        request = self.factory.post('/pybirdai/workflow/trigger-restart/')

        with patch(
            'pybirdai.views.workflow.async_operations.threading.Thread',
            side_effect=RuntimeError('secret restart thread path /tmp/restart.sock'),
        ):
            response = trigger_server_restart(request)

        self.assertEqual(response.status_code, 500)
        payload = json.loads(response.content)
        self.assertNotIn('/tmp/restart.sock', payload['error'])

    def test_get_enhanced_lineage_hides_internal_exception_details(self):
        request = self.factory.get('/pybirdai/api/enhanced-lineage/1/')

        with patch(
            'pybirdai.api.enhanced_lineage_api_v2.get_object_or_404',
            return_value=SimpleNamespace(id=1, name='Trail One', created_at=SimpleNamespace(isoformat=lambda: '2026-01-01T00:00:00'), execution_context={}, metadata_trail=None),
        ), patch(
            'pybirdai.api.enhanced_lineage_api_v2.process_database_tables',
            side_effect=RuntimeError('secret lineage database path /tmp/lineage.sqlite3'),
        ):
            response = get_enhanced_lineage(request, 1)

        self.assertEqual(response.status_code, 500)
        payload = json.loads(response.content)
        self.assertEqual(payload['error'], 'Enhanced lineage extraction failed')
        self.assertNotIn('/tmp/lineage.sqlite3', payload['message'])

    def test_get_domain_members_hides_internal_exception_details(self):
        request = self.factory.get('/pybirdai/semantic-integration/domain-members/VAR1/')

        with patch(
            'pybirdai.views.core.semantic_integration_views.VARIABLE.objects.get',
            side_effect=RuntimeError('secret variable lookup path /tmp/semantic.sqlite3'),
        ):
            response = get_domain_members(request, 'VAR1')

        self.assertEqual(response.status_code, 500)
        payload = json.loads(response.content)
        self.assertEqual(payload['status'], 'error')
        self.assertNotIn('/tmp/semantic.sqlite3', payload['message'])

    def test_view_csv_file_hides_csv_parser_exception_details(self):
        request = self.factory.get('/pybirdai/lineage/view/test.csv/')

        with tempfile.TemporaryDirectory() as tmpdir:
            lineage_dir = Path(tmpdir) / 'results' / 'lineage_output'
            lineage_dir.mkdir(parents=True)
            (lineage_dir / 'test.csv').write_text('id,name\n1,Alice\n', encoding='utf-8')

            with self.settings(BASE_DIR=tmpdir), patch(
                'pybirdai.views.core.csv_views.csv.DictReader',
                side_effect=csv.Error('secret csv parser path /tmp/parser-state'),
            ):
                response = view_csv_file(request, 'test.csv')

        self.assertEqual(response.status_code, 400)
        self.assertNotIn('/tmp/parser-state', response.content.decode('utf-8'))

    def test_import_mapping_from_csv_hides_internal_exception_details(self):
        request = self.factory.post(
            '/pybirdai/import-mapping-from-csv/',
            data={
                'mapping_name': 'Demo Mapping',
                'mapping_code': 'DEMO_MAP',
                'mapping_type': 'DIRECT',
                'algorithm': 'algo',
                'parsed_data': json.dumps({'rows': []}),
            },
        )

        with patch(
            'pybirdai.entry_points.template_mapping_definition.RunImportMappingData.run_import_mapping_data',
            side_effect=RuntimeError('secret mapping import path /tmp/mapping.sqlite3'),
        ):
            response = import_mapping_from_csv(request)

        self.assertEqual(response.status_code, 500)
        payload = json.loads(response.content)
        self.assertEqual(payload['status'], 'error')
        self.assertNotIn('/tmp/mapping.sqlite3', payload['message'])
