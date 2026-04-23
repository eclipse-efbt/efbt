import json
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import mock_open, patch

from django.test import Client, RequestFactory, SimpleTestCase

from pybirdai.api.ancrdt_tables_graph_api import get_ancrdt_tables_graph
from pybirdai.api.workflow_api import GitHubIntegrationService
from pybirdai.process_steps.database_setup.automode_orchestrator import (
    _run_migrations_in_subprocess,
)
from pybirdai.views.core.process_execution_views import execute_data_point
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
from pybirdai.views.workflow.ancrdt.table_views import execute_ancrdt_table
from pybirdai.views.workflow.ancrdt.workflow_views import (
    api_ancrdt_cube_structure,
    api_ancrdt_cubes,
    download_ancrdt_csv,
)
from pybirdai.process_steps.joins_configuration.joins_configuration_manager import (
    JoinsConfigurationManager,
)
from pybirdai.views.workflow.ancrdt.sql_fixture_editor_views import save_sql_fixture
from pybirdai.views.workflow.code_sync import CodeSyncManager


class SecurityHardeningTests(SimpleTestCase):
    def setUp(self):
        self.factory = RequestFactory()

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
