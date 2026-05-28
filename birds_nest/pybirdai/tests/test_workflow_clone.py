import json
import time
import tempfile
from unittest.mock import Mock, patch

from django.test import RequestFactory, SimpleTestCase

from pybirdai.views.workflow.session import (
    _clone_generated_artefact_summary,
    _clone_task_execution_data,
    _count_csv_data_rows,
    _run_clone_test_suite,
    workflow_clone_import,
)
from pybirdai.views.workflow.status import (
    _clone_import_status,
    _reset_clone_import_status,
    workflow_clone_import_status,
)


class WorkflowCloneTests(SimpleTestCase):
    def setUp(self):
        self.factory = RequestFactory()
        _reset_clone_import_status()

    def tearDown(self):
        _reset_clone_import_status()

    def test_clone_import_endpoint_starts_background_thread(self):
        request = self.factory.post(
            '/pybirdai/workflow/clone-import/',
            HTTP_X_REQUESTED_WITH='XMLHttpRequest',
        )

        with patch('pybirdai.views.workflow.session.threading.Thread') as thread_cls:
            response = workflow_clone_import(request)

        payload = json.loads(response.content)

        self.assertEqual(response.status_code, 200)
        self.assertTrue(payload['success'])
        self.assertEqual(payload['status'], 'started')
        self.assertEqual(payload['check_status_url'], '/pybirdai/workflow/clone-import-status/')
        self.assertTrue(_clone_import_status['running'])
        self.assertEqual(_clone_import_status['current_step'], 'starting')
        thread_cls.assert_called_once()
        self.assertEqual(thread_cls.call_args.kwargs['target'].__name__, '_run_clone_import_async')
        self.assertTrue(thread_cls.call_args.kwargs['daemon'])
        thread_cls.return_value.start.assert_called_once_with()

    def test_clone_import_endpoint_reuses_running_status(self):
        _clone_import_status['running'] = True
        request = self.factory.post(
            '/pybirdai/workflow/clone-import/',
            HTTP_X_REQUESTED_WITH='XMLHttpRequest',
        )

        with patch('pybirdai.views.workflow.session.threading.Thread') as thread_cls:
            response = workflow_clone_import(request)

        payload = json.loads(response.content)

        self.assertEqual(response.status_code, 200)
        self.assertFalse(payload['success'])
        self.assertEqual(payload['status'], 'already_running')
        self.assertEqual(payload['check_status_url'], '/pybirdai/workflow/clone-import-status/')
        thread_cls.assert_not_called()

    def test_clone_import_status_reports_elapsed_time(self):
        _clone_import_status.update({
            'running': True,
            'completed': False,
            'success': False,
            'message': 'Running clone import...',
            'started_at': time.time() - 7,
            'current_step': 'importing_metadata',
            'completed_steps': ['Read CSV files'],
        })

        response = workflow_clone_import_status(
            self.factory.get('/pybirdai/workflow/clone-import-status/')
        )
        payload = json.loads(response.content)
        status = payload['clone_import_status']

        self.assertTrue(payload['success'])
        self.assertTrue(status['running'])
        self.assertGreaterEqual(status['elapsed_time'], 6)
        self.assertEqual(status['current_step'], 'importing_metadata')
        self.assertEqual(status['completed_steps'], ['Read CSV files'])

    def test_clone_execution_data_matches_task_completion_shapes(self):
        import_summary = {
            'successful_imports': 3,
            'total_files': 3,
            'total_objects': 42,
        }
        test_suite_results = {
            'tests_executed': True,
            'suites_run': ['bird-default-test-suite'],
            'test_results': {
                'bird-default-test-suite': {
                    'status': 'completed',
                    'config_file': 'tests/bird-default-test-suite/configuration_file_tests.json',
                }
            },
            'errors': [],
        }
        expected_bool_counts = {
            1: 5,
            2: 2,
            3: 2,
            4: 1,
        }

        for task_number, expected_bool_count in expected_bool_counts.items():
            execution_data = _clone_task_execution_data(
                task_number,
                import_summary,
                test_suite_results,
            )

            self.assertEqual(execution_data['source'], 'clone_import')
            self.assertEqual(execution_data['mode'], 'clone')
            self.assertEqual(execution_data['import_summary'], import_summary)
            self.assertEqual(
                sum(1 for value in execution_data.values() if isinstance(value, bool)),
                expected_bool_count,
            )

            if task_number == 4:
                self.assertTrue(execution_data['tests_executed'])
                self.assertEqual(execution_data['test_suites'], ['bird-default-test-suite'])
                self.assertEqual(execution_data['test_suite_results'], test_suite_results)

    def test_clone_task4_records_unexecuted_test_suite(self):
        execution_data = _clone_task_execution_data(
            4,
            {'successful_imports': 3},
            {
                'tests_executed': False,
                'suites_run': [],
                'test_results': {},
                'errors': ['No test suites found'],
            },
        )

        self.assertFalse(execution_data['tests_executed'])
        self.assertEqual(execution_data['test_errors'], ['No test suites found'])
        self.assertIn('did not complete', execution_data['steps_completed'][-1])

    def test_clone_test_suite_uses_automode_runner(self):
        expected_results = {
            'tests_executed': True,
            'suites_run': ['bird-default-test-suite'],
            'errors': [],
        }
        service = Mock()
        service._run_tests_suite.return_value = expected_results

        with patch(
            'pybirdai.views.workflow.session.AutomodeConfigurationService',
            return_value=service,
        ):
            results = _run_clone_test_suite()

        self.assertEqual(results, expected_results)
        service._run_tests_suite.assert_called_once_with()

    def test_clone_importer_loads_metadata_model_module(self):
        from pybirdai.utils.clone_mode.import_from_metadata_export import CSVDataImporter

        with tempfile.TemporaryDirectory() as tmpdir:
            importer = CSVDataImporter(results_dir=tmpdir)

        self.assertIn('pybirdai_cube', importer.model_map)
        self.assertEqual(
            importer.column_mappings['pybirdai_cube_link'][11],
            'cube_link_type',
        )

    def test_bulk_import_rows_skip_export_id_for_auto_pk_table(self):
        from pybirdai.models.bird_meta_data_model import CELL_POSITION
        from pybirdai.utils.clone_mode.import_from_metadata_export import CSVDataImporter

        with tempfile.TemporaryDirectory() as tmpdir:
            importer = CSVDataImporter(results_dir=tmpdir)

        headers = ['ID', 'CELL_ID', 'AXIS_ORDINATE_ID']
        rows = [['1', '127152_REF', 'FINREP_REF_F_00_01_REF_FINREP_1_010']]

        django_headers, sqlite_rows = importer._build_bulk_sqlite_import_rows(
            headers,
            rows,
            CELL_POSITION,
            'pybirdai_cell_position',
        )

        self.assertEqual(django_headers, ['id', 'cell_id_id', 'axis_ordinate_id_id'])
        self.assertEqual(sqlite_rows, [[1, '127152_REF', 'FINREP_REF_F_00_01_REF_FINREP_1_010']])

    def test_clone_id_mapping_does_not_hash_unsaved_auto_pk_models(self):
        from pybirdai.models.bird_meta_data_model import MEMBER_MAPPING_ITEM
        from pybirdai.utils.clone_mode.import_from_metadata_export import CSVDataImporter

        with tempfile.TemporaryDirectory() as tmpdir:
            importer = CSVDataImporter(results_dir=tmpdir)

        first_obj = MEMBER_MAPPING_ITEM()
        second_obj = MEMBER_MAPPING_ITEM()
        old_id_to_row_data = {
            52795: {'position': 0},
            52796: {'position': 1},
        }
        id_to_object_map = {}

        importer._store_bulk_id_mappings(
            'pybirdai_member_mapping_item',
            [first_obj, second_obj],
            old_id_to_row_data,
            id_to_object_map,
        )

        self.assertIs(id_to_object_map[52795], first_obj)
        self.assertIs(id_to_object_map[52796], second_obj)
        self.assertIs(importer.id_mappings['pybirdai_member_mapping_item'][52795], first_obj)

    def test_clone_clear_order_covers_all_metadata_tables_once(self):
        from pybirdai.utils.clone_mode.import_from_metadata_export import CSVDataImporter

        with tempfile.TemporaryDirectory() as tmpdir:
            importer = CSVDataImporter(results_dir=tmpdir)

        clear_order = importer._get_clear_table_order()

        self.assertEqual(len(clear_order), len(set(clear_order)))
        self.assertEqual(set(clear_order), set(importer.model_map))
        self.assertLess(
            clear_order.index('pybirdai_cell_position'),
            clear_order.index('pybirdai_table_cell'),
        )
        self.assertLess(
            clear_order.index('pybirdai_cube_to_combination'),
            clear_order.index('pybirdai_cube'),
        )

    def test_clone_generated_artefact_summary_blocks_empty_required_links(self):
        csv_data = {
            'cube_link.csv': 'MAINTENANCE_AGENCY_ID,CUBE_LINK_ID',
            'cube_structure_item_link.csv': 'CUBE_STRUCTURE_ITEM_LINK_ID,CUBE_LINK_ID\nlink1,cube1\n',
            'member_link.csv': 'ID,CUBE_STRUCTURE_ITEM_LINK_ID\n',
        }

        row_counts, blocking_files = _clone_generated_artefact_summary(csv_data)

        self.assertEqual(_count_csv_data_rows(csv_data['cube_link.csv']), 0)
        self.assertEqual(row_counts['cube_structure_item_link.csv'], 1)
        self.assertEqual(blocking_files, ['cube_link.csv'])

    def test_clone_exporter_loads_metadata_model_module(self):
        from pybirdai.utils.clone_mode.export_with_ids import clean_whitespace

        self.assertEqual(clean_whitespace('one\n two'), 'one two')
