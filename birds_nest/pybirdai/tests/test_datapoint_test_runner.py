import tempfile
from pathlib import Path

from django.test import SimpleTestCase

from pybirdai.utils.datapoint_test_run.run_tests import RegulatoryTemplateTestRunner


class DatapointTestRunnerGeneratedTestTests(SimpleTestCase):
    def write_generated_test(self, directory, value):
        test_path = Path(directory) / "test_cell_example.py"
        test_path.write_text(
            "def test_execute_datapoint(value: int=%s):\n"
            "    assert str(value)\n" % value,
            encoding="utf-8",
        )
        return test_path

    def test_reads_expected_value_from_generated_datapoint_test(self):
        with tempfile.TemporaryDirectory() as directory:
            test_path = self.write_generated_test(directory, 83491261)

            self.assertEqual(
                RegulatoryTemplateTestRunner.read_generated_datapoint_expected_value(str(test_path)),
                "83491261",
            )

    def test_regenerates_generated_test_when_expected_value_is_stale(self):
        runner = RegulatoryTemplateTestRunner(parser_=False)

        with tempfile.TemporaryDirectory() as directory:
            test_path = self.write_generated_test(directory, 83491261)

            self.assertTrue(runner.should_regenerate_test_file(str(test_path), "83491250"))
            self.assertFalse(runner.should_regenerate_test_file(str(test_path), "83491261"))
