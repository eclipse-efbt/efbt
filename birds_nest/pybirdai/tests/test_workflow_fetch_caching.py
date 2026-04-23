import os
import json
import tempfile
from pathlib import Path
from unittest.mock import patch

from django.test import SimpleTestCase

from pybirdai.api.workflow_api import AutomodeConfigurationService
from pybirdai.api.clone_repo_service import CloneRepoService


class WorkflowFetchCachingTests(SimpleTestCase):
    def _write_file(self, path: Path, contents: str = "ok"):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(contents, encoding="utf-8")

    def _write_state(self, base_dir: Path, state_name: str, payload: dict):
        state_path = base_dir / ".workflow_fetch_state" / f"{state_name}.json"
        self._write_file(state_path, json.dumps(payload))

    def test_fetch_from_github_skips_clone_when_cached_artefacts_match(self):
        repo_url = "https://github.com/regcommunity/FreeBIRD_EIL_67"
        branch = "main"

        with tempfile.TemporaryDirectory() as tmpdir:
            base_dir = Path(tmpdir)
            self._write_state(
                base_dir,
                "bird_content",
                {
                    "github_url": repo_url,
                    "branch": branch,
                },
            )
            self._write_file(base_dir / "artefacts" / "smcubes_artefacts" / "technical_export.csv")
            self._write_file(base_dir / "artefacts" / "smcubes_artefacts" / "logical_transformation_rule.csv")
            self._write_file(base_dir / "artefacts" / "joins_configuration" / "joins.csv")
            self._write_file(base_dir / "resources" / "il" / "model.csv")
            self._write_file(base_dir / "resources" / "derivation_files" / "derivation_config.csv")

            with self.settings(BASE_DIR=str(base_dir)):
                service = AutomodeConfigurationService()
                with patch(
                    "pybirdai.api.clone_repo_service.CloneRepoService",
                    side_effect=AssertionError("clone should be skipped when artefacts are cached"),
                ):
                    result = service._fetch_from_github(
                        repo_url,
                        force_refresh=False,
                        branch=branch,
                    )

            self.assertEqual(result, 1)

    def test_clone_repo_service_anchors_target_directories_to_base_dir(self):
        with tempfile.TemporaryDirectory() as base_dir, tempfile.TemporaryDirectory() as cwd:
            original_cwd = os.getcwd()
            os.chdir(cwd)
            try:
                with self.settings(BASE_DIR=base_dir):
                    CloneRepoService()

                self.assertTrue((Path(base_dir) / "resources" / "admin").exists())
                self.assertFalse((Path(cwd) / "resources" / "admin").exists())
            finally:
                os.chdir(original_cwd)

    def test_fetch_test_suite_skips_clone_when_cached_suite_matches(self):
        repo_url = "https://github.com/regcommunity/bird-default-test-suite-eil-67"
        branch = "main"
        repo_name = "bird-default-test-suite-eil-67"

        with tempfile.TemporaryDirectory() as tmpdir:
            base_dir = Path(tmpdir)
            self._write_state(
                base_dir,
                "test_suite",
                {
                    "github_url": repo_url,
                    "branch": branch,
                },
            )
            self._write_file(base_dir / "tests" / repo_name / "smoke_test.json")

            with self.settings(BASE_DIR=str(base_dir)):
                service = AutomodeConfigurationService()
                with patch(
                    "pybirdai.api.clone_repo_service.CloneRepoService",
                    side_effect=AssertionError("clone should be skipped when the test suite is cached"),
                ):
                    result = service._fetch_test_suite_from_github(
                        repo_url,
                        force_refresh=False,
                        branch=branch,
                    )

            self.assertEqual(result, 1)
