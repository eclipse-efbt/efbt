import os
import json
import tempfile
from pathlib import Path
from unittest.mock import patch

from django.test import SimpleTestCase

from pybirdai.api.workflow_api import AutomodeConfigurationService
from pybirdai.api.clone_repo_service import CloneRepoService
from pybirdai.process_steps.database_setup.automode_orchestrator import (
    _create_setup_ready_marker,
    _prepare_derivation_files,
    is_setup_ready,
)


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
                service.context.ldm_or_il = "il"
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

    def test_eldm_cached_artefacts_require_arcs_csv(self):
        repo_url = "https://github.com/regcommunity/FreeBIRD_67"
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
            for file_name in (
                "DM_AVT.csv",
                "DM_Attributes.csv",
                "DM_Classification_Types.csv",
                "DM_Domain_AVT.csv",
                "DM_Domains.csv",
                "DM_Entities.csv",
                "DM_Logical_To_Native.csv",
                "DM_Mappings.csv",
                "DM_Relations.csv",
            ):
                self._write_file(base_dir / "resources" / "ldm" / file_name)

            original_cwd = os.getcwd()
            os.chdir(base_dir)
            try:
                with self.settings(BASE_DIR=str(base_dir)):
                    service = AutomodeConfigurationService()
                    service.context.ldm_or_il = "ldm"
                    self.assertFalse(service._bird_content_fetch_is_current(repo_url, branch))

                    self._write_file(base_dir / "resources" / "ldm" / "arcs.csv")
                    self.assertTrue(service._bird_content_fetch_is_current(repo_url, branch))
            finally:
                os.chdir(original_cwd)

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

    def test_clone_repo_service_removes_stale_filtered_artefact_files(self):
        with tempfile.TemporaryDirectory() as base_dir:
            base_path = Path(base_dir)
            repo_root = base_path / "FreeBIRD_67"
            extracted_root = repo_root / "FreeBIRD_67-main"
            self._write_file(
                base_path / "artefacts" / "derivation_files" / "derivation_config.csv",
                "stale",
            )
            self._write_file(
                extracted_root
                / "artefacts"
                / "derivation_files"
                / "manually_generated"
                / "manual_derivations.py",
                "# current repo file",
            )

            with self.settings(BASE_DIR=base_dir):
                CloneRepoService().setup_files("FreeBIRD_67")

            self.assertFalse(
                (base_path / "artefacts" / "derivation_files" / "derivation_config.csv").exists()
            )
            self.assertTrue(
                (
                    base_path
                    / "resources"
                    / "derivation_files"
                    / "manually_generated"
                    / "manual_derivations.py"
                ).exists()
            )

    def test_setup_ready_marker_is_stale_when_inputs_change(self):
        with tempfile.TemporaryDirectory() as base_dir:
            base_path = Path(base_dir)
            derivation_config = base_path / "artefacts" / "derivation_files" / "derivation_config.csv"
            self._write_file(derivation_config, "stale")

            _create_setup_ready_marker(base_path)
            self.assertTrue(is_setup_ready(base_path))

            derivation_config.unlink()
            self.assertFalse(is_setup_ready(base_path))

    def test_old_setup_ready_marker_without_fingerprint_is_stale(self):
        with tempfile.TemporaryDirectory() as base_dir:
            base_path = Path(base_dir)
            self._write_file(
                base_path / ".setup_ready_marker",
                json.dumps({"step": 2, "status": "complete"}),
            )

            self.assertFalse(is_setup_ready(base_path))

    def test_eldm_derivation_setup_removes_eil_logical_derivation_config(self):
        with tempfile.TemporaryDirectory() as base_dir:
            base_path = Path(base_dir)
            self._write_file(
                base_path / "resources" / "derivation_files" / "derivation_config.csv",
                "class_name,field_name,enabled,notes\nINSTRMNT_RL,GRSS_CRRYNG_AMNT,true,\n",
            )
            self._write_file(
                base_path
                / "resources"
                / "derivation_files"
                / "generated_from_logical_transformation_rules"
                / "INSTRMNT_RL_GRSS_CRRYNG_AMNT_derived.py",
                "# stale EIL derivation",
            )

            _prepare_derivation_files(base_path, {"data_model_type": "ELDM"})

            self.assertFalse(
                (base_path / "resources" / "derivation_files" / "derivation_config.csv").exists()
            )
            self.assertFalse(
                (
                    base_path
                    / "resources"
                    / "derivation_files"
                    / "generated_from_logical_transformation_rules"
                    / "INSTRMNT_RL_GRSS_CRRYNG_AMNT_derived.py"
                ).exists()
            )
            self.assertTrue(
                (
                    base_path
                    / "resources"
                    / "derivation_files"
                    / "generated_from_logical_transformation_rules"
                    / "tmp"
                ).exists()
            )

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

    def test_fetch_test_suite_removes_previously_downloaded_suites(self):
        repo_url = "https://github.com/example/current-suite"
        branch = "main"
        repo_name = "current-suite"

        with tempfile.TemporaryDirectory() as tmpdir:
            base_dir = Path(tmpdir)
            extracted_root = base_dir / repo_name / f"{repo_name}-{branch}"
            self._write_file(base_dir / "tests" / "__init__.py", "")
            self._write_file(base_dir / "tests" / "old-eil-suite" / "suite_manifest.json", "{}")
            self._write_file(
                base_dir / "tests" / "old-ldm-suite" / "configuration_file_tests.json",
                "{}",
            )
            self._write_file(extracted_root / "suite_manifest.json", "{}")
            self._write_file(extracted_root / "configuration_file_tests.json", "{}")
            self._write_file(
                extracted_root / "tests" / "code" / "test_current.py",
                "# current suite",
            )

            with self.settings(BASE_DIR=str(base_dir)):
                service = AutomodeConfigurationService()
                with patch(
                    "pybirdai.api.clone_repo_service.CloneRepoService.clone_repo",
                    return_value=True,
                ):
                    result = service._fetch_test_suite_from_github(
                        repo_url,
                        force_refresh=True,
                        branch=branch,
                    )

            self.assertEqual(result, 1)
            self.assertTrue((base_dir / "tests" / "__init__.py").exists())
            self.assertFalse((base_dir / "tests" / "old-eil-suite").exists())
            self.assertFalse((base_dir / "tests" / "old-ldm-suite").exists())
            self.assertTrue((base_dir / "tests" / repo_name / "suite_manifest.json").exists())
            self.assertTrue(
                (base_dir / "tests" / repo_name / "tests" / "code" / "test_current.py").exists()
            )
            self.assertFalse((base_dir / repo_name).exists())

    def test_fetch_test_suite_keeps_existing_suites_when_clone_fails(self):
        repo_url = "https://github.com/example/current-suite"
        branch = "main"

        with tempfile.TemporaryDirectory() as tmpdir:
            base_dir = Path(tmpdir)
            self._write_file(base_dir / "tests" / "__init__.py", "")
            self._write_file(base_dir / "tests" / "old-suite" / "suite_manifest.json", "{}")

            with self.settings(BASE_DIR=str(base_dir)):
                service = AutomodeConfigurationService()
                with patch(
                    "pybirdai.api.clone_repo_service.CloneRepoService.clone_repo",
                    return_value=False,
                ):
                    with self.assertRaises(RuntimeError):
                        service._fetch_test_suite_from_github(
                            repo_url,
                            force_refresh=True,
                            branch=branch,
                        )

            self.assertTrue((base_dir / "tests" / "old-suite" / "suite_manifest.json").exists())

    def test_database_setup_retrieves_github_artefacts_with_force_refresh(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            base_dir = Path(tmpdir)
            self._write_file(
                base_dir / "automode_config.json",
                json.dumps(
                    {
                        "data_model_type": "ELDM",
                        "technical_export_source": "GITHUB",
                        "technical_export_github_url": "https://github.com/regcommunity/FreeBIRD_67",
                        "test_suite_source": "MANUAL",
                        "when_to_stop": "DATABASE_CREATION",
                    }
                ),
            )

            with self.settings(BASE_DIR=str(base_dir)):
                from pybirdai.views.workflow import async_operations

                with patch(
                    "pybirdai.api.workflow_api.AutomodeConfigurationService"
                ) as service_cls, patch(
                    "pybirdai.entry_points.database_setup.RunApplicationSetup"
                ) as setup_cls:
                    service = service_cls.return_value
                    service.fetch_files_from_source.return_value = {"technical_export": 1}
                    setup_cls.return_value.run_automode_setup.return_value = {
                        "message": "done"
                    }

                    async_operations._run_database_setup_async()

            call_kwargs = service.fetch_files_from_source.call_args.kwargs
            self.assertIs(call_kwargs["force_refresh"], True)
