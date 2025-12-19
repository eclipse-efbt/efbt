# coding=UTF-8
# Copyright (c) 2025 Bird Software Solutions Ltd
# Pytest fixtures for framework isolation tests

import pytest
import os
import sys
import json
from pathlib import Path

# Add the project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

# Configure Django before imports
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'birds_nest.settings')

import django
django.setup()

from django.test import Client, TransactionTestCase
from pybirdai.context.sdd_context_django import SDDContext


@pytest.fixture
def test_client():
    """Provide a Django test client."""
    return Client()


@pytest.fixture
def clean_sdd_context():
    """
    Provide a clean SDDContext with reset class-level dictionaries.
    This ensures no state leakage between tests.
    """
    # Store original class-level state
    original_state = {}
    for attr in dir(SDDContext):
        if not attr.startswith('_') and isinstance(getattr(SDDContext, attr, None), dict):
            original_state[attr] = getattr(SDDContext, attr, {}).copy()

    # Reset all class-level dictionaries
    dict_attrs = [
        'mapping_definition_dictionary', 'bird_cube_dictionary',
        'bird_cube_structure_dictionary', 'bird_cube_structure_item_dictionary',
        'framework_dictionary', 'domain_dictionary', 'member_dictionary',
        'variable_dictionary', 'cube_link_dictionary',
        'cube_structure_item_links_dictionary', 'member_hierarchy_dictionary',
        'member_hierarchy_node_dictionary', 'report_tables_dictionary'
    ]
    for attr in dict_attrs:
        if hasattr(SDDContext, attr):
            setattr(SDDContext, attr, {})

    # Create fresh instance
    context = SDDContext()

    yield context

    # Restore original state after test
    for key, value in original_state.items():
        setattr(SDDContext, key, value)


@pytest.fixture
def temp_results_dir(tmp_path):
    """
    Create a temporary results directory structure for test isolation.
    """
    results_dir = tmp_path / "results"
    results_dir.mkdir()

    # Create subdirectories that match production structure
    (results_dir / "technical_export").mkdir()
    (results_dir / "generated_python_filters").mkdir()
    (results_dir / "generated_python_joins").mkdir()
    (results_dir / "ancrdt_csv").mkdir()
    (results_dir / "clone_export").mkdir()

    yield results_dir


@pytest.fixture
def main_flow_config():
    """Configuration for MAIN/FINREP workflow testing."""
    return {
        "data_model_type": "EIL",
        "clone_mode": "false",
        "technical_export_source": "GITHUB",
        "technical_export_github_url": "https://github.com/regcommunity/FreeBIRD_IL_66",
        "config_files_source": "GITHUB",
        "config_files_github_url": "https://github.com/regcommunity/FreeBIRD_IL_66",
        "test_suite_source": "GITHUB",
        "test_suite_github_url": "https://github.com/BIRD-Software-Solutions/bird-default-test-suite",
        "github_branch": "main",
        "when_to_stop": "RESOURCE_DOWNLOAD",
        "enable_lineage_tracking": False,
        "framework": "FINREP"
    }


@pytest.fixture
def anacredit_flow_config():
    """Configuration for AnaCredit workflow testing."""
    return {
        "data_model_type": "ANCRDT",
        "clone_mode": "false",
        "technical_export_source": "ECB",
        "framework": "ANCRDT",
        "when_to_stop": "FULL_EXECUTION",
        "enable_lineage_tracking": False
    }


@pytest.fixture
def database_state_manager():
    """
    Capture and restore database state for isolation testing.
    Returns a manager that can snapshot and compare DB state.
    """
    class DatabaseStateManager:
        def __init__(self):
            self.snapshots = {}

        def take_snapshot(self, name):
            """Take a snapshot of current database state."""
            from pybirdai.models.bird_meta_data_model import (
                CUBE, CUBE_STRUCTURE, CUBE_STRUCTURE_ITEM,
                CUBE_LINK, MEMBER, VARIABLE, DOMAIN, FRAMEWORK
            )

            self.snapshots[name] = {
                'cube_count': CUBE.objects.count(),
                'cube_structure_count': CUBE_STRUCTURE.objects.count(),
                'cube_link_count': CUBE_LINK.objects.count(),
                'member_count': MEMBER.objects.count(),
                'variable_count': VARIABLE.objects.count(),
                'domain_count': DOMAIN.objects.count(),
                'framework_count': FRAMEWORK.objects.count(),
                'cube_ids': list(CUBE.objects.values_list('cube_id', flat=True)),
                'framework_ids': list(FRAMEWORK.objects.values_list('framework_id', flat=True)),
            }
            return self.snapshots[name]

        def compare_snapshots(self, name1, name2):
            """Compare two snapshots and return differences."""
            snap1 = self.snapshots.get(name1, {})
            snap2 = self.snapshots.get(name2, {})

            differences = {}
            for key in snap1:
                if snap1[key] != snap2.get(key):
                    differences[key] = {
                        'before': snap1[key],
                        'after': snap2.get(key)
                    }
            return differences

        def get_snapshot(self, name):
            return self.snapshots.get(name)

    return DatabaseStateManager()


class FrameworkIsolationTestCase(TransactionTestCase):
    """
    Base test case for framework isolation tests.
    Provides common setup/teardown and utility methods.
    """
    reset_sequences = True

    def setUp(self):
        """Set up test environment."""
        super().setUp()
        self.client = Client()
        self.original_sdd_state = self._capture_sdd_state()

    def tearDown(self):
        """Clean up after test."""
        self._restore_sdd_state(self.original_sdd_state)
        super().tearDown()

    def _capture_sdd_state(self):
        """Capture current SDDContext class-level state."""
        return {
            attr: getattr(SDDContext, attr, {}).copy()
            if isinstance(getattr(SDDContext, attr, None), dict) else getattr(SDDContext, attr, None)
            for attr in dir(SDDContext)
            if not attr.startswith('_') and isinstance(getattr(SDDContext, attr, None), dict)
        }

    def _restore_sdd_state(self, state):
        """Restore SDDContext class-level state."""
        for attr, value in state.items():
            if value is not None:
                setattr(SDDContext, attr, value)

    def assertFrameworkIsolation(self, framework1_data, framework2_data):
        """
        Assert that two framework datasets don't overlap.
        """
        framework1_cubes = set(framework1_data.get('cube_ids', []))
        framework2_cubes = set(framework2_data.get('cube_ids', []))
        overlap = framework1_cubes & framework2_cubes

        self.assertEqual(
            len(overlap), 0,
            f"Framework isolation violated: shared cube IDs: {overlap}"
        )
