# coding=UTF-8
# Copyright (c) 2025 Arfa Digital Consulting
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDX-License-Identifier: EPL-2.0
#
# Contributors:
#    Benjamin Arfa - initial API and implementation
#
"""
Clone Mode Test Suite

This module provides automated tests for the clone mode save/load functionality.
Tests cover the Main, DPM, and ANCRDT workflows.

Usage:
    python -m pytest pybirdai/tests/clone_mode/test_clone_mode.py -v
    python -m pytest pybirdai/tests/clone_mode/test_clone_mode.py::TestDPMWorkflow -v
"""

import os
import sys
import json
import shutil
import tempfile
import subprocess
from pathlib import Path

import pytest

# Add the birds_nest directory to the path
# Path: birds_nest/pybirdai/tests/clone_mode/test_clone_mode.py
BIRDS_NEST_DIR = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(BIRDS_NEST_DIR))

# Set up Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'birds_nest.settings')

import django
django.setup()

from django.core.management import call_command
from django.conf import settings


class CloneModeTestBase:
    """Base class for clone mode tests with common utilities."""

    @classmethod
    def setup_class(cls):
        """Set up test class."""
        cls.export_dir = os.path.join(settings.BASE_DIR, 'results', 'clone_export_test')
        cls.db_path = os.path.join(settings.BASE_DIR, 'db.sqlite3')
        cls.db_backup_path = os.path.join(settings.BASE_DIR, 'db.sqlite3.test_backup')

    def backup_database(self):
        """Backup the current database."""
        if os.path.exists(self.db_path):
            shutil.copy2(self.db_path, self.db_backup_path)

    def restore_database(self):
        """Restore the database from backup."""
        if os.path.exists(self.db_backup_path):
            shutil.copy2(self.db_backup_path, self.db_path)
            os.remove(self.db_backup_path)

    def clear_database(self):
        """Clear the database and run migrations."""
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        call_command('migrate', '--run-syncdb', verbosity=0)

    def get_model_counts(self):
        """Get counts of all relevant models."""
        from pybirdai.models.bird_meta_data_model import (
            MAINTENANCE_AGENCY, FRAMEWORK, DOMAIN, VARIABLE, MEMBER,
            TABLE, AXIS, AXIS_ORDINATE, ORDINATE_ITEM, TABLE_CELL,
            CELL_POSITION, MEMBER_HIERARCHY, MEMBER_HIERARCHY_NODE,
            CUBE, CUBE_STRUCTURE, CUBE_STRUCTURE_ITEM
        )

        return {
            'MAINTENANCE_AGENCY': MAINTENANCE_AGENCY.objects.count(),
            'FRAMEWORK': FRAMEWORK.objects.count(),
            'DOMAIN': DOMAIN.objects.count(),
            'VARIABLE': VARIABLE.objects.count(),
            'MEMBER': MEMBER.objects.count(),
            'TABLE': TABLE.objects.count(),
            'AXIS': AXIS.objects.count(),
            'AXIS_ORDINATE': AXIS_ORDINATE.objects.count(),
            'ORDINATE_ITEM': ORDINATE_ITEM.objects.count(),
            'TABLE_CELL': TABLE_CELL.objects.count(),
            'CELL_POSITION': CELL_POSITION.objects.count(),
            'MEMBER_HIERARCHY': MEMBER_HIERARCHY.objects.count(),
            'MEMBER_HIERARCHY_NODE': MEMBER_HIERARCHY_NODE.objects.count(),
            'CUBE': CUBE.objects.count(),
            'CUBE_STRUCTURE': CUBE_STRUCTURE.objects.count(),
            'CUBE_STRUCTURE_ITEM': CUBE_STRUCTURE_ITEM.objects.count(),
        }

    def save_clone_state(self, output_dir=None, force=True):
        """Save clone state to a directory."""
        if output_dir is None:
            output_dir = self.export_dir

        args = ['--local-only', '--output-dir', output_dir]
        if force:
            args.append('--force')

        call_command('save_clone_state', *args)

        # Return the database_export subdirectory
        return os.path.join(output_dir, 'database_export')

    def load_clone_state(self, source_dir, force=True, skip_cleanup=True):
        """Load clone state from a directory."""
        args = ['--local-path', source_dir]
        if force:
            args.append('--force')
        if skip_cleanup:
            args.append('--skip-cleanup')

        call_command('load_clone_state', *args)

    def verify_clone_integrity(self, original_counts, restored_counts):
        """Verify that restored counts match original counts."""
        mismatches = []
        for model, original in original_counts.items():
            restored = restored_counts.get(model, 0)
            if original != restored:
                mismatches.append(f"{model}: {original} -> {restored}")

        if mismatches:
            pytest.fail(f"Data integrity check failed:\n" + "\n".join(mismatches))

        return True

    def cleanup(self):
        """Clean up test artifacts."""
        if os.path.exists(self.export_dir):
            shutil.rmtree(self.export_dir)
        if os.path.exists(self.db_backup_path):
            os.remove(self.db_backup_path)


class TestCloneModeBasic(CloneModeTestBase):
    """Basic clone mode functionality tests."""

    def test_save_creates_export_files(self):
        """Test that save_clone_state creates the expected files."""
        self.backup_database()
        try:
            export_path = self.save_clone_state()

            # Check that export directory exists
            assert os.path.exists(export_path), "Export directory not created"

            # Check for process_metadata.json
            metadata_path = os.path.join(export_path, 'process_metadata.json')
            assert os.path.exists(metadata_path), "process_metadata.json not created"

            # Check that metadata is valid JSON
            with open(metadata_path) as f:
                metadata = json.load(f)
            assert 'version' in metadata, "metadata missing version"
            assert 'workflows' in metadata, "metadata missing workflows"

            # Check for CSV files
            csv_files = [f for f in os.listdir(export_path) if f.endswith('.csv')]
            assert len(csv_files) > 0, "No CSV files created"

        finally:
            self.restore_database()
            self.cleanup()

    def test_save_load_round_trip(self):
        """Test that save followed by load preserves data."""
        self.backup_database()
        try:
            # Get original counts
            original_counts = self.get_model_counts()

            # Skip if database is empty
            if sum(original_counts.values()) == 0:
                pytest.skip("Database is empty, skipping round-trip test")

            # Save clone state
            export_path = self.save_clone_state()

            # Clear database
            self.clear_database()

            # Verify database is empty
            empty_counts = self.get_model_counts()
            assert sum(empty_counts.values()) == 0, "Database not properly cleared"

            # Load clone state
            self.load_clone_state(export_path)

            # Verify restored counts
            restored_counts = self.get_model_counts()
            self.verify_clone_integrity(original_counts, restored_counts)

        finally:
            self.restore_database()
            self.cleanup()


class TestDPMWorkflow(CloneModeTestBase):
    """Tests for DPM workflow clone mode functionality."""

    def run_dpm_step1(self, frameworks=None):
        """Run DPM Step 1: Extract DPM Metadata."""
        if frameworks is None:
            frameworks = ['FINREP']

        from pybirdai.entry_points.import_dpm_data import RunImportDPMData
        RunImportDPMData.run_import_phase_a(frameworks=frameworks)

    def run_dpm_step2(self):
        """Run DPM Step 2: Process & Import Tables."""
        from pybirdai.entry_points.import_dpm_data import RunImportDPMData
        RunImportDPMData.run_import_phase_b()

    def run_dpm_step3(self):
        """Run DPM Step 3: Create Output Layers."""
        from pybirdai.entry_points.dpm_output_layer_creation import run_output_layer_creation
        run_output_layer_creation()

    @pytest.mark.slow
    def test_dpm_step1_clone(self):
        """Test clone mode after DPM Step 1."""
        self.backup_database()
        try:
            # Clear and set up fresh database
            self.clear_database()
            call_command('complete_automode_setup', verbosity=0)

            # Run DPM Step 1
            self.run_dpm_step1()

            # Get counts after step 1
            original_counts = self.get_model_counts()

            # Save clone state
            export_path = self.save_clone_state()

            # Clear database
            self.clear_database()

            # Load clone state
            self.load_clone_state(export_path)

            # Verify
            restored_counts = self.get_model_counts()
            self.verify_clone_integrity(original_counts, restored_counts)

        finally:
            self.restore_database()
            self.cleanup()

    @pytest.mark.slow
    def test_dpm_step2_clone(self):
        """Test clone mode after DPM Step 2."""
        self.backup_database()
        try:
            # Clear and set up fresh database
            self.clear_database()
            call_command('complete_automode_setup', verbosity=0)

            # Run DPM Steps 1 and 2
            self.run_dpm_step1()
            self.run_dpm_step2()

            # Get counts after step 2
            original_counts = self.get_model_counts()

            # Verify we have data
            assert original_counts['TABLE'] > 0, "No tables after DPM Step 2"
            assert original_counts['AXIS_ORDINATE'] > 0, "No ordinates after DPM Step 2"

            # Save clone state
            export_path = self.save_clone_state()

            # Clear database
            self.clear_database()

            # Load clone state
            self.load_clone_state(export_path)

            # Verify
            restored_counts = self.get_model_counts()
            self.verify_clone_integrity(original_counts, restored_counts)

        finally:
            self.restore_database()
            self.cleanup()

    @pytest.mark.slow
    def test_dpm_step3_clone(self):
        """Test clone mode after DPM Step 3 (Output Layers)."""
        self.backup_database()
        try:
            # Clear and set up fresh database
            self.clear_database()
            call_command('complete_automode_setup', verbosity=0)

            # Run DPM Steps 1, 2, and 3
            self.run_dpm_step1()
            self.run_dpm_step2()
            self.run_dpm_step3()

            # Get counts after step 3
            original_counts = self.get_model_counts()

            # Verify we have output layer data
            assert original_counts['CUBE'] > 0, "No cubes after DPM Step 3"

            # Save clone state
            export_path = self.save_clone_state()

            # Clear database
            self.clear_database()

            # Load clone state
            self.load_clone_state(export_path)

            # Verify
            restored_counts = self.get_model_counts()
            self.verify_clone_integrity(original_counts, restored_counts)

        finally:
            self.restore_database()
            self.cleanup()


class TestMainWorkflow(CloneModeTestBase):
    """Tests for Main workflow clone mode functionality."""

    @pytest.mark.slow
    def test_main_step1_clone(self):
        """Test clone mode after Main workflow Step 1 (SMCubes Core Creation)."""
        self.backup_database()
        try:
            # Clear and set up fresh database
            self.clear_database()
            call_command('complete_automode_setup', verbosity=0)

            # Get counts after step 1
            original_counts = self.get_model_counts()

            # Save clone state
            export_path = self.save_clone_state()

            # Clear database
            self.clear_database()

            # Load clone state
            self.load_clone_state(export_path)

            # Verify
            restored_counts = self.get_model_counts()
            self.verify_clone_integrity(original_counts, restored_counts)

        finally:
            self.restore_database()
            self.cleanup()


class TestANCRDTWorkflow(CloneModeTestBase):
    """Tests for ANCRDT workflow clone mode functionality."""

    @pytest.mark.slow
    def test_ancrdt_step0_clone(self):
        """Test clone mode after ANCRDT Step 0 (Initial Setup)."""
        self.backup_database()
        try:
            # Clear and set up fresh database
            self.clear_database()
            call_command('complete_automode_setup', verbosity=0)

            # Get counts after setup
            original_counts = self.get_model_counts()

            # Save clone state
            export_path = self.save_clone_state()

            # Clear database
            self.clear_database()

            # Load clone state
            self.load_clone_state(export_path)

            # Verify
            restored_counts = self.get_model_counts()
            self.verify_clone_integrity(original_counts, restored_counts)

        finally:
            self.restore_database()
            self.cleanup()


# Pytest configuration
def pytest_configure(config):
    """Configure pytest markers."""
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')"
    )


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
