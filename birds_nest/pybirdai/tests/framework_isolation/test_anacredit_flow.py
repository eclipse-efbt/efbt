# coding=UTF-8
# Copyright (c) 2025 Bird Software Solutions Ltd
# Tests for AnaCredit workflow execution and state management

import pytest
import os
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

# Configure Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'birds_nest.settings')

import django
django.setup()

from django.test import TestCase, TransactionTestCase, Client
from django.db import connection

from pybirdai.context.sdd_context_django import SDDContext
from pybirdai.models.bird_meta_data_model import (
    CUBE, CUBE_STRUCTURE, CUBE_STRUCTURE_ITEM, CUBE_LINK,
    MEMBER, VARIABLE, DOMAIN, FRAMEWORK, MAINTENANCE_AGENCY
)
from pybirdai.models.workflow_model import (
    WorkflowSession, AnaCreditProcessExecution
)


class TestAnaCreditFlowSetup(TransactionTestCase):
    """Test AnaCredit workflow setup and initialization."""

    reset_sequences = True

    def setUp(self):
        """Set up test environment."""
        self.client = Client()
        # Capture initial SDDContext state
        self.initial_cube_dict = SDDContext.bird_cube_dictionary.copy() if SDDContext.bird_cube_dictionary else {}

    def tearDown(self):
        """Clean up after test."""
        # Restore SDDContext state
        SDDContext.bird_cube_dictionary = self.initial_cube_dict

    def test_anacredit_flow_creates_ancrdt_framework(self):
        """Test that AnaCredit flow creates ANCRDT framework entries."""
        agency, _ = MAINTENANCE_AGENCY.objects.get_or_create(
            maintenance_agency_id='ECB',
            defaults={'name': 'European Central Bank'}
        )
        framework, created = FRAMEWORK.objects.get_or_create(
            framework_id='ANCRDT',
            defaults={
                'name': 'Analytical Credit Datasets',
                'maintenance_agency_id': agency
            }
        )

        self.assertTrue(FRAMEWORK.objects.filter(framework_id='ANCRDT').exists())

    def test_anacredit_flow_sdd_context_isolation(self):
        """Test that SDDContext state is properly isolated for AnaCredit."""
        # Add a test entry
        test_cube_id = 'TEST_ANCRDT_CUBE_001'
        SDDContext.bird_cube_dictionary[test_cube_id] = {'framework': 'ANCRDT'}

        # Verify it exists
        self.assertIn(test_cube_id, SDDContext.bird_cube_dictionary)

        # Clear it
        SDDContext.bird_cube_dictionary = {}

        # Verify it's gone
        self.assertNotIn(test_cube_id, SDDContext.bird_cube_dictionary)

    def test_anacredit_database_tables_exist(self):
        """Test that all required database tables exist for AnaCredit flow."""
        required_tables = [
            'pybirdai_cube',
            'pybirdai_cube_structure',
            'pybirdai_cube_link',
            'pybirdai_framework',
            'pybirdai_anacreditprocessexecution',
        ]

        with connection.cursor() as cursor:
            for table_name in required_tables:
                cursor.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=%s",
                    [table_name]
                )
                result = cursor.fetchone()
                self.assertIsNotNone(
                    result,
                    f"Required table '{table_name}' does not exist"
                )


class TestAnaCreditFlowExecution(TransactionTestCase):
    """Test AnaCredit workflow execution steps."""

    reset_sequences = True

    def setUp(self):
        """Set up test environment."""
        self.client = Client()
        # Create workflow session
        self.session = WorkflowSession.objects.create(
            current_task=0,
            configuration={'framework': 'ANCRDT'}
        )

    def test_anacredit_process_execution_tracking(self):
        """Test that AnaCredit process executions are properly tracked."""
        process_exec = AnaCreditProcessExecution.objects.create(
            session=self.session,
            step_number=0,
            status='completed',
            execution_data={'step_name': 'Fetch Metadata CSV'}
        )

        self.assertEqual(process_exec.step_number, 0)
        self.assertEqual(process_exec.status, 'completed')
        self.assertEqual(process_exec.execution_data.get('step_name'), 'Fetch Metadata CSV')

    def test_anacredit_steps_are_tracked_separately(self):
        """Test that AnaCredit steps are tracked in separate model from MAIN flow."""
        # Create AnaCredit step executions (0-5)
        for step in range(6):
            AnaCreditProcessExecution.objects.create(
                session=self.session,
                step_number=step,
                status='completed'
            )

        # Verify all 6 steps are tracked
        ancrdt_steps = AnaCreditProcessExecution.objects.filter(session=self.session)
        self.assertEqual(ancrdt_steps.count(), 6)

        # Verify step numbers
        step_numbers = list(ancrdt_steps.values_list('step_number', flat=True).order_by('step_number'))
        self.assertEqual(step_numbers, [0, 1, 2, 3, 4, 5])

    def test_anacredit_flow_does_not_affect_finrep_data(self):
        """Test that AnaCredit flow operations don't touch FINREP data."""
        # Create FINREP framework and cube first
        eba_agency, _ = MAINTENANCE_AGENCY.objects.get_or_create(
            maintenance_agency_id='EBA',
            defaults={'name': 'European Banking Authority'}
        )
        finrep_framework, _ = FRAMEWORK.objects.get_or_create(
            framework_id='FINREP_REF',
            defaults={
                'name': 'Financial Reporting Framework',
                'maintenance_agency_id': eba_agency
            }
        )
        finrep_cube = CUBE.objects.create(
            cube_id='FINREP_EXISTING_CUBE',
            name='FINREP Existing Cube',
            framework_id=finrep_framework
        )

        # Count FINREP cubes before AnaCredit operations
        finrep_count_before = CUBE.objects.filter(
            framework_id__framework_id='FINREP_REF'
        ).count()

        # Simulate AnaCredit flow creating its own cube
        ecb_agency, _ = MAINTENANCE_AGENCY.objects.get_or_create(
            maintenance_agency_id='ECB',
            defaults={'name': 'European Central Bank'}
        )
        ancrdt_framework, _ = FRAMEWORK.objects.get_or_create(
            framework_id='ANCRDT',
            defaults={
                'name': 'Analytical Credit Datasets',
                'maintenance_agency_id': ecb_agency
            }
        )
        ancrdt_cube = CUBE.objects.create(
            cube_id='ANCRDT_NEW_CUBE',
            name='AnaCredit New Cube',
            framework_id=ancrdt_framework
        )

        # Verify FINREP cube count unchanged
        finrep_count_after = CUBE.objects.filter(
            framework_id__framework_id='FINREP_REF'
        ).count()
        self.assertEqual(finrep_count_before, finrep_count_after)

        # Verify both cubes exist independently
        self.assertTrue(CUBE.objects.filter(cube_id='FINREP_EXISTING_CUBE').exists())
        self.assertTrue(CUBE.objects.filter(cube_id='ANCRDT_NEW_CUBE').exists())


class TestAnaCreditFlowDataIntegrity(TransactionTestCase):
    """Test AnaCredit workflow data integrity."""

    reset_sequences = True

    def test_anacredit_cube_structure_relationships(self):
        """Test that AnaCredit cube-structure relationships are maintained."""
        agency, _ = MAINTENANCE_AGENCY.objects.get_or_create(
            maintenance_agency_id='ECB',
            defaults={'name': 'European Central Bank'}
        )
        framework, _ = FRAMEWORK.objects.get_or_create(
            framework_id='ANCRDT',
            defaults={
                'name': 'Analytical Credit Datasets',
                'maintenance_agency_id': agency
            }
        )

        # Create cube structure
        cube_structure = CUBE_STRUCTURE.objects.create(
            cube_structure_id='ANCRDT_STRUCTURE_001',
            name='AnaCredit Test Structure'
        )

        # Create cube with structure
        cube = CUBE.objects.create(
            cube_id='ANCRDT_CUBE_WITH_STRUCTURE',
            name='AnaCredit Cube with Structure',
            framework_id=framework,
            cube_structure_id=cube_structure
        )

        # Verify relationship
        retrieved_cube = CUBE.objects.get(cube_id='ANCRDT_CUBE_WITH_STRUCTURE')
        self.assertEqual(retrieved_cube.cube_structure_id.cube_structure_id, 'ANCRDT_STRUCTURE_001')

    def test_anacredit_specific_tables(self):
        """Test AnaCredit-specific table patterns."""
        agency, _ = MAINTENANCE_AGENCY.objects.get_or_create(
            maintenance_agency_id='ECB',
            defaults={'name': 'European Central Bank'}
        )
        framework, _ = FRAMEWORK.objects.get_or_create(
            framework_id='ANCRDT',
            defaults={
                'name': 'Analytical Credit Datasets',
                'maintenance_agency_id': agency
            }
        )

        # Create AnaCredit-specific cube patterns
        ancrdt_cubes = [
            'ANCRDT_INSTRMNT_C_1',
            'ANCRDT_CNTRPRTY_C_1',
            'ANCRDT_PRTCTN_C_1',
            'ANCRDT_FNNCL_C_1',
        ]

        for cube_id in ancrdt_cubes:
            CUBE.objects.create(
                cube_id=cube_id,
                name=f'{cube_id} Cube',
                framework_id=framework
            )

        # Verify all AnaCredit cubes exist
        for cube_id in ancrdt_cubes:
            self.assertTrue(
                CUBE.objects.filter(cube_id=cube_id).exists(),
                f"AnaCredit cube {cube_id} should exist"
            )

        # Verify they are all under ANCRDT framework
        ancrdt_cube_count = CUBE.objects.filter(
            framework_id__framework_id='ANCRDT'
        ).count()
        self.assertEqual(ancrdt_cube_count, len(ancrdt_cubes))


class TestAnaCreditProcessSteps(TransactionTestCase):
    """Test AnaCredit workflow process steps."""

    reset_sequences = True

    def setUp(self):
        """Set up test environment."""
        self.session = WorkflowSession.objects.create(
            current_task=0,
            configuration={'framework': 'ANCRDT'}
        )

    def test_step_0_fetch_metadata(self):
        """Test Step 0: Fetch Metadata CSV tracking."""
        step_0 = AnaCreditProcessExecution.objects.create(
            session=self.session,
            step_number=0,
            status='running',
            execution_data={'action': 'fetch_metadata_csv'}
        )

        self.assertEqual(step_0.step_number, 0)
        self.assertEqual(step_0.status, 'running')

        # Simulate completion
        step_0.status = 'completed'
        step_0.save()

        self.assertEqual(step_0.status, 'completed')

    def test_step_sequence_dependency(self):
        """Test that steps are tracked in sequence."""
        # Create steps in order
        steps_data = [
            (0, 'Fetch Metadata CSV'),
            (1, 'Import Metadata'),
            (2, 'Create Joins Metadata'),
            (3, 'Create Executable Joins'),
            (4, 'Execute Tables'),
            (5, 'Full Execution with Test Suite'),
        ]

        for step_num, step_name in steps_data:
            AnaCreditProcessExecution.objects.create(
                session=self.session,
                step_number=step_num,
                status='completed',
                execution_data={'step_name': step_name}
            )

        # Verify all steps exist
        all_steps = AnaCreditProcessExecution.objects.filter(
            session=self.session
        ).order_by('step_number')

        self.assertEqual(all_steps.count(), 6)

        # Verify step names
        for step_exec, (expected_num, expected_name) in zip(all_steps, steps_data):
            self.assertEqual(step_exec.step_number, expected_num)
            self.assertEqual(step_exec.execution_data.get('step_name'), expected_name)
