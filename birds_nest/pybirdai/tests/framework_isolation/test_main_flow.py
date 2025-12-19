# coding=UTF-8
# Copyright (c) 2025 Bird Software Solutions Ltd
# Tests for MAIN/FINREP workflow execution and state management

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
    WorkflowSession, WorkflowTaskExecution, DPMProcessExecution
)


class TestMainFlowSetup(TransactionTestCase):
    """Test MAIN/FINREP workflow setup and initialization."""

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

    def test_main_flow_creates_finrep_framework(self):
        """Test that MAIN flow creates FINREP framework entries."""
        # Create a FINREP framework entry
        agency, _ = MAINTENANCE_AGENCY.objects.get_or_create(
            maintenance_agency_id='EBA',
            defaults={'name': 'European Banking Authority'}
        )
        framework, created = FRAMEWORK.objects.get_or_create(
            framework_id='FINREP_REF',
            defaults={
                'name': 'Financial Reporting Framework',
                'maintenance_agency_id': agency
            }
        )

        self.assertTrue(FRAMEWORK.objects.filter(framework_id='FINREP_REF').exists())

    def test_main_flow_sdd_context_isolation(self):
        """Test that SDDContext state is properly isolated."""
        # Add a test entry
        test_cube_id = 'TEST_FINREP_CUBE_001'
        SDDContext.bird_cube_dictionary[test_cube_id] = {'test': True}

        # Verify it exists
        self.assertIn(test_cube_id, SDDContext.bird_cube_dictionary)

        # Clear it
        SDDContext.bird_cube_dictionary = {}

        # Verify it's gone
        self.assertNotIn(test_cube_id, SDDContext.bird_cube_dictionary)

    def test_main_flow_database_tables_exist(self):
        """Test that all required database tables exist for MAIN flow."""
        required_tables = [
            'pybirdai_cube',
            'pybirdai_cube_structure',
            'pybirdai_cube_structure_item',
            'pybirdai_cube_link',
            'pybirdai_member',
            'pybirdai_variable',
            'pybirdai_domain',
            'pybirdai_framework',
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


class TestMainFlowExecution(TransactionTestCase):
    """Test MAIN/FINREP workflow execution steps."""

    reset_sequences = True

    def setUp(self):
        """Set up test environment."""
        self.client = Client()

    def test_workflow_task_execution_tracking(self):
        """Test that workflow task executions are properly tracked."""
        # Create a task execution
        task_exec = WorkflowTaskExecution.objects.create(
            task_number=1,
            operation_type='do',
            status='completed',
            execution_data={'framework': 'FINREP'}
        )

        self.assertEqual(task_exec.task_number, 1)
        self.assertEqual(task_exec.status, 'completed')
        self.assertEqual(task_exec.execution_data.get('framework'), 'FINREP')

    def test_workflow_session_creation(self):
        """Test workflow session creation."""
        session = WorkflowSession.objects.create(
            current_task=1,
            configuration={'framework': 'FINREP'}
        )

        self.assertIsNotNone(session.session_id)
        self.assertEqual(session.current_task, 1)

    def test_main_flow_does_not_affect_ancrdt_data(self):
        """Test that MAIN flow operations don't touch ANCRDT data."""
        # Create ANCRDT framework
        agency, _ = MAINTENANCE_AGENCY.objects.get_or_create(
            maintenance_agency_id='ECB',
            defaults={'name': 'European Central Bank'}
        )
        ancrdt_framework, _ = FRAMEWORK.objects.get_or_create(
            framework_id='ANCRDT',
            defaults={
                'name': 'Analytical Credit Datasets',
                'maintenance_agency_id': agency
            }
        )

        # Create a cube for ANCRDT
        ancrdt_cube = CUBE.objects.create(
            cube_id='ANCRDT_TEST_CUBE',
            name='AnaCredit Test Cube',
            framework_id=ancrdt_framework
        )

        # Simulate MAIN flow creating its own cube
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
            cube_id='FINREP_TEST_CUBE',
            name='FINREP Test Cube',
            framework_id=finrep_framework
        )

        # Verify both cubes exist independently
        self.assertTrue(CUBE.objects.filter(cube_id='ANCRDT_TEST_CUBE').exists())
        self.assertTrue(CUBE.objects.filter(cube_id='FINREP_TEST_CUBE').exists())

        # Verify framework isolation
        ancrdt_cubes = CUBE.objects.filter(framework_id__framework_id='ANCRDT')
        finrep_cubes = CUBE.objects.filter(framework_id__framework_id='FINREP_REF')

        self.assertEqual(ancrdt_cubes.count(), 1)
        self.assertEqual(finrep_cubes.count(), 1)


class TestMainFlowDataIntegrity(TransactionTestCase):
    """Test MAIN/FINREP workflow data integrity."""

    reset_sequences = True

    def test_cube_structure_relationships(self):
        """Test that cube-structure relationships are maintained."""
        agency, _ = MAINTENANCE_AGENCY.objects.get_or_create(
            maintenance_agency_id='EBA',
            defaults={'name': 'European Banking Authority'}
        )
        framework, _ = FRAMEWORK.objects.get_or_create(
            framework_id='FINREP_REF',
            defaults={
                'name': 'Financial Reporting Framework',
                'maintenance_agency_id': agency
            }
        )

        # Create cube structure
        cube_structure = CUBE_STRUCTURE.objects.create(
            cube_structure_id='TEST_STRUCTURE_001',
            name='Test Structure'
        )

        # Create cube with structure
        cube = CUBE.objects.create(
            cube_id='TEST_CUBE_WITH_STRUCTURE',
            name='Test Cube with Structure',
            framework_id=framework,
            cube_structure_id=cube_structure
        )

        # Verify relationship
        retrieved_cube = CUBE.objects.get(cube_id='TEST_CUBE_WITH_STRUCTURE')
        self.assertEqual(retrieved_cube.cube_structure_id.cube_structure_id, 'TEST_STRUCTURE_001')

    def test_multiple_frameworks_coexist(self):
        """Test that multiple frameworks can coexist in the database."""
        agency, _ = MAINTENANCE_AGENCY.objects.get_or_create(
            maintenance_agency_id='MULTI',
            defaults={'name': 'Multi Agency'}
        )

        frameworks_to_create = ['FINREP', 'COREP', 'AE', 'ANCRDT']
        created_frameworks = []

        for fw_id in frameworks_to_create:
            fw, _ = FRAMEWORK.objects.get_or_create(
                framework_id=fw_id,
                defaults={
                    'name': f'{fw_id} Framework',
                    'maintenance_agency_id': agency
                }
            )
            created_frameworks.append(fw)

        # Verify all frameworks exist
        for fw_id in frameworks_to_create:
            self.assertTrue(
                FRAMEWORK.objects.filter(framework_id=fw_id).exists(),
                f"Framework {fw_id} should exist"
            )
