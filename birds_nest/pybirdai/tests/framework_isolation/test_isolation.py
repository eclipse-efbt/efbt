# coding=UTF-8
# Copyright (c) 2025 Bird Software Solutions Ltd
# Cross-flow isolation tests for PyBIRD AI
# Tests to verify MAIN/FINREP and AnaCredit workflows don't interfere with each other

import pytest
import os
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

# Configure Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'birds_nest.settings')

import django
django.setup()

from django.test import TransactionTestCase, Client
from django.db import connection

from pybirdai.context.sdd_context_django import SDDContext
from pybirdai.models.bird_meta_data_model import (
    CUBE, CUBE_STRUCTURE, CUBE_STRUCTURE_ITEM, CUBE_LINK,
    MEMBER, VARIABLE, DOMAIN, FRAMEWORK, MAINTENANCE_AGENCY,
    CUBE_STRUCTURE_ITEM_LINK
)
from pybirdai.models.workflow_model import (
    WorkflowSession, WorkflowTaskExecution,
    DPMProcessExecution, AnaCreditProcessExecution
)


class TestSequentialExecution(TransactionTestCase):
    """
    Test sequential execution of MAIN and AnaCredit workflows.
    Verifies that running one workflow after another doesn't corrupt data.
    """

    reset_sequences = True

    def setUp(self):
        """Set up test environment."""
        self.client = Client()
        self._reset_sdd_context()

        # Create agencies
        self.eba_agency, _ = MAINTENANCE_AGENCY.objects.get_or_create(
            maintenance_agency_id='EBA',
            defaults={'name': 'European Banking Authority'}
        )
        self.ecb_agency, _ = MAINTENANCE_AGENCY.objects.get_or_create(
            maintenance_agency_id='ECB',
            defaults={'name': 'European Central Bank'}
        )

    def tearDown(self):
        """Clean up after test."""
        self._reset_sdd_context()

    def _reset_sdd_context(self):
        """Reset all SDDContext class-level dictionaries."""
        dict_attrs = [
            'mapping_definition_dictionary', 'bird_cube_dictionary',
            'bird_cube_structure_dictionary', 'bird_cube_structure_item_dictionary',
            'framework_dictionary', 'domain_dictionary', 'member_dictionary',
            'variable_dictionary', 'cube_link_dictionary',
            'cube_structure_item_links_dictionary'
        ]
        for attr in dict_attrs:
            if hasattr(SDDContext, attr):
                setattr(SDDContext, attr, {})

    def test_main_then_anacredit_isolation(self):
        """
        Test: Run MAIN flow first, then AnaCredit flow.
        Verify MAIN data is unchanged after AnaCredit completes.
        """
        # Step 1: Create MAIN/FINREP data
        finrep_framework, _ = FRAMEWORK.objects.get_or_create(
            framework_id='FINREP_REF',
            defaults={
                'name': 'Financial Reporting Framework',
                'maintenance_agency_id': self.eba_agency
            }
        )

        # Create FINREP cubes
        finrep_cubes = []
        for i in range(3):
            cube = CUBE.objects.create(
                cube_id=f'FINREP_CUBE_{i:03d}',
                name=f'FINREP Test Cube {i}',
                framework_id=finrep_framework
            )
            finrep_cubes.append(cube.cube_id)

        # Record MAIN state
        main_state_before = {
            'cube_count': CUBE.objects.filter(framework_id__framework_id='FINREP_REF').count(),
            'cube_ids': set(CUBE.objects.filter(framework_id__framework_id='FINREP_REF').values_list('cube_id', flat=True))
        }

        # Step 2: Create AnaCredit data
        ancrdt_framework, _ = FRAMEWORK.objects.get_or_create(
            framework_id='ANCRDT',
            defaults={
                'name': 'Analytical Credit Datasets',
                'maintenance_agency_id': self.ecb_agency
            }
        )

        # Create AnaCredit cubes
        for i in range(5):
            CUBE.objects.create(
                cube_id=f'ANCRDT_CUBE_{i:03d}',
                name=f'AnaCredit Test Cube {i}',
                framework_id=ancrdt_framework
            )

        # Step 3: Verify MAIN state unchanged
        main_state_after = {
            'cube_count': CUBE.objects.filter(framework_id__framework_id='FINREP_REF').count(),
            'cube_ids': set(CUBE.objects.filter(framework_id__framework_id='FINREP_REF').values_list('cube_id', flat=True))
        }

        self.assertEqual(
            main_state_before['cube_count'],
            main_state_after['cube_count'],
            "FINREP cube count changed after AnaCredit operations"
        )
        self.assertEqual(
            main_state_before['cube_ids'],
            main_state_after['cube_ids'],
            "FINREP cube IDs changed after AnaCredit operations"
        )

    def test_anacredit_then_main_isolation(self):
        """
        Test: Run AnaCredit flow first, then MAIN flow.
        Verify AnaCredit data is unchanged after MAIN completes.
        """
        # Step 1: Create AnaCredit data first
        ancrdt_framework, _ = FRAMEWORK.objects.get_or_create(
            framework_id='ANCRDT',
            defaults={
                'name': 'Analytical Credit Datasets',
                'maintenance_agency_id': self.ecb_agency
            }
        )

        for i in range(4):
            CUBE.objects.create(
                cube_id=f'ANCRDT_FIRST_CUBE_{i:03d}',
                name=f'AnaCredit First Cube {i}',
                framework_id=ancrdt_framework
            )

        # Record AnaCredit state
        ancrdt_state_before = {
            'cube_count': CUBE.objects.filter(framework_id__framework_id='ANCRDT').count(),
            'cube_ids': set(CUBE.objects.filter(framework_id__framework_id='ANCRDT').values_list('cube_id', flat=True))
        }

        # Step 2: Create MAIN/FINREP data
        finrep_framework, _ = FRAMEWORK.objects.get_or_create(
            framework_id='FINREP_REF',
            defaults={
                'name': 'Financial Reporting Framework',
                'maintenance_agency_id': self.eba_agency
            }
        )

        for i in range(6):
            CUBE.objects.create(
                cube_id=f'FINREP_SECOND_CUBE_{i:03d}',
                name=f'FINREP Second Cube {i}',
                framework_id=finrep_framework
            )

        # Step 3: Verify AnaCredit state unchanged
        ancrdt_state_after = {
            'cube_count': CUBE.objects.filter(framework_id__framework_id='ANCRDT').count(),
            'cube_ids': set(CUBE.objects.filter(framework_id__framework_id='ANCRDT').values_list('cube_id', flat=True))
        }

        self.assertEqual(
            ancrdt_state_before['cube_count'],
            ancrdt_state_after['cube_count'],
            "AnaCredit cube count changed after FINREP operations"
        )
        self.assertEqual(
            ancrdt_state_before['cube_ids'],
            ancrdt_state_after['cube_ids'],
            "AnaCredit cube IDs changed after FINREP operations"
        )


class TestSDDContextIsolation(TransactionTestCase):
    """
    Test SDDContext class-level dictionary isolation between frameworks.
    This is critical because SDDContext uses class variables that persist.
    """

    reset_sequences = True

    def setUp(self):
        """Set up test environment."""
        self._capture_original_state()

    def tearDown(self):
        """Restore original state."""
        self._restore_original_state()

    def _capture_original_state(self):
        """Capture original SDDContext state."""
        self.original_state = {}
        for attr in dir(SDDContext):
            if not attr.startswith('_') and isinstance(getattr(SDDContext, attr, None), dict):
                self.original_state[attr] = getattr(SDDContext, attr, {}).copy()

    def _restore_original_state(self):
        """Restore original SDDContext state."""
        for attr, value in self.original_state.items():
            setattr(SDDContext, attr, value)

    def test_bird_cube_dictionary_isolation(self):
        """Test that bird_cube_dictionary is isolated between frameworks."""
        # Set up FINREP entries
        SDDContext.bird_cube_dictionary['FINREP_CUBE_001'] = {'framework': 'FINREP'}
        SDDContext.bird_cube_dictionary['FINREP_CUBE_002'] = {'framework': 'FINREP'}

        finrep_cubes_before = {
            k: v for k, v in SDDContext.bird_cube_dictionary.items()
            if k.startswith('FINREP_')
        }

        # Add ANCRDT entries (simulating AnaCredit flow)
        SDDContext.bird_cube_dictionary['ANCRDT_CUBE_001'] = {'framework': 'ANCRDT'}
        SDDContext.bird_cube_dictionary['ANCRDT_CUBE_002'] = {'framework': 'ANCRDT'}

        # Verify FINREP entries unchanged
        finrep_cubes_after = {
            k: v for k, v in SDDContext.bird_cube_dictionary.items()
            if k.startswith('FINREP_')
        }

        self.assertEqual(finrep_cubes_before, finrep_cubes_after)

    def test_framework_dictionary_isolation(self):
        """Test that framework_dictionary maintains separate entries."""
        # Add frameworks
        SDDContext.framework_dictionary['FINREP_REF'] = {'name': 'FINREP'}
        SDDContext.framework_dictionary['ANCRDT'] = {'name': 'AnaCredit'}

        # Verify both exist independently
        self.assertIn('FINREP_REF', SDDContext.framework_dictionary)
        self.assertIn('ANCRDT', SDDContext.framework_dictionary)
        self.assertNotEqual(
            SDDContext.framework_dictionary['FINREP_REF'],
            SDDContext.framework_dictionary['ANCRDT']
        )

    def test_sdd_context_reset_affects_all_frameworks(self):
        """
        Test that resetting SDDContext affects ALL frameworks.
        This is a potential issue - resetting for one framework resets all.
        """
        # Add entries for both frameworks
        SDDContext.bird_cube_dictionary['FINREP_CUBE'] = {'framework': 'FINREP'}
        SDDContext.bird_cube_dictionary['ANCRDT_CUBE'] = {'framework': 'ANCRDT'}

        # Reset the dictionary (simulates what happens in delete operations)
        SDDContext.bird_cube_dictionary = {}

        # Both should be gone - this is the isolation issue!
        self.assertNotIn('FINREP_CUBE', SDDContext.bird_cube_dictionary)
        self.assertNotIn('ANCRDT_CUBE', SDDContext.bird_cube_dictionary)


class TestDatabaseIsolation(TransactionTestCase):
    """
    Test database-level isolation between frameworks.
    """

    reset_sequences = True

    def setUp(self):
        """Set up test environment."""
        self.eba_agency, _ = MAINTENANCE_AGENCY.objects.get_or_create(
            maintenance_agency_id='EBA',
            defaults={'name': 'European Banking Authority'}
        )
        self.ecb_agency, _ = MAINTENANCE_AGENCY.objects.get_or_create(
            maintenance_agency_id='ECB',
            defaults={'name': 'European Central Bank'}
        )

    def test_cube_framework_filtering(self):
        """Test that cubes can be filtered by framework."""
        # Create frameworks
        finrep, _ = FRAMEWORK.objects.get_or_create(
            framework_id='FINREP_REF',
            defaults={'name': 'FINREP', 'maintenance_agency_id': self.eba_agency}
        )
        ancrdt, _ = FRAMEWORK.objects.get_or_create(
            framework_id='ANCRDT',
            defaults={'name': 'AnaCredit', 'maintenance_agency_id': self.ecb_agency}
        )

        # Create cubes for each
        for i in range(3):
            CUBE.objects.create(
                cube_id=f'FIN_{i}', name=f'FINREP {i}', framework_id=finrep
            )
        for i in range(5):
            CUBE.objects.create(
                cube_id=f'ANC_{i}', name=f'ANCRDT {i}', framework_id=ancrdt
            )

        # Test filtering
        finrep_cubes = CUBE.objects.filter(framework_id__framework_id='FINREP_REF')
        ancrdt_cubes = CUBE.objects.filter(framework_id__framework_id='ANCRDT')

        self.assertEqual(finrep_cubes.count(), 3)
        self.assertEqual(ancrdt_cubes.count(), 5)

    def test_cube_link_framework_isolation(self):
        """Test that cube links are isolated by framework."""
        # Create frameworks
        finrep, _ = FRAMEWORK.objects.get_or_create(
            framework_id='FINREP_REF',
            defaults={'name': 'FINREP', 'maintenance_agency_id': self.eba_agency}
        )
        ancrdt, _ = FRAMEWORK.objects.get_or_create(
            framework_id='ANCRDT',
            defaults={'name': 'AnaCredit', 'maintenance_agency_id': self.ecb_agency}
        )

        # Create cubes
        finrep_cube = CUBE.objects.create(
            cube_id='FIN_LINK_TEST', name='FINREP Link Test', framework_id=finrep
        )
        ancrdt_cube = CUBE.objects.create(
            cube_id='ANC_LINK_TEST', name='ANCRDT Link Test', framework_id=ancrdt
        )

        # Create cube links
        finrep_link = CUBE_LINK.objects.create(
            cube_link_id='FINREP_LINK_001',
            name='FINREP Link',
            foreign_cube_id=finrep_cube
        )
        ancrdt_link = CUBE_LINK.objects.create(
            cube_link_id='ANCRDT_LINK_001',
            name='ANCRDT Link',
            foreign_cube_id=ancrdt_cube
        )

        # Verify isolation via foreign key relationship
        finrep_links = CUBE_LINK.objects.filter(
            foreign_cube_id__framework_id__framework_id='FINREP_REF'
        )
        ancrdt_links = CUBE_LINK.objects.filter(
            foreign_cube_id__framework_id__framework_id='ANCRDT'
        )

        self.assertEqual(finrep_links.count(), 1)
        self.assertEqual(ancrdt_links.count(), 1)


class TestWorkflowSessionIsolation(TransactionTestCase):
    """
    Test workflow session isolation between different workflow types.
    """

    reset_sequences = True

    def test_separate_execution_models(self):
        """Test that MAIN and AnaCredit use separate execution models."""
        session = WorkflowSession.objects.create(
            current_task=1,
            configuration={'test': True}
        )

        # Create MAIN workflow execution
        main_exec = WorkflowTaskExecution.objects.create(
            task_number=1,
            operation_type='do',
            status='completed'
        )

        # Create AnaCredit execution
        ancrdt_exec = AnaCreditProcessExecution.objects.create(
            session=session,
            step_number=0,
            status='completed'
        )

        # Verify they are in different tables
        self.assertEqual(WorkflowTaskExecution.objects.count(), 1)
        self.assertEqual(AnaCreditProcessExecution.objects.count(), 1)

        # Verify they don't interfere
        main_exec.status = 'failed'
        main_exec.save()

        # AnaCredit status should be unchanged
        ancrdt_exec.refresh_from_db()
        self.assertEqual(ancrdt_exec.status, 'completed')

    def test_dpm_execution_separate_from_anacredit(self):
        """Test that DPM and AnaCredit executions are separate."""
        session = WorkflowSession.objects.create(
            current_task=1,
            configuration={'test': True}
        )

        # Create DPM execution
        dpm_exec = DPMProcessExecution.objects.create(
            session=session,
            step_number=1,
            status='running',
            selected_frameworks=['FINREP', 'COREP']
        )

        # Create AnaCredit execution
        ancrdt_exec = AnaCreditProcessExecution.objects.create(
            session=session,
            step_number=0,
            status='completed'
        )

        # Verify separate tracking
        self.assertEqual(DPMProcessExecution.objects.count(), 1)
        self.assertEqual(AnaCreditProcessExecution.objects.count(), 1)

        # Verify different data
        self.assertEqual(dpm_exec.selected_frameworks, ['FINREP', 'COREP'])
        self.assertIsNone(getattr(ancrdt_exec, 'selected_frameworks', None))


class TestConcurrentAccessIsolation(TransactionTestCase):
    """
    Test isolation when multiple frameworks are accessed concurrently.
    """

    reset_sequences = True

    def setUp(self):
        """Set up test environment."""
        self.eba_agency, _ = MAINTENANCE_AGENCY.objects.get_or_create(
            maintenance_agency_id='EBA',
            defaults={'name': 'European Banking Authority'}
        )
        self.ecb_agency, _ = MAINTENANCE_AGENCY.objects.get_or_create(
            maintenance_agency_id='ECB',
            defaults={'name': 'European Central Bank'}
        )

    def test_simultaneous_framework_data_creation(self):
        """Test creating data for multiple frameworks simultaneously."""
        # Create frameworks
        finrep, _ = FRAMEWORK.objects.get_or_create(
            framework_id='FINREP_REF',
            defaults={'name': 'FINREP', 'maintenance_agency_id': self.eba_agency}
        )
        ancrdt, _ = FRAMEWORK.objects.get_or_create(
            framework_id='ANCRDT',
            defaults={'name': 'AnaCredit', 'maintenance_agency_id': self.ecb_agency}
        )

        # Simulate interleaved cube creation
        cubes_created = []
        for i in range(5):
            # Alternate between frameworks
            if i % 2 == 0:
                cube = CUBE.objects.create(
                    cube_id=f'INTERLEAVE_FIN_{i}',
                    name=f'Interleaved FINREP {i}',
                    framework_id=finrep
                )
            else:
                cube = CUBE.objects.create(
                    cube_id=f'INTERLEAVE_ANC_{i}',
                    name=f'Interleaved ANCRDT {i}',
                    framework_id=ancrdt
                )
            cubes_created.append(cube)

        # Verify correct framework assignment
        finrep_cubes = CUBE.objects.filter(
            cube_id__startswith='INTERLEAVE_FIN_'
        )
        ancrdt_cubes = CUBE.objects.filter(
            cube_id__startswith='INTERLEAVE_ANC_'
        )

        # All FINREP cubes should have FINREP framework
        for cube in finrep_cubes:
            self.assertEqual(cube.framework_id.framework_id, 'FINREP_REF')

        # All ANCRDT cubes should have ANCRDT framework
        for cube in ancrdt_cubes:
            self.assertEqual(cube.framework_id.framework_id, 'ANCRDT')

    def test_no_cross_contamination_on_bulk_operations(self):
        """Test that bulk operations don't cross-contaminate frameworks."""
        # Create frameworks
        finrep, _ = FRAMEWORK.objects.get_or_create(
            framework_id='FINREP_REF',
            defaults={'name': 'FINREP', 'maintenance_agency_id': self.eba_agency}
        )
        ancrdt, _ = FRAMEWORK.objects.get_or_create(
            framework_id='ANCRDT',
            defaults={'name': 'AnaCredit', 'maintenance_agency_id': self.ecb_agency}
        )

        # Create FINREP cubes
        finrep_cube_objects = [
            CUBE(cube_id=f'BULK_FIN_{i}', name=f'Bulk FINREP {i}', framework_id=finrep)
            for i in range(10)
        ]
        CUBE.objects.bulk_create(finrep_cube_objects)

        # Create ANCRDT cubes
        ancrdt_cube_objects = [
            CUBE(cube_id=f'BULK_ANC_{i}', name=f'Bulk ANCRDT {i}', framework_id=ancrdt)
            for i in range(10)
        ]
        CUBE.objects.bulk_create(ancrdt_cube_objects)

        # Verify counts
        self.assertEqual(
            CUBE.objects.filter(framework_id__framework_id='FINREP_REF', cube_id__startswith='BULK_').count(),
            10
        )
        self.assertEqual(
            CUBE.objects.filter(framework_id__framework_id='ANCRDT', cube_id__startswith='BULK_').count(),
            10
        )

        # Bulk delete ANCRDT cubes only
        CUBE.objects.filter(cube_id__startswith='BULK_ANC_').delete()

        # FINREP cubes should be unaffected
        self.assertEqual(
            CUBE.objects.filter(framework_id__framework_id='FINREP_REF', cube_id__startswith='BULK_').count(),
            10
        )
        # ANCRDT cubes should be gone
        self.assertEqual(
            CUBE.objects.filter(cube_id__startswith='BULK_ANC_').count(),
            0
        )
