# coding=UTF-8
# Copyright (c) 2025 Bird Software Solutions Ltd
# Tests for framework-specific deletion with orphan cleanup

import os
import sys
from pathlib import Path

# Configure Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'birds_nest.settings')

import django
django.setup()

from django.test import TransactionTestCase
from django.db import connection

from pybirdai.context.sdd_context_django import SDDContext
from pybirdai.models.bird_meta_data_model import (
    CUBE, CUBE_STRUCTURE, CUBE_STRUCTURE_ITEM, CUBE_LINK,
    MEMBER, VARIABLE, DOMAIN, FRAMEWORK, MAINTENANCE_AGENCY
)
from pybirdai.process_steps.joins_meta_data.delete_joins_meta_data import (
    TransformationMetaDataDestroyer
)


class TestFrameworkSpecificDeletion(TransactionTestCase):
    """Test framework-specific deletion with orphan cleanup."""

    reset_sequences = True

    def setUp(self):
        """Set up test data with two frameworks."""
        # Create agencies
        self.eba_agency, _ = MAINTENANCE_AGENCY.objects.get_or_create(
            maintenance_agency_id='EBA',
            defaults={'name': 'European Banking Authority'}
        )
        self.ecb_agency, _ = MAINTENANCE_AGENCY.objects.get_or_create(
            maintenance_agency_id='ECB',
            defaults={'name': 'European Central Bank'}
        )

        # Create frameworks
        self.finrep_framework, _ = FRAMEWORK.objects.get_or_create(
            framework_id='FINREP_REF',
            defaults={
                'name': 'Financial Reporting Framework',
                'maintenance_agency_id': self.eba_agency
            }
        )
        self.ancrdt_framework, _ = FRAMEWORK.objects.get_or_create(
            framework_id='ANCRDT',
            defaults={
                'name': 'Analytical Credit Datasets',
                'maintenance_agency_id': self.ecb_agency
            }
        )

        # Create shared domain
        self.shared_domain, _ = DOMAIN.objects.get_or_create(
            domain_id='SHARED_DOMAIN',
            defaults={
                'name': 'Shared Domain',
                'maintenance_agency_id': self.eba_agency
            }
        )

        # Create shared variable
        self.shared_variable, _ = VARIABLE.objects.get_or_create(
            variable_id='SHARED_VAR',
            defaults={
                'name': 'Shared Variable',
                'maintenance_agency_id': self.eba_agency,
                'domain_id': self.shared_domain
            }
        )

        # Create destroyer instance
        self.destroyer = TransformationMetaDataDestroyer()

    def test_config_file_loads(self):
        """Test that the orphan cleanup config file loads correctly."""
        config = self.destroyer._load_orphan_config()
        self.assertIn('framework_output_tables', config)
        self.assertIn('orphan_cleanup', config)
        self.assertIn('sdd_context_cleanup', config)

    def test_delete_finrep_preserves_ancrdt(self):
        """Test that deleting FINREP data preserves ANCRDT data."""
        # Create FINREP cubes
        finrep_structure = CUBE_STRUCTURE.objects.create(
            cube_structure_id='FINREP_STRUCTURE_001',
            name='FINREP Structure'
        )
        finrep_cube = CUBE.objects.create(
            cube_id='FINREP_CUBE_001',
            name='FINREP Cube',
            framework_id=self.finrep_framework,
            cube_structure_id=finrep_structure
        )

        # Create ANCRDT cubes
        ancrdt_structure = CUBE_STRUCTURE.objects.create(
            cube_structure_id='ANCRDT_STRUCTURE_001',
            name='ANCRDT Structure'
        )
        ancrdt_cube = CUBE.objects.create(
            cube_id='ANCRDT_CUBE_001',
            name='ANCRDT Cube',
            framework_id=self.ancrdt_framework,
            cube_structure_id=ancrdt_structure
        )

        # Verify both exist
        self.assertEqual(CUBE.objects.filter(framework_id=self.finrep_framework).count(), 1)
        self.assertEqual(CUBE.objects.filter(framework_id=self.ancrdt_framework).count(), 1)

        # Delete FINREP framework data
        sdd_context = SDDContext()
        result = self.destroyer.delete_framework_with_orphan_cleanup(
            context=None,
            sdd_context=sdd_context,
            framework_id='FINREP_REF'
        )

        # Verify FINREP cube is deleted
        self.assertEqual(CUBE.objects.filter(framework_id=self.finrep_framework).count(), 0)

        # Verify ANCRDT cube is preserved
        self.assertEqual(CUBE.objects.filter(framework_id=self.ancrdt_framework).count(), 1)
        self.assertTrue(CUBE.objects.filter(cube_id='ANCRDT_CUBE_001').exists())

    def test_delete_ancrdt_preserves_finrep(self):
        """Test that deleting ANCRDT data preserves FINREP data."""
        # Create FINREP cubes
        finrep_cube = CUBE.objects.create(
            cube_id='FINREP_CUBE_002',
            name='FINREP Cube 2',
            framework_id=self.finrep_framework
        )

        # Create ANCRDT cubes
        ancrdt_cube = CUBE.objects.create(
            cube_id='ANCRDT_CUBE_002',
            name='ANCRDT Cube 2',
            framework_id=self.ancrdt_framework
        )

        # Delete ANCRDT framework data
        sdd_context = SDDContext()
        result = self.destroyer.delete_framework_with_orphan_cleanup(
            context=None,
            sdd_context=sdd_context,
            framework_id='ANCRDT'
        )

        # Verify ANCRDT cube is deleted
        self.assertEqual(CUBE.objects.filter(framework_id=self.ancrdt_framework).count(), 0)

        # Verify FINREP cube is preserved
        self.assertEqual(CUBE.objects.filter(framework_id=self.finrep_framework).count(), 1)
        self.assertTrue(CUBE.objects.filter(cube_id='FINREP_CUBE_002').exists())

    def test_shared_domain_preserved_when_used_by_other_framework(self):
        """Test that shared domains are preserved when still referenced by active cubes."""
        # Create a cube structure for ANCRDT
        ancrdt_structure = CUBE_STRUCTURE.objects.create(
            cube_structure_id='ANCRDT_SHARED_TEST_STRUCTURE',
            name='ANCRDT Shared Test Structure'
        )

        # Create a cube for ANCRDT
        ancrdt_cube = CUBE.objects.create(
            cube_id='ANCRDT_SHARED_TEST_CUBE',
            name='ANCRDT Shared Test Cube',
            framework_id=self.ancrdt_framework,
            cube_structure_id=ancrdt_structure
        )

        # Create a variable that uses the shared domain
        shared_var = VARIABLE.objects.create(
            variable_id='SHARED_VAR_IN_USE',
            name='Shared Variable In Use',
            maintenance_agency_id=self.eba_agency,
            domain_id=self.shared_domain
        )

        # Create a cube structure item that references the variable
        # This makes the variable "in use" and not an orphan
        CUBE_STRUCTURE_ITEM.objects.create(
            cube_structure_id=ancrdt_structure,
            variable_id=shared_var
        )

        # Delete FINREP framework data
        sdd_context = SDDContext()
        self.destroyer.delete_framework_with_orphan_cleanup(
            context=None,
            sdd_context=sdd_context,
            framework_id='FINREP_REF'
        )

        # Variable should still exist (referenced by ANCRDT's cube structure item)
        self.assertTrue(VARIABLE.objects.filter(variable_id='SHARED_VAR_IN_USE').exists())

        # Shared domain should still exist (referenced by the variable)
        self.assertTrue(DOMAIN.objects.filter(domain_id='SHARED_DOMAIN').exists())

    def test_sequential_flow_execution(self):
        """Test running MAIN flow then ANCRDT flow sequentially."""
        # Create FINREP data
        finrep_cube = CUBE.objects.create(
            cube_id='SEQ_FINREP_CUBE',
            name='Sequential FINREP Cube',
            framework_id=self.finrep_framework
        )

        # Run "MAIN flow" (simulated by just having data)
        finrep_cube_count = CUBE.objects.filter(framework_id=self.finrep_framework).count()
        self.assertEqual(finrep_cube_count, 1)

        # Create ANCRDT data (simulating AnaCredit flow)
        ancrdt_cube = CUBE.objects.create(
            cube_id='SEQ_ANCRDT_CUBE',
            name='Sequential ANCRDT Cube',
            framework_id=self.ancrdt_framework
        )

        # Verify both frameworks have data
        self.assertEqual(CUBE.objects.filter(framework_id=self.finrep_framework).count(), 1)
        self.assertEqual(CUBE.objects.filter(framework_id=self.ancrdt_framework).count(), 1)

        # Now re-run ANCRDT flow (delete old data, create new)
        sdd_context = SDDContext()
        self.destroyer.delete_framework_with_orphan_cleanup(
            context=None,
            sdd_context=sdd_context,
            framework_id='ANCRDT'
        )

        # Create new ANCRDT data
        new_ancrdt_cube = CUBE.objects.create(
            cube_id='SEQ_ANCRDT_CUBE_V2',
            name='Sequential ANCRDT Cube V2',
            framework_id=self.ancrdt_framework
        )

        # FINREP data should be unchanged
        self.assertEqual(CUBE.objects.filter(framework_id=self.finrep_framework).count(), 1)
        self.assertTrue(CUBE.objects.filter(cube_id='SEQ_FINREP_CUBE').exists())

        # ANCRDT should have new data
        self.assertEqual(CUBE.objects.filter(framework_id=self.ancrdt_framework).count(), 1)
        self.assertTrue(CUBE.objects.filter(cube_id='SEQ_ANCRDT_CUBE_V2').exists())
        self.assertFalse(CUBE.objects.filter(cube_id='SEQ_ANCRDT_CUBE').exists())

    def test_deletion_returns_summary(self):
        """Test that deletion returns a summary of deleted records."""
        # Create some test data
        cube = CUBE.objects.create(
            cube_id='SUMMARY_TEST_CUBE',
            name='Summary Test Cube',
            framework_id=self.finrep_framework
        )

        sdd_context = SDDContext()
        result = self.destroyer.delete_framework_with_orphan_cleanup(
            context=None,
            sdd_context=sdd_context,
            framework_id='FINREP_REF'
        )

        # Verify result structure
        self.assertIn('framework_output', result)
        self.assertIn('junction_tables', result)
        self.assertIn('orphan_cleanup', result)


class TestOrphanCleanup(TransactionTestCase):
    """Test orphan cleanup functionality."""

    reset_sequences = True

    def setUp(self):
        """Set up test environment."""
        self.agency, _ = MAINTENANCE_AGENCY.objects.get_or_create(
            maintenance_agency_id='TEST_AGENCY',
            defaults={'name': 'Test Agency'}
        )
        self.framework, _ = FRAMEWORK.objects.get_or_create(
            framework_id='TEST_FRAMEWORK',
            defaults={
                'name': 'Test Framework',
                'maintenance_agency_id': self.agency
            }
        )
        self.destroyer = TransformationMetaDataDestroyer()

    def test_orphan_config_cleanup_order(self):
        """Test that orphan cleanup order is defined in config."""
        config = self.destroyer._load_orphan_config()
        orphan_config = config.get('orphan_cleanup', {})
        cleanup_order = orphan_config.get('cleanup_order', [])

        # Should have a defined order
        self.assertGreater(len(cleanup_order), 0)

        # Leaf nodes should come before parent nodes
        if 'ORDINATE_ITEM' in cleanup_order and 'AXIS_ORDINATE' in cleanup_order:
            ordinate_item_idx = cleanup_order.index('ORDINATE_ITEM')
            axis_ordinate_idx = cleanup_order.index('AXIS_ORDINATE')
            self.assertLess(ordinate_item_idx, axis_ordinate_idx)

    def test_table_exists_check(self):
        """Test the _table_exists helper method."""
        with connection.cursor() as cursor:
            # Should exist
            self.assertTrue(self.destroyer._table_exists(cursor, 'pybirdai_cube'))
            self.assertTrue(self.destroyer._table_exists(cursor, 'pybirdai_framework'))

            # Should not exist
            self.assertFalse(self.destroyer._table_exists(cursor, 'nonexistent_table'))

    def test_safe_delete_nonexistent_table(self):
        """Test that safe_delete handles nonexistent tables gracefully."""
        with connection.cursor() as cursor:
            # Should return 0 and not raise an error
            result = self.destroyer._safe_delete(cursor, 'nonexistent_table')
            self.assertEqual(result, 0)
