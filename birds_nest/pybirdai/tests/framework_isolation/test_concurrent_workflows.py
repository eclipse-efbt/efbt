# coding=UTF-8
# Copyright (c) 2025 Bird Software Solutions Ltd
# Tests for concurrent workflow execution with framework isolation

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
from pybirdai.process_steps.input_model.import_database_to_sdd_model import (
    ImportDatabaseToSDDModel
)


class TestFrameworkFilteredImport(TransactionTestCase):
    """Test framework-filtered database imports."""

    reset_sequences = True

    def setUp(self):
        """Set up test data with multiple frameworks."""
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

        # Create cube structures
        self.finrep_structure = CUBE_STRUCTURE.objects.create(
            cube_structure_id='FINREP_STRUCT_001',
            name='FINREP Structure'
        )
        self.ancrdt_structure = CUBE_STRUCTURE.objects.create(
            cube_structure_id='ANCRDT_STRUCT_001',
            name='ANCRDT Structure'
        )

        # Create cubes for each framework
        self.finrep_cube = CUBE.objects.create(
            cube_id='FINREP_CUBE_001',
            name='FINREP Cube',
            framework_id=self.finrep_framework,
            cube_structure_id=self.finrep_structure
        )
        self.ancrdt_cube = CUBE.objects.create(
            cube_id='ANCRDT_CUBE_001',
            name='ANCRDT Cube',
            framework_id=self.ancrdt_framework,
            cube_structure_id=self.ancrdt_structure
        )

        # Create cube links
        self.finrep_link = CUBE_LINK.objects.create(
            cube_link_id='FINREP_LINK_001',
            foreign_cube_id=self.finrep_cube,
            primary_cube_id=self.finrep_cube,
            join_identifier='FINREP_JOIN'
        )
        self.ancrdt_link = CUBE_LINK.objects.create(
            cube_link_id='ANCRDT_LINK_001',
            foreign_cube_id=self.ancrdt_cube,
            primary_cube_id=self.ancrdt_cube,
            join_identifier='ANCRDT_JOIN'
        )

    def test_create_all_rol_cubes_without_filter(self):
        """Test that without filter, all cubes are loaded."""
        sdd_context = SDDContext()
        importer = ImportDatabaseToSDDModel()

        # Load without filter
        importer.create_all_rol_cubes(sdd_context)

        # Should have both cubes
        self.assertEqual(len(sdd_context.bird_cube_dictionary), 2)
        self.assertIn('FINREP_CUBE_001', sdd_context.bird_cube_dictionary)
        self.assertIn('ANCRDT_CUBE_001', sdd_context.bird_cube_dictionary)

    def test_create_all_rol_cubes_with_finrep_filter(self):
        """Test that FINREP filter only loads FINREP cubes."""
        sdd_context = SDDContext()
        importer = ImportDatabaseToSDDModel()

        # Load with FINREP filter
        importer.create_all_rol_cubes(sdd_context, framework_id='FINREP_REF')

        # Should only have FINREP cube
        self.assertEqual(len(sdd_context.bird_cube_dictionary), 1)
        self.assertIn('FINREP_CUBE_001', sdd_context.bird_cube_dictionary)
        self.assertNotIn('ANCRDT_CUBE_001', sdd_context.bird_cube_dictionary)

    def test_create_all_rol_cubes_with_ancrdt_filter(self):
        """Test that ANCRDT filter only loads ANCRDT cubes."""
        sdd_context = SDDContext()
        importer = ImportDatabaseToSDDModel()

        # Load with ANCRDT filter
        importer.create_all_rol_cubes(sdd_context, framework_id='ANCRDT')

        # Should only have ANCRDT cube
        self.assertEqual(len(sdd_context.bird_cube_dictionary), 1)
        self.assertNotIn('FINREP_CUBE_001', sdd_context.bird_cube_dictionary)
        self.assertIn('ANCRDT_CUBE_001', sdd_context.bird_cube_dictionary)

    def test_create_cube_links_without_filter(self):
        """Test that without filter, all cube links are loaded."""
        sdd_context = SDDContext()
        importer = ImportDatabaseToSDDModel()

        # Load without filter
        importer.create_cube_links(sdd_context)

        # Should have both links
        self.assertEqual(len(sdd_context.cube_link_dictionary), 2)
        self.assertIn('FINREP_LINK_001', sdd_context.cube_link_dictionary)
        self.assertIn('ANCRDT_LINK_001', sdd_context.cube_link_dictionary)

    def test_create_cube_links_with_finrep_filter(self):
        """Test that FINREP filter only loads FINREP cube links."""
        sdd_context = SDDContext()
        importer = ImportDatabaseToSDDModel()

        # Load with FINREP filter
        importer.create_cube_links(sdd_context, framework_id='FINREP_REF')

        # Should only have FINREP link
        self.assertEqual(len(sdd_context.cube_link_dictionary), 1)
        self.assertIn('FINREP_LINK_001', sdd_context.cube_link_dictionary)
        self.assertNotIn('ANCRDT_LINK_001', sdd_context.cube_link_dictionary)

    def test_create_cube_links_with_ancrdt_filter(self):
        """Test that ANCRDT filter only loads ANCRDT cube links."""
        sdd_context = SDDContext()
        importer = ImportDatabaseToSDDModel()

        # Load with ANCRDT filter
        importer.create_cube_links(sdd_context, framework_id='ANCRDT')

        # Should only have ANCRDT link
        self.assertEqual(len(sdd_context.cube_link_dictionary), 1)
        self.assertNotIn('FINREP_LINK_001', sdd_context.cube_link_dictionary)
        self.assertIn('ANCRDT_LINK_001', sdd_context.cube_link_dictionary)

    def test_import_sdd_for_joins_by_framework_finrep(self):
        """Test framework-filtered import for FINREP."""
        sdd_context = SDDContext()
        importer = ImportDatabaseToSDDModel()

        # Import for FINREP only
        importer.import_sdd_for_joins_by_framework(
            sdd_context,
            ['CUBE', 'CUBE_LINK'],
            'FINREP_REF'
        )

        # Should only have FINREP data
        self.assertEqual(len(sdd_context.bird_cube_dictionary), 1)
        self.assertIn('FINREP_CUBE_001', sdd_context.bird_cube_dictionary)
        self.assertEqual(len(sdd_context.cube_link_dictionary), 1)
        self.assertIn('FINREP_LINK_001', sdd_context.cube_link_dictionary)

    def test_import_sdd_for_joins_by_framework_ancrdt(self):
        """Test framework-filtered import for ANCRDT."""
        sdd_context = SDDContext()
        importer = ImportDatabaseToSDDModel()

        # Import for ANCRDT only
        importer.import_sdd_for_joins_by_framework(
            sdd_context,
            ['CUBE', 'CUBE_LINK'],
            'ANCRDT'
        )

        # Should only have ANCRDT data
        self.assertEqual(len(sdd_context.bird_cube_dictionary), 1)
        self.assertIn('ANCRDT_CUBE_001', sdd_context.bird_cube_dictionary)
        self.assertEqual(len(sdd_context.cube_link_dictionary), 1)
        self.assertIn('ANCRDT_LINK_001', sdd_context.cube_link_dictionary)


class TestConcurrentWorkflowIsolation(TransactionTestCase):
    """Test that workflows can run concurrently without interference."""

    reset_sequences = True

    def setUp(self):
        """Set up test environment."""
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

    def test_separate_sdd_context_instances(self):
        """Test that separate SDDContext instances don't share data."""
        # Create separate contexts for each framework
        finrep_context = SDDContext()
        finrep_context.current_framework = 'FINREP_REF'

        ancrdt_context = SDDContext()
        ancrdt_context.current_framework = 'ANCRDT'

        # Add data to FINREP context
        finrep_context.bird_cube_dictionary = {'FINREP_CUBE': 'data'}

        # Add data to ANCRDT context
        ancrdt_context.bird_cube_dictionary = {'ANCRDT_CUBE': 'data'}

        # Verify isolation
        self.assertIn('FINREP_CUBE', finrep_context.bird_cube_dictionary)
        self.assertNotIn('ANCRDT_CUBE', finrep_context.bird_cube_dictionary)
        self.assertIn('ANCRDT_CUBE', ancrdt_context.bird_cube_dictionary)
        self.assertNotIn('FINREP_CUBE', ancrdt_context.bird_cube_dictionary)

    def test_framework_context_attribute(self):
        """Test that current_framework attribute is properly set."""
        sdd_context = SDDContext()
        sdd_context.current_framework = 'FINREP_REF'

        self.assertEqual(sdd_context.current_framework, 'FINREP_REF')

    def test_sequential_framework_imports_dont_mix(self):
        """Test that sequential imports for different frameworks don't mix."""
        importer = ImportDatabaseToSDDModel()

        # Create some cubes
        finrep_structure = CUBE_STRUCTURE.objects.create(
            cube_structure_id='SEQ_FINREP_STRUCT',
            name='Sequential FINREP Structure'
        )
        ancrdt_structure = CUBE_STRUCTURE.objects.create(
            cube_structure_id='SEQ_ANCRDT_STRUCT',
            name='Sequential ANCRDT Structure'
        )

        CUBE.objects.create(
            cube_id='SEQ_FINREP_CUBE',
            name='Sequential FINREP Cube',
            framework_id=self.finrep_framework,
            cube_structure_id=finrep_structure
        )
        CUBE.objects.create(
            cube_id='SEQ_ANCRDT_CUBE',
            name='Sequential ANCRDT Cube',
            framework_id=self.ancrdt_framework,
            cube_structure_id=ancrdt_structure
        )

        # First, import FINREP
        finrep_context = SDDContext()
        importer.create_all_rol_cubes(finrep_context, framework_id='FINREP_REF')

        # Then, import ANCRDT (different context)
        ancrdt_context = SDDContext()
        importer.create_all_rol_cubes(ancrdt_context, framework_id='ANCRDT')

        # Verify each context has only its framework's data
        self.assertEqual(len(finrep_context.bird_cube_dictionary), 1)
        self.assertIn('SEQ_FINREP_CUBE', finrep_context.bird_cube_dictionary)

        self.assertEqual(len(ancrdt_context.bird_cube_dictionary), 1)
        self.assertIn('SEQ_ANCRDT_CUBE', ancrdt_context.bird_cube_dictionary)
