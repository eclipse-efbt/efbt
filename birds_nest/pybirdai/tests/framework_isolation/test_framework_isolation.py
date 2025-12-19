# coding=UTF-8
# Copyright (c) 2025 Bird Software Solutions Ltd
# Tests for framework isolation - verifying workflows don't interfere with each other

import os
import sys
from pathlib import Path

# Configure Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'birds_nest.settings')

import django
django.setup()

from django.test import TransactionTestCase
from django.db import connection

from pybirdai.models.bird_meta_data_model import (
    TABLE_CELL, ORDINATE_ITEM, CELL_POSITION, TABLE, AXIS, AXIS_ORDINATE,
    FRAMEWORK, CUBE, CUBE_LINK, CUBE_STRUCTURE, CUBE_STRUCTURE_ITEM,
    CUBE_STRUCTURE_ITEM_LINK, MEMBER_LINK, MAINTENANCE_AGENCY
)
from pybirdai.models.bird_meta_data_model_extension import FRAMEWORK_TABLE


class TestFrameworkFilteredDeletion(TransactionTestCase):
    """Test that framework-filtered deletion works correctly."""

    reset_sequences = True

    def setUp(self):
        """Set up test data with multiple frameworks."""
        # Create maintenance agency
        self.agency, _ = MAINTENANCE_AGENCY.objects.get_or_create(
            maintenance_agency_id='EBA',
            defaults={'name': 'European Banking Authority'}
        )

        # Create frameworks
        self.finrep_fw, _ = FRAMEWORK.objects.get_or_create(
            framework_id='EBA_FINREP',
            defaults={'name': 'Financial Reporting', 'maintenance_agency_id': self.agency}
        )
        self.corep_fw, _ = FRAMEWORK.objects.get_or_create(
            framework_id='EBA_COREP',
            defaults={'name': 'Common Reporting', 'maintenance_agency_id': self.agency}
        )

        # Create tables for each framework
        self.finrep_table = TABLE.objects.create(
            table_id='FINREP_TABLE_001',
            code='F_01_01',
            name='FINREP Test Table'
        )
        self.corep_table = TABLE.objects.create(
            table_id='COREP_TABLE_001',
            code='C_01_00',
            name='COREP Test Table'
        )

        # Link tables to frameworks
        FRAMEWORK_TABLE.objects.create(
            framework_id=self.finrep_fw,
            table_id=self.finrep_table
        )
        FRAMEWORK_TABLE.objects.create(
            framework_id=self.corep_fw,
            table_id=self.corep_table
        )

        # Create table cells for each framework
        for i in range(5):
            TABLE_CELL.objects.create(
                cell_id=f'FINREP_CELL_{i:03d}',
                table_id=self.finrep_table,
                table_cell_combination_id=f'FINREP_COMB_{i:03d}'
            )
            TABLE_CELL.objects.create(
                cell_id=f'COREP_CELL_{i:03d}',
                table_id=self.corep_table,
                table_cell_combination_id=f'COREP_COMB_{i:03d}'
            )

    def test_framework_filtered_delete_table_cell(self):
        """Test that deleting FINREP cells doesn't affect COREP cells."""
        from pybirdai.process_steps.website_to_sddmodel.import_func.csv_copy_importer import (
            get_framework_filtered_delete_sql
        )

        # Verify initial state
        self.assertEqual(TABLE_CELL.objects.filter(cell_id__startswith='FINREP').count(), 5)
        self.assertEqual(TABLE_CELL.objects.filter(cell_id__startswith='COREP').count(), 5)

        # Get framework-filtered DELETE SQL
        sql, params = get_framework_filtered_delete_sql('pybirdai_table_cell', 'EBA_FINREP')

        # Execute the delete
        with connection.cursor() as cursor:
            cursor.execute(sql, params)
            deleted_count = cursor.rowcount

        # Verify FINREP cells are deleted
        self.assertEqual(TABLE_CELL.objects.filter(cell_id__startswith='FINREP').count(), 0)

        # Verify COREP cells are PRESERVED
        self.assertEqual(TABLE_CELL.objects.filter(cell_id__startswith='COREP').count(), 5)

    def test_framework_filtered_delete_returns_none_without_framework(self):
        """Test that no SQL is returned when framework is None."""
        from pybirdai.process_steps.website_to_sddmodel.import_func.csv_copy_importer import (
            get_framework_filtered_delete_sql
        )

        sql, params = get_framework_filtered_delete_sql('pybirdai_table_cell', None)

        self.assertIsNone(sql)
        self.assertIsNone(params)


class TestWorkflowIsolation(TransactionTestCase):
    """Test that different workflows don't interfere with each other."""

    reset_sequences = True

    def setUp(self):
        """Set up test data for multiple workflows."""
        # Create maintenance agencies
        self.eba_agency, _ = MAINTENANCE_AGENCY.objects.get_or_create(
            maintenance_agency_id='EBA',
            defaults={'name': 'European Banking Authority'}
        )
        self.ecb_agency, _ = MAINTENANCE_AGENCY.objects.get_or_create(
            maintenance_agency_id='ECB',
            defaults={'name': 'European Central Bank'}
        )

        # Create frameworks for different workflows
        self.finrep_fw, _ = FRAMEWORK.objects.get_or_create(
            framework_id='EBA_FINREP',
            defaults={'name': 'Financial Reporting (DPM)', 'maintenance_agency_id': self.eba_agency}
        )
        self.ancrdt_fw, _ = FRAMEWORK.objects.get_or_create(
            framework_id='ANCRDT',
            defaults={'name': 'AnaCredit', 'maintenance_agency_id': self.ecb_agency}
        )
        self.bird_fw, _ = FRAMEWORK.objects.get_or_create(
            framework_id='BIRD_EIL',
            defaults={'name': 'BIRD EIL (MAIN)', 'maintenance_agency_id': self.eba_agency}
        )

    def test_cube_isolation_by_framework(self):
        """Test that cubes are isolated by framework."""
        from pybirdai.models.bird_meta_data_model import CUBE_STRUCTURE

        # Create cube structures
        dpm_structure = CUBE_STRUCTURE.objects.create(
            cube_structure_id='DPM_STRUCT_001',
            name='DPM Structure'
        )
        ancrdt_structure = CUBE_STRUCTURE.objects.create(
            cube_structure_id='ANCRDT_STRUCT_001',
            name='ANCRDT Structure'
        )
        main_structure = CUBE_STRUCTURE.objects.create(
            cube_structure_id='MAIN_STRUCT_001',
            name='MAIN Structure'
        )

        # Create cubes for each framework
        CUBE.objects.create(
            cube_id='DPM_CUBE_001',
            name='DPM Cube',
            framework_id=self.finrep_fw,
            cube_structure_id=dpm_structure
        )
        CUBE.objects.create(
            cube_id='ANCRDT_CUBE_001',
            name='ANCRDT Cube',
            framework_id=self.ancrdt_fw,
            cube_structure_id=ancrdt_structure
        )
        CUBE.objects.create(
            cube_id='MAIN_CUBE_001',
            name='MAIN Cube',
            framework_id=self.bird_fw,
            cube_structure_id=main_structure
        )

        # Verify each framework has its own cube
        self.assertEqual(CUBE.objects.filter(framework_id=self.finrep_fw).count(), 1)
        self.assertEqual(CUBE.objects.filter(framework_id=self.ancrdt_fw).count(), 1)
        self.assertEqual(CUBE.objects.filter(framework_id=self.bird_fw).count(), 1)

        # Delete DPM cube (simulate workflow re-run)
        CUBE.objects.filter(framework_id=self.finrep_fw).delete()

        # Verify only DPM cube is deleted
        self.assertEqual(CUBE.objects.filter(framework_id=self.finrep_fw).count(), 0)
        self.assertEqual(CUBE.objects.filter(framework_id=self.ancrdt_fw).count(), 1)
        self.assertEqual(CUBE.objects.filter(framework_id=self.bird_fw).count(), 1)

    def test_domain_member_variable_accumulate(self):
        """Test that DOMAIN, MEMBER, VARIABLE use accumulate strategy (ignore_conflicts)."""
        from pybirdai.models.bird_meta_data_model import DOMAIN, MEMBER, VARIABLE

        # Create initial domains
        DOMAIN.objects.bulk_create([
            DOMAIN(domain_id='DOM_FINREP_001', name='FINREP Domain'),
            DOMAIN(domain_id='DOM_SHARED_001', name='Shared Domain'),
        ], ignore_conflicts=True)

        initial_count = DOMAIN.objects.count()

        # Simulate another workflow importing overlapping data
        DOMAIN.objects.bulk_create([
            DOMAIN(domain_id='DOM_SHARED_001', name='Shared Domain Updated'),  # Conflict
            DOMAIN(domain_id='DOM_COREP_001', name='COREP Domain'),  # New
        ], ignore_conflicts=True)

        # Verify accumulation: shared domain not duplicated, new domain added
        self.assertEqual(DOMAIN.objects.count(), initial_count + 1)  # Only DOM_COREP_001 added
        self.assertEqual(DOMAIN.objects.filter(domain_id='DOM_SHARED_001').count(), 1)
        self.assertEqual(DOMAIN.objects.filter(domain_id='DOM_COREP_001').count(), 1)


class TestDatasetConfigFramework(TransactionTestCase):
    """Test that DatasetConfig correctly handles framework parameter."""

    def test_config_stores_framework(self):
        """Test that DatasetConfig stores the framework parameter."""
        from pybirdai.process_steps.website_to_sddmodel.import_func.config import DatasetConfig

        config = DatasetConfig(dataset_type='dpm', framework='EBA_FINREP')

        self.assertEqual(config.framework, 'EBA_FINREP')
        self.assertEqual(config.dataset_type, 'dpm')

    def test_config_framework_defaults_to_none(self):
        """Test that DatasetConfig defaults framework to None."""
        from pybirdai.process_steps.website_to_sddmodel.import_func.config import DatasetConfig

        config = DatasetConfig(dataset_type='finrep')

        self.assertIsNone(config.framework)

    def test_config_stores_frameworks_list(self):
        """Test that DatasetConfig stores the frameworks list parameter."""
        from pybirdai.process_steps.website_to_sddmodel.import_func.config import DatasetConfig

        config = DatasetConfig(dataset_type='dpm', frameworks=['BIRD', 'EBA_FINREP', 'EBA_COREP'])

        self.assertEqual(config.frameworks, ['BIRD', 'EBA_FINREP', 'EBA_COREP'])

    def test_get_frameworks_list_from_frameworks(self):
        """Test get_frameworks_list returns frameworks list when set."""
        from pybirdai.process_steps.website_to_sddmodel.import_func.config import DatasetConfig

        config = DatasetConfig(dataset_type='dpm', frameworks=['BIRD', 'ANCRDT'])

        self.assertEqual(config.get_frameworks_list(), ['BIRD', 'ANCRDT'])

    def test_get_frameworks_list_from_single_framework(self):
        """Test get_frameworks_list returns list with single framework."""
        from pybirdai.process_steps.website_to_sddmodel.import_func.config import DatasetConfig

        config = DatasetConfig(dataset_type='dpm', framework='EBA_FINREP')

        self.assertEqual(config.get_frameworks_list(), ['EBA_FINREP'])

    def test_get_frameworks_list_returns_none_when_empty(self):
        """Test get_frameworks_list returns None when no framework set."""
        from pybirdai.process_steps.website_to_sddmodel.import_func.config import DatasetConfig

        config = DatasetConfig(dataset_type='finrep')

        self.assertIsNone(config.get_frameworks_list())


class TestMultiFrameworkDeletion(TransactionTestCase):
    """Test that multi-framework deletion works correctly."""

    reset_sequences = True

    def setUp(self):
        """Set up test data with multiple frameworks."""
        # Create maintenance agency
        self.agency, _ = MAINTENANCE_AGENCY.objects.get_or_create(
            maintenance_agency_id='TEST',
            defaults={'name': 'Test Agency'}
        )

        # Create frameworks
        self.bird_fw, _ = FRAMEWORK.objects.get_or_create(
            framework_id='BIRD',
            defaults={'name': 'BIRD Framework', 'maintenance_agency_id': self.agency}
        )
        self.finrep_fw, _ = FRAMEWORK.objects.get_or_create(
            framework_id='EBA_FINREP',
            defaults={'name': 'Financial Reporting', 'maintenance_agency_id': self.agency}
        )
        self.ancrdt_fw, _ = FRAMEWORK.objects.get_or_create(
            framework_id='ANCRDT',
            defaults={'name': 'AnaCredit', 'maintenance_agency_id': self.agency}
        )

        # Create tables for each framework
        self.bird_table = TABLE.objects.create(
            table_id='BIRD_TABLE_001',
            code='BIRD_T1',
            name='BIRD Test Table'
        )
        self.finrep_table = TABLE.objects.create(
            table_id='FINREP_TABLE_001',
            code='F_01_01',
            name='FINREP Test Table'
        )
        self.ancrdt_table = TABLE.objects.create(
            table_id='ANCRDT_TABLE_001',
            code='ANCRDT_T1',
            name='ANCRDT Test Table'
        )

        # Link tables to frameworks
        FRAMEWORK_TABLE.objects.create(framework_id=self.bird_fw, table_id=self.bird_table)
        FRAMEWORK_TABLE.objects.create(framework_id=self.finrep_fw, table_id=self.finrep_table)
        FRAMEWORK_TABLE.objects.create(framework_id=self.ancrdt_fw, table_id=self.ancrdt_table)

        # Create table cells
        for i in range(3):
            TABLE_CELL.objects.create(
                cell_id=f'BIRD_CELL_{i:03d}',
                table_id=self.bird_table,
                table_cell_combination_id=f'BIRD_COMB_{i:03d}'
            )
            TABLE_CELL.objects.create(
                cell_id=f'FINREP_CELL_{i:03d}',
                table_id=self.finrep_table,
                table_cell_combination_id=f'FINREP_COMB_{i:03d}'
            )
            TABLE_CELL.objects.create(
                cell_id=f'ANCRDT_CELL_{i:03d}',
                table_id=self.ancrdt_table,
                table_cell_combination_id=f'ANCRDT_COMB_{i:03d}'
            )

    def test_multi_framework_delete_preserves_other_workflows(self):
        """Test that deleting BIRD+FINREP cells preserves ANCRDT cells."""
        from pybirdai.process_steps.website_to_sddmodel.import_func.csv_copy_importer import (
            get_framework_filtered_delete_sql
        )

        # Verify initial state
        self.assertEqual(TABLE_CELL.objects.filter(cell_id__startswith='BIRD').count(), 3)
        self.assertEqual(TABLE_CELL.objects.filter(cell_id__startswith='FINREP').count(), 3)
        self.assertEqual(TABLE_CELL.objects.filter(cell_id__startswith='ANCRDT').count(), 3)

        # Delete BIRD + FINREP cells (simulating DPM workflow with multi-framework)
        sql, params = get_framework_filtered_delete_sql('pybirdai_table_cell', ['BIRD', 'EBA_FINREP'])

        with connection.cursor() as cursor:
            cursor.execute(sql, params)

        # Verify BIRD and FINREP cells are deleted
        self.assertEqual(TABLE_CELL.objects.filter(cell_id__startswith='BIRD').count(), 0)
        self.assertEqual(TABLE_CELL.objects.filter(cell_id__startswith='FINREP').count(), 0)

        # Verify ANCRDT cells are PRESERVED
        self.assertEqual(TABLE_CELL.objects.filter(cell_id__startswith='ANCRDT').count(), 3)

    def test_single_string_framework_converted_to_list(self):
        """Test that a single framework string is converted to list in SQL."""
        from pybirdai.process_steps.website_to_sddmodel.import_func.csv_copy_importer import (
            get_framework_filtered_delete_sql
        )

        # Pass single string - should work the same as list with one element
        sql, params = get_framework_filtered_delete_sql('pybirdai_table_cell', 'EBA_FINREP')

        self.assertIsNotNone(sql)
        self.assertEqual(params, ['EBA_FINREP'])


class TestCubeAndLinkFrameworkIsolation(TransactionTestCase):
    """Test that cube and link entities support framework-filtered deletion."""

    reset_sequences = True

    def setUp(self):
        """Set up test data with cubes and links for multiple frameworks."""
        # Create maintenance agency
        self.agency, _ = MAINTENANCE_AGENCY.objects.get_or_create(
            maintenance_agency_id='TEST',
            defaults={'name': 'Test Agency'}
        )

        # Create frameworks
        self.finrep_fw, _ = FRAMEWORK.objects.get_or_create(
            framework_id='EBA_FINREP',
            defaults={'name': 'Financial Reporting', 'maintenance_agency_id': self.agency}
        )
        self.ancrdt_fw, _ = FRAMEWORK.objects.get_or_create(
            framework_id='ANCRDT',
            defaults={'name': 'AnaCredit', 'maintenance_agency_id': self.agency}
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

        # Create cubes linked to frameworks
        self.finrep_cube = CUBE.objects.create(
            cube_id='FINREP_CUBE_001',
            name='FINREP Cube',
            framework_id=self.finrep_fw,
            cube_structure_id=self.finrep_structure
        )
        self.ancrdt_cube = CUBE.objects.create(
            cube_id='ANCRDT_CUBE_001',
            name='ANCRDT Cube',
            framework_id=self.ancrdt_fw,
            cube_structure_id=self.ancrdt_structure
        )

        # Create cube links
        self.finrep_cube_link = CUBE_LINK.objects.create(
            cube_link_id='FINREP_LINK_001',
            name='FINREP Link',
            foreign_cube_id=self.finrep_cube
        )
        self.ancrdt_cube_link = CUBE_LINK.objects.create(
            cube_link_id='ANCRDT_LINK_001',
            name='ANCRDT Link',
            foreign_cube_id=self.ancrdt_cube
        )

    def test_cube_framework_filtered_deletion(self):
        """Test that deleting FINREP cubes preserves ANCRDT cubes."""
        from pybirdai.process_steps.website_to_sddmodel.import_func.csv_copy_importer import (
            get_framework_filtered_delete_sql
        )

        # Verify initial state
        self.assertEqual(CUBE.objects.filter(cube_id__startswith='FINREP').count(), 1)
        self.assertEqual(CUBE.objects.filter(cube_id__startswith='ANCRDT').count(), 1)

        with connection.cursor() as cursor:
            # Disable FK checks for deletion test (SQLite requires this)
            cursor.execute("PRAGMA foreign_keys = 0;")

            # First delete CUBE_LINK (child) before CUBE (parent)
            link_sql, link_params = get_framework_filtered_delete_sql('pybirdai_cube_link', 'EBA_FINREP')
            cursor.execute(link_sql, link_params)

            # Get and execute framework-filtered DELETE for FINREP cubes
            sql, params = get_framework_filtered_delete_sql('pybirdai_cube', 'EBA_FINREP')
            self.assertIsNotNone(sql)
            cursor.execute(sql, params)

            cursor.execute("PRAGMA foreign_keys = 1;")

        # Verify FINREP cube is deleted
        self.assertEqual(CUBE.objects.filter(cube_id__startswith='FINREP').count(), 0)

        # Verify ANCRDT cube is PRESERVED
        self.assertEqual(CUBE.objects.filter(cube_id__startswith='ANCRDT').count(), 1)

    def test_cube_structure_framework_filtered_deletion(self):
        """Test that deleting FINREP cube structures preserves ANCRDT cube structures."""
        from pybirdai.process_steps.website_to_sddmodel.import_func.csv_copy_importer import (
            get_framework_filtered_delete_sql
        )

        # Verify initial state
        self.assertEqual(CUBE_STRUCTURE.objects.filter(cube_structure_id__startswith='FINREP').count(), 1)
        self.assertEqual(CUBE_STRUCTURE.objects.filter(cube_structure_id__startswith='ANCRDT').count(), 1)

        with connection.cursor() as cursor:
            # Disable FK checks for deletion test (SQLite requires this)
            cursor.execute("PRAGMA foreign_keys = 0;")

            # Delete CUBE_LINK first (depends on CUBE)
            link_sql, link_params = get_framework_filtered_delete_sql('pybirdai_cube_link', 'EBA_FINREP')
            cursor.execute(link_sql, link_params)

            # Delete CUBE_STRUCTURE BEFORE CUBE (SQL uses CUBE to find structures)
            sql, params = get_framework_filtered_delete_sql('pybirdai_cube_structure', 'EBA_FINREP')
            self.assertIsNotNone(sql)
            cursor.execute(sql, params)

            # Delete CUBE last (after structure is deleted)
            cube_sql, cube_params = get_framework_filtered_delete_sql('pybirdai_cube', 'EBA_FINREP')
            cursor.execute(cube_sql, cube_params)

            cursor.execute("PRAGMA foreign_keys = 1;")

        # Verify FINREP cube structure is deleted
        self.assertEqual(CUBE_STRUCTURE.objects.filter(cube_structure_id__startswith='FINREP').count(), 0)

        # Verify ANCRDT cube structure is PRESERVED
        self.assertEqual(CUBE_STRUCTURE.objects.filter(cube_structure_id__startswith='ANCRDT').count(), 1)

    def test_cube_link_framework_filtered_deletion(self):
        """Test that deleting FINREP cube links preserves ANCRDT cube links."""
        from pybirdai.process_steps.website_to_sddmodel.import_func.csv_copy_importer import (
            get_framework_filtered_delete_sql
        )

        # Verify initial state
        self.assertEqual(CUBE_LINK.objects.filter(cube_link_id__startswith='FINREP').count(), 1)
        self.assertEqual(CUBE_LINK.objects.filter(cube_link_id__startswith='ANCRDT').count(), 1)

        # Get and execute framework-filtered DELETE for FINREP cube links
        sql, params = get_framework_filtered_delete_sql('pybirdai_cube_link', 'EBA_FINREP')
        self.assertIsNotNone(sql)

        with connection.cursor() as cursor:
            cursor.execute(sql, params)

        # Verify FINREP cube link is deleted
        self.assertEqual(CUBE_LINK.objects.filter(cube_link_id__startswith='FINREP').count(), 0)

        # Verify ANCRDT cube link is PRESERVED
        self.assertEqual(CUBE_LINK.objects.filter(cube_link_id__startswith='ANCRDT').count(), 1)

    def test_all_cube_entities_return_valid_sql(self):
        """Test that all cube/link entity types return valid SQL."""
        from pybirdai.process_steps.website_to_sddmodel.import_func.csv_copy_importer import (
            get_framework_filtered_delete_sql
        )

        # All cube and link entities should return valid SQL
        entities = [
            'pybirdai_cube',
            'pybirdai_cube_structure',
            'pybirdai_cube_structure_item',
            'pybirdai_cube_link',
            'pybirdai_cube_structure_item_link',
            'pybirdai_member_link',
        ]

        for entity in entities:
            sql, params = get_framework_filtered_delete_sql(entity, 'EBA_FINREP')
            self.assertIsNotNone(sql, f"SQL should not be None for {entity}")
            self.assertEqual(params, ['EBA_FINREP'], f"Params incorrect for {entity}")


if __name__ == '__main__':
    import unittest
    unittest.main()
