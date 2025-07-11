# coding=UTF-8
# Copyright (c) 2024 Bird Software Solutions Ltd
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDX-License-Identifier: EPL-2.0
#
# Contributors:
#    Neil Mackenzie - initial API and implementation

from django.test import TestCase, TransactionTestCase
from django.contrib.contenttypes.models import ContentType
from pybirdai.aorta_model import (
    Trail, MetaDataTrail, DatabaseTable, DerivedTable,
    DatabaseField, Function, FunctionText, TableCreationFunction,
    PopulatedDataBaseTable, EvaluatedDerivedTable, DatabaseRow,
    DerivedTableRow, DatabaseColumnValue, EvaluatedFunction,
    AortaTableReference, FunctionColumnReference, DerivedRowSourceReference,
    EvaluatedFunctionSourceValue, TableCreationSourceTable
)
from pybirdai.process_steps.pybird.orchestration import Orchestration
from pybirdai.annotations.decorators import lineage, set_lineage_orchestration


class AortaModelTests(TestCase):
    """Test AORTA model creation and relationships"""

    def setUp(self):
        """Set up test data"""
        # Create metadata trail and trail
        self.metadata_trail = MetaDataTrail.objects.create()
        self.trail = Trail.objects.create(
            name="Test Trail",
            metadata_trail=self.metadata_trail
        )

        # Create a database table
        self.db_table = DatabaseTable.objects.create(name="TestTable")
        self.db_field1 = DatabaseField.objects.create(
            name="field1",
            table=self.db_table
        )
        self.db_field2 = DatabaseField.objects.create(
            name="field2",
            table=self.db_table
        )

        # Add table reference to metadata trail
        AortaTableReference.objects.create(
            metadata_trail=self.metadata_trail,
            table_content_type='DatabaseTable',
            table_id=self.db_table.id
        )

    def test_trail_creation(self):
        """Test Trail and MetaDataTrail creation"""
        self.assertEqual(self.trail.name, "Test Trail")
        self.assertEqual(self.trail.metadata_trail, self.metadata_trail)
        self.assertIsNotNone(self.trail.created_at)

    def test_database_table_creation(self):
        """Test DatabaseTable and DatabaseField creation"""
        self.assertEqual(self.db_table.name, "TestTable")
        self.assertEqual(self.db_table.database_fields.count(), 2)
        self.assertEqual(self.db_field1.table, self.db_table)

    def test_derived_table_creation(self):
        """Test DerivedTable with functions"""
        # Create function text
        func_text = FunctionText.objects.create(
            text="return field1 + field2",
            language="python"
        )

        # Create derived table
        derived_table = DerivedTable.objects.create(name="DerivedTestTable")

        # Create function
        function = Function.objects.create(
            name="sum_fields",
            function_text=func_text,
            table=derived_table
        )

        # Test relationships
        self.assertEqual(derived_table.derived_functions.count(), 1)
        self.assertEqual(function.table, derived_table)
        self.assertEqual(function.function_text.text, "return field1 + field2")

    def test_populated_table_with_rows(self):
        """Test PopulatedDataBaseTable with rows and values"""
        # Create populated table
        pop_table = PopulatedDataBaseTable.objects.create(
            trail=self.trail,
            table=self.db_table
        )

        # Create rows
        row1 = DatabaseRow.objects.create(
            populated_table=pop_table,
            row_identifier="row_1"
        )
        row2 = DatabaseRow.objects.create(
            populated_table=pop_table,
            row_identifier="row_2"
        )

        # Create values
        val1 = DatabaseColumnValue.objects.create(
            value=100.0,
            column=self.db_field1,
            row=row1
        )
        val2 = DatabaseColumnValue.objects.create(
            value=200.0,
            column=self.db_field2,
            row=row1
        )

        # Test relationships
        self.assertEqual(pop_table.databaserow_set.count(), 2)
        self.assertEqual(row1.column_values.count(), 2)
        self.assertEqual(val1.column, self.db_field1)
        self.assertEqual(val1.value, 100.0)

    def test_function_column_references(self):
        """Test tracking column references in functions"""
        # Create derived table and function
        derived_table = DerivedTable.objects.create(name="DerivedTable")
        func_text = FunctionText.objects.create(text="sum function")
        function = Function.objects.create(
            name="sum_function",
            function_text=func_text,
            table=derived_table
        )

        # Create column reference
        col_ref = FunctionColumnReference.objects.create(
            function=function,
            content_type=ContentType.objects.get_for_model(DatabaseField),
            object_id=self.db_field1.id
        )

        # Test generic relation
        self.assertEqual(col_ref.referenced_column, self.db_field1)
        self.assertEqual(function.column_references.count(), 1)

    def test_evaluated_function_lineage(self):
        """Test EvaluatedFunction with source value tracking"""
        # Create derived table structure
        derived_table = DerivedTable.objects.create(name="ComputedTable")
        func_text = FunctionText.objects.create(text="compute")
        function = Function.objects.create(
            name="compute_value",
            function_text=func_text,
            table=derived_table
        )

        # Create populated structures
        pop_db_table = PopulatedDataBaseTable.objects.create(
            trail=self.trail,
            table=self.db_table
        )
        pop_derived_table = EvaluatedDerivedTable.objects.create(
            trail=self.trail,
            table=derived_table
        )

        # Create source row and values
        source_row = DatabaseRow.objects.create(populated_table=pop_db_table)
        source_val1 = DatabaseColumnValue.objects.create(
            value=10.0,
            column=self.db_field1,
            row=source_row
        )
        source_val2 = DatabaseColumnValue.objects.create(
            value=20.0,
            column=self.db_field2,
            row=source_row
        )

        # Create derived row
        derived_row = DerivedTableRow.objects.create(
            populated_table=pop_derived_table
        )

        # Track source row reference
        DerivedRowSourceReference.objects.create(
            derived_row=derived_row,
            content_type=ContentType.objects.get_for_model(DatabaseRow),
            object_id=source_row.id
        )

        # Create evaluated function
        eval_func = EvaluatedFunction.objects.create(
            value=30.0,  # 10 + 20
            function=function,
            row=derived_row
        )

        # Track source values
        EvaluatedFunctionSourceValue.objects.create(
            evaluated_function=eval_func,
            content_type=ContentType.objects.get_for_model(DatabaseColumnValue),
            object_id=source_val1.id
        )
        EvaluatedFunctionSourceValue.objects.create(
            evaluated_function=eval_func,
            content_type=ContentType.objects.get_for_model(DatabaseColumnValue),
            object_id=source_val2.id
        )

        # Test lineage
        self.assertEqual(eval_func.value, 30.0)
        self.assertEqual(eval_func.source_value_references.count(), 2)
        self.assertEqual(derived_row.source_row_references.count(), 1)


class AortaOrchestrationTests(TransactionTestCase):
    """Test AORTA integration with orchestration"""

    def setUp(self):
        """Set up orchestration with lineage"""
        self.orchestration = Orchestration()

    def test_orchestration_with_lineage(self):
        """Test orchestration creates AORTA trail"""
        # Create a mock table object
        class MockTable:
            pass

        mock_table = MockTable()
        mock_table.__class__.__name__ = "TestData_Table"

        # Initialize with lineage
        self.orchestration.init_with_lineage(mock_table, "Test Execution")

        # Check trail creation
        self.assertIsNotNone(self.orchestration.trail)
        self.assertEqual(self.orchestration.trail.name, "Test Execution")
        self.assertIsNotNone(self.orchestration.metadata_trail)

    def test_lineage_decorator(self):
        """Test lineage decorator functionality"""
        # Set up orchestration for decorator
        orchestration = Orchestration()
        orchestration.init_with_lineage(None, "Decorator Test")
        set_lineage_orchestration(orchestration)

        # Create a test class with lineage tracking
        class TestCalculation:
            def __init__(self):
                self.base_value = 100

            @lineage(dependencies={"base_value"})
            def calculate(self):
                return self.base_value * 2

        # Execute calculation
        calc = TestCalculation()
        result = calc.calculate()

        # Verify result
        self.assertEqual(result, 200)

        # Verify lineage was tracked (check orchestration state)
        self.assertTrue(orchestration.lineage_enabled)
        self.assertIsNotNone(orchestration.trail)

    def test_export_lineage_graph(self):
        """Test lineage graph export"""
        # Create test data
        metadata_trail = MetaDataTrail.objects.create()
        trail = Trail.objects.create(
            name="Graph Test",
            metadata_trail=metadata_trail
        )

        # Add some tables
        db_table = DatabaseTable.objects.create(name="SourceTable")
        derived_table = DerivedTable.objects.create(name="ComputedTable")

        # Add references
        AortaTableReference.objects.create(
            metadata_trail=metadata_trail,
            table_content_type='DatabaseTable',
            table_id=db_table.id
        )
        AortaTableReference.objects.create(
            metadata_trail=metadata_trail,
            table_content_type='DerivedTable',
            table_id=derived_table.id
        )

        # Export graph
        orchestration = Orchestration()
        graph = orchestration.export_lineage_graph(trail.id)

        # Verify graph structure
        self.assertIn('nodes', graph)
        self.assertIn('edges', graph)
        self.assertIn('trail', graph)
        self.assertEqual(len(graph['nodes']), 2)
        self.assertEqual(graph['trail']['name'], 'Graph Test')


class AortaAPITests(TestCase):
    """Test AORTA API endpoints"""

    def setUp(self):
        """Set up test data for API tests"""
        # Create trail
        metadata_trail = MetaDataTrail.objects.create()
        self.trail = Trail.objects.create(
            name="API Test Trail",
            metadata_trail=metadata_trail,
            execution_context={'test': True}
        )

        # Create table
        self.table = DatabaseTable.objects.create(name="APITestTable")
        AortaTableReference.objects.create(
            metadata_trail=metadata_trail,
            table_content_type='DatabaseTable',
            table_id=self.table.id
        )

    def test_trail_list_api(self):
        """Test trail list endpoint"""
        response = self.client.get('/pybirdai/api/aorta/trails/')
        self.assertEqual(response.status_code, 200)

        data = response.json()
        self.assertIn('trails', data)
        self.assertEqual(len(data['trails']), 1)
        self.assertEqual(data['trails'][0]['name'], 'API Test Trail')

    def test_trail_detail_api(self):
        """Test trail detail endpoint"""
        response = self.client.get(f'/pybirdai/api/aorta/trails/{self.trail.id}/')
        self.assertEqual(response.status_code, 200)

        data = response.json()
        self.assertEqual(data['trail']['name'], 'API Test Trail')
        self.assertEqual(len(data['tables']), 1)
        self.assertEqual(data['tables'][0]['name'], 'APITestTable')

    def test_lineage_graph_api(self):
        """Test lineage graph endpoint"""
        response = self.client.get(f'/pybirdai/api/aorta/trails/{self.trail.id}/graph/')
        self.assertEqual(response.status_code, 200)

        data = response.json()
        self.assertIn('nodes', data)
        self.assertIn('edges', data)
        self.assertIn('trail', data)
