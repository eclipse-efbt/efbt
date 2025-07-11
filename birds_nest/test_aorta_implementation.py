#!/usr/bin/env python3
"""
Test script to verify the enhanced AORTA lineage tracking implementation
"""

import os
import sys
import django
from django.conf import settings

# Add the project root to the path
sys.path.insert(0, '/home/neil/development/cocalimo/efbt/birds_nest')

# Configure Django settings
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'birds_nest.settings')
django.setup()

from pybirdai.process_steps.pybird.orchestration import Orchestration

from pybirdai.aorta_model import *

from pybirdai.annotations.decorators import lineage, track_table_init, set_lineage_orchestration


class TestTable:
    """Mock table class for testing"""
    def __init__(self):
        self.data_items = []
        self.CRRYNG_AMNT = 1000
        self.ACCNTNG_CLSSFCTN = "ASSET"

    @track_table_init
    def init(self):
        """Initialize the test table"""
        # Simulate data population
        self.data_items = [
            {'CRRYNG_AMNT': 1000, 'ACCNTNG_CLSSFCTN': 'ASSET'},
            {'CRRYNG_AMNT': 2000, 'ACCNTNG_CLSSFCTN': 'LIABILITY'},
            {'CRRYNG_AMNT': 1500, 'ACCNTNG_CLSSFCTN': 'EQUITY'}
        ]
        print(f"TestTable initialized with {len(self.data_items)} items")


class TestComputedTable:
    """Mock computed table for testing"""
    def __init__(self):
        self.base_table = None
        self.computed_values = []

    @lineage(dependencies={"base_table.CRRYNG_AMNT"})
    def compute_total(self):
        """Compute total carrying amount"""
        if self.base_table and hasattr(self.base_table, 'data_items'):
            total = sum(item['CRRYNG_AMNT'] for item in self.base_table.data_items)
            return total
        return 0

    @lineage(dependencies={"base_table.data_items"})
    def metric_value(self):
        """Compute metric value"""
        return self.compute_total() * 1.1


def test_aorta_implementation():
    """Test the enhanced AORTA implementation"""
    print("=" * 60)
    print("Testing Enhanced AORTA Lineage Tracking Implementation")
    print("=" * 60)


    # Clear any existing data
    Trail.objects.all().delete()
    MetaDataTrail.objects.all().delete()

    # Create orchestration
    orchestration = Orchestration()

    # Test 1: Initialize with lineage
    print("\n1. Testing table initialization with lineage...")

    # Create a mock table object
    mock_table = TestTable()
    mock_table.__class__.__name__ = "TestData_Table"

    # Initialize with lineage
    orchestration.init_with_lineage(mock_table, "Test Enhanced AORTA")
    mock_table.init()


    # Check what was created
    print(f"Created Trail: {Trail.objects.count()}")
    print(f"Created MetaDataTrail: {MetaDataTrail.objects.count()}")
    print(f"Created DatabaseTable: {DatabaseTable.objects.count()}")
    print(f"Created PopulatedDataBaseTable: {PopulatedDataBaseTable.objects.count()}")
    print(f"Created DatabaseField: {DatabaseField.objects.count()}")
    print(f"Created DatabaseRow: {DatabaseRow.objects.count()}")
    print(f"Created DatabaseColumnValue: {DatabaseColumnValue.objects.count()}")


    # Test 2: Function execution tracking
    print("\n2. Testing function execution tracking...")

    computed_table = TestComputedTable()
    computed_table.base_table = mock_table

    # Execute computation
    result = computed_table.compute_total()
    print(f"Computed total: {result}")

    # Execute metric computation
    metric_result = computed_table.metric_value()
    print(f"Computed metric: {metric_result}")


    # Check function tracking
    print(f"Created DerivedTable: {DerivedTable.objects.count()}")
    print(f"Created Function: {Function.objects.count()}")
    print(f"Created FunctionText: {FunctionText.objects.count()}")
    print(f"Created FunctionColumnReference: {FunctionColumnReference.objects.count()}")


    # Test 3: Export lineage graph
    print("\n3. Testing lineage graph export...")


    trail = orchestration.get_lineage_trail()
    if trail:
        graph = orchestration.export_lineage_graph(trail.id)
        if graph:
            print(f"Graph nodes: {len(graph.get('nodes', []))}")
            print(f"Graph edges: {len(graph.get('edges', []))}")
            print(f"Trail info: {graph.get('trail', {}).get('name', 'Unknown')}")
        else:
            print("No graph generated")


    # Test 4: Show detailed results
    print("\n4. Detailed Results:")
    print("-" * 40)

    for trail in Trail.objects.all():
        print(f"Trail: {trail.name} (ID: {trail.id})")
        print(f"  Created: {trail.created_at}")


        if trail.metadata_trail:
            print(f"  MetaDataTrail ID: {trail.metadata_trail.id}")
            for table_ref in trail.metadata_trail.table_references.all():
                print(f"    Table Reference: {table_ref.table_content_type} (ID: {table_ref.table_id})")

        pop_tables = PopulatedDataBaseTable.objects.filter(trail=trail)
        for pop_table in pop_tables:
            print(f"  PopulatedTable: {pop_table.table.name}")
            print(f"    Rows: {pop_table.databaserow_set.count()}")
            for row in pop_table.databaserow_set.all():
                print(f"      Row {row.row_identifier}: {row.column_values.count()} values")

    for derived_table in DerivedTable.objects.all():
        print(f"DerivedTable: {derived_table.name}")
        print(f"  Functions: {derived_table.derived_functions.count()}")
        for func in derived_table.derived_functions.all():
            print(f"    Function: {func.name}")
            print(f"      References: {func.column_references.count()}")

    print("\n" + "=" * 60)
    print("Test completed successfully!")
    print("=" * 60)


if __name__ == "__main__":
    test_aorta_implementation()
