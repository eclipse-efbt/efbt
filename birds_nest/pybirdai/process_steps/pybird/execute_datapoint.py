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
from pybirdai.process_steps.filter_code.report_cells import *
import importlib
import os
from datetime import datetime
from django.conf import settings
from django.db import transaction
from contextlib import contextmanager
from contextvars import ContextVar

_lineage_cleanup_state = ContextVar('pybirdai_lineage_cleanup_state', default=None)


@contextmanager
def lineage_file_cleanup_scope(cleaned=False):
    """Clean lineage CSV output at most once for a batch of datapoint executions."""
    token = _lineage_cleanup_state.set({'done': cleaned})
    try:
        yield
    finally:
        _lineage_cleanup_state.reset(token)

class ExecuteDataPoint:
    @transaction.atomic
    def execute_data_point(data_point_id):
        cleanup_state = _lineage_cleanup_state.get()
        if cleanup_state is None:
            ExecuteDataPoint.delete_lineage_data()
        elif not cleanup_state.get('done'):
            ExecuteDataPoint.delete_lineage_data()
            cleanup_state['done'] = True
        debug_lineage = os.environ.get('PYBIRDAI_DEBUG_LINEAGE', '').lower() in {'1', 'true', 'yes', 'on'}
        print(f"Executing data point with ID: {data_point_id}")

        # Set up AORTA lineage tracking
        from pybirdai.process_steps.pybird.orchestration import Orchestration, OrchestrationWithLineage
        from pybirdai.annotations.decorators import set_lineage_orchestration
        from pybirdai.context.context import Context
        from pybirdai.process_steps.pybird.lineage_collector import reset_collector

        # Reset the lineage collector for this new execution
        reset_collector()

        # Create orchestration based on configuration
        orchestration = Orchestration()
        execution_name = f"DataPoint_{data_point_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        # Only set up lineage if using the lineage-enhanced orchestrator and lineage is enabled
        if isinstance(orchestration, OrchestrationWithLineage) and orchestration.lineage_enabled:
            # Initialize the trail and metadata without dummy objects
            orchestration.trail = None
            orchestration.metadata_trail = None
            orchestration.current_populated_tables = {}
            orchestration.current_rows = {}

            # Create trail directly
            from pybirdai.models import MetaDataTrail, Trail
            orchestration.metadata_trail = MetaDataTrail.objects.create()
            orchestration.trail = Trail.objects.create(
                name=execution_name,
                metadata_trail=orchestration.metadata_trail
            )
            orchestration._trail_is_new = True
            print(f"Created AORTA Trail: {orchestration.trail.name}")

            # Set the global lineage context
            set_lineage_orchestration(orchestration)
        elif isinstance(orchestration, OrchestrationWithLineage):
            print(f"Using lineage orchestrator but lineage tracking is disabled in config")
            # Clear the global lineage context since lineage is disabled
            set_lineage_orchestration(None)
        else:
            print(f"Using original orchestrator - lineage tracking disabled")
            # Clear the global lineage context since we're using original orchestrator
            set_lineage_orchestration(None)

        # Initialize with lineage tracking
        klass = globals()['Cell_' + str(data_point_id)]
        datapoint = klass()

        # Set calculation context early if lineage is enabled
        if isinstance(orchestration, OrchestrationWithLineage) and orchestration.lineage_enabled:
            calculation_name = datapoint.__class__.__name__
            orchestration.current_calculation = calculation_name
            print(f"Set calculation context: {calculation_name}")

            if debug_lineage:
                from pybirdai.api.debug_tracking import add_debug_to_orchestration
                add_debug_to_orchestration(orchestration)

            # Start a calculation chain to track the full computation
            # Parse output table name from cell class name
            output_table = ""
            if calculation_name.startswith('Cell_'):
                parts = calculation_name.split('_')
                if len(parts) >= 7:
                    # e.g., Cell_F_01_01_REF_FINREP_3_0_12345 -> F_01_01_REF_FINREP_3_0
                    output_table = '_'.join(parts[1:8]) if len(parts) > 7 else '_'.join(parts[1:])

            orchestration.start_calculation_chain(
                chain_name=calculation_name,
                output_table=output_table,
                output_cell=calculation_name
            )

            # Set calculation context BEFORE initialization to capture all function calls
            orchestration.current_calculation = calculation_name
            print(f"Set orchestration context to: {calculation_name}")

            # CRITICAL FIX: Apply wrapper BEFORE init() so calc_referenced_items is wrapped when called
            from pybirdai.process_steps.pybird.automatic_tracking_wrapper import create_smart_tracking_wrapper
            datapoint = create_smart_tracking_wrapper(datapoint, orchestration)
            print(f"Added automatic tracking wrapper to {calculation_name}")

        # Execute the datapoint (now init() will call the wrapped calc_referenced_items)
        datapoint.init()
        metric_value = str(datapoint.metric_value())

        # Finalize lineage and print summary
        if isinstance(orchestration, OrchestrationWithLineage) and orchestration.lineage_enabled:
            # End the calculation chain if one was started
            if hasattr(orchestration, '_current_calculation_chain') and orchestration._current_calculation_chain:
                try:
                    # Count contributing rows
                    from pybirdai.models import CalculationUsedRow
                    contributing_rows = CalculationUsedRow.objects.filter(
                        trail=orchestration.trail,
                        calculation_name=orchestration.current_calculation
                    ).count()

                    orchestration.end_calculation_chain(
                        final_value=float(metric_value) if metric_value.replace('.', '').replace('-', '').isdigit() else None,
                        final_string_value=metric_value,
                        total_contributing_rows=contributing_rows
                    )
                except Exception as e:
                    print(f"Error ending calculation chain: {e}")

            # Track cell lineage for the output
            try:
                # Parse cell info from datapoint class name
                class_name = datapoint.__class__.__name__
                if class_name.startswith('Cell_'):
                    parts = class_name.split('_')
                    if len(parts) >= 7:
                        report_template = f"F_{parts[2]}.{parts[3]}"
                        framework = parts[5] if len(parts) > 5 else 'FINREP'
                        orchestration.track_cell_lineage(
                            report_template=report_template,
                            cell_code=class_name,
                            computed_value=float(metric_value) if metric_value.replace('.', '').replace('-', '').isdigit() else metric_value,
                            framework=framework
                        )
            except Exception as e:
                print(f"Error tracking cell lineage: {e}")

            # Finalize lineage - ensure all relationships are created
            orchestration.finalize_lineage()

            trail = orchestration.get_lineage_trail()
            if trail and debug_lineage:
                print(f"\n=== AORTA Lineage Summary ===")
                print(f"Trail: {trail.name} (ID: {trail.id})")
                from pybirdai.models import (
                    DatabaseTable, PopulatedDataBaseTable, DatabaseField, DatabaseRow,
                    DerivedTable, EvaluatedDerivedTable, DerivedTableRow,
                    Function, EvaluatedFunction,
                    CalculationUsedRow, CalculationUsedField,
                    FunctionColumnReference, DerivedRowSourceReference,
                    EvaluatedFunctionSourceValue, TableCreationSourceTable,
                    DataFlowEdge, TransformationStep, CalculationChain, CellLineage
                )
                print(f"\n  Table Structures:")
                print(f"    DatabaseTables: {DatabaseTable.objects.count()}")
                print(f"    DerivedTables: {DerivedTable.objects.count()}")
                print(f"    DatabaseFields: {DatabaseField.objects.count()}")
                print(f"    Functions: {Function.objects.count()}")

                print(f"\n  Populated Data:")
                print(f"    PopulatedDatabaseTables: {PopulatedDataBaseTable.objects.filter(trail=trail).count()}")
                print(f"    EvaluatedDerivedTables: {EvaluatedDerivedTable.objects.filter(trail=trail).count()}")
                print(f"    DatabaseRows: {DatabaseRow.objects.filter(populated_table__trail=trail).count()}")
                print(f"    DerivedTableRows: {DerivedTableRow.objects.filter(populated_table__trail=trail).count()}")
                print(f"    EvaluatedFunctions: {EvaluatedFunction.objects.filter(row__populated_table__trail=trail).count()}")

                print(f"\n  Lineage Relationships:")
                print(f"    FunctionColumnReferences: {FunctionColumnReference.objects.count()}")
                print(f"    DerivedRowSourceReferences: {DerivedRowSourceReference.objects.count()}")
                print(f"    EvaluatedFunctionSourceValues: {EvaluatedFunctionSourceValue.objects.count()}")
                print(f"    TableCreationSourceTables: {TableCreationSourceTable.objects.count()}")

                print(f"\n  Enhanced Tracking:")
                print(f"    DataFlowEdges: {DataFlowEdge.objects.filter(trail=trail).count()}")
                print(f"    TransformationSteps: {TransformationStep.objects.filter(trail=trail).count()}")
                print(f"    CalculationChains: {CalculationChain.objects.filter(trail=trail).count()}")
                print(f"    CellLineages: {CellLineage.objects.filter(trail=trail).count()}")

                # Print calculation usage tracking
                used_rows = CalculationUsedRow.objects.filter(trail=trail)
                used_fields = CalculationUsedField.objects.filter(trail=trail)
                print(f"\n  Calculation Usage Tracking:")
                print(f"    Tracked Used Rows: {used_rows.count()}")
                print(f"    Tracked Used Fields: {used_fields.count()}")

                if used_rows.exists():
                    calculation_names = used_rows.values_list('calculation_name', flat=True).distinct()
                    for calc_name in calculation_names:
                        row_count = used_rows.filter(calculation_name=calc_name).count()
                        field_count = used_fields.filter(calculation_name=calc_name).count()
                        print(f"      {calc_name}: {row_count} rows, {field_count} fields")

                print(f"\n=== End Lineage Summary ===\n")

        del datapoint
        return metric_value

    def delete_lineage_data():
        base_dir = settings.BASE_DIR
        lineage_dir = os.path.join(base_dir, 'results', 'lineage')
        for file in os.listdir(lineage_dir):
            if file != "__init__.py":
                try:
                    os.remove(os.path.join(lineage_dir, file))
                except FileNotFoundError:
                    pass
