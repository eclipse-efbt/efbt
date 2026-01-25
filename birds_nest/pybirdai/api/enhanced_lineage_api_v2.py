# coding=UTF-8
# Copyright (c) 2025 Bird Software Solutions Ltd
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDX-License-Identifier: EPL-2.0
#
# Contributors:
#    Neil Mackenzie - initial API and implementation
#
"""
Enhanced Lineage API v2 - Provides comprehensive lineage data including:
- Transformation steps and chains
- Data flow edges for Sankey diagrams
- Cell lineage for output-centric views
- Summary statistics
"""

from django.shortcuts import get_object_or_404
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from django.db.models import Count, Sum, Avg, Max
from django.contrib.contenttypes.models import ContentType
from pybirdai.models import (
    Trail, MetaDataTrail, DatabaseTable, DerivedTable,
    DatabaseField, Function, FunctionText, TableCreationFunction,
    PopulatedDataBaseTable, EvaluatedDerivedTable, DatabaseRow,
    DerivedTableRow, DatabaseColumnValue, EvaluatedFunction,
    AortaTableReference, FunctionColumnReference, DerivedRowSourceReference,
    EvaluatedFunctionSourceValue, TableCreationSourceTable, TableCreationFunctionColumn,
    CalculationUsedRow, CalculationUsedField,
    # New enhanced models
    TransformationStep, TransformationStepInput, TransformationStepOutput,
    CalculationChain, CalculationChainStep, DataFlowEdge,
    CellLineage, CellSourceRow, LineageSummaryCache
)
from datetime import datetime
import logging
import re

logger = logging.getLogger(__name__)


def serialize_datetime(obj):
    """JSON serializer for datetime objects"""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


@require_http_methods(["GET"])
def get_enhanced_lineage(request, trail_id):
    """
    Comprehensive API endpoint that returns all lineage information including:
    - Basic lineage (tables, fields, rows, values)
    - Transformation steps and chains
    - Data flow edges for visualization
    - Cell lineage for output-centric views
    - Summary statistics

    Query parameters:
    - detail: 'table', 'column', 'row', 'value' (default: 'table')
    - max_rows: Maximum rows per table (default: 10)
    - hide_empty: Hide tables with no data (default: true)
    - include_flow: Include data flow edges (default: true)
    - include_steps: Include transformation steps (default: true)
    - include_cells: Include cell lineage (default: true)
    """
    trail = get_object_or_404(Trail, pk=trail_id)

    # Parse query parameters
    detail_level = request.GET.get('detail', 'table')
    max_rows = int(request.GET.get('max_rows', 10))
    hide_empty = request.GET.get('hide_empty', 'true').lower() == 'true'
    include_flow = request.GET.get('include_flow', 'true').lower() == 'true'
    include_steps = request.GET.get('include_steps', 'true').lower() == 'true'
    include_cells = request.GET.get('include_cells', 'true').lower() == 'true'

    try:
        lineage_data = {
            "trail": serialize_trail(trail),
            "database_tables": [],
            "derived_tables": [],
            "populated_database_tables": [],
            "evaluated_derived_tables": [],
            "lineage_relationships": {
                "function_column_references": [],
                "derived_row_source_references": [],
                "evaluated_function_source_values": [],
                "table_creation_source_tables": [],
                "table_creation_function_columns": []
            },
            "metadata": {
                "generation_timestamp": datetime.now().isoformat(),
                "detail_level": detail_level,
                "max_rows": max_rows,
                "total_counts": {}
            }
        }

        # Process database tables
        lineage_data = process_database_tables(trail, lineage_data, detail_level, max_rows, hide_empty)

        # Process derived tables
        lineage_data = process_derived_tables(trail, lineage_data, detail_level, max_rows, hide_empty)

        # Process lineage relationships
        lineage_data = process_lineage_relationships(trail, lineage_data)

        # Add transformation steps if requested
        if include_steps:
            lineage_data["transformation_steps"] = get_transformation_steps(trail)
            lineage_data["calculation_chains"] = get_calculation_chains(trail)

        # Add data flow edges if requested
        if include_flow:
            lineage_data["data_flow_edges"] = get_data_flow_edges(trail)

            # If no explicit edges, infer from relationships
            if not lineage_data["data_flow_edges"]:
                lineage_data["data_flow_edges"] = infer_data_flow_edges(lineage_data)

        # Add cell lineage if requested
        if include_cells:
            lineage_data["cell_lineages"] = get_cell_lineages(trail)

            # If no explicit cell lineage, infer from evaluated tables
            if not lineage_data["cell_lineages"]:
                lineage_data["cell_lineages"] = infer_cell_lineages(trail, lineage_data)

        # Update counts
        lineage_data["metadata"]["total_counts"] = calculate_counts(lineage_data)

        # Add summary statistics
        lineage_data["summary"] = generate_summary(trail, lineage_data)

        return JsonResponse(lineage_data, json_dumps_params={'default': serialize_datetime, 'indent': 2})

    except Exception as e:
        logger.exception(f"Error in get_enhanced_lineage for trail {trail_id}")
        return JsonResponse({
            'error': 'Enhanced lineage extraction failed',
            'trail_id': trail_id,
            'trail_name': trail.name,
            'message': str(e)
        }, status=500)


def serialize_trail(trail):
    """Serialize trail metadata"""
    return {
        "id": trail.id,
        "name": trail.name,
        "created_at": trail.created_at.isoformat(),
        "execution_context": trail.execution_context,
        "metadata_trail_id": trail.metadata_trail.id if trail.metadata_trail else None
    }


def process_database_tables(trail, lineage_data, detail_level, max_rows, hide_empty):
    """Process database tables and their data"""
    populated_tables = PopulatedDataBaseTable.objects.filter(
        trail=trail
    ).select_related('table').prefetch_related(
        'table__database_fields',
        'databaserow_set__column_values__column'
    )

    database_table_ids = set()

    for pop_table in populated_tables:
        table = pop_table.table
        rows = list(pop_table.databaserow_set.all()[:max_rows])

        if hide_empty and not rows:
            continue

        database_table_ids.add(table.id)

        # Add table definition if not already added
        if not any(dt['id'] == table.id for dt in lineage_data['database_tables']):
            table_data = {
                "id": table.id,
                "name": table.name,
                "fields": []
            }

            if detail_level in ['column', 'row', 'value']:
                for field in table.database_fields.all():
                    table_data['fields'].append({
                        "id": field.id,
                        "name": field.name,
                        "table_id": table.id
                    })

            lineage_data['database_tables'].append(table_data)

        # Add populated table instance
        pop_table_data = {
            "id": pop_table.id,
            "table_id": table.id,
            "table_name": table.name,
            "trail_id": trail.id,
            "row_count": pop_table.databaserow_set.count(),
            "rows": []
        }

        if detail_level in ['row', 'value']:
            for row in rows:
                row_data = {
                    "id": row.id,
                    "row_identifier": row.row_identifier,
                    "populated_table_id": pop_table.id,
                    "values": []
                }

                if detail_level == 'value':
                    for col_value in row.column_values.all():
                        row_data['values'].append({
                            "id": col_value.id,
                            "value": col_value.value,
                            "string_value": col_value.string_value,
                            "column_id": col_value.column.id,
                            "column_name": col_value.column.name,
                            "row_id": row.id
                        })

                pop_table_data['rows'].append(row_data)

        lineage_data['populated_database_tables'].append(pop_table_data)

    return lineage_data


def process_derived_tables(trail, lineage_data, detail_level, max_rows, hide_empty):
    """Process derived tables and their data"""
    evaluated_tables = EvaluatedDerivedTable.objects.filter(
        trail=trail
    ).select_related('table', 'table__table_creation_function').prefetch_related(
        'table__derived_functions__function_text',
        'derivedtablerow_set__evaluated_functions__function'
    )

    for eval_table in evaluated_tables:
        table = eval_table.table
        rows = list(eval_table.derivedtablerow_set.all()[:max_rows])

        if hide_empty and not rows:
            continue

        # Add table definition if not already added
        if not any(dt['id'] == table.id for dt in lineage_data['derived_tables']):
            table_data = {
                "id": table.id,
                "name": table.name,
                "table_creation_function_id": table.table_creation_function.id if table.table_creation_function else None,
                "functions": []
            }

            if detail_level in ['column', 'row', 'value']:
                for function in table.derived_functions.all():
                    func_data = {
                        "id": function.id,
                        "name": function.name,
                        "table_id": table.id,
                        "function_text_id": function.function_text.id if function.function_text else None,
                        "function_text": function.function_text.text if function.function_text else None,
                        "function_language": function.function_text.language if function.function_text else None
                    }
                    table_data['functions'].append(func_data)

            lineage_data['derived_tables'].append(table_data)

        # Add evaluated table instance
        eval_table_data = {
            "id": eval_table.id,
            "table_id": table.id,
            "table_name": table.name,
            "trail_id": trail.id,
            "row_count": eval_table.derivedtablerow_set.count(),
            "rows": []
        }

        if detail_level in ['row', 'value']:
            for row in rows:
                row_data = {
                    "id": row.id,
                    "row_identifier": row.row_identifier,
                    "populated_table_id": eval_table.id,
                    "evaluated_functions": []
                }

                if detail_level == 'value':
                    for eval_func in row.evaluated_functions.all():
                        row_data['evaluated_functions'].append({
                            "id": eval_func.id,
                            "value": eval_func.value,
                            "string_value": eval_func.string_value,
                            "function_id": eval_func.function.id,
                            "function_name": eval_func.function.name,
                            "row_id": row.id
                        })

                eval_table_data['rows'].append(row_data)

        lineage_data['evaluated_derived_tables'].append(eval_table_data)

    return lineage_data


def process_lineage_relationships(trail, lineage_data):
    """Process all lineage relationship types"""
    derived_table_ids = {dt['id'] for dt in lineage_data['derived_tables']}

    if not derived_table_ids:
        return lineage_data

    # Function column references - scoped to current trail execution
    func_refs = FunctionColumnReference.objects.filter(
        function__table__id__in=derived_table_ids,
        trail=trail  # Scope to current trail execution
    ).select_related('function', 'content_type')

    for ref in func_refs:
        lineage_data['lineage_relationships']['function_column_references'].append({
            "id": ref.id,
            "function_id": ref.function.id,
            "function_name": ref.function.name,
            "referenced_object_type": ref.content_type.model,
            "referenced_object_id": ref.object_id
        })

    # Get evaluated table IDs
    eval_table_ids = [et['id'] for et in lineage_data['evaluated_derived_tables']]

    # Derived row source references
    if eval_table_ids:
        row_refs = DerivedRowSourceReference.objects.filter(
            derived_row__populated_table__id__in=eval_table_ids
        ).select_related('derived_row', 'content_type')

        for ref in row_refs:
            lineage_data['lineage_relationships']['derived_row_source_references'].append({
                "id": ref.id,
                "derived_row_id": ref.derived_row.id,
                "source_object_type": ref.content_type.model,
                "source_object_id": ref.object_id
            })

        # Evaluated function source values
        value_refs = EvaluatedFunctionSourceValue.objects.filter(
            evaluated_function__row__populated_table__id__in=eval_table_ids
        ).select_related('evaluated_function', 'content_type')

        for ref in value_refs:
            lineage_data['lineage_relationships']['evaluated_function_source_values'].append({
                "id": ref.id,
                "evaluated_function_id": ref.evaluated_function.id,
                "source_object_type": ref.content_type.model,
                "source_object_id": ref.object_id
            })

    # Table creation source tables
    table_creation_functions = TableCreationFunction.objects.filter(
        derivedtable__id__in=derived_table_ids
    )

    table_src_refs = TableCreationSourceTable.objects.filter(
        table_creation_function__in=table_creation_functions
    ).select_related('table_creation_function', 'content_type')

    for ref in table_src_refs:
        lineage_data['lineage_relationships']['table_creation_source_tables'].append({
            "id": ref.id,
            "table_creation_function_id": ref.table_creation_function.id,
            "table_creation_function_name": ref.table_creation_function.name,
            "source_object_type": ref.content_type.model,
            "source_object_id": ref.object_id
        })

    # Table creation function columns
    col_refs = TableCreationFunctionColumn.objects.filter(
        table_creation_function__in=table_creation_functions
    ).select_related('table_creation_function', 'content_type')

    for ref in col_refs:
        lineage_data['lineage_relationships']['table_creation_function_columns'].append({
            "id": ref.id,
            "table_creation_function_id": ref.table_creation_function.id,
            "table_creation_function_name": ref.table_creation_function.name,
            "referenced_object_type": ref.content_type.model,
            "referenced_object_id": ref.object_id,
            "reference_text": ref.reference_text
        })

    return lineage_data


def get_transformation_steps(trail):
    """Get transformation steps for the trail"""
    steps = TransformationStep.objects.filter(
        trail=trail
    ).order_by('step_number').prefetch_related('inputs', 'outputs')

    return [
        {
            "id": step.id,
            "step_number": step.step_number,
            "step_type": step.step_type,
            "step_name": step.step_name,
            "description": step.description,
            "input_row_count": step.input_row_count,
            "output_row_count": step.output_row_count,
            "execution_time_ms": step.execution_time_ms,
            "started_at": step.started_at.isoformat() if step.started_at else None,
            "completed_at": step.completed_at.isoformat() if step.completed_at else None,
            "inputs": [
                {"source_type": inp.source_content_type.model, "source_id": inp.source_object_id}
                for inp in step.inputs.all()
            ],
            "outputs": [
                {"target_type": out.target_content_type.model, "target_id": out.target_object_id}
                for out in step.outputs.all()
            ]
        }
        for step in steps
    ]


def get_calculation_chains(trail):
    """Get calculation chains for the trail"""
    chains = CalculationChain.objects.filter(
        trail=trail
    ).prefetch_related('chain_steps__step')

    return [
        {
            "id": chain.id,
            "chain_name": chain.chain_name,
            "final_value": chain.final_value,
            "final_string_value": chain.final_string_value,
            "output_cell_name": chain.output_cell_name,
            "output_table": chain.output_table,
            "output_row_key": chain.output_row_key,
            "output_column": chain.output_column,
            "total_steps": chain.total_steps,
            "total_source_rows": chain.total_source_rows,
            "total_contributing_rows": chain.total_contributing_rows,
            "started_at": chain.started_at.isoformat() if chain.started_at else None,
            "completed_at": chain.completed_at.isoformat() if chain.completed_at else None,
            "steps": [
                {
                    "order": cs.order_in_chain,
                    "step_id": cs.step.id,
                    "step_name": cs.step.step_name
                }
                for cs in chain.chain_steps.all().order_by('order_in_chain')
            ]
        }
        for chain in chains
    ]


def get_data_flow_edges(trail):
    """Get explicit data flow edges for the trail"""
    edges = DataFlowEdge.objects.filter(trail=trail)

    return [
        {
            "id": edge.id,
            "source_type": edge.source_content_type.model,
            "source_id": edge.source_object_id,
            "source_label": edge.source_label,
            "target_type": edge.target_content_type.model,
            "target_id": edge.target_object_id,
            "target_label": edge.target_label,
            "flow_type": edge.flow_type,
            "row_count": edge.row_count,
            "value_sum": edge.value_sum
        }
        for edge in edges
    ]


def infer_data_flow_edges(lineage_data):
    """Infer data flow edges from table creation source tables"""
    edges = []

    # Build map of table creation function to target tables
    tcf_to_tables = {}
    for table in lineage_data['derived_tables']:
        tcf_id = table.get('table_creation_function_id')
        if tcf_id:
            if tcf_id not in tcf_to_tables:
                tcf_to_tables[tcf_id] = []
            tcf_to_tables[tcf_id].append(table)

    # Create edges from source tables to target tables
    edge_id = 0
    for ref in lineage_data['lineage_relationships']['table_creation_source_tables']:
        tcf_id = ref['table_creation_function_id']
        if tcf_id in tcf_to_tables:
            for target_table in tcf_to_tables[tcf_id]:
                edges.append({
                    "id": f"inferred_{edge_id}",
                    "source_type": ref['source_object_type'],
                    "source_id": ref['source_object_id'],
                    "source_label": "",
                    "target_type": "derivedtable",
                    "target_id": target_table['id'],
                    "target_label": target_table['name'],
                    "flow_type": "DATA",
                    "row_count": 0,
                    "value_sum": None
                })
                edge_id += 1

    # Try to set source labels
    db_tables = {t['id']: t['name'] for t in lineage_data['database_tables']}
    derived_tables = {t['id']: t['name'] for t in lineage_data['derived_tables']}

    for edge in edges:
        if edge['source_type'] == 'databasetable' and edge['source_id'] in db_tables:
            edge['source_label'] = db_tables[edge['source_id']]
        elif edge['source_type'] == 'derivedtable' and edge['source_id'] in derived_tables:
            edge['source_label'] = derived_tables[edge['source_id']]

    return edges


def get_cell_lineages(trail):
    """Get explicit cell lineage data"""
    cells = CellLineage.objects.filter(
        trail=trail
    ).prefetch_related('source_rows')

    return [
        {
            "id": cell.id,
            "report_template": cell.report_template,
            "framework": cell.framework,
            "cell_code": cell.cell_code,
            "row_key": cell.row_key,
            "column_key": cell.column_key,
            "computed_value": cell.computed_value,
            "computed_string_value": cell.computed_string_value,
            "source_table_count": cell.source_table_count,
            "source_row_count": cell.source_row_count,
            "transformation_count": cell.transformation_count,
            "calculation_chain_id": cell.calculation_chain.id if cell.calculation_chain else None,
            "source_rows": [
                {
                    "row_type": sr.row_content_type.model,
                    "row_id": sr.row_object_id,
                    "contribution_type": sr.contribution_type,
                    "contributed_value": sr.contributed_value
                }
                for sr in cell.source_rows.all()
            ]
        }
        for cell in cells
    ]


def infer_cell_lineages(trail, lineage_data):
    """Infer cell lineage from evaluated derived tables"""
    cells = []

    # Pattern for output tables (e.g., F_01_01_REF_FINREP_3_0)
    output_pattern = re.compile(r'^F_(\d{2})_(\d{2})_REF_(\w+)_(\d+)_(\d+)$')

    for eval_table in lineage_data['evaluated_derived_tables']:
        table_name = eval_table['table_name']
        match = output_pattern.match(table_name)

        # Skip intermediate tables (e.g., UnionItem, _filtered_, etc.)
        if not match or any(x in table_name for x in ['UnionItem', '_filtered_', '_aggregated_']):
            continue

        # Extract report info
        row_num, col_num = match.group(1), match.group(2)
        framework = match.group(3)
        version = f"{match.group(4)}.{match.group(5)}"
        report_template = f"F_{row_num}.{col_num}"

        for row in eval_table.get('rows', []):
            for func in row.get('evaluated_functions', []):
                if func.get('value') is not None:
                    cells.append({
                        "id": f"inferred_{len(cells)}",
                        "report_template": report_template,
                        "framework": framework,
                        "cell_code": f"{row.get('row_identifier', 'row')}.{func['function_name']}",
                        "row_key": row.get('row_identifier', ''),
                        "column_key": func['function_name'],
                        "computed_value": func['value'],
                        "computed_string_value": func.get('string_value'),
                        "source_table_count": 0,
                        "source_row_count": 0,
                        "transformation_count": 0,
                        "calculation_chain_id": None,
                        "source_rows": []
                    })

    return cells


def calculate_counts(lineage_data):
    """Calculate summary counts"""
    return {
        "database_tables": len(lineage_data['database_tables']),
        "derived_tables": len(lineage_data['derived_tables']),
        "populated_database_tables": len(lineage_data['populated_database_tables']),
        "evaluated_derived_tables": len(lineage_data['evaluated_derived_tables']),
        "total_database_rows": sum(pt.get('row_count', 0) for pt in lineage_data['populated_database_tables']),
        "total_derived_rows": sum(et.get('row_count', 0) for et in lineage_data['evaluated_derived_tables']),
        "function_column_references": len(lineage_data['lineage_relationships']['function_column_references']),
        "derived_row_source_references": len(lineage_data['lineage_relationships']['derived_row_source_references']),
        "evaluated_function_source_values": len(lineage_data['lineage_relationships']['evaluated_function_source_values']),
        "table_creation_source_tables": len(lineage_data['lineage_relationships']['table_creation_source_tables']),
        "table_creation_function_columns": len(lineage_data['lineage_relationships']['table_creation_function_columns']),
        "transformation_steps": len(lineage_data.get('transformation_steps', [])),
        "calculation_chains": len(lineage_data.get('calculation_chains', [])),
        "data_flow_edges": len(lineage_data.get('data_flow_edges', [])),
        "cell_lineages": len(lineage_data.get('cell_lineages', []))
    }


def generate_summary(trail, lineage_data):
    """Generate summary statistics"""
    counts = lineage_data['metadata']['total_counts']

    # Calculate chain statistics
    chains = lineage_data.get('calculation_chains', [])
    chain_lengths = [c['total_steps'] for c in chains if c['total_steps'] > 0]

    avg_chain_length = sum(chain_lengths) / len(chain_lengths) if chain_lengths else 0
    max_chain_length = max(chain_lengths) if chain_lengths else 0

    return {
        "trail_name": trail.name,
        "created_at": trail.created_at.isoformat(),
        "total_tables": counts['database_tables'] + counts['derived_tables'],
        "total_rows": counts['total_database_rows'] + counts['total_derived_rows'],
        "total_relationships": (
            counts['function_column_references'] +
            counts['derived_row_source_references'] +
            counts['table_creation_source_tables']
        ),
        "has_transformation_steps": counts['transformation_steps'] > 0,
        "has_calculation_chains": counts['calculation_chains'] > 0,
        "has_data_flow_edges": counts['data_flow_edges'] > 0,
        "has_cell_lineages": counts['cell_lineages'] > 0,
        "avg_chain_length": round(avg_chain_length, 2),
        "max_chain_length": max_chain_length,
        "data_coverage": {
            "database_tables_with_data": sum(1 for pt in lineage_data['populated_database_tables'] if pt.get('row_count', 0) > 0),
            "derived_tables_with_data": sum(1 for et in lineage_data['evaluated_derived_tables'] if et.get('row_count', 0) > 0)
        }
    }


@require_http_methods(["GET"])
def get_lineage_graph_data(request, trail_id):
    """
    Returns lineage data formatted for Cytoscape.js graph visualization.
    """
    trail = get_object_or_404(Trail, pk=trail_id)

    detail_level = request.GET.get('detail', 'table')
    max_rows = int(request.GET.get('max_rows', 10))
    hide_empty = request.GET.get('hide_empty', 'true').lower() == 'true'

    try:
        nodes = []
        edges = []

        # Database tables
        populated_tables = PopulatedDataBaseTable.objects.filter(
            trail=trail
        ).select_related('table').prefetch_related('table__database_fields')

        for pop_table in populated_tables:
            table = pop_table.table
            row_count = pop_table.databaserow_set.count()

            if hide_empty and row_count == 0:
                continue

            nodes.append({
                "id": f"db_table_{table.id}",
                "label": table.name,
                "type": "database_table",
                "details": {
                    "type": "Database Table",
                    "name": table.name,
                    "row_count": row_count,
                    "column_count": table.database_fields.count()
                }
            })

            if detail_level != 'table':
                for field in table.database_fields.all():
                    nodes.append({
                        "id": f"db_field_{field.id}",
                        "label": field.name,
                        "type": "database_field",
                        "details": {
                            "type": "Database Field",
                            "name": field.name,
                            "table": table.name
                        }
                    })
                    edges.append({
                        "source": f"db_table_{table.id}",
                        "target": f"db_field_{field.id}",
                        "type": "has_field"
                    })

        # Derived tables
        evaluated_tables = EvaluatedDerivedTable.objects.filter(
            trail=trail
        ).select_related('table', 'table__table_creation_function').prefetch_related('table__derived_functions')

        for eval_table in evaluated_tables:
            table = eval_table.table
            row_count = eval_table.derivedtablerow_set.count()

            if hide_empty and row_count == 0:
                continue

            # Determine node type
            is_output = bool(re.match(r'^F_\d{2}_\d{2}_REF_', table.name)) and 'UnionItem' not in table.name
            node_type = 'output_table' if is_output else 'derived_table'

            nodes.append({
                "id": f"derived_table_{table.id}",
                "label": table.name,
                "type": node_type,
                "details": {
                    "type": "Output Table" if is_output else "Derived Table",
                    "name": table.name,
                    "row_count": row_count,
                    "function_count": table.derived_functions.count()
                }
            })

            if detail_level != 'table':
                for function in table.derived_functions.all():
                    nodes.append({
                        "id": f"function_{function.id}",
                        "label": function.name.split('.')[-1] if '.' in function.name else function.name,
                        "type": "function",
                        "details": {
                            "type": "Function",
                            "name": function.name,
                            "table": table.name,
                            "function_text": function.function_text.text if function.function_text else None
                        }
                    })
                    edges.append({
                        "source": f"derived_table_{table.id}",
                        "target": f"function_{function.id}",
                        "type": "has_function"
                    })

        # Table creation source edges
        derived_table_ids = {t['id'].replace('derived_table_', '') for t in nodes if 'derived_table_' in t['id']}
        table_creation_functions = TableCreationFunction.objects.filter(
            derivedtable__id__in=[int(i) for i in derived_table_ids if i.isdigit()]
        )

        for tcf in table_creation_functions:
            # Find target derived tables
            target_tables = DerivedTable.objects.filter(table_creation_function=tcf)

            # Find source tables
            source_refs = TableCreationSourceTable.objects.filter(
                table_creation_function=tcf
            ).select_related('content_type')

            for source_ref in source_refs:
                source_type = 'db_table' if source_ref.content_type.model == 'databasetable' else 'derived_table'
                source_id = f"{source_type}_{source_ref.object_id}"

                for target in target_tables:
                    target_id = f"derived_table_{target.id}"

                    # Only add edge if both nodes exist
                    if any(n['id'] == source_id for n in nodes) and any(n['id'] == target_id for n in nodes):
                        edges.append({
                            "source": source_id,
                            "target": target_id,
                            "type": "data_flow",
                            "label": ""
                        })

        return JsonResponse({
            "nodes": nodes,
            "edges": edges,
            "summary": {
                "total_nodes": len(nodes),
                "total_edges": len(edges),
                "table_count": {
                    "database": sum(1 for n in nodes if n['type'] == 'database_table'),
                    "derived": sum(1 for n in nodes if n['type'] in ['derived_table', 'output_table'])
                }
            }
        })

    except Exception as e:
        logger.exception(f"Error in get_lineage_graph_data for trail {trail_id}")
        return JsonResponse({
            'error': 'Graph data generation failed',
            'message': str(e)
        }, status=500)


@require_http_methods(["GET"])
def get_lineage_sankey_data(request, trail_id):
    """
    Returns lineage data formatted for Sankey diagram visualization.
    """
    trail = get_object_or_404(Trail, pk=trail_id)

    try:
        nodes = []
        links = []
        node_index = {}

        # Get database tables
        db_tables = PopulatedDataBaseTable.objects.filter(
            trail=trail
        ).select_related('table')

        for pop_table in db_tables:
            table = pop_table.table
            row_count = pop_table.databaserow_set.count()
            if row_count > 0:
                node_id = f"db_{table.id}"
                node_index[node_id] = len(nodes)
                nodes.append({
                    "id": node_id,
                    "name": table.name,
                    "type": "source",
                    "value": row_count
                })

        # Get derived tables
        derived_tables = EvaluatedDerivedTable.objects.filter(
            trail=trail
        ).select_related('table', 'table__table_creation_function')

        for eval_table in derived_tables:
            table = eval_table.table
            row_count = eval_table.derivedtablerow_set.count()
            if row_count > 0:
                is_output = bool(re.match(r'^F_\d{2}_\d{2}_REF_', table.name)) and 'UnionItem' not in table.name
                node_id = f"derived_{table.id}"
                node_index[node_id] = len(nodes)
                nodes.append({
                    "id": node_id,
                    "name": table.name,
                    "type": "output" if is_output else "transform",
                    "value": row_count
                })

        # Build links from table creation sources
        derived_table_ids = [t.table.id for t in derived_tables]
        table_creation_functions = TableCreationFunction.objects.filter(
            derivedtable__id__in=derived_table_ids
        )

        for tcf in table_creation_functions:
            target_tables = DerivedTable.objects.filter(table_creation_function=tcf)
            source_refs = TableCreationSourceTable.objects.filter(
                table_creation_function=tcf
            ).select_related('content_type')

            for source_ref in source_refs:
                source_type = 'db' if source_ref.content_type.model == 'databasetable' else 'derived'
                source_id = f"{source_type}_{source_ref.object_id}"

                for target in target_tables:
                    target_id = f"derived_{target.id}"

                    if source_id in node_index and target_id in node_index:
                        links.append({
                            "source": node_index[source_id],
                            "target": node_index[target_id],
                            "value": 1
                        })

        return JsonResponse({
            "nodes": nodes,
            "links": links
        })

    except Exception as e:
        logger.exception(f"Error in get_lineage_sankey_data for trail {trail_id}")
        return JsonResponse({
            'error': 'Sankey data generation failed',
            'message': str(e)
        }, status=500)
