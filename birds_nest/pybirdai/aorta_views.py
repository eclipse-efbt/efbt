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

from django.http import JsonResponse, HttpResponseBadRequest
from django.views.decorators.http import require_http_methods
from django.shortcuts import get_object_or_404
from django.core.paginator import Paginator
from django.views import View
from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_exempt
import json

from .models import (
    Trail, MetaDataTrail, DatabaseTable, DerivedTable,
    DatabaseField, Function, FunctionText, TableCreationFunction,
    PopulatedDataBaseTable, EvaluatedDerivedTable, DatabaseRow,
    DerivedTableRow, DatabaseColumnValue, EvaluatedFunction,
    AortaTableReference, FunctionColumnReference, DerivedRowSourceReference,
    EvaluatedFunctionSourceValue, TableCreationSourceTable
)
from .process_steps.pybird.orchestration import Orchestration


class AortaTrailListView(View):
    """List all execution trails"""
    
    def get(self, request):
        trails = Trail.objects.all().order_by('-created_at')
        
        # Pagination
        page_number = request.GET.get('page', 1)
        page_size = request.GET.get('page_size', 20)
        paginator = Paginator(trails, page_size)
        page_obj = paginator.get_page(page_number)
        
        trails_data = []
        for trail in page_obj:
            trails_data.append({
                'id': trail.id,
                'name': trail.name,
                'created_at': trail.created_at.isoformat(),
                'table_count': trail.metadata_trail.table_references.count() if trail.metadata_trail else 0,
                'execution_context': trail.execution_context
            })
        
        return JsonResponse({
            'trails': trails_data,
            'total': paginator.count,
            'page': page_obj.number,
            'pages': paginator.num_pages
        })


class AortaTrailDetailView(View):
    """Get detailed information about a specific trail"""
    
    def get(self, request, trail_id):
        trail = get_object_or_404(Trail, id=trail_id)
        
        # Get all tables in this trail
        tables = []
        for table_ref in trail.metadata_trail.table_references.all():
            if table_ref.table_content_type == 'DatabaseTable':
                table = DatabaseTable.objects.get(id=table_ref.table_id)
                tables.append({
                    'id': table.id,
                    'type': 'DatabaseTable',
                    'name': table.name,
                    'field_count': table.database_fields.count()
                })
            elif table_ref.table_content_type == 'DerivedTable':
                table = DerivedTable.objects.get(id=table_ref.table_id)
                tables.append({
                    'id': table.id,
                    'type': 'DerivedTable',
                    'name': table.name,
                    'function_count': table.derived_functions.count()
                })
        
        # Get populated tables
        populated_tables = []
        for pop_table in trail.populated_database_tables.all():
            populated_tables.append({
                'id': pop_table.id,
                'table_name': pop_table.table.name,
                'row_count': pop_table.databaserow_set.count()
            })
        
        for pop_table in trail.evaluated_derived_tables.all():
            populated_tables.append({
                'id': pop_table.id,
                'table_name': pop_table.table.name,
                'row_count': pop_table.derivedtablerow_set.count()
            })
        
        return JsonResponse({
            'trail': {
                'id': trail.id,
                'name': trail.name,
                'created_at': trail.created_at.isoformat(),
                'execution_context': trail.execution_context
            },
            'tables': tables,
            'populated_tables': populated_tables
        })


class AortaValueLineageView(View):
    """Get lineage tree for a specific value"""
    
    def get(self, request, value_id):
        # Determine if it's a DatabaseColumnValue or EvaluatedFunction
        value = None
        value_type = request.GET.get('type', 'evaluated')
        
        if value_type == 'database':
            value = get_object_or_404(DatabaseColumnValue, id=value_id)
        else:
            value = get_object_or_404(EvaluatedFunction, id=value_id)
        
        lineage_tree = self._build_lineage_tree(value)
        
        return JsonResponse(lineage_tree)
    
    def _build_lineage_tree(self, value, visited=None):
        """Recursively build lineage tree"""
        if visited is None:
            visited = set()
        
        # Avoid cycles
        value_key = f"{type(value).__name__}_{value.id}"
        if value_key in visited:
            return {'id': value_key, 'type': 'circular_reference'}
        visited.add(value_key)
        
        if isinstance(value, DatabaseColumnValue):
            return {
                'id': value.id,
                'type': 'DatabaseColumnValue',
                'value': value.value or value.string_value,
                'column': value.column.name,
                'table': value.column.table.name,
                'row_id': value.row.id
            }
        
        elif isinstance(value, EvaluatedFunction):
            # Get source values
            source_values = []
            for source_ref in value.source_value_references.all():
                if source_ref.source_value:
                    source_values.append(self._build_lineage_tree(source_ref.source_value, visited))
            
            return {
                'id': value.id,
                'type': 'EvaluatedFunction',
                'value': value.value or value.string_value,
                'function': value.function.name,
                'row_id': value.row.id,
                'source_values': source_values
            }
        
        return {'id': str(value), 'type': 'unknown'}


class AortaTableDependenciesView(View):
    """Get dependencies for a specific table"""
    
    def get(self, request, table_id):
        table_type = request.GET.get('type', 'derived')
        
        if table_type == 'database':
            table = get_object_or_404(DatabaseTable, id=table_id)
            dependencies = {'upstream': [], 'downstream': []}
        else:
            table = get_object_or_404(DerivedTable, id=table_id)
            dependencies = self._get_table_dependencies(table)
        
        return JsonResponse({
            'table': {
                'id': table.id,
                'name': table.name,
                'type': table_type
            },
            'dependencies': dependencies
        })
    
    def _get_table_dependencies(self, table):
        """Get upstream and downstream dependencies for a table"""
        dependencies = {
            'upstream': [],
            'downstream': []
        }
        
        # Get upstream dependencies (tables this table depends on)
        if hasattr(table, 'table_creation_function') and table.table_creation_function:
            for source_ref in table.table_creation_function.source_table_references.all():
                if source_ref.source_table:
                    dependencies['upstream'].append({
                        'id': source_ref.source_table.id,
                        'name': source_ref.source_table.name,
                        'type': type(source_ref.source_table).__name__
                    })
        
        # Get downstream dependencies (tables that depend on this table)
        # This would require querying TableCreationSourceTable where this table is the source
        # Implementation depends on specific requirements
        
        return dependencies


class AortaLineageGraphView(View):
    """Export lineage as a graph structure for visualization"""
    
    def get(self, request, trail_id):
        orchestration = Orchestration()
        graph = orchestration.export_lineage_graph(trail_id)
        
        if not graph:
            return HttpResponseBadRequest("Trail not found")
        
        # Enhance graph with additional data
        trail = Trail.objects.get(id=trail_id)
        
        # Add function nodes and edges
        for table_ref in trail.metadata_trail.table_references.all():
            if table_ref.table_content_type == 'DerivedTable':
                table = DerivedTable.objects.get(id=table_ref.table_id)
                
                # Add function nodes
                for function in table.derived_functions.all():
                    graph['nodes'].append({
                        'id': f'function_{function.id}',
                        'type': 'Function',
                        'name': function.name,
                        'table_id': f'table_{table.id}'
                    })
                    
                    # Add edges from function to table
                    graph['edges'].append({
                        'source': f'function_{function.id}',
                        'target': f'table_{table.id}',
                        'type': 'produces'
                    })
                    
                    # Add edges from source columns to function
                    for col_ref in function.column_references.all():
                        if col_ref.referenced_column:
                            source_id = f'column_{col_ref.referenced_column.id}'
                            graph['edges'].append({
                                'source': source_id,
                                'target': f'function_{function.id}',
                                'type': 'uses'
                            })
        
        return JsonResponse(graph)


# URL patterns to be added to urls.py
aorta_urlpatterns = [
    # path('api/aorta/trails/', AortaTrailListView.as_view(), name='aorta-trail-list'),
    # path('api/aorta/trails/<int:trail_id>/', AortaTrailDetailView.as_view(), name='aorta-trail-detail'),
    # path('api/aorta/values/<int:value_id>/lineage/', AortaValueLineageView.as_view(), name='aorta-value-lineage'),
    # path('api/aorta/tables/<int:table_id>/dependencies/', AortaTableDependenciesView.as_view(), name='aorta-table-dependencies'),
    # path('api/aorta/trails/<int:trail_id>/graph/', AortaLineageGraphView.as_view(), name='aorta-lineage-graph'),
]