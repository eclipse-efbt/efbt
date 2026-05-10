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
Lineage Collector - Collects lineage information during execution and resolves
relationships at finalization time.

This solves the problem of trying to create relationships between objects that
don't exist yet in the database during execution.
"""

from datetime import datetime
from django.contrib.contenttypes.models import ContentType
from collections import defaultdict
from contextvars import ContextVar
import os


class LineageCollector:
    """
    Collects lineage information during execution for deferred resolution.

    Instead of trying to create database relationships immediately (which fails
    because referenced objects don't exist yet), we collect all the information
    and create the relationships at finalization time.
    """

    def __init__(self):
        # Pending relationships to be created at finalization
        self.pending_function_column_refs = []  # [(function_name, column_name, table_hint)]
        self.pending_table_creation_sources = []  # [(derived_table_name, source_table_name)]
        self.pending_derived_row_sources = []  # [(derived_row_id, source_table_name, source_row_identifier)]
        self.pending_evaluated_function_sources = []  # [(eval_func_id, source_value_info)]

        # Mappings built during execution
        self.table_name_to_db_id = {}  # table_name -> (table_type, table_id)
        self.function_name_to_db_id = {}  # function_name -> function_id
        self.python_obj_id_to_row_id = {}  # id(python_obj) -> (row_type, row_id)
        self.row_identifier_to_row = {}  # (table_name, row_identifier) -> (row_type, row_id)

        # Track all created objects for relationship building
        self.created_tables = []  # [(table_type, table_id, table_name)]
        self.created_functions = []  # [(function_id, function_name, table_name, dependencies)]
        self.created_rows = []  # [(row_type, row_id, table_name, row_identifier, python_obj_id)]
        self.created_evaluated_functions = []  # [(eval_func_id, function_name, row_id, source_values)]

        # Source table tracking for derived tables
        self.derived_table_sources = defaultdict(set)  # derived_table_name -> {source_table_names}

        # Function dependency tracking
        self.function_dependencies = defaultdict(set)  # function_name -> {dependency_strings}

        # Object relationship tracking
        self.object_relationships = []  # [(child_obj_id, parent_obj_id, relationship_type)]

        # Value source tracking
        self.value_computations = []  # [(function_name, row_id, source_value_refs, computed_value)]
        self.debug_lineage = os.environ.get('PYBIRDAI_DEBUG_LINEAGE', '').lower() in {'1', 'true', 'yes', 'on'}
        self._created_table_keys = set()
        self._created_function_keys = set()
        self._created_row_keys = set()
        self._created_evaluated_function_keys = set()
        self._object_relationship_keys = set()
        self._column_resolution_cache = {}

    def _debug(self, message):
        if self.debug_lineage:
            print(message)

    def register_table(self, table_type, table_id, table_name):
        """Register a created table"""
        self.table_name_to_db_id[table_name] = (table_type, table_id)
        key = (table_type, table_id, table_name)
        if key not in self._created_table_keys:
            self.created_tables.append(key)
            self._created_table_keys.add(key)
        self._debug(f"LineageCollector: Registered table {table_name} ({table_type}, id={table_id})")

    def register_function(self, function_id, function_name, table_name, dependencies=None):
        """Register a created function with its dependencies"""
        self.function_name_to_db_id[function_name] = function_id
        deps = dependencies or []
        key = (function_id, function_name, table_name)
        if key not in self._created_function_keys:
            self.created_functions.append((function_id, function_name, table_name, deps))
            self._created_function_keys.add(key)

        # Store dependencies for later resolution
        if deps:
            self.function_dependencies[function_name].update(deps)

        self._debug(f"LineageCollector: Registered function {function_name} (id={function_id}) with {len(deps)} dependencies")

    def register_row(self, row_type, row_id, table_name, row_identifier, python_obj=None):
        """Register a created row"""
        python_obj_id = id(python_obj) if python_obj else None
        key = (row_type, row_id)
        if key not in self._created_row_keys:
            self.created_rows.append((row_type, row_id, table_name, row_identifier, python_obj_id))
            self._created_row_keys.add(key)

        if python_obj_id:
            self.python_obj_id_to_row_id[python_obj_id] = (row_type, row_id)

        self.row_identifier_to_row[(table_name, row_identifier)] = (row_type, row_id)
        self._debug(f"LineageCollector: Registered row {row_identifier} in {table_name} ({row_type}, id={row_id})")

    def register_evaluated_function(self, eval_func_id, function_name, row_id, source_values=None):
        """Register an evaluated function result"""
        key = (eval_func_id, function_name, row_id)
        if key not in self._created_evaluated_function_keys:
            self.created_evaluated_functions.append((eval_func_id, function_name, row_id, source_values or []))
            self._created_evaluated_function_keys.add(key)
        self._debug(f"LineageCollector: Registered evaluated function {function_name} (eval_id={eval_func_id})")

    def add_table_source(self, derived_table_name, source_table_name):
        """Record that a derived table uses a source table"""
        self.derived_table_sources[derived_table_name].add(source_table_name)
        self._debug(f"LineageCollector: Added table source {derived_table_name} <- {source_table_name}")

    def add_function_dependency(self, function_name, dependency_string):
        """Record a function's dependency on a column/field"""
        self.function_dependencies[function_name].add(dependency_string)

    def add_object_relationship(self, child_obj, parent_obj, relationship_type='contains'):
        """Record a relationship between two Python objects"""
        if child_obj and parent_obj:
            key = (id(child_obj), id(parent_obj), relationship_type)
            if key not in self._object_relationship_keys:
                self.object_relationships.append(key)
                self._object_relationship_keys.add(key)
            self._debug(f"LineageCollector: Added object relationship {type(child_obj).__name__} <- {type(parent_obj).__name__}")

    def add_value_computation(self, function_name, row_id, source_value_refs, computed_value):
        """Record a value computation with its sources"""
        self.value_computations.append((function_name, row_id, source_value_refs, computed_value))

    def add_runtime_dependency(self, dependent_function_name, source_table_name, source_field_name):
        """Record a runtime dependency discovered during execution"""
        dependency_string = f"{source_table_name}.{source_field_name}"
        self.function_dependencies[dependent_function_name].add(dependency_string)
        # Also record as table source
        if dependent_function_name in self.function_name_to_db_id:
            # Find the table for this function
            for func_id, func_name, table_name, _ in self.created_functions:
                if func_name == dependent_function_name:
                    self.derived_table_sources[table_name].add(source_table_name)
                    break

    def finalize(self, trail, metadata_trail):
        """
        Resolve all pending relationships and create database records.
        This is called after all objects have been created.
        """
        self._debug("\n=== LineageCollector Finalization ===")
        self._debug(f"Tables: {len(self.created_tables)}")
        self._debug(f"Functions: {len(self.created_functions)}")
        self._debug(f"Rows: {len(self.created_rows)}")
        self._debug(f"Evaluated Functions: {len(self.created_evaluated_functions)}")
        self._debug(f"Table Sources: {sum(len(v) for v in self.derived_table_sources.values())}")
        self._debug(f"Function Dependencies: {sum(len(v) for v in self.function_dependencies.values())}")
        self._debug(f"Object Relationships: {len(self.object_relationships)}")

        stats = {
            'function_column_references': 0,
            'table_creation_source_tables': 0,
            'table_creation_function_columns': 0,
            'derived_row_source_references': 0,
            'evaluated_function_source_values': 0,
            'data_flow_edges': 0
        }

        # Import models here to avoid circular imports
        from pybirdai.models import (
            DatabaseTable, DerivedTable, DatabaseField, Function,
            FunctionColumnReference, TableCreationFunction, FunctionText,
            TableCreationSourceTable, TableCreationFunctionColumn,
            DerivedRowSourceReference, EvaluatedFunctionSourceValue,
            DatabaseColumnValue, DatabaseRow, DerivedTableRow, DataFlowEdge
        )
        from pybirdai.models import EvaluatedFunction

        function_ids = [function_id for function_id, _, _, _ in self.created_functions]
        function_ct = ContentType.objects.get_for_model(Function)
        database_field_ct = ContentType.objects.get_for_model(DatabaseField)
        database_column_value_ct = ContentType.objects.get_for_model(DatabaseColumnValue)
        database_table_ct = ContentType.objects.get_for_model(DatabaseTable)
        derived_table_ct = ContentType.objects.get_for_model(DerivedTable)
        database_row_ct = ContentType.objects.get_for_model(DatabaseRow)
        derived_row_ct = ContentType.objects.get_for_model(DerivedTableRow)
        evaluated_function_ct = ContentType.objects.get_for_model(EvaluatedFunction)

        content_type_by_model = {
            Function: function_ct,
            DatabaseField: database_field_ct,
            DatabaseColumnValue: database_column_value_ct,
            DatabaseTable: database_table_ct,
            DerivedTable: derived_table_ct,
            DatabaseRow: database_row_ct,
            DerivedTableRow: derived_row_ct,
            EvaluatedFunction: evaluated_function_ct,
        }
        content_type_by_name = {
            'Function': function_ct,
            'DatabaseField': database_field_ct,
            'DatabaseColumnValue': database_column_value_ct,
            'DatabaseTable': database_table_ct,
            'DerivedTable': derived_table_ct,
            'DatabaseRow': database_row_ct,
            'DerivedTableRow': derived_row_ct,
            'EvaluatedFunction': evaluated_function_ct,
        }

        database_table_ids = [
            table_id for table_type, table_id in self.table_name_to_db_id.values()
            if table_type == 'DatabaseTable'
        ]
        derived_table_ids = [
            table_id for table_type, table_id in self.table_name_to_db_id.values()
            if table_type == 'DerivedTable'
        ]
        database_tables_by_id = DatabaseTable.objects.in_bulk(database_table_ids) if database_table_ids else {}
        derived_tables_by_id = DerivedTable.objects.in_bulk(derived_table_ids) if derived_table_ids else {}

        existing_function_ref_keys = set()
        if function_ids:
            existing_function_ref_keys = set(
                FunctionColumnReference.objects.filter(
                    function_id__in=function_ids
                ).values_list('function_id', 'content_type_id', 'object_id')
            )
        function_refs_to_create = []

        # 1. Create FunctionColumnReferences
        self._debug("\n1. Creating FunctionColumnReferences...")
        self._debug(f"  Have {len(self.created_functions)} registered functions")
        self._debug(f"  Function name to DB ID mapping: {len(self.function_name_to_db_id)} entries")

        for function_id, function_name, table_name, dependencies in self.created_functions:
            all_deps = dependencies + list(self.function_dependencies.get(function_name, []))
            self._debug(f"  Processing function {function_name} (id={function_id}) with {len(all_deps)} dependencies: {all_deps[:5]}...")

            for dep in all_deps:
                # First try to resolve using our own function mappings
                # This handles cases like "base.GRSS_CRRYNG_AMNT" -> find function named "*GRSS_CRRYNG_AMNT"
                clean_dep = dep.replace('base.', '').replace('self.', '').replace('row.', '')
                column_name = clean_dep.split('.')[-1] if '.' in clean_dep else clean_dep

                # Try to find a registered function that ends with this column name
                ref_function_id = None
                for fn_name, fn_id in self.function_name_to_db_id.items():
                    if fn_name.endswith(f'.{column_name}') or fn_name == column_name:
                        ref_function_id = fn_id
                        break

                if ref_function_id:
                    # Reference is to another Function
                    try:
                        ref_key = (function_id, function_ct.id, ref_function_id)
                        if ref_key not in existing_function_ref_keys:
                            function_refs_to_create.append(FunctionColumnReference(
                                function_id=function_id,
                                content_type_id=function_ct.id,
                                object_id=ref_function_id
                            ))
                            existing_function_ref_keys.add(ref_key)
                            stats['function_column_references'] += 1
                            self._debug(f"  Created: {function_name} -> {dep} (Function ref)")
                    except Exception as e:
                        self._debug(f"  Error creating FunctionColumnReference (function): {e}")
                else:
                    # Try database resolution for DatabaseField
                    column_obj = self._resolve_column(dep, DatabaseField, Function)
                    if column_obj:
                        try:
                            content_type = content_type_by_model[column_obj.__class__]
                            ref_key = (function_id, content_type.id, column_obj.id)
                            if ref_key not in existing_function_ref_keys:
                                function_refs_to_create.append(FunctionColumnReference(
                                    function_id=function_id,
                                    content_type_id=content_type.id,
                                    object_id=column_obj.id
                                ))
                                existing_function_ref_keys.add(ref_key)
                                stats['function_column_references'] += 1
                                self._debug(f"  Created: {function_name} -> {dep} (DB field)")
                        except Exception as e:
                            self._debug(f"  Error creating FunctionColumnReference (field): {e}")
                    else:
                        self._debug(f"  Could not resolve dependency: {dep}")
        if function_refs_to_create:
            FunctionColumnReference.objects.bulk_create(function_refs_to_create, batch_size=1000)

        # 2. Create TableCreationSourceTables
        self._debug("\n2. Creating TableCreationSourceTables...")
        self._debug(f"  Have {len(self.derived_table_sources)} derived tables with sources")
        self._debug(f"  Table name to DB ID mapping: {len(self.table_name_to_db_id)} entries")
        for name, info in self.table_name_to_db_id.items():
            self._debug(f"    {name} -> {info}")

        table_creation_function_ids = [
            table.table_creation_function_id
            for table in derived_tables_by_id.values()
            if table.table_creation_function_id
        ]
        existing_table_source_keys = set()
        if table_creation_function_ids:
            existing_table_source_keys = set(
                TableCreationSourceTable.objects.filter(
                    table_creation_function_id__in=table_creation_function_ids
                ).values_list('table_creation_function_id', 'content_type_id', 'object_id')
            )
        existing_edge_keys = set()
        if trail:
            existing_edge_keys = set(
                DataFlowEdge.objects.filter(trail=trail).values_list(
                    'trail_id',
                    'source_content_type_id',
                    'source_object_id',
                    'target_content_type_id',
                    'target_object_id',
                    'flow_type'
                )
            )
        table_sources_to_create = []
        data_flow_edges_to_create = []

        for derived_table_name, source_names in self.derived_table_sources.items():
            self._debug(f"  Processing derived table: {derived_table_name} with sources: {source_names}")
            try:
                derived_info = self.table_name_to_db_id.get(derived_table_name)
                if not derived_info:
                    self._debug(f"    WARNING: No table info found for {derived_table_name}")
                    continue
                if derived_info[0] != 'DerivedTable':
                    self._debug(f"    Skipping {derived_table_name} - not a DerivedTable (type={derived_info[0]})")
                    continue

                derived_table = derived_tables_by_id.get(derived_info[1])
                if not derived_table:
                    derived_table = DerivedTable.objects.filter(id=derived_info[1]).first()
                    if not derived_table:
                        continue
                    derived_tables_by_id[derived_table.id] = derived_table

                tcf = derived_table.table_creation_function
                if not tcf:
                    func_text = FunctionText.objects.create(
                        text=f"# Table creation function for {derived_table_name}",
                        language='python'
                    )
                    tcf = TableCreationFunction.objects.create(
                        name=f"create_{derived_table_name}",
                        function_text=func_text
                    )
                    derived_table.table_creation_function = tcf
                    derived_table.save()

                for source_name in source_names:
                    source_info = self.table_name_to_db_id.get(source_name)
                    if not source_info:
                        self._debug(f"    WARNING: Source table {source_name} not found in collector mappings")
                        try:
                            db_source = DatabaseTable.objects.filter(name=source_name).first()
                            if db_source:
                                source_info = ('DatabaseTable', db_source.id)
                                database_tables_by_id[db_source.id] = db_source
                                self._debug(f"    Found {source_name} in DatabaseTable: id={db_source.id}")
                            else:
                                dv_source = DerivedTable.objects.filter(name=source_name).first()
                                if dv_source:
                                    source_info = ('DerivedTable', dv_source.id)
                                    derived_tables_by_id[dv_source.id] = dv_source
                                    self._debug(f"    Found {source_name} in DerivedTable: id={dv_source.id}")
                        except Exception as e:
                            self._debug(f"    Error searching for {source_name}: {e}")

                    if not source_info:
                        continue

                    source_type, source_id = source_info
                    content_type = content_type_by_name.get(source_type)
                    if not content_type:
                        continue

                    source_table = (
                        database_tables_by_id.get(source_id)
                        if source_type == 'DatabaseTable'
                        else derived_tables_by_id.get(source_id)
                    )
                    source_label = source_table.name if source_table else source_name

                    source_ref_key = (tcf.id, content_type.id, source_id)
                    if source_ref_key not in existing_table_source_keys:
                        table_sources_to_create.append(TableCreationSourceTable(
                            table_creation_function_id=tcf.id,
                            content_type_id=content_type.id,
                            object_id=source_id
                        ))
                        existing_table_source_keys.add(source_ref_key)
                        stats['table_creation_source_tables'] += 1
                        self._debug(f"  Created: {derived_table_name} <- {source_name}")

                    if trail:
                        edge_key = (
                            trail.id,
                            content_type.id,
                            source_id,
                            derived_table_ct.id,
                            derived_table.id,
                            'DATA'
                        )
                        if edge_key not in existing_edge_keys:
                            data_flow_edges_to_create.append(DataFlowEdge(
                                trail=trail,
                                source_content_type_id=content_type.id,
                                source_object_id=source_id,
                                source_label=source_label,
                                target_content_type_id=derived_table_ct.id,
                                target_object_id=derived_table.id,
                                target_label=derived_table_name,
                                flow_type='DATA'
                            ))
                            existing_edge_keys.add(edge_key)
                            stats['data_flow_edges'] += 1
            except Exception as e:
                self._debug(f"  Error processing derived table {derived_table_name}: {e}")

        if table_sources_to_create:
            TableCreationSourceTable.objects.bulk_create(table_sources_to_create, batch_size=1000)
        if data_flow_edges_to_create:
            DataFlowEdge.objects.bulk_create(data_flow_edges_to_create, batch_size=1000)

        # 3. Create DerivedRowSourceReferences from object relationships
        self._debug("\n3. Creating DerivedRowSourceReferences...")
        child_derived_row_ids = []
        for child_obj_id, _, _ in self.object_relationships:
            child_info = self.python_obj_id_to_row_id.get(child_obj_id)
            if child_info and child_info[0] == 'DerivedTableRow':
                child_derived_row_ids.append(child_info[1])

        existing_derived_source_keys = set()
        if child_derived_row_ids:
            existing_derived_source_keys = set(
                DerivedRowSourceReference.objects.filter(
                    derived_row_id__in=child_derived_row_ids
                ).values_list('derived_row_id', 'content_type_id', 'object_id')
            )
        derived_sources_to_create = []

        for child_obj_id, parent_obj_id, rel_type in self.object_relationships:
            child_info = self.python_obj_id_to_row_id.get(child_obj_id)
            parent_info = self.python_obj_id_to_row_id.get(parent_obj_id)

            if child_info and parent_info:
                child_row_type, child_row_id = child_info
                parent_row_type, parent_row_id = parent_info

                if child_row_type == 'DerivedTableRow':
                    try:
                        content_type = derived_row_ct if parent_row_type == 'DerivedTableRow' else database_row_ct
                        ref_key = (child_row_id, content_type.id, parent_row_id)
                        if ref_key not in existing_derived_source_keys:
                            derived_sources_to_create.append(DerivedRowSourceReference(
                                derived_row_id=child_row_id,
                                content_type_id=content_type.id,
                                object_id=parent_row_id
                            ))
                            existing_derived_source_keys.add(ref_key)
                            stats['derived_row_source_references'] += 1
                            self._debug(f"  Created: row {child_row_id} <- row {parent_row_id}")
                    except Exception as e:
                        self._debug(f"  Error creating DerivedRowSourceReference: {e}")

        if derived_sources_to_create:
            DerivedRowSourceReference.objects.bulk_create(derived_sources_to_create, batch_size=1000)

        # 4. Create EvaluatedFunctionSourceValues from value computations
        self._debug("\n4. Creating EvaluatedFunctionSourceValues...")
        evaluated_function_ids = [
            eval_func_id for eval_func_id, _, _, _ in self.created_evaluated_functions
        ]
        existing_evaluated_source_keys = set()
        if evaluated_function_ids:
            existing_evaluated_source_keys = set(
                EvaluatedFunctionSourceValue.objects.filter(
                    evaluated_function_id__in=evaluated_function_ids
                ).values_list('evaluated_function_id', 'content_type_id', 'object_id')
            )
        evaluated_sources_to_create = []

        for eval_func_id, function_name, row_id, source_values in self.created_evaluated_functions:
            for source_ref in source_values:
                content_type = None
                object_id = None

                if isinstance(source_ref, dict):
                    ref_type = source_ref.get('type')
                    ref_id = source_ref.get('id')
                    if ref_type and ref_id and ref_type in content_type_by_name:
                        content_type = content_type_by_name[ref_type]
                        object_id = ref_id
                    elif source_ref.get('column_value_id'):
                        content_type = database_column_value_ct
                        object_id = source_ref['column_value_id']
                    elif source_ref.get('eval_func_id'):
                        content_type = evaluated_function_ct
                        object_id = source_ref['eval_func_id']

                if content_type is None or object_id is None:
                    source_obj = self._resolve_source_value(source_ref, trail)
                    if source_obj:
                        content_type = content_type_by_model[source_obj.__class__]
                        object_id = source_obj.id

                if content_type is None or object_id is None:
                    continue

                try:
                    ref_key = (eval_func_id, content_type.id, object_id)
                    if ref_key not in existing_evaluated_source_keys:
                        evaluated_sources_to_create.append(EvaluatedFunctionSourceValue(
                            evaluated_function_id=eval_func_id,
                            content_type_id=content_type.id,
                            object_id=object_id
                        ))
                        existing_evaluated_source_keys.add(ref_key)
                        stats['evaluated_function_source_values'] += 1
                        self._debug(f"  Created: eval_func {eval_func_id} <- source")
                except Exception as e:
                    self._debug(f"  Error creating EvaluatedFunctionSourceValue: {e}")

        if evaluated_sources_to_create:
            EvaluatedFunctionSourceValue.objects.bulk_create(evaluated_sources_to_create, batch_size=1000)

        self._debug("\n=== Finalization Complete ===")
        for key, value in stats.items():
            self._debug(f"  {key}: {value}")

        return stats

    def _resolve_column(self, dep_string, *model_classes):
        """Resolve a dependency string to a column object"""
        cache_key = (dep_string, tuple(model_class.__name__ for model_class in model_classes))
        if cache_key in self._column_resolution_cache:
            return self._column_resolution_cache[cache_key]

        # Parse dependency: "base.COLUMN" or "TABLE.COLUMN" or just "COLUMN"
        # Clean up common patterns
        clean_dep = dep_string
        for prefix in ['base.', 'self.', 'row.']:
            clean_dep = clean_dep.replace(prefix, '')

        parts = clean_dep.split('.')
        column_name = parts[-1] if parts else clean_dep
        table_hint = parts[0] if len(parts) > 1 else None

        # Try to resolve with the collector's mappings first
        if table_hint:
            table_info = self.table_name_to_db_id.get(table_hint)
            if not table_info:
                # Try case-insensitive table lookup (precise - same name, different case)
                for tname, tinfo in self.table_name_to_db_id.items():
                    if tname.lower() == table_hint.lower():
                        table_info = tinfo
                        break
            if table_info:
                table_type, table_id = table_info
                from pybirdai.models import DatabaseTable, DerivedTable, DatabaseField
                try:
                    if table_type == 'DatabaseTable':
                        table = DatabaseTable.objects.get(id=table_id)
                        # Case-insensitive exact field search
                        field = table.database_fields.filter(name__iexact=column_name).first()
                        if field:
                            self._column_resolution_cache[cache_key] = field
                            return field
                    else:
                        table = DerivedTable.objects.get(id=table_id)
                        # Check for function with exact name match (case-insensitive)
                        func = table.derived_functions.filter(name__iexact=column_name).first()
                        if not func:
                            # Also try matching the method name part (e.g., "ClassName.method" -> "method")
                            func = table.derived_functions.filter(name__iendswith=f'.{column_name}').first()
                        if func:
                            self._column_resolution_cache[cache_key] = func
                            return func
                except Exception as e:
                    self._debug(f"  Error resolving column {column_name} in table {table_hint}: {e}")

        # Fallback to generic search (precise matching only)
        for model_class in model_classes:
            try:
                # First try exact match
                obj = model_class.objects.filter(name=column_name).first()
                if obj:
                    self._column_resolution_cache[cache_key] = obj
                    return obj

                # Try case-insensitive exact match
                obj = model_class.objects.filter(name__iexact=column_name).first()
                if obj:
                    self._column_resolution_cache[cache_key] = obj
                    return obj
            except Exception:
                pass

        self._column_resolution_cache[cache_key] = None
        return None

    def _resolve_source_value(self, source_ref, trail):
        """Resolve a source value reference to a database object"""
        from pybirdai.models import DatabaseColumnValue, EvaluatedFunction, DatabaseRow, DerivedTableRow

        # source_ref could be a dict with info or just a value
        if isinstance(source_ref, dict):
            # Handle format: {'type': 'TypeName', 'id': 123}
            if 'type' in source_ref and 'id' in source_ref:
                ref_type = source_ref['type']
                ref_id = source_ref['id']
                try:
                    if ref_type == 'DatabaseColumnValue':
                        return DatabaseColumnValue.objects.get(id=ref_id)
                    elif ref_type == 'EvaluatedFunction':
                        return EvaluatedFunction.objects.get(id=ref_id)
                    elif ref_type == 'DatabaseRow':
                        return DatabaseRow.objects.get(id=ref_id)
                    elif ref_type == 'DerivedTableRow':
                        return DerivedTableRow.objects.get(id=ref_id)
                except Exception:
                    pass

            if 'column_value_id' in source_ref:
                try:
                    return DatabaseColumnValue.objects.get(id=source_ref['column_value_id'])
                except Exception:
                    pass
            if 'eval_func_id' in source_ref:
                try:
                    return EvaluatedFunction.objects.get(id=source_ref['eval_func_id'])
                except Exception:
                    pass

        return None


# Context-local instance for the current execution
_current_collector = ContextVar('pybirdai_lineage_collector', default=None)


def get_collector():
    """Get the current lineage collector"""
    collector = _current_collector.get()
    if collector is None:
        collector = LineageCollector()
        _current_collector.set(collector)
    return collector


def reset_collector():
    """Reset the lineage collector for a new execution"""
    collector = LineageCollector()
    _current_collector.set(collector)
    return collector


def finalize_collector(trail, metadata_trail):
    """Finalize the current collector and return stats"""
    collector = _current_collector.get()
    if collector:
        return collector.finalize(trail, metadata_trail)
    return {}
