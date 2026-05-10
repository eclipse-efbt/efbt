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


from django.apps import apps
from pybirdai.process_steps.pybird.csv_converter import CSVConverter
from pybirdai.models import (
    Trail, MetaDataTrail, DatabaseTable, DerivedTable,
    DatabaseField, Function, FunctionText, TableCreationFunction,
    PopulatedDataBaseTable, EvaluatedDerivedTable, DatabaseRow,
    DerivedTableRow, DatabaseColumnValue, EvaluatedFunction,
    AortaTableReference, FunctionColumnReference, DerivedRowSourceReference,
    EvaluatedFunctionSourceValue, TableCreationSourceTable,
    TableCreationFunctionColumn,
    CalculationUsedRow, CalculationUsedField,
    # Enhanced lineage models
    TransformationStep, TransformationStepInput, TransformationStepOutput,
    CalculationChain, CalculationChainStep, DataFlowEdge,
    CellLineage, CellSourceRow
)
from datetime import datetime
from django.contrib.contenttypes.models import ContentType

import importlib
import os
import re
import time
from pybirdai.process_steps.pybird.lineage_collector import get_collector, reset_collector, finalize_collector


class OrchestrationWithLineage:
	# Class variable to track initialized objects
	_initialized_objects = set()

	# AORTA lineage tracking
	def __init__(self):
		self.trail = None
		self.metadata_trail = None
		self.current_populated_tables = {}  # Map table names to PopulatedTable instances
		self.current_rows = {}  # Track current row being processed
		self.lineage_enabled = True  # Can be disabled for performance
		self.evaluated_functions_cache = {}  # Cache to track evaluated functions per row
		self.object_contexts = {}  # Map object id -> derived row context
		self.debug_lineage = os.environ.get('PYBIRDAI_DEBUG_LINEAGE', '').lower() in {'1', 'true', 'yes', 'on'}
		self._content_type_cache = {}
		self._django_model_cache = {}
		self._table_lookup_cache = {}
		self._database_field_cache = {}
		self._function_cache = {}
		self._function_lookup_cache = {}
		self._dependency_resolution_cache = {}
		self._function_column_reference_keys = set()
		self._calculation_used_row_keys = set()
		self._calculation_used_field_keys = set()
		self._derived_row_source_reference_keys = set()
		self._evaluated_function_source_value_keys = set()
		self._table_creation_function_cache = {}
		self._table_creation_function_column_keys = set()
		self._table_creation_source_table_keys = set()
		self._derived_row_cache = {}
		self._database_row_cache = {}
		self._derived_row_by_id_cache = {}
		self._evaluated_function_lookup_cache = {}
		self._value_object_cache = {}
		self._relationship_tracked_keys = set()
		self._transitive_used_object_keys = set()
		self._data_flow_edge_cache = {}
		self._new_database_table_ids = set()
		self._new_derived_table_ids = set()

		# Get the current collector (don't reset here - reset should be done at start of execution)
		self.collector = get_collector()

		# Note: Do not automatically register this instance globally
		# Global registration should be done explicitly when setting up lineage tracking

	def _debug(self, message):
		if self.debug_lineage:
			print(message)

	def _get_content_type(self, model_or_obj):
		model_class = model_or_obj if isinstance(model_or_obj, type) else model_or_obj.__class__
		content_type = self._content_type_cache.get(model_class)
		if content_type is None:
			content_type = ContentType.objects.get_for_model(model_class)
			self._content_type_cache[model_class] = content_type
		return content_type

	def _get_django_model(self, table_name):
		if table_name not in self._django_model_cache:
			try:
				self._django_model_cache[table_name] = apps.get_model('pybirdai', table_name)
			except LookupError:
				self._django_model_cache[table_name] = None
		return self._django_model_cache[table_name]

	def _get_or_create_database_field(self, table, field_name):
		cache_key = (table.id, field_name)
		field = self._database_field_cache.get(cache_key)
		if field is not None:
			return field

		field = DatabaseField.objects.filter(table=table, name=field_name).first()
		if field is None:
			field = DatabaseField.objects.create(name=field_name, table=table)
		self._database_field_cache[cache_key] = field
		return field

	def _get_or_create_database_fields(self, table, field_names):
		"""Return DatabaseField objects for field_names, creating missing fields in batches."""
		unique_field_names = list(dict.fromkeys(name for name in field_names if name))
		fields_by_name = {}
		missing_names = []

		for field_name in unique_field_names:
			cache_key = (table.id, field_name)
			field = self._database_field_cache.get(cache_key)
			if field is None:
				missing_names.append(field_name)
			else:
				fields_by_name[field_name] = field

		if missing_names:
			existing_fields = DatabaseField.objects.filter(
				table=table,
				name__in=missing_names
			)
			for field in existing_fields:
				fields_by_name[field.name] = field
				self._database_field_cache[(table.id, field.name)] = field

			create_names = [name for name in missing_names if name not in fields_by_name]
			if create_names:
				created_fields = DatabaseField.objects.bulk_create(
					[DatabaseField(name=name, table=table) for name in create_names],
					batch_size=500
				)
				if any(field.id is None for field in created_fields):
					created_fields = list(DatabaseField.objects.filter(
						table=table,
						name__in=create_names
					))
				for field in created_fields:
					fields_by_name[field.name] = field
					self._database_field_cache[(table.id, field.name)] = field

		return fields_by_name

	def _split_numeric_value(self, value):
		numeric_value = None
		string_value = None

		if value is not None:
			try:
				numeric_value = float(value)
			except (ValueError, TypeError):
				string_value = str(value)

		return numeric_value, string_value

	def _remember_value_object(self, value, value_object):
		if value is None or value_object is None:
			return

		self._value_object_cache.setdefault(('str', str(value)), value_object)
		try:
			self._value_object_cache.setdefault(('num', round(float(value), 4)), value_object)
		except (ValueError, TypeError):
			pass

	def _get_remembered_value_object(self, value):
		if value is None:
			return None

		value_object = self._value_object_cache.get(('str', str(value)))
		if value_object is not None:
			return value_object

		try:
			return self._value_object_cache.get(('num', round(float(value), 4)))
		except (ValueError, TypeError):
			return None

	def init_with_lineage(self, theObject, execution_name=None):
		"""Initialize object with AORTA lineage tracking"""
		# Create trail if not exists
		if not self.trail and self.lineage_enabled:
			execution_name = execution_name or f"Execution_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
			self.metadata_trail = MetaDataTrail.objects.create()
			self.trail = Trail.objects.create(
				name=execution_name,
				metadata_trail=self.metadata_trail
			)
			print(f"Created AORTA Trail: {self.trail.name}")

		# Track object initialization in AORTA
		if self.lineage_enabled:
			self._track_object_initialization(theObject)

		# Perform standard initialization
		return self.init(theObject)

	def _is_django_model(self, table_name):
		"""Check if a table name corresponds to a Django model"""
		return self._get_django_model(table_name) is not None

	def _is_business_data_class(self, class_name):
		"""
		Check if a class name represents a business/data class that should be tracked as a table source.
		This replaces hardcoded table name checks with a generic pattern-based approach.
		"""
		if not class_name or class_name.startswith('_'):
			return False

		# Explicit table classes
		if class_name.endswith('_Table'):
			return True

		# Skip common non-data class patterns
		skip_patterns = [
			'Cell_',      # Report cell classes
			'Wrapper',    # Wrapper classes
			'Iterator',   # Iterator classes
			'Exception',  # Exception classes
			'Error',      # Error classes
		]
		for pattern in skip_patterns:
			if pattern in class_name:
				return False

		# Skip built-in types
		builtin_types = {'str', 'int', 'float', 'bool', 'list', 'dict', 'tuple', 'set', 'NoneType', 'type'}
		if class_name in builtin_types:
			return False

		# Classes with report-style prefixes (F_XX_XX_) that also have a suffix are likely wrapper classes
		# e.g., F_05_01_REF_FINREP_3_0_UnionItem is a wrapper, not a source table
		import re
		report_prefix_pattern = r'^F_\d{2}_\d{2}_[A-Z]+_[A-Z]+_\d+_\d+_'
		if re.match(report_prefix_pattern, class_name):
			# This looks like a report-prefixed class (wrapper), not a base data class
			return False

		# Business data classes typically:
		# - Have underscores separating words (snake_case style names)
		# - Don't have the F_XX_XX report prefix pattern
		# - Are not empty/simple utility classes
		if '_' in class_name and not class_name.startswith('F_'):
			return True

		# Also accept CamelCase business class names without underscores
		# if they don't match other patterns
		if class_name[0].isupper() and not class_name.startswith('F_'):
			return True

		return False

	def _track_object_initialization(self, obj):
		"""Track object in AORTA metadata trail"""
		if not obj:
			return

		obj_class_name = obj.__class__.__name__

		# Check if this is a table object
		if hasattr(obj, '__class__') and obj_class_name.endswith('_Table'):
			# Extract meaningful table name
			table_name = obj_class_name.replace('_Table', '')

			# Skip dummy objects
			if table_name == 'Dummy':
				return

			# Check if we already have a populated table for this name in the current trail
			if table_name in self.current_populated_tables:
				self._debug(f"Reusing existing table for: {table_name}")
				# Still check if DerivedTable needs table_creation_function linked
				populated_table = self.current_populated_tables[table_name]
				if hasattr(populated_table, 'table') and isinstance(populated_table.table, DerivedTable):
					if populated_table.table.table_creation_function is None:
						self._analyze_table_creation_functions(obj, populated_table.table)
				return

			# Determine if this is a Django model or a derived table
			is_django_model = self._is_django_model(table_name)

			# Check for existing table with same name in this trail
			aorta_table = None
			populated_table = None
			table_exists = False

			if self.trail:
				if is_django_model:
					# Look for existing DatabaseTable with same name in this trail
					existing_populated = PopulatedDataBaseTable.objects.filter(
						trail=self.trail,
						table__name=table_name
					).select_related('table').first()
					if existing_populated:
						aorta_table = existing_populated.table
						populated_table = existing_populated
						table_exists = True
						self._debug(f"Found existing DatabaseTable for: {table_name}")
				else:
					# Look for existing DerivedTable with same name in this trail
					existing_evaluated = EvaluatedDerivedTable.objects.filter(
						trail=self.trail,
						table__name=table_name
					).select_related('table').first()
					if existing_evaluated:
						aorta_table = existing_evaluated.table
						populated_table = existing_evaluated
						table_exists = True
						self._debug(f"Found existing DerivedTable for: {table_name}")

			# Create new table if not found
			if not aorta_table:
				if is_django_model:
					# Create DatabaseTable for Django model classes
					aorta_table = DatabaseTable.objects.create(name=table_name)
					self._new_database_table_ids.add(aorta_table.id)
				else:
					# Create DerivedTable for non-Django model classes
					aorta_table = DerivedTable.objects.create(name=table_name)
					self._new_derived_table_ids.add(aorta_table.id)
				self._debug(f"Created new table for: {table_name}")

			# Add to metadata trail if not already added
			if self.metadata_trail:
				table_type = 'DatabaseTable' if is_django_model else 'DerivedTable'
				existing_ref = AortaTableReference.objects.filter(
					metadata_trail=self.metadata_trail,
					table_content_type=table_type,
					table_id=aorta_table.id
				).exists()

				if not existing_ref:
					AortaTableReference.objects.create(
						metadata_trail=self.metadata_trail,
						table_content_type=table_type,
						table_id=aorta_table.id
					)

			# Create populated/evaluated table only if we didn't find an existing one
			if self.trail and not populated_table:
				if is_django_model:
					# Create PopulatedDataBaseTable for Django model tables
					populated_table = PopulatedDataBaseTable.objects.create(
						trail=self.trail,
						table=aorta_table
					)
				else:
					# Create EvaluatedDerivedTable for derived tables
					populated_table = EvaluatedDerivedTable.objects.create(
						trail=self.trail,
						table=aorta_table
					)
			elif not self.trail:
				print(f"Warning: No trail available for populated table {table_name}")
				return

			# Store the populated table in our tracking dictionary
			self.current_populated_tables[table_name] = populated_table

			# Register table with the lineage collector
			table_type = 'DatabaseTable' if is_django_model else 'DerivedTable'
			self.collector.register_table(table_type, aorta_table.id, table_name)

			# Track table columns/fields only if this is a new table
			if not table_exists:
				self._track_table_columns(obj, aorta_table)

			# Analyze table creation functions (calc_ methods)
			# Run for new tables OR if existing DerivedTable doesn't have table_creation_function set
			needs_tcf_analysis = not table_exists
			if not needs_tcf_analysis and isinstance(aorta_table, DerivedTable):
				if aorta_table.table_creation_function is None:
					needs_tcf_analysis = True

			if needs_tcf_analysis:
				self._analyze_table_creation_functions(obj, aorta_table)

			# print(f"Tracked table initialization: {table_name}")

	def _track_table_columns(self, table_obj, aorta_table):
		"""Track columns/fields in a table"""
		try:
			# Only track fields for DatabaseTable instances
			# For DerivedTable instances, columns are tracked as Functions
			if isinstance(aorta_table, DatabaseTable):
				fields_to_track = []
				table_name = aorta_table.name

				# For Django models, use the model's _meta.fields
				if self._is_django_model(table_name):
					try:
						from django.apps import apps
						model_class = apps.get_model('pybirdai', table_name)
						fields_to_track = [field.name for field in model_class._meta.fields]
						self._debug(f"Using Django model fields for {table_name}: {len(fields_to_track)} fields")
					except Exception as e:
						print(f"Error getting Django model fields for {table_name}: {e}")
						fields_to_track = []
				else:
					# For non-Django tables, detect column methods from the row objects they contain
					fields_to_track = self._detect_table_fields_from_row_type(table_obj)

				# Create DatabaseField instances
				self._get_or_create_database_fields(aorta_table, fields_to_track)
		except Exception as e:
			print(f"Error tracking columns for {aorta_table.name}: {e}")

	def _detect_table_fields_from_row_type(self, table_obj):
		"""Detect fields by examining the row object type that this table contains"""
		try:
			# For derived tables, try to determine the row object type
			table_class_name = table_obj.__class__.__name__

			# Pattern: F_01_01_REF_FINREP_3_0_Table contains F_01_01_REF_FINREP_3_0 objects
			if table_class_name.endswith('_Table'):
				row_class_name = table_class_name[:-6]  # Remove '_Table' suffix

				# Try to import and inspect the row class
				try:
					# Look for the row class in the same module
					table_module = table_obj.__class__.__module__
					module = __import__(table_module, fromlist=[row_class_name])

					if hasattr(module, row_class_name):
						row_class = getattr(module, row_class_name)
						# Create a temporary instance to inspect its methods
						try:
							row_instance = row_class()
							fields = self._detect_column_methods(row_instance)
							print(f"Detected {len(fields)} fields from row type {row_class_name}: {fields[:5]}...")
							return fields
						except Exception as e:
							print(f"Could not instantiate {row_class_name}: {e}")

				except Exception as e:
					print(f"Could not find row class {row_class_name}: {e}")

			# Fallback: try to detect from table attributes that might be lists of row objects
			for attr_name in dir(table_obj):
				if not attr_name.startswith('_'):
					attr_value = getattr(table_obj, attr_name, None)
					if isinstance(attr_value, list) and len(attr_value) > 0:
						# Try to get column methods from the first item in the list
						first_item = attr_value[0]
						fields = self._detect_column_methods(first_item)
						if fields:
							print(f"Detected {len(fields)} fields from list attribute {attr_name}: {fields[:5]}...")
							return fields

			print(f"No fields detected for non-Django table {table_class_name}")
			return []

		except Exception as e:
			print(f"Error detecting fields for table {table_obj.__class__.__name__}: {e}")
			return []

	def _detect_column_methods(self, table_obj):
		"""Detect column methods in non-Django table objects using robust approaches"""
		import inspect
		column_methods = set()

		# Get all callable methods
		methods = [name for name in dir(table_obj)
				  if (not name.startswith('_') and
					  callable(getattr(table_obj, name, None)))]

		for method_name in methods:
			try:
				method = getattr(table_obj, method_name)

				# Approach 1: Check for @lineage decorator (most reliable)
				if self._has_lineage_decorator(method):
					column_methods.add(method_name)
					continue

				# Approach 2: Method signature and naming patterns
				if self._is_likely_column_method(method, method_name):
					column_methods.add(method_name)

			except Exception:
				continue

		# Filter out known infrastructure methods
		excluded_methods = {'init', 'metric_value'}
		excluded_prefixes = {'calc_'}

		final_methods = []
		for method_name in column_methods:
			if (method_name not in excluded_methods and
				not any(method_name.startswith(prefix) for prefix in excluded_prefixes)):
				final_methods.append(method_name)

		print(f"Detected {len(final_methods)} column methods for non-Django table: {final_methods[:5]}...")
		return final_methods

	def _has_lineage_decorator(self, method):
		"""Check if a method has the @lineage decorator"""
		try:
			# Check if method has wrapper attributes indicating decoration
			if hasattr(method, '__wrapped__'):
				return True

			# Check method name or qualname for lineage wrapper signs
			if hasattr(method, '__qualname__') and 'lineage' in str(method.__qualname__):
				return True

			# Check for common decorator attributes
			if hasattr(method, '__dict__') and any('lineage' in str(key) for key in method.__dict__):
				return True

			return False
		except:
			return False

	def _is_likely_column_method(self, method, method_name):
		"""Check if method is likely a column method based on signature and naming"""
		try:
			import inspect

			# Check method name - should be all uppercase (common column pattern)
			if not method_name.isupper():
				return False

			# Check method signature - should only have 'self' parameter
			sig = inspect.signature(method)
			params = list(sig.parameters.keys())
			if len(params) != 1 or params[0] != 'self':
				return False

			# Check return type annotation if present
			if sig.return_annotation != inspect.Signature.empty:
				valid_types = [int, str, 'int', 'str']
				if sig.return_annotation in valid_types:
					return True

			# Check docstring for enumeration mentions (common in column methods)
			if method.__doc__ and 'enumeration' in method.__doc__.lower():
				return True

			return True  # If all checks pass, likely a column method

		except Exception:
			return False

	def init(self,theObject):
		# Check if this object has already been initialized
		object_id = id(theObject)
		if object_id in self.__class__._initialized_objects:
			self._debug(f"Object of type {theObject.__class__.__name__} already initialized, skipping.")
			# Even if we're skipping full initialization, we still need to ensure references are set
			self._ensure_references_set(theObject)
			return

		# Mark this object as initialized
		self.__class__._initialized_objects.add(object_id)

		# Check if we have lineage tracking enabled globally and this looks like a table
		from pybirdai.annotations.decorators import _lineage_context
		global_orchestration = _lineage_context.get('orchestration')
		if global_orchestration and global_orchestration is not self:
			try:
				trail_id = global_orchestration.trail.id if global_orchestration.trail else None
				metadata_trail_id = global_orchestration.metadata_trail.id if global_orchestration.metadata_trail else None
				if (
					(trail_id and not Trail.objects.filter(id=trail_id).exists()) or
					(metadata_trail_id and not MetaDataTrail.objects.filter(id=metadata_trail_id).exists())
				):
					global_orchestration = None
			except Exception:
				global_orchestration = None

		if (global_orchestration and
			hasattr(theObject, '__class__') and
			theObject.__class__.__name__.endswith('_Table')):

			self._debug(f"Using global orchestration for {theObject.__class__.__name__}")
			self._debug(f"  Global Trail: {global_orchestration.trail.id if global_orchestration.trail else None}")
			self._debug(f"  Global MetaDataTrail: {global_orchestration.metadata_trail.id if global_orchestration.metadata_trail else None}")

			# This is a table and we have lineage tracking - track it
			if global_orchestration and global_orchestration.lineage_enabled:
				global_orchestration._track_object_initialization(theObject)

		# Set up references for the object (use global orchestration if available)
		if global_orchestration and global_orchestration.lineage_enabled:
			# Use the global orchestration for reference setup to maintain lineage context
			global_orchestration._ensure_references_set(theObject)
		else:
			# Fallback to local orchestration
			self._ensure_references_set(theObject)

	def _ensure_references_set(self, theObject):
		"""
		Ensure that all table references are properly set for the object.
		This is called both during full initialization and when initialization is skipped.
		"""
		references = [method for method in dir(theObject.__class__) if not callable(
		getattr(theObject.__class__, method)) and not method.startswith('__')]
		for eReference in references:
			if eReference.endswith("Table"):
				# Only set the reference if it's currently None
				if getattr(theObject, eReference) is None:
					from django.apps import apps
					table_name = eReference.split('_Table')[0]

					# For ANCRDT intermediate tables - skip Django model lookup, create directly
					is_ancrdt_intermediate = (table_name.startswith('ANCRDT_') and
						any(pattern in table_name for pattern in ['Union', 'Loans_and_advances', '_filtered_', '_aggregated_']))

					relevant_model = None
					if not is_ancrdt_intermediate:
						try:
							relevant_model = apps.get_model('pybirdai',table_name)
						except LookupError:
							self._debug("LookupError: " + table_name)

					if relevant_model:
						self._debug("relevant_model: " + str(relevant_model))
						newObject = relevant_model.objects.all()
						self._debug("newObject: " + str(newObject))
						if newObject.exists():
							setattr(theObject,eReference,newObject)
							# Original CSV persistence
							CSVConverter.persist_object_as_csv(newObject,True);
							
							# Enhanced lineage tracking - track when tables are created but distinguish from usage tracking
							if self.debug_lineage and self.lineage_enabled and self.trail and hasattr(newObject, '__iter__'):
								try:
									row_count = newObject.count()
									if row_count:
										self._debug(f"Table Created: {row_count} {table_name} objects available")
								except Exception as e:
									self._debug(f"Warning: Could not process {table_name} objects for lineage: {e}")

					else:
						newObject = OrchestrationWithLineage.createObjectFromReferenceType(eReference);

						operations = [method for method in dir(newObject.__class__) if callable(
							getattr(newObject.__class__, method)) and not method.startswith('__')]

						for operation in operations:
							if operation == "init":
								try:
									getattr(newObject, operation)()

									# Check if lineage tracking is enabled and track data after initialization
									from pybirdai.annotations.decorators import _lineage_context
									orchestration = _lineage_context.get('orchestration')
									if (orchestration and orchestration.lineage_enabled and
										hasattr(newObject, '__class__') and
										newObject.__class__.__name__.endswith('_Table')):

										# Debug: print orchestration state
										self._debug(f"Orchestration for {newObject.__class__.__name__}:")
										self._debug(f"  Trail: {orchestration.trail.id if orchestration.trail else None}")
										self._debug(f"  MetaDataTrail: {orchestration.metadata_trail.id if orchestration.metadata_trail else None}")

										# First track the table itself if not already tracked
										if orchestration.metadata_trail:
											orchestration._track_object_initialization(newObject)
										else:
											self._debug(f"WARNING: No metadata_trail for {newObject.__class__.__name__}")

										# Track any data that was populated during initialization
										table_name = newObject.__class__.__name__.replace('_Table', '')
										for attr_name in dir(newObject):
											if (not attr_name.startswith('_') and
												hasattr(newObject, attr_name)):
												attr_value = getattr(newObject, attr_name)
												if isinstance(attr_value, list) and len(attr_value) > 0:
													# CRITICAL FIX: DO NOT auto-track derived table data during initialization
													# Only track when explicitly used in calculations
													if orchestration.metadata_trail:
														self._debug(f"Found {len(attr_value)} items in {table_name}_{attr_name} (not tracking as used yet)")
													else:
														self._debug(f"WARNING: Cannot process data for {table_name}_{attr_name} - no metadata_trail")

								except Exception as e:
									print(f"Could not call function called {operation}: {e}")

						setattr(theObject,eReference,newObject)

	@classmethod
	def reset_initialization(cls):
		"""
		Reset the initialization tracking.
		This can be useful for testing or when re-initialization is required.
		"""
		cls._initialized_objects.clear()
		print("Initialization tracking has been reset.")

	@classmethod
	def is_initialized(cls, obj):
		"""
		Check if an object has been initialized.

		Args:
			obj: The object to check

		Returns:
			bool: True if the object has been initialized, False otherwise
		"""
		return id(obj) in cls._initialized_objects

	@staticmethod
	def createObjectFromReferenceType(eReference):
		try:
			# First try the old output_tables location for backwards compatibility
			try:
				cls = getattr(importlib.import_module('pybirdai.process_steps.filter_code.output_tables'), eReference)
				new_object = cls()
				return new_object
			except (ImportError, AttributeError):
				pass

			# If that fails, try to find the class in the logic files
			# Extract the report prefix from the class name (e.g., F_05_01_REF_FINREP_3_0 from F_05_01_REF_FINREP_3_0_Other_loans_Table)
			if "_" in eReference:
				parts = eReference.split("_")
				# Look for report pattern: F_XX_XX_REF_FINREP_X_X
				if len(parts) >= 7 and parts[0] == "F" and parts[3] == "REF" and parts[4] == "FINREP":
					# Extract report prefix (first 7 parts: F_05_01_REF_FINREP_3_0)
					report_prefix = "_".join(parts[:7])
					logic_module_name = f"pybirdai.process_steps.filter_code.{report_prefix}_logic"

					try:
						module = importlib.import_module(logic_module_name)
						cls = getattr(module, eReference)
						new_object = cls()
						return new_object
					except (ImportError, AttributeError) as e:
						print(f"Could not find {eReference} in {logic_module_name}: {e}")

				# Check for ANCRDT pattern: ANCRDT_INSTRMNT_C_1_UnionTable
				if len(parts) >= 2 and parts[0] == "ANCRDT":
					# Extract report prefix by finding the _C_<number> pattern
					# Pattern: ANCRDT_INSTRMNT_C_1_Loans_and_advances_Table -> ANCRDT_INSTRMNT_C_1
					# Pattern: ANCRDT_INSTRMNT_C_1_UnionTable -> ANCRDT_INSTRMNT_C_1
					match = re.search(r'(ANCRDT_\w+_C_\d+)', eReference)
					if match:
						report_prefix = match.group(1)
					else:
						# Fallback to old suffix removal logic for backward compatibility
						report_prefix = eReference
						for suffix in ['_UnionTable', '_Table', '_UnionItem', '_Base']:
							if report_prefix.endswith(suffix):
								report_prefix = report_prefix[:-len(suffix)]
								break

					# Import from filter_code (executable production code)
					logic_module_name = f"pybirdai.process_steps.filter_code.{report_prefix}_logic"

					try:
						module = importlib.import_module(logic_module_name)
						cls = getattr(module, eReference)
						new_object = cls()
						return new_object
					except (ImportError, AttributeError) as e:
						print(f"Could not find {eReference} in {logic_module_name}: {e}")

			# If all else fails, print error
			print(f"Error: Could not find class {eReference} in any expected location")
			return None
		except Exception as e:
			print(f"Error creating object from reference {eReference}: {e}")
			return None

	# AORTA Lineage Tracking Methods

	def track_function_execution(self, function_name, source_columns, result_column=None, source_code=None):
		"""Track the execution of a function in AORTA"""
		if not self.lineage_enabled or not self.trail:
			return

		# Create or get derived table for this function
		class_name = function_name.split('.')[0] if '.' in function_name else 'DynamicFunctions'

		# For individual objects, use their parent table name
		if not class_name.endswith('_Table'):
			parent_table_name = self._get_parent_table_name(class_name)
			if parent_table_name and parent_table_name != class_name:
				class_name = parent_table_name

		populated_table = self.current_populated_tables.get(class_name)
		if populated_table and hasattr(populated_table, 'table') and isinstance(populated_table.table, DerivedTable):
			derived_table = populated_table.table
		else:
			existing_evaluated = EvaluatedDerivedTable.objects.filter(
				trail=self.trail,
				table__name=class_name
			).select_related('table').first()
			if existing_evaluated:
				derived_table = existing_evaluated.table
				self.current_populated_tables[class_name] = existing_evaluated
			else:
				derived_table = None

		if derived_table is None:
			derived_table = DerivedTable.objects.create(name=class_name)
			self._new_derived_table_ids.add(derived_table.id)

			# Add to metadata trail
			if self.metadata_trail:
				AortaTableReference.objects.get_or_create(
					metadata_trail=self.metadata_trail,
					table_content_type='DerivedTable',
					table_id=derived_table.id
				)

			# Create EvaluatedDerivedTable
			if self.trail:
				evaluated_table = EvaluatedDerivedTable.objects.create(
					trail=self.trail,
					table=derived_table
				)
				self.current_populated_tables[class_name] = evaluated_table

		# Check if Function already exists for this name and table
		function_key = (derived_table.id, function_name)
		function = self._function_cache.get(function_key)
		if function is None and derived_table.id not in self._new_derived_table_ids:
			function = Function.objects.filter(
				name=function_name,
				table=derived_table
			).first()

		if function is None:
			# Create new Function record
			function_text = FunctionText.objects.create(
				text=source_code or function_name,
				language='python'
			)

			function = Function.objects.create(
				name=function_name,
				function_text=function_text,
				table=derived_table
			)
			# print(f"Created new function: {function_name}")
		self._function_cache[function_key] = function
		self._function_lookup_cache[(function_name, derived_table.id)] = function

		# Note: TableCreationFunction instances are now created in _analyze_table_creation_functions
		# during table initialization, which analyzes class variables for source tables

		# Register function with the lineage collector (for deferred resolution)
		self.collector.register_function(function.id, function_name, class_name, source_columns or [])

		# Track column references using the enhanced method
		# Always try to create references (ensure_function_column_references checks for existing)
		if source_columns:
			self.ensure_function_column_references(function, source_columns)

		return function

	def track_polymorphic_function_execution(self, function_name, base_class_name, 
											source_columns, result_column=None, 
											wrapper_obj=None, base_obj=None):
		"""Track the execution of a polymorphic function in AORTA"""
		if not self.lineage_enabled or not self.trail:
			return

		# Use the wrapper class name for the table/function organization
		# CRITICAL FIX: Extract the FULL wrapper class name, not just the first part
		# For function_name like "F_05_01_REF_FINREP_3_0_UnionItem.GRSS_CRRYNG_AMNT"
		# we want "F_05_01_REF_FINREP_3_0_UnionItem", not "F_05_01_REF_FINREP_3_0"
		if '.' in function_name:
			# Get everything before the last dot (the method name)
			wrapper_class_name = function_name.rsplit('.', 1)[0]
		else:
			wrapper_class_name = 'DynamicFunctions'
		
		self._debug(f"track_polymorphic_function_execution: function_name={function_name}, wrapper_class_name={wrapper_class_name}, base_class_name={base_class_name}")
		
		# Create more descriptive function name that includes the base class
		polymorphic_function_name = f"{function_name}@{base_class_name}"

		# Create or get derived table for the wrapper class
		populated_table = self.current_populated_tables.get(wrapper_class_name)
		if populated_table and hasattr(populated_table, 'table') and isinstance(populated_table.table, DerivedTable):
			derived_table = populated_table.table
		else:
			existing_evaluated = EvaluatedDerivedTable.objects.filter(
				trail=self.trail,
				table__name=wrapper_class_name
			).select_related('table').first()
			if existing_evaluated:
				derived_table = existing_evaluated.table
				self.current_populated_tables[wrapper_class_name] = existing_evaluated
			else:
				derived_table = None

		if derived_table is None:
			derived_table = DerivedTable.objects.create(name=wrapper_class_name)
			self._new_derived_table_ids.add(derived_table.id)

			# Add to metadata trail
			if self.metadata_trail:
				AortaTableReference.objects.get_or_create(
					metadata_trail=self.metadata_trail,
					table_content_type='DerivedTable',
					table_id=derived_table.id
				)

			# Create EvaluatedDerivedTable
			if self.trail:
				evaluated_table = EvaluatedDerivedTable.objects.create(
					trail=self.trail,
					table=derived_table
				)
				self.current_populated_tables[wrapper_class_name] = evaluated_table

		# Check if Function already exists for this polymorphic name and table
		function_key = (derived_table.id, polymorphic_function_name)
		function = self._function_cache.get(function_key)
		if function is None and derived_table.id not in self._new_derived_table_ids:
			function = Function.objects.filter(
				name=polymorphic_function_name,
				table=derived_table
			).first()

		if function is None:
			# Create source code that shows the polymorphic delegation
			method_name = function_name.split('.')[-1] if '.' in function_name else function_name
			source_code = f"def {method_name}(self) -> Any: return self.base.{method_name}()  # Polymorphic delegation to {base_class_name}"
			
			# Create new Function record
			function_text = FunctionText.objects.create(
				text=source_code,
				language='python'
			)

			function = Function.objects.create(
				name=polymorphic_function_name,
				function_text=function_text,
				table=derived_table
			)
			self._debug(f"Created polymorphic function: {polymorphic_function_name}")
		self._function_cache[function_key] = function
		self._function_lookup_cache[(polymorphic_function_name, derived_table.id)] = function

		# Register function with the lineage collector (for deferred resolution)
		self.collector.register_function(function.id, polymorphic_function_name, wrapper_class_name, source_columns or [])

		# Track column references for polymorphic dependencies
		for col_ref in source_columns:
			try:
				# Try to resolve the actual column object
				resolved_field = self._resolve_column_reference(col_ref)
				if resolved_field:
					# Check if this column reference already exists
					content_type = self._get_content_type(resolved_field)
					ref_key = (function.id, content_type.id, resolved_field.id, None)
					if ref_key not in self._function_column_reference_keys:
						existing_col_ref = FunctionColumnReference.objects.filter(
							function=function,
							content_type=content_type,
							object_id=resolved_field.id
						).exists()
						if existing_col_ref:
							self._function_column_reference_keys.add(ref_key)
							continue
						# Create FunctionColumnReference
						FunctionColumnReference.objects.create(
							function=function,
							content_type=content_type,
							object_id=resolved_field.id
						)
						self._function_column_reference_keys.add(ref_key)
			except Exception as e:
				print(f"Could not resolve polymorphic column reference {col_ref}: {e}")

		return function

	def _extract_source_table_names(self, source_columns):
		"""Extract unique table names from column dependencies"""
		table_names = set()

		for col_ref in source_columns:
			try:
				# Column references are in format "TABLE_NAME.column_name" or nested
				parts = col_ref.split('.')
				if len(parts) >= 2:
					# First part is typically the table name
					table_name = parts[0]
					# Only add if it looks like a table name (not a lowercase attribute)
					if table_name and table_name.isupper():
						table_names.add(table_name)
			except Exception as e:
				print(f"Error extracting table name from {col_ref}: {e}")

		return list(table_names)

	def _analyze_table_creation_functions(self, table_obj, aorta_table):
		"""Analyze calc_ methods in table classes and create TableCreationFunction instances"""
		try:
			class_name = table_obj.__class__.__name__
			table_name = class_name.replace('_Table', '')

			# Extract source table names from class variables FIRST and register with collector
			source_table_names = self._extract_source_tables_from_class_variables(table_obj)
			for source_name in source_table_names:
				self.collector.add_table_source(table_name, source_name)

			# Also look for any object attributes that reference other tables
			self._discover_table_relationships(table_obj, table_name)

			# Find all calc_ methods in this class
			calc_methods = [name for name in dir(table_obj)
						   if name.startswith('calc_') and callable(getattr(table_obj, name))]

			for calc_method_name in calc_methods:
				calc_method = getattr(table_obj, calc_method_name)
				full_function_name = f"{class_name}.{calc_method_name}"

				# Get function source code
				try:
					import inspect
					source_code = inspect.getsource(calc_method)
				except:
					source_code = f"def {calc_method_name}(self): # Source code not available"

				# Check if this calc_ method has a @lineage decorator and extract dependencies
				lineage_dependencies = self._extract_lineage_dependencies(calc_method)
				if lineage_dependencies:
					# Use lineage dependencies for more detailed function text
					source_code += f"\n# Lineage dependencies: {lineage_dependencies}"

				table_creation_function = self._table_creation_function_cache.get(full_function_name)

				# Cache whether function existed BEFORE we potentially create it
				is_new_function = table_creation_function is None

				if is_new_function:
					# Create FunctionText
					function_text = FunctionText.objects.create(
						text=source_code,
						language='python'
					)

					# Create TableCreationFunction
					table_creation_function = TableCreationFunction.objects.create(
						name=full_function_name,
						function_text=function_text
					)
					# print(f"Created new table creation function: {full_function_name}")
				self._table_creation_function_cache[full_function_name] = table_creation_function

				# Link the DerivedTable to this TableCreationFunction
				if isinstance(aorta_table, DerivedTable) and aorta_table.table_creation_function is None:
					aorta_table.table_creation_function = table_creation_function
					aorta_table.save()

				# Create TableCreationSourceTable entries (only for new functions to avoid duplicates)
				if is_new_function:
					for source_table_name in source_table_names:
						# Find the source table in DatabaseTable or DerivedTable
						source_table = self._find_table_by_name(source_table_name)
						if source_table:
							content_type = self._get_content_type(source_table)
							# Check if this source table reference already exists
							source_ref_key = (table_creation_function.id, content_type.id, source_table.id)
							if source_ref_key not in self._table_creation_source_table_keys:
								existing_source_ref = TableCreationSourceTable.objects.filter(
									table_creation_function=table_creation_function,
									content_type=content_type,
									object_id=source_table.id
								).exists()
								if existing_source_ref:
									self._table_creation_source_table_keys.add(source_ref_key)
									continue
								TableCreationSourceTable.objects.create(
									table_creation_function=table_creation_function,
									content_type=content_type,
									object_id=source_table.id
								)
								self._table_creation_source_table_keys.add(source_ref_key)
								self._debug(f"Tracked table creation source: {full_function_name} -> {source_table_name}")

								# Also create a data flow edge
								if hasattr(aorta_table, 'name'):
									self.create_data_flow_edge(
										source_table, aorta_table, 'DATA'
									)

				# Create TableCreationFunctionColumn entries directly from lineage dependencies
				# Parse qualified column references (TABLE.COLUMN format) and link to actual objects
				from pybirdai.models import TableCreationFunctionColumn, DatabaseField

				# Use the lineage dependencies directly from the decorator
				if lineage_dependencies:
					# Parse individual dependencies from the text
					deps = [d.strip() for d in lineage_dependencies.split(',') if d.strip()]
					for dep_text in deps:
						# Check if this column reference already exists (by reference_text)
						column_ref_key = (table_creation_function.id, dep_text)
						if column_ref_key not in self._table_creation_function_column_keys:
							existing_column_ref = TableCreationFunctionColumn.objects.filter(
								table_creation_function=table_creation_function,
								reference_text=dep_text
							).exists()
							if existing_column_ref:
								self._table_creation_function_column_keys.add(column_ref_key)
								continue
							# Parse the qualified reference: TABLE_NAME.COLUMN_NAME
							column_obj = None
							content_type = None
							object_id = None

							if '.' in dep_text:
								table_name, column_name = dep_text.split('.', 1)

								# Try to find the table (DatabaseTable or DerivedTable)
								ref_table = self._find_table_by_name(table_name)

								if ref_table:
									if isinstance(ref_table, DatabaseTable):
										# Look up DatabaseField on the table
										db_field = DatabaseField.objects.filter(
											table=ref_table,
											name=column_name
										).first()
										if db_field:
											column_obj = db_field
											content_type = self._get_content_type(DatabaseField)
											object_id = db_field.id
									elif isinstance(ref_table, DerivedTable):
										# Look up Function on the derived table
										func = Function.objects.filter(
											table=ref_table,
											name=column_name
										).first()
										if func:
											column_obj = func
											content_type = self._get_content_type(Function)
											object_id = func.id

							# If we couldn't resolve, still store with reference_text for traceability
							if not content_type:
								content_type = self._get_content_type(TableCreationFunction)
								object_id = table_creation_function.id

							TableCreationFunctionColumn.objects.create(
								table_creation_function=table_creation_function,
								content_type=content_type,
								object_id=object_id,
								reference_text=dep_text
							)
							self._table_creation_function_column_keys.add(column_ref_key)

							if column_obj:
								self._debug(f"  Created TableCreationFunctionColumn: {full_function_name} -> {dep_text} (linked to {type(column_obj).__name__})")
							else:
								self._debug(f"  Created TableCreationFunctionColumn: {full_function_name} -> {dep_text} (unresolved)")

				dep_count = len([d.strip() for d in lineage_dependencies.split(',') if d.strip()]) if lineage_dependencies else 0
				self._debug(f"Processed TableCreationFunction {full_function_name}: {len(source_table_names)} source tables, {dep_count} dependencies")

		except Exception as e:
			print(f"Error analyzing table creation functions for {table_obj.__class__.__name__}: {e}")

	def _extract_source_tables_from_class_variables(self, table_obj):
		"""Extract source table names from class variables ending with _Table"""
		source_table_names = set()

		# Get all attributes that end with _Table
		for attr_name in dir(table_obj):
			if attr_name.endswith('_Table') and not attr_name.startswith('_'):
				# Remove _Table suffix to get the table name
				table_name = attr_name.replace('_Table', '')
				source_table_names.add(table_name)

		return list(source_table_names)

	def _discover_table_relationships(self, table_obj, table_name):
		"""Discover relationships between tables by examining object attributes"""
		try:
			# Look for list attributes that might contain data from other tables
			for attr_name in dir(table_obj):
				if attr_name.startswith('_'):
					continue

				try:
					attr_value = getattr(table_obj, attr_name)

					# Check if it's a list of objects
					if isinstance(attr_value, (list, tuple)) and len(attr_value) > 0:
						first_item = attr_value[0]
						if hasattr(first_item, '__class__'):
							source_class = first_item.__class__.__name__
							# Register this as a potential source
							if source_class != table_name and not source_class.startswith('_'):
								self.collector.add_table_source(table_name, source_class)

					# Check if it's an object with a class that looks like a table reference
					elif hasattr(attr_value, '__class__'):
						source_class = attr_value.__class__.__name__
						if self._is_business_data_class(source_class):
							clean_name = source_class.replace('_Table', '')
							if clean_name != table_name:
								self.collector.add_table_source(table_name, clean_name)

				except Exception:
					pass

		except Exception as e:
			print(f"Error discovering table relationships: {e}")

	def _extract_lineage_dependencies(self, method):
		"""Extract dependencies from @lineage decorator if present"""
		try:
			decorator_dependencies = getattr(method, '_lineage_dependencies', None)
			if decorator_dependencies:
				return ', '.join(decorator_dependencies)

			import inspect
			import re

			# Get the source code of the method
			source_code = inspect.getsource(method)

			# Look for @lineage decorator with dependencies parameter
			# Pattern matches: @lineage(dependencies={"base.COLUMN", "table.COLUMN"}) including multiline
			lineage_pattern = r'@lineage\s*\(\s*dependencies\s*=\s*\{([^}]+)\}\s*\)'
			matches = re.search(lineage_pattern, source_code, re.DOTALL)

			if matches:
				# Extract the dependencies content
				dependencies_content = matches.group(1)

				# Extract individual dependency strings (remove quotes and whitespace)
				dependency_pattern = r'\"([^\"]+)\"'
				dependency_matches = re.findall(dependency_pattern, dependencies_content)

				if dependency_matches:
					dependencies_text = ', '.join(dependency_matches)
					self._debug(f"Extracted lineage dependencies: {dependencies_text}")
					return dependencies_text

			return None

		except Exception as e:
			print(f"Error extracting lineage dependencies: {e}")
			return None

	def _find_table_by_name(self, table_name):
		"""Find a table by name in DatabaseTable or DerivedTable"""
		try:
			cache_key = ('DatabaseTable', table_name)
			if cache_key in self._table_lookup_cache:
				return self._table_lookup_cache[cache_key]
			cache_key = ('DerivedTable', table_name)
			if cache_key in self._table_lookup_cache:
				return self._table_lookup_cache[cache_key]

			# First try DatabaseTable
			database_tables = DatabaseTable.objects.filter(name=table_name)
			if database_tables.exists():
				table = database_tables.first()
				self._table_lookup_cache[('DatabaseTable', table_name)] = table
				return table

			# Then try DerivedTable
			derived_tables = DerivedTable.objects.filter(name=table_name)
			if derived_tables.exists():
				table = derived_tables.first()
				self._table_lookup_cache[('DerivedTable', table_name)] = table
				return table

			return None
		except Exception as e:
			print(f"Error finding table {table_name}: {e}")
			return None

	def _resolve_column_reference(self, column_ref):
		"""Resolve a column reference string to an actual DatabaseField object"""
		try:
			# Handle nested references like "base.CRRYNG_AMNT"
			parts = column_ref.split('.')

			# For simple column references, look in existing tables
			if len(parts) == 1:
				# Look for this column in any tracked table
				for table_name, populated_table in self.current_populated_tables.items():
					if hasattr(populated_table, 'table') and hasattr(populated_table.table, 'database_fields'):
						fields = populated_table.table.database_fields.filter(name=parts[0])
						if fields.exists():
							return fields.first()

			# For complex references, try to resolve based on patterns
			elif len(parts) > 1:
				# Look for the column name in the last part
				column_name = parts[-1]
				for table_name, populated_table in self.current_populated_tables.items():
					if hasattr(populated_table, 'table') and hasattr(populated_table.table, 'database_fields'):
						fields = populated_table.table.database_fields.filter(name=column_name)
						if fields.exists():
							return fields.first()

		except Exception as e:
			print(f"Error resolving column reference {column_ref}: {e}")

		return None

	def track_row_processing(self, table_name, row_data, row_identifier=None):
		"""Track row-level lineage"""
		if not self.lineage_enabled or not self.trail:
			return

		try:
			# Get the populated table for this table name
			populated_table = self.current_populated_tables.get(table_name)
			if not populated_table:
				print(f"No populated table found for {table_name}")
				return

			# Determine if this is a derived table or database table
			is_derived_table = isinstance(populated_table, EvaluatedDerivedTable)

			# Check for existing row with same data to prevent duplicates
			existing_row = None
			if row_identifier:
				row_cache_key = (populated_table.id, row_identifier)
				row_cache = self._derived_row_cache if is_derived_table else self._database_row_cache
				existing_row = row_cache.get(row_cache_key)
				if existing_row is None:
					if is_derived_table:
						existing_row = populated_table.derivedtablerow_set.filter(row_identifier=row_identifier).first()
					else:
						existing_row = populated_table.databaserow_set.filter(row_identifier=row_identifier).first()
					if existing_row:
						row_cache[row_cache_key] = existing_row
			elif not is_derived_table and isinstance(row_data, dict):
				# For database tables, check if a row with the same data already exists
				existing_rows = populated_table.databaserow_set.all()
				for existing in existing_rows:
					if self._rows_have_same_data(existing, row_data):
						existing_row = existing
						print(f"Found existing row for {table_name}, reusing instead of creating duplicate")
						break

			if existing_row:
				db_row = existing_row
			else:
				# Create row identifier if not provided
				if not row_identifier:
					if is_derived_table:
						row_identifier = f"row_{len(populated_table.derivedtablerow_set.all()) + 1}"
					else:
						row_identifier = f"row_{len(populated_table.databaserow_set.all()) + 1}"

				# Create appropriate row type
				if is_derived_table:
					# Create DerivedTableRow for derived tables
					db_row = DerivedTableRow.objects.create(
						populated_table=populated_table,
						row_identifier=row_identifier
					)
					self._derived_row_cache[(populated_table.id, row_identifier)] = db_row
					self._derived_row_by_id_cache[db_row.id] = db_row
					# Register with collector for deferred resolution
					self.collector.register_row('DerivedTableRow', db_row.id, table_name, row_identifier, row_data)
				else:
					# Create DatabaseRow for database tables
					db_row = DatabaseRow.objects.create(
						populated_table=populated_table,
						row_identifier=row_identifier
					)
					self._database_row_cache[(populated_table.id, row_identifier)] = db_row
					# Register with collector for deferred resolution
					self.collector.register_row('DatabaseRow', db_row.id, table_name, row_identifier, row_data)

			# Track individual column values (only for DatabaseRow and only if this is a new row)
			if not is_derived_table and not existing_row:
				if isinstance(row_data, dict):
					for column_name, value in row_data.items():
						self._track_column_value(db_row, column_name, value)
				elif hasattr(row_data, '__dict__'):
					# Handle object with attributes
					for attr_name in dir(row_data):
						if not attr_name.startswith('_') and not callable(getattr(row_data, attr_name)):
							value = getattr(row_data, attr_name)
							self._track_column_value(db_row, attr_name, value)

			# Store current row context for value tracking
			if isinstance(db_row, DerivedTableRow):
				self._derived_row_by_id_cache[db_row.id] = db_row
			self.current_rows['source'] = db_row.id
			self.current_rows['table'] = table_name

			# Clear evaluated functions cache when switching to a new row
			self.evaluated_functions_cache.clear()

			# print(f"Tracked row processing: {table_name} row {row_identifier}")
			return db_row

		except Exception as e:
			print(f"Error tracking row processing: {e}")
			return None

	def _track_column_value(self, db_row, column_name, value):
		"""Track individual column values"""
		try:
			# Find the corresponding DatabaseField
			table = db_row.populated_table.table
			field = self._get_or_create_database_field(table, column_name)

			# Create DatabaseColumnValue
			# Try to convert to float, otherwise use string_value
			numeric_value, string_value = self._split_numeric_value(value)

			column_value = DatabaseColumnValue.objects.create(
				value=numeric_value,
				string_value=string_value,
				column=field,
				row=db_row
			)
			self._remember_value_object(value, column_value)

			# print(f"Tracked column value: {table.name}.{column_name} = {value}")
		except Exception as e:
			print(f"Error tracking column value {column_name}: {e}")

	def _rows_have_same_data(self, existing_row, new_row_data):
		"""Check if an existing DatabaseRow has the same data as new_row_data"""
		try:
			# Get all column values for the existing row
			existing_values = {}
			for column_value in existing_row.column_values.all():
				column_name = column_value.column.name
				value = column_value.value if column_value.value is not None else column_value.string_value
				existing_values[column_name] = value

			# Compare with new row data
			if len(existing_values) != len(new_row_data):
				return False

			for column_name, new_value in new_row_data.items():
				existing_value = existing_values.get(column_name)

				# Handle numeric vs string comparison
				if existing_value is None and new_value is None:
					continue
				elif existing_value is None or new_value is None:
					return False

				# Try numeric comparison first
				try:
					if float(existing_value) == float(new_value):
						continue
				except (ValueError, TypeError):
					pass

				# Fall back to string comparison
				if str(existing_value) != str(new_value):
					return False

			return True

		except Exception as e:
			print(f"Error comparing row data: {e}")
			return False

	def track_derived_row_processing(self, table_name, derived_row_data, source_row_ids=None):
		"""Track derived/computed row processing"""
		if not self.lineage_enabled or not self.trail:
			return

		try:
			# Get the evaluated derived table
			evaluated_table = self.current_populated_tables.get(table_name)
			if not evaluated_table or not isinstance(evaluated_table, EvaluatedDerivedTable):
				print(f"No evaluated derived table found for {table_name}")
				return

			# Create DerivedTableRow
			derived_row = DerivedTableRow.objects.create(
				populated_table=evaluated_table
			)
			self._derived_row_by_id_cache[derived_row.id] = derived_row

			# Register with collector for deferred resolution
			row_identifier = f"derived_row_{derived_row.id}"
			self.collector.register_row('DerivedTableRow', derived_row.id, table_name, row_identifier, derived_row_data)

			# Track source row references
			if source_row_ids:
				for source_row_id in source_row_ids:
					try:
						source_row = DatabaseRow.objects.get(id=source_row_id)
						DerivedRowSourceReference.objects.create(
							derived_row=derived_row,
							content_type=self._get_content_type(DatabaseRow),
							object_id=source_row.id
						)
					except DatabaseRow.DoesNotExist:
						print(f"Source row {source_row_id} not found")

			# Store current derived row context
			self.current_rows['derived'] = derived_row.id

			# Clear evaluated functions cache when switching to a new derived row
			self.evaluated_functions_cache.clear()

			# print(f"Tracked derived row processing: {table_name}")
			return derived_row

		except Exception as e:
			print(f"Error tracking derived row processing: {e}")
			return None

	def track_value_computation(self, function_name, source_values, computed_value):
		"""Track value-level lineage"""
		self._debug(f"track_value_computation called: {function_name}, value={computed_value}")
		
		if not self.lineage_enabled or not self.trail:
			self._debug("track_value_computation: lineage disabled or no trail")
			return

		try:
			# Get the current derived row if available
			derived_row_id = self.current_rows.get('derived')
			if not derived_row_id:
				self._debug(f"track_value_computation: No derived row context for value computation: {function_name}")
				self._debug(f"track_value_computation: Current rows context: {self.current_rows}")
				return
			
			self._debug(f"track_value_computation: Using derived_row_id: {derived_row_id}")

			# Check cache first
			cache_key = f"{derived_row_id}:{function_name}"
			if cache_key in self.evaluated_functions_cache:
				# Return cached evaluated function
				return self.evaluated_functions_cache[cache_key]

			# Get the derived row
			derived_row = self._derived_row_by_id_cache.get(derived_row_id)
			if derived_row is None:
				derived_row = DerivedTableRow.objects.select_related('populated_table__table').get(id=derived_row_id)
				self._derived_row_by_id_cache[derived_row_id] = derived_row

			# Find the corresponding Function object
			function_parts = function_name.split('.')
			class_name = function_parts[0] if len(function_parts) > 1 else 'DynamicFunctions'
			method_name = function_parts[-1]

			derived_table = derived_row.populated_table.table
			function_lookup_key = (function_name, derived_table.id)
			function = self._function_lookup_cache.get(function_lookup_key)
			functions = None

			if function is None:
				# Look for the function by name across all Function objects
				functions = list(Function.objects.filter(name=function_name).select_related('table'))

			if function is None and not functions:
				print(f"Function {function_name} not found for value computation")
				return

			# CRITICAL FIX: When multiple functions exist with the same name,
			# prefer the one that belongs to the same table as the derived row
			if function is None:
				function = functions[0]  # Default fallback
			
				# Try to find a function from the same table as the derived row
				for func in functions:
					if func.table.id == derived_table.id:
						function = func
						self._debug(f"track_value_computation: Using function {func.id} from correct table {derived_table.name}")
						break
				else:
					# If no exact table match, try by table name (for data consistency)
					for func in functions:
						if func.table.name == derived_table.name:
							function = func
							self._debug(f"track_value_computation: Using function {func.id} from table with matching name {derived_table.name}")
							break
				self._function_lookup_cache[function_lookup_key] = function

			# Check if we already have an EvaluatedFunction for this function and row
			evaluated_lookup_key = (function.id, derived_row.id)
			existing_evaluated = self._evaluated_function_lookup_cache.get(evaluated_lookup_key)
			if existing_evaluated is None:
				existing_evaluated = EvaluatedFunction.objects.filter(
					function=function,
					row=derived_row
				).first()

			if existing_evaluated:
				# We already have this function evaluated for this row
				# Since functions are immutable, the result should be the same
				# Cache it and return
				self.evaluated_functions_cache[cache_key] = existing_evaluated
				self._evaluated_function_lookup_cache[evaluated_lookup_key] = existing_evaluated
				# print(f"Reusing existing EvaluatedFunction for {function_name} on row {derived_row_id}")
				return existing_evaluated

			# Create EvaluatedFunction only if it doesn't exist
			# Try to store as numeric value if possible
			numeric_value, string_value = self._split_numeric_value(computed_value)

			self._debug(f"track_value_computation: Creating EvaluatedFunction for {function.name} (ID: {function.id}) on row {derived_row.id}")
			evaluated_function = EvaluatedFunction.objects.create(
				value=numeric_value,
				string_value=string_value,
				function=function,
				row=derived_row
			)
			self._evaluated_function_lookup_cache[evaluated_lookup_key] = evaluated_function
			self._debug(f"track_value_computation: Created EvaluatedFunction ID: {evaluated_function.id}")

			# Register with collector for deferred resolution - convert source values to serializable refs
			source_value_refs = []
			for sv in source_values:
				if sv is not None:
					# Store info about source values for later resolution
					if hasattr(sv, 'id'):
						source_value_refs.append({'type': type(sv).__name__, 'id': sv.id})
					else:
						source_value_refs.append({'value': str(sv)})
			self.collector.register_evaluated_function(evaluated_function.id, function_name, derived_row.id, source_value_refs)

			# Track source values using enhanced method (optional - don't fail if this doesn't work)
			source_value_refs_created = 0
			for source_value in source_values:
				if source_value is not None:
					try:
						# Try to find the corresponding DatabaseColumnValue or EvaluatedFunction
						source_value_obj = self._find_source_value_object(source_value)
						if source_value_obj:
							ref = self.create_evaluated_function_source_value(evaluated_function, source_value_obj)
							if ref:
								source_value_refs_created += 1
					except Exception as e:
						# Source value tracking is optional - don't fail the main function evaluation
						print(f"Debug: Could not create source value link for '{source_value}': {e}")

			if source_value_refs_created > 0:
				self._debug(f"Created {source_value_refs_created} source value references for {function_name}")

			# Cache the evaluated function
			self._remember_value_object(computed_value, evaluated_function)
			self.evaluated_functions_cache[cache_key] = evaluated_function

			# print(f"Tracked value computation: {function_name} with {len(source_values)} source values = {computed_value}")
			return evaluated_function

		except Exception as e:
			print(f"Error tracking value computation: {e}")
			return None

	def _ensure_derived_row_context(self, derived_obj, function_name):
		"""Ensure a derived row context exists for the given derived object"""
		if not self.lineage_enabled or not self.trail:
			return None

		try:
			# Check if we already have a context for this specific object
			obj_id = id(derived_obj)
			if obj_id in self.object_contexts:
				return self.object_contexts[obj_id]

			# Get the class name to determine table name
			class_name = derived_obj.__class__.__name__

			# Only create derived tables for *_Table classes
			# Individual objects should be treated as rows within their parent table
			if class_name.endswith('_Table'):
				table_name = class_name.replace('_Table', '')
			else:
				# For individual objects, use their class name as the table name
				# This ensures proper isolation between different object types
				table_name = class_name

			# Ensure we have an EvaluatedDerivedTable for this specific class
			if table_name not in self.current_populated_tables:
				evaluated_table = EvaluatedDerivedTable.objects.filter(
					trail=self.trail,
					table__name=table_name
				).select_related('table').first()

				if evaluated_table:
					derived_table = evaluated_table.table
				else:
					# Check if a DerivedTable already exists for this name
					derived_table = DerivedTable.objects.filter(name=table_name).first()
					if derived_table is None:
						# Create a new DerivedTable
						derived_table = DerivedTable.objects.create(name=table_name)
						self._new_derived_table_ids.add(derived_table.id)
					self._table_lookup_cache[('DerivedTable', table_name)] = derived_table

					if self.metadata_trail:
						AortaTableReference.objects.get_or_create(
							metadata_trail=self.metadata_trail,
							table_content_type='DerivedTable',
							table_id=derived_table.id
						)

					# Create EvaluatedDerivedTable
					evaluated_table = EvaluatedDerivedTable.objects.create(
						trail=self.trail,
						table=derived_table
					)

				self.current_populated_tables[table_name] = evaluated_table
				self._debug(f"Created EvaluatedDerivedTable for {table_name}")

			# Get the EvaluatedDerivedTable
			evaluated_table = self.current_populated_tables[table_name]

			# Create a unique identifier for this derived row based on object identity
			row_identifier = f"{class_name}_{id(derived_obj)}"

			# Check if we already have a DerivedTableRow for this object
			row_cache_key = (evaluated_table.id, row_identifier)
			derived_row = self._derived_row_cache.get(row_cache_key)
			if derived_row is None:
				derived_row = evaluated_table.derivedtablerow_set.filter(row_identifier=row_identifier).first()
				if derived_row:
					self._derived_row_cache[row_cache_key] = derived_row
					self._derived_row_by_id_cache[derived_row.id] = derived_row

			if derived_row:
				derived_row_id = derived_row.id
			else:
				# Create a new DerivedTableRow
				derived_row = DerivedTableRow.objects.create(
					populated_table=evaluated_table,
					row_identifier=row_identifier
				)
				derived_row_id = derived_row.id
				self._derived_row_cache[row_cache_key] = derived_row
				self._derived_row_by_id_cache[derived_row_id] = derived_row
				# Register with collector for deferred resolution
				self.collector.register_row('DerivedTableRow', derived_row_id, table_name, row_identifier, derived_obj)
				self._debug(f"Created DerivedTableRow {derived_row_id} for {function_name}")

			# Store the context for this specific object
			self.object_contexts[obj_id] = derived_row_id
			return derived_row_id

		except Exception as e:
			print(f"Error ensuring derived row context: {e}")
			return None

	def _get_parent_table_name(self, class_name):
		"""Determine the parent table name for an individual object"""
		# Handle special cases first
		if class_name.endswith('_UnionItem'):
			return class_name.replace('_UnionItem', '_UnionTable').replace('_Table', '')

		# Direct match - if the class itself is a table
		if class_name in self.current_populated_tables:
			return class_name

		# For objects with specific report prefixes (e.g., F_05_01_REF_FINREP_3_0_Other_loans)
		if '_' in class_name and class_name.split('_')[0].startswith('F_'):
			# Extract the report prefix (e.g., F_05_01_REF_FINREP_3_0)
			parts = class_name.split('_')
			report_prefix_parts = []
			for i, part in enumerate(parts):
				report_prefix_parts.append(part)
				# Stop when we hit a part that looks like a class name (starts with uppercase after numbers)
				if i > 0 and len(part) > 0 and part[0].isupper() and not part.isdigit():
					# Check if this forms a valid report table name
					report_table_name = '_'.join(report_prefix_parts)
					if report_table_name in self.current_populated_tables:
						return report_table_name

			# If no exact match, return the full class name as table name
			# This ensures F_05_01_REF_FINREP_3_0_Other_loans gets its own table
			return class_name

		# For base objects (e.g., Other_loans), create their own table
		# Don't try to match them to other tables - this was causing the mixing issue
		return class_name

	def get_derived_context_for_object(self, obj):
		"""Get the correct derived context for a specific object"""
		obj_id = id(obj)
		if obj_id in self.object_contexts:
			return self.object_contexts[obj_id]
		return None
	
	def track_calculation_used_row(self, calculation_name, row):
		"""Track that a specific row was used in a calculation (passed filters)"""
		self._debug(f"track_calculation_used_row called: {calculation_name}, {type(row).__name__}")
		
		if not self.lineage_enabled or not self.trail:
			self._debug(f"Lineage tracking disabled or no trail: lineage_enabled={self.lineage_enabled}, trail={self.trail}")
			return
		
		try:
			# Determine the type of row
			if isinstance(row, DatabaseRow):
				content_type = self._get_content_type(DatabaseRow)
			elif isinstance(row, DerivedTableRow):
				content_type = self._get_content_type(DerivedTableRow)
			# Check if this is a Django model instance (database record)
			elif hasattr(row, '_meta') and hasattr(row._meta, 'model'):
				# This is a Django model instance - we need to create/find the appropriate DatabaseRow
				model_name = type(row).__name__
				
				# Ensure we have a database table for this model
				# Check if we already have the wrong type of table stored
				existing_table = self.current_populated_tables.get(model_name)
				if not existing_table or not isinstance(existing_table, PopulatedDataBaseTable):
					populated_table = PopulatedDataBaseTable.objects.filter(
						trail=self.trail,
						table__name=model_name
					).select_related('table').first()
					if populated_table:
						db_table = populated_table.table
					else:
						# Create database table
						db_table = DatabaseTable.objects.create(name=model_name)
						self._new_database_table_ids.add(db_table.id)

						# Add to metadata trail
						if self.metadata_trail:
							AortaTableReference.objects.get_or_create(
								metadata_trail=self.metadata_trail,
								table_content_type='DatabaseTable',
								table_id=db_table.id
							)

						# Create PopulatedDataBaseTable
						populated_table = PopulatedDataBaseTable.objects.create(
							trail=self.trail,
							table=db_table
						)

					# Register with collector for deferred resolution
					self.collector.register_table('DatabaseTable', db_table.id, model_name)

					self.current_populated_tables[model_name] = populated_table
					self._debug(f"Created database table for Django model: {model_name}")
				else:
					populated_table = existing_table
				
				# Get the populated database table
				populated_table = self.current_populated_tables[model_name]
				
				# Create unique identifier for this model instance
				if hasattr(row, 'pk') and row.pk:
					object_identifier = f"{model_name}_{row.pk}"
				else:
					object_identifier = f"{model_name}_{id(row)}"
				
				# Check if we already have a database row for this model instance
				row_cache_key = (populated_table.id, object_identifier)
				db_row = self._database_row_cache.get(row_cache_key)
				if db_row is None:
					db_row = populated_table.databaserow_set.filter(
						row_identifier=object_identifier
					).first()
					if db_row:
						self._database_row_cache[row_cache_key] = db_row
				
				if db_row:
					pass
				else:
					# Create new database row for this model instance
					db_row = DatabaseRow.objects.create(
						populated_table=populated_table,
						row_identifier=object_identifier
					)
					self._database_row_cache[row_cache_key] = db_row
					# Register with collector for deferred resolution
					self.collector.register_row('DatabaseRow', db_row.id, model_name, object_identifier, row)

					# Create column values for the model fields in batches. This is on the
					# hot path for cell filtering, so avoiding hundreds of tiny writes matters.
					self._track_column_values_for_django_row(db_row, row, populated_table.table)
					
					self._debug(f"Created database row for Django model {model_name}")
					
					# DISABLED: Don't automatically track all Django model fields as used
					# Only track fields that are actually accessed during calculations (via wrapper)
					# self._track_django_model_fields_as_used(calculation_name, populated_table.table, row)
				
				row = db_row
				
				content_type = self._get_content_type(DatabaseRow)
			else:
				# For business objects, determine the appropriate tracking strategy
				row_class_name = type(row).__name__
				
				# Check if this is a business object that should be tracked as a derived table row
				if hasattr(row, '__dict__'):
					# This is a business object - create/find appropriate derived table
					tracked_row = None
					
					# Look for or create an appropriate derived table for this object type
					table_name = row_class_name
					
					# Check if we already have a derived table for this object type
					if table_name not in self.current_populated_tables:
						evaluated_table = EvaluatedDerivedTable.objects.filter(
							trail=self.trail,
							table__name=table_name
						).select_related('table').first()

						if evaluated_table is None:
							# Create derived table for this object type
							derived_table = DerivedTable.objects.create(name=table_name)
							self._new_derived_table_ids.add(derived_table.id)
							self._table_lookup_cache[('DerivedTable', table_name)] = derived_table

							# Add to metadata trail
							if self.metadata_trail:
								AortaTableReference.objects.get_or_create(
									metadata_trail=self.metadata_trail,
									table_content_type='DerivedTable',
									table_id=derived_table.id
								)

							# Create EvaluatedDerivedTable
							evaluated_table = EvaluatedDerivedTable.objects.create(
								trail=self.trail,
								table=derived_table
							)
						
						self.current_populated_tables[table_name] = evaluated_table
						self._debug(f"Created derived table for business object type: {table_name}")
					
					# Get the evaluated derived table
					evaluated_table = self.current_populated_tables[table_name]
					
					# Create unique identifier for this specific object instance
					object_identifier = f"{row_class_name}_{id(row)}"
					
					# Check if we already have a derived row for this object
					row_cache_key = (evaluated_table.id, object_identifier)
					tracked_row = self._derived_row_cache.get(row_cache_key)
					if tracked_row is None:
						tracked_row = evaluated_table.derivedtablerow_set.filter(
							row_identifier=object_identifier
						).first()
						if tracked_row:
							self._derived_row_cache[row_cache_key] = tracked_row
							self._derived_row_by_id_cache[tracked_row.id] = tracked_row
					
					if tracked_row:
						pass
					else:
						# Create new derived table row for this object
						tracked_row = DerivedTableRow.objects.create(
							populated_table=evaluated_table,
							row_identifier=object_identifier
						)
						self._derived_row_cache[row_cache_key] = tracked_row
						self._derived_row_by_id_cache[tracked_row.id] = tracked_row
						# Register with collector for deferred resolution
						self.collector.register_row('DerivedTableRow', tracked_row.id, table_name, object_identifier, row)
						self._debug(f"Created derived table row for {row_class_name}")
					
					if tracked_row:
						self.object_contexts[id(row)] = tracked_row.id
						# ENHANCED: Track object relationships via DerivedRowSourceReference
						# Do this for both new AND existing rows to ensure relationships are captured
						relationship_key = (tracked_row.id, id(row))
						if relationship_key not in self._relationship_tracked_keys:
							self._track_object_relationships(tracked_row, row)
							self._relationship_tracked_keys.add(relationship_key)
						
						# ENHANCED: Also track transitively referenced objects as used
						# This ensures that objects referenced via unionOfLayers.base etc. are also marked as used
						transitive_key = (calculation_name, id(row))
						if transitive_key not in self._transitive_used_object_keys:
							self._track_transitive_used_objects(calculation_name, row)
							self._transitive_used_object_keys.add(transitive_key)
						
						row = tracked_row
						content_type = self._get_content_type(DerivedTableRow)
					else:
						print(f"Failed to create derived table row for {row_class_name}")
						return
				else:
					print(f"Cannot track row of type {type(row)} - not a trackable object")
					return
			
			# Check if this row is already tracked for this calculation
			used_row_key = (self.trail.id, calculation_name, content_type.id, row.id)
			existing = used_row_key in self._calculation_used_row_keys
			if not existing:
				existing = CalculationUsedRow.objects.filter(
					trail=self.trail,
					calculation_name=calculation_name,
					content_type=content_type,
					object_id=row.id
				).exists()
			
			if not existing:
				CalculationUsedRow.objects.create(
					trail=self.trail,
					calculation_name=calculation_name,
					content_type=content_type,
					object_id=row.id
				)
				self._debug(f"Created CalculationUsedRow: {calculation_name} -> {type(row).__name__} (id: {row.id})")
			else:
				self._debug(f"CalculationUsedRow already exists for {calculation_name} -> {type(row).__name__}")
			self._calculation_used_row_keys.add(used_row_key)
		
		except Exception as e:
			print(f"Error tracking calculation used row: {e}")
			import traceback
			traceback.print_exc()
	
	def _track_django_model_fields_as_used(self, calculation_name, database_table, django_model_instance):
		"""Track all fields of a Django model as used fields when the model instance is tracked as a used row"""
		try:
			# Get all database fields for this table
			database_fields = database_table.database_fields.all()
			
			# Track each database field as a used field
			for db_field in database_fields:
				# Check if the Django model actually has this field
				if hasattr(django_model_instance, db_field.name):
					try:
						# Track this field as used
						self.track_calculation_used_field(calculation_name, db_field.name, django_model_instance)
						print(f"  Tracked Django model field as used: {database_table.name}.{db_field.name}")
					except Exception as e:
						print(f"  Failed to track Django model field {db_field.name}: {e}")
		except Exception as e:
			print(f"Error tracking Django model fields as used: {e}")

	def track_calculation_used_field(self, calculation_name, field_name, row=None, function_obj=None):
		"""Track that a specific field was accessed during a calculation"""
		if not self.lineage_enabled or not self.trail:
			return
		
		try:
			# Find the field object
			field = None
			content_type = None
			
			# If function_obj is provided, use it directly to avoid ID mismatch issues
			if function_obj:
				field = function_obj
				content_type = self._get_content_type(Function)
				self._debug(f"Using provided function object: {field.name} (ID: {field.id})")
			else:
				field_lookup_key = ('used_field', field_name)
				if field_lookup_key in self._dependency_resolution_cache:
					field_lookup = self._dependency_resolution_cache[field_lookup_key]
					if field_lookup is None:
						return
					field, content_type = field_lookup
				else:
					# First try to find as DatabaseField
					field = DatabaseField.objects.filter(name=field_name).first()
					if field:
						content_type = self._get_content_type(DatabaseField)
					else:
						# Try to find as Function
						field = Function.objects.filter(name=field_name).first()
						if field:
							content_type = self._get_content_type(Function)
			
			if not field:
				# Try to find with more context if field_name includes table reference
				if '.' in field_name:
					parts = field_name.split('.')
					actual_field_name = parts[-1]
					field = DatabaseField.objects.filter(name=actual_field_name).first()
					if field:
						content_type = self._get_content_type(DatabaseField)
					else:
						field = Function.objects.filter(name=actual_field_name).first()
						if field:
							content_type = self._get_content_type(Function)
			
			if not field:
				self._debug(f"Cannot find field {field_name} to track")
				if not function_obj:
					self._dependency_resolution_cache[('used_field', field_name)] = None
				return
			if not function_obj:
				self._dependency_resolution_cache[('used_field', field_name)] = (field, content_type)
			
			# Prepare row tracking if provided
			row_content_type = None
			row_object_id = None
			if row:
				if isinstance(row, DatabaseRow):
					row_content_type = self._get_content_type(DatabaseRow)
					row_object_id = row.id
				elif isinstance(row, DerivedTableRow):
					row_content_type = self._get_content_type(DerivedTableRow)
					row_object_id = row.id
			
			# Check if this field is already tracked for this calculation
			used_field_key = (
				self.trail.id,
				calculation_name,
				content_type.id,
				field.id,
				row_content_type.id if row_content_type else None,
				row_object_id
			)
			exists = used_field_key in self._calculation_used_field_keys
			if not exists:
				query = CalculationUsedField.objects.filter(
					trail=self.trail,
					calculation_name=calculation_name,
					content_type=content_type,
					object_id=field.id
				)

				if row_content_type and row_object_id:
					query = query.filter(
						row_content_type=row_content_type,
						row_object_id=row_object_id
					)
				exists = query.exists()
			
			if not exists:
				CalculationUsedField.objects.create(
					trail=self.trail,
					calculation_name=calculation_name,
					content_type=content_type,
					object_id=field.id,
					row_content_type=row_content_type,
					row_object_id=row_object_id
				)
				# print(f"Tracked used field for {calculation_name}: {field_name}")
			self._calculation_used_field_keys.add(used_field_key)
		
		except Exception as e:
			print(f"Error tracking calculation used field: {e}")
	
	def get_calculation_used_rows(self, calculation_name):
		"""Get all rows that were used in a specific calculation"""
		if not self.trail:
			return []
		
		used_rows = CalculationUsedRow.objects.filter(
			trail=self.trail,
			calculation_name=calculation_name
		)
		
		return [ur.used_row for ur in used_rows]
	
	def get_calculation_used_fields(self, calculation_name):
		"""Get all fields that were accessed during a specific calculation"""
		if not self.trail:
			return []
		
		used_fields = CalculationUsedField.objects.filter(
			trail=self.trail,
			calculation_name=calculation_name
		)
		
		return [uf.used_field for uf in used_fields]

	def _is_trackable_related_object(self, value):
		"""Return True when a value looks like a row/business object relationship."""
		if value is None or isinstance(value, (str, bytes, int, float, bool)):
			return False
		if isinstance(value, (DatabaseRow, DerivedTableRow)):
			return True
		if hasattr(value, '_meta') and hasattr(value._meta, 'model'):
			return True
		if hasattr(value, '__dict__') and getattr(value.__class__, '__module__', '') != 'builtins':
			return True
		return False

	def _iter_related_objects(self, business_object):
		"""Yield related objects from concrete instance attributes without naming tables."""
		try:
			for attr_name, attr_value in getattr(business_object, '__dict__', {}).items():
				if attr_name.startswith('_') or attr_value is None:
					continue

				values = list(attr_value.values()) if isinstance(attr_value, dict) else attr_value
				if isinstance(values, (list, tuple, set)):
					candidates = values
				else:
					candidates = (values,)

				for related_obj in candidates:
					if related_obj is business_object:
						continue
					if self._is_trackable_related_object(related_obj):
						yield attr_name, related_obj
		except Exception as e:
			print(f"Error iterating related objects for {type(business_object).__name__}: {e}")
	
	def _track_object_relationships(self, tracked_row, business_object):
		"""Track object relationships via DerivedRowSourceReference."""
		try:
			if not isinstance(tracked_row, DerivedTableRow):
				return

			self._debug(f"Tracking object relationships for {type(business_object).__name__}")
			relationships_created = 0
			
			for attr_name, source_obj in self._iter_related_objects(business_object):
				# Register relationship with collector for deferred resolution.
				self.collector.add_object_relationship(business_object, source_obj, attr_name)

				# Try to find the corresponding lineage row for the source object.
				source_row = source_obj if isinstance(source_obj, (DatabaseRow, DerivedTableRow)) else self._find_derived_row_for_object(source_obj)
				if source_row:
					ref = self.create_derived_row_source_reference(tracked_row, source_row)
					if ref:
						relationships_created += 1
						self._debug(f"Created relationship via {attr_name}: {tracked_row.populated_table.table.name} <- {source_row.populated_table.table.name}")
				else:
					if hasattr(source_obj, '_meta') and hasattr(source_obj._meta, 'model'):
						continue

					# If source row not found, try to create it.
					source_class_name = type(source_obj).__name__
					self._debug(f"Source row for {source_class_name} not found, attempting to create...")

					source_row_id = self._ensure_derived_row_context(source_obj, f"{source_class_name}.init")
					if source_row_id:
						try:
							source_row = DerivedTableRow.objects.get(id=source_row_id)
							ref = self.create_derived_row_source_reference(tracked_row, source_row)
							if ref:
								relationships_created += 1
								self._debug(f"Created relationship via {attr_name} (deferred): {tracked_row.populated_table.table.name} <- {source_row.populated_table.table.name}")
						except DerivedTableRow.DoesNotExist:
							print(f"Could not find newly created source row {source_row_id}")

			# Also check class name-based relationships (e.g. F_05_01_REF_FINREP_3_0_UnionItem -> Other_loans)
			obj_class_name = type(business_object).__name__
			if '_' in obj_class_name:
				parts = obj_class_name.split('_')
				# Look for potential source class names in the parts
				for i in range(len(parts)):
					potential_source_class = '_'.join(parts[i:])
					if potential_source_class != obj_class_name:
						# Try to find objects of this source class
						source_obj = self._find_object_by_class_suffix(business_object, potential_source_class)
						if source_obj:
							# Register relationship with collector for deferred resolution
							self.collector.add_object_relationship(business_object, source_obj, 'class_based')

							source_row = self._find_derived_row_for_object(source_obj)
							if source_row:
								ref = self.create_derived_row_source_reference(tracked_row, source_row)
								if ref:
									relationships_created += 1
									self._debug(f"Created class-based relationship: {tracked_row.populated_table.table.name} <- {source_row.populated_table.table.name}")

			if relationships_created > 0:
				self._debug(f"Total relationships created for {type(business_object).__name__}: {relationships_created}")

		except Exception as e:
			print(f"Error tracking object relationships: {e}")
			import traceback
			traceback.print_exc()
	
	def _find_derived_row_for_object(self, obj):
		"""Find the DerivedTableRow that corresponds to a business object"""
		try:
			derived_row_id = self.object_contexts.get(id(obj))
			if derived_row_id:
				derived_row = self._derived_row_by_id_cache.get(derived_row_id)
				if derived_row is None:
					derived_row = DerivedTableRow.objects.get(id=derived_row_id)
					self._derived_row_by_id_cache[derived_row_id] = derived_row
				return derived_row

			obj_class_name = type(obj).__name__
			object_identifier = f"{obj_class_name}_{id(obj)}"
			
			# Look in current populated tables for matching row
			for table_name, populated_table in self.current_populated_tables.items():
				if hasattr(populated_table, 'derivedtablerow_set'):
					matching_rows = populated_table.derivedtablerow_set.filter(
						row_identifier=object_identifier
					)
					if matching_rows.exists():
						return matching_rows.first()
			
			return None
		except Exception as e:
			print(f"Error finding derived row for object: {e}")
			return None
	
	def _find_object_by_class_suffix(self, business_object, suffix):
		"""Find a related object by class name suffix"""
		try:
			# This is a heuristic approach - look for attributes that might contain objects of the target class
			for attr_name in dir(business_object):
				if not attr_name.startswith('_') and not callable(getattr(business_object, attr_name, None)):
					attr_value = getattr(business_object, attr_name)
					if attr_value and hasattr(attr_value, '__class__'):
						if type(attr_value).__name__.endswith(suffix):
							return attr_value
			return None
		except Exception as e:
			print(f"Error finding object by class suffix: {e}")
			return None

	def _track_transitive_used_objects(self, calculation_name, business_object):
		"""Track objects that are transitively referenced by the current object as also being used"""
		try:
			if not hasattr(self, '_transitive_tracking_stack'):
				self._transitive_tracking_stack = set()

			object_id = id(business_object)
			if object_id in self._transitive_tracking_stack:
				return
			self._transitive_tracking_stack.add(object_id)

			self._debug(f"Tracking transitive used objects for {type(business_object).__name__}")
			
			for attr_name, referenced_obj in self._iter_related_objects(business_object):
				self._debug(f"Found transitive reference: {type(business_object).__name__}.{attr_name} -> {type(referenced_obj).__name__}")
				self.track_calculation_used_row(calculation_name, referenced_obj)
						
		except Exception as e:
			print(f"Error tracking transitive used objects: {e}")
		finally:
			if hasattr(self, '_transitive_tracking_stack'):
				self._transitive_tracking_stack.discard(id(business_object))

	def _track_column_values_for_django_row(self, db_row, django_model_instance, table):
		"""Track non-null Django model field values for a DatabaseRow using bulk inserts."""
		try:
			field_values = []
			for model_field in django_model_instance._meta.fields:
				if hasattr(django_model_instance, model_field.name):
					field_value = getattr(django_model_instance, model_field.name)
					if field_value is not None:
						field_values.append((model_field.name, field_value))

			if not field_values:
				return

			fields_by_name = self._get_or_create_database_fields(
				table,
				[field_name for field_name, _ in field_values]
			)

			column_values = []
			original_values = []
			for field_name, field_value in field_values:
				field = fields_by_name.get(field_name)
				if not field:
					continue
				numeric_value, string_value = self._split_numeric_value(field_value)
				column_values.append(DatabaseColumnValue(
					value=numeric_value,
					string_value=string_value,
					column=field,
					row=db_row
				))
				original_values.append(field_value)

			created_values = DatabaseColumnValue.objects.bulk_create(
				column_values,
				batch_size=1000
			)
			for original_value, column_value in zip(original_values, created_values):
				self._remember_value_object(original_value, column_value)

		except Exception as e:
			print(f"Error tracking Django row fields: {e}")

	def _track_column_value_for_django_field(self, db_row, field_name, field_value, table):
		"""Helper method to track column values for Django model fields"""
		try:
			# Find or create the DatabaseField
			field = self._get_or_create_database_field(table, field_name)
			
			# Create DatabaseColumnValue
			numeric_value, string_value = self._split_numeric_value(field_value)
			
			column_value = DatabaseColumnValue.objects.create(
				value=numeric_value,
				string_value=string_value,
				column=field,
				row=db_row
			)
			self._remember_value_object(field_value, column_value)
			
		except Exception as e:
			print(f"Error tracking Django field {field_name}: {e}")

	def _find_source_value_object(self, source_value):
		"""Find the EvaluatedFunction or DatabaseColumnValue object for a given source value"""
		try:
			value_object = self._get_remembered_value_object(source_value)
			if value_object is not None:
				return value_object

			# First, check the evaluated_functions_cache for a match by value
			# This is the most efficient lookup for recently computed values
			if hasattr(self, 'evaluated_functions_cache'):
				str_value = str(source_value)
				for cache_key, eval_func in self.evaluated_functions_cache.items():
					if eval_func.string_value == str_value:
						return eval_func
					# Also check numeric value
					if eval_func.value is not None:
						try:
							if abs(float(eval_func.value) - float(source_value)) < 0.0001:
								return eval_func
						except (ValueError, TypeError):
							pass

			# Look for DatabaseColumnValue with matching value
			source_row_id = self.current_rows.get('source') if hasattr(self, 'current_rows') and self.current_rows else None
			if source_row_id:
				source_row = DatabaseRow.objects.get(id=source_row_id)
				column_values = source_row.column_values.filter(value=str(source_value))
				if column_values.exists():
					return column_values.first()

			# Search in current populated tables for matching EvaluatedFunction
			if hasattr(self, 'current_populated_tables'):
				for table_name, populated_table in self.current_populated_tables.items():
					# Check DerivedTableRow objects for EvaluatedFunction matches
					if hasattr(populated_table, 'derivedtablerow_set'):
						for row in populated_table.derivedtablerow_set.all():
							evaluated_funcs = row.evaluatedfunction_set.filter(
								string_value=str(source_value)
							)
							if evaluated_funcs.exists():
								return evaluated_funcs.first()

					# Also check traditional DatabaseRow objects
					if hasattr(populated_table, 'databaserow_set'):
						for row in populated_table.databaserow_set.all():
							column_values = row.column_values.filter(value=str(source_value))
							if column_values.exists():
								return column_values.first()

		except Exception as e:
			# Make this a debug message instead of error to reduce noise
			pass

		return None

	def _bulk_track_database_row_data(self, table_name, populated_table, data_items):
		"""Track a batch of dictionary row data for a PopulatedDataBaseTable."""
		if not data_items:
			return

		row_payloads = []
		row_identifiers = []
		for i, row_data in enumerate(data_items):
			row_identifier = f"{table_name}_row_{i}"
			row_payloads.append((row_identifier, row_data))
			row_identifiers.append(row_identifier)

		existing_rows_by_identifier = {}
		missing_identifiers = []
		for row_identifier in row_identifiers:
			cache_key = (populated_table.id, row_identifier)
			existing_row = self._database_row_cache.get(cache_key)
			if existing_row is None:
				missing_identifiers.append(row_identifier)
			else:
				existing_rows_by_identifier[row_identifier] = existing_row

		if missing_identifiers:
			existing_rows = DatabaseRow.objects.filter(
				populated_table=populated_table,
				row_identifier__in=missing_identifiers
			)
			for row in existing_rows:
				existing_rows_by_identifier[row.row_identifier] = row
				self._database_row_cache[(populated_table.id, row.row_identifier)] = row

		rows_to_create = [
			(row_identifier, row_data)
			for row_identifier, row_data in row_payloads
			if row_identifier not in existing_rows_by_identifier
		]
		if not rows_to_create:
			if row_identifiers:
				last_row = existing_rows_by_identifier.get(row_identifiers[-1])
				if last_row:
					self.current_rows['source'] = last_row.id
					self.current_rows['table'] = table_name
					self.evaluated_functions_cache.clear()
			return

		created_rows = DatabaseRow.objects.bulk_create(
			[
				DatabaseRow(
					populated_table=populated_table,
					row_identifier=row_identifier
				)
				for row_identifier, _ in rows_to_create
			],
			batch_size=1000
		)
		if any(row.id is None for row in created_rows):
			created_identifiers = [row_identifier for row_identifier, _ in rows_to_create]
			created_rows = list(DatabaseRow.objects.filter(
				populated_table=populated_table,
				row_identifier__in=created_identifiers
			).order_by('id'))

		created_payloads = []
		for row, (row_identifier, row_data) in zip(created_rows, rows_to_create):
			self._database_row_cache[(populated_table.id, row_identifier)] = row
			self.collector.register_row('DatabaseRow', row.id, table_name, row_identifier, row_data)
			created_payloads.append((row, row_data))

		field_names = []
		for _, row_data in created_payloads:
			field_names.extend(row_data.keys())
		fields_by_name = self._get_or_create_database_fields(populated_table.table, field_names)

		column_values = []
		original_values = []
		for row, row_data in created_payloads:
			for column_name, value in row_data.items():
				field = fields_by_name.get(column_name)
				if not field:
					continue
				numeric_value, string_value = self._split_numeric_value(value)
				column_values.append(DatabaseColumnValue(
					value=numeric_value,
					string_value=string_value,
					column=field,
					row=row
				))
				original_values.append(value)

		created_column_values = DatabaseColumnValue.objects.bulk_create(
			column_values,
			batch_size=1000
		)
		for original_value, column_value in zip(original_values, created_column_values):
			self._remember_value_object(original_value, column_value)

		if created_rows:
			last_row = created_rows[-1]
			self.current_rows['source'] = last_row.id
			self.current_rows['table'] = table_name
			self.evaluated_functions_cache.clear()

	def track_data_processing(self, table_name, data_items, django_model_objects=None):
		"""Track processing of data items in a table"""
		if not self.lineage_enabled or not self.trail or not self.metadata_trail:
			return
		
		# Also track that these rows and tables are being used in calculations
		current_calculation = getattr(self, 'current_calculation', None)
		self._debug(f"track_data_processing: table={table_name}, items={len(data_items)}, django_objects={len(django_model_objects) if django_model_objects else 0}, current_calculation={current_calculation}")
		# CRITICAL FIX: Do NOT auto-track all processed items here.
		# Rows should only be marked as used when they pass a cell's calc_referenced_items filter
		# or are explicitly tracked by targeted logic (e.g., wrapper or explicit calls).
		# This prevents unrelated rows (e.g., Advances_that_are_not_loans) from appearing as used.
		
		try:
			# Ensure we have a populated table for this table name
			if table_name not in self.current_populated_tables:
				# Determine if this is a Django model or a derived table
				is_django_model = self._is_django_model(table_name)

				# Check for existing PopulatedDataBaseTable/EvaluatedDerivedTable first
				populated_table = None
				temp_table = None
				table_exists = False

				if is_django_model:
					# Look for existing DatabaseTable with same name in this trail
					existing_populated = PopulatedDataBaseTable.objects.filter(
						trail=self.trail,
						table__name=table_name
					).select_related('table').first()
					if existing_populated:
						temp_table = existing_populated.table
						populated_table = existing_populated
						table_exists = True
						self._debug(f"Found existing DatabaseTable for: {table_name}")
				else:
					# Look for existing DerivedTable with same name in this trail
					existing_evaluated = EvaluatedDerivedTable.objects.filter(
						trail=self.trail,
						table__name=table_name
					).select_related('table').first()
					if existing_evaluated:
						temp_table = existing_evaluated.table
						populated_table = existing_evaluated
						table_exists = True
						self._debug(f"Found existing DerivedTable for: {table_name}")

				# Create new table only if not found
				if not temp_table:
					if is_django_model:
						temp_table = DatabaseTable.objects.create(name=table_name)
						self._new_database_table_ids.add(temp_table.id)
						table_type = 'DatabaseTable'
					else:
						temp_table = DerivedTable.objects.create(name=table_name)
						self._new_derived_table_ids.add(temp_table.id)
						table_type = 'DerivedTable'

					if self.metadata_trail:
						AortaTableReference.objects.create(
							metadata_trail=self.metadata_trail,
							table_content_type=table_type,
							table_id=temp_table.id
						)
					else:
						print(f"Warning: No metadata_trail available for tracking table {table_name}")

				# Create populated table only if not found
				if self.trail and not populated_table:
					if is_django_model:
						populated_table = PopulatedDataBaseTable.objects.create(
							trail=self.trail,
							table=temp_table
						)
					else:
						populated_table = EvaluatedDerivedTable.objects.create(
							trail=self.trail,
							table=temp_table
						)
				elif not self.trail:
					print(f"Warning: No trail available for PopulatedDataBaseTable {table_name}")
					return
				self.current_populated_tables[table_name] = populated_table

			populated_table = self.current_populated_tables.get(table_name)
			if (
				isinstance(populated_table, PopulatedDataBaseTable)
				and all(isinstance(item, dict) for item in data_items)
			):
				self._bulk_track_database_row_data(table_name, populated_table, data_items)
				return

			# Track each data item as a row
			for i, item in enumerate(data_items):
				row_id = f"{table_name}_row_{i}"

				# Extract data from the item
				if isinstance(item, dict):
					# Item is already a dictionary
					row_data = item
				elif hasattr(item, '__dict__'):
					row_data = {}
					for attr in dir(item):
						if not attr.startswith('_') and not callable(getattr(item, attr)):
							try:
								value = getattr(item, attr)
								# Only include non-callable attributes to avoid unwanted method evaluations
								if not callable(value):
									row_data[attr] = value
							except:
								pass
				else:
					row_data = {'value': str(item)}

				# Track the row
				self.track_row_processing(table_name, row_data, row_id)

			# print(f"Tracked data processing for {table_name}: {len(data_items)} items")

		except Exception as e:
			print(f"Error tracking data processing: {e}")

	# ========================================================================
	# ENHANCED LINEAGE TRACKING METHODS
	# ========================================================================

	def start_transformation_step(self, step_type, step_name, description=""):
		"""Start tracking a new transformation step"""
		if not self.lineage_enabled or not self.trail:
			return None

		try:
			# Get the next step number
			existing_steps = TransformationStep.objects.filter(trail=self.trail).count()
			step_number = existing_steps + 1

			step = TransformationStep.objects.create(
				trail=self.trail,
				step_number=step_number,
				step_type=step_type,
				step_name=step_name,
				description=description,
				started_at=datetime.now()
			)

			# Store current step for tracking
			self._current_transformation_step = step
			self._step_start_time = time.time()

			self._debug(f"Started transformation step {step_number}: {step_name} ({step_type})")
			return step

		except Exception as e:
			print(f"Error starting transformation step: {e}")
			return None

	def end_transformation_step(self, input_row_count=0, output_row_count=0):
		"""End the current transformation step"""
		if not self.lineage_enabled or not hasattr(self, '_current_transformation_step'):
			return

		try:
			step = self._current_transformation_step
			step.completed_at = datetime.now()
			step.input_row_count = input_row_count
			step.output_row_count = output_row_count

			if hasattr(self, '_step_start_time'):
				step.execution_time_ms = int((time.time() - self._step_start_time) * 1000)

			step.save()

			self._debug(f"Completed transformation step {step.step_number}: {step.step_name} ({input_row_count} -> {output_row_count} rows)")

			self._current_transformation_step = None
			self._step_start_time = None

		except Exception as e:
			print(f"Error ending transformation step: {e}")

	def add_step_input(self, source_table):
		"""Add an input source to the current transformation step"""
		if not hasattr(self, '_current_transformation_step') or not self._current_transformation_step:
			return

		try:
			content_type = ContentType.objects.get_for_model(source_table.__class__)
			TransformationStepInput.objects.create(
				step=self._current_transformation_step,
				source_content_type=content_type,
				source_object_id=source_table.id
			)
		except Exception as e:
			print(f"Error adding step input: {e}")

	def add_step_output(self, target_table):
		"""Add an output target to the current transformation step"""
		if not hasattr(self, '_current_transformation_step') or not self._current_transformation_step:
			return

		try:
			content_type = ContentType.objects.get_for_model(target_table.__class__)
			TransformationStepOutput.objects.create(
				step=self._current_transformation_step,
				target_content_type=content_type,
				target_object_id=target_table.id
			)
		except Exception as e:
			print(f"Error adding step output: {e}")

	def create_data_flow_edge(self, source_table, target_table, flow_type='DATA', row_count=0, value_sum=None):
		"""Create a data flow edge between two tables"""
		if not self.lineage_enabled or not self.trail:
			return None

		try:
			source_content_type = self._get_content_type(source_table)
			target_content_type = self._get_content_type(target_table)

			# Get labels
			source_label = source_table.name if hasattr(source_table, 'name') else str(source_table)
			target_label = target_table.name if hasattr(target_table, 'name') else str(target_table)

			edge_key = (
				self.trail.id,
				source_content_type.id,
				source_table.id,
				target_content_type.id,
				target_table.id,
				flow_type
			)
			existing = self._data_flow_edge_cache.get(edge_key)
			if existing is not None:
				# Update row count if higher
				if row_count > existing.row_count:
					existing.row_count = row_count
					existing.save()
				return existing

			edge = DataFlowEdge.objects.create(
				trail=self.trail,
				source_content_type=source_content_type,
				source_object_id=source_table.id,
				source_label=source_label,
				target_content_type=target_content_type,
				target_object_id=target_table.id,
				target_label=target_label,
				flow_type=flow_type,
				row_count=row_count,
				value_sum=value_sum
			)
			self._data_flow_edge_cache[edge_key] = edge

			self._debug(f"Created data flow edge: {source_label} -> {target_label} ({flow_type}, {row_count} rows)")
			return edge

		except Exception as e:
			print(f"Error creating data flow edge: {e}")
			return None

	def start_calculation_chain(self, chain_name, output_table="", output_cell=""):
		"""Start tracking a calculation chain"""
		if not self.lineage_enabled or not self.trail:
			return None

		try:
			# Store current calculation name for use in other tracking methods
			self.current_calculation = chain_name

			chain = CalculationChain.objects.create(
				trail=self.trail,
				chain_name=chain_name,
				output_table=output_table,
				output_cell_name=output_cell,
				started_at=datetime.now()
			)

			self._current_calculation_chain = chain
			self._debug(f"Started calculation chain: {chain_name}")
			return chain

		except Exception as e:
			print(f"Error starting calculation chain: {e}")
			return None

	def end_calculation_chain(self, final_value=None, final_string_value=None,
							  total_source_rows=0, total_contributing_rows=0):
		"""End the current calculation chain"""
		if not hasattr(self, '_current_calculation_chain') or not self._current_calculation_chain:
			return

		try:
			chain = self._current_calculation_chain
			chain.completed_at = datetime.now()
			chain.final_value = final_value
			chain.final_string_value = final_string_value
			chain.total_source_rows = total_source_rows
			chain.total_contributing_rows = total_contributing_rows

			# Count total steps in chain
			chain.total_steps = CalculationChainStep.objects.filter(chain=chain).count()

			chain.save()

			self._debug(f"Completed calculation chain: {chain.chain_name} = {final_value}")

			self._current_calculation_chain = None
			self.current_calculation = None

		except Exception as e:
			print(f"Error ending calculation chain: {e}")

	def add_step_to_chain(self, step):
		"""Add a transformation step to the current calculation chain"""
		if not hasattr(self, '_current_calculation_chain') or not self._current_calculation_chain:
			return

		if not step:
			return

		try:
			existing_count = CalculationChainStep.objects.filter(
				chain=self._current_calculation_chain
			).count()

			CalculationChainStep.objects.create(
				chain=self._current_calculation_chain,
				step=step,
				order_in_chain=existing_count + 1
			)
		except Exception as e:
			print(f"Error adding step to chain: {e}")

	def track_cell_lineage(self, report_template, cell_code, computed_value,
						   framework='FINREP', row_key='', column_key=''):
		"""Track lineage for an output cell"""
		if not self.lineage_enabled or not self.trail:
			return None

		try:
			# Check if cell already exists
			cell, created = CellLineage.objects.update_or_create(
				trail=self.trail,
				report_template=report_template,
				cell_code=cell_code,
				row_key=row_key,
				column_key=column_key,
				defaults={
					'framework': framework,
					'computed_value': computed_value if isinstance(computed_value, (int, float)) else None,
					'computed_string_value': str(computed_value) if computed_value is not None else None,
					'calculation_chain': getattr(self, '_current_calculation_chain', None)
				}
			)

			if created:
				self._debug(f"Created cell lineage: {framework} {report_template} [{cell_code}] = {computed_value}")

			return cell

		except Exception as e:
			print(f"Error tracking cell lineage: {e}")
			return None

	def add_cell_source_row(self, cell, source_row, contribution_type='', contributed_value=None):
		"""Add a source row to a cell's lineage"""
		if not cell or not source_row:
			return

		try:
			row_content_type = self._get_content_type(source_row)

			CellSourceRow.objects.create(
				cell=cell,
				row_content_type=row_content_type,
				row_object_id=source_row.id,
				contribution_type=contribution_type,
				contributed_value=contributed_value
			)
		except Exception as e:
			print(f"Error adding cell source row: {e}")

	def ensure_function_column_references(self, function, dependency_strings):
		"""
		Ensure FunctionColumnReference entries exist for a function's dependencies.

		Dependencies must be fully qualified names like "Other_loans.GRSS_CRRYNG_AMNT"
		meaning the GRSS_CRRYNG_AMNT column on the Other_loans table.
		"""
		if not self.lineage_enabled or not function:
			return

		created_count = 0
		for dep in dependency_strings:
			try:
				# Handle special "base." prefix (for polymorphic references)
				dep_clean = dep.replace('base.', '') if dep.startswith('base.') else dep

				# Require fully qualified name: TABLE_NAME.COLUMN_NAME
				if '.' not in dep_clean:
					self._debug(f"Dependency '{dep}' must be fully qualified (TABLE.COLUMN) for {function.name}")
					continue

				table_name, column_name = dep_clean.rsplit('.', 1)
				resolution_key = ('function_column', dep_clean)
				if resolution_key in self._dependency_resolution_cache:
					field_obj = self._dependency_resolution_cache[resolution_key]
					if field_obj is None:
						continue
				else:
					field_obj = None

				# Try as DatabaseField with exact table name
				if not field_obj:
					field_obj = DatabaseField.objects.filter(
						name=column_name,
						table__name=table_name
					).first()

				# Try as DatabaseField with prefixed table name (e.g., F_05_01_REF_FINREP_3_0_Other_loans)
				if not field_obj:
					field_obj = DatabaseField.objects.filter(
						name=column_name,
						table__name__endswith=f"_{table_name}"
					).first()

				# Try as Function with exact table name
				if not field_obj:
					field_obj = Function.objects.filter(
						name=column_name,
						table__name=table_name
					).first()

				# Try as Function with prefixed table name
				if not field_obj:
					field_obj = Function.objects.filter(
						name=column_name,
						table__name__endswith=f"_{table_name}"
					).first()

				# Try as Function with qualified name (TABLE.COLUMN format in function name)
				if not field_obj:
					field_obj = Function.objects.filter(
						name__endswith=f".{column_name}",
						table__name=table_name
					).first()

				if not field_obj:
					field_obj = Function.objects.filter(
						name__endswith=f".{column_name}",
						table__name__endswith=f"_{table_name}"
					).first()

				if field_obj:
					self._dependency_resolution_cache[resolution_key] = field_obj
					content_type = self._get_content_type(field_obj)
					ref_key = (function.id, content_type.id, field_obj.id, self.trail.id)
					if ref_key in self._function_column_reference_keys:
						continue

					# Check for existing record scoped to THIS trail
					existing = FunctionColumnReference.objects.filter(
						function=function,
						content_type=content_type,
						object_id=field_obj.id,
						trail=self.trail  # Scope to current trail
					).exists()

					if not existing:
						FunctionColumnReference.objects.create(
							function=function,
							content_type=content_type,
							object_id=field_obj.id,
							dependency_string=dep,  # Store original dependency string
							trail=self.trail  # Associate with current trail
						)
						created_count += 1
						table_info = field_obj.table.name if hasattr(field_obj, 'table') and field_obj.table else 'unknown'
						self._debug(f"Created FunctionColumnReference: {function.name} -> {dep} (resolved to {table_info}.{column_name})")
					self._function_column_reference_keys.add(ref_key)
				else:
					self._dependency_resolution_cache[resolution_key] = None
					self._debug(f"Could not find '{column_name}' on table '{table_name}' for {function.name}")

			except Exception as e:
				print(f"Error creating function column reference for {dep}: {e}")

		if created_count > 0:
			self._debug(f"Created {created_count} FunctionColumnReference entries for {function.name}")

	def ensure_table_creation_sources(self, derived_table, source_table_names):
		"""
		Ensure TableCreationSourceTable entries exist for a derived table's sources.
		"""
		if not self.lineage_enabled or not derived_table:
			return

		# Get or create TableCreationFunction for this derived table
		tcf = derived_table.table_creation_function
		if not tcf:
			# Create FunctionText first (required by TableCreationFunction)
			func_text = FunctionText.objects.create(
				text=f"# Table creation function for {derived_table.name}",
				language='python'
			)
			tcf = TableCreationFunction.objects.create(
				name=f"create_{derived_table.name}",
				function_text=func_text
			)
			derived_table.table_creation_function = tcf
			derived_table.save()

		created_count = 0
		for source_name in source_table_names:
			try:
				# Find source table
				source_table = DatabaseTable.objects.filter(name=source_name).first()
				if not source_table:
					source_table = DerivedTable.objects.filter(name=source_name).first()

				if source_table:
					content_type = self._get_content_type(source_table)
					source_ref_key = (tcf.id, content_type.id, source_table.id)
					if source_ref_key in self._table_creation_source_table_keys:
						continue

					# Check if reference already exists
					existing = TableCreationSourceTable.objects.filter(
						table_creation_function=tcf,
						content_type=content_type,
						object_id=source_table.id
					).exists()

					if not existing:
						TableCreationSourceTable.objects.create(
							table_creation_function=tcf,
							content_type=content_type,
							object_id=source_table.id
						)
						created_count += 1
						self._debug(f"Created TableCreationSourceTable: {derived_table.name} <- {source_name}")
					self._table_creation_source_table_keys.add(source_ref_key)
				else:
					self._debug(f"Source table '{source_name}' not found for {derived_table.name}")

			except Exception as e:
				print(f"Error creating table creation source for {source_name}: {e}")

		if created_count > 0:
			self._debug(f"Created {created_count} TableCreationSourceTable entries for {derived_table.name}")

	def create_derived_row_source_reference(self, derived_row, source_row):
		"""Create a DerivedRowSourceReference between two rows"""
		if not derived_row or not source_row:
			return None

		try:
			content_type = self._get_content_type(source_row)
			ref_key = (derived_row.id, content_type.id, source_row.id)
			if ref_key in self._derived_row_source_reference_keys:
				return None

			# Check if reference already exists
			existing = DerivedRowSourceReference.objects.filter(
				derived_row=derived_row,
				content_type=content_type,
				object_id=source_row.id
			).exists()

			if not existing:
				ref = DerivedRowSourceReference.objects.create(
					derived_row=derived_row,
					content_type=content_type,
					object_id=source_row.id
				)
				self._derived_row_source_reference_keys.add(ref_key)
				self._debug(f"Created DerivedRowSourceReference: row {derived_row.id} <- row {source_row.id}")
				return ref
			self._derived_row_source_reference_keys.add(ref_key)

		except Exception as e:
			print(f"Error creating derived row source reference: {e}")

		return None

	def create_evaluated_function_source_value(self, evaluated_function, source_value_obj):
		"""Create an EvaluatedFunctionSourceValue relationship"""
		if not evaluated_function or not source_value_obj:
			return None

		try:
			content_type = self._get_content_type(source_value_obj)
			ref_key = (evaluated_function.id, content_type.id, source_value_obj.id)
			if ref_key in self._evaluated_function_source_value_keys:
				return None

			# Check if reference already exists
			existing = EvaluatedFunctionSourceValue.objects.filter(
				evaluated_function=evaluated_function,
				content_type=content_type,
				object_id=source_value_obj.id
			).exists()

			if not existing:
				ref = EvaluatedFunctionSourceValue.objects.create(
					evaluated_function=evaluated_function,
					content_type=content_type,
					object_id=source_value_obj.id
				)
				self._evaluated_function_source_value_keys.add(ref_key)
				self._debug(f"Created EvaluatedFunctionSourceValue: {evaluated_function.function.name} <- value {source_value_obj.id}")
				return ref
			self._evaluated_function_source_value_keys.add(ref_key)

		except Exception as e:
			print(f"Error creating evaluated function source value: {e}")

		return None

	def finalize_lineage(self):
		"""
		Finalize lineage tracking by ensuring all relationships are properly created.
		This should be called after all processing is complete.
		"""
		if not self.lineage_enabled or not self.trail:
			return

		self._debug("Finalizing lineage tracking...")

		try:
			# CRITICAL: Use the collector to create all deferred relationships
			# This resolves relationships that couldn't be created during execution
			# because the referenced objects didn't exist yet
			self._debug("\n=== Running Deferred Resolution via LineageCollector ===")
			collector_stats = finalize_collector(self.trail, self.metadata_trail)
			if collector_stats and self.debug_lineage:
				self._debug("Collector deferred resolution results:")
				for key, value in collector_stats.items():
					self._debug(f"  {key}: {value}")

			for edge in DataFlowEdge.objects.filter(trail=self.trail):
				self._data_flow_edge_cache[(
					self.trail.id,
					edge.source_content_type_id,
					edge.source_object_id,
					edge.target_content_type_id,
					edge.target_object_id,
					edge.flow_type
				)] = edge

			current_database_table_ids = set(AortaTableReference.objects.filter(
				metadata_trail=self.metadata_trail,
				table_content_type='DatabaseTable'
			).values_list('table_id', flat=True))
			current_derived_table_ids = set(AortaTableReference.objects.filter(
				metadata_trail=self.metadata_trail,
				table_content_type='DerivedTable'
			).values_list('table_id', flat=True))

			# Build data flow edges from table creation source tables
			tcf_refs = TableCreationSourceTable.objects.filter(
				table_creation_function__derivedtable__in=DerivedTable.objects.filter(
					id__in=current_derived_table_ids
				)
			).select_related('table_creation_function', 'content_type')

			for ref in tcf_refs:
				try:
					# Get source table
					if ref.content_type.model == 'databasetable':
						if ref.object_id not in current_database_table_ids:
							continue
						source_table = DatabaseTable.objects.get(id=ref.object_id)
					else:
						if ref.object_id not in current_derived_table_ids:
							continue
						source_table = DerivedTable.objects.get(id=ref.object_id)

					# Get target table
					target_table = ref.table_creation_function.derivedtable_set.first()

					if source_table and target_table:
						# Create data flow edge if it doesn't exist
						self.create_data_flow_edge(source_table, target_table, 'DATA')
				except Exception as e:
					print(f"Error creating data flow edge from TCF ref: {e}")

			# Count statistics
			if self.debug_lineage:
				current_derived_tables = DerivedTable.objects.filter(id__in=current_derived_table_ids)
				stats = {
					'database_tables': len(current_database_table_ids),
					'derived_tables': len(current_derived_table_ids),
					'function_column_references': FunctionColumnReference.objects.filter(
						function__table__in=current_derived_tables
					).count(),
					'table_creation_source_tables': TableCreationSourceTable.objects.filter(
						table_creation_function__derivedtable__in=current_derived_tables
					).count(),
					'data_flow_edges': DataFlowEdge.objects.filter(trail=self.trail).count(),
					'transformation_steps': TransformationStep.objects.filter(trail=self.trail).count(),
					'calculation_chains': CalculationChain.objects.filter(trail=self.trail).count(),
				}

				self._debug(f"Lineage finalization complete:")
				for key, value in stats.items():
					self._debug(f"  {key}: {value}")

		except Exception as e:
			print(f"Error finalizing lineage: {e}")

	def get_lineage_trail(self):
		"""Get the current lineage trail"""
		return self.trail

	def export_lineage_graph(self, trail_id=None):
		"""Export lineage as a graph structure for visualization"""
		if trail_id:
			trail = Trail.objects.get(id=trail_id)
		else:
			trail = self.trail

		if not trail:
			return None

		# Build graph structure
		graph = {
			'nodes': [],
			'edges': [],
			'trail': {
				'id': trail.id,
				'name': trail.name,
				'created_at': trail.created_at.isoformat()
			}
		}

		# Add table nodes
		for table_ref in trail.metadata_trail.table_references.all():
			if table_ref.table_content_type == 'DatabaseTable':
				table = DatabaseTable.objects.get(id=table_ref.table_id)
				graph['nodes'].append({
					'id': f'table_{table.id}',
					'type': 'DatabaseTable',
					'name': table.name
				})
			elif table_ref.table_content_type == 'DerivedTable':
				table = DerivedTable.objects.get(id=table_ref.table_id)
				graph['nodes'].append({
					'id': f'table_{table.id}',
					'type': 'DerivedTable',
					'name': table.name
				})

		# TODO: Add edges based on function references and data flow

		return graph


# Original Orchestration class from develop branch
class OrchestrationOriginal:
	# Class variable to track initialized objects
	_initialized_objects = set()

	def init(self,theObject):
		# Check if this object has already been initialized
		object_id = id(theObject)
		if object_id in OrchestrationOriginal._initialized_objects:
			print(f"Object of type {theObject.__class__.__name__} already initialized, skipping.")
			# Even if we're skipping full initialization, we still need to ensure references are set
			self._ensure_references_set(theObject)
			return

		# Mark this object as initialized
		OrchestrationOriginal._initialized_objects.add(object_id)

		# Set up references for the object
		self._ensure_references_set(theObject)

	def _ensure_references_set(self, theObject):
		"""
		Ensure that all table references are properly set for the object.
		This is called both during full initialization and when initialization is skipped.
		"""
		references = [method for method in dir(theObject.__class__) if not callable(
		getattr(theObject.__class__, method)) and not method.startswith('__')]
		for eReference in references:
			if eReference.endswith("Table"):
				# Only set the reference if it's currently None
				if getattr(theObject, eReference) is None:
					from django.apps import apps
					table_name = eReference.split('_Table')[0]

					# For ANCRDT intermediate tables - skip Django model lookup, create directly
					is_ancrdt_intermediate = (table_name.startswith('ANCRDT_') and
						any(pattern in table_name for pattern in ['Union', 'Loans_and_advances', '_filtered_', '_aggregated_']))

					relevant_model = None
					if not is_ancrdt_intermediate:
						try:
							relevant_model = apps.get_model('pybirdai',table_name)
						except LookupError:
							print("LookupError: " + table_name)

					if relevant_model:
						print("relevant_model: " + str(relevant_model))
						newObject = relevant_model.objects.all()
						print("newObject: " + str(newObject))
						# Always set the QuerySet even if empty (empty QuerySet evaluates to False in boolean context)
						setattr(theObject,eReference,newObject)
						if newObject.exists():
							CSVConverter.persist_object_as_csv(newObject,True);

					else:
						newObject = OrchestrationOriginal.createObjectFromReferenceType(eReference);

						operations = [method for method in dir(newObject.__class__) if callable(
							getattr(newObject.__class__, method)) and not method.startswith('__')]

						for operation in operations:
							if operation == "init":
								try:
									getattr(newObject, operation)()
								except Exception as e:
									import traceback
									print(f" could not call function called {operation}:")
									traceback.print_exc()

						setattr(theObject,eReference,newObject)

	@classmethod
	def reset_initialization(cls):
		"""
		Reset the initialization tracking.
		This can be useful for testing or when re-initialization is required.
		"""
		cls._initialized_objects.clear()
		print("Initialization tracking has been reset.")

	@classmethod
	def is_initialized(cls, obj):
		"""
		Check if an object has been initialized.

		Args:
			obj: The object to check

		Returns:
			bool: True if the object has been initialized, False otherwise
		"""
		return id(obj) in cls._initialized_objects

	@staticmethod
	def createObjectFromReferenceType(eReference):
		try:
			# First try the old output_tables location for backwards compatibility
			try:
				cls = getattr(importlib.import_module('pybirdai.process_steps.filter_code.output_tables'), eReference)
				new_object = cls()
				return new_object
			except (ImportError, AttributeError):
				pass

			# If that fails, try to find the class in the logic files
			# Extract the report prefix from the class name (e.g., F_05_01_REF_FINREP_3_0 from F_05_01_REF_FINREP_3_0_Other_loans_Table)
			if "_" in eReference:
				parts = eReference.split("_")
				# Look for report pattern: F_XX_XX_REF_FINREP_X_X
				if len(parts) >= 7 and parts[0] == "F" and parts[3] == "REF" and parts[4] == "FINREP":
					# Extract report prefix (first 7 parts: F_05_01_REF_FINREP_3_0)
					report_prefix = "_".join(parts[:7])
					logic_module_name = f"pybirdai.process_steps.filter_code.{report_prefix}_logic"

					try:
						module = importlib.import_module(logic_module_name)
						cls = getattr(module, eReference)
						new_object = cls()
						return new_object
					except (ImportError, AttributeError) as e:
						print(f"Could not find {eReference} in {logic_module_name}: {e}")

				# Check for ANCRDT pattern: ANCRDT_INSTRMNT_C_1_UnionTable
				if len(parts) >= 2 and parts[0] == "ANCRDT":
					# Extract report prefix by finding the _C_<number> pattern
					# Pattern: ANCRDT_INSTRMNT_C_1_Loans_and_advances_Table -> ANCRDT_INSTRMNT_C_1
					# Pattern: ANCRDT_INSTRMNT_C_1_UnionTable -> ANCRDT_INSTRMNT_C_1
					match = re.search(r'(ANCRDT_\w+_C_\d+)', eReference)
					if match:
						report_prefix = match.group(1)
					else:
						# Fallback to old suffix removal logic for backward compatibility
						report_prefix = eReference
						for suffix in ['_UnionTable', '_Table', '_UnionItem', '_Base']:
							if report_prefix.endswith(suffix):
								report_prefix = report_prefix[:-len(suffix)]
								break

					# Import from filter_code (executable production code)
					logic_module_name = f"pybirdai.process_steps.filter_code.{report_prefix}_logic"

					try:
						module = importlib.import_module(logic_module_name)
						cls = getattr(module, eReference)
						new_object = cls()
						return new_object
					except (ImportError, AttributeError) as e:
						print(f"Could not find {eReference} in {logic_module_name}: {e}")

			# If all else fails, print error
			print(f"Error: Could not find class {eReference} in any expected location")
			return None
		except Exception as e:
			print(f"Error creating object from reference {eReference}: {e}")
			return None


# Factory function to create the appropriate Orchestration instance
def create_orchestration():
	"""
	Factory function that returns the appropriate Orchestration instance
	based on the context configuration.
	"""
	from pybirdai.context.context import Context

	# Use the static method to read directly from config file
	# This avoids the issue where Context class attribute isn't updated yet
	lineage_enabled = Context.get_current_lineage_setting()
	debug_lineage = os.environ.get('PYBIRDAI_DEBUG_LINEAGE', '').lower() in {'1', 'true', 'yes', 'on'}

	if lineage_enabled:
		if debug_lineage:
			print("Using lineage-enhanced orchestrator")
		return OrchestrationWithLineage()
	else:
		if debug_lineage:
			print("Using original orchestrator")
		return OrchestrationOriginal()


# For backwards compatibility - Orchestration points to the factory function result
def Orchestration():
	"""
	Factory function that returns the appropriate Orchestration instance.
	This maintains backwards compatibility while allowing version selection.
	"""
	return create_orchestration()
