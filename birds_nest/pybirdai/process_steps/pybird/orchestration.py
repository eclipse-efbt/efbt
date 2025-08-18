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
    CalculationUsedRow, CalculationUsedField
)
from datetime import datetime
from django.contrib.contenttypes.models import ContentType

import importlib
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

		# Note: Do not automatically register this instance globally
		# Global registration should be done explicitly when setting up lineage tracking

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
		try:
			# Try to get the model from Django's app registry
			apps.get_model('pybirdai', table_name)
			return True
		except LookupError:
			# Not a Django model
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
				print(f"Reusing existing table for: {table_name}")
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
						print(f"Found existing DatabaseTable for: {table_name}")
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
						print(f"Found existing DerivedTable for: {table_name}")

			# Create new table if not found
			if not aorta_table:
				if is_django_model:
					# Create DatabaseTable for Django model classes
					aorta_table = DatabaseTable.objects.create(name=table_name)
				else:
					# Create DerivedTable for non-Django model classes
					aorta_table = DerivedTable.objects.create(name=table_name)
				print(f"Created new table for: {table_name}")

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

			# Track table columns/fields only if this is a new table
			if not table_exists:
				self._track_table_columns(obj, aorta_table)

				# Analyze table creation functions (calc_ methods)
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
						print(f"Using Django model fields for {table_name}: {len(fields_to_track)} fields")
					except Exception as e:
						print(f"Error getting Django model fields for {table_name}: {e}")
						fields_to_track = []
				else:
					# For non-Django tables, detect column methods from the row objects they contain
					fields_to_track = self._detect_table_fields_from_row_type(table_obj)

				# Create DatabaseField instances
				for field_name in fields_to_track:
					db_field = DatabaseField.objects.create(
						name=field_name,
						table=aorta_table
					)
					# print(f"Tracked column: {aorta_table.name}.{field_name}")
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
			print(f"Object of type {theObject.__class__.__name__} already initialized, skipping.")
			# Even if we're skipping full initialization, we still need to ensure references are set
			self._ensure_references_set(theObject)
			return

		# Mark this object as initialized
		self.__class__._initialized_objects.add(object_id)

		# Check if we have lineage tracking enabled globally and this looks like a table
		from pybirdai.annotations.decorators import _lineage_context
		global_orchestration = _lineage_context.get('orchestration')

		if (global_orchestration and
			hasattr(theObject, '__class__') and
			theObject.__class__.__name__.endswith('_Table')):

			print(f"DEBUG: Using global orchestration for {theObject.__class__.__name__}")
			print(f"  Global Trail: {global_orchestration.trail.id if global_orchestration.trail else None}")
			print(f"  Global MetaDataTrail: {global_orchestration.metadata_trail.id if global_orchestration.metadata_trail else None}")

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
					relevant_model = None
					try:
						relevant_model = apps.get_model('pybirdai',table_name)
					except LookupError:
						print("LookupError: " + table_name)

					if relevant_model:
						print("relevant_model: " + str(relevant_model))
						newObject = relevant_model.objects.all()
						print("newObject: " + str(newObject))
						if newObject:
							setattr(theObject,eReference,newObject)
							CSVConverter.persist_object_as_csv(newObject,True);

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
										print(f"DEBUG: Orchestration for {newObject.__class__.__name__}:")
										print(f"  Trail: {orchestration.trail.id if orchestration.trail else None}")
										print(f"  MetaDataTrail: {orchestration.metadata_trail.id if orchestration.metadata_trail else None}")

										# First track the table itself if not already tracked
										if orchestration.metadata_trail:
											orchestration._track_object_initialization(newObject)
										else:
											print(f"WARNING: No metadata_trail for {newObject.__class__.__name__}")

										# Track any data that was populated during initialization
										table_name = newObject.__class__.__name__.replace('_Table', '')
										for attr_name in dir(newObject):
											if (not attr_name.startswith('_') and
												hasattr(newObject, attr_name)):
												attr_value = getattr(newObject, attr_name)
												if isinstance(attr_value, list) and len(attr_value) > 0:
													if orchestration.metadata_trail:
														orchestration.track_data_processing(f"{table_name}_{attr_name}", attr_value)
													else:
														print(f"WARNING: Cannot track data for {table_name}_{attr_name} - no metadata_trail")

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
			cls = getattr(importlib.import_module('pybirdai.process_steps.filter_code.output_tables'), eReference)
			new_object = cls()
			return new_object;
		except:
			print("Error: " + eReference)

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

		derived_table = None

		# Check if we already have a derived table for this class in the current trail
		existing_tables = None
		if self.metadata_trail:
			# Look for tables in the current metadata trail
			existing_refs = AortaTableReference.objects.filter(
				metadata_trail=self.metadata_trail,
				table_content_type='DerivedTable'
			)
			for ref in existing_refs:
				table = DerivedTable.objects.get(id=ref.table_id)
				if table.name == class_name:
					existing_tables = [table]
					break

		if existing_tables:
			derived_table = existing_tables[0]
		else:
			derived_table = DerivedTable.objects.create(name=class_name)

			# Add to metadata trail
			if self.metadata_trail:
				AortaTableReference.objects.create(
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
		existing_functions = Function.objects.filter(
			name=function_name,
			table=derived_table
		)

		if existing_functions.exists():
			# Reuse existing function
			function = existing_functions.first()
			# print(f"Reusing existing function: {function_name}")
		else:
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

		# Note: TableCreationFunction instances are now created in _analyze_table_creation_functions
		# during table initialization, which analyzes class variables for source tables

		# Track column references (only for newly created functions to avoid duplicates)
		if not existing_functions.exists():
			for col_ref in source_columns:
				try:
					# Try to resolve the actual column object
					resolved_field = self._resolve_column_reference(col_ref)
					if resolved_field:
						# Check if this column reference already exists
						content_type = ContentType.objects.get_for_model(resolved_field.__class__)
						existing_col_refs = FunctionColumnReference.objects.filter(
							function=function,
							content_type=content_type,
							object_id=resolved_field.id
						)

						if not existing_col_refs.exists():
							# Create FunctionColumnReference
							FunctionColumnReference.objects.create(
								function=function,
								content_type=content_type,
								object_id=resolved_field.id
							)
							# print(f"Tracked column reference: {function_name} -> {col_ref}")
				except Exception as e:
					print(f"Could not resolve column reference {col_ref}: {e}")

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

			# Find all calc_ methods in this class
			calc_methods = [name for name in dir(table_obj)
						   if name.startswith('calc_') and callable(getattr(table_obj, name))]

			for calc_method_name in calc_methods:
				calc_method = getattr(table_obj, calc_method_name)
				full_function_name = f"{class_name}.{calc_method_name}"

				# Extract source table names from class variables ending with _Table
				source_table_names = self._extract_source_tables_from_class_variables(table_obj)

				# Get function source code
				try:
					import inspect
					source_code = inspect.getsource(calc_method)
				except:
					source_code = f"def {calc_method_name}(self): # Source code not available"

				# Check if this calc_ method has a @lineage decorator and extract dependencies
				lineage_dependencies = self._extract_lineage_dependencies(calc_method)
				lineage_column_references = []
				if lineage_dependencies:
					# Use lineage dependencies for more detailed function text
					source_code += f"\n# Lineage dependencies: {lineage_dependencies}"
					# Extract column references from the lineage dependencies
					lineage_column_references = self._parse_lineage_dependencies(lineage_dependencies)

				# Check if TableCreationFunction already exists
				existing_table_creation_functions = TableCreationFunction.objects.filter(
					name=full_function_name
				)

				if existing_table_creation_functions.exists():
					# Reuse existing table creation function
					table_creation_function = existing_table_creation_functions.first()
					# print(f"Reusing existing table creation function: {full_function_name}")
				else:
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

				# Create TableCreationSourceTable entries (only for new functions)
				if not existing_table_creation_functions.exists():
					for source_table_name in source_table_names:
						# Find the source table in DatabaseTable or DerivedTable
						source_table = self._find_table_by_name(source_table_name)
						if source_table:
							content_type = ContentType.objects.get_for_model(source_table.__class__)
							# Check if this source table reference already exists
							existing_source_refs = TableCreationSourceTable.objects.filter(
								table_creation_function=table_creation_function,
								content_type=content_type,
								object_id=source_table.id
							)

							if not existing_source_refs.exists():
								TableCreationSourceTable.objects.create(
									table_creation_function=table_creation_function,
									content_type=content_type,
									object_id=source_table.id
								)
								# print(f"Tracked table creation source: {full_function_name} -> {source_table_name}")

					# Create TableCreationFunctionColumn entries for lineage dependencies
					for column_ref in lineage_column_references:
						column_obj = column_ref['column']
						reference_text = column_ref['reference_text']

						content_type = ContentType.objects.get_for_model(column_obj.__class__)
						from pybirdai.models import TableCreationFunctionColumn

						# Check if this column reference already exists
						existing_column_refs = TableCreationFunctionColumn.objects.filter(
							table_creation_function=table_creation_function,
							content_type=content_type,
							object_id=column_obj.id,
							reference_text=reference_text
						)

						if not existing_column_refs.exists():
							TableCreationFunctionColumn.objects.create(
								table_creation_function=table_creation_function,
								content_type=content_type,
								object_id=column_obj.id,
								reference_text=reference_text
							)
							# print(f"Tracked column reference: {full_function_name} -> {column_obj}")

				print(f"Created TableCreationFunction for {full_function_name} with {len(source_table_names)} source tables and {len(lineage_column_references)} column references")

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

	def _extract_lineage_dependencies(self, method):
		"""Extract dependencies from @lineage decorator if present"""
		try:
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
					print(f"Extracted lineage dependencies: {dependencies_text}")
					return dependencies_text

			return None

		except Exception as e:
			print(f"Error extracting lineage dependencies: {e}")
			return None

	def _parse_lineage_dependencies(self, lineage_dependencies_text):
		"""Parse lineage dependencies text to extract column references"""
		column_references = []

		if not lineage_dependencies_text:
			return column_references

		try:
			# Extract column references from the lineage dependencies text
			# Look for patterns like "base.COLUMN_NAME", "table.COLUMN_NAME"
			import re

			# Find all patterns that look like column references
			# This regex looks for word.WORD patterns (table.column references)
			pattern = r'\b(\w+)\.([A-Za-z_][A-Za-z0-9_]*)\b'
			matches = re.findall(pattern, lineage_dependencies_text)

			for table_ref, column_name in matches:
				# Try to find the actual column object
				column_obj = self._find_column_by_name(column_name, table_ref)
				if column_obj:
					column_references.append({
						'column': column_obj,
						'reference_text': f"{table_ref}.{column_name}"
					})
				else:
					# If we can't find the specific column, try a broader search
					column_obj = self._find_column_by_name(column_name)
					if column_obj:
						column_references.append({
							'column': column_obj,
							'reference_text': f"{table_ref}.{column_name}"
						})

			print(f"Parsed {len(column_references)} column references from lineage dependencies")
			return column_references

		except Exception as e:
			print(f"Error parsing lineage dependencies: {e}")
			return column_references

	def _find_column_by_name(self, column_name, table_hint=None):
		"""Find a column (DatabaseField or Function) by name, optionally with table hint"""
		try:
			# First try to find in DatabaseField
			database_fields = DatabaseField.objects.filter(name=column_name)
			if table_hint:
				# Filter by table name if hint provided
				database_fields = database_fields.filter(table__name__icontains=table_hint)

			if database_fields.exists():
				return database_fields.first()

			# Then try to find in Function
			functions = Function.objects.filter(name=column_name)
			if table_hint:
				# Filter by table name if hint provided
				functions = functions.filter(table__name__icontains=table_hint)

			if functions.exists():
				return functions.first()

			# Fallback: try without table hint if we had one
			if table_hint:
				return self._find_column_by_name(column_name, None)

			return None

		except Exception as e:
			print(f"Error finding column {column_name}: {e}")
			return None

	def _find_table_by_name(self, table_name):
		"""Find a table by name in DatabaseTable or DerivedTable"""
		try:
			# First try DatabaseTable
			database_tables = DatabaseTable.objects.filter(name=table_name)
			if database_tables.exists():
				return database_tables.first()

			# Then try DerivedTable
			derived_tables = DerivedTable.objects.filter(name=table_name)
			if derived_tables.exists():
				return derived_tables.first()

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
			if not is_derived_table and isinstance(row_data, dict):
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
				else:
					# Create DatabaseRow for database tables
					db_row = DatabaseRow.objects.create(
						populated_table=populated_table,
						row_identifier=row_identifier
					)

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
			fields = table.database_fields.filter(name=column_name)

			if not fields.exists():
				# Create the field if it doesn't exist
				field = DatabaseField.objects.create(
					name=column_name,
					table=table
				)
				print(f"Created missing column: {table.name}.{column_name}")
			else:
				field = fields.first()

			# Create DatabaseColumnValue
			# Try to convert to float, otherwise use string_value
			numeric_value = None
			string_value = None

			if value is not None:
				try:
					numeric_value = float(value)
				except (ValueError, TypeError):
					string_value = str(value)

			DatabaseColumnValue.objects.create(
				value=numeric_value,
				string_value=string_value,
				column=field,
				row=db_row
			)

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

			# Track source row references
			if source_row_ids:
				for source_row_id in source_row_ids:
					try:
						source_row = DatabaseRow.objects.get(id=source_row_id)
						DerivedRowSourceReference.objects.create(
							derived_row=derived_row,
							content_type=ContentType.objects.get_for_model(DatabaseRow),
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
		if not self.lineage_enabled or not self.trail:
			return

		try:
			# Get the current derived row if available
			derived_row_id = self.current_rows.get('derived')
			if not derived_row_id:
				print(f"DEBUG: No derived row context for value computation: {function_name}")
				print(f"DEBUG: Current rows context: {self.current_rows}")
				return

			# Check cache first
			cache_key = f"{derived_row_id}:{function_name}"
			if cache_key in self.evaluated_functions_cache:
				# Return cached evaluated function
				return self.evaluated_functions_cache[cache_key]

			# Get the derived row
			derived_row = DerivedTableRow.objects.get(id=derived_row_id)

			# Find the corresponding Function object
			function_parts = function_name.split('.')
			class_name = function_parts[0] if len(function_parts) > 1 else 'DynamicFunctions'
			method_name = function_parts[-1]

			# Look for the function by name across all Function objects
			functions = Function.objects.filter(name=function_name)

			if not functions.exists():
				print(f"Function {function_name} not found for value computation")
				return

			function = functions.first()

			# Check if we already have an EvaluatedFunction for this function and row
			existing_evaluated = EvaluatedFunction.objects.filter(
				function=function,
				row=derived_row
			).first()

			if existing_evaluated:
				# We already have this function evaluated for this row
				# Since functions are immutable, the result should be the same
				# Cache it and return
				self.evaluated_functions_cache[cache_key] = existing_evaluated
				# print(f"Reusing existing EvaluatedFunction for {function_name} on row {derived_row_id}")
				return existing_evaluated

			# Create EvaluatedFunction only if it doesn't exist
			# Try to store as numeric value if possible
			numeric_value = None
			string_value = None

			if computed_value is not None:
				try:
					numeric_value = float(computed_value)
				except (ValueError, TypeError):
					string_value = str(computed_value)

			evaluated_function = EvaluatedFunction.objects.create(
				value=numeric_value,
				string_value=string_value,
				function=function,
				row=derived_row
			)

			# Track source values
			for source_value in source_values:
				if source_value is not None:
					# Try to find the corresponding DatabaseColumnValue
					source_value_obj = self._find_source_value_object(source_value)
					if source_value_obj:
						EvaluatedFunctionSourceValue.objects.create(
							evaluated_function=evaluated_function,
							content_type=ContentType.objects.get_for_model(source_value_obj.__class__),
							object_id=source_value_obj.id
						)

			# Cache the evaluated function
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
				# Check if a DerivedTable already exists for this name
				existing_derived_tables = DerivedTable.objects.filter(name=table_name)
				if existing_derived_tables.exists():
					derived_table = existing_derived_tables.first()
				else:
					# Create a new DerivedTable
					derived_table = DerivedTable.objects.create(name=table_name)

				if self.metadata_trail:
					# Check if reference already exists
					existing_refs = AortaTableReference.objects.filter(
						metadata_trail=self.metadata_trail,
						table_content_type='DerivedTable',
						table_id=derived_table.id
					)

					if not existing_refs.exists():
						AortaTableReference.objects.create(
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
				print(f"Created EvaluatedDerivedTable for {table_name}")

			# Get the EvaluatedDerivedTable
			evaluated_table = self.current_populated_tables[table_name]

			# Create a unique identifier for this derived row based on object identity
			row_identifier = f"{class_name}_{id(derived_obj)}"

			# Check if we already have a DerivedTableRow for this object
			existing_rows = evaluated_table.derivedtablerow_set.filter(row_identifier=row_identifier)
			if existing_rows.exists():
				derived_row_id = existing_rows.first().id
			else:
				# Create a new DerivedTableRow
				derived_row = DerivedTableRow.objects.create(
					populated_table=evaluated_table,
					row_identifier=row_identifier
				)
				derived_row_id = derived_row.id
				print(f"Created DerivedTableRow {derived_row_id} for {function_name}")

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
		print(f"🔍 track_calculation_used_row called: {calculation_name}, {type(row).__name__}")
		
		if not self.lineage_enabled or not self.trail:
			print(f"❌ Lineage tracking disabled or no trail: lineage_enabled={self.lineage_enabled}, trail={self.trail}")
			return
		
		try:
			# Determine the type of row
			if isinstance(row, DatabaseRow):
				content_type = ContentType.objects.get_for_model(DatabaseRow)
			elif isinstance(row, DerivedTableRow):
				content_type = ContentType.objects.get_for_model(DerivedTableRow)
			# Check if this is a Django model instance (database record)
			elif hasattr(row, '_meta') and hasattr(row._meta, 'model'):
				# This is a Django model instance - we need to create/find the appropriate DatabaseRow
				model_name = type(row).__name__
				
				# Ensure we have a database table for this model
				if model_name not in self.current_populated_tables:
					# Create database table
					db_table = DatabaseTable.objects.create(name=model_name)
					
					# Add to metadata trail
					if self.metadata_trail:
						AortaTableReference.objects.create(
							metadata_trail=self.metadata_trail,
							table_content_type='DatabaseTable',
							table_id=db_table.id
						)
					
					# Create PopulatedDataBaseTable
					populated_table = PopulatedDataBaseTable.objects.create(
						trail=self.trail,
						table=db_table
					)
					
					self.current_populated_tables[model_name] = populated_table
					print(f"Created database table for Django model: {model_name}")
				
				# Get the populated database table
				populated_table = self.current_populated_tables[model_name]
				
				# Create unique identifier for this model instance
				if hasattr(row, 'pk') and row.pk:
					object_identifier = f"{model_name}_{row.pk}"
				else:
					object_identifier = f"{model_name}_{id(row)}"
				
				# Check if we already have a database row for this model instance
				existing_rows = populated_table.databaserow_set.filter(
					row_identifier=object_identifier
				)
				
				if existing_rows.exists():
					db_row = existing_rows.first()
				else:
					# Create new database row for this model instance
					db_row = DatabaseRow.objects.create(
						populated_table=populated_table,
						row_identifier=object_identifier
					)
					
					# Create column values for the model fields
					for field in row._meta.fields:
						if hasattr(row, field.name):
							field_value = getattr(row, field.name)
							if field_value is not None:
								self._track_column_value_for_django_field(db_row, field.name, field_value, populated_table.table)
					
					print(f"Created database row for Django model {model_name}")
				
				row = db_row
				
				content_type = ContentType.objects.get_for_model(DatabaseRow)
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
						# Create derived table for this object type
						derived_table = DerivedTable.objects.create(name=table_name)
						
						# Add to metadata trail
						if self.metadata_trail:
							AortaTableReference.objects.create(
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
						print(f"Created derived table for business object type: {table_name}")
					
					# Get the evaluated derived table
					evaluated_table = self.current_populated_tables[table_name]
					
					# Create unique identifier for this specific object instance
					object_identifier = f"{row_class_name}_{id(row)}"
					
					# Check if we already have a derived row for this object
					existing_rows = evaluated_table.derivedtablerow_set.filter(
						row_identifier=object_identifier
					)
					
					if existing_rows.exists():
						tracked_row = existing_rows.first()
					else:
						# Create new derived table row for this object
						tracked_row = DerivedTableRow.objects.create(
							populated_table=evaluated_table,
							row_identifier=object_identifier
						)
						print(f"Created derived table row for {row_class_name}")
					
					if tracked_row:
						row = tracked_row
						content_type = ContentType.objects.get_for_model(DerivedTableRow)
					else:
						print(f"Failed to create derived table row for {row_class_name}")
						return
				else:
					print(f"Cannot track row of type {type(row)} - not a trackable object")
					return
			
			# Check if this row is already tracked for this calculation
			existing = CalculationUsedRow.objects.filter(
				trail=self.trail,
				calculation_name=calculation_name,
				content_type=content_type,
				object_id=row.id
			).exists()
			
			if not existing:
				used_row = CalculationUsedRow.objects.create(
					trail=self.trail,
					calculation_name=calculation_name,
					content_type=content_type,
					object_id=row.id
				)
				print(f"✅ Created CalculationUsedRow: {calculation_name} -> {type(row).__name__} (id: {row.id})")
			else:
				print(f"⚠️ CalculationUsedRow already exists for {calculation_name} -> {type(row).__name__}")
		
		except Exception as e:
			print(f"❌ Error tracking calculation used row: {e}")
			import traceback
			traceback.print_exc()
	
	def track_calculation_used_field(self, calculation_name, field_name, row=None):
		"""Track that a specific field was accessed during a calculation"""
		if not self.lineage_enabled or not self.trail:
			return
		
		try:
			# Find the field object
			field = None
			content_type = None
			
			# First try to find as DatabaseField
			database_fields = DatabaseField.objects.filter(name=field_name)
			if database_fields.exists():
				field = database_fields.first()
				content_type = ContentType.objects.get_for_model(DatabaseField)
			else:
				# Try to find as Function
				functions = Function.objects.filter(name=field_name)
				if functions.exists():
					field = functions.first()
					content_type = ContentType.objects.get_for_model(Function)
			
			if not field:
				# Try to find with more context if field_name includes table reference
				if '.' in field_name:
					parts = field_name.split('.')
					actual_field_name = parts[-1]
					database_fields = DatabaseField.objects.filter(name=actual_field_name)
					if database_fields.exists():
						field = database_fields.first()
						content_type = ContentType.objects.get_for_model(DatabaseField)
					else:
						functions = Function.objects.filter(name=actual_field_name)
						if functions.exists():
							field = functions.first()
							content_type = ContentType.objects.get_for_model(Function)
			
			if not field:
				print(f"Cannot find field {field_name} to track")
				return
			
			# Prepare row tracking if provided
			row_content_type = None
			row_object_id = None
			if row:
				if isinstance(row, DatabaseRow):
					row_content_type = ContentType.objects.get_for_model(DatabaseRow)
					row_object_id = row.id
				elif isinstance(row, DerivedTableRow):
					row_content_type = ContentType.objects.get_for_model(DerivedTableRow)
					row_object_id = row.id
			
			# Check if this field is already tracked for this calculation
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
			
			if not query.exists():
				CalculationUsedField.objects.create(
					trail=self.trail,
					calculation_name=calculation_name,
					content_type=content_type,
					object_id=field.id,
					row_content_type=row_content_type,
					row_object_id=row_object_id
				)
				# print(f"Tracked used field for {calculation_name}: {field_name}")
		
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
	
	def _track_column_value_for_django_field(self, db_row, field_name, field_value, table):
		"""Helper method to track column values for Django model fields"""
		try:
			# Find or create the DatabaseField
			fields = table.database_fields.filter(name=field_name)
			
			if not fields.exists():
				field = DatabaseField.objects.create(
					name=field_name,
					table=table
				)
			else:
				field = fields.first()
			
			# Create DatabaseColumnValue
			numeric_value = None
			string_value = None
			
			if field_value is not None:
				try:
					numeric_value = float(field_value)
				except (ValueError, TypeError):
					string_value = str(field_value)
			
			DatabaseColumnValue.objects.create(
				value=numeric_value,
				string_value=string_value,
				column=field,
				row=db_row
			)
			
		except Exception as e:
			print(f"Error tracking Django field {field_name}: {e}")

	def _find_source_value_object(self, source_value):
		"""Find the DatabaseColumnValue object for a given source value"""
		try:
			# Look for DatabaseColumnValue with matching value
			source_row_id = self.current_rows.get('source')
			if source_row_id:
				source_row = DatabaseRow.objects.get(id=source_row_id)
				column_values = source_row.column_values.filter(value=str(source_value))
				if column_values.exists():
					return column_values.first()

			# Fallback: look across all current rows
			for table_name, populated_table in self.current_populated_tables.items():
				if hasattr(populated_table, 'databaserow_set'):
					for row in populated_table.databaserow_set.all():
						column_values = row.column_values.filter(value=str(source_value))
						if column_values.exists():
							return column_values.first()

		except Exception as e:
			print(f"Error finding source value object: {e}")

		return None

	def track_data_processing(self, table_name, data_items, django_model_objects=None):
		"""Track processing of data items in a table"""
		if not self.lineage_enabled or not self.trail or not self.metadata_trail:
			return
		
		# Also track that these rows and tables are being used in calculations
		current_calculation = getattr(self, 'current_calculation', None)
		print(f"🔗 track_data_processing: table={table_name}, items={len(data_items)}, django_objects={len(django_model_objects) if django_model_objects else 0}, current_calculation={current_calculation}")
		if current_calculation and hasattr(self, 'track_calculation_used_row'):
			# Prefer Django model objects over dictionary items for tracking
			items_to_track = django_model_objects if django_model_objects else data_items
			
			# Track each data item as a used row
			for i, item in enumerate(items_to_track):
				try:
					print(f"  🔗 Attempting to track data item {i}: {type(item).__name__}")
					self.track_calculation_used_row(current_calculation, item)
					print(f"  ✅ Successfully tracked data item {i}")
				except Exception as e:
					print(f"  ❌ Failed to track data item {i}: {e}")
					pass  # Don't let tracking errors break the main processing
		
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
						print(f"Found existing DatabaseTable for: {table_name}")
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
						print(f"Found existing DerivedTable for: {table_name}")

				# Create new table only if not found
				if not temp_table:
					if is_django_model:
						temp_table = DatabaseTable.objects.create(name=table_name)
						table_type = 'DatabaseTable'
					else:
						temp_table = DerivedTable.objects.create(name=table_name)
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
								if callable(value):
									value = value()
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
					relevant_model = None
					try:
						relevant_model = apps.get_model('pybirdai',table_name)
					except LookupError:
						print("LookupError: " + table_name)

					if relevant_model:
						print("relevant_model: " + str(relevant_model))
						newObject = relevant_model.objects.all()
						print("newObject: " + str(newObject))
						if newObject:
							setattr(theObject,eReference,newObject)
							CSVConverter.persist_object_as_csv(newObject,True);

					else:
						newObject = OrchestrationOriginal.createObjectFromReferenceType(eReference);

						operations = [method for method in dir(newObject.__class__) if callable(
							getattr(newObject.__class__, method)) and not method.startswith('__')]

						for operation in operations:
							if operation == "init":
								try:
									getattr(newObject, operation)()
								except:
									print (" could not call function called " + operation)

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
			cls = getattr(importlib.import_module('pybirdai.process_steps.filter_code.output_tables'), eReference)
			new_object = cls()
			return new_object;
		except:
			print("Error: " + eReference)


# Factory function to create the appropriate Orchestration instance
def create_orchestration():
	"""
	Factory function that returns the appropriate Orchestration instance
	based on the context configuration.
	"""
	from pybirdai.context.context import Context

	if hasattr(Context, 'enable_lineage_tracking') and Context.enable_lineage_tracking:
		print("Using lineage-enhanced orchestrator")
		return OrchestrationWithLineage()
	else:
		print("Using original orchestrator")
		return OrchestrationOriginal()


# For backwards compatibility - Orchestration points to the factory function result
def Orchestration():
	"""
	Factory function that returns the appropriate Orchestration instance.
	This maintains backwards compatibility while allowing version selection.
	"""
	return create_orchestration()
