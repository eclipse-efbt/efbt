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
    EvaluatedFunctionSourceValue, TableCreationSourceTable
)
from datetime import datetime
from django.contrib.contenttypes.models import ContentType

import importlib
class Orchestration:
	# Class variable to track initialized objects
	_initialized_objects = set()
	
	# AORTA lineage tracking
	def __init__(self):
		self.trail = None
		self.metadata_trail = None
		self.current_populated_tables = {}  # Map table names to PopulatedTable instances
		self.current_rows = {}  # Track current row being processed
		self.lineage_enabled = True  # Can be disabled for performance
		
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
			
			# Determine if this is a Django model or a derived table
			is_django_model = self._is_django_model(table_name)
			
			if is_django_model:
				# Create DatabaseTable for Django model classes
				aorta_table = DatabaseTable.objects.create(name=table_name)
			else:
				# Create DerivedTable for non-Django model classes
				aorta_table = DerivedTable.objects.create(name=table_name)
			
			# Add to metadata trail
			if self.metadata_trail:
				table_type = 'DatabaseTable' if is_django_model else 'DerivedTable'
				AortaTableReference.objects.create(
					metadata_trail=self.metadata_trail,
					table_content_type=table_type,
					table_id=aorta_table.id
				)
			else:
				print(f"Warning: No metadata_trail available for table {table_name}")
			
			# Create appropriate populated/evaluated table
			if self.trail:
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
			else:
				print(f"Warning: No trail available for populated table {table_name}")
				return
			
			self.current_populated_tables[table_name] = populated_table
			
			# Track table columns/fields
			self._track_table_columns(obj, aorta_table)
			
			# Analyze table creation functions (calc_ methods)
			self._analyze_table_creation_functions(obj, aorta_table)
			
			print(f"Tracked table initialization: {table_name}")
	
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
					print(f"Tracked column: {aorta_table.name}.{field_name}")
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
		if object_id in Orchestration._initialized_objects:
			print(f"Object of type {theObject.__class__.__name__} already initialized, skipping.")
			# Even if we're skipping full initialization, we still need to ensure references are set
			self._ensure_references_set(theObject)
			return
		
		# Mark this object as initialized
		Orchestration._initialized_objects.add(object_id)
		
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
						newObject = Orchestration.createObjectFromReferenceType(eReference);
						
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
		derived_table = None
		
		# Check if we already have a derived table for this class
		existing_tables = DerivedTable.objects.filter(name=class_name)
		if existing_tables.exists():
			derived_table = existing_tables.first()
		else:
			derived_table = DerivedTable.objects.create(name=class_name)
			
			# Add to metadata trail
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
			
			self.current_populated_tables[class_name] = evaluated_table
		
		# Create Function record
		function_text = FunctionText.objects.create(
			text=source_code or function_name,
			language='python'
		)
		
		function = Function.objects.create(
			name=function_name,
			function_text=function_text,
			table=derived_table
		)
		
		# Note: TableCreationFunction instances are now created in _analyze_table_creation_functions
		# during table initialization, which analyzes class variables for source tables
		
		# Track column references
		for col_ref in source_columns:
			try:
				# Try to resolve the actual column object
				resolved_field = self._resolve_column_reference(col_ref)
				if resolved_field:
					# Create FunctionColumnReference
					content_type = ContentType.objects.get_for_model(resolved_field.__class__)
					FunctionColumnReference.objects.create(
						function=function,
						content_type=content_type,
						object_id=resolved_field.id
					)
					print(f"Tracked column reference: {function_name} -> {col_ref}")
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
				
				# Check if this calc_ method has a @lineage decorator
				lineage_dependencies = self._extract_lineage_dependencies(calc_method)
				if lineage_dependencies:
					# Use lineage dependencies for more detailed function text
					source_code += f"\n# Lineage dependencies: {lineage_dependencies}"
				
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
				
				# Create TableCreationSourceTable entries
				for source_table_name in source_table_names:
					# Find the source table in DatabaseTable or DerivedTable
					source_table = self._find_table_by_name(source_table_name)
					if source_table:
						content_type = ContentType.objects.get_for_model(source_table.__class__)
						TableCreationSourceTable.objects.create(
							table_creation_function=table_creation_function,
							content_type=content_type,
							object_id=source_table.id
						)
						print(f"Tracked table creation source: {full_function_name} -> {source_table_name}")
				
				print(f"Created TableCreationFunction for {full_function_name} with {len(source_table_names)} source tables")
		
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
			# Check if the method has lineage decorator information
			if hasattr(method, '__wrapped__'):
				# This suggests a decorator was applied
				# We can't easily extract the original decorator args here,
				# but we can indicate that lineage was applied
				return "Method has @lineage decorator"
			return None
		except:
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
			
			# Track individual column values (only for DatabaseRow)
			if not is_derived_table:
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
			
			print(f"Tracked row processing: {table_name} row {row_identifier}")
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
			
			print(f"Tracked column value: {table.name}.{column_name} = {value}")
		except Exception as e:
			print(f"Error tracking column value {column_name}: {e}")
	
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
			
			print(f"Tracked derived row processing: {table_name}")
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
			
			# Create EvaluatedFunction
			evaluated_function = EvaluatedFunction.objects.create(
				value=str(computed_value) if computed_value is not None else None,
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
			
			print(f"Tracked value computation: {function_name} with {len(source_values)} source values = {computed_value}")
			return evaluated_function
			
		except Exception as e:
			print(f"Error tracking value computation: {e}")
			return None
	
	def _ensure_derived_row_context(self, derived_obj, function_name):
		"""Ensure a derived row context exists for the given derived object"""
		if not self.lineage_enabled or not self.trail:
			return None
			
		try:
			# Get the class name to determine table name
			class_name = derived_obj.__class__.__name__
			table_name = class_name
			
			# Ensure we have an EvaluatedDerivedTable for this derived object
			if table_name not in self.current_populated_tables:
				# Create a DerivedTable
				derived_table = DerivedTable.objects.create(name=table_name)
				
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
				print(f"Created EvaluatedDerivedTable for {table_name}")
			
			# Get the EvaluatedDerivedTable
			evaluated_table = self.current_populated_tables[table_name]
			
			# Create a unique identifier for this derived row based on object identity
			row_identifier = f"{class_name}_{id(derived_obj)}"
			
			# Check if we already have a DerivedTableRow for this object
			existing_rows = evaluated_table.derivedtablerow_set.filter(row_identifier=row_identifier)
			if existing_rows.exists():
				return existing_rows.first().id
			
			# Create a new DerivedTableRow
			derived_row = DerivedTableRow.objects.create(
				populated_table=evaluated_table,
				row_identifier=row_identifier
			)
			
			print(f"Created DerivedTableRow {derived_row.id} for {function_name}")
			return derived_row.id
			
		except Exception as e:
			print(f"Error ensuring derived row context: {e}")
			return None
	
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
	
	def track_data_processing(self, table_name, data_items):
		"""Track processing of data items in a table"""
		if not self.lineage_enabled or not self.trail or not self.metadata_trail:
			return
		
		try:
			# Ensure we have a populated table for this table name
			if table_name not in self.current_populated_tables:
				# Determine if this is a Django model or a derived table
				is_django_model = self._is_django_model(table_name)
				
				# Create appropriate table type
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
				
				if self.trail:
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
				else:
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
			
			print(f"Tracked data processing for {table_name}: {len(data_items)} items")
			
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
		
		





