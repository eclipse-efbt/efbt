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
			
			print(f"Tracked table initialization: {table_name}")
	
	def _track_table_columns(self, table_obj, aorta_table):
		"""Track columns/fields in a table"""
		try:
			# Get all non-method attributes that could be columns
			attributes = [attr for attr in dir(table_obj) 
						if not attr.startswith('_') 
						and not callable(getattr(table_obj, attr, None))]
			
			# Common column patterns in generated code
			column_patterns = ['CRRYNG_AMNT', 'ACCNTNG_CLSSFCTN', 'OBSRVD_AGNT', 
							'INSTRMNT_ID', 'PRTY_ID', 'RPRTNG_AGNT_ID', 'OBSRVTN_DT']
			
			# Only track fields for DatabaseTable instances
			# For DerivedTable instances, columns are tracked as Functions
			if isinstance(aorta_table, DatabaseTable):
				# Track columns based on patterns and actual object structure
				for attr in attributes:
					if (any(pattern in attr for pattern in column_patterns) or 
						attr.upper() == attr):  # Uppercase attributes are likely columns
						
						# Create DatabaseField for this column
						db_field = DatabaseField.objects.create(
							name=attr,
							table=aorta_table
						)
						
						print(f"Tracked column: {aorta_table.name}.{attr}")
		except Exception as e:
			print(f"Error tracking columns for {aorta_table.name}: {e}")
	
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
				print(f"No derived row context for value computation: {function_name}")
				return
			
			# Get the derived row
			derived_row = DerivedTableRow.objects.get(id=derived_row_id)
			
			# Find the corresponding Function object
			function_parts = function_name.split('.')
			class_name = function_parts[0] if len(function_parts) > 1 else 'DynamicFunctions'
			method_name = function_parts[-1]
			
			# Look for the function in the derived table
			derived_table = derived_row.populated_table.table
			functions = derived_table.derived_functions.filter(name=function_name)
			
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
		
		





