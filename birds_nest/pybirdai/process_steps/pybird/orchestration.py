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
		
		# Register this orchestration instance for lineage tracking
		from pybirdai.annotations.decorators import set_lineage_orchestration
		set_lineage_orchestration(self)
	
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
	
	def _track_object_initialization(self, obj):
		"""Track object in AORTA metadata trail"""
		obj_class_name = obj.__class__.__name__
		
		# Check if this is a table object
		if hasattr(obj, '__class__') and obj_class_name.endswith('_Table'):
			table_name = obj_class_name.replace('_Table', '')
			
			# Create DatabaseTable in AORTA
			aorta_table = DatabaseTable.objects.create(name=table_name)
			
			# Add to metadata trail
			AortaTableReference.objects.create(
				metadata_trail=self.metadata_trail,
				table_content_type='DatabaseTable',
				table_id=aorta_table.id
			)
			
			# Create PopulatedDataBaseTable
			populated_table = PopulatedDataBaseTable.objects.create(
				trail=self.trail,
				table=aorta_table
			)
			
			self.current_populated_tables[table_name] = populated_table
			print(f"Tracked table initialization: {table_name}")
	
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
						newObject = Orchestration.createObjectFromReferenceType(eReference);
						
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

	def createObjectFromReferenceType(eReference):
		try:
			cls = getattr(importlib.import_module('pybirdai.process_steps.filter_code.output_tables'), eReference)
			new_object = cls()		
			return new_object;	
		except:
			print("Error: " + eReference)
	
	# AORTA Lineage Tracking Methods
	
	def track_function_execution(self, function_name, source_columns, result_column=None):
		"""Track the execution of a function in AORTA"""
		if not self.lineage_enabled or not self.trail:
			return
		
		# Create Function record
		function_text = FunctionText.objects.create(
			text=function_name,
			language='python'
		)
		
		# For now, we'll create a placeholder derived table if needed
		# In real implementation, this should link to the actual derived table
		if not hasattr(self, '_default_derived_table'):
			self._default_derived_table = DerivedTable.objects.create(
				name="DynamicFunctions"
			)
		
		function = Function.objects.create(
			name=function_name,
			function_text=function_text,
			table=self._default_derived_table
		)
		
		# Track column references
		for col_ref in source_columns:
			# This is a simplified version - in practice, we'd resolve the actual column objects
			print(f"Tracking column reference: {function_name} -> {col_ref}")
		
		return function
	
	def track_row_processing(self, source_row_id, derived_row_id=None):
		"""Track row-level lineage"""
		if not self.lineage_enabled or not self.trail:
			return
		
		# Store current row context for value tracking
		self.current_rows['source'] = source_row_id
		self.current_rows['derived'] = derived_row_id
	
	def track_value_computation(self, function, source_values, computed_value):
		"""Track value-level lineage"""
		if not self.lineage_enabled or not self.trail:
			return
		
		# This is a simplified implementation
		# In practice, we'd create EvaluatedFunction records with proper linkage
		print(f"Tracked value computation: {function} with {len(source_values)} source values = {computed_value}")
	
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
		
		





