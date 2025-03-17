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

import importlib
class Orchestration:
	# Class variable to track initialized objects
	_initialized_objects = set()
	
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
		
		





