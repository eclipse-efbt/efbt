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
import os
import threading

from pybirdai.context.sdd_context_django import SDDContext
from django.conf import settings
from django.db.models import QuerySet
from django.db.models.fields.related import ReverseOneToOneDescriptor

_DEBUG_LINEAGE = os.environ.get('PYBIRDAI_DEBUG_LINEAGE', '').lower() in {'1', 'true', 'yes', 'on'}

def _csv_debug(message):
	if _DEBUG_LINEAGE:
		print(message)

class CSVConverter:
	_django_references_cache = {}
	_operations_cache = {}
	_write_lock = threading.Lock()

	def persist_object_as_csv(theObject,useLongNames):
		if _DEBUG_LINEAGE:
			_csv_debug("persist_object_as_csv theObject: " + str(theObject))
		#if 'FNNCL_ASST_INSTRMNT_DRVD_DT' in str(theObject):
		#	import pdb;pdb.set_trace()
		fileName = ""
		base_dir = settings.BASE_DIR 
		output_directory = os.path.join(base_dir, 'results','lineage')
		table_name = CSVConverter.get_table_name(theObject)
		csvString = CSVConverter.createCSVStringForTable(theObject,useLongNames,table_name)
		try:
			with CSVConverter._write_lock:
				if (useLongNames):
					fileName = table_name + "_longnames.csv"
					with open(output_directory + os.sep + fileName, "w",  encoding='utf-8') as file:
						file.write(csvString)
				else:
					fileName = table_name + ".csv"
					with open(output_directory + os.sep + fileName, "w",  encoding='utf-8') as file:
						file.write(csvString)

		except Exception as e: 
			print("Exception  " + str(e)  )
			print("File " + fileName  + " already exists" )

		if _DEBUG_LINEAGE:
			_csv_debug("persist_object_as_csv succesfully written: " + str(theObject))

	def get_table_name(theObject):
		table_name = None
		if isinstance(theObject, QuerySet):
			table_name = theObject.model.__name__
		else:
			class_name = theObject.__class__.__name__
			table_name = class_name.split('_Table')[0]
		return table_name
	
	def createCSVStringForTable( theObject,  useLongNames, table_name):
		object_list = []
		headerCreated = False
		csv_lines = []
		django_model = False
		if isinstance(theObject, QuerySet):
			object_list = theObject
			django_model = True

			# Note: Removed broad Django model access tracking as it pollutes lineage with unused fields
			# Lineage is now tracked through improved @lineage decorators on actual calculations
			# CSVConverter._track_django_model_access(table_name, object_list)
		else:
			object_list = CSVConverter.get_contained_objects(theObject)

		for o in object_list:
			if not headerCreated:
				csv_lines.append(CSVConverter.createCSVHeaderStringForRow(o,django_model).rstrip('\n'))
				headerCreated = True
			csv_lines.append(CSVConverter.createCSVStringForRow(o, useLongNames,django_model).rstrip('\n'))



		return "\n".join(csv_lines) + "\n"

	def _get_django_references(model_class):
		references = CSVConverter._django_references_cache.get(model_class)
		if references is not None:
			return references

		references = [
			method for method in dir(model_class)
			if not callable(getattr(model_class, method)) and not method.startswith('__')
		]
		references = [
			relationship for relationship in references
			if not(relationship == "objects") and not(relationship == "_meta") and
				not(relationship.endswith("_domain")) and
				not isinstance(getattr(model_class, relationship), ReverseOneToOneDescriptor)
		]
		CSVConverter._django_references_cache[model_class] = references
		return references

	def _get_operations(model_class):
		operations = CSVConverter._operations_cache.get(model_class)
		if operations is not None:
			return operations

		operations = [
			method for method in dir(model_class)
			if callable(getattr(model_class, method)) and not method.startswith('__')
		]
		CSVConverter._operations_cache[model_class] = operations
		return operations
		
	def get_contained_objects(theObject):
		'''
		Get all contianed/composed objects
		Q.) How do we recognise composed objects?
		A.) 1.) If it is a djangomodel get the list of object/rows
			2.) Look at the instance members of the object, if it is a list, and does not have a name that ends in Table then get the list. 

		'''
		rows = []
		try:
			rows = theObject.objects.all()
			return rows
		except:
			instance_members = [member for member in dir(theObject.__class__) if not callable(
            getattr(theObject.__class__, member)) and not member.startswith('__')]
		
			for member in instance_members:
				if not (member.endswith("Table")) and isinstance(getattr(theObject.__class__, member), list):
					rows = getattr(theObject, member)
					return rows

		return rows

					

	def createCSVStringForRow(theObject,useLongNames,django_model):
		clazz = None
		eClass = theObject.__class__
		values = []
		if django_model:
			references = CSVConverter._get_django_references(theObject.__class__)
			for relationship in references:
				referencedItem = getattr(theObject,relationship)
				referencedItemString = str(referencedItem)
				if referencedItemString.endswith(".None"):
					referencedItemString = "None"
				values.append(str(referencedItemString))
		else:
			# For non-Django objects (like ANCRDT row objects), call methods to get values
			# Get all callable methods (same as header creation logic)
			operations = CSVConverter._get_operations(theObject.__class__)

			for eOperation in operations:
				try:
					# Call the method to get the value
					value = getattr(theObject, eOperation)()
					valueStr = str(value) if value is not None else ""
					values.append(valueStr)
				except Exception as e:
					# If method call fails, use empty string
					values.append("")

		return ",".join(values) + "\n"

	def createCSVHeaderStringForRow(theObject,django_model):
		clazz = None
		eClass = theObject.__class__
		
		if django_model:
			return ",".join(CSVConverter._get_django_references(theObject.__class__)) + "\n"
		else:
			return ",".join(CSVConverter._get_operations(theObject.__class__)) + "\n"

		return "\n"

	
	def getReferencedItemString(eStructuralFeature, referencedItem,useLongNames):
		returnString  = None
		 #temporary vaiable
		is_reference = True
		is_attribute = False
		is_enum = False
		is_date = False
		if ( referencedItem is None):
			returnString = "null"
		#else if (eStructuralFeature instanceof EReference)
		if is_reference:

			upperbound = 1
			if upperbound == 1:
				# somehow get the return type of the method
				eClass = referencedItem.__class__
                # somehow get the identifying attribute of the class
				# idattr = eClass.getEIDAttribute()
				idattr = None
				references = [method for method in dir(eStructuralFeature.__class__) if not callable(
            		getattr(eStructuralFeature.__class__, method)) and not method.startswith('__')]
            
				for eStructuralFeature2 in references:
				
					if eStructuralFeature2 == idattr:
						attributeValue = getattr(referencedItem.__class__, eStructuralFeature2)
						if (attributeValue):
							returnString = str(attributeValue)

					if (returnString is None):
						returnString = eStructuralFeature.__name__

			else:
				returnString = "multiple_"+ eStructuralFeature.getName()
		elif is_attribute and  is_enum:

			if (useLongNames):
				pass#returnString = "\"" + ((Enumerator) referencedItem).getName().replace('_',' ') + " (" + ((Enumerator) referencedItem).getLiteral() + ")\"" 
			else:
				pass# returnString = ((Enumerator) referencedItem).getLiteral()

		else:
			if is_date:
				pattern = "MM/dd/yyyy"
				#SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern)
				#returnString =  simpleDateFormat.format((java.util.Date) referencedItem)
				returnString = str(referencedItem)
			else:
				returnString = str(referencedItem)
		return returnString
	
	@staticmethod
	def _track_django_model_access(table_name, queryset):
		"""Track Django model access through orchestration for lineage"""
		try:
			# Import here to avoid circular imports
			from pybirdai.annotations.decorators import _lineage_context
			
			# Get the orchestration instance from the global context
			orchestration = _lineage_context.get('orchestration')
			if not orchestration or not orchestration.lineage_enabled:
				return
			
			# Create a mock table wrapper object for the Django model
			class DjangoModelTableWrapper:
				def __init__(self, model_name):
					self.__class__.__name__ = f"{model_name}_Table"
					self.model_name = model_name
					
				def __str__(self):
					return f"DjangoModelTableWrapper({self.model_name})"
			
			# Create the wrapper and track it
			wrapper = DjangoModelTableWrapper(table_name)
			
			# Check if orchestration supports lineage tracking
			if hasattr(orchestration, 'init_with_lineage'):
				# Initialize through orchestration to create PopulatedDataBaseTable
				orchestration.init_with_lineage(wrapper, f"Django Model Access: {table_name}")
				
				# Track the data if there are rows
				if queryset.exists():
					# Convert QuerySet to list of dictionaries for tracking
					data_items = []
					django_model_objects = []  # Keep track of original Django model objects
					for obj in queryset:
						row_data = {}
						# Get model fields to extract data
						for field in obj._meta.fields:
							try:
								value = getattr(obj, field.name)
								if value is not None:
									row_data[field.name] = value
							except:
								pass
						if row_data:
							data_items.append(row_data)
							django_model_objects.append(obj)  # Store the original Django object
					
					# Track the data processing - use the original table name, not "_data" suffix
					# This ensures Django model data is associated with PopulatedDataBaseTable, not EvaluatedDerivedTable
					if data_items:
						# Pass both the dictionaries (for CSV) and Django objects (for tracking)
						orchestration.track_data_processing(table_name, data_items, django_model_objects)
			else:
				# Original orchestrator - no lineage tracking
				pass
			
			if _DEBUG_LINEAGE:
				_csv_debug(f"Tracked Django model access: {table_name} ({queryset.count()} rows)")
			
		except Exception as e:
			print(f"Error tracking Django model access for {table_name}: {e}")
			# Don't let tracking errors break the actual processing
			pass
