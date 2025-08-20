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

from pybirdai.context.sdd_context_django import SDDContext
from django.conf import settings
from django.apps import apps
from django.db.models import QuerySet
from django.db.models.fields.related import ReverseOneToOneDescriptor

class CSVConverter:

	def persist_object_as_csv(theObject,useLongNames):
		print("persist_object_as_csv theObject: " + str(theObject))
		#if 'FNNCL_ASST_INSTRMNT_DRVD_DT' in str(theObject):
		#	import pdb;pdb.set_trace()
		fileName = ""
		base_dir = settings.BASE_DIR 
		output_directory = os.path.join(base_dir, 'results','lineage')
		table_name = CSVConverter.get_table_name(theObject)
		csvString = CSVConverter.createCSVStringForTable(theObject,useLongNames,table_name)
		try:
			if (useLongNames):
				fileName = table_name + "_longnames.csv"
				file = open(output_directory + os.sep + fileName, "w",  encoding='utf-8') 
				file.write(csvString)
			else:
				fileName = table_name + ".csv"
				file = open(output_directory + os.sep + fileName, "w",  encoding='utf-8') 
				file.write(csvString)

		except Exception as e: 
			print("Exception  " + str(e)  )
			print("File " + fileName  + " already exists" )

		print("persist_object_as_csv succesfully written: " + str(theObject))

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
		csvString = ""
		django_model = False
		if isinstance(theObject, QuerySet):
			relevant_model = apps.get_model('pybirdai',table_name)
			print(f"relevant_model: {relevant_model}")
			object_list = relevant_model.objects.all()
			print(f"newObject: {object_list}")
			django_model = True
			
			# Note: Removed broad Django model access tracking as it pollutes lineage with unused fields
			# Lineage is now tracked through improved @lineage decorators on actual calculations
			# CSVConverter._track_django_model_access(table_name, object_list)
		else:
			object_list = CSVConverter.get_contained_objects(theObject)

		for o in object_list:
			if not headerCreated:
				csvString = csvString + CSVConverter.createCSVHeaderStringForRow(o,django_model)
				headerCreated = True
				csvString = csvString + CSVConverter.createCSVStringForRow(o, useLongNames,django_model)



		return csvString + "\n"
		
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
		csvString = ""
		eClass = theObject.__class__
		firstItem = True
		if django_model:
			references = [method for method in dir(theObject.__class__) if not callable(
				getattr(theObject.__class__, method)) and not method.startswith('__')]
			for relationship in references:
				if not(relationship == "objects") and not(relationship == "_meta") and\
					  not(relationship.endswith("_domain")) and\
						not isinstance(getattr(theObject.__class__,relationship),ReverseOneToOneDescriptor):
					cardinality = 1
					if not (relationship is None):
						cardinality = 1
						
					if not(cardinality == -1):
						if firstItem:
							referencedItem = getattr(theObject,relationship)
							referencedItemString = str(referencedItem)
							if referencedItemString.endswith(".None"):
								referencedItemString = "None"
							csvString = csvString + str(referencedItemString)
							firstItem = False

						else:
							#change next line
							referencedItem = getattr(theObject, relationship)
							#referencedItemString = CSVConverter.getReferencedItemString(relationship, referencedItem,useLongNames)
							referencedItemString = str(referencedItem)
							if referencedItemString.endswith(".None"):
								referencedItemString = "None"
							csvString = csvString + "," + str(referencedItemString)
		else:
			# Don't automatically call all methods - this causes unwanted function evaluations
			# Instead, just serialize the object representation
			if (firstItem):
				csvString = csvString + str(theObject)
				firstItem = False
			else:
				csvString = csvString + "," + str(theObject)

		return csvString + "\n"

	def createCSVHeaderStringForRow(theObject,django_model):
		clazz = None
		csvString = ""
		eClass = theObject.__class__
		
		if django_model:
			sfs = [method for method in dir(theObject.__class__) if not callable(
					getattr(theObject.__class__, method)) and not method.startswith('__')]
			firstItem = True
			for  eStructuralFeature in  sfs:
				if not(eStructuralFeature == "objects") and not(eStructuralFeature == "_meta") and\
					  not(eStructuralFeature.endswith("_domain")) and\
						not isinstance(getattr(theObject.__class__,eStructuralFeature),ReverseOneToOneDescriptor):
				#boolean relationship = (eStructuralFeature instanceof EReference)
					relationship = True
					cardinality = 1
					if relationship:
						#cardinality = ((EReference) eStructuralFeature).getUpperBound()
						cardinality = 1

					#dont show any items in the inout data that have  cardinality	of -1
					if(cardinality != -1):
						if (firstItem):
							csvString = csvString + eStructuralFeature
							firstItem = False
						else:
							csvString = csvString + "," + eStructuralFeature
		else:
			firstItem =True
			operations = [method for method in dir(theObject.__class__) if callable(
				getattr(theObject.__class__, method)) and not method.startswith('__')]

			for eOperation in operations:
				if firstItem:
					csvString = csvString + eOperation
					firstItem = False

				else:
					csvString = csvString + "," + eOperation

		return csvString + "\n"

	
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
			
			print(f"Tracked Django model access: {table_name} ({queryset.count()} rows)")
			
		except Exception as e:
			print(f"Error tracking Django model access for {table_name}: {e}")
			# Don't let tracking errors break the actual processing
			pass

