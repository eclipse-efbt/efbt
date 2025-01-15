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

	
	
	def init(self,theObject):
		
		references = [method for method in dir(theObject.__class__) if not callable(
		getattr(theObject.__class__, method)) and not method.startswith('__')]
		for eReference in references:
			if eReference.endswith("Table"):

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
								print (" coud not call function called " + operation)

					setattr(theObject,eReference,newObject)

	def createObjectFromReferenceType(eReference):
		
		
		try:
			cls = getattr(importlib.import_module('pybirdai.process_steps.filter_code.output_tables'), eReference)
			new_object = cls()		
			return new_object;	
		except :
			print("Error: " + eReference)
		
		





