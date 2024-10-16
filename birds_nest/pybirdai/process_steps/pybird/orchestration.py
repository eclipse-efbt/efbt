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

	resourceURI = "";

	def setup(self, theObject):
		print("setup called")
		print("theObject : " + str(theObject))
		print("self " + str(self))
		
		operations = [method for method in dir(theObject.__class__) if callable(
			getattr(theObject.__class__, method)) and not method.startswith('__')]
		if operations:
			print("operations: " + str(operations))
		else:
			print("no operations")
		for operation in operations:
			print("operation1: " + operation)
			if operation == "init":
				print("operation2: " + operation)
				try:
					getattr(theObject, operation)()
				except:

					print("theObject1: " + str(theObject))
					print (" coud not call function " + operation)
                    
		CSVConverter().persist_object_as_csv(theObject,True);
		# contents = Orchestration.get_contained_objects(theObject);
		

		#for o in contents:
		# operations = [method for method in dir(theObject.__class__) if callable(
        # getattr(theObject.__class__, method)) and not method.startswith('__')]
			
		# for operation in operations:
				
		# if not (operation.__name__ == "init"):
		# 	result = getattr(theObject, operation.__name__)()
		# 	print(result);


	def get_contained_objects(theObject):
		'''
		Recursvely get all contianed/composed objects
		Q.) How do we recognise composed objects?
		A.) 1.) If it is a djangomodel it has no further contained objects
			2.) If it is not a django model 
		'''
		pass

	def init(self,theObject):
		print("init called")
		print("self: " + str(self))


		eclass = theObject.__class__;
		references = [method for method in dir(theObject.__class__) if not callable(
		getattr(theObject.__class__, method)) and not method.startswith('__')]
		for eReference in references:
			if eReference.endswith("Table"):

				from django.apps import apps
				print("eReference: " + eReference)
				table_name = eReference.split('_Table')[0]
				print("table_name: " + table_name)
				relevant_model = None
				try:
					relevant_model = apps.get_model('pybirdai',table_name)
				except LookupError:
					print("LookupError: " + table_name)

				if relevant_model:
					newObject = relevant_model.objects.all()
					if newObject:
						setattr(theObject,eReference,newObject)
						CSVConverter.persist_object_as_csv(newObject,True);						
					
				else:
					newObject = Orchestration.createObjectFromReferenceType(eReference);
					print("newObject: " + str(newObject))
					
					operations = [method for method in dir(newObject.__class__) if callable(
						getattr(newObject.__class__, method)) and not method.startswith('__')]
					
					for operation in operations:
						if operation == "init":
							try:
								getattr(newObject, operation)()
							except:
								print (" coud not call function called " + operation)
						
					#persistObject(newObject);

					setattr(theObject,eReference,newObject)
		#CSVConverter.persist_object_as_csv(theObject,True)

	

	def createObjectFromReferenceType(eReference):
		
		# find the class of the reference somehow, by naming or typ hints
		

		print("eReference2: " + eReference)
		
		try:
			cls = getattr(importlib.import_module('pybirdai.process_steps.filter_code.output_tables'), eReference)
			new_object = cls()		
			return new_object;	
		except :
			print("Error: " + eReference)
		
		





