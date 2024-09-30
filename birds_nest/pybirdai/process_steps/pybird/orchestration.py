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
class Orchestration:

	resourceURI = "";
	isSetUp = False;

	def setup(self, theObject):
	
		isSetUp = True;
		
		operations = [method for method in dir(theObject.__class__) if callable(
			getattr(theObject.__class__, method)) and not method.startswith('__')]

		for operation in operations:
			if operation.__name__ == "init":
				try:
					getattr(theObject, operation.__name__)()
				except:
					print (" coud not call function " + operation.__name__)
                    
		CSVConverter().persist_object_as_csv(theObject,True);
		contents = Orchestration.get_contained_objects(theObject);
		

		for o in contents:

			operations = [method for method in dir(theObject.__class__) if callable(
            getattr(theObject.__class__, method)) and not method.startswith('__')]
			
			for operation in operations:
				
				if not (operation.__name__ == "init"):
					result = getattr(theObject, operation.__name__)()
					print(result);


	def get_contained_objects(theObject):
		pass

	def init(self,theObject):
		if not self.isSetUp:
			Orchestration.setup(self,theObject);
		else:

			eclass = theObject.theObject.__class__;
			references = [method for method in dir(theObject.__class__) if not callable(
            getattr(theObject.__class__, method)) and not method.startswith('__')]
			for eReference in references:
				if eReference.__name__.endswith("Table"):

					from django.apps import apps
					table_name = eReference.__name__.split('_')[0]
					relevant_model = apps.get_model(table_name)
					newObject = relevant_model.objects.all()
					if newObject:
						setattr(theObject,eReference.__name__,newObject)
						CSVConverter.persistObjectAsCSV(newObject,True);						
					else:

						newObject = Orchestration.createObjectFromReferenceType( eReference);
						
						newObjectsClass = newObject.__class__
						operations = [method for method in dir(theObject.__class__) if callable(
							getattr(theObject.__class__, method)) and not method.startswith('__')]
						
						for operation in operations:
							if operation.__name__ == "init":
								try:
									getattr(theObject, operation.__name__)()
								except:
									print (" coud not call function " + operation.__name__)
							
						#persistObject(newObject);

						setattr(theObject,eReference.__name__,newObject)

	

	def createObjectFromReferenceType(eReference):
		
		# find the class of the reference somehow, by naming or typ hints
		theClass = eReference.__class__
		newObject = theClass();
		return newObject;
		





