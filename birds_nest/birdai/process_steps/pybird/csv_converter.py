
import os

class CSVConverter:

	def persist_object_as_csv(theObject,useLongNames,sdd_context):
		csvString = CSVConverter.createCSVStringForTable(theObject,useLongNames)
		fileName = ""
		try:
			if (useLongNames):
				fileName = theObject.__class__.__name__ + "_longnames.csv"
				file = open(sdd_context.output_directory + os.sep + fileName, "a",  encoding='utf-8') 
				file.write(csvString)
			else:
				fileName = theObject.eClass().getName() + ".csv"
				file = open(sdd_context.output_directory + os.sep + fileName, "a",  encoding='utf-8') 
				file.write(csvString)

		except: 
			print("File " + fileName  + " already exists" )

	def createCSVStringForTable( theObject,  useLongNames):
		clazz = None
		csvString = ""
		eClass = theObject.__class__
		sfs = [method for method in dir(theObject.__class__) if not callable(
            getattr(theObject.__class__, method)) and not method.startswith('__')]
		for eStructuralFeature in sfs:

			#boolean cont = ((EReference)eStructuralFeature).isContainment()
			cont = True    
			if (cont):
				headerCreated = False
				objectContents = CSVConverter.get_contained_objects(theObject)
				for o in objectContents:
					if not headerCreated:
						csvString = csvString + CSVConverter.createCSVHeaderStringForRow(o)
						headerCreated = True
						csvString = csvString + CSVConverter.createCSVStringForRow(o, useLongNames)

		return csvString + "\n"
		
	def get_contained_objects(theObject):
		pass

	def createCSVStringForRow(theObject,useLongNames):
		clazz = None
		csvString = ""
		eClass = theObject.eClass()
		sfs = eClass.getEAllStructuralFeatures()
		firstItem = True
		references = [method for method in dir(theObject.__class__) if not callable(
            getattr(theObject.__class__, method)) and not method.startswith('__')]
		for relationship in references:
			cardinality = 1
			if not (relationship is None):
				cardinality = 1
				#cardinality = ((EReference) eStructuralFeature).getUpperBound()

			#dont show any items in the inout data that have  cardinality	of -1
			if not(cardinality == -1):
				if firstItem:
					referencedItem = getattr(theObject,relationship.__name__)
					referencedItemString = CSVConverter.getReferencedItemString(relationship, referencedItem,useLongNames)
					csvString = csvString + referencedItemString
					firstItem = False

				else:
					#change next line
					referencedItem = getattr(theObject, relationship.__name__)
					referencedItemString = CSVConverter.getReferencedItemString(relationship, referencedItem,useLongNames)
					csvString = csvString + "," + referencedItemString

		operations = [method for method in dir(theObject.__class__) if callable(
							getattr(theObject.__class__, method)) and not method.startswith('__')]
		for eOperation in operations:
			if (firstItem):
				try:
					result = getattr(theObject, eOperation.__name__)()
					resultString = None
					if result:
						resultString = str(result)
						csvString = csvString + resultString
						firstItem = False
				except:
					print("Error getting operation result for " + eOperation.__name__)
			else:
				try:
					result = getattr(theObject, eOperation.__name__)()
					resultString = None
					if(result):
						resultString = str(result)
						csvString = csvString + "," + resultString
						firstItem = False
				except:
					print("Error getting operation result for " + eOperation.__name__)

		return csvString + "\n"

	def createCSVHeaderStringForRow(theObject):
		clazz = None
		csvString = ""
		eClass = theObject.__class__
		sfs = [method for method in dir(theObject.__class__) if callable(
							getattr(theObject.__class__, method)) and not method.startswith('__')]
		firstItem = True
		for  eStructuralFeature in  sfs:
			#boolean relationship = (eStructuralFeature instanceof EReference)
			relationship = True
			cardinality = 1
			if relationship:
				#cardinality = ((EReference) eStructuralFeature).getUpperBound()
				cardinality = 1

			#dont show any items in the inout data that have  cardinality	of -1
			if(cardinality != -1):
				if (firstItem):
					csvString = csvString + eStructuralFeature.__name__
					firstItem = False
				else:
					csvString = csvString + "," + eStructuralFeature.__name__

		operations = [method for method in dir(theObject.__class__) if callable(
			getattr(theObject.__class__, method)) and not method.startswith('__')]

		for eOperation in operations:
			if firstItem:
				csvString = csvString + eOperation.getName()
				firstItem = False

		else:
			csvString = csvString + "," + eOperation.getName()

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

