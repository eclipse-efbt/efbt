# coding=UTF-8#
# Copyright (c) 2020 Bird Software Solutions Ltd
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDX-License-Identifier: EPL-2.0
#
# Contributors:
#    Neil Mackenzie - initial API and implementation
#
from pickle import TRUE
'''
Created on 22 Jan 2022

@author: Neil
'''
from open_reg_specs import *
from pyecore.resources import ResourceSet, URI
from pyecore.ecore import *
from pyecore.resources.xmi import XMIResource
from pyecore.resources.xmi import XMIOptions

import csv
class SQLDeveloperImport(object):
    
    # we create the main 4 'primitive' data types
    xString = XDataType()
    xString.name = "String"
    
    xDouble = XDataType()
    xDouble.name = "Double"
    
    xInt = XDataType()
    xInt.name = "Integer"
    
    xDate = XDataType()
    xDate.name = "Date"

    # This is the root packae where we add each type, class and enum
    rpmnPackage = XPackage(name='bird')
    
    # classesMap keeps a reference between ldm ID's for classes and
    # the class instance
    classesMap = {}
    # classesMap keeps a reference between ldm ID's for classes and
    # the list of these calsses that we call tables
    tableMap = {}
    # A map between the LDM names for primitive types types, and
    # our standard primitive types such as XString  
    datatypeMap = {}
    # For the reference output layers we record a map between variables 
    # and domains
    variableToDomainMap = {}
    # For the reference output layers we record a map between domains 
    # and domain names
    domainToDomainNameMap = {}
    # For the reference output layers we record a map between members ids  
    # andtheir containing domains
    memberIDToDomainMap = {}
    # For the reference output layers we record a map between members ids  
    # and their names
    memberIDToMemberNameMap = {}
    # For the reference output layers we record a map between members ids  
    # and their codes
    memberIDToMemberCodeMap = {}
    # we record a list of missing domains
    missingDomains = []
    # enumMap keeps a reference between ldm ID's for domains and
    # the enum instance
    enumMap = {}
    # the directory where we get our input files
    fileDirectory = ""
    # the directory where we save our outputs.
    outputDirectory = ""
         
    def convert (self,theFileDirectory,theOutputDirectory, ROLAvailable):
        '''
        from a set of CSV files exported from SQL developer, we create a RPMN file
        representing the LDM
        
        If ROLAvailable is true we also ensure that the data about reference
        output layers is also added to the RPMN file
        '''
        self.fileDirectory = theFileDirectory
        self.outputDirectory = theOutputDirectory
        # add the data types to the package
        self.rpmnPackage.classifiers.append(self.xString)
        self.rpmnPackage.classifiers.append(self.xDouble)
        self.rpmnPackage.classifiers.append(self.xInt)
        
        SQLDeveloperImport.importLDM(self)
        if (ROLAvailable):
            SQLDeveloperImport.importReferenceOutputLayers(self) 
        
        SQLDeveloperImport.saveModelAsXMIFile(self)  
        SQLDeveloperImport.saveModelAsRPMNFile(self ) 
    
    def importLDM(self):
        '''
        import the items from the BIRD LDM csv files
        '''
        SQLDeveloperImport.addLDMClassesToPackage(self)
        SQLDeveloperImport.setLDMSuperClasses(self)
        SQLDeveloperImport.addLDMEnumsToPackage(self)
        SQLDeveloperImport.addLDMLiteralsToEnums(self) 
        SQLDeveloperImport.createLDMTypesMap(self) 
        SQLDeveloperImport.addLDMAttributesToClasses(self) 
        SQLDeveloperImport.removeLDMAttributesAlreadyInSuperClass(self) 
        SQLDeveloperImport.addLDMRelationshipsBetweenClasses(self)
        
    def importReferenceOutputLayers(self): 
        '''
        import the items from the Output layer csv files
        '''
        SQLDeveloperImport.addROLClassesToPackage(self) 
        SQLDeveloperImport.createVariableToDomainMap(self) 
        SQLDeveloperImport.createDomainToDomainNameMap(self)
        SQLDeveloperImport.createMemberMaps(self)
        SQLDeveloperImport.addROLEnumsAndLiteralsToPackage(self)
        SQLDeveloperImport.addROLAttributesToClasses(self)
        
    def addLDMClassesToPackage(self):
        '''
        for each entity in the LDM, create a class and add it to the package
        '''
        
        fileLocation = self.fileDirectory + "\\DM_Entities.csv"
        
        
        headerSkipped = False
        # Load all the entities from the csv file, make an XClass per entity,
        # and add the XClass to the package
        with open(fileLocation) as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                # skip the first line which is the header.
                if (not headerSkipped):
                    headerSkipped = True
                else:

                    className = row[0];
                    objectID = row[1];
                    engineering_type = row[27];
                    Num_SuperTypeEntity_ID = row[26];
                    
                    alteredClassName = SQLDeveloperImport.makeValidID(self,className);  
                    if(alteredClassName.endswith("_derived")):
                        xclass = XClass(name=alteredClassName)
                        xclassTable = XClass(name=alteredClassName+"_DerivedTable")
                        xclassTable.containedEntityType = xclass
                        containmentReference  = XReference()
                        containmentReference.name=xclass.name+"s"
                        containmentReference.type=xclass
                        containmentReference.upperBound = -1
                        containmentReference.lowerBound=0
                        containmentReference.containment= True
                        xclassTable.members.append(containmentReference)
                        xclassTableOperation = XOperation()
                        xclassTableOperation.name=xclass.name+"s"
                        xclassTableOperation.type=xclass
                        xclassTableOperation.upperBound = -1
                        xclassTableOperation.lowerBound=0
                        xclassTable.members.append(xclassTableOperation)
                        self.rpmnPackage.classifiers.extend([xclass])
                        self.rpmnPackage.classifiers.extend([xclassTable])
                    elif(className.startswith("OUTPUT_LAYER_")):
                        xclass = XClass(name=alteredClassName)
                        
                        self.rpmnPackage.classifiers.extend([xclass])
                      
                    else:
                        xclass = XClass(name=alteredClassName)
                        # of engineering type is single table, as i should be for all members of a type
                        # heirarchy, and num_suptype is blanck, then this means that this class is a root
                        # of a type heirarchy....we will set such classes to be abstract.
                        if((engineering_type == "Single Table") and (Num_SuperTypeEntity_ID=="")   )    :
                            xclass.abstract=True
                        xclassTable = XClass(name=alteredClassName+"_BaseTable")
                        containmentReference  = XReference()
                        containmentReference.name=xclass.name+"s"
                        containmentReference.type=xclass
                        containmentReference.upperBound = -1
                        containmentReference.lowerBound=0
                        containmentReference.containment= True
                        xclassTable.members.append(containmentReference)
                        self.rpmnPackage.classifiers.extend([xclass])
                        self.rpmnPackage.classifiers.extend([xclassTable])
        
                    # maintain a map a objectIDs to XClasses
                    self.classesMap[objectID]=xclass
                    self.tableMap[xclass]=xclassTable
         
    def setLDMSuperClasses(self):           
        fileLocation = self.fileDirectory + "\\DM_Entities.csv"
        headerSkipped = False
        
        # Where an nxtity has a superclass, set the superclass on the XClass
        with open(fileLocation) as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                # skip the first line which is the header.
                if (not headerSkipped):
                    headerSkipped = True
                else:
                    classID = row[1]
                    superclassID =row[25]
                    if ( not (len(superclassID.strip()) == 0)):
                        theclass = self.classesMap[classID]
                        superclass = self.classesMap[superclassID]
                        theclass.superTypes.extend([superclass])
                        
                       
    def addLDMEnumsToPackage(self):
        '''
        for each domain in the LDM add an enum to the package
        '''
        fileLocation = self.fileDirectory + "\\DM_Domains.csv"
        headerSkipped = False
        counter = 0
        # Create an XEnum for each domain, and add it to the XPackage
        with open(fileLocation) as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if (not headerSkipped):
                        headerSkipped = True
                else:
                    counter=counter+1
                    enumID = row[0]
                    enumName = row[1]
                    adaptedEnumName = SQLDeveloperImport.makeValidID(self,enumName)+"_domain"
                    if(not SQLDeveloperImport.inEnumBlackList(self,adaptedEnumName)):
                        theEnum = XEnum()
                        theEnum.name = adaptedEnumName
                        #maintain a map of enum IDS to XEnum objects
                        self.enumMap[enumID] = theEnum
                        self.rpmnPackage.classifiers.extend([theEnum])
                        
    def addLDMLiteralsToEnums(self):
        '''
        for each memebr of a domain the LDM, add a literal to the corresponding enum
        '''
        fileLocation = self.fileDirectory + "\\DM_Domain_AVT.csv"
        headerSkipped = False
        counter = 0
        # Add the members of a domain as literals of the related Enum
        with open(fileLocation) as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if (not headerSkipped):
                        headerSkipped = True
                else:
                    try:
                        counter=counter+1
                        enumID = row[0]
                        enumUsedName = "m_" + SQLDeveloperImport.makeValidID(self,row[3])
                        enumName = row[5]
                        adaptedEnumName = SQLDeveloperImport.makeValidID(self,enumName)
                        value = row[4]
                        adaptedValue = SQLDeveloperImport.makeValidID(self,value)
                        try:
                            theEnum = self.enumMap[enumID]
                            newAdaptedValue = SQLDeveloperImport.uniqueValue(self,theEnum,adaptedValue)
                            newAdaptedName = SQLDeveloperImport.uniqueName(self,theEnum,adaptedEnumName)
                            enumLiteral = XEnumLiteral()
                            enumLiteral.name = newAdaptedName
                            enumLiteral.literal = newAdaptedValue
                            enumLiteral.value = counter
                            theEnum.literals.extend([enumLiteral])
                                
                        except KeyError:
                            print( "missing domain: " + enumID )
                            
                    except IndexError:
                        print( "row in DM_Domain_AVT.csv skipped  due to improper formatting at row number")
                        print(counter)

    def createLDMTypesMap(self):                
        # for each logicalDatatype for orcle 12c, make a Datatype if we have an
        # equivalent
        
        fileLocation = self.fileDirectory + "\\DM_Logical_To_Native.csv"
        headerSkipped = False
        with open(fileLocation) as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if (not headerSkipped):
                        headerSkipped = True
                else:
                    rdbms_Type = row[3]
                    rdbms_Version = row[4]
                    dataTypeID = row[0]
                    if ((rdbms_Type.strip() == "Oracle Database") and (rdbms_Version.strip() =="12cR2")):
                        native_type = row[2]

                        if (native_type.strip() == "VARCHAR") :

                            self.datatypeMap[dataTypeID] = self.xString
                      
                        if (native_type.strip() == "VARCHAR2") :

                            self.datatypeMap[dataTypeID] = self.xString
                      
                        if (native_type.strip() == "INTEGER") :

                            self.datatypeMap[dataTypeID] = self.xInt
                      
                        if (native_type.strip() == "DATE") :

                            self.datatypeMap[dataTypeID] = self.xDate
                        
                        if (native_type.strip() == "NUMBER") :

                            self.datatypeMap[dataTypeID] = self.xDouble
                        
                        if (native_type.strip() == "UNKNOWN") :

                            self.datatypeMap[dataTypeID] = self.xString
             
    def addLDMAttributesToClasses(self):
        '''
        For each attribute on an entity of the LDM, add an attribute
        to the relevant class in the package
        '''
            
        fileLocation = self.fileDirectory + "\\DM_Attributes.csv"
        headerSkipped = False
        # For each attribute add an XAttribute to the correct XClass representing the Entity
        # the attribute should have the correct type, which may be a specific
        # enumeration

        with open(fileLocation) as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if (not headerSkipped):
                        headerSkipped = True
                else:
                    attributeName = row[0]
                    amendedAttributeName = SQLDeveloperImport.makeValidID(self,attributeName);
                    attributeKind = row[7]
                   
                    classID = row[4]
                    relationID = row[32]
                    primary_key_or_not = row[35]
                    theClass = self.classesMap[classID]
                    
                    classIsDerived = False
                    if (theClass.name.endswith("_derived")):
                        classIsDerived = True
                        
                    theAttributeName =  amendedAttributeName
                    
                    # we only add attributes here if they are not representing a relationship
                    if relationID == "":
                        
                        if (attributeKind == "Domain"):
                            enumID = row[12]
                            theEnum = self.enumMap[enumID]
                            
                            attribute = XAttribute()
                            if(primary_key_or_not == "P"):
                                attribute.iD = True
                                
                            attribute.lowerBound=0
                            attribute.upperBound=1
                            if(theEnum.name == "String"):
                                attribute.name = theAttributeName
                                attribute.type = self.xString
                            elif(theEnum.name.startswith("String_")):
                                attribute.name = theAttributeName
                                attribute.type = self.xString
                            elif(theEnum.name == "Number"):
                                attribute.name = theAttributeName
                                attribute.type = self.xDouble
                            elif(theEnum.name.startswith("Real_")):
                                attribute.name = theAttributeName
                                attribute.type = self.xDouble
                            elif(theEnum.name.startswith("Monetary")):
                                attribute.name = theAttributeName
                                attribute.type = self.xInt
                            elif(theEnum.name.startswith("Non_negative_monetary_amounts_with_2_decimals")): 
                                attribute.name = theAttributeName
                                attribute.type = self.xInt
                            elif(theEnum.name.startswith("Non_negative_integers")): 
                                attribute.name = theAttributeName
                                attribute.type = self.xInt
                            elif(theEnum.name.startswith("All_possible_dates")):
                                attribute.name = theAttributeName
                                attribute.type = self.xDate
                                
                            # This is a common domain used for String identifiers in BIRD in SQLDeveloper
                            
                            else:
                                attribute.name = theAttributeName
                                attribute.type = theEnum  
                            
                            if classIsDerived:
                                operation = XOperation()
                                operation.lowerBound=0
                                operation.upperBound=1
                                if(theEnum.name == "String"):
                                    operation.name = theAttributeName
                                    operation.type = self.xString
                                elif(theEnum.name.startswith("String_")):
                                    operation.name = theAttributeName
                                    operation.type = self.xString
                                elif(theEnum.name == "Number"):
                                    operation.name = theAttributeName
                                    operation.type = self.xDouble
                                
                                elif(theEnum.name.startswith("Real_")):
                                    operation.name = theAttributeName
                                    operation.type = self.xDouble
                                elif(theEnum.name.startswith("Monetary")): 
                                    operation.name = theAttributeName
                                    operation.type = self.xInt
                                elif(theEnum.name.startswith("Non_negative_monetary_amounts_with_2_decimals")): 
                                    operation.name = theAttributeName
                                    operation.type = self.xInt
                                elif(theEnum.name.startswith("Non_negative_integers")): 
                                    operation.name = theAttributeName
                                    operation.type = self.xInt
                                elif(theEnum.name.startswith("All_possible_dates")):   
                                    operation.name = theAttributeName
                                    operation.type = self.xDate  
                                else:
                                    operation.name = theAttributeName
                                    operation.type = theEnum  
                                          
    
                        if (attributeKind == "Logical Type"):
                            print("Logical Type")
                            dataTypeID = row[14]
                            try:
                                datatype = self.datatypeMap[dataTypeID]
                                attribute = XAttribute()
                                attribute.lowerBound=0
                                attribute.upperBound=1
                                attribute.name =amendedAttributeName
                                attribute.type = SQLDeveloperImport.getEcoreDataTypeForDataType(self)
                                
                                if classIsDerived:
                                    operation = XOperation()
                                    operation.lowerBound=0
                                    operation.upperBound=1
                                    operation.name =amendedAttributeName
                                    operation.type = SQLDeveloperImport.getEcoreDataTypeForDataType(self)
                                
                            except KeyError:
                                print("missing datatype: ")
                                print(dataTypeID)                       
    
                        
    
                        try:
    
                            theClass = self.classesMap[classID]
                            theClass.members.extend([attribute])
                            if classIsDerived:
                                theClass.members.extend([operation])
    
                        except:
                            print( "missing class2: " )
                            print(classID)
                        
    def removeLDMAttributesAlreadyInSuperClass(self):
        '''
        if we already have created an attribute in both a subclass and a superclass
        then we delete it in the subclass
        '''    
        for theClass in self.classesMap.values():
            if len(theClass.superTypes) > 0:
                superclass = theClass.superTypes[0]
                if (superclass):
                    
                    attributes = theClass.members
                    attributesToDelete = []
                    for theAttribute in attributes :
                        if SQLDeveloperImport.superclassContainsFeature(self,superclass, theAttribute):
                            attributesToDelete.append(theAttribute);
    
                    for theAttribute in attributesToDelete :
                        theClass.members.remove(theAttribute);
                        print( "removed attribute or reference since it exists in the superclass")
                        print(  theAttribute.name )
               
    def addLDMRelationshipsBetweenClasses(self):
        '''
        For each relationship in the LDM, add a reference between the relevant classes
        '''    
        fileLocation = self.fileDirectory + "\\DM_Relations.csv"
        headerSkipped = False
        with open(fileLocation) as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if (not headerSkipped):
                        headerSkipped = True
                else:
                    sourceID = row[16]
                    targetID = row[18]
                    sourceTo_Target_Cardinality = row[10]
                    targetTo_Source_Cardinality = row[11]
                    targetClassName = row[7]
                    source_Optional = row[12]
                    target_Optional = row[13]
                    attributeKind = row[7].strip();
                    referenceID = row[4].strip();
                    
                    referenceName = "the" + SQLDeveloperImport.makeValidID(self,targetClassName);

                    try:
                        theClass = self.classesMap[sourceID]
                    except KeyError:
                        print("missing class1: " + sourceID)
                    
                    try:   
                        targetClass = self.classesMap[targetID] 
                    except KeyError:
                        print("missing target class: " + targetID) 
                                          
                    numOfRelations = SQLDeveloperImport.numberofRelationShipsToThisClass(self,theClass,targetClass)
                    if(numOfRelations>0):
                        print("numOfRelations")
                        print(numOfRelations)
                        referenceName = referenceName + str(numOfRelations)
                    relationalAttribute = None
                    if (target_Optional.strip() == "Y"):
                        if (sourceTo_Target_Cardinality.strip() == "*"):
                            referenceName = referenceName + "s"
                            eReference  = XReference()
                            eReference.name=referenceName
                            eReference.type=targetClass
                            #upper bound of -1 means there is no upper bounds, so represents an open list of reference
                            eReference.upperBound = -1
                            eReference.lowerBound=0
                            eReference.containment= False
                            if (theClass.name.endswith("_derived")):
                                theSourceTable = self.tableMap[theClass]
                                theTargetTable = self.tableMap[targetClass]
                                if not(SQLDeveloperImport.hasMemberCalled(self, theSourceTable, "sourceTable1")):
                                   
                                    sourceTablesReference  = XReference()
                                    sourceTablesReference.name="sourceTable1"
                                    sourceTablesReference.type=theTargetTable
                                    sourceTablesReference.upperBound = -1
                                    sourceTablesReference.lowerBound=0
                                    sourceTablesReference.containment= False
                                    theSourceTable.members.append(sourceTablesReference)
                                else:
                                   
                                    sourceTablesReference  = XReference()
                                    sourceTablesReference.name="sourceTable2"
                                    sourceTablesReference.type=theTargetTable
                                    sourceTablesReference.upperBound = -1
                                    sourceTablesReference.lowerBound=0
                                    sourceTablesReference.containment= False
                                    theSourceTable.members.append(sourceTablesReference)
                        else:
                            eReference  = XReference()
                            eReference.name=referenceName
                            eReference.type=targetClass
                            eReference.upperBound = 1
                            eReference.lowerBound=0
                            eReference.containment= False
                            if (theClass.name.endswith("_derived")):
                                theSourceTable = self.tableMap[theClass]
                                theTargetTable = self.tableMap[targetClass]
                                if not(SQLDeveloperImport.hasMemberCalled(self, theSourceTable, "sourceTable1")):
                                    
                                    sourceTablesReference  = XReference()
                                    sourceTablesReference.name="sourceTable1"
                                    sourceTablesReference.type=theTargetTable
                                    sourceTablesReference.upperBound = -1
                                    sourceTablesReference.lowerBound=0
                                    sourceTablesReference.containment= False
                                    theSourceTable.members.append(sourceTablesReference)
                                else:
                                    
                                    sourceTablesReference  = XReference()
                                    sourceTablesReference.name="sourceTable2"
                                    sourceTablesReference.type=theTargetTable
                                    sourceTablesReference.upperBound = -1
                                    sourceTablesReference.lowerBound=0
                                    sourceTablesReference.containment= False
                                    theSourceTable.members.append(sourceTablesReference)                 
                    else:
                        if (sourceTo_Target_Cardinality.strip() == "*"):
                            referenceName = referenceName + "s"                       
                            eReference  = XReference()
                            eReference.name=referenceName
                            eReference.type=targetClass
                            eReference.upperBound = -1
                            eReference.lowerBound=1
                            eReference.containment= False
                            if (theClass.name.endswith("_derived")):

                                theSourceTable = self.tableMap[theClass]
                                theTargetTable = self.tableMap[targetClass]
                                if not(SQLDeveloperImport.hasMemberCalled(self, theSourceTable, "sourceTable1")):
                                    
                                    sourceTablesReference  = XReference()
                                    sourceTablesReference.name="sourceTable1"
                                    sourceTablesReference.type=theTargetTable
                                    sourceTablesReference.upperBound = -1
                                    sourceTablesReference.lowerBound=0
                                    sourceTablesReference.containment= False
                                    theSourceTable.members.append(sourceTablesReference)
                                else:
                                    sourceTablesReference = XReference("sourceTable2", theTargetTable, upper=-1, lower=0, containment=False)
                                    theSourceTable.members.append(sourceTablesReference)                       
                        else:      
                            eReference  = XReference()
                            eReference.name=referenceName
                            eReference.type=targetClass
                            eReference.upperBound = 1
                            eReference.lowerBound=1
                            eReference.containment= False
                            if (theClass.name.endswith("_derived")):
                                theSourceTable = self.tableMap[theClass]
                                theTargetTable = self.tableMap[targetClass]
                                if not(SQLDeveloperImport.hasMemberCalled(self, theSourceTable, "sourceTable1")):
                                    
                                    sourceTablesReference  = XReference()
                                    sourceTablesReference.name="sourceTable1"
                                    sourceTablesReference.type=theTargetTable
                                    sourceTablesReference.upperBound = -1
                                    sourceTablesReference.lowerBound=0
                                    sourceTablesReference.containment= False
                                    theSourceTable.members.append(sourceTablesReference)
                                else:
                                    
                                    sourceTablesReference  = XReference()
                                    sourceTablesReference.name="sourceTable2"
                                    sourceTablesReference.type=theTargetTable
                                    sourceTablesReference.upperBound = -1
                                    sourceTablesReference.lowerBound=0
                                    sourceTablesReference.containment= False
                                    theSourceTable.members.append(sourceTablesReference)
                    if (not (theClass is None) ) :                 
                        theClass.members.append(eReference)
                        
    

        
    def addROLClassesToPackage(self):        
        fileLocation = self.fileDirectory + "\\cube.csv"
        headerSkipped = False
        # Load all the entities from the csv file, make an XClass per entity,
        # and add the XClass to the package
        with open(fileLocation,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                # skip the first line which is the header.
                if (not headerSkipped):
                    headerSkipped = True
                else:

                    className = row[5]
                    objectID  = className
                    cube_type = row[6]
                    valid_to  = row[9]
                    version   = row[10]
                    
                    if ( (version ==  "FINREP 3.0") and (cube_type == "RC") and (valid_to == "31.12.9999") ) :

                        alteredClassName = SQLDeveloperImport.makeValidID(self,className);  
                        
                        xclass = XClass(name=alteredClassName+"_OutputItem")
                        xclassTable = XClass(name=alteredClassName+"_OutputTable")
                        xclassTable.containedEntityType = xclass
                        containmentReference  = XReference()
                        containmentReference.name=xclass.name+"s"
                        containmentReference.type=xclass
                        containmentReference.upperBound = -1
                        containmentReference.lowerBound=0
                        containmentReference.containment= True
                        xclassTable.members.append(containmentReference)
                        xclassTableOperation = XOperation()
                        xclassTableOperation.name=xclass.name+"s"
                        xclassTableOperation.type=xclass
                        xclassTableOperation.upperBound = -1
                        xclassTableOperation.lowerBound=0
                        xclassTable.members.append(xclassTableOperation)
                        self.rpmnPackage.classifiers.extend([xclass])
                        self.rpmnPackage.classifiers.extend([xclassTable])
                        
            
                        # maintain a map a objectIDs to XClasses
                        self.classesMap[objectID]=xclass
                        self.tableMap[xclass]=xclassTable
                        
    def createVariableToDomainMap(self):       
        # Make a variable to Domain Map
        fileLocation = self.fileDirectory + "\\variable.csv"
        headerSkipped = False
        # or each attribute add an Xattribute to the correct XClass represtnting the Entity
        # the attribute should have the correct type, which may be a specific
        # enumeration
        
        with open(fileLocation,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if (not headerSkipped):
                        headerSkipped = True
                else:
                    variableName = row[1]
                    #domainName = SQLDeveloperImport.makeValidID(self,row[3])
                    domain = row[4]
                    self.variableToDomainMap[variableName] = domain
             
    def createDomainToDomainNameMap(self):         
        '''
         Make a domain  to Domain Name Map
        ''' 
        fileLocation = self.fileDirectory + "\\domain.csv"
        headerSkipped = False

        domainToDomainNameMap = {}
        with open(fileLocation,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if (not headerSkipped):
                        headerSkipped = True
                else:
                    domainID = row[1]
                    #domainName = SQLDeveloperImport.makeValidID(self,row[3])
                    domainName = row[2]
                    domainToDomainNameMap[domainID] = domainName
                   
    def createMemberMaps(self):   
         # Make a domain  to Domain Name Map 
        fileLocation = self.fileDirectory + "\\member.csv"
        headerSkipped = False

        
        with open(fileLocation,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if (not headerSkipped):
                        headerSkipped = True
                else:
                    memberID = row[1]
                    #domainName = SQLDeveloperImport.makeValidID(self,row[3])
                    memberCode= row[2]
                    memberName = row[3]
                    domainId =  row[4]
                    self.memberIDToDomainMap[memberID] = domainId
                    self.memberIDToMemberNameMap[memberID] = memberName
                    self.memberIDToMemberCodeMap[memberID] = memberCode
                    
                       
                       
    def addROLEnumsAndLiteralsToPackage(self):                   
        fileLocation = self.fileDirectory + "\\cube_structure_item.csv"
        headerSkipped = False
        # or each attribute add an Xattribute to the correct XClass represtnting the Entity
        # the attribute should have the correct type, which may be a specific
        # enumeration

        with open(fileLocation,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if (not headerSkipped):
                        headerSkipped = True
                else:
                    attributeName = row[1]
                    amendedAttributeName = SQLDeveloperImport.makeValidID(self,attributeName)
                    variable = row[2]
                   
                    classID = row[0]
                    try: 
                        theClass = self.classesMap[classID]
                        
                        classIsDerived = True
                            
                        theAttributeName =  amendedAttributeName
       
                        domainID = self.variableToDomainMap[variable]
                        domain_ID_Name = self.domainToDomainNameMap[domainID]
                        amendedDomainName = SQLDeveloperImport.makeValidID(self,domain_ID_Name)
                        theEnum =  SQLDeveloperImport.findROLEnum(self,amendedDomainName+"_domain",self.enumMap)
                        if  theEnum is not None:                     
                            print( "not missing domainID: " )
                        else:
                            print( "missing domainID: " )
                            print(domainID)
                            print(classID)
                            if not(domainID in self.missingDomains):
                                self.missingDomains.append(domainID)
                    except:
                            print( "missing ROL class: " )
                            print(classID)    
                            
        for theDomain in self.missingDomains:
            
            domain_ID_Name = self.domainToDomainNameMap[theDomain]
            amendedDomainName = SQLDeveloperImport.makeValidID(self,domain_ID_Name)+ "_domain"
            if not( (amendedDomainName == "String") or (amendedDomainName == "Date")  ):
                theEnum = XEnum()
                theEnum.name = amendedDomainName 
                #maintain a map of enum IDS to XEnum objects
                self.enumMap[amendedDomainName] = theEnum
                self.rpmnPackage.classifiers.extend([theEnum])
                theDomainMembers= SQLDeveloperImport.getMembersOfTheDomain(self,theDomain, self.memberIDToDomainMap)
                counter1 = 0
                for member in theDomainMembers:
                    enumLiteral = XEnumLiteral()
                    enumUsedName = "m_" + SQLDeveloperImport.makeValidID(self,self.memberIDToMemberCodeMap[member])
                    adaptedValue = SQLDeveloperImport.makeValidID(self,self.memberIDToMemberNameMap[member])
                    newAdaptedValue = SQLDeveloperImport.uniqueValue(self, theEnum, adaptedValue)
                    newAdaptedName = SQLDeveloperImport.uniqueName(self, theEnum, enumUsedName)
                    
                    
                    
                    enumLiteral.name = newAdaptedName
                    enumLiteral.literal = newAdaptedValue
                    counter1 = counter1 + 1
                    enumLiteral.value = counter1
                    theEnum.literals.extend([enumLiteral])
            
    def addROLAttributesToClasses(self):
        '''
        For each attribute add an Xattribute to the correct XClass represtnting the Entity
        the attribute should have the correct type, which may be a specific
        enumeration
        '''
        fileLocation = self.fileDirectory + "\\cube_structure_item.csv"
        headerSkipped = False
        

        with open(fileLocation,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if (not headerSkipped):
                        headerSkipped = True
                else:
                    attributeName = row[1]
                    amendedAttributeName = SQLDeveloperImport.makeValidID(self,attributeName)
                    variable = row[2]
                   
                    classID = row[0]
                    try: 
                        theClass = self.classesMap[classID]
                        
                        classIsDerived = True
                            
                        theAttributeName =  amendedAttributeName
       
                        domainID = self.variableToDomainMap[variable]
                        domain_ID_Name = self.domainToDomainNameMap[domainID]
                        amendedDomainName = SQLDeveloperImport.makeValidID(self,domain_ID_Name)
                        theEnum =  SQLDeveloperImport.findROLEnum(self,amendedDomainName+"_domain",self.enumMap)
                        if  theEnum is not None:                     
                            attribute = XAttribute()

                            attribute.lowerBound=0
                            attribute.upperBound=1
                            if(theEnum.name == "String"):
                                attribute.name = theAttributeName
                                attribute.type = self.xString
                            elif(theEnum.name.startswith("String_")):
                                attribute.name = theAttributeName
                                attribute.type = self.xString
                            elif(theEnum.name == "Number"):
                                attribute.name = theAttributeName
                                attribute.type = self.xDouble
                            elif(theEnum.name.startswith("Real_")):
                                attribute.name = theAttributeName
                                attribute.type = self.xDouble
                            elif(theEnum.name.startswith("Monetary")):
                                attribute.name = theAttributeName
                                attribute.type = self.xInt
                            elif(theEnum.name.startswith("Non_negative_monetary_amounts_with_2_decimals")): 
                                attribute.name = theAttributeName
                                attribute.type = self.xInt
                            elif(theEnum.name.startswith("Non_negative_integers")): 
                                attribute.name = theAttributeName
                                attribute.type = self.xInt
                            elif(theEnum.name.startswith("All_possible_dates")):
                                attribute.name = theAttributeName
                                attribute.type = self.xDate
                                
                            # This is a common domain used for String identifiers in BIRD in SQLDeveloper
                            
                            else:
                                attribute.name = theAttributeName
                                attribute.type = theEnum  
                            
                            if classIsDerived:
                                operation = XOperation()
                                operation.lowerBound=0
                                operation.upperBound=1
                                if(theEnum.name == "String"):
                                    operation.name = theAttributeName
                                    operation.type = self.xString
                                elif(theEnum.name.startswith("String_")):
                                    operation.name = theAttributeName
                                    operation.type = self.xString
                                elif(theEnum.name == "Number"):
                                    operation.name = theAttributeName
                                    operation.type = self.xDouble
                                
                                elif(theEnum.name.startswith("Real_")):
                                    operation.name = theAttributeName
                                    operation.type = self.xDouble
                                elif(theEnum.name.startswith("Monetary")): 
                                    operation.name = theAttributeName
                                    operation.type = self.xInt
                                elif(theEnum.name.startswith("Non_negative_monetary_amounts_with_2_decimals")): 
                                    operation.name = theAttributeName
                                    operation.type = self.xInt
                                elif(theEnum.name.startswith("Non_negative_integers")): 
                                    operation.name = theAttributeName
                                    operation.type = self.xInt
                                elif(theEnum.name.startswith("All_possible_dates")):   
                                    operation.name = theAttributeName
                                    operation.type = self.xDate  
                                else:
                                    operation.name = theAttributeName
                                    operation.type = theEnum  
        
                            try:
            
                                theClass = self.classesMap[classID]
                                theClass.members.extend([attribute])
                                if classIsDerived:
                                    theClass.members.extend([operation])
            
                            except:
                                print( "missing class2: " )
                        else:
                            print( "missing domainID: " )
                            print(domainID)
                            print(classID)
                            if not(domainID in self.missingDomains):
                                self.missingDomains.append(domainID)
                    except:
                            print( "missing ROL class: " )
                            print(classID)                                
                            
        

     
    def uniqueValue (self, theEnum, adaptedValue):
        '''
            if the adapted value already exists in the enum then append it with _x2
            if the that string appended with _x2 already exists, then append with_x3 instead
            if that exists then _x4 etc.
        '''
        newAdaptedValue = adaptedValue
        if SQLDeveloperImport.containsLiteral(self,theEnum.literals, adaptedValue ):
            newAdaptedValue = adaptedValue +"_x2"
        counter = 1
        finished = False
        # within the bird data model there is re-use of the same id or name
        # for multiple members, which is not ideal. For a very small number 
        # of domains this is in the hundreds or over one thousand,
        # which is why we need a high limit here.
        # it would be better if BIRD addressed this repetition.
        # it i particularly noticeable in NUTS and NACE codes.
        # this high limit increases the processing time from under 1 minute
        # to a few minutes for the full BIRD data model.
        limit = 1400
        while ((counter < limit) and not(finished)):
            counter = counter + 1
            if SQLDeveloperImport.containsLiteral(self,theEnum.literals, adaptedValue +"_x" + str(counter)):
                newAdaptedValue = adaptedValue +"_x"+ str(counter+1)
            else:
                finished = True
        
        return newAdaptedValue
    
    
    def uniqueName (self, theEnum, enumUsedName):
        '''
        if the adapted name already exists in the enum then append it with _x2
        if the that string appended with _x2 already exists, then append with_x3 instead
        if that exists then _x4 etc.
        ''' 
        newAdaptedName = enumUsedName
        counter = 1
        finished = False
        limit = 1400
        if SQLDeveloperImport.containsName(self,theEnum.literals, enumUsedName ):
                newAdaptedName = enumUsedName +"_x2"
                
        while ((counter < limit) and not(finished)):
            counter = counter + 1
            if SQLDeveloperImport.containsName(self,theEnum.literals, enumUsedName +"_x" + str(counter)):
                newAdaptedName = enumUsedName +"_x"+ str(counter+1)
            else:
                finished = True 
  
        return newAdaptedName
    
    def  getMembersOfTheDomain  (self,  theDomain, memberIDToDomainMap):
        returnList = []
        for key, value in memberIDToDomainMap.items():
            if value == theDomain:
                returnList.append(key)
        return returnList
    
    def saveModelAsRPMNFile(self ):
        
            f = open(self.outputDirectory + 'data_model.rpmn', "a",  encoding='utf-8')
            f.write("\t\t package " + self.rpmnPackage.name + "\r")    
            for classifier in  self.rpmnPackage.classifiers:
                if isinstance(classifier,XClass):
                    f.write("\t\t\t")
                    if classifier.abstract==True:
                        f.write("abstract ")
                    f.write("class " + classifier.name)
                    if (hasattr(classifier, "superTypes")  and len(classifier.superTypes) > 0):
                        f.write(" extends " +  classifier.superTypes[0].name) 
                    f.write( " {\r")
                    for member in classifier.members:
                        
                        if isinstance(member, XReference):
                            if (member.containment):
                                f.write("\t\t\t\tcontains "  )
                            else:
                                f.write("\t\t\t\trefers "  )
                          
                            f.write(member.type.name + " " )
                            if member.upperBound == -1:
                                f.write("[] ")
                            elif ( (member.lowerBound == 0) and (member.upperBound == 1)):
                                f.write(" ")
                            else:
                                f.write("[" + str(member.lowerBound) + ".." +str(member.upperBound) + "] ")
                            f.write(member.name)
                            f.write(" \r"  )
                        elif isinstance(member, XAttribute):
                            f.write("\t\t\t\t")
                            # if member.iD:
                            #   f.write("id ")
                            f.write(member.type.name + " " )
                            
                            if member.upperBound == -1:
                                f.write("[] ")
                            elif ( (member.lowerBound == 0) and (member.upperBound == 1)):
                                f.write(" ")
                            else:
                                f.write("[" + str(member.lowerBound) + ".." +str(member.upperBound) + "] ")
                            f.write(member.name)
                            f.write(" \r"  )
                        elif isinstance(member, XOperation):
                            f.write("\t\t\t\top " + member.type.name + " " )
                            if member.upperBound == -1:
                                f.write("[] ")
                            elif ( (member.lowerBound == 0) and (member.upperBound == 1)):
                                f.write(" ")
                            else:
                                f.write("[" + str(member.lowerBound) + ".." +str(member.upperBound) + "] ")
                            f.write(member.name)
                            f.write("() {}")
                            f.write(" \r"  )
                            
                    f.write("\t\t\t}\r")
                if isinstance(classifier,XEnum):
                    f.write("\t\t\tenum " + classifier.name)
                    
                    f.write(" { ")  
                    for theLiteral in classifier.literals:
                        f.write(" " + theLiteral.literal + " as \"" + theLiteral.name + "\" = " + str(theLiteral.value)  )
                        
                    f.write(" }\r")
                
            f.write("\t\t\ttype Double wraps Double\r")
            f.write("\t\t\ttype String wraps String\r")
            f.write("\t\t\ttype Integer wraps Integer\r")  
            f.write("\t\t\ttype Date wraps Date\r")        
            f.close()
        
        
    def saveModelAsXMIFile(self):
        '''
         save model as an xmi file representing an object tree.
        '''
        rset = ResourceSet()

        resource = rset.create_resource(URI(self.outputDirectory + 'ldm.rpmn'))  # This will create an XMI resource
        resource.append(self.rpmnPackage)
        resource.save()
    
   
        
    def superclassContainsFeature(self,theSuperClass, attribute) :
        '''
        Checks if a superclass contains the attribute
        '''
        attributes = theSuperClass.members
        contains = False;
        for attribute2 in attributes:
            if (attribute2.name == attribute.name):
                contains = True;

        return contains
    
    def hasMemberCalled(self,theClass,memberName) :
        '''
        Checks if the class has a member with the name memberName
        '''
        
        members = theClass.members
        contains = False;
        for member in members:
            if (member.name == memberName):
                contains = True;

        return contains
    
    def numberofRelationShipsToThisClass(self,sourceClass, targetClass):
        '''
        Checks how many relationships there are between 2 classes
        It is possible that one class might have 2 different relationships 
        to the same class.
        '''
        features = sourceClass.members
        counter = 0;
        # do this for relationship attributes only.
        for feature in features:
            if ( isinstance(feature,XReference)):
                featureType = feature.type            
                if (featureType == targetClass):
                    counter = counter+1
            
        return counter
    
    
    def makeValidID(self,inputString):
        ''' 
        Tranlate text to be a valid id, without special characters, and following
        the rules for valid id's in RPMN
        '''
       
        # we do not allow id's to start with  number, if it does then we prepend with an underscore
        if(len(inputString) > 0):
            if((inputString[0] >= '0') and (inputString[0] <= '9')) :
                inputString = "_" + inputString
        # we replace special characters not allowed in id's with an underscore
        amendedInputString  =  inputString.replace(' ', '_').replace(')', '_').replace('(', '_') \
                .replace(',', '_').replace('\\', '_').replace('/', '_').replace('-', '_').replace(':', '_') \
                .replace('+', '_').replace('.', '_').replace('?', '_').replace('\'', '_').replace('>', '_') \
                .replace('<', '_').replace('\"', '_').replace(';', '_').replace('$', '_').replace('=', '_').replace('#', '_') \
                .replace('&', '_').replace('%', '_').replace('[', '_').replace(']', '_').replace('?', '_').replace('–', '_') \
                .replace( chr(0x2019), '_').replace( chr(65533), '_') \
                .replace(chr(0x2018), '_').replace(chr(0x0060), '_').replace(chr(0x00B4), '_') 
          
        returnString = SQLDeveloperImport.replaceAcutesGravesAndCircumflexes(self, amendedInputString)      

        return returnString;
    
    def replaceAcutesGravesAndCircumflexes(self, theInputString):
        '''
        We replace letters with acutes , graves, and circumflexes, with the basic letter.
        So for example "a acute" is replaced with "a"
        '''
        returnString = theInputString.replace(chr(0x00E9), 'e').replace(chr(0x00C9), 'E').replace(chr(0x00E8), 'e').replace(chr(0x00EB), 'e').replace(chr(0x00CB), 'E') \
                .replace(chr(0x00CA), 'E').replace(chr(0x00EA), 'e').replace(chr(0x00E7), 'c').replace(chr(0x00FC), 'u') \
                .replace(chr(0x00DA), 'U').replace(chr(0x00FA), 'u').replace(chr(0x00DC), 'U').replace(chr(0x00FC), 'u').replace(chr(0x00F6), 'o') \
                .replace(chr(0x200B), '_').replace(chr(0x202F), '_').replace(chr(0x205F), '_').replace(chr(0x3000), '_') \
                .replace(chr(0x2000), '_').replace(chr(0x2001), '_').replace(chr(0x2002), '_').replace(chr(0x2003), '_') \
                .replace(chr(0x2004), '_').replace(chr(0x2005), '_').replace(chr(0x2006), '_').replace(chr(0x2007), '_') \
                .replace(chr(0x2008), '_').replace(chr(0x2009), '_').replace(chr(0x200A), '_').replace(chr(0x00A0), '_') \
                .replace(chr(0x00ED), 'i').replace(chr(0x00CC), 'I').replace(chr(0x00EC), 'i').replace(chr(0x00CE), 'I') \
                .replace(chr(0x00EE), 'i').replace(chr(0x00E4), 'a').replace(chr(0x00E1), 'a').replace(chr(0x00C1), 'A').replace(chr(0x00D6), 'O') \
                .replace(chr(0x00D3), 'O').replace(chr(0x00F3), 'o').replace(chr(0x00D1), 'N').replace(chr(0x00F1), 'n') \
                .replace(chr(0x00DF), 'ss').replace(chr(0x00E0), 'a').replace(chr(0x00C0), 'A').replace(chr(0x00C2), 'A').replace(chr(0x00E2), 'a') \
                .replace(chr(0x00C3), 'A').replace(chr(0x00E3), 'a') \
                .replace(chr(0x00FD), 'y').replace(chr(0x017D), 'Z').replace(chr(0x017E), 'z') \
                .replace(chr(0x0160), 'S').replace(chr(0x0161), 's').replace(chr(0x00D8), 'O').replace(chr(0x00F8), 'o').replace(chr(0x00F4), 'o').replace(chr(0x00D5), 'O').replace(chr(0x00F5), 'o')  \
                .replace(chr(0x00C6), 'AE').replace(chr(0x01E2), 'AE').replace(chr(0x01FC), 'AE')   \
                .replace(chr(0x00E6), 'ae').replace(chr(0x01E3), 'ae').replace(chr(0x01FD), 'ae')    \
                .replace(chr(0x00C5), 'A').replace(chr(0x00E5), 'a')
             
        return returnString;

    def inEnumBlackList(self,adaptedEnumName):
        # TODO not sure if we still need this, it was introduces to deal with 
        # problematic domains in the past.
        if( (adaptedEnumName == "All_last_days_of_months___YYYY_MM") or
            (adaptedEnumName =="All_last_days_of_quarters___YYYY_MM") or
            (adaptedEnumName == "All_possible_dates_YYYY_MM_DD")  ):

            print(" field in blacklist: " + adaptedEnumName)
            return True
        else:
            return False 
        
    def containsLiteral(self,members,adaptedValue):
        '''
        checks if an enum contains a particular literal
        '''
        contains = False;
        for eEnumLiteral in members:
            if (eEnumLiteral.literal.lower() == adaptedValue.lower()):
                contains = True

        return contains
    
    def containsName(self,members,adaptedName):
        '''
        checks if an enum contains a particular name
        '''
        contains = False;
        for eEnumLiteral in members:
            if (eEnumLiteral.name.lower() == adaptedName.lower()):
                contains = True

        return contains
    
    def getLiteralsForEnumeration(self, domain, membersModule):
        '''
        returns the list of literals for an enumerations
        ''' 
        returnMembersList = []   
        for member in membersModule.members:
            if (member.domain_id == domain):
                returnMembersList.append(member) 
        return returnMembersList
    
    
    def getEcoreDataTypeForDataType(self):

        return self.xString
    
    def findROLEnum(self,enumName, enumMap):
        returnVal= None
        for key,value in enumMap.items():
            if value.name.lower() == enumName.lower():
               returnVal =  value
        
        return returnVal
    
if __name__ == '__main__':
    SQLDeveloperImport().convert('C:\\Users\\LENOVO\\freebirdtools-develop-nov\\git\\efbt\\openregspecs\\python\\resources','C:\\Users\\LENOVO\\freebirdtools-develop-nov\\git\\efbt\\openregspecs\\python\\results\\', True)
    
            
            
