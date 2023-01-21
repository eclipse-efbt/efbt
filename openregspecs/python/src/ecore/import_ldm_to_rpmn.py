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
from pyecore.ecore import *
import csv
from Utils import Utils 
from context import Context

class LDMImport(object):
    

    def doImport(self,context):
        '''
        import the items from the BIRD LDM csv files
        '''
        LDMImport.addLDMClassesToPackage(self,context)
        LDMImport.setLDMSuperClasses(self,context)
        LDMImport.addLDMEnumsToPackage(self,context)
        LDMImport.addLDMLiteralsToEnums(self,context) 
        LDMImport.createLDMTypesMap(self,context) 
        LDMImport.addLDMAttributesToClasses(self,context) 
        LDMImport.removeLDMAttributesAlreadyInSuperClass(self,context) 
        LDMImport.addLDMRelationshipsBetweenClasses(self,context)
        
    
        
    def addLDMClassesToPackage(self,context):
        '''
        for each entity in the LDM, create a class and add it to the package
        '''
        
        fileLocation = context.fileDirectory + "\\DM_Entities.csv"
        
        
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
                    
                    alteredClassName = Utils.makeValidID(className);  
                    if(alteredClassName.endswith("_derived")):
                        xclass = EClass(name=alteredClassName)
                        xclassTable = EClass(name=alteredClassName+"_DerivedTable")
                        xclassTable.containedEntityType = xclass
                        containmentReference  = EReference()
                        containmentReference.name=xclass.name+"s"
                        containmentReference.eType=xclass
                        containmentReference.upperBound = -1
                        containmentReference.lowerBound=0
                        containmentReference.containment= True
                        xclassTable.eStructuralFeatures.append(containmentReference)
                        xclassTableOperation = EOperation()
                        xclassTableOperation.name=xclass.name+"s"
                        xclassTableOperation.eType=xclass
                        xclassTableOperation.upperBound = -1
                        xclassTableOperation.lowerBound=0
                        xclassTable.members.append(xclassTableOperation)
                        context.rpmnPackage.eClassifiers.extend([xclass])
                        context.rpmnPackage.eClassifiers.extend([xclassTable])
                    elif(className.startswith("OUTPUT_LAYER_")):
                        xclass = EClass(name=alteredClassName)
                        
                        context.rpmnPackage.eClassifiers.extend([xclass])
                      
                    else:
                        xclass = EClass(name=alteredClassName)
                        # of engineering type is single table, as i should be for all members of a type
                        # heirarchy, and num_suptype is blanck, then this means that this class is a root
                        # of a type heirarchy....we will set such classes to be abstract.
                        if((engineering_type == "Single Table") and (Num_SuperTypeEntity_ID=="")   )    :
                            xclass.abstract=True
                        xclassTable = EClass(name=alteredClassName+"_BaseTable")
                        containmentReference  = EReference()
                        containmentReference.name=xclass.name+"s"
                        containmentReference.eType=xclass
                        containmentReference.upperBound = -1
                        containmentReference.lowerBound=0
                        containmentReference.containment= True
                        xclassTable.eStructuralFeatures.append(containmentReference)
                        context.rpmnPackage.eClassifiers.extend([xclass])
                        context.rpmnPackage.eClassifiers.extend([xclassTable])
        
                    # maintain a map a objectIDs to XClasses
                    context.classesMap[objectID]=xclass
                    context.tableMap[xclass]=xclassTable
         
    def setLDMSuperClasses(self,context):           
        fileLocation = context.fileDirectory + "\\DM_Entities.csv"
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
                        theclass = context.classesMap[classID]
                        superclass = context.classesMap[superclassID]
                        theclass.superTypes.extend([superclass])
                        
                       
    def addLDMEnumsToPackage(self,context):
        '''
        for each domain in the LDM add an enum to the package
        '''
        fileLocation = context.fileDirectory + "\\DM_Domains.csv"
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
                    adaptedEnumName = Utils.makeValidID(enumName)+"_domain"
                    if(not Utils.inEnumBlackList(self,adaptedEnumName)):
                        theEnum = EEnum()
                        theEnum.name = adaptedEnumName
                        #maintain a map of enum IDS to XEnum objects
                        context.enumMap[enumID] = theEnum
                        context.rpmnPackage.eClassifiers.extend([theEnum])
                        
    def addLDMLiteralsToEnums(self,context):
        '''
        for each memebr of a domain the LDM, add a literal to the corresponding enum
        '''
        fileLocation = context.fileDirectory + "\\DM_Domain_AVT.csv"
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
                        enumUsedName = Utils.makeValidID(self,row[3])
                        #enumName = row[5]
                        adaptedEnumName = Utils.makeValidID(self,enumUsedName)
                        value = row[4]
                        adaptedValue = Utils.makeValidID(self,value)
                        try:
                            theEnum = context.enumMap[enumID]
                            newAdaptedValue = Utils.uniqueValue(self,theEnum,adaptedValue)
                            newAdaptedName = Utils.uniqueName(self,theEnum,adaptedEnumName)
                            enumLiteral = EEnumLiteral()
                            enumLiteral.name = newAdaptedName
                            enumLiteral.literal = newAdaptedValue
                            enumLiteral.value = counter
                            theEnum.eLiterals.extend([enumLiteral])
                                
                        except KeyError:
                            print( "missing domain: " + enumID )
                            
                    except IndexError:
                        print( "row in DM_Domain_AVT.csv skipped  due to improper formatting at row number")
                        print(counter)

    def createLDMTypesMap(self,context):                
        # for each logicalDatatype for orcle 12c, make a Datatype if we have an
        # equivalent
        
        fileLocation = context.fileDirectory + "\\DM_Logical_To_Native.csv"
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

                            context.datatypeMap[dataTypeID] = context.xString
                      
                        if (native_type.strip() == "VARCHAR2") :

                            context.datatypeMap[dataTypeID] = context.xString
                      
                        if (native_type.strip() == "INTEGER") :

                            context.datatypeMap[dataTypeID] = context.xInt
                      
                        if (native_type.strip() == "DATE") :

                            context.datatypeMap[dataTypeID] = context.xDate
                        
                        if (native_type.strip() == "NUMBER") :

                            context.datatypeMap[dataTypeID] = context.xDouble
                        
                        if (native_type.strip() == "UNKNOWN") :

                            context.datatypeMap[dataTypeID] = context.xString
             
    def addLDMAttributesToClasses(self,context):
        '''
        For each attribute on an entity of the LDM, add an attribute
        to the relevant class in the package
        '''
            
        fileLocation = context.fileDirectory + "\\DM_Attributes.csv"
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
                    amendedAttributeName = Utils.makeValidID(self,attributeName);
                    attributeKind = row[7]
                   
                    classID = row[4]
                    relationID = row[32]
                    primary_key_or_not = row[35]
                    theClass = context.classesMap[classID]
                    
                    classIsDerived = False
                    if (theClass.name.endswith("_derived")):
                        classIsDerived = True
                        
                    theAttributeName =  amendedAttributeName
                    
                    # we only add attributes here if they are not representing a relationship
                    if relationID == "":
                        
                        if (attributeKind == "Domain"):
                            enumID = row[12]
                            theEnum = context.enumMap[enumID]
                            
                            attribute = EAttribute()
                            if(primary_key_or_not == "P"):
                                attribute.iD = True
                                
                            attribute.lowerBound=0
                            attribute.upperBound=1
                            if(theEnum.name == "String"):
                                attribute.name = theAttributeName
                                attribute.eType = context.xString
                            elif(theEnum.name.startswith("String_")):
                                attribute.name = theAttributeName
                                attribute.eType = context.xString
                            elif(theEnum.name == "Number"):
                                attribute.name = theAttributeName
                                attribute.eType = context.xDouble
                            elif(theEnum.name.startswith("Real_")):
                                attribute.name = theAttributeName
                                attribute.eType = context.xDouble
                            elif(theEnum.name.startswith("Monetary")):
                                attribute.name = theAttributeName
                                attribute.eType = context.xInt
                            elif(theEnum.name.startswith("Non_negative_monetary_amounts_with_2_decimals")): 
                                attribute.name = theAttributeName
                                attribute.eType = context.xInt
                            elif(theEnum.name.startswith("Non_negative_integers")): 
                                attribute.name = theAttributeName
                                attribute.eType = context.xInt
                            elif(theEnum.name.startswith("All_possible_dates")):
                                attribute.name = theAttributeName
                                attribute.eType = context.xDate
                                
                            # This is a common domain used for String identifiers in BIRD in SQLDeveloper
                            
                            else:
                                attribute.name = theAttributeName
                                attribute.eType = theEnum  
                            
                            if classIsDerived:
                                operation = EOperation()
                                operation.lowerBound=0
                                operation.upperBound=1
                                if(theEnum.name == "String"):
                                    operation.name = theAttributeName
                                    operation.eType = context.xString
                                elif(theEnum.name.startswith("String_")):
                                    operation.name = theAttributeName
                                    operation.eType = context.xString
                                elif(theEnum.name == "Number"):
                                    operation.name = theAttributeName
                                    operation.eType = context.xDouble
                                
                                elif(theEnum.name.startswith("Real_")):
                                    operation.name = theAttributeName
                                    operation.eType = context.xDouble
                                elif(theEnum.name.startswith("Monetary")): 
                                    operation.name = theAttributeName
                                    operation.eType = context.xInt
                                elif(theEnum.name.startswith("Non_negative_monetary_amounts_with_2_decimals")): 
                                    operation.name = theAttributeName
                                    operation.eType = context.xInt
                                elif(theEnum.name.startswith("Non_negative_integers")): 
                                    operation.name = theAttributeName
                                    operation.eType = context.xInt
                                elif(theEnum.name.startswith("All_possible_dates")):   
                                    operation.name = theAttributeName
                                    operation.eType = context.xDate  
                                else:
                                    operation.name = theAttributeName
                                    operation.eType = theEnum  
                                          
    
                        if (attributeKind == "Logical Type"):
                            print("Logical Type")
                            dataTypeID = row[14]
                            try:
                                datatype = context.datatypeMap[dataTypeID]
                                attribute = EAttribute()
                                attribute.lowerBound=0
                                attribute.upperBound=1
                                attribute.name =amendedAttributeName
                                attribute.eType = Utils.getEcoreDataTypeForDataType(self)
                                
                                if classIsDerived:
                                    operation = EOperation()
                                    operation.lowerBound=0
                                    operation.upperBound=1
                                    operation.name =amendedAttributeName
                                    operation.eType = Utils.getEcoreDataTypeForDataType(self)
                                
                            except KeyError:
                                print("missing datatype: ")
                                print(dataTypeID)                       
    
                        
    
                        try:
    
                            theClass = context.classesMap[classID]
                            theClass.eStructuralFeatures.extend([attribute])
                            if classIsDerived:
                                theClass.eStructuralFeatures.extend([operation])
    
                        except:
                            print( "missing class2: " )
                            print(classID)
                        
    def removeLDMAttributesAlreadyInSuperClass(self,context):
        '''
        if we already have created an attribute in both a subclass and a superclass
        then we delete it in the subclass
        '''    
        for theClass in context.classesMap.values():
            if len(theClass.superTypes) > 0:
                superclass = theClass.superTypes[0]
                if (superclass):
                    
                    attributes = theClass.members
                    attributesToDelete = []
                    for theAttribute in attributes :
                        if Utils.superclassContainsFeature(self,superclass, theAttribute):
                            attributesToDelete.append(theAttribute);
    
                    for theAttribute in attributesToDelete :
                        theClass.eStructuralFeatures.remove(theAttribute);
                        print( "removed attribute or reference since it exists in the superclass")
                        print(  theAttribute.name )
               
    def addLDMRelationshipsBetweenClasses(self,context):
        '''
        For each relationship in the LDM, add a reference between the relevant classes
        '''    
        fileLocation = context.fileDirectory + "\\DM_Relations.csv"
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
                    
                    referenceName = "the" + Utils.makeValidID(self,targetClassName);

                    try:
                        theClass = context.classesMap[sourceID]
                    except KeyError:
                        print("missing class1: " + sourceID)
                    
                    try:   
                        targetClass = context.classesMap[targetID] 
                    except KeyError:
                        print("missing target class: " + targetID) 
                                          
                    numOfRelations = Utils.numberofRelationShipsToThisClass(self,theClass,targetClass)
                    if(numOfRelations>0):
                        print("numOfRelations")
                        print(numOfRelations)
                        referenceName = referenceName + str(numOfRelations)
                    relationalAttribute = None
                    if (target_Optional.strip() == "Y"):
                        if (sourceTo_Target_Cardinality.strip() == "*"):
                            referenceName = referenceName + "s"
                            eReference  = EReference()
                            eReference.name=referenceName
                            eReference.eType=targetClass
                            #upper bound of -1 means there is no upper bounds, so represents an open list of reference
                            eReference.upperBound = -1
                            eReference.lowerBound=0
                            eReference.containment= False
                            if (theClass.name.endswith("_derived")):
                                theSourceTable = context.tableMap[theClass]
                                theTargetTable = context.tableMap[targetClass]
                                if not(Utils.hasMemberCalled(self, theSourceTable, "sourceTable1")):
                                   
                                    sourceTablesReference  = EReference()
                                    sourceTablesReference.name="sourceTable1"
                                    sourceTablesReference.eType=theTargetTable
                                    sourceTablesReference.upperBound = -1
                                    sourceTablesReference.lowerBound=0
                                    sourceTablesReference.containment= False
                                    theSourceTable.eStructuralFeatures.append(sourceTablesReference)
                                else:
                                   
                                    sourceTablesReference  = EReference()
                                    sourceTablesReference.name="sourceTable2"
                                    sourceTablesReference.eType=theTargetTable
                                    sourceTablesReference.upperBound = -1
                                    sourceTablesReference.lowerBound=0
                                    sourceTablesReference.containment= False
                                    theSourceTable.eStructuralFeatures.append(sourceTablesReference)
                        else:
                            eReference  = EReference()
                            eReference.name=referenceName
                            eReference.eType=targetClass
                            eReference.upperBound = 1
                            eReference.lowerBound=0
                            eReference.containment= False
                            if (theClass.name.endswith("_derived")):
                                theSourceTable = context.tableMap[theClass]
                                theTargetTable = context.tableMap[targetClass]
                                if not(Utils.hasMemberCalled(self, theSourceTable, "sourceTable1")):
                                    
                                    sourceTablesReference  = EReference()
                                    sourceTablesReference.name="sourceTable1"
                                    sourceTablesReference.eType=theTargetTable
                                    sourceTablesReference.upperBound = -1
                                    sourceTablesReference.lowerBound=0
                                    sourceTablesReference.containment= False
                                    theSourceTable.eStructuralFeatures.append(sourceTablesReference)
                                else:
                                    
                                    sourceTablesReference  = EReference()
                                    sourceTablesReference.name="sourceTable2"
                                    sourceTablesReference.eType=theTargetTable
                                    sourceTablesReference.upperBound = -1
                                    sourceTablesReference.lowerBound=0
                                    sourceTablesReference.containment= False
                                    theSourceTable.eStructuralFeatures.append(sourceTablesReference)                 
                    else:
                        if (sourceTo_Target_Cardinality.strip() == "*"):
                            referenceName = referenceName + "s"                       
                            eReference  = EReference()
                            eReference.name=referenceName
                            eReference.eType=targetClass
                            eReference.upperBound = -1
                            eReference.lowerBound=1
                            eReference.containment= False
                            if (theClass.name.endswith("_derived")):

                                theSourceTable = context.tableMap[theClass]
                                theTargetTable = context.tableMap[targetClass]
                                if not(Utils.hasMemberCalled(self, theSourceTable, "sourceTable1")):
                                    
                                    sourceTablesReference  = EReference()
                                    sourceTablesReference.name="sourceTable1"
                                    sourceTablesReference.eType=theTargetTable
                                    sourceTablesReference.upperBound = -1
                                    sourceTablesReference.lowerBound=0
                                    sourceTablesReference.containment= False
                                    theSourceTable.members.append(sourceTablesReference)
                                else:
                                    sourceTablesReference = EReference("sourceTable2", theTargetTable, upper=-1, lower=0, containment=False)
                                    theSourceTable.eStructuralFeatures.append(sourceTablesReference)                       
                        else:      
                            eReference  = EReference()
                            eReference.name=referenceName
                            eReference.eType=targetClass
                            eReference.upperBound = 1
                            eReference.lowerBound=1
                            eReference.containment= False
                            if (theClass.name.endswith("_derived")):
                                theSourceTable = context.tableMap[theClass]
                                theTargetTable = context.tableMap[targetClass]
                                if not(Utils.hasMemberCalled(self, theSourceTable, "sourceTable1")):
                                    
                                    sourceTablesReference  = EReference()
                                    sourceTablesReference.name="sourceTable1"
                                    sourceTablesReference.eType=theTargetTable
                                    sourceTablesReference.upperBound = -1
                                    sourceTablesReference.lowerBound=0
                                    sourceTablesReference.containment= False
                                    theSourceTable.eStructuralFeatures.append(sourceTablesReference)
                                else:
                                    
                                    sourceTablesReference  = EReference()
                                    sourceTablesReference.name="sourceTable2"
                                    sourceTablesReference.eType=theTargetTable
                                    sourceTablesReference.upperBound = -1
                                    sourceTablesReference.lowerBound=0
                                    sourceTablesReference.containment= False
                                    theSourceTable.eStructuralFeatures.append(sourceTablesReference)
                    if (not (theClass is None) ) :                 
                        theClass.members.append(eReference)
   
            
