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
        
    
    
    def convert (self,fileDirectory,outputDirectory, ROLAvailable):  
        
        xString = XDataType()
        xString.name = "String"
        
        xDouble = XDataType()
        xDouble.name = "Double"
        
        xInt = XDataType()
        xInt.name = "Integer"
        
        xDate = XDataType()
        xDate.name = "Date"
    
        rpmnPackage = XPackage(name='bird')
        
        rpmnPackage.classifiers.append(xString)
        rpmnPackage.classifiers.append(xDouble)
        rpmnPackage.classifiers.append(xInt)
        classesMap = {}
        tableMap = {}
        fileLocation = fileDirectory + "\\DM_Entities.csv"
        enumMap = {}
        missingDomains = []
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
                    
                    
                    alteredClassName = SQLDeveloperImport.replaceSpaceWithUnderscore(self,className);  
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
                        rpmnPackage.classifiers.extend([xclass])
                        rpmnPackage.classifiers.extend([xclassTable])
                    elif(className.startswith("OUTPUT_LAYER_")):
                        xclass = XClass(name=alteredClassName)
                        
                        rpmnPackage.classifiers.extend([xclass])
                      
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
                        rpmnPackage.classifiers.extend([xclass])
                        rpmnPackage.classifiers.extend([xclassTable])
        
                    # maintain a map a objectIDs to XClasses
                    classesMap[objectID]=xclass
                    tableMap[xclass]=xclassTable
                    
        fileLocation = fileDirectory + "\\DM_Entities.csv"
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
                        theclass = classesMap[classID]
                        superclass = classesMap[superclassID]
                        theclass.superTypes.extend([superclass])
                        
                       

        fileLocation = fileDirectory + "\\DM_Domains.csv"
        headerSkipped = False
        counter = 0
        # Creat an XEnum for each domain, and add it to the XPackage
        with open(fileLocation) as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if (not headerSkipped):
                        headerSkipped = True
                else:
                    counter=counter+1
                    enumID = row[0]
                    enumName = row[1]
                    adaptedEnumName = SQLDeveloperImport.replaceSpaceWithUnderscore(self,enumName)+"_domain"
                    if(not SQLDeveloperImport.inEnumBlackList(self,adaptedEnumName)):
                        theEnum = XEnum()
                        theEnum.name = adaptedEnumName
                        #maintain a map of enum IDS to XEnum objects
                        enumMap[enumID] = theEnum
                        rpmnPackage.classifiers.extend([theEnum])
                        

        fileLocation = fileDirectory + "\\DM_Domain_AVT.csv"
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
                        enumUsedName = "m_" + SQLDeveloperImport.replaceSpaceWithUnderscore(self,row[3])
                        enumName = row[5]
                        adaptedEnumName = SQLDeveloperImport.replaceSpaceWithUnderscore(self,enumName)
                        value = row[4]
                        adaptedValue = SQLDeveloperImport.replaceSpaceWithUnderscore(self,value)
                        newAdaptedValue = adaptedValue
                        try:
                            theEnum = enumMap[enumID]
                            if SQLDeveloperImport.containsLiteral(self,theEnum.literals, adaptedValue ):
                                newAdaptedValue = adaptedValue +"_x2"
                            if SQLDeveloperImport.containsLiteral(self,theEnum.literals, adaptedValue +"_x2" ):
                                newAdaptedValue = adaptedValue +"_x3"
                            if SQLDeveloperImport.containsLiteral(self,theEnum.literals, adaptedValue +"_x3" ):
                                newAdaptedValue = adaptedValue +"_x4"
                            if SQLDeveloperImport.containsLiteral(self,theEnum.literals, adaptedValue +"_x4" ):
                                newAdaptedValue = adaptedValue +"_x5"
                            if SQLDeveloperImport.containsLiteral(self,theEnum.literals, adaptedValue +"_x5" ):
                                newAdaptedValue = adaptedValue +"_x6"
                            if SQLDeveloperImport.containsLiteral(self,theEnum.literals, adaptedValue +"_x6" ):
                                newAdaptedValue = adaptedValue +"_x7"
                            if SQLDeveloperImport.containsLiteral(self,theEnum.literals, adaptedValue +"_x7" ):
                                newAdaptedValue = adaptedValue +"_x8"
                            if SQLDeveloperImport.containsLiteral(self,theEnum.literals, adaptedValue +"_x8" ):
                                newAdaptedValue = adaptedValue +"_x9"
                            if SQLDeveloperImport.containsLiteral(self,theEnum.literals, adaptedValue +"_x9" ):
                                newAdaptedValue = adaptedValue +"_x10"
                            if SQLDeveloperImport.containsLiteral(self,theEnum.literals, adaptedValue +"_x10" ):
                                newAdaptedValue = adaptedValue +"_x11"
                            if SQLDeveloperImport.containsLiteral(self,theEnum.literals, adaptedValue +"_x11" ):
                                newAdaptedValue = adaptedValue +"_x12"
                            if SQLDeveloperImport.containsLiteral(self,theEnum.literals, adaptedValue +"_x12" ):
                                newAdaptedValue = adaptedValue +"_x13"
                            if SQLDeveloperImport.containsLiteral(self,theEnum.literals, adaptedValue +"_x13" ):
                                newAdaptedValue = adaptedValue +"_x14"
                            if SQLDeveloperImport.containsLiteral(self,theEnum.literals, adaptedValue +"_x14" ):
                                newAdaptedValue = adaptedValue +"_x15"
                            if SQLDeveloperImport.containsLiteral(self,theEnum.literals, adaptedValue +"_x15" ):
                                newAdaptedValue = adaptedValue +"_x16"
                            if SQLDeveloperImport.containsLiteral(self,theEnum.literals, adaptedValue +"_x16" ):
                                newAdaptedValue = adaptedValue +"_x17"
                            if SQLDeveloperImport.containsLiteral(self,theEnum.literals, adaptedValue +"_x17" ):
                                newAdaptedValue = adaptedValue +"_x18"
                            if SQLDeveloperImport.containsLiteral(self,theEnum.literals, adaptedValue +"_x18" ):
                                newAdaptedValue = adaptedValue +"_x19"
                            if SQLDeveloperImport.containsLiteral(self,theEnum.literals, adaptedValue +"_x19" ):
                                newAdaptedValue = adaptedValue +"_x20"
                            if SQLDeveloperImport.containsLiteral(self,theEnum.literals, adaptedValue +"_x20" ):
                                newAdaptedValue = adaptedValue +"_x21"
                            if SQLDeveloperImport.containsLiteral(self,theEnum.literals, adaptedValue +"_x21" ):
                                newAdaptedValue = adaptedValue +"_x22"
                            if SQLDeveloperImport.containsLiteral(self,theEnum.literals, adaptedValue +"_x22" ):
                                newAdaptedValue = adaptedValue +"_x23"
                            if SQLDeveloperImport.containsLiteral(self,theEnum.literals, adaptedValue +"_x23" ):
                                newAdaptedValue = adaptedValue +"_x24"
                            if SQLDeveloperImport.containsLiteral(self,theEnum.literals, adaptedValue +"_x24" ):
                                newAdaptedValue = adaptedValue +"_x25"
                            if SQLDeveloperImport.containsLiteral(self,theEnum.literals, adaptedValue +"_x25" ):
                                newAdaptedValue = adaptedValue +"_x26"
                            if SQLDeveloperImport.containsLiteral(self,theEnum.literals, adaptedValue +"_x26" ):
                                newAdaptedValue = adaptedValue +"_x27"
                            if SQLDeveloperImport.containsLiteral(self,theEnum.literals, adaptedValue +"_x27" ):
                                newAdaptedValue = adaptedValue +"_x28"
                            if SQLDeveloperImport.containsLiteral(self,theEnum.literals, adaptedValue +"_x28" ):
                                newAdaptedValue = adaptedValue +"_x29"
                                
                            newAdaptedName = enumUsedName
                            if SQLDeveloperImport.containsName(self,theEnum.literals, enumUsedName ):
                                newAdaptedName = enumUsedName +"_x2"
                            if SQLDeveloperImport.containsName(self,theEnum.literals, enumUsedName +"_x2" ):
                                newAdaptedName = enumUsedName +"_x3"
                            if SQLDeveloperImport.containsName(self,theEnum.literals, enumUsedName +"_x3" ):
                                newAdaptedName = enumUsedName +"_x4"
                            if SQLDeveloperImport.containsName(self,theEnum.literals, enumUsedName +"_x4" ):
                                newAdaptedName = enumUsedName +"_x5"
                            if SQLDeveloperImport.containsName(self,theEnum.literals, enumUsedName +"_x5" ):
                                newAdaptedName = enumUsedName +"_x6"
                            if SQLDeveloperImport.containsName(self,theEnum.literals, enumUsedName +"_x6" ):
                                newAdaptedName = enumUsedName +"_x7"
                            if SQLDeveloperImport.containsName(self,theEnum.literals, enumUsedName +"_x7" ):
                                newAdaptedName = enumUsedName +"_x8"
                            if SQLDeveloperImport.containsName(self,theEnum.literals, enumUsedName +"_x8" ):
                                newAdaptedName = enumUsedName +"_x9"
                            if SQLDeveloperImport.containsName(self,theEnum.literals, enumUsedName +"_x9" ):
                                newAdaptedName = enumUsedName +"_x10"
                            if SQLDeveloperImport.containsName(self,theEnum.literals, enumUsedName +"_x10" ):
                                newAdaptedName = enumUsedName +"_x11"
                            if SQLDeveloperImport.containsName(self,theEnum.literals, enumUsedName +"_x11" ):
                                newAdaptedName = enumUsedName +"_x12"
                            if SQLDeveloperImport.containsName(self,theEnum.literals, enumUsedName +"_12" ):
                                newAdaptedName = enumUsedName +"_x13"
                            if SQLDeveloperImport.containsName(self,theEnum.literals, enumUsedName +"_x13" ):
                                newAdaptedName = enumUsedName +"_x14"
                            if SQLDeveloperImport.containsName(self,theEnum.literals, enumUsedName +"_x14" ):
                                newAdaptedName = enumUsedName +"_x15"
                            if SQLDeveloperImport.containsName(self,theEnum.literals, enumUsedName +"_x15" ):
                                newAdaptedName = enumUsedName +"_x16"
                            if SQLDeveloperImport.containsName(self,theEnum.literals, enumUsedName +"_x16" ):
                                newAdaptedName = enumUsedName +"_x17"
                            if SQLDeveloperImport.containsName(self,theEnum.literals, enumUsedName +"_x17" ):
                                newAdaptedName = enumUsedName +"_x18"
                            if SQLDeveloperImport.containsName(self,theEnum.literals, enumUsedName +"_x18" ):
                                newAdaptedName = enumUsedName +"_x19"
                            if SQLDeveloperImport.containsName(self,theEnum.literals, enumUsedName +"_x19" ):
                                newAdaptedName = enumUsedName +"_x20"
                            if SQLDeveloperImport.containsName(self,theEnum.literals, enumUsedName +"_x20" ):
                                newAdaptedName = enumUsedName +"_x21"
                            if SQLDeveloperImport.containsName(self,theEnum.literals, enumUsedName +"_x21" ):
                                newAdaptedName = enumUsedName +"_x22"
                            if SQLDeveloperImport.containsName(self,theEnum.literals, enumUsedName +"_x22" ):
                                newAdaptedName = enumUsedName +"_x23"
                            if SQLDeveloperImport.containsName(self,theEnum.literals, enumUsedName +"_x23" ):
                                newAdaptedName = enumUsedName +"_x24"
                            if SQLDeveloperImport.containsName(self,theEnum.literals, enumUsedName +"_x24" ):
                                newAdaptedName = enumUsedName +"_x25"
                            if SQLDeveloperImport.containsName(self,theEnum.literals, enumUsedName +"_x25" ):
                                newAdaptedName = enumUsedName +"_x26"
                            if SQLDeveloperImport.containsName(self,theEnum.literals, enumUsedName +"_x26" ):
                                newAdaptedName = enumUsedName +"_x27"
                            if SQLDeveloperImport.containsName(self,theEnum.literals, enumUsedName +"_x27" ):
                                newAdaptedName = enumUsedName +"_x28"
                            if SQLDeveloperImport.containsName(self,theEnum.literals, enumUsedName +"_x28" ):
                                newAdaptedName = enumUsedName +"_x29"
                            if SQLDeveloperImport.containsName(self,theEnum.literals, enumUsedName +"_x29" ):
                                newAdaptedName = enumUsedName +"_x30"
                            if SQLDeveloperImport.containsName(self,theEnum.literals, enumUsedName +"_x30" ):
                                newAdaptedName = enumUsedName +"_x31"
                            if SQLDeveloperImport.containsName(self,theEnum.literals, enumUsedName +"_x31" ):
                                newAdaptedName = enumUsedName +"_x32"
                            
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

                        
        # for each logicalDatatype for orcle 12c, make a Datatype if we have an
        # equivalent
        datatypeMap = {}
        domainDataTypeMap = {}
        fileLocation = fileDirectory + "\\DM_Logical_To_Native.csv"
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

                            datatypeMap[dataTypeID] = xString
                      
                        if (native_type.strip() == "VARCHAR2") :

                            datatypeMap[dataTypeID] = xString
                      
                        if (native_type.strip() == "INTEGER") :

                            datatypeMap[dataTypeID] = xInt
                      
                        if (native_type.strip() == "DATE") :

                            datatypeMap[dataTypeID] = xDate
                        
                        if (native_type.strip() == "NUMBER") :

                            datatypeMap[dataTypeID] = xDouble
                        
                        if (native_type.strip() == "UNKNOWN") :

                            datatypeMap[dataTypeID] = xString
             
        
        fileLocation = fileDirectory + "\\DM_Attributes.csv"
        headerSkipped = False
        # or each attribute add an Xattribute to the correct XClass represtnting the Entity
        # the attribute should have the correct type, which may be a specific
        # enumeration

        with open(fileLocation) as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if (not headerSkipped):
                        headerSkipped = True
                else:
                    attributeName = row[0]
                    amendedAttributeName = SQLDeveloperImport.replaceSpaceWithUnderscore(self,attributeName);
                    attributeKind = row[7]
                   
                    classID = row[4]
                    relationID = row[32]
                    primary_key_or_not = row[35]
                    theClass = classesMap[classID]
                    
                    classIsDerived = False
                    if (theClass.name.endswith("_derived")):
                        classIsDerived = True
                        
                    theAttributeName =  amendedAttributeName
                    
                    # we only add attributes here if they are not representing a relationship
                    if relationID == "":
                        
                        if (attributeKind == "Domain"):
                            enumID = row[12]
                            theEnum = enumMap[enumID]
                            
                            attribute = XAttribute()
                            if(primary_key_or_not == "P"):
                                attribute.iD = True
                                
                            attribute.lowerBound=0
                            attribute.upperBound=1
                            if(theEnum.name == "String"):
                                attribute.name = theAttributeName
                                attribute.type = xString
                            elif(theEnum.name.startswith("String_")):
                                attribute.name = theAttributeName
                                attribute.type = xString
                            elif(theEnum.name == "Number"):
                                attribute.name = theAttributeName
                                attribute.type = xDouble
                            elif(theEnum.name.startswith("Real_")):
                                attribute.name = theAttributeName
                                attribute.type = xDouble
                            elif(theEnum.name.startswith("Monetary")):
                                attribute.name = theAttributeName
                                attribute.type = xInt
                            elif(theEnum.name.startswith("Non_negative_monetary_amounts_with_2_decimals")): 
                                attribute.name = theAttributeName
                                attribute.type = xInt
                            elif(theEnum.name.startswith("Non_negative_integers")): 
                                attribute.name = theAttributeName
                                attribute.type = xInt
                            elif(theEnum.name.startswith("All_possible_dates")):
                                attribute.name = theAttributeName
                                attribute.type = xDate
                                
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
                                    operation.type = xString
                                elif(theEnum.name.startswith("String_")):
                                    operation.name = theAttributeName
                                    operation.type = xString
                                elif(theEnum.name == "Number"):
                                    operation.name = theAttributeName
                                    operation.type = xDouble
                                
                                elif(theEnum.name.startswith("Real_")):
                                    operation.name = theAttributeName
                                    operation.type = xDouble
                                elif(theEnum.name.startswith("Monetary")): 
                                    operation.name = theAttributeName
                                    operation.type = xInt
                                elif(theEnum.name.startswith("Non_negative_monetary_amounts_with_2_decimals")): 
                                    operation.name = theAttributeName
                                    operation.type = xInt
                                elif(theEnum.name.startswith("Non_negative_integers")): 
                                    operation.name = theAttributeName
                                    operation.type = xInt
                                elif(theEnum.name.startswith("All_possible_dates")):   
                                    operation.name = theAttributeName
                                    operation.type = xDate  
                                else:
                                    operation.name = theAttributeName
                                    operation.type = theEnum  
                                          
    
                        if (attributeKind == "Logical Type"):
                            print("Logical Type")
                            dataTypeID = row[14]
                            try:
                                datatype = datatypeMap[dataTypeID]
                                attribute = XAttribute()
                                attribute.lowerBound=0
                                attribute.upperBound=1
                                attribute.name =amendedAttributeName
                                attribute.type = SQLDeveloperImport.getEcoreDataTypeForDataType(self,datatypeMap,datatype,xString)
                                
                                if classIsDerived:
                                    operation = XOperation()
                                    operation.lowerBound=0
                                    operation.upperBound=1
                                    operation.name =amendedAttributeName
                                    operation.type = SQLDeveloperImport.getEcoreDataTypeForDataType(self,datatypeMap,datatype,xString)
                                
                            except KeyError:
                                print("missing datatype: ")
                                print(dataTypeID)                       
    
                        
    
                        try:
    
                            theClass = classesMap[classID]
                            theClass.members.extend([attribute])
                            if classIsDerived:
                                 theClass.members.extend([operation])
    
                        except:
                            print( "missing class2: " )
                            print(classID)
                        
        #remove any attributes that already exist in superclass.

        for theClass in classesMap.values():
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
               
          
        #for each relationship of the entity add a Xreference to the XClass for the entity
        fileLocation = fileDirectory + "\\DM_Relations.csv"
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
                    
                    referenceName = "the" + SQLDeveloperImport.replaceSpaceWithUnderscore(self,targetClassName);

                    try:
                        theClass = classesMap[sourceID]
                    except KeyError:
                        print("missing class1: " + sourceID)
                    
                    try:   
                        targetClass = classesMap[targetID] 
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
                                theSourceTable = tableMap[theClass]
                                theTargetTable = tableMap[targetClass]
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
                                theSourceTable = tableMap[theClass]
                                theTargetTable = tableMap[targetClass]
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

                                theSourceTable = tableMap[theClass]
                                theTargetTable = tableMap[targetClass]
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
                                theSourceTable = tableMap[theClass]
                                theTargetTable = tableMap[targetClass]
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
        if ROLAvailable:           
            fileLocation = fileDirectory + "\\cube.csv"
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
    
                            alteredClassName = SQLDeveloperImport.replaceSpaceWithUnderscore(self,className);  
                            
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
                            rpmnPackage.classifiers.extend([xclass])
                            rpmnPackage.classifiers.extend([xclassTable])
                            
                
                            # maintain a map a objectIDs to XClasses
                            classesMap[objectID]=xclass
                            tableMap[xclass]=xclassTable
                            
            
            # Make a variable to Domain Map
            fileLocation = fileDirectory + "\\variable.csv"
            headerSkipped = False
            # or each attribute add an Xattribute to the correct XClass represtnting the Entity
            # the attribute should have the correct type, which may be a specific
            # enumeration
            variableToDomainMap = {}
            with open(fileLocation,  encoding='utf-8') as csvfile:
                filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
                for row in filereader:
                    if (not headerSkipped):
                            headerSkipped = True
                    else:
                        variableName = row[1]
                        #domainName = SQLDeveloperImport.replaceSpaceWithUnderscore(self,row[3])
                        domain = row[4]
                        variableToDomainMap[variableName] = domain
                       
            # Make a domain  to Domain Name Map 
            fileLocation = fileDirectory + "\\domain.csv"
            headerSkipped = False
    
            domainToDomainNameMap = {}
            with open(fileLocation,  encoding='utf-8') as csvfile:
                filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
                for row in filereader:
                    if (not headerSkipped):
                            headerSkipped = True
                    else:
                        domainID = row[1]
                        #domainName = SQLDeveloperImport.replaceSpaceWithUnderscore(self,row[3])
                        domainName = row[2]
                        domainToDomainNameMap[domainID] = domainName
                       
            
             # Make a domain  to Domain Name Map 
            fileLocation = fileDirectory + "\\member.csv"
            headerSkipped = False
    
            memberIDToDomainMap = {}
            memberIDToMemberNameMap = {}
            memberIDToMemberCodeMap = {}
            with open(fileLocation,  encoding='utf-8') as csvfile:
                filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
                for row in filereader:
                    if (not headerSkipped):
                            headerSkipped = True
                    else:
                        memberID = row[1]
                        #domainName = SQLDeveloperImport.replaceSpaceWithUnderscore(self,row[3])
                        memberCode= row[2]
                        memberName = row[3]
                        domainId =  row[4]
                        memberIDToDomainMap[memberID] = domainId
                        memberIDToMemberNameMap[memberID] = memberName
                        memberIDToMemberCodeMap[memberID] = memberCode
                        
                       
                       
                       
            fileLocation = fileDirectory + "\\cube_structure_item.csv"
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
                        amendedAttributeName = SQLDeveloperImport.replaceSpaceWithUnderscore(self,attributeName)
                        variable = row[2]
                       
                        classID = row[0]
                        try: 
                            theClass = classesMap[classID]
                            
                            classIsDerived = True
                                
                            theAttributeName =  amendedAttributeName
           
                            domainID = variableToDomainMap[variable]
                            domain_ID_Name = domainToDomainNameMap[domainID]
                            amendedDomainName = SQLDeveloperImport.replaceSpaceWithUnderscore(self,domain_ID_Name)
                            theEnum =  SQLDeveloperImport.findROLEnum(self,amendedDomainName+"_domain",enumMap)
                            if  theEnum is not None:                     
                                print( "not missing domainID: " )
                            else:
                                print( "missing domainID: " )
                                print(domainID)
                                print(classID)
                                if not(domainID in missingDomains):
                                    missingDomains.append(domainID)
                        except:
                                print( "missing ROL class: " )
                                print(classID)    
                                
            for theDomain in missingDomains:
                
                domain_ID_Name = domainToDomainNameMap[theDomain]
                amendedDomainName = SQLDeveloperImport.replaceSpaceWithUnderscore(self,domain_ID_Name)+ "_domain"
                if not( (amendedDomainName == "String") or (amendedDomainName == "Date")  ):
                    theEnum = XEnum()
                    theEnum.name = amendedDomainName 
                    #maintain a map of enum IDS to XEnum objects
                    enumMap[amendedDomainName] = theEnum
                    rpmnPackage.classifiers.extend([theEnum])
                    theDomainMembers= SQLDeveloperImport.getMembersOfTheDomain(self,theDomain, memberIDToDomainMap)
                    counter1 = 0
                    for member in theDomainMembers:
                        enumLiteral = XEnumLiteral()
                        enumUsedName = "m_" + SQLDeveloperImport.replaceSpaceWithUnderscore(self,memberIDToMemberCodeMap[member])
                        adaptedValue = SQLDeveloperImport.replaceSpaceWithUnderscore(self,memberIDToMemberNameMap[member])
                        newAdaptedValue = adaptedValue
                        if SQLDeveloperImport.containsLiteral(self,theEnum.literals, adaptedValue ):
                            newAdaptedValue = adaptedValue +"_x2"
                        if SQLDeveloperImport.containsLiteral(self,theEnum.literals, adaptedValue +"_x2" ):
                            newAdaptedValue = adaptedValue +"_x3"
                        if SQLDeveloperImport.containsLiteral(self,theEnum.literals, adaptedValue +"_x3" ):
                            newAdaptedValue = adaptedValue +"_x4"
                        if SQLDeveloperImport.containsLiteral(self,theEnum.literals, adaptedValue +"_x4" ):
                            newAdaptedValue = adaptedValue +"_x5"
                        if SQLDeveloperImport.containsLiteral(self,theEnum.literals, adaptedValue +"_x5" ):
                            newAdaptedValue = adaptedValue +"_x6"
                        if SQLDeveloperImport.containsLiteral(self,theEnum.literals, adaptedValue +"_x6" ):
                            newAdaptedValue = adaptedValue +"_x7"
                        if SQLDeveloperImport.containsLiteral(self,theEnum.literals, adaptedValue +"_x7" ):
                            newAdaptedValue = adaptedValue +"_x8"
                        if SQLDeveloperImport.containsLiteral(self,theEnum.literals, adaptedValue +"_x8" ):
                            newAdaptedValue = adaptedValue +"_x9"
                        if SQLDeveloperImport.containsLiteral(self,theEnum.literals, adaptedValue +"_x9" ):
                            newAdaptedValue = adaptedValue +"_x10"
                        if SQLDeveloperImport.containsLiteral(self,theEnum.literals, adaptedValue +"_x10" ):
                            newAdaptedValue = adaptedValue +"_x11"
                        if SQLDeveloperImport.containsLiteral(self,theEnum.literals, adaptedValue +"_x11" ):
                            newAdaptedValue = adaptedValue +"_x12"
                        if SQLDeveloperImport.containsLiteral(self,theEnum.literals, adaptedValue +"_x12" ):
                            newAdaptedValue = adaptedValue +"_x13"
                        if SQLDeveloperImport.containsLiteral(self,theEnum.literals, adaptedValue +"_x13" ):
                            newAdaptedValue = adaptedValue +"_x14"
                        if SQLDeveloperImport.containsLiteral(self,theEnum.literals, adaptedValue +"_x14" ):
                            newAdaptedValue = adaptedValue +"_x15"
                        if SQLDeveloperImport.containsLiteral(self,theEnum.literals, adaptedValue +"_x15" ):
                            newAdaptedValue = adaptedValue +"_x16"
                        if SQLDeveloperImport.containsLiteral(self,theEnum.literals, adaptedValue +"_x16" ):
                            newAdaptedValue = adaptedValue +"_x17"
                        if SQLDeveloperImport.containsLiteral(self,theEnum.literals, adaptedValue +"_x17" ):
                            newAdaptedValue = adaptedValue +"_x18"
                        if SQLDeveloperImport.containsLiteral(self,theEnum.literals, adaptedValue +"_x18" ):
                            newAdaptedValue = adaptedValue +"_x19"
                        if SQLDeveloperImport.containsLiteral(self,theEnum.literals, adaptedValue +"_x19" ):
                            newAdaptedValue = adaptedValue +"_x20"
                        if SQLDeveloperImport.containsLiteral(self,theEnum.literals, adaptedValue +"_x20" ):
                            newAdaptedValue = adaptedValue +"_x21"
                        if SQLDeveloperImport.containsLiteral(self,theEnum.literals, adaptedValue +"_x21" ):
                            newAdaptedValue = adaptedValue +"_x22"
                        if SQLDeveloperImport.containsLiteral(self,theEnum.literals, adaptedValue +"_x22" ):
                            newAdaptedValue = adaptedValue +"_x23"
                        if SQLDeveloperImport.containsLiteral(self,theEnum.literals, adaptedValue +"_x23" ):
                            newAdaptedValue = adaptedValue +"_x24"
                        if SQLDeveloperImport.containsLiteral(self,theEnum.literals, adaptedValue +"_x24" ):
                            newAdaptedValue = adaptedValue +"_x25"
                        if SQLDeveloperImport.containsLiteral(self,theEnum.literals, adaptedValue +"_x25" ):
                            newAdaptedValue = adaptedValue +"_x26"
                        if SQLDeveloperImport.containsLiteral(self,theEnum.literals, adaptedValue +"_x26" ):
                            newAdaptedValue = adaptedValue +"_x27"
                        if SQLDeveloperImport.containsLiteral(self,theEnum.literals, adaptedValue +"_x27" ):
                            newAdaptedValue = adaptedValue +"_x28"
                        if SQLDeveloperImport.containsLiteral(self,theEnum.literals, adaptedValue +"_x28" ):
                            newAdaptedValue = adaptedValue +"_x29"
                            
                        newAdaptedName = enumUsedName
                        if SQLDeveloperImport.containsName(self,theEnum.literals, enumUsedName ):
                            newAdaptedName = enumUsedName +"_x2"
                        if SQLDeveloperImport.containsName(self,theEnum.literals, enumUsedName +"_x2" ):
                            newAdaptedName = enumUsedName +"_x3"
                        if SQLDeveloperImport.containsName(self,theEnum.literals, enumUsedName +"_x3" ):
                            newAdaptedName = enumUsedName +"_x4"
                        if SQLDeveloperImport.containsName(self,theEnum.literals, enumUsedName +"_x4" ):
                            newAdaptedName = enumUsedName +"_x5"
                        if SQLDeveloperImport.containsName(self,theEnum.literals, enumUsedName +"_x5" ):
                            newAdaptedName = enumUsedName +"_x6"
                        if SQLDeveloperImport.containsName(self,theEnum.literals, enumUsedName +"_x6" ):
                            newAdaptedName = enumUsedName +"_x7"
                        if SQLDeveloperImport.containsName(self,theEnum.literals, enumUsedName +"_x7" ):
                            newAdaptedName = enumUsedName +"_x8"
                        if SQLDeveloperImport.containsName(self,theEnum.literals, enumUsedName +"_x8" ):
                            newAdaptedName = enumUsedName +"_x9"
                        if SQLDeveloperImport.containsName(self,theEnum.literals, enumUsedName +"_x9" ):
                            newAdaptedName = enumUsedName +"_x10"
                        if SQLDeveloperImport.containsName(self,theEnum.literals, enumUsedName +"_x10" ):
                            newAdaptedName = enumUsedName +"_x11"
                        if SQLDeveloperImport.containsName(self,theEnum.literals, enumUsedName +"_x11" ):
                            newAdaptedName = enumUsedName +"_x12"
                        if SQLDeveloperImport.containsName(self,theEnum.literals, enumUsedName +"_12" ):
                            newAdaptedName = enumUsedName +"_x13"
                        if SQLDeveloperImport.containsName(self,theEnum.literals, enumUsedName +"_x13" ):
                            newAdaptedName = enumUsedName +"_x14"
                        if SQLDeveloperImport.containsName(self,theEnum.literals, enumUsedName +"_x14" ):
                            newAdaptedName = enumUsedName +"_x15"
                        if SQLDeveloperImport.containsName(self,theEnum.literals, enumUsedName +"_x15" ):
                            newAdaptedName = enumUsedName +"_x16"
                        if SQLDeveloperImport.containsName(self,theEnum.literals, enumUsedName +"_x16" ):
                            newAdaptedName = enumUsedName +"_x17"
                        if SQLDeveloperImport.containsName(self,theEnum.literals, enumUsedName +"_x17" ):
                            newAdaptedName = enumUsedName +"_x18"
                        if SQLDeveloperImport.containsName(self,theEnum.literals, enumUsedName +"_x18" ):
                            newAdaptedName = enumUsedName +"_x19"
                        if SQLDeveloperImport.containsName(self,theEnum.literals, enumUsedName +"_x19" ):
                            newAdaptedName = enumUsedName +"_x20"
                        if SQLDeveloperImport.containsName(self,theEnum.literals, enumUsedName +"_x20" ):
                            newAdaptedName = enumUsedName +"_x21"
                        if SQLDeveloperImport.containsName(self,theEnum.literals, enumUsedName +"_x21" ):
                            newAdaptedName = enumUsedName +"_x22"
                        if SQLDeveloperImport.containsName(self,theEnum.literals, enumUsedName +"_x22" ):
                            newAdaptedName = enumUsedName +"_x23"
                        if SQLDeveloperImport.containsName(self,theEnum.literals, enumUsedName +"_x23" ):
                            newAdaptedName = enumUsedName +"_x24"
                        if SQLDeveloperImport.containsName(self,theEnum.literals, enumUsedName +"_x24" ):
                            newAdaptedName = enumUsedName +"_x25"
                        if SQLDeveloperImport.containsName(self,theEnum.literals, enumUsedName +"_x25" ):
                            newAdaptedName = enumUsedName +"_x26"
                        if SQLDeveloperImport.containsName(self,theEnum.literals, enumUsedName +"_x26" ):
                            newAdaptedName = enumUsedName +"_x27"
                        if SQLDeveloperImport.containsName(self,theEnum.literals, enumUsedName +"_x27" ):
                            newAdaptedName = enumUsedName +"_x28"
                        if SQLDeveloperImport.containsName(self,theEnum.literals, enumUsedName +"_x28" ):
                            newAdaptedName = enumUsedName +"_x29"
                        if SQLDeveloperImport.containsName(self,theEnum.literals, enumUsedName +"_x29" ):
                            newAdaptedName = enumUsedName +"_x30"
                        if SQLDeveloperImport.containsName(self,theEnum.literals, enumUsedName +"_x30" ):
                            newAdaptedName = enumUsedName +"_x31"
                        if SQLDeveloperImport.containsName(self,theEnum.literals, enumUsedName +"_x31" ):
                            newAdaptedName = enumUsedName +"_x32"
                        
                        enumLiteral.name = newAdaptedName
                        enumLiteral.literal = newAdaptedValue
                        counter1 = counter1 + 1
                        enumLiteral.value = counter1
                        theEnum.literals.extend([enumLiteral])
                
                # create the meber as a literal , with its name and code and add it to the enum.
            fileLocation = fileDirectory + "\\cube_structure_item.csv"
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
                        amendedAttributeName = SQLDeveloperImport.replaceSpaceWithUnderscore(self,attributeName)
                        variable = row[2]
                       
                        classID = row[0]
                        try: 
                            theClass = classesMap[classID]
                            
                            classIsDerived = True
                                
                            theAttributeName =  amendedAttributeName
           
                            domainID = variableToDomainMap[variable]
                            domain_ID_Name = domainToDomainNameMap[domainID]
                            amendedDomainName = SQLDeveloperImport.replaceSpaceWithUnderscore(self,domain_ID_Name)
                            theEnum =  SQLDeveloperImport.findROLEnum(self,amendedDomainName+"_domain",enumMap)
                            if  theEnum is not None:                     
                                attribute = XAttribute()
    
                                attribute.lowerBound=0
                                attribute.upperBound=1
                                if(theEnum.name == "String"):
                                    attribute.name = theAttributeName
                                    attribute.type = xString
                                elif(theEnum.name.startswith("String_")):
                                    attribute.name = theAttributeName
                                    attribute.type = xString
                                elif(theEnum.name == "Number"):
                                    attribute.name = theAttributeName
                                    attribute.type = xDouble
                                elif(theEnum.name.startswith("Real_")):
                                    attribute.name = theAttributeName
                                    attribute.type = xDouble
                                elif(theEnum.name.startswith("Monetary")):
                                    attribute.name = theAttributeName
                                    attribute.type = xInt
                                elif(theEnum.name.startswith("Non_negative_monetary_amounts_with_2_decimals")): 
                                    attribute.name = theAttributeName
                                    attribute.type = xInt
                                elif(theEnum.name.startswith("Non_negative_integers")): 
                                    attribute.name = theAttributeName
                                    attribute.type = xInt
                                elif(theEnum.name.startswith("All_possible_dates")):
                                    attribute.name = theAttributeName
                                    attribute.type = xDate
                                    
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
                                        operation.type = xString
                                    elif(theEnum.name.startswith("String_")):
                                        operation.name = theAttributeName
                                        operation.type = xString
                                    elif(theEnum.name == "Number"):
                                        operation.name = theAttributeName
                                        operation.type = xDouble
                                    
                                    elif(theEnum.name.startswith("Real_")):
                                        operation.name = theAttributeName
                                        operation.type = xDouble
                                    elif(theEnum.name.startswith("Monetary")): 
                                        operation.name = theAttributeName
                                        operation.type = xInt
                                    elif(theEnum.name.startswith("Non_negative_monetary_amounts_with_2_decimals")): 
                                        operation.name = theAttributeName
                                        operation.type = xInt
                                    elif(theEnum.name.startswith("Non_negative_integers")): 
                                        operation.name = theAttributeName
                                        operation.type = xInt
                                    elif(theEnum.name.startswith("All_possible_dates")):   
                                        operation.name = theAttributeName
                                        operation.type = xDate  
                                    else:
                                        operation.name = theAttributeName
                                        operation.type = theEnum  
            
                                try:
                
                                    theClass = classesMap[classID]
                                    theClass.members.extend([attribute])
                                    if classIsDerived:
                                         theClass.members.extend([operation])
                
                                except:
                                    print( "missing class2: " )
                            else:
                                print( "missing domainID: " )
                                print(domainID)
                                print(classID)
                                if not(domainID in missingDomains):
                                    missingDomains.append(domainID)
                        except:
                                print( "missing ROL class: " )
                                print(classID)                                
                            
        SQLDeveloperImport.saveModelAsXMIFile(self, rpmnPackage, outputDirectory )  
        SQLDeveloperImport.saveModelAsRPMNFile(self, rpmnPackage, outputDirectory ) 

    def  getMembersOfTheDomain  (self,  theDomain, memberIDToDomainMap):
        returnList = []
        for key, value in memberIDToDomainMap.items():
            if value == theDomain:
                returnList.append(key)
        return returnList
    
    def saveModelAsRPMNFile(self, rpmnPackage, outputDirectory ):
        
            f = open(outputDirectory + 'data_model.rpmn', "a",  encoding='utf-8')
            f.write("\t\t package " + rpmnPackage.name + "\r")    
            for classifier in  rpmnPackage.classifiers:
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
                        f.write(" " + theLiteral.name + " as \"" + theLiteral.literal + "\" = " + str(theLiteral.value)  )
                        
                    f.write(" }\r")
                
            f.write("\t\t\ttype Double wraps Double\r")
            f.write("\t\t\ttype String wraps String\r")
            f.write("\t\t\ttype Integer wraps Integer\r")  
            f.write("\t\t\ttype Date wraps Date\r")        
            f.close()
        
        
    def saveModelAsXMIFile(self, rpmnPackage, outputDirectory ):
        # save model as a xmi file
        rset = ResourceSet()

        resource = rset.create_resource(URI(outputDirectory + 'ldm.rpmn'))  # This will create an XMI resource
        resource.append(rpmnPackage)
        resource.save()
    
   
        
    def superclassContainsFeature(self,theSuperClass, attribute) :
        attributes = theSuperClass.members
        contains = False;
        for attribute2 in attributes:
            if (attribute2.name == attribute.name):
                contains = True;

        return contains
    
    def hasMemberCalled(self,theClass,memberName) :
        members = theClass.members
        contains = False;
        for member in members:
            if (member.name == memberName):
                contains = True;

        return contains
    
    def numberofRelationShipsToThisClass(self,sourceClass, targetClass):
        features = sourceClass.members
        counter = 0;
        # do this for relationship attributes only.
        for feature in features:
            if ( isinstance(feature,XReference)):
                featureType = feature.type            
                if (featureType == targetClass):
                    counter = counter+1
            
        return counter
    
    def replaceSpaceWithUnderscore(self,className):
        originalClassName = className
        if(len(className) > 0):
            if((className[0] >= '0') and (className[0] <= '9')) :
                className = "_" + className
        
        newClassName =  className.replace(' ', '_').replace(')', '_').replace('(', '_') \
                .replace(',', '_').replace('\\', '_').replace('/', '_').replace('-', '_').replace(':', '_') \
                .replace('+', '_').replace('.', '_').replace('?', '_').replace('\'', '_').replace('>', '_') \
                .replace('<', '_').replace('\"', '_').replace(';', '_').replace('$', '_').replace('=', '_').replace('#', '_') \
                .replace('&', '_').replace('%', '_').replace('[', '_').replace(']', '_').replace('?', '_').replace('–', '_') \
                .replace( chr(0x2019), '_').replace( chr(65533), '_') \
                .replace(chr(0x2018), '_').replace(chr(0x0060), '_').replace(chr(0x00B4), '_') \
                .replace(chr(0x00E9), 'e').replace(chr(0x00C9), 'E').replace(chr(0x00E8), 'e').replace(chr(0x00EB), 'e').replace(chr(0x00CB), 'E') \
                .replace(chr(0x00CA), 'E').replace(chr(0x00EA), 'e').replace(chr(0x00E7), 'c').replace(chr(0x00FC), 'u') \
                .replace(chr(0x00DA), 'U').replace(chr(0x00FA), 'u').replace(chr(0x00DC), 'U').replace(chr(0x00FC), 'u').replace(chr(0x00F6), 'o') \
                .replace(chr(0x200B), '_').replace(chr(0x202F), '_').replace(chr(0x205F), '_').replace(chr(0x3000), '_') \
                .replace(chr(0x2000), '_').replace(chr(0x2001), '_').replace(chr(0x2002), '_').replace(chr(0x2003), '_') \
                .replace(chr(0x2004), '_').replace(chr(0x2005), '_').replace(chr(0x2006), '_').replace(chr(0x2007), '_') \
                .replace(chr(0x2008), '_').replace(chr(0x2009), '_').replace(chr(0x200A), '_').replace(chr(0x00A0), '_') \
                .replace(chr(0x00ED), 'i').replace(chr(0x00CC), 'I').replace(chr(0x00EC), 'i').replace(chr(0x00CE), 'I').replace(chr(0x00EE), 'i').replace(chr(0x00E4), 'a').replace(chr(0x00E1), 'a').replace(chr(0x00C1), 'A').replace(chr(0x00D6), 'O') \
                .replace(chr(0x00D3), 'O').replace(chr(0x00F3), 'o').replace(chr(0x00D1), 'N').replace(chr(0x00F1), 'n') \
                .replace(chr(0x00DF), 'ss').replace(chr(0x00E0), 'a').replace(chr(0x00C0), 'A').replace(chr(0x00C2), 'A').replace(chr(0x00E2), 'a') \
                .replace(chr(0x00C3), 'A').replace(chr(0x00E3), 'a') \
                .replace(chr(0x00FD), 'y').replace(chr(0x017D), 'Z').replace(chr(0x017E), 'z') \
                .replace(chr(0x0160), 'S').replace(chr(0x0161), 's').replace(chr(0x00D8), 'O').replace(chr(0x00F8), 'o').replace(chr(0x00F4), 'o').replace(chr(0x00D5), 'O').replace(chr(0x00F5), 'o')  \
                .replace(chr(0x00C6), 'AE').replace(chr(0x01E2), 'AE').replace(chr(0x01FC), 'AE')   \
                .replace(chr(0x00E6), 'ae').replace(chr(0x01E3), 'ae').replace(chr(0x01FD), 'ae')    \
                .replace(chr(0x00C5), 'A').replace(chr(0x00E5), 'a')
             
        return newClassName;

    def inEnumBlackList(self,adaptedEnumName):
        #TODO currently investigating why ecore is not likeing these and considering them as duplictes of similarly named enums
        if( (adaptedEnumName == "All_last_days_of_months___YYYY_MM") or
            (adaptedEnumName =="All_last_days_of_quarters___YYYY_MM") or
            (adaptedEnumName == "All_possible_dates_YYYY_MM_DD")  ):

            print(" field in blacklist: " + adaptedEnumName)
            return True
        else:
            return False 
        
    def containsLiteral(self,members,adaptedValue):
        contains = False;
        for eEnumLiteral in members:
            if (eEnumLiteral.literal.lower() == adaptedValue.lower()):
                contains = True

        return contains
    
    def containsName(self,members,adaptedName):
        contains = False;
        for eEnumLiteral in members:
            if (eEnumLiteral.name.lower() == adaptedName.lower()):
                contains = True

        return contains
    
    def getLiteralsForEnumeration(self, domain, membersModule): 
        returnMembersList = []   
        for member in membersModule.members:
            if (member.domain_id == domain):
                returnMembersList.append(member) 
        return returnMembersList
    
    def getDomainForDataType(self,domainDataTypeMap,datatype):
        returnDomain = domainDataTypeMap[datatype]
        if (returnDomain == None):
            returnDomain = DOMAIN()
            returnDomain.name = datatype.name
            returnDomain.data_type[datatype] = returnDomain
            subDomain  = SUBDOMAIN()
            domainMap[datatype.name] = theDomain
            subDomainMap[datatype.name] = theSubDomain
            
        return returnDomain
    
    def getEcoreDataTypeForDataType(self,domainDataTypeMap,datatype,xString):

        return xString
    
    def findROLEnum(self,enumName, enumMap):
        returnVal= None
        for key,value in enumMap.items():
            if value.name.lower() == enumName.lower():
               returnVal =  value
        
        return returnVal
if __name__ == '__main__':
    SQLDeveloperImport().convert('C:\\Users\\Neil\\freebirdtools-develop-oct-22\\git\\efbt\\openregspecs\\python\\resources','C:\\Users\\Neil\\freebirdtools-develop-oct-22\\git\\efbt\\openregspecs\\python\\results\\', False)
    
            
            
