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
import os
import csv
from utils import Utils

from ecore4reg import ELAttribute, ELClass, ELEnum, ELEnumLiteral, ELOperation, ELReference

class LDMImport(object):
    '''
    Documentation for LDMImport
    '''

    def doImport(self, context):
        '''
        import the items from the BIRD LDM csv files
        '''
        LDMImport.addLDMClassesToPackage(self, context)
        LDMImport.setLDMSuperClasses(self, context)
        LDMImport.addLDMEnumsToPackage(self, context)
        LDMImport.addLDMLiteralsToEnums(self, context)
        LDMImport.createLDMTypesMap(self, context)
        LDMImport.addLDMAttributesToClasses(self, context)
        LDMImport.removeLDMAttributesAlreadyInSuperClass(self, context)
        LDMImport.addLDMRelationshipsBetweenClasses(self, context)

    def addLDMClassesToPackage(self, context):
        '''
        for each entity in the LDM, create a class and add it to the package
        '''

        fileLocation = context.fileDirectory + os.sep + "DM_Entities.csv"

        headerSkipped = False
        # Load all the entities from the csv file, make an ELClass per entity,
        # and add the ELClass to the package
        with open(fileLocation,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                # skip the first line which is the header.
                if not headerSkipped:
                    headerSkipped = True
                else:

                    className = row[0]
                    objectID = row[1]
                    engineering_type = row[27]
                    Num_SuperTypeEntity_ID = row[26]

                    alteredClassName = Utils.makeValidID(className)
                    if alteredClassName.endswith("_derived"):
                        xclass = ELClass(name=alteredClassName)
                        xclassTable = ELClass(
                            name=alteredClassName+"_DerivedTable")
                        xclassTable.containedEntityType = xclass
                        containmentReference = ELReference()
                        containmentReference.name = xclass.name+"s"
                        containmentReference.eType = xclass
                        containmentReference.upperBound = -1
                        containmentReference.lowerBound = 0
                        containmentReference.containment = True
                        xclassTable.eStructuralFeatures.append(
                            containmentReference)
                        xclassTableOperation = ELOperation()
                        xclassTableOperation.name = xclass.name+"s"
                        xclassTableOperation.eType = xclass
                        xclassTableOperation.upperBound = -1
                        xclassTableOperation.lowerBound = 0
                        xclassTable.eOperations.append(xclassTableOperation)
                        context.inputLayerEntitiesPackage.eClassifiers.extend([
                                                                              xclass])
                        context.inputLayerEntitiesPackage.eClassifiers.extend([
                                                                              xclassTable])
                    elif (className.startswith("OUTPUT_LAYER_")):
                        xclass = ELClass(name=alteredClassName)

                        context.inputLayerEntitiesPackage.eClassifiers.extend([
                                                                              xclass])

                    else:
                        xclass = ELClass(name=alteredClassName)
                        # of engineering type is single table, as i should be 
                        # for all members of a type
                        # heirarchy, and num_suptype is blank,
                        # then this means that this class is a root
                        # of a type heirarchy....we will set such classes
                        #  to be abstract.
                        if (engineering_type == "Single Table") and (Num_SuperTypeEntity_ID == ""):
                            xclass.abstract = True
                        xclassTable = ELClass(
                            name=alteredClassName+"_BaseTable")
                        containmentReference = ELReference()
                        containmentReference.name = xclass.name+"s"
                        containmentReference.eType = xclass
                        containmentReference.upperBound = -1
                        containmentReference.lowerBound = 0
                        containmentReference.containment = True
                        xclassTable.eStructuralFeatures.append(
                            containmentReference)
                        context.inputLayerEntitiesPackage.eClassifiers.extend([
                                                                              xclass])
                        context.inputLayerEntitiesPackage.eClassifiers.extend([
                                                                              xclassTable])

                    # maintain a map a objectIDs to ELClasses
                    context.classesMap[objectID] = xclass
                    context.tableMap[xclass] = xclassTable

    def setLDMSuperClasses(self, context):
        '''
        for each entity in the LDM, set the superclass of the class
        '''
        fileLocation = context.fileDirectory + os.sep + "DM_Entities.csv"
        headerSkipped = False

        # Where an nxtity has a superclass, set the superclass on the ELClass
        with open(fileLocation,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                # skip the first line which is the header.
                if not headerSkipped:
                    headerSkipped = True
                else:
                    classID = row[1]
                    superclassID = row[25]
                    if not (len(superclassID.strip()) == 0):
                        theclass = context.classesMap[classID]
                        superclass = context.classesMap[superclassID]
                        theclass.superTypes.extend([superclass])

    def addLDMEnumsToPackage(self, context):
        '''
        for each domain in the LDM add an enum to the package
        '''
        fileLocation = context.fileDirectory + os.sep + "DM_Domains.csv"
        headerSkipped = False
        counter = 0
        # Create an ELEnum for each domain, and add it to the ELPackage
        with open(fileLocation,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if not headerSkipped:
                    headerSkipped = True
                else:
                    counter = counter+1
                    enumID = row[0]
                    enumName = row[1]
                    adaptedEnumName = Utils.makeValidID(enumName)+"_domain"
                    if not Utils.inEnumBlackList( adaptedEnumName):
                        theEnum = ELEnum()
                        theEnum.name = adaptedEnumName
                        # maintain a map of enum IDS to ELEnum objects
                        context.enumMap[enumID] = theEnum
                        context.inputLayerEnumsPackage.eClassifiers.extend([
                                                                           theEnum])

    def addLDMLiteralsToEnums(self, context):
        '''
        for each memebr of a domain the LDM, add a literal to the corresponding enum
        '''
        fileLocation = context.fileDirectory + os.sep + "DM_Domain_AVT.csv"
        headerSkipped = False
        counter = 0
        # Add the members of a domain as literals of the related Enum
        with open(fileLocation,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if not headerSkipped:
                    headerSkipped = True
                else:
                    try:
                        counter = counter+1
                        enumID = row[0]
                        enumUsedName = Utils.makeValidID( row[3])
                        # enumName = row[5]
                        adaptedEnumName = Utils.makeValidID( enumUsedName)
                        value = row[4]
                        adaptedValue = Utils.makeValidID( value)
                        try:
                            theEnum = context.enumMap[enumID]
                            newAdaptedValue = Utils.uniqueValue(
                                 theEnum, adaptedValue)
                            newAdaptedName = Utils.uniqueName(
                                 theEnum, adaptedEnumName)
                            enumLiteral = ELEnumLiteral()
                            enumLiteral.name = newAdaptedValue
                            enumLiteral.literal = newAdaptedName
                            enumLiteral.value = counter
                            theEnum.eLiterals.extend([enumLiteral])

                        except KeyError:
                            print("missing domain: " + enumID)

                    except IndexError:
                        print(
                            "row in DM_Domain_AVT.csv skipped  due to improper formatting at row number")
                        print(counter)

    def createLDMTypesMap(self, context):
        '''
        for each type in the LDM, create a type in the ELPackage
        '''
        # for each logicalDatatype for orcle 12c, make a Datatype if we have an
        # equivalent

        fileLocation = context.fileDirectory + os.sep + "DM_Logical_To_Native.csv"
        headerSkipped = False
        with open(fileLocation,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if not headerSkipped:
                    headerSkipped = True
                else:
                    rdbms_Type = row[3]
                    rdbms_Version = row[4]
                    dataTypeID = row[0]
                    if (rdbms_Type.strip() == "Oracle Database") and (rdbms_Version.strip() == "12cR2"):
                        native_type = row[2]

                        if native_type.strip() == "VARCHAR":

                            context.datatypeMap[dataTypeID] = context.xString

                        if native_type.strip() == "VARCHAR2":

                            context.datatypeMap[dataTypeID] = context.xString

                        if native_type.strip() == "INTEGER":

                            context.datatypeMap[dataTypeID] = context.xInt

                        if native_type.strip() == "DATE":

                            context.datatypeMap[dataTypeID] = context.xDate

                        if native_type.strip() == "NUMBER":

                            context.datatypeMap[dataTypeID] = context.xDouble

                        if native_type.strip() == "UNKNOWN":

                            context.datatypeMap[dataTypeID] = context.xString

    def addLDMAttributesToClasses(self, context):
        '''
        For each attribute on an entity of the LDM, add an attribute
        to the relevant class in the package
        '''

        fileLocation = context.fileDirectory + os.sep + "DM_Attributes.csv"
        headerSkipped = False
        # For each attribute add an ELAttribute to the correct ELClass representing the Entity
        # the attribute should have the correct type, which may be a specific
        # enumeration

        with open(fileLocation,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if not headerSkipped:
                    headerSkipped = True
                else:
                    attributeName = row[0]
                    amendedAttributeName = Utils.makeValidID(
                         attributeName)
                    attributeKind = row[7]

                    classID = row[4]
                    relationID = row[32]
                    primary_key_or_not = row[35]
                    theClass = context.classesMap[classID]

                    classIsDerived = False
                    if theClass.name.endswith("_derived"):
                        classIsDerived = True

                    theAttributeName = amendedAttributeName

                    # we only add attributes here if they are not representing a relationship
                    if relationID == "":

                        if attributeKind == "Domain":
                            enumID = row[12]
                            theEnum = context.enumMap[enumID]

                            attribute = ELAttribute()
                            if primary_key_or_not == "P":
                                attribute.iD = True

                            attribute.lowerBound = 0
                            attribute.upperBound = 1
                            if theEnum.name == "String":
                                attribute.name = theAttributeName
                                attribute.eType = context.xString
                                attribute.eAttributeType = context.xString
                            elif theEnum.name.startswith("String_"):
                                attribute.name = theAttributeName
                                attribute.eType = context.xString
                                attribute.eAttributeType = context.xString
                            elif theEnum.name == "Number":
                                attribute.name = theAttributeName
                                attribute.eType = context.xDouble
                                attribute.eAttributeType = context.xDouble
                            elif theEnum.name.startswith("Real_"):
                                attribute.name = theAttributeName
                                attribute.eType = context.xDouble
                                attribute.eAttributeType = context.xDouble
                            elif theEnum.name.startswith("Monetary"):
                                attribute.name = theAttributeName
                                attribute.eType = context.xInt
                                attribute.eAttributeType = context.xInt
                            elif theEnum.name.startswith("Non_negative_monetary_amounts_with_2_decimals"):
                                attribute.name = theAttributeName
                                attribute.eType = context.xInt
                                attribute.eAttributeType = context.xInt
                            elif theEnum.name.startswith("Non_negative_integers"):
                                attribute.name = theAttributeName
                                attribute.eType = context.xInt
                                attribute.eAttributeType = context.xInt
                            elif theEnum.name.startswith("All_possible_dates"):
                                attribute.name = theAttributeName
                                attribute.eType = context.xDate
                                attribute.eAttributeType = context.xDate

                            # This is a common domain used for String identifiers in BIRD in SQLDeveloper

                            else:
                                attribute.name = theAttributeName
                                attribute.eType = theEnum
                                attribute.eAttributeType = theEnum

                            if classIsDerived:
                                operation = ELOperation()
                                operation.lowerBound = 0
                                operation.upperBound = 1
                                if theEnum.name == "String":
                                    operation.name = theAttributeName
                                    operation.eType = context.xString
                                elif theEnum.name.startswith("String_"):
                                    operation.name = theAttributeName
                                    operation.eType = context.xString
                                elif theEnum.name == "Number":
                                    operation.name = theAttributeName
                                    operation.eType = context.xDouble

                                elif theEnum.name.startswith("Real_"):
                                    operation.name = theAttributeName
                                    operation.eType = context.xDouble
                                elif theEnum.name.startswith("Monetary"):
                                    operation.name = theAttributeName
                                    operation.eType = context.xInt
                                elif theEnum.name.startswith("Non_negative_monetary_amounts_with_2_decimals"):
                                    operation.name = theAttributeName
                                    operation.eType = context.xInt
                                elif theEnum.name.startswith("Non_negative_integers"):
                                    operation.name = theAttributeName
                                    operation.eType = context.xInt
                                elif theEnum.name.startswith("All_possible_dates"):
                                    operation.name = theAttributeName
                                    operation.eType = context.xDate
                                else:
                                    operation.name = theAttributeName
                                    operation.eType = theEnum

                        if (attributeKind == "Logical Type"):
                            dataTypeID = row[14]
                            try:

                                attribute = ELAttribute()
                                attribute.lowerBound = 0
                                attribute.upperBound = 1
                                attribute.name = amendedAttributeName
                                attribute.eType = Utils.getEcoreDataTypeForDataType(
                                    self)
                                attribute.eAttributeType = Utils.getEcoreDataTypeForDataType(
                                    self)

                                if classIsDerived:
                                    operation = ELOperation()
                                    operation.lowerBound = 0
                                    operation.upperBound = 1
                                    operation.name = amendedAttributeName
                                    operation.eType = Utils.getEcoreDataTypeForDataType(
                                        self)

                            except KeyError:
                                print("missing datatype: ")
                                print(dataTypeID)

                        try:

                            theClass = context.classesMap[classID]
                            theClass.eStructuralFeatures.extend([attribute])
                            if classIsDerived:
                                theClass.eOperations.extend([operation])

                        except:
                            print("missing class2: ")
                            print(classID)

    def removeLDMAttributesAlreadyInSuperClass(self, context):
        '''
        if we already have created an attribute in both a subclass and a superclass
        then we delete it in the subclass
        '''
        for theClass in context.classesMap.values():
            if len(theClass.superTypes) > 0:
                superclass = theClass.superTypes[0]
                if superclass:

                    attributes = theClass.eStructuralFeatures
                    attributesToDelete = []
                    for theAttribute in attributes:
                        if Utils.superclassContainsFeature( superclass, theAttribute):
                            attributesToDelete.append(theAttribute)

                    for theAttribute in attributesToDelete:
                        theClass.eStructuralFeatures.remove(theAttribute)


    def addLDMRelationshipsBetweenClasses(self, context):
        '''
        For each relationship in the LDM, add a reference between the relevant classes
        '''
        fileLocation = context.fileDirectory + os.sep + "DM_Relations.csv"
        headerSkipped = False
        with open(fileLocation,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if not headerSkipped:
                    headerSkipped = True
                else:
                    sourceID = row[16]
                    targetID = row[18]
                    sourceTo_Target_Cardinality = row[10]
                    targetClassName = row[7]
                    target_Optional = row[13]

                    referenceName = "the" + \
                        Utils.makeValidID(targetClassName)

                    try:
                        theClass = context.classesMap[sourceID]
                    except KeyError:
                        print("missing class1: " + sourceID)

                    try:
                        targetClass = context.classesMap[targetID]
                    except KeyError:
                        print("missing target class: " + targetID)

                    numOfRelations = Utils.numberofRelationShipsToThisClass(
                        theClass, targetClass)
                    if numOfRelations > 0:
                        referenceName = referenceName + str(numOfRelations)

                    if target_Optional.strip() == "Y":
                        if sourceTo_Target_Cardinality.strip() == "*":
                            referenceName = referenceName + "s"
                            eReference = ELReference()
                            eReference.name = referenceName
                            eReference.eType = targetClass
                            # upper bound of -1 means there is no upper bounds, 
                            # so represents an open list of reference
                            eReference.upperBound = -1
                            eReference.lowerBound = 0
                            eReference.containment = False
                            if theClass.name.endswith("_derived"):
                                theSourceTable = context.tableMap[theClass]
                                theTargetTable = context.tableMap[targetClass]
                                if not Utils.hasMemberCalled( theSourceTable, "sourceTable1"):

                                    sourceTablesReference = ELReference()
                                    sourceTablesReference.name = "sourceTable1"
                                    sourceTablesReference.eType = theTargetTable
                                    sourceTablesReference.upperBound = -1
                                    sourceTablesReference.lowerBound = 0
                                    sourceTablesReference.containment = False
                                    theSourceTable.eStructuralFeatures.append(
                                        sourceTablesReference)
                                else:

                                    sourceTablesReference = ELReference()
                                    sourceTablesReference.name = "sourceTable2"
                                    sourceTablesReference.eType = theTargetTable
                                    sourceTablesReference.upperBound = -1
                                    sourceTablesReference.lowerBound = 0
                                    sourceTablesReference.containment = False
                                    theSourceTable.eStructuralFeatures.append(
                                        sourceTablesReference)
                        else:
                            eReference = ELReference()
                            eReference.name = referenceName
                            eReference.eType = targetClass
                            eReference.upperBound = 1
                            eReference.lowerBound = 0
                            eReference.containment = False
                            if theClass.name.endswith("_derived"):
                                theSourceTable = context.tableMap[theClass]
                                theTargetTable = context.tableMap[targetClass]
                                if not Utils.hasMemberCalled(theSourceTable, "sourceTable1"):

                                    sourceTablesReference = ELReference()
                                    sourceTablesReference.name = "sourceTable1"
                                    sourceTablesReference.eType = theTargetTable
                                    sourceTablesReference.upperBound = -1
                                    sourceTablesReference.lowerBound = 0
                                    sourceTablesReference.containment = False
                                    theSourceTable.eStructuralFeatures.append(
                                        sourceTablesReference)
                                else:

                                    sourceTablesReference = ELReference()
                                    sourceTablesReference.name = "sourceTable2"
                                    sourceTablesReference.eType = theTargetTable
                                    sourceTablesReference.upperBound = -1
                                    sourceTablesReference.lowerBound = 0
                                    sourceTablesReference.containment = False
                                    theSourceTable.eStructuralFeatures.append(
                                        sourceTablesReference)
                    else:
                        if sourceTo_Target_Cardinality.strip() == "*":
                            referenceName = referenceName + "s"
                            eReference = ELReference()
                            eReference.name = referenceName
                            eReference.eType = targetClass
                            eReference.upperBound = -1
                            eReference.lowerBound = 1
                            eReference.containment = False
                            if theClass.name.endswith("_derived"):

                                theSourceTable = context.tableMap[theClass]
                                theTargetTable = context.tableMap[targetClass]
                                if not Utils.hasMemberCalled(theSourceTable, "sourceTable1"):

                                    sourceTablesReference = ELReference()
                                    sourceTablesReference.name = "sourceTable1"
                                    sourceTablesReference.eType = theTargetTable
                                    sourceTablesReference.upperBound = -1
                                    sourceTablesReference.lowerBound = 0
                                    sourceTablesReference.containment = False
                                    theSourceTable.eStructuralFeatures.append(
                                        sourceTablesReference)
                                else:
                                    sourceTablesReference = ELReference(
                                        "sourceTable2", theTargetTable, upper=-1, lower=0, containment=False)
                                    theSourceTable.eStructuralFeatures.append(
                                        sourceTablesReference)
                        else:
                            eReference = ELReference()
                            eReference.name = referenceName
                            eReference.eType = targetClass
                            eReference.upperBound = 1
                            eReference.lowerBound = 1
                            eReference.containment = False
                            if theClass.name.endswith("_derived"):
                                theSourceTable = context.tableMap[theClass]
                                theTargetTable = context.tableMap[targetClass]
                                if not (Utils.hasMemberCalled(theSourceTable, "sourceTable1")):

                                    sourceTablesReference = ELReference()
                                    sourceTablesReference.name = "sourceTable1"
                                    sourceTablesReference.eType = theTargetTable
                                    sourceTablesReference.upperBound = -1
                                    sourceTablesReference.lowerBound = 0
                                    sourceTablesReference.containment = False
                                    theSourceTable.eStructuralFeatures.append(
                                        sourceTablesReference)
                                else:

                                    sourceTablesReference = ELReference()
                                    sourceTablesReference.name = "sourceTable2"
                                    sourceTablesReference.eType = theTargetTable
                                    sourceTablesReference.upperBound = -1
                                    sourceTablesReference.lowerBound = 0
                                    sourceTablesReference.containment = False
                                    theSourceTable.eStructuralFeatures.append(
                                        sourceTablesReference)
                    if not (theClass is None):
                        theClass.eStructuralFeatures.append(eReference)
