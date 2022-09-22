#
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
        
    
    
    def convert (self,fileDirectory,outputDirectory):  
        
        xString = XDataType()
        xString.name = "String"
        
        xDouble = XDataType()
        xDouble.name = "Double"
    
        moduleList = ModuleList() 
        dataModel = EntityModule()
        dataModel.name="ors"
        rpmnPackage = XPackage(name='pack')
        dataModel.package = rpmnPackage

        moduleList.modules.extend([dataModel])

        rpmnPackage.classifiers.append(xString)
        rpmnPackage.classifiers.append(xDouble)
        classesMap = {}
        fileLocation = fileDirectory + "\\DM_Entities.csv"
        enumMap = {}
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
                    # of engineering type is single table, as i should be for all members of a type
                    # heirarchy, and num_suptype is blanck, then this means that this class is a root
                    # of a type heirarchy....we will set such classes to be abstract.
                    alteredClassName = SQLDeveloperImport.replaceSpaceWithUnderscore(self,className);                    
                    xclass = XClass(name=alteredClassName)      
                    if((engineering_type == "Single Table") and (Num_SuperTypeEntity_ID=="")   )    :
                       xclass.abstract=True       
                    rpmnPackage.classifiers.extend([xclass])
                    # maintain a map a objectIDs to XClasses
                    classesMap[objectID]=xclass
                    
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
                    adaptedEnumName = SQLDeveloperImport.replaceSpaceWithUnderscore(self,enumName)
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
                        enumName = row[5]
                        adaptedEnumName = SQLDeveloperImport.replaceSpaceWithUnderscore(self,enumName)
                        value = row[4]
                        adaptedValue = SQLDeveloperImport.replaceSpaceWithUnderscore(self,value)
                        try:
                            theEnum = enumMap[enumID]
                            enumLiteral = XEnumLiteral()
                            enumLiteral.name = adaptedValue
                            enumLiteral.literal = adaptedValue
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

                            datatypeMap[dataTypeID] = EString
                      
                        if (native_type.strip() == "VARCHAR2") :

                            datatypeMap[dataTypeID] = EString
                      
                        if (native_type.strip() == "INTEGER") :

                            datatypeMap[dataTypeID] = EInt
                      
                        if (native_type.strip() == "DATE") :

                            datatypeMap[dataTypeID] = EDate
                        
                        if (native_type.strip() == "NUMBER") :

                            datatypeMap[dataTypeID] = EDouble
                        
                        if (native_type.strip() == "UNKNOWN") :

                            datatypeMap[dataTypeID] = EString
             
        
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
                    print(attributeName)
                    amendedAttributeName = SQLDeveloperImport.replaceSpaceWithUnderscore(self,attributeName);
                    attributeKind = row[7]
                   
                    classID = row[4]
                    relationID = row[32]
                    theClass = classesMap[classID]
                    theAttributeName =  amendedAttributeName
                    
                    # we only add attributes here if they are not representing a relationship
                    if relationID == "":
                    
                        if (attributeKind == "Domain"):
                            enumID = row[12]
                            theEnum = enumMap[enumID]
                            print("Domain")
                            attribute = XAttribute()
                            attribute.lowerBound=0
                            attribute.upperBound=1
                            if(theEnum.name == "String"):
                                attribute.name = theAttributeName
                                attribute.type = xString
                                
                            elif(theEnum.name == "Number"):
                                attribute.name = theAttributeName
                                attribute.type = xDouble
                            # This is a common domain used for String identifiers in BIRD in SQLDeveloper
                            elif(theEnum.name == "String_up_to_60_characters_limited_to_letters__capital_and_low_cases___numbers__dash_and_underscore_1"):
                                attribute.name = theAttributeName
                                attribute.type = xString
                                    
                            else:
                                attribute.name = theAttributeName
                                attribute.type = theEnum            
    
                        if (attributeKind == "Logical Type"):
                            print("Logical Type")
                            dataTypeID = row[14]
                            try:
                                datatype = datatypeMap[dataTypeID]
                                attribute = XAttribute()
                                attribute.lowerBound=0
                                attribute.upperBound=1
                                attribute.name =amendedAttributeName
                                attribute.type = SQLDeveloperImport.getEcoreDataTypeForDataType(self,datatype)
                            except KeyError:
                                print("missing datatype: ")
                                print(dataTypeID)                       
    
                        
    
                        try:
    
                            theClass = classesMap[classID]
                            theClass.members.extend([attribute])
    
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
                        else:
                            eReference = XReference(referenceName, targetClass, upper=1, lower=0, containment=False)                 
                    else:
                        if (sourceTo_Target_Cardinality.strip() == "*"):
                            referenceName = referenceName + "s"                       
                            eReference  = XReference()
                            eReference.name=referenceName
                            eReference.type=targetClass
                            eReference.upperBound = -1
                            eReference.lowerBound=1
                            eReference.containment= False                        
                        else:      
                            eReference  = XReference()
                            eReference.name=referenceName
                            eReference.type=targetClass
                            eReference.upperBound = 1
                            eReference.lowerBound=1
                            eReference.containment= False
                    if (not (theClass is None) ) :                 
                        theClass.members.append(eReference)
                   
        SQLDeveloperImport.saveModelAsXMIFile(self, moduleList, outputDirectory )  
        SQLDeveloperImport.saveModelAsRPMNFile(self, moduleList, outputDirectory ) 

        
    def saveModelAsRPMNFile(self, moduleList, outputDirectory ):
        f = open(outputDirectory + 'data_model.rpmn', "a")
        f.write("ModuleList{\r")
        f.write("\t modules{ \r")
        for datamodel in moduleList.modules:
            
            f.write("\t\t EntityModule " + datamodel.name +"  { \r")
            f.write("\t\t package " + datamodel.package.name + "\r")    
            for classifier in  datamodel.package.classifiers:
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
                            f.write("\t\t\t\t" + member.type.name + " " )
                            if member.upperBound == -1:
                                f.write("[] ")
                            elif ( (member.lowerBound == 0) and (member.upperBound == 1)):
                                f.write(" ")
                            else:
                                f.write("[" + str(member.lowerBound) + ".." +str(member.upperBound) + "] ")
                            f.write(member.name)
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
            f.write("\t\t  }\r")
            
        f.write("\t  }\r")
        f.write("  }\r")
        f.close()
        
        
    def saveModelAsXMIFile(self, openRegSpecs, outputDirectory ):
        # save model as a xmi file
        rset = ResourceSet()
        print("openRegSpecs")
        print(openRegSpecs)
        print("openRegSpecs")
        resource = rset.create_resource(URI(outputDirectory + 'ldm.rpmn'))  # This will create an XMI resource
        resource.append(openRegSpecs)
        resource.save()
    
   
        
    def superclassContainsFeature(self,theSuperClass, attribute) :
        attributes = theSuperClass.members
        contains = False;
        for attribute2 in attributes:
            if (attribute2.name == attribute.name):
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
                .replace('&', '_').replace('%', '_').replace('[', '_').replace(']', '_') \
                .replace( chr(0x2019), '_').replace( chr(65533), '_') \
                .replace(chr(0x2018), '_').replace(chr(0x0060), '_').replace(chr(0x00B4), '_')
             
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
            if (eEnumLiteral.name == adaptedValue):
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
    
    def getEcoreDataTypeForDataType(self,domainDataTypeMap,datatype):

        return xString
        
if __name__ == '__main__':
    SQLDeveloperImport().convert('C:\\Users\\Neil\\freebirdtools-develop-presep\\git\\efbt\\openregspecs\\python\\resources','C:\\Users\\Neil\\freebirdtools-develop-presep\\git\\efbt\\openregspecs\\python\\results\\')
    
            
            
