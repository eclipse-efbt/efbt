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
        # baseEntityModule = BaseEntityModule()
        # derivedEntityModule = DerivedEntityModule()
        # generatedEntityModule = GeneratedEntityModule()
        # baseEntityModule.name = 'baseEntityModule'
        # derivedEntityModule.name = 'derivedEntityModule'
        # generatedEntityModule.name = 'generatedEntityModule'
        moduleList.modules.extend([dataModel])
        # dataModel.baseEntityModule = baseEntityModule
        # dataModel.derivedEntityModule = derivedEntityModule
        # dataModel.generatedEntityModule = generatedEntityModule
        
        # smcubesCoreModel = SMCubesCoreModel()
        # smcubesCoreModel.name = "SMCubesCoreModel1"
        # openRegSpecs.types_and_concepts = smcubesCoreModel
        rpmnPackage.classifiers.append(xString)
        rpmnPackage.classifiers.append(xDouble)
        classesMap = {}
        fileLocation = fileDirectory + "\\DM_Entities.csv"
        enumMap = {}
        headerSkipped = False
        with open(fileLocation) as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                # skip the first line which is the header.
                if (not headerSkipped):
                    headerSkipped = True
                else:
                    className = row[0];
                    objectID = row[1];
                    alteredClassName = SQLDeveloperImport.replaceSpaceWithUnderscore(self,className);
                    if(alteredClassName.endswith("_derived")):
                        eclass = XClass(name=alteredClassName)     
                        rpmnPackage.classifiers.extend([eclass])
                    elif(className.startswith("OUTPUT_LAYER_")):
                        eclass = XClass(name=alteredClassName)
                        
                        rpmnPackage.classifiers.extend([eclass])
                      
                    else:
                        eclass = XClass(name=alteredClassName)                   
                        rpmnPackage.classifiers.extend([eclass])

                    # eclass.name = alteredClassName
                    print("objectID")
                    print(objectID)
                    classesMap[objectID]=eclass
        fileLocation = fileDirectory + "\\DM_Entities.csv"
        headerSkipped = False
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
                        enumMap[enumID] = theEnum
                        rpmnPackage.classifiers.extend([theEnum])
                        

        fileLocation = fileDirectory + "\\DM_Domain_AVT.csv"
        headerSkipped = False
        counter = 0
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
        #ecorePackage = EcoreFactory.eINSTANCE.getEcorePackage();
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
             
        print("datatypeMap")
        print(datatypeMap)
        # Add all the concepts to an EPackage
        # for each attribute add an attribute to the correct Entity
        # the attribute should have the correct type, which may be a specific
        # enumeration

        fileLocation = fileDirectory + "\\DM_Attributes.csv"
        headerSkipped = False
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
                    theClass = classesMap[classID]
                    theAttributeName =  amendedAttributeName
                    

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
                        elif(theEnum.name == "String_up_to_60_characters_limited_to_letters__capital_and_low_cases___numbers__dash_and_underscore_1"):
                            attribute.name = theAttributeName
                            attribute.type = xString
                                
                        else:
                            attribute.name = theAttributeName
                            attribute.type = theEnum
                        # attribute.concept = variable
                        # attribute.eAttributeType = theEnum
                   

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
                        print("attribute")
                        print(attribute)
                        print("classesMap")
                        print(classesMap)
                        
                        theClass = classesMap[classID]
                        print("theClass")
                        print(theClass)
                  
                        print("attribute.eAttributeType")
                        # print(attribute.eAttributeType)
                        print("attribute.name")
                        print(attribute.name)
                        theClass.members.extend([attribute])
                        print("theClass.members")
                        print(theClass.members)
                    except:
                        print( "missing class2: " )
                        print(classID)
                        
        #remove any attributes that already exist in superclass.

        for theClass in classesMap.values():
            if len(theClass.superTypes) > 0:
                superclass = theClass.superTypes[0]
                if (superclass):
                    
                    attributes = theClass.members
                    print("theClass.members")
                    print(theClass.members)
                    attributesToDelete = []
                    for theAttribute in attributes :
                        if SQLDeveloperImport.superclassContainsFeature(self,superclass, theAttribute):
                            attributesToDelete.append(theAttribute);
    
                    for theAttribute in attributesToDelete :
                        theClass.members.remove(theAttribute);
                        print( "removed eStructuralFeature since it exists in the superclass")
                        print(  theAttribute.name )
               
          
        #for each relationship add a reference
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

                    f.write("\t\t\tclass " + classifier.name)
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
                        f.write(" " + theLiteral.name + " as" + theLiteral.literal + " = " + str(theLiteral.value)  )
                        
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
        #TODO Auto-generated method stub
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
        
        #if(not (originalClassName == newClassName)) :
        #    print( " replaced identifier " + originalClassName +  " with " + newClassName +"\n")
        
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
    SQLDeveloperImport().convert('C:\\Users\\Neil\\freebirdtools-develop-jun22\\git\\efbt\\openregspecs\\python\\resources','C:\\Users\\Neil\\freebirdtools-develop-jun22\\git\\efbt\\openregspecs\\python\\results\\')
    
            
            
