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
from open_reg_specs import EntityModule, Entity, DerivedEntity, GeneratedEntity, BasicEntity,Attribute,OneToOneRelationshipAttribute,OneToManyRelationshipAttribute,RelationshipAttribute, MEMBER, DOMAIN, FACET_VALUE_TYPE, SUBDOMAIN,VARIABLE,  DomainModule, OpenRegSpecs, SMCubesCoreModel, MemberModule, VariableModule,SubDomainModule
from pyecore.resources import ResourceSet, URI
from pyecore.ecore import *
from pyecore.resources.xmi import XMIResource
from pyecore.resources.xmi import XMIOptions

import csv
class SQLDeveloperImport(object):
        
    def convert (self,fileDirectory,outputDirectory):  
        openRegSpecs = OpenRegSpecs() 
        entityModule = EntityModule( nsURI='"http://www.eclipse.org/open_reg_specs"', nsPrefix='open_reg_specs')
        entityModule.name = 'open_reg_specs'
        openRegSpecs.data_model.extend([entityModule])
        
        smcubesCoreModel = SMCubesCoreModel()
        smcubesCoreModel.name = "SMCubesCoreModel1"
        openRegSpecs.types_and_concepts = smcubesCoreModel
        classesMap = {}
        fileLocation = fileDirectory + "\\DM_Entities.csv"
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
                        eclass = DerivedEntity()
                    elif(className.startswith("OUTPUT_LAYER_")):
                        eclass = GeneratedEntity()
                    else:
                        eclass = BasicEntity()
                    eclass.name = alteredClassName
                    entityModule.entities.extend([eclass])
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
                        theclass.superClass= superclass
                        #print("theclass")
                        print(theclass)
                        #print("theclass.eSuperTypes")
                        print('superclass = ' + theclass.superClass.name)

        domainMap = {}
        subDomainMap = {}
        variableMap = {}
        fileLocation = fileDirectory + "\\DM_Domains.csv"
        headerSkipped = False
        counter = 0
        domainsModule = DomainModule()
        domainsModule.name = "domainsModule"
        membersModule = MemberModule()
        membersModule.name = "membersModule"
        variablesModule = VariableModule()
        variablesModule.name = "variablesModule"
        subDomainsModule = SubDomainModule()
        subDomainsModule.name = "subDomainsModule"
        
        smcubesCoreModel.variableModules.extend([variablesModule])
        smcubesCoreModel.domainModules.extend([domainsModule])
        smcubesCoreModel.memberModules.extend([membersModule])
        smcubesCoreModel.subDomainModules.extend([subDomainsModule])
        
        
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
                    print(SQLDeveloperImport.inEnumBlackList(self,adaptedEnumName))
                    if(not SQLDeveloperImport.inEnumBlackList(self,adaptedEnumName)):
                        theDomain = DOMAIN()
                        theDomain.name = adaptedEnumName+"_Domain"
                        theSubDomain = SUBDOMAIN()
                        theSubDomain.name = adaptedEnumName+"_SubDomain"
                        theSubDomain.domain_id = theDomain
                        subDomainsModule.subdomains.extend([theSubDomain])
                        domainsModule.domains.extend([theDomain])
                        domainMap[enumID] = theDomain
                        subDomainMap[enumID] = theSubDomain

                        
                        
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
                            theDomain = domainMap[enumID]
                            members = SQLDeveloperImport.getLiteralsForEnumeration(self,theDomain,membersModule)
                            #if the member does not exist already, then add it
                            if (not SQLDeveloperImport.containsLiteral(self,members, adaptedValue)):
                                member = MEMBER()
                                member.name = adaptedValue
                                member.domain_id = theDomain
                                #member.value = counter
                                membersModule.members.extend([member])
                                
                        except KeyError:
                            print( "missing domain: " + enumID )
                            
                        members = SQLDeveloperImport.getLiteralsForEnumeration(self,theDomain,membersModule)
                        #if the member does not exist already, then add it
                        if (not SQLDeveloperImport.containsLiteral(self,members, adaptedValue)):
                            member = MEMBER()
                            member.name = adaptedValue
                            #member.value = counter;
                            members.extend([member])
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

                            datatypeMap[dataTypeID] = FACET_VALUE_TYPE.String
                      
                        if (native_type.strip() == "VARCHAR2") :

                            datatypeMap[dataTypeID] = FACET_VALUE_TYPE.String
                      
                        if (native_type.strip() == "INTEGER") :

                            datatypeMap[dataTypeID] = FACET_VALUE_TYPE.BigInteger
                      
                        if (native_type.strip() == "DATE") :

                            datatypeMap[dataTypeID] = FACET_VALUE_TYPE.DateTime
                        
                        if (native_type.strip() == "NUMBER") :

                            datatypeMap[dataTypeID] = FACET_VALUE_TYPE.Double
                        
                        if (native_type.strip() == "UNKNOWN") :

                            datatypeMap[dataTypeID] = FACET_VALUE_TYPE.String
             
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
                    amendedAttributeName = SQLDeveloperImport.replaceSpaceWithUnderscore(self,attributeName);
                    attributeKind = row[7]
                    attribute = Attribute()
                    attribute.name = amendedAttributeName
                    

                    if (attributeKind == "Domain"):
                        domainID = row[12]
                        theDomain = domainMap[domainID]
                        theDomain.is_enumerated = True
                        theSubDomain = subDomainMap[domainID]
                        attribute = Attribute()
                        attribute.name = amendedAttributeName
                        try:
                            variable = variableMap[amendedAttributeName]
                        except KeyError:
                            variable  = VARIABLE()
                            variable.name = amendedAttributeName + "_variable"
                            variable.domain_id = theDomain
                            variableMap[amendedAttributeName] = variable
                            variablesModule.variables.extend([variable])
                        # attribute.concept = variable
                        attribute.type = theSubDomain
                   

                    if (attributeKind == "Logical Type"):
                        dataTypeID = row[14]
                        try:
                            datatype = datatypeMap[dataTypeID]
                            attribute = EAttribute()
                            attribute.name =amendedAttributeName
                            try:
                                variable = variableMap[amendedAttributeName]
                            except KeyError:
                                variable  = VARIABLE()
                                variable.name = amendedAttributeName + "_variable"
                                variable.domain_id = theDomain
                                variableMap[amendedAttributeName] = variable
                                variablesModule.variables.extend([variable])
                            theDomain = SQLDeveloperImport.getDomainForDataType(self,domainDataTypeMap,datatype)
                            theSubDomain = subDomainMap[datatype.name]
                            variable.domain_id = theDomain
                            variablesModule.variables.extend([variable])
                            # attribute.concept = variable
                            attribute.type = theSubDomain
                        except KeyError:
                            print("missing datatype: ")
                            print(dataTypeID)                       

                    classID = row[4]

                    try:
                        theClass = classesMap[classID]
                        attribute.name=theClass.name+"_"+ attribute.name
                        theClass.attributes.extend([attribute])
                    except:
                        print( "missing class2: " )
                        print(classID)
                        
        #remove any attributes that already exist in superclass.

        for theClass in classesMap.values():
            if (isinstance(theClass, BasicEntity)):
                superclass = theClass.superClass
                if (superclass):
                    
                    attributes = theClass.attributes
                    attributesToDelete = []
                    for theAttribute in attributes :
                        if SQLDeveloperImport.superclassContainsFeature(self,superclass, theAttribute):
                            attributesToDelete.append(theAttribute);
    
                    for theAttribute in attributesToDelete :
                        theClass.attributes.remove(theAttribute);
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
                     # if the reference name already exists on this EClass
                    # then add a number  the end like 2 if there is already 1 reference
                    # or 3 if there already exists 2 other references to this class
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
                        referenceName = referenceName + str(numOfRelations)
                    relationalAttribute = None
                    if (target_Optional.strip() == "Y"):
                        if (sourceTo_Target_Cardinality.strip() == "*"):
                            referenceName = referenceName + "s"
                            relationalAttribute = OneToManyRelationshipAttribute()
                            relationalAttribute.name = referenceName
                            relationalAttribute.entity = targetClass
                            relationalAttribute.containment = False
                            relationalAttribute.mandatory = False
                           
                        else:
                            relationalAttribute = OneToOneRelationshipAttribute()
                            relationalAttribute.name = referenceName
                            relationalAttribute.entity = targetClass
                            relationalAttribute.containment = False
                            relationalAttribute.mandatory = False
                    else:
                        if (sourceTo_Target_Cardinality.strip() == "*"):
                            referenceName = referenceName + "s"
                            relationalAttribute = OneToManyRelationshipAttribute()
                            relationalAttribute.name = referenceName
                            relationalAttribute.entity = targetClass
                            relationalAttribute.containment = False
                            relationalAttribute.mandatory = True
                           
                        else:
                            relationalAttribute = OneToOneRelationshipAttribute()
                            relationalAttribute.name = referenceName
                            relationalAttribute.entity = targetClass
                            relationalAttribute.containment = False
                            relationalAttribute.mandatory = True

                    
                    
                    if (not (theClass is None) ) :
                        relationalAttribute.name=theClass.name+"_"+ relationalAttribute.name
                        theClass.attributes.extend([relationalAttribute])

        SQLDeveloperImport.saveModelAsXMIFile(self, openRegSpecs, outputDirectory )  
        SQLDeveloperImport.saveModelAsRPMNFile(self, openRegSpecs, outputDirectory ) 
        #SQLDeveloperImport.createAndSaveDetailedMetaModel(self, openRegSpecs, outputDirectory )                  
        
    def addMembersToEnum(self, openRegSpecs, enum):
        for membersModule in openRegSpecs.types_and_concepts.memberModules:
            for member in membersModule.members:
                theDomain = member.domain_id
                if(theDomain.name == enum.name):
                    theEnumLiteral =EEnumLiteral(member.name)
                    enum.eLiterals.extend([theEnumLiteral])
                    
    def createAndSaveDetailedMetaModel(self, openRegSpecs, outputDirectory ):
        # create the enums first.
        
        my_ecore_schema = EPackage('detailed_model', nsURI='http://detailed_model', nsPrefix='detailed_model')
        
        for domainsModel in openRegSpecs.types_and_concepts.domainModules:
            for domain in domainsModel.domains:
                theEnum =EEnum(domain.name)
                print ("DDD")
                print (theEnum.eClass.ePackage.name)
                SQLDeveloperImport.addMembersToEnum(self,openRegSpecs,theEnum)
                my_ecore_schema.eClassifiers.extend([theEnum])
                print (theEnum.eClass.ePackage.name)
                
        for entityModule in openRegSpecs.data_model:
            
            for entity in entityModule.entities:
                if isinstance(entity,BasicEntity):
                    # Define a Root that can contain A and B instances,
                    # B instances can hold references towards A instances

                    theClass = EClass(entity.name)
                    # theClass.ePackage = my_ecore_schema

                    # Add all the concepts to an EPackage
                    
                    my_ecore_schema.eClassifiers.extend([theClass])

                    
                    for attribute in entity.attributes:
                        if not(isinstance(attribute, RelationshipAttribute)):
                            if(attribute.type.domain_id.is_enumerated):
                                eenum = SQLDeveloperImport.getEEnumForDomain(self,attribute.type.name, my_ecore_schema.eClassifiers)
                                theClass.eStructuralFeatures.append(EAttribute(attribute.name, eenum))

            rset = ResourceSet()
            print(rset.metamodel_registry)
            resource = rset.create_resource(URI(outputDirectory + 'detailed_model.ecore'))  # This will create an XMI resource
            print(resource.reverse_nsmap)
            resource = XMIResource(URI(outputDirectory + 'detailed_model.ecore'))
            resource.append(my_ecore_schema)  # we add the EPackage instance in the resource
            resource.save()  # we then serialize it
            print(resource.reverse_nsmap)
                   

    def getEEnumForDomain(self,attributeTypeName, classifiers):
        for classifier in classifiers:
            if classifier.name == attributeTypeName:
                return classifier
        
    def getETypeForAttribute(self, attribute):
        return EString
        
    def saveModelAsXMIFile(self, openRegSpecs, outputDirectory ):
        # save model as a xmi file
        rset = ResourceSet()
        
        resource = rset.create_resource(URI(outputDirectory + 'ldm.open_reg_specs'))  # This will create an XMI resource
        resource.append(openRegSpecs)
        resource.save()
    
    def saveModelAsRPMNFile(self, openRegSpecs, outputDirectory ):
        f = open(outputDirectory + 'data_model.open_reg_specs', "a")
        f.write("OpenRegSpecs {\r")
        for entityModule in openRegSpecs.data_model:
            f.write("\t data_model { \r")
            f.write("\t\t EntityModule " + entityModule.name + "{ { \r")    
            for entity in entityModule.entities:
                if isinstance(entity,DerivedEntity):
                    f.write("\t\t\t DerivedEntity " + entity.name + "{\r")
                    
                    for attribute in entity.attributes:
                        f.write("\t\t\t\t" + type(attribute).__name__  + " " + attribute.name  )
                        if isinstance(attribute, RelationshipAttribute):
                            f.write(" entity "   + attribute.entity.name)
                            if (attribute.containment):
                                 f.write(" owning"  )
                            if (attribute.mandatory):
                                 f.write(" mandatory"  )
                            if (attribute.dominant):
                                 f.write(" dominant"  )
                            f.write(" \r"  )
                        else:
                            f.write(" type \""  + SQLDeveloperImport.qualifiedTypeName(self,attribute.type) + "\"\r" )
                            
                    f.write("\t\t\t}\r")
                if isinstance(entity,BasicEntity):
                    f.write("\t\t\t BasicEntity " + entity.name)
                    if entity.superClass:
                        f.write(" inheritsFrom " +  entity.superClass.name) 
                    f.write(" { \r")  
                    for attribute in entity.attributes:
                        f.write("\t\t\t\t" + type(attribute).__name__  + " " + attribute.name  )
                        if isinstance(attribute, RelationshipAttribute):
                            f.write(" entity "   + attribute.entity.name)
                            if (attribute.containment):
                                 f.write(" owning"  )
                            if (attribute.mandatory):
                                 f.write(" mandatory"  )
                            if (attribute.dominant):
                                 f.write(" dominant"  )
                            f.write(" \r"  )
                        else:
                            f.write(" type \""  + SQLDeveloperImport.qualifiedTypeName(self,attribute.type) + "\"\r" )
                            
                    f.write("\t\t\t}\r")
                if isinstance(entity,GeneratedEntity):
                    f.write("\t\t\t GeneratedEntity " + entity.name + "{\r")
                    for attribute in entity.attributes:
                        f.write("\t\t\t\t  Attribute " + attribute.name )
                        f.write(" type \""  + SQLDeveloperImport.qualifiedTypeName(self,attribute.type) + "\"\r" ) 
                    f.write("\t\t\t  }\r")

            f.write("\t\t  }\r")
        f.write("\t  }\r")
        f.write("  }\r")
        f.close()
        
        f1 = open(outputDirectory + 'domains.open_reg_specs', "a")
        f1.write("OpenRegSpecs {\r")
        f1.write("\t types_and_concepts SMCubesCoreModel " + openRegSpecs.types_and_concepts.name + " {\r")
        for domainModule in openRegSpecs.types_and_concepts.domainModules:
            f1.write("\t\t domainModules { DomainModule " + domainModule.name + " { {\r")
            for domain in domainModule.domains:
                f1.write("\t\t\t DOMAIN " + domain.name )
                if domain.is_enumerated:
                    f1.write(" is_enumerated")
                else:
                    f1.write( " data_type " + domain.data_type.name)
                f1.write("\r")
        f1.write("\t\t  }\r")
        f1.write("\t  }\r")
        f1.write("  }\r")
        f1.close()
                    
        f2 = open(outputDirectory + 'members.open_reg_specs', "a")
        f2.write("OpenRegSpecs {\r")
        f2.write("\t types_and_concepts SMCubesCoreModel " + openRegSpecs.types_and_concepts.name + " {\r")
        
        for memberModule in openRegSpecs.types_and_concepts.memberModules:
            f2.write("\t\t memberModules { MemberModule " + memberModule.name + " { {\r")
            for member in memberModule.members:
                f2.write("\t\t\t MEMBER " + member.name )
                f2.write( " domain \"" + SQLDeveloperImport.qualifiedDomainName(self,member.domain_id) + "\"\r")
                f2.write("\r")
        f2.write("\t\t  }\r")
        f2.write("\t  }\r")
        f2.write("  }\r")
        f2.close()
        
        f3 = open(outputDirectory + 'subdomains.open_reg_specs', "a")
        f3.write("OpenRegSpecs {\r")
        f3.write("\t types_and_concepts SMCubesCoreModel " + openRegSpecs.types_and_concepts.name + " {\r")
        
        for subDomainModule in openRegSpecs.types_and_concepts.subDomainModules:
            f3.write("\t\t subDomainModules { SubDomainModule " + subDomainModule.name + " { {\r")
            for subdomain in subDomainModule.subdomains:
                f3.write("\t\t\t SUBDOMAIN " + subdomain.name )
                f3.write( " domain \"" + SQLDeveloperImport.qualifiedDomainName(self,member.domain_id) + "\"\r")
                f3.write("\r")
        f3.write("\t\t  }\r")
        f3.write("\t  }\r")
        f3.write("  }\r")
        f3.close()
        
    def qualifiedEntityName(self, entity):
        returnName = entity.name
        if entity.eContainer():
            if not(isinstance(type.eContainer(),OpenRegSpecs)):
                returnName = entity.eContainer().name + "." +returnName
        if entity.eContainer().eContainer():
            if not(isinstance(type.eContainer().eContainer(),OpenRegSpecs)):
                returnName = entity.eContainer().eContainer().name + "." +returnName
        if entity.eContainer().eContainer().eContainer():
            if not(isinstance(type.eContainer().eContainer().eContainer(),OpenRegSpecs)):
                returnName = entity.eContainer().eContainer().eContainer().name + "." +returnName
        if entity.eContainer().eContainer().eContainer().eContainer():
            if not(isinstance(type.eContainer().eContainer().eContainer(),OpenRegSpecs)):
                returnName = entity.eContainer().eContainer().eContainer().eContainer().name + "." +returnName
        return returnName;
        
    def qualifiedTypeName(self, type):
        returnName = type.name
        if type.eContainer():
            if not(isinstance(type.eContainer(),OpenRegSpecs)):
                returnName = type.eContainer().name + "." + returnName
        if type.eContainer().eContainer():
            if not(isinstance(type.eContainer().eContainer(),OpenRegSpecs)):
                returnName = type.eContainer().eContainer().name + "." + returnName
        if type.eContainer().eContainer().eContainer():
            if not(isinstance(type.eContainer().eContainer().eContainer(),OpenRegSpecs)):
                returnName = type.eContainer().eContainer().eContainer().name + "." + returnName
        if type.eContainer().eContainer().eContainer().eContainer():
            if not(isinstance(type.eContainer().eContainer().eContainer().eContainer(),OpenRegSpecs)):
                returnName = type.eContainer().eContainer().eContainer().eContainer().name + "." + returnName
        return returnName;
    
    def qualifiedDomainName(self, domain):
        returnName = domain.name
        if domain.eContainer():
            if not(isinstance(domain.eContainer(),OpenRegSpecs)):
                returnName = domain.eContainer().name + "." + returnName
        if domain.eContainer().eContainer():
            if not(isinstance(domain.eContainer().eContainer(),OpenRegSpecs)):
                returnName = domain.eContainer().eContainer().name + "." + returnName
        if domain.eContainer().eContainer().eContainer():
            if not(isinstance(domain.eContainer().eContainer().eContainer(),OpenRegSpecs)):
                returnName = domain.eContainer().eContainer().eContainer().name + "." + returnName
        if domain.eContainer().eContainer().eContainer().eContainer():
            if not(isinstance(domain.eContainer().eContainer().eContainer().eContainer(),OpenRegSpecs)):
                returnName = domain.eContainer().eContainer().eContainer().eContainer().name + "." + returnName
        return returnName;
    
        
    def superclassContainsFeature(self,theSuperClass, attribute) :
        attributes = theSuperClass.attributes
        contains = False;
        for attribute2 in attributes:
            if (attribute2.name == attribute.name):
                contains = True;

        return contains
    
    def numberofRelationShipsToThisClass(self,sourceClass, targetClass):
        #TODO Auto-generated method stub
        attributes = sourceClass.attributes
        counter = 0;
        # do this for relationship attributes only.
        for attribute in attributes:
            if ( isinstance(attribute,RelationshipAttribute)):
                sourceAttributeType = attribute.entity            
                if (sourceAttributeType == targetClass):
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
        
if __name__ == '__main__':
    SQLDeveloperImport().convert('C:\\Users\\Neil\\freebirdtools-develop-jun22\\git\\efbt\\openregspecs\\python\\resources','C:\\Users\\Neil\\freebirdtools-develop-jun22\\git\\efbt\\openregspecs\\python\\results\\')
    
            
            
