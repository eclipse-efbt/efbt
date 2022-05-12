'''
Created on 22 Jan 2022

@author: Neil
'''
from bird_model import EntityModule, Entity, DerivedEntity, BasicEntity,Attribute,OneToOneRelationshipAttribute,OneToManyRelationshipAttribute,RelationshipAttribute, MEMBER, DOMAIN, FACET_VALUE_TYPE, SUBDOMAIN,VARIABLE,  DomainModule, BIRDModel, SMCubesCoreModel, MemberModule, VariableModule,SubDomainModule
from pyecore.resources import ResourceSet, URI
import csv
class SQLDeveloperImport(object):
        
    def convert (self,fileDirectory,outputDirectory):  
        birdModel = BIRDModel() 
        birdpackage = EntityModule( nsURI='"http://www.eclipse.org/bird"', nsPrefix='bird')
        birdpackage.name = 'bird'
        birdModel.entityModule.extend([birdpackage])
        
        smcubesCoreModel = SMCubesCoreModel()
        birdModel.smcubesCoreModel = smcubesCoreModel
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
                    eclass = BasicEntity()
                    eclass.name = alteredClassName
                    birdpackage.entities.extend([eclass])
                    classesMap[objectID]=eclass
                    #print("birdpackage.eClassifiers")
                    #print(birdpackage.eClassifiers)
                    #print("classesMap")
                    #print(classesMap)

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
                        theDomain.name = adaptedEnumName
                        theSubDomain = SUBDOMAIN()
                        theSubDomain.name = adaptedEnumName
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
                        attribute.variable = variable
                        attribute.classifier = theSubDomain
                   

                    if (attributeKind == "Logical Type"):
                        dataTypeID = row[14]
                        try:
                            datatype = datatypeMap[dataTypeID]
                            attribute = EAttribute()
                            attribue.name =amendedAttributeName
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
                            attribute.variable = variable
                            attribute.classifier = theSubDomain
                        except KeyError:
                            print("missing datatype: ")
                            print(dataTypeID)                       

                    classID = row[4]

                    try:
                        theClass = classesMap[classID]
                        theClass.attributes.extend([attribute])
                    except:
                        print( "missing class2: " )
                        print(classID)
                        
        #remove any attributes that already exist in superclass.

        for theClass in classesMap.values():
            
            superclass = theClass.superClass
            if (superclass):
                
                attributes = superclass.attributes
                attributesToDelete = []
                for theAttribute in attributes :
                    if SQLDeveloperImport.superclassContainsFeature(self,superclass, theAttribute):
                        attributesToDelete.append(theAttribute);

                for theAttribute in attributesToDelete :
                    attributes.remove(theAttribute);
                    print( "removed eStructuralFeature since it exists in the superclass")
                    print(  theAttribute )
               
          
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
                        theClass.attributes.extend([relationalAttribute])

        rset = ResourceSet()
        
        resource = rset.create_resource(URI(outputDirectory + 'ldm.bird_model'))  # This will create an XMI resource
        resource.append(birdModel)
        resource.save()
        
        
        
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
    SQLDeveloperImport().convert('C:\\Users\\Neil\\freebirdtools-develop-march22\\git\\efbt\\python\\resources','C:\\Users\\Neil\\freebirdtools-develop-march22\\git\\efbt\\python\\results\\')
    
            
            
