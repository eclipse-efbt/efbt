'''
Created on 22 Jan 2022

@author: Neil
'''
from data_meta_model import EntityModule, Entity, DerivedEntity, BasicEntity
from core import MEMBER
from pyecore.resources import ResourceSet, URI
import csv
class SQLDeveloperImport(object):
        
    def convert (self,fileDirectory,outputDirectory):   
        birdpackage = EntityModule( nsURI='"http://www.eclipse.org/bird"', nsPrefix='bird')
        birdpackage.name = 'bird'
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

        enumMap = {}
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
                    print(SQLDeveloperImport.inEnumBlackList(self,adaptedEnumName))
                    if(not SQLDeveloperImport.inEnumBlackList(self,adaptedEnumName)):
                        theEnumeration = MEMBER(adaptedEnumName)
                        birdpackage.eClassifiers.extend([theEnumeration])
                        enumMap[enumID] = theEnumeration

                        
                        
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
                        value = row[3]
                        adaptedValue = SQLDeveloperImport.replaceSpaceWithUnderscore(self,value)
                        try:
                            theEnumeration = enumMap[enumID]
                            literals = theEnumeration.eLiterals
                            #if the literal does not exist already, then add it
                            if (not SQLDeveloperImport.containsLiteral(self,literals, adaptedValue)):
                                literal = EEnumLiteral(adaptedValue)
                                literal.value = counter
                                literals.add(literal)
                                
                        except KeyError:
                            print( "missing domain: " + enumID )
                            
                        literals = theEnumeration.eLiterals
                        #if the literal does not exist already, then add it
                        if (not SQLDeveloperImport.containsLiteral(self,literals, adaptedValue)):
                            literal = EEnumLiteral(adaptedValue)
                            literal.value = counter;
                            literals.add(literal)
                    except IndexError:
                        print( "row in DM_Domain_AVT.csv skipped  due to improper formatting at row number")
                        print(counter)

                        
        # for each logicalDatatype for orcle 12c, make a Datatype if we have an
        # equivalent
        datatypeMap = {}
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
                    amendedAttributeName = SQLDeveloperImport.replaceSpaceWithUnderscore(self,attributeName);
                    attributeKind = row[7]
                    attribute = EAttribute(amendedAttributeName)
                    

                    if (attributeKind == "Domain"):
                        domainID = row[12]
                        enumeration = enumMap[domainID]
                        attribute = EAttribute(amendedAttributeName,enumeration)
                   

                    if (attributeKind == "Logical Type"):
                        dataTypeID = row[14]
                        try:
                            datatype = datatypeMap[dataTypeID]
                            attribute = EAttribute(amendedAttributeName,datatype)
                        except KeyError:
                            print("missing datatype: ")
                            print(dataTypeID)                       

                    classID = row[4]

                    try:
                        theClass = classesMap[classID]
                        theClass.eStructuralFeatures.append(attribute)
                    except:
                        print( "missing class: " )
                        print(classID)
                        
        #remove any attributes that already exist in superclass.

        for theClass in classesMap.values():
            
            superclasses = theClass.eAllSuperTypes()
            if (len(superclasses) > 0) :
                for theSuperClass in superclasses:
                    features = theClass.eStructuralFeatures
                    featuresToDelete = []
                for eStructuralFeature in features :
                    if SQLDeveloperImport.superclassContainsFeature(self,theSuperClass, eStructuralFeature):
                        featuresToDelete.append(eStructuralFeature);

                for eStructuralFeature in featuresToDelete :
                    features.remove(eStructuralFeature);
                    print( "removed eStructuralFeature since it exists in the superclass")
                    print(  eStructuralFeature )
               
          
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
                        print("missing class: " + sourceID)
                    
                    try:   
                        targetClass = classesMap[targetID] 
                    except KeyError:
                        print("missing target class: " + targetID) 
                                          
                    numOfRelations = SQLDeveloperImport.numberofRelationShipsToThisClass(self,theClass,targetClass)
                    if(numOfRelations>0):
                        referenceName = referenceName + str(numOfRelations)
                    
                    if (target_Optional.strip() == "Y"):
                        if (sourceTo_Target_Cardinality.strip() == "*"):
                            referenceName = referenceName + "s"
                            reference = EReference(referenceName,containment=False, eType=targetClass, lower=0,upper=-1)
                        else:
                            reference = EReference(referenceName,containment=False, eType=targetClass, lower=0,upper=1)
                    else:
                        if (sourceTo_Target_Cardinality.strip() == "*"):
                            referenceName = referenceName + "s"
                            reference = EReference(referenceName,containment=False, eType=targetClass, lower=1,upper=-1)
                        else:
                            reference = EReference(referenceName,containment=False, eType=targetClass, lower=1,upper=1)

                    
                    
                    if (not (theClass is None) ) :
                        theClass.eStructuralFeatures.append(reference)

        rset = ResourceSet()
        resource = rset.create_resource(URI(outputDirectory + 'ldm.ecore'))  # This will create an XMI resource
        resource.append(birdpackage)  # we add the EPackage instance in the resource
        resource.save()  # we then serialize it
        
    def superclassContainsFeature(self,theSuperClass, eStructuralFeature) :
        features = theSuperClass.eAllStructuralFeatures()
        contains = False;
        for eStructuralFeature2 in features:
            if (eStructuralFeature2.name == eStructuralFeature.name):
                contains = True;

        return contains
    
    def numberofRelationShipsToThisClass(self,sourceClass, targetClass):
        #TODO Auto-generated method stub
        features = sourceClass.eStructuralFeatures
        counter = 0;
        
        for eStructuralFeature in features:
            sourceFeatureType = eStructuralFeature.eType            
            if (sourceFeatureType == targetClass):
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
        
    def containsLiteral(self,literals,adaptedValue):
        contains = False;
        for eEnumLiteral in literals:
            if (eEnumLiteral.name == adaptedValue):
                contains = True

        return contains     
    
if __name__ == '__main__':
    SQLDeveloperImport().convert('C:\\Users\\Neil\\freebirdtools-develop-march22\\git\\efbt\\python\\resources','C:\\Users\\Neil\\freebirdtools-develop-march22\\git\\efbt\\python\\results\\')
    
            
            
